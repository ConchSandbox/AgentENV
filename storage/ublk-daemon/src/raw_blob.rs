use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};

use anyhow::{bail, Context, Result};
use storage_util::io_ring::IoRingHandle;
use tokio::sync::Mutex;
use uvm_ublk::{RawBlobTarget, RawReadMode, UVMUblkCtrlBuilder, UVMUblkDev, UVMUblkDevBuilder};

use crate::protocol::DaemonResponse;
use crate::runtime::InvalidRequest;

#[derive(Debug, Clone, Eq)]
pub(crate) struct RawBlobSpec {
    pub blob_digest: String,
    pub blob_path: PathBuf,
    pub blob_url: Option<String>,
    pub size_bytes: u64,
    pub read_mode: RawReadMode,
}

impl PartialEq for RawBlobSpec {
    fn eq(&self, other: &Self) -> bool {
        self.blob_digest == other.blob_digest
            && self.blob_path.as_os_str() == other.blob_path.as_os_str()
            && self.blob_url == other.blob_url
            && self.size_bytes == other.size_bytes
            && self.read_mode == other.read_mode
    }
}

impl RawBlobSpec {
    fn validate(&self) -> Result<()> {
        if self.blob_digest.is_empty() {
            return Err(InvalidRequest("raw blob requires a digest".into()).into());
        }
        match &self.blob_url {
            Some(url) if !url.is_empty() && self.blob_path.as_os_str().is_empty() => {
                if self.read_mode != RawReadMode::CopyBuffered {
                    return Err(
                        InvalidRequest("HTTP raw blobs require copy-buffered mode".into()).into(),
                    );
                }
            }
            None if self.blob_path.is_absolute() => (),
            _ => {
                return Err(InvalidRequest(
                    "provide exactly one absolute blob_path or nonempty blob_url".into(),
                )
                .into())
            }
        }
        uvm_ublk::impls::raw_blob_target::validate_size(self.size_bytes)
            .map_err(|err| InvalidRequest(err.to_string()).into())
    }
}

/// One activation owns its device ID until DEL_DEV has positively completed.
/// Failed create/delete operations keep this record available for keyed cleanup.
#[derive(Default)]
struct RawBlobEntry {
    spec: Option<RawBlobSpec>,
    terminated: bool,
    ready: bool,
    dev_id: Option<u32>,
    dev: Option<UVMUblkDev<RawBlobTarget>>,
    // Keep the backing FD alive after closing the char device, through DEL_DEV.
    target: Option<Arc<RawBlobTarget>>,
}

impl RawBlobEntry {
    fn bind(&mut self, spec: &RawBlobSpec) -> Result<()> {
        if self.terminated {
            return Err(InvalidRequest("raw blob activation key has been deleted".into()).into());
        }
        match &self.spec {
            Some(existing) if existing != spec => {
                Err(InvalidRequest("raw blob activation key arguments differ".into()).into())
            }
            Some(_) => Ok(()),
            None => {
                self.spec = Some(spec.clone());
                Ok(())
            }
        }
    }

    fn response(&self) -> Result<DaemonResponse> {
        match (self.ready, self.dev_id, &self.spec) {
            (true, Some(dev_id), Some(spec)) => Ok(DaemonResponse::RawBlobDevice {
                dev_id,
                device_path: PathBuf::from(format!("/dev/ublkb{dev_id}")),
                size_bytes: spec.size_bytes,
                read_mode: spec.read_mode,
            }),
            (_, None, _) => Ok(DaemonResponse::RawBlobMissing),
            _ => bail!("raw blob device requires keyed cleanup before it can be used"),
        }
    }

    async fn cleanup(&mut self, ring: IoRingHandle<io_uring::squeue::Entry128>) -> Result<()> {
        self.ready = false;
        let Some(dev_id) = self.dev_id else {
            self.target = None;
            return Ok(());
        };
        if let Some(dev) = &mut self.dev {
            if let Err(err) = dev.ctrl.stop_dev().await {
                if !is_missing(&err) {
                    return Err(err).context("stop raw blob device");
                }
            }
            // Do not release buffers, FDs, or the source lease while I/O is live.
            dev.wait_for_bg_tasks().await;
        }
        // DEL_DEV waits for every char-device FD to close.
        self.dev = None;
        let mut ctrl = UVMUblkCtrlBuilder::new().dev_id(dev_id).build(ring)?;
        if let Err(err) = ctrl.del_dev().await {
            if !is_missing(&err) {
                return Err(err).context("delete raw blob device");
            }
        }
        // Never reuse this ID after confirmation: it may already name a new device.
        self.dev_id = None;
        self.target = None;
        Ok(())
    }
}

fn is_missing(err: &anyhow::Error) -> bool {
    err.downcast_ref::<std::io::Error>()
        .and_then(|e| e.raw_os_error())
        == Some(libc::ENODEV)
}

#[derive(Default)]
struct Registry {
    closed: bool,
    entries: HashMap<String, Arc<Mutex<RawBlobEntry>>>,
}

/// Key locks serialize create/query/delete, including connection-loss retries.
/// Tombstones last for this daemon process; there is no business refcount here.
#[derive(Default)]
pub(crate) struct RawBlobDevices {
    registry: StdMutex<Registry>,
}

impl RawBlobDevices {
    fn entry(&self, key: &str) -> Result<Arc<Mutex<RawBlobEntry>>> {
        if key.is_empty() {
            return Err(InvalidRequest("raw blob device_key must not be empty".into()).into());
        }
        let mut registry = self.registry.lock().unwrap();
        if registry.closed {
            bail!("raw blob service is shutting down");
        }
        Ok(registry.entries.entry(key.to_owned()).or_default().clone())
    }

    pub(crate) async fn create(
        &self,
        key: &str,
        spec: RawBlobSpec,
        ring: IoRingHandle<io_uring::squeue::Entry128>,
    ) -> Result<DaemonResponse> {
        spec.validate()?;
        let entry = self.entry(key)?;
        let mut entry = entry.lock().await;
        entry.bind(&spec)?;
        if entry.ready {
            return entry.response();
        }
        if entry.dev_id.is_some() {
            bail!("raw blob creation requires keyed cleanup");
        }
        let target = Arc::new(match &spec.blob_url {
            Some(url) => RawBlobTarget::new_http(url, spec.size_bytes, spec.read_mode)?,
            None => RawBlobTarget::new(&spec.blob_path, spec.size_bytes, spec.read_mode)?,
        });
        let ctrl = UVMUblkCtrlBuilder::new()
            .name("raw-blob")
            .zero_copy(spec.read_mode.is_zero_copy())
            .no_auto_partition_scan()
            .build(ring.clone())?;
        let features = ctrl
            .get_features()
            .await
            .context("query raw blob ublk features")?;
        if features & uvm_ublk::ublk_caps::UBLK_F_NO_AUTO_PART_SCAN == 0 {
            bail!("raw blobs require UBLK_F_NO_AUTO_PART_SCAN to prevent eager partition reads");
        }
        if spec.read_mode.is_zero_copy() && features & uvm_ublk::ublk_caps::UBLK_F_AUTO_BUF_REG == 0
        {
            bail!("requested raw blob zero-copy mode requires UBLK_F_AUTO_BUF_REG");
        }
        entry.target = Some(Arc::clone(&target));
        let result = async {
            let dev = UVMUblkDevBuilder::new(ctrl)
                .set_shared_target(target)
                .build_tracked(&mut entry.dev_id)
                .await?;
            entry.dev = Some(dev);
            let dev = entry.dev.as_mut().unwrap();
            let actual_zero_copy =
                dev.ctrl.dev_info.flags & uvm_ublk::ublk_caps::UBLK_F_AUTO_BUF_REG != 0;
            if actual_zero_copy != spec.read_mode.is_zero_copy()
                || dev.ctrl.dev_info.flags & uvm_ublk::ublk_caps::UBLK_F_NO_AUTO_PART_SCAN == 0
            {
                bail!("kernel did not preserve the requested raw blob buffer mode");
            }
            dev.start().await.context("start raw blob device")?;
            uvm_ublk::wait_for_ublk_dev(dev.dev_id()).context("wait for raw blob device")?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(err) = result {
            if let Err(cleanup) = entry.cleanup(ring).await {
                return Err(err.context(format!("raw blob cleanup remains pending: {cleanup:#}")));
            }
            return Err(err);
        }
        entry.ready = true;
        entry.response()
    }

    pub(crate) async fn get(&self, key: &str) -> Result<DaemonResponse> {
        self.entry(key)?.lock().await.response()
    }

    pub(crate) async fn delete(
        &self,
        key: &str,
        ring: IoRingHandle<io_uring::squeue::Entry128>,
    ) -> Result<DaemonResponse> {
        let entry = self.entry(key)?;
        let mut entry = entry.lock().await;
        // Set the fence before attempting cleanup, even when no Create arrived.
        entry.terminated = true;
        entry.cleanup(ring).await?;
        Ok(DaemonResponse::RawBlobDeleted)
    }

    pub(crate) async fn shutdown(&self, ring: IoRingHandle<io_uring::squeue::Entry128>) {
        let entries: Vec<_> = {
            let mut registry = self.registry.lock().unwrap();
            registry.closed = true;
            registry.entries.values().cloned().collect()
        };
        for entry in entries {
            let mut entry = entry.lock().await;
            entry.terminated = true;
            if let Err(err) = entry.cleanup(ring.clone()).await {
                tracing::error!(?err, "raw blob shutdown cleanup failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> RawBlobSpec {
        RawBlobSpec {
            blob_digest: "sha256:trusted".into(),
            blob_path: "/blob".into(),
            blob_url: None,
            size_bytes: 4096,
            read_mode: RawReadMode::CopyBuffered,
        }
    }

    #[test]
    fn exact_args_are_required_even_after_failed_creation() {
        let mut entry = RawBlobEntry::default();
        entry.bind(&spec()).unwrap();
        entry.bind(&spec()).unwrap();
        let mut alternatives = vec![spec(); 5];
        alternatives[0].blob_digest.push('x');
        alternatives[1].blob_path = "/other".into();
        alternatives[2].size_bytes = 8192;
        alternatives[3].read_mode = RawReadMode::CopyDirect;
        alternatives[4].blob_path = "/./blob".into();
        for changed in alternatives {
            assert!(entry.bind(&changed).is_err());
        }
    }

    #[test]
    fn http_source_requires_exclusive_url_and_copy_buffered() {
        let mut remote = spec();
        remote.blob_url = Some("http://example.invalid/blob".into());
        assert!(remote.validate().is_err());
        remote.blob_path = PathBuf::new();
        assert!(remote.validate().is_ok());
        for mode in [
            RawReadMode::CopyDirect,
            RawReadMode::ZcBuffered,
            RawReadMode::ZcDirect,
        ] {
            remote.read_mode = mode;
            assert!(remote.validate().is_err());
        }
        remote.read_mode = RawReadMode::CopyBuffered;
        let mut entry = RawBlobEntry::default();
        entry.bind(&remote).unwrap();
        remote.blob_url = Some("http://example.invalid/different".into());
        assert!(entry.bind(&remote).is_err());
        remote.blob_url = None;
        assert!(remote.validate().is_err());
        remote.blob_url = Some(String::new());
        assert!(remote.validate().is_err());
    }

    #[tokio::test]
    async fn terminated_key_is_never_rebound_and_pending_cleanup_is_not_missing() {
        let devices = RawBlobDevices::default();
        let first = devices.entry("activation").unwrap();
        let second = devices.entry("activation").unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        first.lock().await.terminated = true;
        assert!(second.lock().await.bind(&spec()).is_err());
        assert!(matches!(
            devices.get("activation").await.unwrap(),
            DaemonResponse::RawBlobMissing
        ));
        let mut pending = RawBlobEntry {
            dev_id: Some(4),
            ..Default::default()
        };
        assert!(pending.response().is_err());
        pending.spec = Some(spec());
        pending.ready = true;
        assert!(matches!(
            pending.response().unwrap(),
            DaemonResponse::RawBlobDevice {
                dev_id: 4,
                size_bytes: 4096,
                ..
            }
        ));
    }

    #[test]
    fn empty_key_and_shutdown_admission_are_rejected() {
        let devices = RawBlobDevices::default();
        assert!(devices.entry("").is_err());
        let entry = devices.entry("activation").unwrap();
        devices.registry.lock().unwrap().closed = true;
        assert!(devices.entry("activation").is_err());
        assert!(devices.entry("new").is_err());
        assert_eq!(Arc::strong_count(&entry), 2);
    }

    #[tokio::test]
    #[ignore = "requires root and ublk"]
    async fn raw_blob_partial_create_cleanup_keeps_key_and_releases_fd() {
        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file().set_len(4096).unwrap();
        let target =
            Arc::new(RawBlobTarget::new(file.path(), 4096, RawReadMode::CopyBuffered).unwrap());
        let weak_target = Arc::downgrade(&target);
        let (ring, _worker) =
            storage_util::io_ring::spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
        let ctrl = UVMUblkCtrlBuilder::new().build(ring.clone()).unwrap();
        let mut entry = RawBlobEntry::default();
        entry.bind(&spec()).unwrap();
        entry.target = Some(Arc::clone(&target));
        let mut dev = UVMUblkDevBuilder::new(ctrl)
            .set_shared_target(target)
            .build_tracked(&mut entry.dev_id)
            .await
            .unwrap();
        let dev_id = dev.dev_id();
        // Force the parameter setup failure that occurs after ADD_DEV.
        assert!(dev.ctrl.set_params(Default::default()).await.is_err());
        entry.dev = Some(dev);
        assert!(entry.response().is_err());
        assert!(weak_target.upgrade().is_some());
        entry.terminated = true;
        entry.cleanup(ring.clone()).await.unwrap();
        assert!(weak_target.upgrade().is_none());
        assert!(entry.dev_id.is_none());
        assert!(!std::path::Path::new(&format!("/dev/ublkc{dev_id}")).exists());
        assert!(entry.bind(&spec()).is_err());
        entry.cleanup(ring).await.unwrap();
        assert!(matches!(
            entry.response().unwrap(),
            DaemonResponse::RawBlobMissing
        ));
    }
}
