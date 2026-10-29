#![cfg(feature = "test-support")]

use anyhow::{ensure, Context, Result};
use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use storage_util::io_ring::{spawn_io_ring_worker, IoRingHandle};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uvm_ublk::{
    RawBlobTarget, RawCache, RawCacheTestControl, RawReadMode, UVMUblkCtrlBuilder, UVMUblkDev,
    UVMUblkDevBuilder,
};

const PAGE: usize = 4096;
const CAPACITY: u64 = 256 * 1024;
const OFFSET: u64 = 4 * 1024 * 1024;
// Linux fs.h: _IOWR('X', 119/120, int).
const FREEZE_FS: libc::c_ulong = 0xc004_5877;
const THAW_FS: libc::c_ulong = 0xc004_5878;

struct Aligned {
    ptr: *mut u8,
    layout: Layout,
}
impl Aligned {
    fn new(len: usize) -> Self {
        let layout = Layout::from_size_align(len, PAGE).unwrap();
        let ptr = unsafe { alloc_zeroed(layout) };
        assert!(!ptr.is_null());
        Self { ptr, layout }
    }
    fn bytes(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.layout.size()) }
    }
}
impl Drop for Aligned {
    fn drop(&mut self) {
        unsafe { dealloc(self.ptr, self.layout) };
    }
}

struct Origin {
    url: String,
    data: Arc<Vec<u8>>,
    requests: Arc<Mutex<Vec<(u64, u64)>>>,
    unavailable: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}
impl Origin {
    async fn start() -> Result<Self> {
        let data = Arc::new(
            (0..8 * 1024 * 1024)
                .map(|i| ((i / PAGE) * 37 + i * 13 + i / (PAGE * 251)) as u8)
                .collect::<Vec<_>>(),
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let unavailable = Arc::new(AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/blob", listener.local_addr()?);
        let (body, log, offline) = (data.clone(), requests.clone(), unavailable.clone());
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let (body, log, offline) = (body.clone(), log.clone(), offline.clone());
                tokio::spawn(async move {
                    let result: Result<()> = async {
                        let mut header = Vec::new();
                        while !header.ends_with(b"\r\n\r\n") {
                            ensure!(header.len() < 8192, "unbounded HTTP request header");
                            header.push(stream.read_u8().await?);
                        }
                        let header = String::from_utf8(header)?.to_lowercase();
                        let range = header.lines().find_map(|s| s.strip_prefix("range: bytes="))
                            .context("missing HTTP Range")?;
                        let (start, end) = range.split_once('-').context("invalid HTTP Range")?;
                        let (start, end): (u64, u64) = (start.parse()?, end.parse()?);
                        ensure!(start <= end && end < body.len() as u64);
                        log.lock().unwrap().push((start, end + 1));
                        if offline.load(Ordering::SeqCst) {
                            stream.write_all(b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await?;
                        } else {
                            let header = format!("HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len(), end-start+1);
                            stream.write_all(header.as_bytes()).await?;
                            // Fragment transport independently of the aligned device request.
                            for part in body[start as usize..=end as usize].chunks(997) {
                                stream.write_all(part).await?;
                            }
                        }
                        Ok(())
                    }.await;
                    if let Err(error) = result {
                        eprintln!("test origin connection: {error:#}");
                    }
                });
            }
        });
        Ok(Self {
            url,
            data,
            requests,
            unavailable,
            task,
        })
    }

    fn count(&self, offset: u64, len: usize) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(start, end)| *start < offset + len as u64 && *end > offset)
            .count()
    }

    async fn quiet(&self) {
        // Existing host probes remain enabled and are allowed to settle.
        let mut previous = self.requests.lock().unwrap().len();
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let current = self.requests.lock().unwrap().len();
            if current == previous {
                return;
            }
            previous = current;
        }
    }

    async fn read(&self, path: &Path, offset: u64, len: usize) -> Result<()> {
        let actual = direct_read(path, offset, len).await?;
        ensure!(
            actual == self.data[offset as usize..offset as usize + len],
            "device data mismatch at {offset}"
        );
        Ok(())
    }
}
impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn direct_read(path: &Path, offset: u64, len: usize) -> Result<Vec<u8>> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;
        let mut buffer = Aligned::new(len);
        file.read_exact_at(buffer.bytes(), offset)?;
        Ok(buffer.bytes().to_vec())
    })
    .await?
}

struct Device {
    dev: Option<UVMUblkDev<RawBlobTarget>>,
    ring: IoRingHandle<io_uring::squeue::Entry128>,
    path: PathBuf,
    id: u32,
}
impl Device {
    async fn start(target: Arc<RawBlobTarget>, depth: u16) -> Result<Self> {
        let (ring, _worker) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
        let ctrl = UVMUblkCtrlBuilder::new()
            .name("raw-cache-test")
            .depth(depth)
            .no_auto_partition_scan()
            .build(ring.clone())?;
        let mut dev = UVMUblkDevBuilder::new(ctrl)
            .set_shared_target(target)
            .build()
            .await?;
        dev.start().await?;
        let id = dev.dev_id();
        uvm_ublk::wait_for_ublk_dev(id)?;
        Ok(Self {
            dev: Some(dev),
            ring,
            path: format!("/dev/ublkb{id}").into(),
            id,
        })
    }

    async fn stop(mut self) -> Result<()> {
        let dev = self.dev.as_mut().unwrap();
        dev.ctrl.stop_dev().await?;
        dev.wait_for_bg_tasks().await;
        self.dev = None;
        let mut ctrl = UVMUblkCtrlBuilder::new()
            .dev_id(self.id)
            .build(self.ring.clone())?;
        ctrl.del_dev().await?;
        ensure!(!self.path.exists(), "deleted block device remains");
        Ok(())
    }
}

fn cache_dir() -> Result<tempfile::TempDir> {
    let base = std::env::var_os("RAW_BLOB_TEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    Ok(tempfile::tempdir_in(base)?)
}

fn resident_pages(path: &Path, offset: u64, len: usize) -> Result<usize> {
    let file = File::open(path)?;
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_NONE,
            libc::MAP_PRIVATE,
            file.as_raw_fd(),
            offset as libc::off_t,
        )
    };
    ensure!(
        ptr != libc::MAP_FAILED,
        "map cache for mincore: {}",
        std::io::Error::last_os_error()
    );
    let mut residency = vec![0u8; len.div_ceil(PAGE)];
    let result = unsafe { libc::mincore(ptr, len, residency.as_mut_ptr()) };
    let error = std::io::Error::last_os_error();
    unsafe { libc::munmap(ptr, len) };
    ensure!(result == 0, "cache mincore: {error}");
    Ok(residency.into_iter().filter(|page| page & 1 != 0).count())
}

fn target(origin: &Origin, cache: &Arc<RawCache>) -> Result<Arc<RawBlobTarget>> {
    Ok(Arc::new(RawBlobTarget::new_http(
        &origin.url,
        origin.data.len() as u64,
        RawReadMode::CopyDirect,
        "opaque-test-identity",
        cache.clone(),
    )?))
}

async fn idle(cache: &RawCache) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let stats = cache.stats();
            ensure!(
                stats.used_bytes <= stats.capacity_bytes,
                "cache exceeded budget: {stats:?}"
            );
            if stats.pending_fills == 0
                && stats.write_starts == stats.write_completions
                && stats.reserved_bytes == 0
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("cache did not finish independent writes while no new FETCH was sent")?
}

fn backing(dir: &Path) -> Result<PathBuf> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with("raw-cache-")
        {
            return Ok(entry.path());
        }
    }
    anyhow::bail!("cache backing file was not created")
}

/// This test needs an ext4/xfs directory with STATX_DIOALIGN and real ublk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires privileged ublk and RAW_BLOB_TEST_DIR on a DIO-capable filesystem"]
async fn raw_cache_read_precedes_write_and_same_slot_keeps_original_t() -> Result<()> {
    let dir = cache_dir()?;
    let cache = RawCache::new(dir.path(), CAPACITY)?;
    let control = Arc::new(RawCacheTestControl::default());
    cache.set_test_control(control.clone());
    let origin = Origin::start().await?;
    // Depth one forces the next read on A to reuse exactly the same slot.
    let first = Device::start(target(&origin, &cache)?, 1).await?;
    let second = Device::start(target(&origin, &cache)?, 1).await?;
    origin.quiet().await;
    idle(&cache).await?;
    let result: Result<()> = async {
        let before = cache.stats();
        let requests = origin.count(OFFSET, PAGE);
        control.set_write_paused(true);
        tokio::time::timeout(
            Duration::from_secs(2),
            origin.read(&first.path, OFFSET, PAGE),
        )
        .await
        .context("READ waited for the paused SSD write")??;
        tokio::time::timeout(
            Duration::from_secs(2),
            control.wait_for_write_starts(before.write_starts + 1),
        )
        .await?;
        let pending = cache.stats();
        ensure!(
            pending.write_completions == before.write_completions,
            "gate did not hold the write before its completion"
        );
        ensure!(pending.reserved_bytes >= PAGE as u64);
        ensure!(origin.count(OFFSET, PAGE) == requests + 1);
        // A different device has its own T and can borrow the original ready T.
        tokio::time::timeout(
            Duration::from_secs(2),
            origin.read(&second.path, OFFSET, PAGE),
        )
        .await
        .context("overlapping READ waited for SSD readiness")??;
        ensure!(
            origin.count(OFFSET, PAGE) == requests + 1,
            "overlapping READ downloaded data instead of borrowing ready T"
        );
        let path = first.path.clone();
        let mut next =
            tokio::spawn(async move { direct_read(&path, OFFSET + PAGE as u64, PAGE).await });
        ensure!(
            tokio::time::timeout(Duration::from_millis(150), &mut next)
                .await
                .is_err(),
            "next same-slot READ overwrote a T still owned by the cache write"
        );
        control.set_write_paused(false);
        let next_data = tokio::time::timeout(Duration::from_secs(5), next).await???;
        ensure!(next_data == origin.data[OFFSET as usize + PAGE..OFFSET as usize + PAGE * 2]);
        // No further block request is needed to publish the completed cache write.
        idle(&cache).await?;
        let file = backing(dir.path())?;
        ensure!(
            resident_pages(&file, OFFSET, PAGE * 2)? == 0,
            "DIO cache payload unexpectedly populated backing page cache"
        );
        let persisted = direct_read(&file, OFFSET, PAGE * 2).await?;
        ensure!(
            persisted == origin.data[OFFSET as usize..OFFSET as usize + PAGE * 2],
            "later slot reuse corrupted the independently written original bytes"
        );
        let cached_requests = origin.count(OFFSET, PAGE * 2);
        origin
            .read(&first.path, OFFSET - PAGE as u64, PAGE * 3)
            .await?;
        ensure!(
            origin.count(OFFSET, PAGE * 2) == cached_requests,
            "partial cache hit redownloaded the already-complete suffix"
        );
        ensure!(
            origin.count(OFFSET - PAGE as u64, PAGE) == 1,
            "partial cache miss did not fetch precisely the missing prefix"
        );
        Ok(())
    }
    .await;
    control.set_write_paused(false);
    first.stop().await?;
    second.stop().await?;
    result?;
    // The service survives the final device reference and creates a fresh P.
    let requests = origin.count(OFFSET, PAGE * 2);
    origin.unavailable.store(true, Ordering::SeqCst);
    let recreated = Device::start(target(&origin, &cache)?, 1).await?;
    let result = origin.read(&recreated.path, OFFSET, PAGE * 2).await;
    recreated.stop().await?;
    result?;
    ensure!(
        origin.count(OFFSET, PAGE * 2) == requests,
        "recreated device fetched an already-complete cache range"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires privileged ublk and RAW_BLOB_TEST_DIR on a DIO-capable filesystem"]
async fn raw_cache_write_failures_are_optional_and_stop_drains() -> Result<()> {
    for delayed in [false, true] {
        let dir = cache_dir()?;
        let cache = RawCache::new(dir.path(), CAPACITY)?;
        let control = Arc::new(RawCacheTestControl::default());
        cache.set_test_control(control.clone());
        let origin = Origin::start().await?;
        let device = Device::start(target(&origin, &cache)?, 1).await?;
        origin.quiet().await;
        idle(&cache).await?;
        control.fail_writes(true);
        control.set_write_paused(delayed);
        let starts = cache.stats().write_starts;
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            origin.read(&device.path, OFFSET, PAGE),
        )
        .await;
        let started = tokio::time::timeout(
            Duration::from_secs(2),
            control.wait_for_write_starts(starts + 1),
        )
        .await;
        let mut stopping = tokio::spawn(device.stop());
        let early_stop = if delayed {
            tokio::time::timeout(Duration::from_millis(150), &mut stopping)
                .await
                .ok()
        } else {
            None
        };
        control.set_write_paused(false);
        let stopped_early = early_stop.is_some();
        match early_stop {
            Some(outcome) => outcome??,
            None => tokio::time::timeout(Duration::from_secs(5), stopping).await???,
        }
        started?;
        ensure!(
            !stopped_early,
            "STOP released T/FD/ring while a cache write still held them"
        );
        result.context("valid READ waited on an optional cache write")??;
        idle(&cache).await?;
        control.fail_writes(false);
        let requests = origin.count(OFFSET, PAGE);
        let retry = Device::start(target(&origin, &cache)?, 1).await?;
        let result = origin.read(&retry.path, OFFSET, PAGE).await;
        retry.stop().await?;
        result?;
        ensure!(
            origin.count(OFFSET, PAGE) == requests + 1,
            "failed write incorrectly published a complete cache range"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires privileged ublk and RAW_BLOB_TEST_DIR on a DIO-capable filesystem"]
async fn raw_cache_lru_is_bounded_and_cache_read_failure_is_not_a_miss() -> Result<()> {
    let dir = cache_dir()?;
    let cache = RawCache::new(dir.path(), CAPACITY)?;
    let control = Arc::new(RawCacheTestControl::default());
    cache.set_test_control(control.clone());
    let origin = Origin::start().await?;
    let device = Device::start(target(&origin, &cache)?, 1).await?;
    origin.quiet().await;
    idle(&cache).await?;
    let result: Result<()> = async {
        // Individual 4 KiB ranges give an exact LRU order, while twice the
        // budget displaces any incidental startup probes from the cache.
        for page in 0..128 {
            origin
                .read(&device.path, OFFSET + page * PAGE as u64, PAGE)
                .await?;
            idle(&cache).await?;
        }
        ensure!(cache.stats().used_bytes == CAPACITY);
        let retained = OFFSET + 64 * PAGE as u64;
        let victim = retained + PAGE as u64;
        let old_retained = origin.count(retained, PAGE);
        origin.read(&device.path, retained, PAGE).await?;
        ensure!(
            origin.count(retained, PAGE) == old_retained,
            "complete range missed before eviction"
        );
        origin
            .read(&device.path, OFFSET + 128 * PAGE as u64, PAGE)
            .await?;
        idle(&cache).await?;
        origin.read(&device.path, retained, PAGE).await?;
        ensure!(
            origin.count(retained, PAGE) == old_retained,
            "LRU evicted the freshly touched range"
        );
        let old_victim = origin.count(victim, PAGE);
        origin.read(&device.path, victim, PAGE).await?;
        ensure!(
            origin.count(victim, PAGE) == old_victim + 1,
            "LRU did not evict the oldest unused range"
        );
        idle(&cache).await?;
        let reads = origin.count(victim, PAGE);
        control.fail_reads(true);
        ensure!(
            direct_read(&device.path, victim, PAGE).await.is_err(),
            "cache read failure did not fail data supply"
        );
        ensure!(
            origin.count(victim, PAGE) == reads,
            "failed cache read silently fell back to HTTP in the same request"
        );
        control.fail_reads(false);
        origin.read(&device.path, victim, PAGE).await?;
        ensure!(
            origin.count(victim, PAGE) == reads + 1,
            "failed cache read was not invalidated"
        );
        Ok(())
    }
    .await;
    control.fail_reads(false);
    device.stop().await?;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires privileged ublk and RAW_BLOB_TEST_DIR on a DIO-capable filesystem"]
async fn raw_cache_partial_write_residuals_remain_charged_and_admission_never_fails_read(
) -> Result<()> {
    let dir = cache_dir()?;
    let cache = RawCache::new(dir.path(), CAPACITY)?;
    let control = Arc::new(RawCacheTestControl::default());
    cache.set_test_control(control.clone());
    let origin = Origin::start().await?;
    let device = Device::start(target(&origin, &cache)?, 1).await?;
    origin.quiet().await;
    idle(&cache).await?;
    control.short_writes(true);
    control.fail_cleanup(true);
    let result: Result<()> = async {
        for range in 0..40 {
            let offset = OFFSET + range * (PAGE * 2) as u64;
            origin.read(&device.path, offset, PAGE * 2).await?;
            idle(&cache).await?;
            let stats = cache.stats();
            ensure!(stats.used_bytes <= CAPACITY);
            ensure!(
                stats.used_bytes >= stats.residual_bytes && stats.residual_bytes > 0,
                "failed partial write was uncharged before actual cleanup: {stats:?}"
            );
        }
        let saturated = cache.stats();
        ensure!(
            saturated.used_bytes + (PAGE * 2) as u64 > CAPACITY,
            "fixture did not reach admission pressure: {saturated:?}"
        );
        let starts = saturated.write_starts;
        origin
            .read(&device.path, OFFSET + 41 * (PAGE * 2) as u64, PAGE * 2)
            .await?;
        idle(&cache).await?;
        ensure!(
            cache.stats().write_starts == starts,
            "cache admitted writes beyond residual plus data capacity"
        );
        let requests = origin.count(OFFSET, PAGE * 2);
        origin.read(&device.path, OFFSET, PAGE * 2).await?;
        ensure!(
            origin.count(OFFSET, PAGE * 2) == requests + 1,
            "short write published an incomplete range"
        );
        Ok(())
    }
    .await;
    control.short_writes(false);
    control.fail_cleanup(false);
    device.stop().await?;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires privileged ublk and RAW_BLOB_TEST_DIR on a DIO-capable filesystem"]
async fn raw_cache_exclusive_lock_precedes_cleanup_and_restart_has_no_ready_ranges() -> Result<()> {
    let dir = cache_dir()?;
    let cache = RawCache::new(dir.path(), CAPACITY)?;
    let origin = Origin::start().await?;
    let device = Device::start(target(&origin, &cache)?, 1).await?;
    origin.quiet().await;
    let result = origin.read(&device.path, OFFSET, PAGE).await;
    device.stop().await?;
    result?;
    idle(&cache).await?;
    let file = backing(dir.path())?;
    let before = direct_read(&file, OFFSET, PAGE).await?;
    ensure!(
        RawCache::new(dir.path(), CAPACITY).is_err(),
        "second owner obtained cache directory"
    );
    ensure!(
        direct_read(&file, OFFSET, PAGE).await? == before,
        "failed lock acquisition cleaned the live owner's data"
    );
    drop(cache);
    let restarted = RawCache::new(dir.path(), CAPACITY)?;
    ensure!(
        restarted.stats().used_bytes == 0 && restarted.stats().ready_bytes == 0,
        "restart inferred cache readiness from stale backing files"
    );
    let requests = origin.count(OFFSET, PAGE);
    let fresh = Device::start(target(&origin, &restarted)?, 1).await?;
    let result = origin.read(&fresh.path, OFFSET, PAGE).await;
    fresh.stop().await?;
    result?;
    ensure!(
        origin.count(OFFSET, PAGE) == requests + 1,
        "restart did not fetch data into a new empty runtime cache"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires privileged ublk and 31 seconds to cross the READ deadline"]
async fn raw_cache_old_read_deadline_does_not_cancel_idle_next_fetch() -> Result<()> {
    let dir = cache_dir()?;
    let cache = RawCache::new(dir.path(), CAPACITY)?;
    let origin = Origin::start().await?;
    let device = Device::start(target(&origin, &cache)?, 1).await?;
    origin.quiet().await;
    let result: Result<()> = async {
        origin.read(&device.path, OFFSET, PAGE).await?;
        idle(&cache).await?;
        // COMMIT_AND_FETCH already completed the old READ, but its CQE may
        // still be waiting for the next request when the old deadline passes.
        tokio::time::sleep(Duration::from_secs(31)).await;
        tokio::time::timeout(
            Duration::from_secs(2),
            origin.read(&device.path, OFFSET + PAGE as u64, PAGE),
        )
        .await
        .context("old READ deadline cancelled or wedged the next FETCH")??;
        Ok(())
    }
    .await;
    device.stop().await?;
    result
}

async fn rpc(
    socket: &Path,
    request: uvm_ublk_daemon::protocol::DaemonRequest,
) -> Result<uvm_ublk_daemon::protocol::DaemonResponse> {
    use uvm_ublk_daemon::protocol::{recv_message, send_message};
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut stream = tokio::net::UnixStream::connect(socket).await?;
        send_message(&mut stream, &request).await?;
        recv_message(&mut stream)
            .await?
            .context("daemon closed RPC without response")
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires privileged ublk and RAW_BLOB_TEST_DIR on a DIO-capable filesystem"]
async fn raw_cache_daemon_shutdown_drains_pending_write_before_releasing_device() -> Result<()> {
    use overlaybd::image_service::ImageService;
    use uvm_ublk_daemon::protocol::{DaemonRequest, DaemonResponse};
    use uvm_ublk_daemon::UblkDaemonServer;
    let dir = cache_dir()?;
    let cache = RawCache::new(&dir.path().join("ram"), CAPACITY)?;
    let control = Arc::new(RawCacheTestControl::default());
    cache.set_test_control(control.clone());
    let global = dir.path().join("global.json");
    std::fs::write(
        &global,
        serde_json::to_vec(&serde_json::json!({
            "ioEngine": 0, "download": {"enable": false}, "p2pConfig": {"enable": false},
            "cacheConfig": {"cacheType": "file", "cacheDir": dir.path().join("overlaybd"), "cacheSizeGB": 1}
        }))?,
    )?;
    let image = ImageService::from_config_path(&global).await?;
    let (ring, _worker) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
    let socket = dir.path().join("daemon.sock");
    let mut server = UblkDaemonServer::new(socket.clone(), ring, image, "/dev/null".into());
    server.set_raw_cache(cache.clone());
    let server = Arc::new(server);
    let runner = server.clone();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let mut task = tokio::spawn(async move {
        runner
            .run_with_ready_signal(|| {
                let _ = ready_tx.send(());
                Ok(())
            })
            .await
    });
    ready_rx.await?;
    let origin = Origin::start().await?;
    let mut created_path = None;
    let result: Result<()> = async {
        let response = rpc(
            &socket,
            DaemonRequest::CreateRawBlob {
                device_key: "shutdown-write-test".into(),
                blob_digest: "opaque-test-identity".into(),
                blob_path: PathBuf::new(),
                blob_url: Some(origin.url.clone()),
                size_bytes: origin.data.len() as u64,
                read_mode: RawReadMode::CopyDirect,
            },
        )
        .await?;
        let DaemonResponse::RawBlobDevice { device_path, .. } = response else {
            anyhow::bail!("create remote device: {response:?}");
        };
        created_path = Some(device_path.clone());
        origin.quiet().await;
        idle(&cache).await?;
        let starts = cache.stats().write_starts;
        control.set_write_paused(true);
        tokio::time::timeout(
            Duration::from_secs(2),
            origin.read(&device_path, OFFSET, PAGE),
        )
        .await??;
        tokio::time::timeout(
            Duration::from_secs(2),
            control.wait_for_write_starts(starts + 1),
        )
        .await?;
        server.request_shutdown();
        ensure!(
            tokio::time::timeout(Duration::from_millis(150), &mut task)
                .await
                .is_err(),
            "daemon shutdown abandoned an independently owned cache write"
        );
        Ok(())
    }
    .await;
    control.set_write_paused(false);
    server.request_shutdown();
    if !task.is_finished() {
        tokio::time::timeout(Duration::from_secs(10), &mut task).await???;
    }
    result?;
    if let Some(path) = created_path {
        ensure!(!path.exists(), "daemon shutdown left its raw device alive");
    }
    idle(&cache).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires RAW_BLOB_TEST_DIR on a DIO-capable filesystem"]
async fn raw_cache_unused_identity_churn_does_not_retain_open_backing_files() -> Result<()> {
    let dir = cache_dir()?;
    let cache = RawCache::new(dir.path(), CAPACITY)?;
    let origin = Origin::start().await?;
    let open_cache_files = || -> Result<usize> {
        let mut count = 0;
        for entry in std::fs::read_dir("/proc/self/fd")? {
            if let Ok(path) = std::fs::read_link(entry?.path()) {
                if path.starts_with(dir.path()) && path != dir.path() {
                    count += 1;
                }
            }
        }
        Ok(count)
    };
    for identity in 0..128 {
        let target = RawBlobTarget::new_http(
            &origin.url,
            origin.data.len() as u64,
            RawReadMode::CopyDirect,
            &format!("unused-identity-{identity}"),
            cache.clone(),
        )?;
        drop(target);
        ensure!(
            open_cache_files()? <= 4,
            "unused identity churn retained backing FDs after identity {identity}"
        );
    }
    ensure!(cache.stats().used_bytes == 0);
    ensure!(origin.requests.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires privileged ublk and RAW_BLOB_TEST_DIR on a DIO-capable filesystem"]
async fn raw_cache_pending_reservation_is_not_evicted_or_allowed_to_block_read() -> Result<()> {
    let dir = cache_dir()?;
    let cache = RawCache::new(dir.path(), CAPACITY)?;
    let control = Arc::new(RawCacheTestControl::default());
    cache.set_test_control(control.clone());
    let origin = Origin::start().await?;
    let first = Device::start(target(&origin, &cache)?, 1).await?;
    let second = Device::start(target(&origin, &cache)?, 1).await?;
    origin.quiet().await;
    idle(&cache).await?;
    let before = cache.stats().write_starts;
    control.set_write_paused(true);
    let result: Result<()> = async {
        tokio::time::timeout(
            Duration::from_secs(2),
            origin.read(&first.path, OFFSET, CAPACITY as usize),
        )
        .await??;
        tokio::time::timeout(
            Duration::from_secs(2),
            control.wait_for_write_starts(before + 1),
        )
        .await?;
        ensure!(cache.stats().reserved_bytes == CAPACITY);
        tokio::time::timeout(
            Duration::from_secs(2),
            origin.read(&second.path, OFFSET + CAPACITY, PAGE),
        )
        .await
        .context("cache admission blocked complete READ data")??;
        // A subsequent request on the same slot also proves that failed
        // admission retained no extra write/cleanup ownership of its T.
        let requests = origin.count(OFFSET, PAGE);
        tokio::time::timeout(
            Duration::from_secs(2),
            origin.read(&second.path, OFFSET, PAGE),
        )
        .await??;
        ensure!(origin.count(OFFSET, PAGE) == requests);
        let stats = cache.stats();
        ensure!(stats.used_bytes == CAPACITY && stats.reserved_bytes == CAPACITY);
        ensure!(
            stats.write_starts == before + 1,
            "active reservation was evicted"
        );
        Ok(())
    }
    .await;
    control.set_write_paused(false);
    first.stop().await?;
    second.stop().await?;
    result?;
    idle(&cache).await?;
    let actual = direct_read(&backing(dir.path())?, OFFSET, CAPACITY as usize).await?;
    ensure!(actual == origin.data[OFFSET as usize..(OFFSET + CAPACITY) as usize]);
    Ok(())
}

struct FrozenFilesystem(File);
impl FrozenFilesystem {
    fn new(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        // This destructive-to-progress ioctl is only enabled by an explicit
        // fixture mount path. Never infer the mount from the host's temp dir.
        ensure!(
            unsafe { libc::ioctl(file.as_raw_fd(), FREEZE_FS, 0) } == 0,
            "freeze owned fixture: {}",
            std::io::Error::last_os_error()
        );
        Ok(Self(file))
    }
    fn thaw(&self) -> Result<()> {
        ensure!(
            unsafe { libc::ioctl(self.0.as_raw_fd(), THAW_FS, 0) } == 0,
            "thaw owned fixture: {}",
            std::io::Error::last_os_error()
        );
        Ok(())
    }
}
impl Drop for FrozenFilesystem {
    fn drop(&mut self) {
        // Also thaw during assertion/panic unwinding so failed diagnostics do
        // not strand io_uring workers or the caller's private mount namespace.
        unsafe { libc::ioctl(self.0.as_raw_fd(), THAW_FS, 0) };
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires privileged ublk and RAW_CACHE_FREEZE_MOUNT naming an owned ext4 fixture"]
async fn raw_cache_frozen_layer_creation_does_not_block_existing_cache_hit() -> Result<()> {
    let mount = PathBuf::from(
        std::env::var_os("RAW_CACHE_FREEZE_MOUNT")
            .context("set RAW_CACHE_FREEZE_MOUNT to an owned disposable ext4 mount")?,
    );
    let base = PathBuf::from(
        std::env::var_os("RAW_BLOB_TEST_DIR").context("set RAW_BLOB_TEST_DIR explicitly")?,
    );
    ensure!(mount.is_absolute() && std::fs::canonicalize(&mount)? == std::fs::canonicalize(base)?);
    let dir = cache_dir()?;
    let cache = RawCache::new(dir.path(), CAPACITY)?;
    let origin = Origin::start().await?;
    let device = Device::start(target(&origin, &cache)?, 1).await?;
    origin.quiet().await;
    origin.read(&device.path, OFFSET, PAGE).await?;
    idle(&cache).await?;
    let requests = origin.count(OFFSET, PAGE);
    let frozen = FrozenFilesystem::new(&mount)?;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (url, size, other_cache) = (origin.url.clone(), origin.data.len() as u64, cache.clone());
    let creator = tokio::task::spawn_blocking(move || {
        let tid = unsafe { libc::syscall(libc::SYS_gettid) };
        let _ = started_tx.send(tid);
        RawBlobTarget::new_http(
            &url,
            size,
            RawReadMode::CopyDirect,
            "blocked-metadata-identity",
            other_cache,
        )
    });
    let result: Result<()> = async {
        let tid = started_rx.await?;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let wait = std::fs::read_to_string(format!("/proc/self/task/{tid}/wchan"))?;
                if wait.contains("percpu_rwsem_wait") || wait.contains("sb_start_write") {
                    eprintln!("layer creation is blocked in the owned frozen filesystem: tid={tid} wchan={wait}");
                    break Ok::<(), anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("fixture did not observe layer creation entering the frozen filesystem")??;
        tokio::time::timeout(Duration::from_secs(2), origin.read(&device.path, OFFSET, PAGE))
            .await
            .context("unrelated metadata I/O blocked an existing cache READ")??;
        ensure!(origin.count(OFFSET, PAGE) == requests, "cached READ refetched HTTP data");
        eprintln!("existing cached READ completed before frozen layer creation resumed");
        Ok(())
    }.await;
    frozen.thaw()?;
    drop(tokio::time::timeout(Duration::from_secs(5), creator).await???);
    device.stop().await?;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires privileged ublk and RAW_CACHE_FREEZE_MOUNT naming an owned ext4 fixture"]
async fn raw_cache_real_write_deadline_and_stop_drain_frozen_filesystem() -> Result<()> {
    let mount = PathBuf::from(
        std::env::var_os("RAW_CACHE_FREEZE_MOUNT")
            .context("set RAW_CACHE_FREEZE_MOUNT to an owned disposable ext4 mount")?,
    );
    let base = PathBuf::from(
        std::env::var_os("RAW_BLOB_TEST_DIR").context("set RAW_BLOB_TEST_DIR explicitly")?,
    );
    ensure!(mount.is_absolute() && std::fs::canonicalize(&mount)? == std::fs::canonicalize(base)?);
    let dir = cache_dir()?;
    let cache = RawCache::new(dir.path(), CAPACITY)?;
    let origin = Origin::start().await?;
    let device = Device::start(target(&origin, &cache)?, 1).await?;
    origin.quiet().await;
    idle(&cache).await?;
    let before = cache.stats();
    ensure!(
        before.used_bytes + PAGE as u64 <= CAPACITY,
        "freeze fixture requires admission without eviction"
    );
    ensure!(
        origin.count(OFFSET, PAGE) == 0,
        "freeze fixture range was already fetched"
    );
    eprintln!("before filesystem freeze: {before:?}");
    let frozen = FrozenFilesystem::new(&mount)?;
    // There is no test write gate: the actual io_uring write encounters the
    // frozen filesystem. The normal copy path must still complete this READ.
    let result: Result<()> = async {
        tokio::time::timeout(
            Duration::from_secs(2),
            origin.read(&device.path, OFFSET, PAGE),
        )
        .await
        .context("READ waited for a real outstanding cache write")??;
        tokio::time::timeout(Duration::from_secs(2), async {
            while cache.stats().write_starts == before.write_starts {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        ensure!(cache.stats().write_completions == before.write_completions);
        eprintln!(
            "READ completed while real cache write pending: {:?}",
            cache.stats()
        );
        // Cross the independently submitted cache write's 30-second deadline.
        // A cancellation request alone cannot release the buffer or resources.
        tokio::time::sleep(Duration::from_secs(31)).await;
        ensure!(
            cache.stats().reserved_bytes >= PAGE as u64,
            "timed-out write released its reservation before real I/O/cleanup drained"
        );
        let waits: Vec<_> = std::fs::read_dir("/proc/self/task")?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| std::fs::read_to_string(entry.path().join("wchan")).ok())
            .collect();
        eprintln!(
            "past 30s write deadline: {:?}; kernel wait channels={waits:?}",
            cache.stats()
        );
        Ok(())
    }
    .await;
    let mut stopping = tokio::spawn(device.stop());
    let early = tokio::time::timeout(Duration::from_millis(150), &mut stopping)
        .await
        .ok();
    eprintln!("STOP completed before thaw: {}", early.is_some());
    frozen.thaw()?;
    let stopped_early = early.is_some();
    match early {
        Some(outcome) => outcome??,
        None => tokio::time::timeout(Duration::from_secs(10), stopping).await???,
    }
    result?;
    ensure!(
        !stopped_early,
        "STOP abandoned outstanding frozen cache I/O"
    );
    idle(&cache).await?;
    eprintln!("cache I/O and STOP drained after thaw: {:?}", cache.stats());
    let requests = origin.count(OFFSET, PAGE);
    let retry = Device::start(target(&origin, &cache)?, 1).await?;
    let result = origin.read(&retry.path, OFFSET, PAGE).await;
    retry.stop().await?;
    result?;
    ensure!(
        origin.count(OFFSET, PAGE) == requests + 1,
        "timed-out write incorrectly published a ready cache range"
    );
    Ok(())
}
