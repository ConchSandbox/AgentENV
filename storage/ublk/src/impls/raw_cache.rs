//! Daemon-owned direct-I/O range cache. Availability, allocation accounting,
//! and immutable slot leases have deliberately separate lifetimes.
use anyhow::{bail, Context, Result};
use io_uring::{opcode, types};
use std::collections::{BTreeSet, HashMap};
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use storage_util::io_ring::AsyncIoRing;
use tokio::sync::watch;
use tokio::time::{Duration, Instant};

use super::raw_blob_target::{validate_file_dio_alignment, validate_size, RAW_BLOB_MAX_IO_BYTES};
use crate::io_buffer::{FrozenBuffer, UserBuffer, WeakFrozenBuffer};
const PAGE: u64 = 4096;
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
type PageKey = (u64, u64);
type LayerKey = (String, u64);

pub struct RawCache {
    // The locked directory descriptor must outlive all files and operations.
    _directory: File,
    path: PathBuf,
    capacity: u64,
    state: Mutex<State>,
    // Only activations serialize here; READ and cache fill never acquire it.
    layer_creation: Mutex<()>,
    write_starts: AtomicU64,
    write_completions: AtomicU64,
    pending_fills: AtomicU64,
    #[cfg(feature = "test-support")]
    control: std::sync::OnceLock<Arc<RawCacheTestControl>>,
}

#[derive(Default)]
struct State {
    layers: HashMap<LayerKey, u64>,
    layer_records: HashMap<u64, LayerRecord>,
    pages: HashMap<PageKey, PageRecord>,
    evictable: BTreeSet<(u64, PageKey)>,
    used: u64,
    clock: u64,
    next_id: u64,
}

struct LayerRecord {
    key: LayerKey,
    path: PathBuf,
    active: Weak<CacheLayer>,
    file: Weak<File>,
    pages: usize,
}

pub(crate) struct CacheLayer {
    id: u64,
    file: OnceLock<Arc<File>>,
    // The directory lock outlives every active layer and its file.
    cache: Arc<RawCache>,
}

impl CacheLayer {
    fn file(&self) -> &Arc<File> {
        self.file.get().expect("layer initialization is complete")
    }
}

impl Drop for CacheLayer {
    fn drop(&mut self) {
        let path = self.cache.state.lock().unwrap().remove_empty_layer(self.id);
        if let Some(path) = path {
            remove_empty_file(path);
        }
    }
}

struct PageRecord {
    id: u64,
    phase: Phase,
    charged: bool,
    writing: bool,
    readers: usize,
    touched: u64,
    // A completed disk range is not evicted while its original T is borrowed.
    retained: Option<WeakFrozenBuffer>,
    changed: watch::Sender<()>,
}

enum Phase {
    Fetching,
    Ready {
        buffer: WeakFrozenBuffer,
        offset: usize,
    },
    Disk,
    Residual,
    Evicting,
}

impl State {
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    fn update_page(&mut self, key: PageKey, update: impl FnOnce(&mut PageRecord)) {
        let page = self.pages.get_mut(&key).unwrap();
        self.evictable.remove(&(page.touched, key));
        update(page);
        if page.charged
            && !page.writing
            && page.readers == 0
            && page
                .retained
                .as_ref()
                .is_none_or(WeakFrozenBuffer::is_expired)
            && matches!(page.phase, Phase::Disk | Phase::Residual)
        {
            self.evictable.insert((page.touched, key));
        }
    }

    fn remove_page(&mut self, key: PageKey) -> PageRecord {
        let page = self.pages.remove(&key).unwrap();
        self.evictable.remove(&(page.touched, key));
        self.layer_records.get_mut(&key.0).unwrap().pages -= 1;
        page
    }

    fn remove_empty_layer(&mut self, id: u64) -> Option<PathBuf> {
        let layer = self.layer_records.get(&id)?;
        if layer.pages != 0 || layer.active.strong_count() != 0 {
            return None;
        }
        let layer = self.layer_records.remove(&id).unwrap();
        self.layers.remove(&layer.key);
        Some(layer.path)
    }

    fn expire_ready(&mut self, key: PageKey, id: u64) -> Option<PathBuf> {
        let page = self.pages.get(&key)?;
        if page.id != id {
            return None;
        }
        if matches!(page.phase, Phase::Disk | Phase::Residual)
            && page
                .retained
                .as_ref()
                .is_some_and(WeakFrozenBuffer::is_expired)
        {
            self.update_page(key, |page| page.retained = None);
            return None;
        }
        if !matches!(&page.phase, Phase::Ready { buffer, .. } if buffer.is_expired()) {
            return None;
        }
        debug_assert!(!page.writing);
        if page.charged {
            self.update_page(key, |page| {
                page.phase = Phase::Residual;
                page.retained = None;
                page.changed.send_replace(());
            });
            None
        } else {
            self.remove_page(key).changed.send_replace(());
            self.remove_empty_layer(key.0)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RawCacheStats {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub reserved_bytes: u64,
    pub residual_bytes: u64,
    pub ready_bytes: u64,
    pub write_starts: u64,
    pub write_completions: u64,
    /// Background fills, including admission, eviction, and post-I/O cleanup.
    pub pending_fills: u64,
}

impl RawCache {
    pub fn new(path: &Path, capacity: u64) -> Result<Arc<Self>> {
        if !path.is_absolute()
            || capacity < u64::from(RAW_BLOB_MAX_IO_BYTES)
            || !capacity.is_multiple_of(PAGE)
        {
            bail!(
                "raw cache requires an absolute directory and capacity >= 256 KiB aligned to 4096"
            );
        }
        std::fs::create_dir_all(path).context("create raw cache directory")?;
        let directory = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
            .open(path)
            .context("open raw cache directory")?;
        if unsafe { libc::flock(directory.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(std::io::Error::last_os_error())
                .context("lock exclusive raw cache directory");
        }
        // No restart reuse: only our own files can exist in this dedicated directory.
        for entry in std::fs::read_dir(path).context("list raw cache directory")? {
            let entry = entry?;
            if !entry
                .file_name()
                .to_string_lossy()
                .starts_with("raw-cache-")
                || !entry.file_type()?.is_file()
            {
                bail!("raw cache directory contains an unexpected entry");
            }
            std::fs::remove_file(entry.path()).context("remove stale raw cache file")?;
        }
        let probe_path = path.join("raw-cache-probe");
        let probe = create_file(&probe_path, PAGE)?;
        drop(probe);
        std::fs::remove_file(probe_path).context("remove raw cache probe")?;
        Ok(Arc::new(Self {
            _directory: directory,
            path: path.to_path_buf(),
            capacity,
            state: Mutex::new(State::default()),
            layer_creation: Mutex::new(()),
            write_starts: AtomicU64::new(0),
            write_completions: AtomicU64::new(0),
            pending_fills: AtomicU64::new(0),
            #[cfg(feature = "test-support")]
            control: std::sync::OnceLock::new(),
        }))
    }

    pub fn stats(&self) -> RawCacheStats {
        let state = self.state.lock().unwrap();
        let mut stats = RawCacheStats {
            capacity_bytes: self.capacity,
            used_bytes: state.used,
            reserved_bytes: 0,
            residual_bytes: 0,
            ready_bytes: 0,
            write_starts: self.write_starts.load(Ordering::Relaxed),
            write_completions: self.write_completions.load(Ordering::Relaxed),
            pending_fills: self.pending_fills.load(Ordering::Acquire),
        };
        for page in state.pages.values().filter(|p| p.charged) {
            if page.writing {
                stats.reserved_bytes += PAGE;
            } else if matches!(page.phase, Phase::Disk) {
                stats.ready_bytes += PAGE;
            } else {
                stats.residual_bytes += PAGE;
            }
        }
        stats
    }

    pub(crate) fn layer(self: &Arc<Self>, digest: &str, size: u64) -> Result<Arc<CacheLayer>> {
        validate_size(size)?;
        if digest.is_empty() {
            bail!("raw cache identity cannot be empty");
        }
        // Do not hold the shared READ state lock across any filesystem work.
        // This narrow lock also prevents another activation from observing the
        // incomplete handle while its file is opened outside the state lock.
        let _creating = self.layer_creation.lock().unwrap();
        let key = (digest.to_owned(), size);
        let (layer, path, existing_file, create) = {
            let mut state = self.state.lock().unwrap();
            if let Some(id) = state.layers.get(&key).copied() {
                let record = state.layer_records.get_mut(&id).unwrap();
                if let Some(active) = record.active.upgrade() {
                    return Ok(active);
                }
                let layer = Arc::new(CacheLayer {
                    id,
                    file: OnceLock::new(),
                    cache: self.clone(),
                });
                // The strong handle keeps even an empty identity alive while
                // reopening races with eviction of its final cached page.
                record.active = Arc::downgrade(&layer);
                (layer, record.path.clone(), record.file.upgrade(), false)
            } else {
                state.next_id += 1;
                let id = state.next_id;
                // Content identity never participates in a filesystem path.
                let path = self.path.join(format!("raw-cache-{id}"));
                let layer = Arc::new(CacheLayer {
                    id,
                    file: OnceLock::new(),
                    cache: self.clone(),
                });
                state.layers.insert(key.clone(), id);
                state.layer_records.insert(
                    id,
                    LayerRecord {
                        key,
                        path: path.clone(),
                        active: Arc::downgrade(&layer),
                        file: Weak::new(),
                        pages: 0,
                    },
                );
                (layer, path, None, true)
            }
        };
        let file = match existing_file {
            Some(file) => file,
            None if create => Arc::new(create_file(&path, size)?),
            None => Arc::new(open_file(&path)?),
        };
        layer.file.set(file).unwrap();
        self.state
            .lock()
            .unwrap()
            .layer_records
            .get_mut(&layer.id)
            .unwrap()
            .file = Arc::downgrade(layer.file());
        Ok(layer)
    }

    pub(crate) fn next(
        self: &Arc<Self>,
        layer: &Arc<CacheLayer>,
        offset: u64,
        len: usize,
    ) -> ReadStep {
        let mut state = self.state.lock().unwrap();
        let touched = state.tick();
        let key = (layer.id, offset / PAGE);
        if let Some(page) = state.pages.get(&key) {
            let id = page.id;
            state.expire_ready(key, id);
        }
        if let Some(page) = state.pages.get(&key) {
            match &page.phase {
                Phase::Disk => {
                    let mut count = 0;
                    while count < len / PAGE as usize {
                        let key = (layer.id, offset / PAGE + count as u64);
                        let Some(page) = state.pages.get(&key) else {
                            break;
                        };
                        if !matches!(page.phase, Phase::Disk) {
                            break;
                        }
                        state.update_page(key, |page| {
                            page.readers += 1;
                            page.touched = touched;
                        });
                        count += 1;
                    }
                    return ReadStep::Disk(DiskBorrow {
                        cache: self.clone(),
                        layer: layer.clone(),
                        offset,
                        len: count * PAGE as usize,
                    });
                }
                Phase::Ready { buffer, offset } => {
                    if let Some(buffer) = buffer.upgrade() {
                        let offset = *offset;
                        state.update_page(key, |page| page.touched = touched);
                        return ReadStep::Ready(buffer, offset);
                    }
                }
                Phase::Fetching | Phase::Evicting => {
                    return ReadStep::Wait(page.changed.subscribe())
                }
                Phase::Residual => {}
            }
        }
        // The last lease may disappear between checking and upgrading. Its
        // destructor waits for this lock, so retire only this expired range.
        if let Some(page) = state.pages.get(&key) {
            let id = page.id;
            state.expire_ready(key, id);
        }
        // Claim only the next uninterrupted missing run. Every request progresses
        // in increasing offset order, so overlapping fills cannot wait in a cycle.
        let id = state.tick();
        let mut count = 0;
        while count < len / PAGE as usize {
            let key = (layer.id, offset / PAGE + count as u64);
            if let Some(page) = state.pages.get(&key) {
                let id = page.id;
                state.expire_ready(key, id);
            }
            let charged = match state.pages.get(&key) {
                Some(page) if matches!(page.phase, Phase::Residual) && page.readers == 0 => {
                    page.charged
                }
                Some(_) => break,
                None => false,
            };
            let (changed, _) = watch::channel(());
            if let Some(page) = state.pages.get(&key) {
                let touched = page.touched;
                state.evictable.remove(&(touched, key));
            } else {
                state.layer_records.get_mut(&layer.id).unwrap().pages += 1;
            }
            state.pages.insert(
                key,
                PageRecord {
                    id,
                    phase: Phase::Fetching,
                    charged,
                    writing: false,
                    readers: 0,
                    touched,
                    retained: None,
                    changed,
                },
            );
            count += 1;
        }
        if count == 0 {
            // An invalidated disk range can still have older readers draining.
            return ReadStep::Wait(state.pages.get(&key).unwrap().changed.subscribe());
        }
        ReadStep::Missing(FillClaim {
            cache: self.clone(),
            layer: layer.clone(),
            id,
            offset,
            len: count * PAGE as usize,
            buffer_offset: 0,
            published: false,
        })
    }

    pub(crate) fn publish(
        self: &Arc<Self>,
        mut claims: Vec<FillClaim>,
        user: &mut UserBuffer,
        ring: AsyncIoRing,
    ) {
        if claims.is_empty() {
            return;
        }
        let released: Vec<_> = claims
            .iter()
            .flat_map(|claim| claim.keys().map(|key| (key, claim.id)))
            .collect();
        let cache = Arc::downgrade(self);
        let buffer = user.freeze(move || {
            if let Some(cache) = cache.upgrade() {
                let paths: Vec<_> = {
                    let mut state = cache.state.lock().unwrap();
                    released
                        .into_iter()
                        .filter_map(|(key, id)| state.expire_ready(key, id))
                        .collect()
                };
                for path in paths {
                    remove_empty_file(path);
                }
            }
        });
        {
            let mut state = self.state.lock().unwrap();
            for claim in &mut claims {
                for (index, key) in claim.keys().enumerate() {
                    let page = state.pages.get_mut(&key).unwrap();
                    assert_eq!(page.id, claim.id);
                    page.retained = Some(buffer.downgrade());
                    page.phase = Phase::Ready {
                        buffer: buffer.downgrade(),
                        offset: claim.buffer_offset + index * PAGE as usize,
                    };
                    page.changed.send_replace(());
                }
                claim.published = true;
            }
        }
        self.pending_fills.fetch_add(1, Ordering::Release);
        let cache = self.clone();
        // A slot's wait_reusable keeps this LocalSet and ring alive until this
        // task has processed actual CQEs and released its final buffer lease.
        tokio::task::spawn_local(async move {
            for claim in claims {
                cache.fill(&claim, &buffer, &ring).await;
            }
            drop(buffer);
            cache.pending_fills.fetch_sub(1, Ordering::Release);
        });
    }

    async fn reserve(&self, claim: &FillClaim) -> bool {
        // A failed punch moves to the tail. Limit this admission attempt to the
        // initial candidate count instead of repeatedly scanning failed ranges.
        let mut candidates = self.state.lock().unwrap().evictable.len();
        loop {
            let victim = {
                let mut state = self.state.lock().unwrap();
                let needed = claim
                    .keys()
                    .filter(|key| !state.pages.get(key).unwrap().charged)
                    .count() as u64
                    * PAGE;
                if needed <= self.capacity - state.used {
                    state.used += needed;
                    for key in claim.keys() {
                        state.update_page(key, |page| {
                            page.charged = true;
                            page.writing = true;
                        });
                    }
                    return true;
                }
                if candidates == 0 {
                    return false;
                }
                let Some((_, key)) = state.evictable.first().copied() else {
                    return false;
                };
                candidates -= 1;
                state.update_page(key, |page| page.phase = Phase::Evicting);
                let layer = state.layer_records.get(&key.0).unwrap();
                (key, layer.file.upgrade(), layer.path.clone())
            };
            // Idle layers have no retained FD; reopening and punching are both
            // metadata work outside the state lock and READ completion path.
            let freed = self
                .punch_path(victim.1, victim.2, victim.0 .1 * PAGE, PAGE)
                .await;
            let empty_path = {
                let mut state = self.state.lock().unwrap();
                if freed {
                    let page = state.remove_page(victim.0);
                    page.changed.send_replace(());
                    state.used -= PAGE;
                    state.remove_empty_layer(victim.0 .0)
                } else {
                    let touched = state.tick();
                    state.update_page(victim.0, |page| {
                        page.phase = Phase::Residual;
                        page.touched = touched;
                        page.changed.send_replace(());
                    });
                    None
                }
            };
            if let Some(path) = empty_path {
                let _ = tokio::task::spawn_blocking(move || remove_empty_file(path)).await;
            }
        }
    }

    async fn fill(&self, claim: &FillClaim, buffer: &FrozenBuffer, ring: &AsyncIoRing) {
        if !self.reserve(claim).await {
            tracing::debug!(
                offset = claim.offset,
                len = claim.len,
                "raw cache admission skipped"
            );
            return;
        }
        self.write_starts.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "test-support")]
        if let Some(control) = self.control.get() {
            control.before_write().await;
        }
        let bytes = buffer.subslice(claim.buffer_offset, claim.len);
        let len = bytes.len();
        #[cfg(feature = "test-support")]
        let len = if self
            .control
            .get()
            .is_some_and(|c| c.short_write.load(Ordering::Acquire))
        {
            len.saturating_sub(PAGE as usize)
        } else {
            len
        };
        let failed = self.test_write_failure();
        let result = if failed {
            -libc::EIO
        } else if len == 0 {
            0
        } else {
            let entry = opcode::Write::new(
                types::Fd(claim.layer.file().as_raw_fd()),
                bytes.as_ptr(),
                len as u32,
            )
            .offset(claim.offset)
            .build();
            io_with_deadline(ring, entry, Instant::now() + WRITE_TIMEOUT).await
        };
        let success = result == claim.len as i32;
        let freed = if success {
            false
        } else {
            tracing::warn!(
                offset = claim.offset,
                len = claim.len,
                result,
                "raw cache write failed; READ remains valid"
            );
            self.punch(claim.layer.file().clone(), claim.offset, claim.len as u64)
                .await
        };
        let mut state = self.state.lock().unwrap();
        for key in claim.keys() {
            state.update_page(key, |page| {
                page.writing = false;
                if success {
                    page.phase = Phase::Disk;
                }
                if freed {
                    page.charged = false;
                }
                page.changed.send_replace(());
            });
        }
        if freed {
            state.used -= claim.len as u64;
        }
        self.write_completions.fetch_add(1, Ordering::Release);
    }

    async fn punch(&self, file: Arc<File>, offset: u64, len: u64) -> bool {
        self.punch_path(Some(file), PathBuf::new(), offset, len)
            .await
    }

    async fn punch_path(
        &self,
        file: Option<Arc<File>>,
        path: PathBuf,
        offset: u64,
        len: u64,
    ) -> bool {
        #[cfg(feature = "test-support")]
        if self
            .control
            .get()
            .is_some_and(|c| c.cleanup_error.load(Ordering::Acquire))
        {
            return false;
        }
        // This metadata operation owns its FD until the real syscall returns.
        // It is never part of the READ completion path.
        tokio::task::spawn_blocking(move || {
            let file = match file {
                Some(file) => file,
                None => match open_file(&path) {
                    Ok(file) => Arc::new(file),
                    Err(error) => {
                        tracing::warn!(%error, "reopen raw cache file for eviction failed");
                        return false;
                    }
                },
            };
            unsafe {
                libc::fallocate(
                    file.as_raw_fd(),
                    libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                    offset as libc::off_t,
                    len as libc::off_t,
                ) == 0
            }
        })
        .await
        .unwrap_or(false)
    }

    fn test_write_failure(&self) -> bool {
        #[cfg(feature = "test-support")]
        if self
            .control
            .get()
            .is_some_and(|c| c.write_error.load(Ordering::Acquire))
        {
            return true;
        }
        false
    }

    #[cfg(feature = "test-support")]
    pub fn set_test_control(&self, control: Arc<RawCacheTestControl>) {
        assert!(
            self.control.set(control).is_ok(),
            "test control already configured"
        );
    }
}

// The caller has removed the empty identity while holding the state lock,
// then releases that lock before any potentially blocking filesystem work.
fn remove_empty_file(path: PathBuf) {
    // No charged payload remains. Failed metadata cleanup must not pin an FD
    // or identity indefinitely; startup retries removal of the empty file.
    if let Err(error) = std::fs::remove_file(path) {
        if error.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!(%error, "remove empty raw cache file failed");
        }
    }
}

fn open_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT | libc::O_NOFOLLOW)
        .open(path)
        .context("reopen direct raw cache file")?;
    validate_file_dio_alignment(&file)?;
    Ok(file)
}

fn create_file(path: &Path, size: u64) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_DIRECT | libc::O_NOFOLLOW)
        .open(path)
        .context("create direct raw cache file")?;
    validate_file_dio_alignment(&file)?;
    file.set_len(size).context("size sparse raw cache file")?;
    Ok(file)
}

pub(crate) enum ReadStep {
    Disk(DiskBorrow),
    Ready(FrozenBuffer, usize),
    Wait(watch::Receiver<()>),
    Missing(FillClaim),
}

pub(crate) struct DiskBorrow {
    cache: Arc<RawCache>,
    layer: Arc<CacheLayer>,
    offset: u64,
    pub(crate) len: usize,
}
impl DiskBorrow {
    pub(crate) async fn read(
        &self,
        dst: &mut [u8],
        ring: &AsyncIoRing,
        deadline: Instant,
    ) -> Result<()> {
        let mut result = -libc::EIO;
        #[cfg(feature = "test-support")]
        let fail = self
            .cache
            .control
            .get()
            .is_some_and(|c| c.read_error.load(Ordering::Acquire));
        #[cfg(not(feature = "test-support"))]
        let fail = false;
        if !fail {
            let entry = opcode::Read::new(
                types::Fd(self.layer.file().as_raw_fd()),
                dst.as_mut_ptr(),
                self.len as u32,
            )
            .offset(self.offset)
            .build();
            result = io_with_deadline(ring, entry, deadline).await;
        }
        if result != self.len as i32 {
            let mut state = self.cache.state.lock().unwrap();
            for page in self.offset / PAGE..(self.offset + self.len as u64) / PAGE {
                state.update_page((self.layer.id, page), |record| {
                    record.phase = Phase::Residual;
                    record.changed.send_replace(());
                });
            }
            bail!("raw cache read failed ({result})");
        }
        Ok(())
    }
}
impl Drop for DiskBorrow {
    fn drop(&mut self) {
        let mut state = self.cache.state.lock().unwrap();
        for page in self.offset / PAGE..(self.offset + self.len as u64) / PAGE {
            state.update_page((self.layer.id, page), |record| {
                record.readers -= 1;
                record.changed.send_replace(());
            });
        }
    }
}

pub(crate) struct FillClaim {
    cache: Arc<RawCache>,
    layer: Arc<CacheLayer>,
    id: u64,
    pub(crate) offset: u64,
    pub(crate) len: usize,
    pub(crate) buffer_offset: usize,
    published: bool,
}
impl FillClaim {
    fn keys(&self) -> impl Iterator<Item = PageKey> {
        let layer = self.layer.id;
        (self.offset / PAGE..(self.offset + self.len as u64) / PAGE).map(move |page| (layer, page))
    }
}
impl Drop for FillClaim {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        let mut state = self.cache.state.lock().unwrap();
        for key in self.keys() {
            if let Some(page) = state.pages.get_mut(&key) {
                assert_eq!(page.id, self.id);
                if page.charged {
                    state.update_page(key, |page| {
                        page.phase = Phase::Residual;
                        page.changed.send_replace(());
                    });
                } else {
                    state.remove_page(key).changed.send_replace(());
                }
            }
        }
    }
}

/// Cancel at the business deadline, then drain both CQEs before returning.
/// Dropping RingFuture alone only discards a result; it does not cancel I/O.
pub(crate) async fn io_with_deadline(
    ring: &AsyncIoRing,
    entry: io_uring::squeue::Entry,
    deadline: Instant,
) -> i32 {
    if Instant::now() >= deadline {
        return -libc::ETIMEDOUT;
    }
    let mut completion = match ring.send(entry) {
        Ok(completion) => completion,
        Err(error) => return -error.raw_os_error().unwrap_or(libc::EIO),
    };
    tokio::select! {
        biased;
        result = &mut completion => result,
        _ = tokio::time::sleep_until(deadline) => {
            if let Ok(cancel) = ring.send(opcode::AsyncCancel::new(completion.as_user_data()).build()) {
                cancel.await;
            }
            completion.await;
            -libc::ETIMEDOUT
        }
    }
}

#[cfg(feature = "test-support")]
pub struct RawCacheTestControl {
    paused: watch::Sender<bool>,
    starts: watch::Sender<u64>,
    write_error: std::sync::atomic::AtomicBool,
    short_write: std::sync::atomic::AtomicBool,
    cleanup_error: std::sync::atomic::AtomicBool,
    read_error: std::sync::atomic::AtomicBool,
}
#[cfg(feature = "test-support")]
impl Default for RawCacheTestControl {
    fn default() -> Self {
        Self {
            paused: watch::channel(false).0,
            starts: watch::channel(0).0,
            write_error: false.into(),
            short_write: false.into(),
            cleanup_error: false.into(),
            read_error: false.into(),
        }
    }
}
#[cfg(feature = "test-support")]
impl RawCacheTestControl {
    pub fn set_write_paused(&self, value: bool) {
        self.paused.send_replace(value);
    }
    pub fn fail_writes(&self, value: bool) {
        self.write_error.store(value, Ordering::Release);
    }
    pub fn short_writes(&self, value: bool) {
        self.short_write.store(value, Ordering::Release);
    }
    pub fn fail_cleanup(&self, value: bool) {
        self.cleanup_error.store(value, Ordering::Release);
    }
    pub fn fail_reads(&self, value: bool) {
        self.read_error.store(value, Ordering::Release);
    }
    pub async fn wait_for_write_starts(&self, count: u64) {
        let mut starts = self.starts.subscribe();
        while *starts.borrow_and_update() < count {
            starts.changed().await.unwrap();
        }
    }
    async fn before_write(&self) {
        self.starts.send_modify(|count| *count += 1);
        let mut paused = self.paused.subscribe();
        while *paused.borrow_and_update() {
            paused.changed().await.unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // State tests use an unlinked sparse file: actual DIO and ublk behavior is
    // exercised by the privileged daemon integration suite on ext4.
    fn cache() -> (Arc<RawCache>, Arc<CacheLayer>) {
        let file = tempfile::tempfile().unwrap();
        file.set_len(PAGE * 128).unwrap();
        let cache = Arc::new(RawCache {
            _directory: File::open("/").unwrap(),
            path: PathBuf::from("/"),
            capacity: PAGE * 64,
            state: Mutex::new(State::default()),
            layer_creation: Mutex::new(()),
            write_starts: AtomicU64::new(0),
            write_completions: AtomicU64::new(0),
            pending_fills: AtomicU64::new(0),
            #[cfg(feature = "test-support")]
            control: std::sync::OnceLock::new(),
        });
        let layer = Arc::new(CacheLayer {
            id: 1,
            file: OnceLock::from(Arc::new(file)),
            cache: cache.clone(),
        });
        let key = ("opaque/identity".into(), PAGE * 128);
        {
            let mut state = cache.state.lock().unwrap();
            state.layers.insert(key.clone(), layer.id);
            state.layer_records.insert(
                layer.id,
                LayerRecord {
                    key,
                    path: PathBuf::from("/proc/nonexistent-raw-cache-test"),
                    active: Arc::downgrade(&layer),
                    file: Arc::downgrade(layer.file()),
                    pages: 0,
                },
            );
        }
        (cache, layer)
    }

    #[tokio::test]
    async fn overlap_has_one_owner_and_abandoned_download_wakes_waiters() {
        let (cache, layer) = cache();
        let ReadStep::Missing(claim) = cache.next(&layer, 0, 8192) else {
            panic!("sparse holes are not hits")
        };
        assert_eq!(claim.len, 8192);
        let ReadStep::Wait(mut waiter) = cache.next(&layer, 4096, 8192) else {
            panic!("overlap must coalesce")
        };
        assert_eq!(cache.stats().used_bytes, 0);
        drop(claim);
        waiter.changed().await.unwrap();
        assert!(matches!(
            cache.next(&layer, 4096, 8192),
            ReadStep::Missing(_)
        ));
    }

    #[tokio::test]
    async fn invalidation_preserves_charge_until_storage_is_actually_freed() {
        let (cache, layer) = cache();
        let ReadStep::Missing(claim) = cache.next(&layer, 0, 4096) else {
            panic!()
        };
        {
            let mut state = cache.state.lock().unwrap();
            state.used = PAGE;
            state.pages.get_mut(&(layer.id, 0)).unwrap().charged = true;
        }
        drop(claim);
        assert_eq!(cache.stats().residual_bytes, PAGE);
        let ReadStep::Missing(refill) = cache.next(&layer, 0, 4096) else {
            panic!()
        };
        assert!(cache.reserve(&refill).await);
        assert_eq!(
            cache.stats().used_bytes,
            PAGE,
            "refill must not charge the same bytes twice"
        );
        assert_eq!(cache.stats().reserved_bytes, PAGE);
    }

    #[tokio::test]
    async fn active_disk_borrow_prevents_eviction_and_refill_after_failure() {
        let (cache, layer) = cache();
        let ReadStep::Missing(mut claim) = cache.next(&layer, 0, 4096) else {
            panic!()
        };
        claim.published = true;
        {
            let mut state = cache.state.lock().unwrap();
            state.used = PAGE;
            state.update_page((layer.id, 0), |page| {
                page.charged = true;
                page.phase = Phase::Disk;
            });
        }
        let ReadStep::Disk(reader) = cache.next(&layer, 0, 4096) else {
            panic!()
        };
        cache
            .state
            .lock()
            .unwrap()
            .update_page((layer.id, 0), |page| {
                page.phase = Phase::Residual;
            });
        let ReadStep::Wait(mut changed) = cache.next(&layer, 0, 4096) else {
            panic!("old readers must drain")
        };
        drop(reader);
        changed.changed().await.unwrap();
        assert!(matches!(cache.next(&layer, 0, 4096), ReadStep::Missing(_)));
        assert_eq!(cache.stats().used_bytes, PAGE);
    }

    #[test]
    fn indexed_lru_tracks_only_unborrowed_pages_and_updates_on_hits() {
        let (cache, layer) = cache();
        let ReadStep::Missing(mut claim) = cache.next(&layer, 0, 3 * PAGE as usize) else {
            panic!()
        };
        claim.published = true;
        {
            let mut state = cache.state.lock().unwrap();
            state.used = 3 * PAGE;
            for index in 0..3 {
                state.update_page((layer.id, index), |page| {
                    page.charged = true;
                    page.phase = Phase::Disk;
                });
            }
            assert_eq!(state.evictable.len(), 3);
        }
        let ReadStep::Disk(first) = cache.next(&layer, 0, PAGE as usize) else {
            panic!()
        };
        {
            let state = cache.state.lock().unwrap();
            assert_eq!(state.evictable.len(), 2);
            assert_eq!(state.evictable.first().unwrap().1, (layer.id, 1));
        }
        drop(first);
        let ReadStep::Disk(second) = cache.next(&layer, PAGE, PAGE as usize) else {
            panic!()
        };
        drop(second);
        let state = cache.state.lock().unwrap();
        assert_eq!(state.evictable.len(), 3);
        assert_eq!(state.evictable.first().unwrap().1, (layer.id, 2));
    }

    #[test]
    fn final_empty_layer_releases_identity_and_file_but_cached_layer_can_close_fd() {
        let (cache, layer) = cache();
        let file = Arc::downgrade(layer.file());
        let ReadStep::Missing(mut claim) = cache.next(&layer, 0, PAGE as usize) else {
            panic!()
        };
        claim.published = true;
        {
            let mut state = cache.state.lock().unwrap();
            state.used = PAGE;
            state.update_page((layer.id, 0), |page| {
                page.charged = true;
                page.phase = Phase::Disk;
            });
        }
        let id = layer.id;
        drop(claim);
        drop(layer);
        assert!(
            file.upgrade().is_none(),
            "cached payload must not pin an idle FD"
        );
        let mut state = cache.state.lock().unwrap();
        assert_eq!(
            state.layers.len(),
            1,
            "cached identity must survive activation"
        );
        assert_eq!(state.layer_records.len(), 1);
        state.remove_page((id, 0));
        assert!(state.remove_empty_layer(id).is_some());
        state.used -= PAGE;
        assert!(state.layers.is_empty());
        assert!(state.layer_records.is_empty());
        assert!(state.evictable.is_empty());
    }

    #[test]
    fn dropping_an_unused_layer_does_not_leave_a_historical_registry_entry() {
        let (cache, layer) = cache();
        let file = Arc::downgrade(layer.file());
        drop(layer);
        assert!(file.upgrade().is_none());
        let state = cache.state.lock().unwrap();
        assert!(state.layers.is_empty());
        assert!(state.layer_records.is_empty());
    }

    #[test]
    fn configuration_requires_explicit_positive_aligned_capacity() {
        for bytes in [0, 4096, 262143, 262145] {
            assert!(RawCache::new(Path::new("/unused-test-path"), bytes).is_err());
        }
        assert!(RawCache::new(Path::new("relative"), 262144).is_err());
    }
}
