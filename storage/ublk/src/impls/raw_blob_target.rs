use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use io_uring::{opcode, types};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::sync::Arc;
use storage_util::io_ring::AsyncIoRing;

use super::raw_blob_http::HttpRangeSource;
use super::raw_cache::{CacheLayer, RawCache, ReadStep};
use crate::{IOBuffer, UVMUblkTarget, UblkDescOperation};
use tokio::time::{Duration, Instant};

pub const RAW_BLOB_BLOCK_SIZE: u64 = 4096;
pub const RAW_BLOB_MAX_IO_BYTES: u32 = 256 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RawReadMode {
    CopyBuffered,
    CopyDirect,
    ZcBuffered,
    ZcDirect,
}

impl RawReadMode {
    pub fn is_zero_copy(self) -> bool {
        matches!(self, Self::ZcBuffered | Self::ZcDirect)
    }

    pub fn is_direct(self) -> bool {
        matches!(self, Self::CopyDirect | Self::ZcDirect)
    }
}

/// A trusted immutable blob exposed byte-for-byte, including its metadata.
/// The caller owns content identity and completeness; no image format is parsed.
pub struct RawBlobTarget {
    source: RawBlobSource,
    size_bytes: u64,
    read_mode: RawReadMode,
}

enum RawBlobSource {
    Local(File),
    Http {
        source: HttpRangeSource,
        cache: Arc<RawCache>,
        layer: Arc<CacheLayer>,
    },
}

impl RawBlobTarget {
    pub fn new(path: &Path, size_bytes: u64, read_mode: RawReadMode) -> Result<Self> {
        validate_size(size_bytes)?;
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(if read_mode.is_direct() {
                libc::O_DIRECT | libc::O_NONBLOCK
            } else {
                libc::O_NONBLOCK
            })
            .open(path)
            .with_context(|| format!("open raw blob {}", path.display()))?;
        let metadata = file.metadata().context("stat raw blob")?;
        if !metadata.is_file() || metadata.len() != size_bytes {
            bail!("raw blob must be a regular file of exactly {size_bytes} bytes");
        }
        if read_mode.is_direct() {
            validate_file_dio_alignment(&file)?;
        }
        Ok(Self {
            source: RawBlobSource::Local(file),
            size_bytes,
            read_mode,
        })
    }

    /// Construct a remote target without downloading header, index, or payload.
    pub fn new_http(
        url: &str,
        size_bytes: u64,
        read_mode: RawReadMode,
        digest: &str,
        cache: Arc<RawCache>,
    ) -> Result<Self> {
        validate_size(size_bytes)?;
        if read_mode != RawReadMode::CopyDirect {
            bail!("HTTP raw blobs require copy-direct mode");
        }
        let source = HttpRangeSource::new(url, size_bytes)?;
        let layer = cache.layer(digest, size_bytes)?;
        Ok(Self {
            source: RawBlobSource::Http {
                source,
                cache,
                layer,
            },
            size_bytes,
            read_mode,
        })
    }

    async fn read_remote(
        source: &HttpRangeSource,
        cache: &Arc<RawCache>,
        layer: &Arc<CacheLayer>,
        offset: u64,
        len: usize,
        user: &mut crate::UserBuffer,
        ring: &AsyncIoRing,
    ) -> Result<()> {
        // One deadline covers cache waits, every HTTP subrange, and cache reads.
        // It does not cover background writes or waiting for the next FETCH.
        let deadline = user.request_received() + Duration::from_secs(30);
        let mut done = 0;
        let mut claims = Vec::new();
        while done < len {
            if Instant::now() >= deadline {
                bail!("raw blob data supply timed out");
            }
            match cache.next(layer, offset + done as u64, len - done) {
                ReadStep::Disk(borrow) => {
                    borrow
                        .read(user.subslice_mut(done as u64, borrow.len), ring, deadline)
                        .await?;
                    done += borrow.len;
                }
                ReadStep::Ready(buffer, at) => {
                    user.subslice_mut(done as u64, 4096)
                        .copy_from_slice(buffer.subslice(at, 4096));
                    done += 4096;
                }
                ReadStep::Wait(mut changed) => {
                    let _ = tokio::time::timeout_at(deadline, changed.changed())
                        .await
                        .context("wait for shared raw blob data")?;
                }
                ReadStep::Missing(mut claim) => {
                    claim.buffer_offset = done;
                    tokio::time::timeout_at(
                        deadline,
                        source.read(claim.offset, user.subslice_mut(done as u64, claim.len)),
                    )
                    .await
                    .context("raw blob data supply timed out")??;
                    done += claim.len;
                    claims.push(claim);
                }
            }
        }
        if Instant::now() >= deadline {
            bail!("raw blob data supply timed out");
        }
        if !claims.is_empty() {
            cache.publish(claims, user, ring.clone());
        }
        Ok(())
    }

    fn read_range(&self, desc: ublk_sys::ublksrv_io_desc) -> Result<(u64, usize), i32> {
        // UAPI sectors are always 512 bytes, independently of logical_bs_shift.
        let offset = desc.start_sector.checked_mul(512).ok_or(-libc::EINVAL)?;
        let len = u64::from(desc.nr_sectors)
            .checked_mul(512)
            .ok_or(-libc::EINVAL)?;
        let end = offset.checked_add(len).ok_or(-libc::EINVAL)?;
        if len == 0
            || end > self.size_bytes
            || len > u64::from(RAW_BLOB_MAX_IO_BYTES)
            || !offset.is_multiple_of(RAW_BLOB_BLOCK_SIZE)
            || !len.is_multiple_of(RAW_BLOB_BLOCK_SIZE)
        {
            return Err(-libc::EINVAL);
        }
        Ok((offset, len as usize))
    }

    async fn read(&self, offset: u64, len: usize, buf: &mut IOBuffer, ring: &AsyncIoRing) -> i32 {
        if len > buf.len() || self.read_mode.is_zero_copy() != matches!(buf, IOBuffer::AutoReg(_)) {
            return -libc::EINVAL;
        }
        let file = match &self.source {
            RawBlobSource::Local(file) => file,
            RawBlobSource::Http {
                source,
                cache,
                layer,
            } => {
                let IOBuffer::User(user) = buf else {
                    return -libc::EINVAL;
                };
                return match Self::read_remote(source, cache, layer, offset, len, user, ring).await
                {
                    Ok(()) => len as i32,
                    Err(error) => {
                        tracing::warn!(error = %error, offset, len, "raw blob data supply failed");
                        -libc::EIO
                    }
                };
            }
        };
        let ptr = match buf {
            IOBuffer::User(user) => {
                let ptr = user.subslice_mut(0, len).as_mut_ptr();
                if self.read_mode.is_direct()
                    && !(ptr as usize).is_multiple_of(RAW_BLOB_BLOCK_SIZE as usize)
                {
                    return -libc::EINVAL;
                }
                ptr
            }
            // AUTO_BUF_REG addresses the imported request buffer by byte offset.
            IOBuffer::AutoReg(_) => std::ptr::null_mut(),
        };
        let entry = opcode::ReadFixed::new(
            types::Fd(file.as_raw_fd()),
            ptr,
            len as u32,
            buf.uring_buf_idx(),
        )
        .offset(offset)
        .build();
        loop {
            let result = match ring.send(entry.clone()) {
                Ok(completion) => completion.await,
                Err(error) => return -error.raw_os_error().unwrap_or(libc::EIO),
            };
            if result == -libc::EINTR {
                continue;
            }
            // A short read is an error, never an implicit zero-filled success.
            return if result >= 0 && result as usize != len {
                -libc::EIO
            } else {
                result
            };
        }
    }
}

pub fn validate_size(size_bytes: u64) -> Result<()> {
    if size_bytes == 0
        || size_bytes > i64::MAX as u64
        || !size_bytes.is_multiple_of(RAW_BLOB_BLOCK_SIZE)
    {
        bail!("raw blob size must be positive, fit off_t, and be 4096-byte aligned");
    }
    Ok(())
}

fn validate_dio_alignment(mask: u32, memory: u32, offset: u32) -> Result<()> {
    if mask & libc::STATX_DIOALIGN == 0
        || memory == 0
        || offset == 0
        || !RAW_BLOB_BLOCK_SIZE.is_multiple_of(u64::from(memory))
        || !RAW_BLOB_BLOCK_SIZE.is_multiple_of(u64::from(offset))
    {
        bail!("direct I/O requires confirmed memory/offset alignment compatible with 4096 bytes (mask={mask:#x}, memory={memory}, offset={offset})");
    }
    Ok(())
}

pub(crate) fn validate_file_dio_alignment(file: &File) -> Result<()> {
    let mut stat: libc::statx = unsafe { std::mem::zeroed() };
    let result = unsafe {
        libc::statx(
            file.as_raw_fd(),
            c"".as_ptr(),
            libc::AT_EMPTY_PATH,
            libc::STATX_DIOALIGN,
            &mut stat,
        )
    };
    if result < 0 {
        return Err(std::io::Error::last_os_error()).context("query raw blob DIO alignment");
    }
    validate_dio_alignment(
        stat.stx_mask,
        stat.stx_dio_mem_align,
        stat.stx_dio_offset_align,
    )
}

#[async_trait(?Send)]
impl UVMUblkTarget for RawBlobTarget {
    const DEV_NAME: &str = "raw-blob";

    async fn init(&self, _qid: u16, _io_ring: &AsyncIoRing) -> Result<()> {
        Ok(())
    }

    fn per_slot_extra_buf_len(&self) -> Option<usize> {
        None
    }

    fn ublk_dev_params(&self, info: &ublk_sys::ublksrv_ctrl_dev_info) -> ublk_sys::ublk_params {
        ublk_sys::ublk_params {
            len: std::mem::size_of::<ublk_sys::ublk_params>() as u32,
            types: ublk_sys::UBLK_PARAM_TYPE_BASIC,
            basic: ublk_sys::ublk_param_basic {
                attrs: ublk_sys::UBLK_ATTR_READ_ONLY,
                logical_bs_shift: 12,
                physical_bs_shift: 12,
                io_min_shift: 12,
                io_opt_shift: 12,
                max_sectors: info.max_io_buf_bytes.min(RAW_BLOB_MAX_IO_BYTES) / 512,
                dev_sectors: self.size_bytes / 512,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    async fn handle_io_request(
        self: &Arc<Self>,
        _qid: u16,
        _tag: u16,
        desc: ublk_sys::ublksrv_io_desc,
        buf: &mut IOBuffer,
        _extra: Option<&mut IOBuffer>,
        ring: &AsyncIoRing,
    ) -> i32 {
        match UblkDescOperation::try_from(desc.op_flags & 0xff) {
            Ok(UblkDescOperation::Read) => match self.read_range(desc) {
                Ok((offset, len)) => self.read(offset, len, buf, ring).await,
                Err(errno) => errno,
            },
            Ok(UblkDescOperation::Flush) => 0,
            Ok(
                UblkDescOperation::Write
                | UblkDescOperation::Discard
                | UblkDescOperation::WriteZeroes
                | UblkDescOperation::WriteSame,
            ) => -libc::EROFS,
            _ => -libc::EOPNOTSUPP,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blob(size: u64) -> (tempfile::NamedTempFile, RawBlobTarget) {
        let file = tempfile::NamedTempFile::new().unwrap();
        file.as_file().set_len(size).unwrap();
        let target = RawBlobTarget::new(file.path(), size, RawReadMode::CopyBuffered).unwrap();
        (file, target)
    }

    #[test]
    fn capacity_and_request_offsets_use_512_byte_sectors() {
        let (_file, target) = blob(12288);
        let params = target.ublk_dev_params(&ublk_sys::ublksrv_ctrl_dev_info {
            max_io_buf_bytes: 262144,
            ..Default::default()
        });
        assert_eq!(params.basic.dev_sectors, 24);
        assert_eq!(params.basic.max_sectors, 512);
        assert_eq!(params.basic.logical_bs_shift, 12);
        assert_eq!(params.basic.physical_bs_shift, 12);
        assert_eq!(params.basic.attrs, ublk_sys::UBLK_ATTR_READ_ONLY);
        assert_eq!(params.types, ublk_sys::UBLK_PARAM_TYPE_BASIC);
        assert_eq!(
            target.read_range(ublk_sys::ublksrv_io_desc {
                start_sector: 8,
                nr_sectors: 16,
                ..Default::default()
            }),
            Ok((4096, 8192))
        );
        for (start_sector, nr_sectors) in [
            (1, 8),
            (0, 1),
            (16, 16),
            (u64::MAX, 8),
            (0, 0),
            (0, u32::MAX),
        ] {
            assert_eq!(
                target.read_range(ublk_sys::ublksrv_io_desc {
                    start_sector,
                    nr_sectors,
                    ..Default::default()
                }),
                Err(-libc::EINVAL)
            );
        }
    }

    #[test]
    fn validates_file_type_and_exact_capacity_without_parsing_bytes() {
        let (file, _) = blob(4096);
        for size in [0, 512, 8192, u64::MAX] {
            assert!(RawBlobTarget::new(file.path(), size, RawReadMode::CopyBuffered).is_err());
        }
        assert!(RawBlobTarget::new(
            file.path().parent().unwrap(),
            4096,
            RawReadMode::CopyBuffered
        )
        .is_err());
        let directory = tempfile::tempdir().unwrap();
        let fifo = directory.path().join("fifo");
        nix::unistd::mkfifo(&fifo, nix::sys::stat::Mode::S_IRUSR).unwrap();
        assert!(RawBlobTarget::new(&fifo, 4096, RawReadMode::CopyBuffered).is_err());
    }

    #[test]
    fn direct_alignment_must_be_known_and_supported() {
        assert!(validate_dio_alignment(libc::STATX_DIOALIGN, 512, 4096).is_ok());
        for (mask, memory, offset) in [
            (0, 512, 512),
            (libc::STATX_DIOALIGN, 0, 0),
            (libc::STATX_DIOALIGN, 8192, 4096),
            (libc::STATX_DIOALIGN, 512, 8192),
        ] {
            assert!(validate_dio_alignment(mask, memory, offset).is_err());
        }
    }
}
