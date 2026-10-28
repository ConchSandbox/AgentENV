use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use overlaybd::image_service::ImageService;
use storage_util::io_ring::spawn_io_ring_worker;
use tokio::net::UnixStream;
use uvm_ublk_daemon::protocol::{
    recv_message, send_message, DaemonRequest, DaemonResponse, RawReadMode,
};
use uvm_ublk_daemon::UblkDaemonServer;

async fn rpc(socket: &Path, request: DaemonRequest) -> DaemonResponse {
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut stream = UnixStream::connect(socket).await.unwrap();
        send_message(&mut stream, &request).await.unwrap();
        recv_message(&mut stream).await.unwrap().unwrap()
    })
    .await
    .expect("raw blob RPC timed out")
}

struct Aligned {
    ptr: *mut u8,
    layout: Layout,
}

impl Aligned {
    fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).unwrap();
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
        unsafe {
            dealloc(self.ptr, self.layout);
        }
    }
}

async fn server(
    dir: &Path,
) -> (
    Arc<UblkDaemonServer>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    PathBuf,
) {
    let global = dir.join("global.json");
    std::fs::write(
        &global,
        serde_json::to_vec(&serde_json::json!({
            "ioEngine": 0, "download": {"enable": false}, "p2pConfig": {"enable": false},
            "cacheConfig": {"cacheType": "file", "cacheDir": dir.join("cache"), "cacheSizeGB": 1}
        }))
        .unwrap(),
    )
    .unwrap();
    let image_service = ImageService::from_config_path(&global).await.unwrap();
    let (ring, _worker) = spawn_io_ring_worker::<io_uring::squeue::Entry128>(0);
    let socket = dir.join("daemon.sock");
    let server = Arc::new(UblkDaemonServer::new(
        socket.clone(),
        ring,
        image_service,
        "/dev/null".into(),
    ));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let runner = Arc::clone(&server);
    let task = tokio::spawn(async move {
        runner
            .run_with_ready_signal(|| {
                let _ = ready_tx.send(());
                Ok(())
            })
            .await
    });
    ready_rx.await.unwrap();
    (server, task, socket)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_blob_delete_fences_late_create_over_rpc() {
    let dir = tempfile::tempdir().unwrap();
    let (server, task, socket) = server(dir.path()).await;
    let key = "deleted-before-create".to_owned();
    assert!(matches!(
        rpc(
            &socket,
            DaemonRequest::DeleteRawBlob {
                device_key: key.clone()
            }
        )
        .await,
        DaemonResponse::RawBlobDeleted
    ));
    assert!(matches!(
        rpc(
            &socket,
            DaemonRequest::CreateRawBlob {
                device_key: key.clone(),
                blob_digest: "trusted".into(),
                blob_path: "/absent".into(),
                blob_url: None,
                size_bytes: 4096,
                read_mode: RawReadMode::CopyBuffered
            }
        )
        .await,
        DaemonResponse::InvalidRequest { .. }
    ));
    assert!(matches!(
        rpc(&socket, DaemonRequest::GetRawBlob { device_key: key }).await,
        DaemonResponse::RawBlobMissing
    ));
    server.request_shutdown();
    task.await.unwrap().unwrap();
}

/// Run explicitly as root on a kernel with ublk AUTO_BUF_REG and a DIO-capable filesystem.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires root, ublk AUTO_BUF_REG, and direct I/O support"]
async fn raw_blob_kernel_four_modes_and_key_lifetime() {
    // Use the worktree filesystem instead of /tmp, which may be tmpfs without DIO.
    let base = std::env::var_os("RAW_BLOB_TEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap());
    let dir = tempfile::tempdir_in(base).unwrap();
    let server_dir = tempfile::tempdir().unwrap();
    let (server, task, socket) = server(server_dir.path()).await;
    let source = dir.path().join("blob");
    let data: Vec<u8> = (0..5 * 4096).map(|i| (i * 13 + i / 4096) as u8).collect();
    std::fs::write(&source, &data).unwrap();
    let mut previous_key = None;
    for mode in [
        RawReadMode::CopyBuffered,
        RawReadMode::CopyDirect,
        RawReadMode::ZcBuffered,
        RawReadMode::ZcDirect,
    ] {
        let key = format!("activation-{mode:?}");
        let create = DaemonRequest::CreateRawBlob {
            device_key: key.clone(),
            blob_digest: "trusted-identity".into(),
            blob_path: source.clone(),
            blob_url: None,
            size_bytes: data.len() as u64,
            read_mode: mode,
        };
        let response = rpc(&socket, create.clone()).await;
        let DaemonResponse::RawBlobDevice {
            dev_id,
            device_path,
            size_bytes,
            read_mode,
        } = response
        else {
            panic!("create {mode:?}: {response:?}");
        };
        assert_eq!(size_bytes, data.len() as u64);
        assert_eq!(read_mode, mode);
        let mut concurrent = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let socket = socket.clone();
            let create = create.clone();
            concurrent.spawn(async move { rpc(&socket, create).await });
        }
        while let Some(result) = concurrent.join_next().await {
            assert!(
                matches!(result.unwrap(), DaemonResponse::RawBlobDevice { dev_id: id, .. } if id == dev_id)
            );
        }
        for request in [
            create.clone(),
            DaemonRequest::GetRawBlob {
                device_key: key.clone(),
            },
        ] {
            assert!(
                matches!(rpc(&socket, request).await, DaemonResponse::RawBlobDevice { dev_id: id, .. } if id == dev_id)
            );
        }
        let mut changed = create.clone();
        if let DaemonRequest::CreateRawBlob { blob_digest, .. } = &mut changed {
            *blob_digest = "different".into();
        }
        assert!(matches!(
            rpc(&socket, changed).await,
            DaemonResponse::InvalidRequest { .. }
        ));
        assert!(matches!(
            rpc(&socket, DaemonRequest::Delete { dev_id }).await,
            DaemonResponse::Error { .. }
        ));
        if let Some(old_key) = previous_key.take() {
            assert!(matches!(
                rpc(
                    &socket,
                    DaemonRequest::DeleteRawBlob {
                        device_key: old_key
                    }
                )
                .await,
                DaemonResponse::RawBlobDeleted
            ));
        }
        for (name, expected) in [
            ("size", (data.len() / 512).to_string()),
            ("queue/logical_block_size", "4096".into()),
            ("queue/physical_block_size", "4096".into()),
            ("ro", "1".into()),
        ] {
            let actual =
                std::fs::read_to_string(format!("/sys/class/block/ublkb{dev_id}/{name}")).unwrap();
            assert_eq!(actual.trim(), expected);
        }
        // Force block requests even when the same source bytes were read before.
        let device = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(&device_path)
            .unwrap();
        let mut buffer = Aligned::new(data.len());
        device.read_exact_at(buffer.bytes(), 0).unwrap();
        assert_eq!(buffer.bytes(), data);
        device
            .read_exact_at(&mut buffer.bytes()[..4096], 3 * 4096)
            .unwrap();
        assert_eq!(&buffer.bytes()[..4096], &data[3 * 4096..4 * 4096]);
        let buffered = File::open(&device_path).unwrap();
        let mut last = vec![0; 4096];
        buffered.read_exact_at(&mut last, 4 * 4096).unwrap();
        assert_eq!(last, data[4 * 4096..]);
        if let Ok(writer) = OpenOptions::new().write(true).open(&device_path) {
            assert!(writer.write_at(&data[..4096], 0).is_err());
        }
        if mode == RawReadMode::ZcDirect {
            // Deliberately violate source immutability to exercise EOF handling.
            OpenOptions::new()
                .write(true)
                .open(&source)
                .unwrap()
                .set_len(4096)
                .unwrap();
            let error = device
                .read_at(&mut buffer.bytes()[..4096], 4 * 4096)
                .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::EIO));
        }
        drop(buffered);
        drop(device);
        assert!(matches!(
            rpc(
                &socket,
                DaemonRequest::DeleteRawBlob {
                    device_key: key.clone()
                }
            )
            .await,
            DaemonResponse::RawBlobDeleted
        ));
        assert!(!device_path.exists());
        assert!(matches!(
            rpc(
                &socket,
                DaemonRequest::GetRawBlob {
                    device_key: key.clone()
                }
            )
            .await,
            DaemonResponse::RawBlobMissing
        ));
        assert!(matches!(
            rpc(&socket, create).await,
            DaemonResponse::InvalidRequest { .. }
        ));
        previous_key = Some(key);
    }
    server.request_shutdown();
    task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires root, ublk, and host blkid/other automatic probes disabled"]
async fn raw_blob_http_setup_and_mmap_are_lazy_and_ranges_are_exact() {
    http_device_case(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires root and ublk"]
async fn raw_blob_http_device_reads_and_missing_range_errors() {
    http_device_case(false).await;
}

async fn http_device_case(require_no_external_probes: bool) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let data = Arc::new(
        (0..256 * 4096)
            .map(|i| (i * 17 + i / 4096) as u8)
            .collect::<Vec<_>>(),
    );
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/blob", listener.local_addr().unwrap());
    let http_requests = Arc::clone(&requests);
    let http_data = Arc::clone(&data);
    let http = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                header.push(stream.read_u8().await.unwrap());
                assert!(header.len() < 8192);
            }
            let header = String::from_utf8(header).unwrap().to_lowercase();
            let range = header
                .lines()
                .find_map(|line| line.strip_prefix("range: bytes="))
                .unwrap();
            let (start, end) = range.split_once('-').unwrap();
            let (start, end) = (
                start.parse::<usize>().unwrap(),
                end.parse::<usize>().unwrap(),
            );
            http_requests.lock().unwrap().push((start, end));
            if start == 64 * 4096 {
                stream
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await
                    .unwrap();
                continue;
            }
            let response = format!("HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", http_data.len(), end - start + 1);
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.write_all(&http_data[start..=end]).await.unwrap();
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let (server, task, socket) = server(dir.path()).await;
    let key = "remote-activation".to_owned();
    let response = rpc(
        &socket,
        DaemonRequest::CreateRawBlob {
            device_key: key.clone(),
            blob_digest: "trusted".into(),
            blob_path: PathBuf::new(),
            blob_url: Some(url),
            size_bytes: data.len() as u64,
            read_mode: RawReadMode::CopyBuffered,
        },
    )
    .await;
    let DaemonResponse::RawBlobDevice { device_path, .. } = response else {
        panic!("{response:?}");
    };
    if require_no_external_probes {
        assert!(requests.lock().unwrap().is_empty());
    }
    let device = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(device_path)
        .unwrap();
    let mapped = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            data.len(),
            libc::PROT_READ,
            libc::MAP_PRIVATE,
            device.as_raw_fd(),
            0,
        )
    };
    assert_ne!(mapped, libc::MAP_FAILED);
    if require_no_external_probes {
        assert!(
            requests.lock().unwrap().is_empty(),
            "mmap must not fetch payload"
        );
    }
    assert_eq!(unsafe { libc::munmap(mapped, data.len()) }, 0);
    let mut buffer = Aligned::new(4096);
    // A caller can request just header/index bytes without downloading payload.
    let offsets = [0, data.len() - 4096, 32 * 4096];
    for offset in offsets {
        device.read_exact_at(buffer.bytes(), offset as u64).unwrap();
        assert_eq!(buffer.bytes(), &data[offset..offset + 4096]);
    }
    if require_no_external_probes {
        assert_eq!(
            *requests.lock().unwrap(),
            offsets.map(|offset| (offset, offset + 4095))
        );
    } else {
        // External host probes are separate readers, not source read amplification.
        for offset in offsets {
            assert!(requests.lock().unwrap().contains(&(offset, offset + 4095)));
        }
    }
    buffer.bytes().fill(99);
    let error = device.read_at(buffer.bytes(), 64 * 4096).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EIO));
    drop(device);
    assert!(matches!(
        rpc(&socket, DaemonRequest::DeleteRawBlob { device_key: key }).await,
        DaemonResponse::RawBlobDeleted
    ));
    server.request_shutdown();
    task.await.unwrap().unwrap();
    http.abort();
}
