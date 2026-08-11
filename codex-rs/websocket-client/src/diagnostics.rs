use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::http::Uri;

#[cfg(unix)]
use std::os::fd::AsRawFd;
#[cfg(all(unix, target_os = "linux"))]
use std::os::fd::RawFd;

const DIAGNOSTICS_ENV: &str = "CODEX_WS_TRANSPORT_DIAGNOSTICS";
const NEVER: u64 = u64::MAX;
const TARGET: &str = "codex_websocket_client::transport";

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) struct TransportDiagnostics {
    connection_id: u64,
    created_at: Instant,
    raw_bytes_read: AtomicU64,
    raw_bytes_written: AtomicU64,
    websocket_bytes_read: AtomicU64,
    websocket_bytes_written: AtomicU64,
    last_read_ms: AtomicU64,
    last_write_ms: AtomicU64,
    read_zero: AtomicBool,
    local_write_shutdown: AtomicBool,
    websocket_close_started: AtomicBool,
    transport_dropped: AtomicBool,
    fd: AtomicI32,
}

impl TransportDiagnostics {
    pub(crate) fn from_env(uri: &Uri, route: &str) -> Option<Arc<Self>> {
        diagnostics_enabled().then(|| Self::new(uri, route))
    }

    pub(super) fn new(uri: &Uri, route: &str) -> Arc<Self> {
        let connection_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        let created_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let diagnostics = Arc::new(Self {
            connection_id,
            created_at: Instant::now(),
            raw_bytes_read: AtomicU64::new(0),
            raw_bytes_written: AtomicU64::new(0),
            websocket_bytes_read: AtomicU64::new(0),
            websocket_bytes_written: AtomicU64::new(0),
            last_read_ms: AtomicU64::new(NEVER),
            last_write_ms: AtomicU64::new(NEVER),
            read_zero: AtomicBool::new(false),
            local_write_shutdown: AtomicBool::new(false),
            websocket_close_started: AtomicBool::new(false),
            transport_dropped: AtomicBool::new(false),
            fd: AtomicI32::new(-1),
        });
        tracing::info!(
            target: TARGET,
            ws_connection_id = connection_id,
            event = "connection_created",
            created_at_unix_ms,
            url = %redacted_uri(uri),
            route,
        );
        diagnostics
    }

    pub(crate) fn connection_id(&self) -> u64 {
        self.connection_id
    }

    pub(crate) fn resolved(&self, authority: &str, addresses: &[std::net::SocketAddr]) {
        tracing::info!(
            target: TARGET,
            ws_connection_id = self.connection_id,
            event = "tcp_resolved",
            authority,
            addresses = ?addresses,
        );
    }

    pub(crate) fn tcp_connected(&self, stream: &TcpStream) {
        #[cfg(unix)]
        self.fd.store(stream.as_raw_fd(), Ordering::Release);
        tracing::info!(
            target: TARGET,
            ws_connection_id = self.connection_id,
            event = "tcp_connected",
            fd = self.fd.load(Ordering::Acquire),
            local_addr = ?stream.local_addr(),
            peer_addr = ?stream.peer_addr(),
            socket = %socket_snapshot(stream),
        );
    }

    pub(crate) fn handshake_succeeded(
        &self,
        status: tokio_tungstenite::tungstenite::http::StatusCode,
    ) {
        tracing::info!(
            target: TARGET,
            ws_connection_id = self.connection_id,
            event = "handshake_succeeded",
            tls_handshake_success = true,
            websocket_handshake_success = true,
            websocket_status = status.as_u16(),
            lifetime_ms = self.elapsed_ms(),
        );
    }

    pub(crate) fn websocket_message(&self, direction: &'static str, message: &Message) {
        let bytes = message.len() as u64;
        match direction {
            "read" => {
                self.websocket_bytes_read
                    .fetch_add(bytes, Ordering::Relaxed);
            }
            "write" => {
                self.websocket_bytes_written
                    .fetch_add(bytes, Ordering::Relaxed);
            }
            _ => {}
        }
        let kind = message_kind(message);
        if matches!(
            message,
            Message::Ping(_) | Message::Pong(_) | Message::Close(_)
        ) {
            tracing::info!(
                target: TARGET,
                ws_connection_id = self.connection_id,
                event = "websocket_control_frame",
                direction,
                kind,
                payload_bytes = bytes,
                close = ?close_details(message),
                lifetime_ms = self.elapsed_ms(),
            );
        } else {
            tracing::debug!(
                target: TARGET,
                ws_connection_id = self.connection_id,
                event = "websocket_message",
                direction,
                kind,
                payload_bytes = bytes,
                lifetime_ms = self.elapsed_ms(),
            );
        }
    }

    pub(crate) fn websocket_error(&self, operation: &'static str, error: &WebSocketError) {
        let (io_kind, raw_os_error) = match error {
            WebSocketError::Io(error) => (Some(error.kind()), error.raw_os_error()),
            _ => (None, None),
        };
        tracing::warn!(
            target: TARGET,
            ws_connection_id = self.connection_id,
            event = "websocket_error",
            operation,
            error = %error,
            io_kind = ?io_kind,
            raw_os_error,
            fd = self.fd.load(Ordering::Acquire),
            state = %self.summary(),
        );
    }

    pub(crate) fn stream_ended(&self) {
        tracing::warn!(
            target: TARGET,
            ws_connection_id = self.connection_id,
            event = "websocket_stream_ended",
            state = %self.summary(),
        );
    }

    pub(crate) fn websocket_close_started(&self) {
        if !self.websocket_close_started.swap(true, Ordering::AcqRel) {
            tracing::info!(
                target: TARGET,
                ws_connection_id = self.connection_id,
                event = "local_websocket_close_started",
                lifetime_ms = self.elapsed_ms(),
            );
        }
    }

    pub(crate) fn connection_dropped(&self) {
        tracing::warn!(
            target: TARGET,
            ws_connection_id = self.connection_id,
            event = "websocket_object_dropped",
            fd = self.fd.load(Ordering::Acquire),
            state = %self.summary(),
        );
    }

    fn raw_read(&self, bytes: usize) {
        self.raw_bytes_read
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.last_read_ms
            .store(self.elapsed_ms(), Ordering::Release);
    }

    fn raw_write(&self, bytes: usize) {
        self.raw_bytes_written
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.last_write_ms
            .store(self.elapsed_ms(), Ordering::Release);
    }

    fn raw_read_zero(&self, stream: &TcpStream) {
        self.read_zero.store(true, Ordering::Release);
        tracing::warn!(
            target: TARGET,
            ws_connection_id = self.connection_id,
            event = "raw_tcp_read_zero",
            read_returned_zero = true,
            fd = self.fd.load(Ordering::Acquire),
            local_addr = ?stream.local_addr(),
            peer_addr = ?stream.peer_addr(),
            socket = %socket_snapshot(stream),
            state = %self.summary(),
        );
    }

    fn raw_io_error(&self, operation: &'static str, error: &io::Error, stream: &TcpStream) {
        tracing::warn!(
            target: TARGET,
            ws_connection_id = self.connection_id,
            event = "raw_tcp_io_error",
            operation,
            io_kind = ?error.kind(),
            raw_os_error = error.raw_os_error(),
            error = %error,
            fd = self.fd.load(Ordering::Acquire),
            local_addr = ?stream.local_addr(),
            peer_addr = ?stream.peer_addr(),
            socket = %socket_snapshot(stream),
            state = %self.summary(),
        );
    }

    fn raw_shutdown(&self, stream: &TcpStream) {
        if !self.local_write_shutdown.swap(true, Ordering::AcqRel) {
            tracing::warn!(
                target: TARGET,
                ws_connection_id = self.connection_id,
                event = "local_tcp_write_shutdown_polled",
                fd = self.fd.load(Ordering::Acquire),
                local_addr = ?stream.local_addr(),
                peer_addr = ?stream.peer_addr(),
                state = %self.summary(),
            );
        }
    }

    fn raw_transport_dropped(&self, stream: &TcpStream) {
        self.transport_dropped.store(true, Ordering::Release);
        tracing::warn!(
            target: TARGET,
            ws_connection_id = self.connection_id,
            event = "raw_tcp_transport_dropped",
            fd = self.fd.load(Ordering::Acquire),
            local_addr = ?stream.local_addr(),
            peer_addr = ?stream.peer_addr(),
            socket = %socket_snapshot(stream),
            state = %self.summary(),
        );
    }

    fn elapsed_ms(&self) -> u64 {
        self.created_at
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    fn summary(&self) -> String {
        format!(
            "lifetime_ms={} raw_read={} raw_written={} ws_read={} ws_written={} \
             last_read_ms={:?} last_write_ms={:?} read_zero={} local_write_shutdown={} \
             websocket_close_started={} transport_dropped={}",
            self.elapsed_ms(),
            self.raw_bytes_read.load(Ordering::Relaxed),
            self.raw_bytes_written.load(Ordering::Relaxed),
            self.websocket_bytes_read.load(Ordering::Relaxed),
            self.websocket_bytes_written.load(Ordering::Relaxed),
            timestamp(self.last_read_ms.load(Ordering::Acquire)),
            timestamp(self.last_write_ms.load(Ordering::Acquire)),
            self.read_zero.load(Ordering::Acquire),
            self.local_write_shutdown.load(Ordering::Acquire),
            self.websocket_close_started.load(Ordering::Acquire),
            self.transport_dropped.load(Ordering::Acquire),
        )
    }
}

pub(crate) struct DiagnosticTcpStream {
    inner: TcpStream,
    diagnostics: Arc<TransportDiagnostics>,
}

impl DiagnosticTcpStream {
    pub(crate) fn new(inner: TcpStream, diagnostics: Arc<TransportDiagnostics>) -> Self {
        diagnostics.tcp_connected(&inner);
        Self { inner, diagnostics }
    }
}

impl AsyncRead for DiagnosticTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buffer.filled().len();
        let had_capacity = buffer.remaining() > 0;
        match Pin::new(&mut this.inner).poll_read(context, buffer) {
            Poll::Ready(Ok(())) => {
                let bytes = buffer.filled().len().saturating_sub(before);
                if bytes == 0 && had_capacity {
                    this.diagnostics.raw_read_zero(&this.inner);
                } else if bytes > 0 {
                    this.diagnostics.raw_read(bytes);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                this.diagnostics.raw_io_error("read", &error, &this.inner);
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for DiagnosticTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write(context, buffer) {
            Poll::Ready(Ok(bytes)) => {
                this.diagnostics.raw_write(bytes);
                Poll::Ready(Ok(bytes))
            }
            Poll::Ready(Err(error)) => {
                this.diagnostics.raw_io_error("write", &error, &this.inner);
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_flush(context) {
            Poll::Ready(Err(error)) => {
                this.diagnostics.raw_io_error("flush", &error, &this.inner);
                Poll::Ready(Err(error))
            }
            result => result,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.diagnostics.raw_shutdown(&this.inner);
        match Pin::new(&mut this.inner).poll_shutdown(context) {
            Poll::Ready(Err(error)) => {
                this.diagnostics
                    .raw_io_error("shutdown", &error, &this.inner);
                Poll::Ready(Err(error))
            }
            result => result,
        }
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_write_vectored(context, buffers) {
            Poll::Ready(Ok(bytes)) => {
                this.diagnostics.raw_write(bytes);
                Poll::Ready(Ok(bytes))
            }
            Poll::Ready(Err(error)) => {
                this.diagnostics
                    .raw_io_error("write_vectored", &error, &this.inner);
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for DiagnosticTcpStream {
    fn drop(&mut self) {
        self.diagnostics.raw_transport_dropped(&self.inner);
    }
}

fn diagnostics_enabled() -> bool {
    std::env::var(DIAGNOSTICS_ENV).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn redacted_uri(uri: &Uri) -> String {
    let scheme = uri.scheme_str().unwrap_or("<unknown>");
    let authority = uri
        .authority()
        .map(tokio_tungstenite::tungstenite::http::uri::Authority::as_str)
        .unwrap_or("<unknown>");
    format!("{scheme}://{authority}{}", uri.path())
}

fn message_kind(message: &Message) -> &'static str {
    match message {
        Message::Text(_) => "text",
        Message::Binary(_) => "binary",
        Message::Ping(_) => "ping",
        Message::Pong(_) => "pong",
        Message::Close(_) => "close",
        Message::Frame(_) => "frame",
    }
}

fn close_details(message: &Message) -> Option<(String, String)> {
    let Message::Close(Some(frame)) = message else {
        return None;
    };
    Some((frame.code.to_string(), frame.reason.to_string()))
}

fn timestamp(value: u64) -> Option<u64> {
    (value != NEVER).then_some(value)
}

#[cfg(all(unix, target_os = "linux"))]
fn socket_snapshot(stream: &TcpStream) -> String {
    let fd = stream.as_raw_fd();
    let fd_open = unsafe { libc::fcntl(fd, libc::F_GETFD) } >= 0;
    let mut pollfd = libc::pollfd {
        fd,
        events: libc::POLLIN | libc::POLLOUT | libc::POLLRDHUP,
        revents: 0,
    };
    let poll_result = unsafe {
        libc::poll(&mut pollfd, /*nfds*/ 1, /*timeout*/ 0)
    };
    let poll_revents = (poll_result >= 0).then_some(pollfd.revents);
    let mut byte = 0_u8;
    let recv_peek = unsafe {
        libc::recv(
            fd,
            std::ptr::addr_of_mut!(byte).cast(),
            /*len*/ 1,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    let recv_peek_errno = (recv_peek < 0)
        .then(|| io::Error::last_os_error().raw_os_error())
        .flatten();
    let tcp_state = tcp_state(fd);
    format!(
        "fd_open={fd_open} poll_revents={poll_revents:?} pollin={:?} pollout={:?} \
         pollerr={:?} pollhup={:?} pollrdhup={:?} recv_peek={recv_peek} \
         recv_peek_errno={recv_peek_errno:?} so_error={:?} so_keepalive={:?} \
         tcp_keepidle={:?} tcp_keepintvl={:?} tcp_keepcnt={:?} tcp_state={:?} tcp_state_raw={tcp_state:?}",
        poll_revents.map(|flags| flags & libc::POLLIN != 0),
        poll_revents.map(|flags| flags & libc::POLLOUT != 0),
        poll_revents.map(|flags| flags & libc::POLLERR != 0),
        poll_revents.map(|flags| flags & libc::POLLHUP != 0),
        poll_revents.map(|flags| flags & libc::POLLRDHUP != 0),
        // SO_ERROR is destructive, so this helper is called only at connect or terminal state.
        getsockopt_i32(fd, libc::SOL_SOCKET, libc::SO_ERROR),
        getsockopt_i32(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE),
        getsockopt_i32(fd, libc::IPPROTO_TCP, libc::TCP_KEEPIDLE),
        getsockopt_i32(fd, libc::IPPROTO_TCP, libc::TCP_KEEPINTVL),
        getsockopt_i32(fd, libc::IPPROTO_TCP, libc::TCP_KEEPCNT),
        tcp_state.map(tcp_state_name),
    )
}

#[cfg(not(all(unix, target_os = "linux")))]
fn socket_snapshot(_stream: &TcpStream) -> &'static str {
    "unavailable_on_this_platform"
}

#[cfg(all(unix, target_os = "linux"))]
fn getsockopt_i32(fd: RawFd, level: libc::c_int, option: libc::c_int) -> Option<i32> {
    let mut value = 0_i32;
    let mut length = std::mem::size_of_val(&value) as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            fd,
            level,
            option,
            std::ptr::addr_of_mut!(value).cast(),
            &mut length,
        )
    };
    (result == 0).then_some(value)
}

#[cfg(all(unix, target_os = "linux"))]
fn tcp_state(fd: RawFd) -> Option<u8> {
    let mut info = [0_u8; 256];
    let mut length = info.len() as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_INFO,
            info.as_mut_ptr().cast(),
            &mut length,
        )
    };
    (result == 0 && length > 0).then_some(info[0])
}

#[cfg(all(unix, target_os = "linux"))]
fn tcp_state_name(state: u8) -> &'static str {
    match state {
        1 => "established",
        2 => "syn_sent",
        3 => "syn_received",
        4 => "fin_wait_1",
        5 => "fin_wait_2",
        6 => "time_wait",
        7 => "closed",
        8 => "close_wait",
        9 => "last_ack",
        10 => "listen",
        11 => "closing",
        12 => "new_syn_received",
        _ => "unknown",
    }
}

#[cfg(test)]
#[path = "diagnostics_tests.rs"]
mod tests;
