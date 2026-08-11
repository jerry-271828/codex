use std::sync::Arc;
use std::sync::atomic::Ordering;

use pretty_assertions::assert_eq;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::http::Uri;

use super::DiagnosticTcpStream;
use super::TransportDiagnostics;

#[derive(Debug, PartialEq, Eq)]
struct DiagnosticState {
    raw_bytes_read: u64,
    read_zero: bool,
    transport_dropped: bool,
}

#[tokio::test]
async fn records_real_tcp_eof_before_transport_drop() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let address = listener.local_addr().expect("listener should have address");
    let connect = TcpStream::connect(address);
    let (accepted, client) = tokio::join!(listener.accept(), connect);
    let (server, _) = accepted.expect("server should accept connection");
    let client = client.expect("client should connect");
    let uri = "wss://example.test/stream"
        .parse::<Uri>()
        .expect("URI should parse");
    let diagnostics = TransportDiagnostics::new(&uri, "Direct");
    let mut client = DiagnosticTcpStream::new(client, Arc::clone(&diagnostics));

    drop(server);
    let mut byte = [0_u8; 1];
    let bytes = client
        .read(&mut byte)
        .await
        .expect("orderly TCP FIN should read as EOF");

    assert_eq!(bytes, 0);
    assert_eq!(
        state(&diagnostics),
        DiagnosticState {
            raw_bytes_read: 0,
            read_zero: true,
            transport_dropped: false,
        }
    );

    drop(client);
    assert_eq!(
        state(&diagnostics),
        DiagnosticState {
            raw_bytes_read: 0,
            read_zero: true,
            transport_dropped: true,
        }
    );
}

fn state(diagnostics: &TransportDiagnostics) -> DiagnosticState {
    DiagnosticState {
        raw_bytes_read: diagnostics.raw_bytes_read.load(Ordering::Relaxed),
        read_zero: diagnostics.read_zero.load(Ordering::Acquire),
        transport_dropped: diagnostics.transport_dropped.load(Ordering::Acquire),
    }
}
