//! Proxy-aware WebSocket connection setup shared by Codex API clients.

mod diagnostics;
mod dialer;

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use codex_http_client::BuildCustomCaTransportError;
use codex_http_client::HttpClientFactory;
use codex_http_client::OutboundProxyRoute;
use codex_http_client::build_rustls_client_config_with_custom_ca;
use futures::Sink;
use futures::Stream;
use rustls::ClientConfig;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream as TungsteniteStream;
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::handshake::client::Response;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

use crate::diagnostics::TransportDiagnostics;

/// Connects WebSockets using the outbound proxy policy resolved by application configuration.
///
/// Construct this from the effective [`HttpClientFactory`] rather than selecting proxy behavior at
/// individual call sites. Each connection resolves its destination through that factory before
/// opening a socket.
#[derive(Clone)]
pub struct WebSocketConnector {
    http_client_factory: HttpClientFactory,
    tls_config: Arc<ClientConfig>,
}

impl WebSocketConnector {
    /// Creates a connector using native roots and any configured Codex custom CA bundle.
    pub fn new(
        http_client_factory: &HttpClientFactory,
    ) -> Result<Self, BuildCustomCaTransportError> {
        Ok(Self {
            http_client_factory: http_client_factory.clone(),
            tls_config: build_rustls_client_config_with_custom_ca()?,
        })
    }

    /// Connects a WebSocket after resolving the request destination through the configured proxy
    /// policy.
    pub async fn connect(
        &self,
        request: Request,
        config: WebSocketConfig,
    ) -> Result<(WebSocketConnection, Response), WebSocketError> {
        let proxy_route = self
            .http_client_factory
            .resolve_proxy_route(&request.uri().to_string());
        self.connect_with_route(request, config, proxy_route).await
    }

    /// Connects through a caller-selected HTTP or HTTPS proxy.
    ///
    /// This bypasses system and environment proxy discovery, which makes the selected route
    /// suitable for controlled diagnostics where the proxy endpoint must be known exactly.
    pub async fn connect_with_explicit_proxy(
        &self,
        request: Request,
        config: WebSocketConfig,
        proxy_url: &str,
    ) -> Result<(WebSocketConnection, Response), WebSocketError> {
        self.connect_with_route(
            request,
            config,
            OutboundProxyRoute::Proxy {
                url: proxy_url.to_string(),
            },
        )
        .await
    }

    async fn connect_with_route(
        &self,
        request: Request,
        config: WebSocketConfig,
        proxy_route: OutboundProxyRoute,
    ) -> Result<(WebSocketConnection, Response), WebSocketError> {
        let diagnostics =
            TransportDiagnostics::from_env(request.uri(), &format!("{proxy_route:?}"));
        let result = dialer::connect(
            request,
            config,
            Arc::clone(&self.tls_config),
            proxy_route,
            diagnostics.clone(),
        )
        .await;
        let (inner, response) = match result {
            Ok(result) => result,
            Err(error) => {
                if let Some(diagnostics) = diagnostics.as_ref() {
                    diagnostics.websocket_error("handshake", &error);
                }
                return Err(error);
            }
        };
        if let Some(diagnostics) = diagnostics.as_ref() {
            diagnostics.handshake_succeeded(response.status());
        }
        Ok((WebSocketConnection { inner, diagnostics }, response))
    }
}

/// An established WebSocket independent of its direct, proxy, and TLS transport layers.
///
/// This implements [`Stream`] and [`Sink`] so protocol clients can process Tungstenite messages
/// without knowing which concrete network stream route selection produced.
pub struct WebSocketConnection {
    inner: ConnectionInner,
    diagnostics: Option<Arc<TransportDiagnostics>>,
}

impl WebSocketConnection {
    /// Returns the transport diagnostic connection identifier when diagnostics are enabled.
    pub fn diagnostic_connection_id(&self) -> Option<u64> {
        self.diagnostics
            .as_ref()
            .map(|diagnostics| diagnostics.connection_id())
    }
}

impl Stream for WebSocketConnection {
    type Item = Result<Message, WebSocketError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let result = match &mut this.inner {
            ConnectionInner::TransportDefault(stream) => Pin::new(stream).poll_next(context),
            ConnectionInner::Routed(stream) => Pin::new(stream).poll_next(context),
        };
        if let Poll::Ready(result) = &result
            && let Some(diagnostics) = this.diagnostics.as_ref()
        {
            match result {
                Some(Ok(message)) => diagnostics.websocket_message("read", message),
                Some(Err(error)) => diagnostics.websocket_error("read", error),
                None => diagnostics.stream_ended(),
            }
        }
        result
    }
}

impl Sink<Message> for WebSocketConnection {
    type Error = WebSocketError;

    fn poll_ready(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let result = match &mut this.inner {
            ConnectionInner::TransportDefault(stream) => Pin::new(stream).poll_ready(context),
            ConnectionInner::Routed(stream) => Pin::new(stream).poll_ready(context),
        };
        if let Poll::Ready(Err(error)) = &result
            && let Some(diagnostics) = this.diagnostics.as_ref()
        {
            diagnostics.websocket_error("ready", error);
        }
        result
    }

    fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
        let this = self.get_mut();
        if let Some(diagnostics) = this.diagnostics.as_ref() {
            diagnostics.websocket_message("write", &message);
        }
        let result = match &mut this.inner {
            ConnectionInner::TransportDefault(stream) => Pin::new(stream).start_send(message),
            ConnectionInner::Routed(stream) => Pin::new(stream).start_send(message),
        };
        if let Err(error) = &result
            && let Some(diagnostics) = this.diagnostics.as_ref()
        {
            diagnostics.websocket_error("start_send", error);
        }
        result
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        let result = match &mut this.inner {
            ConnectionInner::TransportDefault(stream) => Pin::new(stream).poll_flush(context),
            ConnectionInner::Routed(stream) => Pin::new(stream).poll_flush(context),
        };
        if let Poll::Ready(Err(error)) = &result
            && let Some(diagnostics) = this.diagnostics.as_ref()
        {
            diagnostics.websocket_error("flush", error);
        }
        result
    }

    fn poll_close(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if let Some(diagnostics) = this.diagnostics.as_ref() {
            diagnostics.websocket_close_started();
        }
        let result = match &mut this.inner {
            ConnectionInner::TransportDefault(stream) => Pin::new(stream).poll_close(context),
            ConnectionInner::Routed(stream) => Pin::new(stream).poll_close(context),
        };
        if let Poll::Ready(Err(error)) = &result
            && let Some(diagnostics) = this.diagnostics.as_ref()
        {
            diagnostics.websocket_error("close", error);
        }
        result
    }
}

impl Drop for WebSocketConnection {
    fn drop(&mut self) {
        if let Some(diagnostics) = self.diagnostics.as_ref() {
            diagnostics.connection_dropped();
        }
    }
}

pub(crate) enum ConnectionInner {
    TransportDefault(TungsteniteStream<MaybeTlsStream<TcpStream>>),
    Routed(TungsteniteStream<MaybeTlsStream<Box<dyn AsyncIo>>>),
}

/// Async network I/O carried through optional proxy and target TLS handshakes.
pub(crate) trait AsyncIo: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> AsyncIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}
