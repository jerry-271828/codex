use std::env;
use std::error::Error;
use std::future::pending;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use codex_http_client::HttpClientFactory;
use codex_http_client::OutboundProxyPolicy;
use codex_websocket_client::WebSocketConnector;
use futures::SinkExt;
use futures::StreamExt;
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::header::COOKIE;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

const AUTHORIZATION_ENV: &str = "CODEX_WS_PROBE_AUTHORIZATION";
const COOKIE_ENV: &str = "CODEX_WS_PROBE_COOKIE";
type AnyError = Box<dyn Error + Send + Sync>;

fn main() -> Result<(), AnyError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(true)
        .try_init()?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), AnyError> {
    let options = Options::parse()?;
    let mut request = options.url.as_str().into_client_request()?;
    add_sensitive_header_from_env(request.headers_mut(), AUTHORIZATION, AUTHORIZATION_ENV)?;
    add_sensitive_header_from_env(request.headers_mut(), COOKIE, COOKIE_ENV)?;

    let factory = HttpClientFactory::new(OutboundProxyPolicy::ReqwestDefault);
    let connector = WebSocketConnector::new(&factory)?;
    let started_at = tokio::time::Instant::now();
    let proxy_path = options.path_label.as_deref().unwrap_or_else(|| {
        options
            .proxy_url
            .as_ref()
            .map_or("system_default", |_| "explicit_http_connect")
    });
    eprintln!(
        "event=probe_started probe_id={} utc_unix_ms={} target={} proxy_path={proxy_path}",
        options.id,
        utc_unix_ms(),
        options.url,
    );
    let (mut websocket, response) = match options.proxy_url.as_deref() {
        Some(proxy_url) => {
            connector
                .connect_with_explicit_proxy(request, WebSocketConfig::default(), proxy_url)
                .await?
        }
        None => {
            connector
                .connect(request, WebSocketConfig::default())
                .await?
        }
    };
    eprintln!(
        "event=websocket_connected probe_id={} utc_unix_ms={} proxy_path={proxy_path} \
         status={} connection_id={:?} duration_seconds={} ping_seconds={}",
        options.id,
        utc_unix_ms(),
        response.status(),
        websocket.diagnostic_connection_id(),
        options.duration.as_secs(),
        options
            .ping_interval
            .map_or(0, |interval| interval.as_secs()),
    );

    let deadline = tokio::time::sleep_until(started_at + options.duration);
    tokio::pin!(deadline);
    let mut ping_timer = options
        .ping_interval
        .map(|interval| tokio::time::interval_at(tokio::time::Instant::now() + interval, interval));
    let mut ping_sequence = 0_u64;

    loop {
        tokio::select! {
            _ = &mut deadline => {
                eprintln!(
                    "event=duration_reached probe_id={} utc_unix_ms={} elapsed_ms={} \
                     action=websocket_close_handshake",
                    options.id,
                    utc_unix_ms(),
                    started_at.elapsed().as_millis(),
                );
                websocket.close().await?;
                return Ok(());
            }
            _ = async {
                match ping_timer.as_mut() {
                    Some(timer) => {
                        timer.tick().await;
                    }
                    None => pending::<()>().await,
                }
            } => {
                ping_sequence = ping_sequence.wrapping_add(1);
                let payload = format!("hmos-probe-{ping_sequence}").into_bytes().into();
                websocket.send(Message::Ping(payload)).await?;
                eprintln!(
                    "event=ping_sent probe_id={} utc_unix_ms={} elapsed_ms={} \
                     sequence={ping_sequence}",
                    options.id,
                    utc_unix_ms(),
                    started_at.elapsed().as_millis(),
                );
            }
            message = websocket.next() => {
                match message {
                    Some(Ok(message)) => {
                        eprintln!(
                            "event=websocket_message probe_id={} utc_unix_ms={} elapsed_ms={} \
                             direction=received kind={} payload_bytes={}",
                            options.id,
                            utc_unix_ms(),
                            started_at.elapsed().as_millis(),
                            message_kind(&message),
                            message.len(),
                        );
                        match message {
                            Message::Ping(payload) => {
                                websocket.send(Message::Pong(payload)).await?;
                            }
                            Message::Close(_) => return Ok(()),
                            Message::Text(_)
                            | Message::Binary(_)
                            | Message::Pong(_)
                            | Message::Frame(_) => {}
                        }
                    }
                    Some(Err(error)) => {
                        report_error(&options.id, started_at.elapsed(), &error);
                        return Err(error.into());
                    }
                    None => {
                        eprintln!(
                            "event=websocket_stream_ended probe_id={} utc_unix_ms={} elapsed_ms={} \
                             error_item=false",
                            options.id,
                            utc_unix_ms(),
                            started_at.elapsed().as_millis(),
                        );
                        return Ok(());
                    }
                }
            }
        }
    }
}

struct Options {
    id: String,
    url: String,
    duration: Duration,
    ping_interval: Option<Duration>,
    proxy_url: Option<String>,
    path_label: Option<String>,
}

impl Options {
    fn parse() -> Result<Self, AnyError> {
        let mut arguments = env::args().skip(1);
        let mut id = "probe".to_string();
        let mut proxy_url = None;
        let mut path_label = None;
        let mut positional = Vec::new();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--id" => id = required_option_value(&mut arguments, "--id")?,
                "--path-label" => {
                    path_label = Some(required_option_value(&mut arguments, "--path-label")?)
                }
                "--proxy" => {
                    let value = required_option_value(&mut arguments, "--proxy")?;
                    validate_proxy_url(&value)?;
                    proxy_url = Some(value);
                }
                _ if argument.starts_with("--") => {
                    return Err(format!("unknown option: {argument}").into());
                }
                _ => positional.push(argument),
            }
        }
        let Some(url) = positional.first().cloned() else {
            return Err("usage: hmos_wss_probe [--id ID] [--path-label LABEL] \
                 [--proxy http://127.0.0.1:PORT] \
                 <wss-url> [duration-seconds=600] [ping-seconds=30]"
                .into());
        };
        let duration_seconds = parse_seconds(positional.get(1).cloned(), /*default*/ 600)?;
        let ping_seconds = parse_seconds(positional.get(2).cloned(), /*default*/ 30)?;
        if positional.len() > 3 {
            return Err("too many arguments".into());
        }
        Ok(Self {
            id,
            url,
            duration: Duration::from_secs(duration_seconds),
            ping_interval: (ping_seconds > 0).then(|| Duration::from_secs(ping_seconds)),
            proxy_url,
            path_label,
        })
    }
}

fn required_option_value(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, AnyError> {
    arguments
        .next()
        .ok_or_else(|| format!("missing value for {option}").into())
}

fn validate_proxy_url(value: &str) -> Result<(), AnyError> {
    let parsed = url::Url::parse(value)?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err("--proxy currently supports explicit HTTP CONNECT proxies only".into());
    }
    let Some(host) = parsed.host_str() else {
        return Err("--proxy URL must include a host".into());
    };
    if !matches!(host, "127.0.0.1" | "localhost" | "::1") {
        return Err("--proxy must use a loopback host for the TUN control experiment".into());
    }
    if parsed.port_or_known_default().is_none() {
        return Err("--proxy URL must include a port".into());
    }
    Ok(())
}

fn parse_seconds(value: Option<String>, default: u64) -> Result<u64, AnyError> {
    match value {
        Some(value) => Ok(value.parse()?),
        None => Ok(default),
    }
}

fn add_sensitive_header_from_env(
    headers: &mut tokio_tungstenite::tungstenite::http::HeaderMap,
    name: tokio_tungstenite::tungstenite::http::HeaderName,
    environment_variable: &'static str,
) -> Result<(), AnyError> {
    if let Ok(value) = env::var(environment_variable) {
        headers.insert(name, HeaderValue::from_str(&value)?);
    }
    Ok(())
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

fn report_error(probe_id: &str, elapsed: Duration, error: &WebSocketError) {
    match error {
        WebSocketError::Io(io_error) => eprintln!(
            "event=websocket_error probe_id={probe_id} utc_unix_ms={} elapsed_ms={} \
             class=io kind={:?} raw_os_error={:?} error={io_error}",
            utc_unix_ms(),
            elapsed.as_millis(),
            io_error.kind(),
            io_error.raw_os_error(),
        ),
        error => eprintln!(
            "event=websocket_error probe_id={probe_id} utc_unix_ms={} elapsed_ms={} \
             class=websocket error={error}",
            utc_unix_ms(),
            elapsed.as_millis(),
        ),
    }
}

fn utc_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
