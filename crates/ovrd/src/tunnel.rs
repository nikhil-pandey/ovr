//! Tunnels through a provider's CLI, for initiators no network path reaches. A tunnel only carries bytes: pairing and
//! pinned mutual TLS run through it unchanged, so it is opened for anonymous access and no credential is involved.
//!
//! The initiator runs the provider's server side (`cloudflared tunnel`, `devtunnel host`) towards its dial-in port;
//! a host runs the client side (`cloudflared access tcp`, `devtunnel connect`), a local forwarder it dials through.

use std::collections::VecDeque;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};
use std::process::{ExitStatus, Stdio};
use std::str::FromStr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ovr_proto::msg::Provider;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use crate::Error;
use crate::server::PORT;

/// How long a provider's CLI may take to report that it is ready.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// Output lines kept for error messages.
const TAIL: usize = 12;

/// Where a host dials its initiator: `host:port`, or the far end of a tunnel, as in `cloudflare:HOSTNAME`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Addr {
    Direct(String),
    Tunnel { provider: Provider, name: String },
}

impl FromStr for Addr {
    type Err = String;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        if let Some((prefix, name)) = addr.split_once(':')
            && let Ok(provider) = prefix.parse::<Provider>()
            // `cloudflare:7420` is a host named cloudflare.
            && !name.bytes().all(|byte| byte.is_ascii_digit())
        {
            let valid = !name.starts_with(['-', '.'])
                && name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.'));
            return if valid {
                Ok(Self::Tunnel {
                    provider,
                    name: name.to_owned(),
                })
            } else {
                Err(format!("{name:?} is not a {provider} tunnel"))
            };
        }
        if addr.is_empty() || addr.starts_with('-') {
            Err(format!("{addr:?} is not an address"))
        } else if addr.contains(':') {
            Ok(Self::Direct(addr.to_owned()))
        } else {
            Ok(Self::Direct(format!("{addr}:{PORT}")))
        }
    }
}

impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Direct(addr) => f.write_str(addr),
            Self::Tunnel { provider, name } => write!(f, "{provider}:{name}"),
        }
    }
}

/// A provider's CLI started by `ovrd`. Dropping it kills the CLI.
pub struct Running {
    program: &'static str,
    child: Child,
    tail: Arc<Mutex<VecDeque<String>>>,
}

impl Running {
    /// Waits for the CLI to exit, describing how it did.
    pub async fn exited(&mut self) -> String {
        let status = self.child.wait().await;
        describe(self.program, status, &self.tail)
    }
}

/// Creates what the initiator needs at the provider before it can serve: for Dev Tunnels, an anonymous tunnel with
/// the dial-in port, whose ID is returned and kept for good.
///
/// # Errors
///
/// Returns an error when the CLI is missing, not logged in, or fails.
pub async fn create(provider: Provider) -> Result<Option<String>, Error> {
    match provider {
        Provider::Cloudflare => Ok(None),
        Provider::Devtunnel => {
            let created = output(
                "devtunnel",
                &[
                    "create",
                    "--allow-anonymous",
                    "--description",
                    "ovr",
                    "--json",
                ],
            )
            .await?;
            let id = devtunnel_id(&created).ok_or_else(|| {
                Error::Host(format!(
                    "devtunnel create printed no tunnel ID: {}",
                    created.trim()
                ))
            })?;
            let port = PORT.to_string();
            if let Err(err) = output("devtunnel", &["port", "create", &id, "-p", &port]).await {
                let _ = delete(provider, Some(&id)).await;
                return Err(err);
            }
            Ok(Some(id))
        }
    }
}

/// Deletes what [`create`] made.
///
/// # Errors
///
/// Returns an error when the CLI fails.
pub async fn delete(provider: Provider, id: Option<&str>) -> Result<(), Error> {
    match (provider, id) {
        (Provider::Devtunnel, Some(id)) => output("devtunnel", &["delete", id, "--force"])
            .await
            .map(drop),
        _ => Ok(()),
    }
}

/// Starts serving the dial-in port through the tunnel, returning once it is reachable, with its address.
///
/// # Errors
///
/// Returns an error when the CLI is missing, fails, or does not become ready.
pub async fn serve(provider: Provider, id: Option<&str>) -> Result<(Running, Addr), Error> {
    match (provider, id) {
        (Provider::Cloudflare, _) => {
            let url = format!("tcp://127.0.0.1:{PORT}");
            let (mut hostname, mut registered) = (None, false);
            let (running, name) = start(
                "cloudflared",
                &["tunnel", "--no-autoupdate", "--url", &url],
                |line| {
                    hostname = hostname.take().or_else(|| quick_tunnel(line));
                    registered |= line.contains("Registered tunnel connection");
                    hostname.clone().filter(|_| registered)
                },
            )
            .await?;
            Ok((running, Addr::Tunnel { provider, name }))
        }
        (Provider::Devtunnel, Some(id)) => {
            let (running, ()) = start("devtunnel", &["host", id], |line| {
                line.contains("Ready to accept connections").then_some(())
            })
            .await?;
            Ok((
                running,
                Addr::Tunnel {
                    provider,
                    name: id.to_owned(),
                },
            ))
        }
        (Provider::Devtunnel, None) => Err(Error::Invalid("a dev tunnel needs an ID".to_owned())),
    }
}

/// Starts the provider's local forwarder to the tunnel `name`, returning the local address that reaches the
/// initiator through it.
///
/// # Errors
///
/// Returns an error when the CLI is missing, fails, or does not start forwarding.
pub async fn forward(provider: Provider, name: &str) -> Result<(Running, SocketAddr), Error> {
    match provider {
        Provider::Cloudflare => {
            let local = SocketAddr::from((
                Ipv4Addr::LOCALHOST,
                std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?
                    .local_addr()?
                    .port(),
            ));
            let url = local.to_string();
            let (running, ()) = start(
                "cloudflared",
                &["access", "tcp", "--hostname", name, "--url", &url],
                |line| line.contains("Start Websocket listener").then_some(()),
            )
            .await?;
            Ok((running, local))
        }
        // `devtunnel connect` picks the local port itself: the remote one when it is free, otherwise the next. It
        // reuses addresses, so on macOS it would take `127.0.0.1:7420` from under an `ovrd` on this machine that
        // listens on every address, and dial-ins to it would loop back into the tunnel. Holding the port keeps it off.
        Provider::Devtunnel => {
            let held = hold_loopback(PORT);
            // It reports the port before it can forward; a connection that early ends its accept loop for good.
            let mut forwarding = None;
            let started = start("devtunnel", &["connect", name], |line| {
                forwarding = forwarding.or_else(|| devtunnel_forwarding(line));
                forwarding.filter(|local| devtunnel_listening(line, *local))
            })
            .await;
            drop(held);
            started
        }
    }
}

/// Listens on `127.0.0.1:port` alongside whatever else may, when the platform allows it.
fn hold_loopback(port: u16) -> Option<tokio::net::TcpListener> {
    let socket = tokio::net::TcpSocket::new_v4().ok()?;
    socket.set_reuseaddr(true).ok()?;
    socket.bind((Ipv4Addr::LOCALHOST, port).into()).ok()?;
    socket.listen(1).ok()
}

/// Starts `program`, collecting its output until `ready` finds what it is waiting for.
async fn start<T>(
    program: &'static str,
    args: &[&str],
    mut ready: impl FnMut(&str) -> Option<T>,
) -> Result<(Running, T), Error> {
    let mut child = command(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|err| missing(program, &err))?;
    let tail = Arc::new(Mutex::new(VecDeque::new()));
    let (lines, mut received) = mpsc::unbounded_channel();
    if let Some(stdout) = child.stdout.take() {
        tokio::spawn(collect(program, stdout, Arc::clone(&tail), lines.clone()));
    }
    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(collect(program, stderr, Arc::clone(&tail), lines));
    }
    let found = tokio::time::timeout(READY_TIMEOUT, async {
        while let Some(line) = received.recv().await {
            if let Some(found) = ready(&line) {
                return Some(found);
            }
        }
        None
    })
    .await;
    match found {
        Ok(Some(found)) => Ok((
            Running {
                program,
                child,
                tail,
            },
            found,
        )),
        Ok(None) => {
            let status = child.wait().await;
            Err(Error::Host(describe(program, status, &tail)))
        }
        Err(_) => Err(Error::Host(format!(
            "{program} was not ready after {}s:\n{}",
            READY_TIMEOUT.as_secs(),
            joined(&tail)
        ))),
    }
}

/// Logs each line of `output`, keeps the last few, and passes them on while anyone listens.
async fn collect(
    program: &'static str,
    output: impl AsyncRead + Unpin,
    tail: Arc<Mutex<VecDeque<String>>>,
    lines: mpsc::UnboundedSender<String>,
) {
    let mut output = BufReader::new(output).lines();
    while let Ok(Some(line)) = output.next_line().await {
        tracing::debug!(program, "{line}");
        {
            let mut tail = tail.lock().unwrap_or_else(PoisonError::into_inner);
            if tail.len() == TAIL {
                tail.pop_front();
            }
            tail.push_back(line.clone());
        }
        let _ = lines.send(line);
    }
}

/// Runs `program` to completion, returning its standard output.
async fn output(program: &'static str, args: &[&str]) -> Result<String, Error> {
    let output = tokio::time::timeout(
        READY_TIMEOUT,
        command(program)
            .args(args)
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| Error::Host(format!("{program} {} timed out", args.join(" "))))?
    .map_err(|err| missing(program, &err))?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    if output.status.success() {
        Ok(stdout)
    } else {
        Err(Error::Host(format!(
            "{program} {} failed ({}): {} {}",
            args.first().copied().unwrap_or_default(),
            output.status,
            stdout.trim(),
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

fn command(program: &str) -> Command {
    #[allow(unused_mut)]
    let mut command = Command::new(program);
    #[cfg(windows)]
    {
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NO_WINDOW);
    }
    command
}

fn missing(program: &str, err: &std::io::Error) -> Error {
    if err.kind() == std::io::ErrorKind::NotFound {
        Error::Host(format!(
            "{program} is not installed; ovrd uses it for tunnels"
        ))
    } else {
        Error::Host(format!("cannot run {program}: {err}"))
    }
}

fn describe(
    program: &str,
    status: std::io::Result<ExitStatus>,
    tail: &Mutex<VecDeque<String>>,
) -> String {
    let status = status.map_or_else(|err| err.to_string(), |status| status.to_string());
    format!("{program} exited ({status}):\n{}", joined(tail))
}

fn joined(tail: &Mutex<VecDeque<String>>) -> String {
    let tail = tail.lock().unwrap_or_else(PoisonError::into_inner);
    tail.iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join("\n")
}

/// The hostname in `cloudflared`'s announcement of a quick tunnel.
fn quick_tunnel(line: &str) -> Option<String> {
    let start = line.find("https://")? + "https://".len();
    let hostname: String = line[start..]
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.'))
        .collect();
    hostname.ends_with(".trycloudflare.com").then_some(hostname)
}

/// The tunnel ID in `devtunnel create --json` output.
fn devtunnel_id(output: &str) -> Option<String> {
    let json: serde_json::Value = serde_json::from_str(&output[output.find('{')?..]).ok()?;
    Some(json.get("tunnel")?.get("tunnelId")?.as_str()?.to_owned())
}

/// The local address in `devtunnel connect`'s report that it forwards the dial-in port.
fn devtunnel_forwarding(line: &str) -> Option<SocketAddr> {
    let rest =
        &line[line.find("Forwarding from 127.0.0.1:")? + "Forwarding from 127.0.0.1:".len()..];
    let (port, remote) = rest.split_once(' ')?;
    (remote.trim_end_matches('.') == format!("to host port {PORT}"))
        .then(|| port.parse().ok())
        .flatten()
        .map(|port| SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}

/// Whether `line` is `devtunnel connect`'s report that it accepts connections on `local`.
fn devtunnel_listening(line: &str, local: SocketAddr) -> bool {
    line.trim_end_matches('.')
        .ends_with(&format!("listening on {local}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_name_a_tunnel_only_with_a_provider_and_a_name() {
        let tunnel = |provider, name: &str| {
            Ok(Addr::Tunnel {
                provider,
                name: name.to_owned(),
            })
        };
        assert_eq!(
            "cloudflare:quiet-river-delta.trycloudflare.com".parse(),
            tunnel(Provider::Cloudflare, "quiet-river-delta.trycloudflare.com")
        );
        assert_eq!(
            "devtunnel:neat-lake-mwft94l.usw3".parse(),
            tunnel(Provider::Devtunnel, "neat-lake-mwft94l.usw3")
        );
        assert_eq!("box".parse(), Ok(Addr::Direct(format!("box:{PORT}"))));
        assert_eq!("box:22".parse(), Ok(Addr::Direct("box:22".to_owned())));
        assert_eq!(
            "cloudflare:7420".parse(),
            Ok(Addr::Direct("cloudflare:7420".to_owned()))
        );
        assert!("cloudflare:--url=x".parse::<Addr>().is_err());
        assert!("devtunnel:a b".parse::<Addr>().is_err());
        assert!("-oProxyCommand=x".parse::<Addr>().is_err());
        assert_eq!(
            "cloudflare:a.trycloudflare.com"
                .parse::<Addr>()
                .map(|addr| addr.to_string()),
            Ok("cloudflare:a.trycloudflare.com".to_owned())
        );
    }

    #[test]
    fn reads_what_the_provider_clis_print() {
        assert_eq!(
            quick_tunnel(
                "2026-11-07T03:22:08Z INF |  https://delivery-suddenly-only-instant.trycloudflare.com      |"
            )
            .as_deref(),
            Some("delivery-suddenly-only-instant.trycloudflare.com")
        );
        assert_eq!(
            quick_tunnel("INF Thank you ... (https://www.cloudflare.com/website-terms/)"),
            None
        );
        assert_eq!(
            devtunnel_id(
                "{\n  \"tunnel\": {\n    \"tunnelId\": \"neat-lake-mwft94l.usw3\"\n  }\n}\n"
            )
            .as_deref(),
            Some("neat-lake-mwft94l.usw3")
        );
        assert_eq!(
            devtunnel_forwarding(&format!(
                "SSH: Forwarding from 127.0.0.1:17999 to host port {PORT}."
            )),
            Some(SocketAddr::from((Ipv4Addr::LOCALHOST, 17999)))
        );
        let local = SocketAddr::from((Ipv4Addr::LOCALHOST, 7421));
        assert!(devtunnel_listening(
            "SSH: PortForwardingService listening on 127.0.0.1:7421.",
            local
        ));
        assert!(!devtunnel_listening(
            "SSH: PortForwardingService also listening on ::1:7421.",
            local
        ));
        assert_eq!(
            devtunnel_forwarding("SSH: Forwarding from 127.0.0.1:3000 to host port 3000."),
            None
        );
        assert_eq!(
            devtunnel_forwarding(&format!(
                "SSH: Forwarding from [::1]:7420 to host port {PORT}."
            )),
            None
        );
    }
}
