//! Tunnels to the dial-in port that the initiator keeps open: opened by `ovrd pair PROVIDER`, restarted whenever the
//! provider's CLI exits, reopened whenever `ovrd` starts, and closed only by `ovrd tunnel close PROVIDER`.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use ovr_proto::msg::{Provider, TunnelStatus};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::tunnel::{self, Addr, Running};
use crate::{Error, state};

/// How long `ovrd pair` waits for a tunnel that is already open but restarting.
const RESTART_WAIT: Duration = Duration::from_secs(60);

/// Restart delays, and how long a tunnel must stay up for them to start over.
const MIN_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_mins(1);
const STABLE: Duration = Duration::from_mins(1);

pub struct Tunnels {
    dir: PathBuf,
    open: Mutex<HashMap<Provider, Open>>,
    /// Opening and closing one at a time keeps the records and the provider in step.
    changing: tokio::sync::Mutex<()>,
}

/// A tunnel being kept open by its supervising task, which ends, killing the provider's CLI, when this is dropped.
struct Open {
    record: state::Tunnel,
    addr: watch::Receiver<Option<Addr>>,
    task: JoinHandle<()>,
}

impl Drop for Open {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Tunnels {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            open: Mutex::default(),
            changing: tokio::sync::Mutex::default(),
        }
    }

    /// Reopens the tunnels recorded by earlier runs of `ovrd`.
    ///
    /// # Errors
    ///
    /// Returns an error when the records cannot be read.
    pub fn restore(&self) -> io::Result<()> {
        let mut open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        for record in state::tunnels(&self.dir)? {
            tracing::info!(provider = %record.provider, "reopening tunnel");
            open.insert(record.provider, keep(record, None));
        }
        Ok(())
    }

    /// The address of the tunnel from `provider`, opened first unless it already is.
    ///
    /// # Errors
    ///
    /// Returns an error when the tunnel cannot be opened, or is open but does not come back up in time.
    pub async fn open(&self, provider: Provider) -> Result<Addr, Error> {
        let changing = self.changing.lock().await;
        let existing = self
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&provider)
            .map(|open| open.addr.clone());
        if let Some(mut addr) = existing {
            drop(changing);
            let up = tokio::time::timeout(RESTART_WAIT, addr.wait_for(Option::is_some)).await;
            return match up {
                Ok(Ok(addr)) => addr.clone().ok_or(Error::Disconnected),
                _ => Err(Error::Host(format!(
                    "the {provider} tunnel is restarting and not up yet; see ovrd's log"
                ))),
            };
        }

        let id = tunnel::create(provider).await?;
        let record = state::Tunnel { provider, id };
        let opened = async {
            let (running, addr) = tunnel::serve(provider, record.id.as_deref()).await?;
            let mut records = state::tunnels(&self.dir)?;
            records.retain(|known| known.provider != provider);
            records.push(record.clone());
            records.sort_by_key(|known| known.provider);
            state::set_tunnels(&self.dir, &records)?;
            Ok::<_, Error>((running, addr))
        }
        .await;
        let (running, addr) = match opened {
            Ok(opened) => opened,
            Err(err) => {
                let _ = tunnel::delete(provider, record.id.as_deref()).await;
                return Err(err);
            }
        };
        tracing::info!(%addr, "tunnel open");
        self.open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(provider, keep(record, Some((running, addr.clone()))));
        Ok(addr)
    }

    pub fn list(&self) -> Vec<TunnelStatus> {
        let open = self.open.lock().unwrap_or_else(PoisonError::into_inner);
        let mut tunnels: Vec<TunnelStatus> = open
            .iter()
            .map(|(provider, open)| TunnelStatus {
                provider: *provider,
                addr: open.addr.borrow().as_ref().map(ToString::to_string),
            })
            .collect();
        tunnels.sort_by_key(|tunnel| tunnel.provider);
        tunnels
    }

    /// Closes the tunnel from `provider` and deletes what was created for it there.
    ///
    /// # Errors
    ///
    /// Returns an error when no such tunnel is open or the provider's CLI fails to delete it.
    pub async fn close(&self, provider: Provider) -> Result<(), Error> {
        let _changing = self.changing.lock().await;
        let open = self
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&provider)
            .ok_or_else(|| Error::Invalid(format!("no {provider} tunnel is open")))?;
        let mut records = state::tunnels(&self.dir)?;
        records.retain(|known| known.provider != provider);
        state::set_tunnels(&self.dir, &records)?;
        let id = open.record.id.clone();
        drop(open);
        tunnel::delete(provider, id.as_deref()).await?;
        tracing::info!(%provider, "tunnel closed");
        Ok(())
    }
}

/// Starts keeping `record` open, from an already running CLI if given.
fn keep(record: state::Tunnel, running: Option<(Running, Addr)>) -> Open {
    let (addr, receiver) = watch::channel(running.as_ref().map(|(_, addr)| addr.clone()));
    let task = tokio::spawn(supervise(
        record.clone(),
        running.map(|(running, _)| running),
        addr,
    ));
    Open {
        record,
        addr: receiver,
        task,
    }
}

/// Runs the provider's CLI forever, restarting it with backoff whenever it exits or fails to start.
async fn supervise(
    record: state::Tunnel,
    mut running: Option<Running>,
    addr: watch::Sender<Option<Addr>>,
) {
    let provider = record.provider;
    let mut backoff = MIN_BACKOFF;
    loop {
        let mut cli = match running.take() {
            Some(cli) => cli,
            None => match tunnel::serve(provider, record.id.as_deref()).await {
                Ok((cli, opened)) => {
                    tracing::info!(addr = %opened, "tunnel open");
                    addr.send_replace(Some(opened));
                    cli
                }
                Err(err) => {
                    tracing::warn!(%provider, %err, "cannot open tunnel; retrying in {}s", backoff.as_secs());
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }
            },
        };
        let started = Instant::now();
        let how = cli.exited().await;
        addr.send_replace(None);
        if started.elapsed() >= STABLE {
            backoff = MIN_BACKOFF;
        }
        tracing::warn!(%provider, "{how}\nreopening the tunnel in {}s", backoff.as_secs());
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}
