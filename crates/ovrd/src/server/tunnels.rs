//! Tunnels to the dial-in port that the initiator keeps open: opened by `ovrd pair PROVIDER`, restarted whenever the
//! provider's CLI exits, reopened whenever `ovrd` starts, and closed only by `ovrd tunnel close PROVIDER`.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
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
    records: Arc<Records>,
    open: Mutex<HashMap<Provider, Open>>,
    /// Opening and closing one at a time keeps the records and the provider in step.
    changing: tokio::sync::Mutex<()>,
}

/// The record of open tunnels in the data directory, written by one task at a time.
struct Records {
    dir: PathBuf,
    writing: Mutex<()>,
}

impl Records {
    fn save(&self, record: &state::Tunnel) -> io::Result<()> {
        let _writing = self.writing.lock().unwrap_or_else(PoisonError::into_inner);
        let mut records = state::tunnels(&self.dir)?;
        records.retain(|known| known.provider != record.provider);
        records.push(record.clone());
        records.sort_by_key(|known| known.provider);
        state::set_tunnels(&self.dir, &records)
    }

    fn remove(&self, provider: Provider) -> io::Result<()> {
        let _writing = self.writing.lock().unwrap_or_else(PoisonError::into_inner);
        let mut records = state::tunnels(&self.dir)?;
        records.retain(|known| known.provider != provider);
        state::set_tunnels(&self.dir, &records)
    }
}

/// A tunnel being kept open by its supervising task, which ends, killing the provider's CLI, when this is dropped.
struct Open {
    /// Shared with the task, which replaces it should the provider lose the tunnel.
    record: Arc<Mutex<state::Tunnel>>,
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
            records: Arc::new(Records {
                dir,
                writing: Mutex::default(),
            }),
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
        for record in state::tunnels(&self.records.dir)? {
            tracing::info!(provider = %record.provider, "reopening tunnel");
            open.insert(record.provider, keep(&self.records, record, None));
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
        // Recorded before anything else can fail, so a tunnel created at the provider is never forgotten: should
        // `ovrd` stop before it is served, the next start reopens it.
        let opened = match self.records.save(&record) {
            Ok(()) => tunnel::serve(provider, record.id.as_deref()).await,
            Err(err) => Err(err.into()),
        };
        let (running, addr) = match opened {
            Ok(opened) => opened,
            Err(err) => {
                match tunnel::delete(provider, record.id.as_deref()).await {
                    Ok(()) => {
                        if let Err(err) = self.records.remove(provider) {
                            tracing::warn!(%provider, %err, "cannot update the tunnel records");
                        }
                    }
                    // Kept, and retried, rather than forgotten at the provider; `ovrd tunnel close` deletes it.
                    Err(err) => {
                        tracing::warn!(%provider, %err, "cannot delete the tunnel; keeping it open");
                        self.open
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .insert(provider, keep(&self.records, record, None));
                    }
                }
                return Err(err);
            }
        };
        tracing::info!(%addr, "tunnel open");
        self.open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                provider,
                keep(&self.records, record, Some((running, addr.clone()))),
            );
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
    /// Returns an error when no such tunnel is open or the provider's CLI fails to delete it, in which case the
    /// tunnel stays open.
    pub async fn close(&self, provider: Provider) -> Result<(), Error> {
        let _changing = self.changing.lock().await;
        let mut open = self
            .open
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&provider)
            .ok_or_else(|| Error::Invalid(format!("no {provider} tunnel is open")))?;
        // Stopped first, so it neither serves nor replaces what is about to be deleted.
        open.task.abort();
        let _ = (&mut open.task).await;
        let record = open
            .record
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        drop(open);
        if let Err(err) = tunnel::delete(provider, record.id.as_deref()).await {
            self.open
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(provider, keep(&self.records, record, None));
            return Err(err);
        }
        self.records.remove(provider)?;
        tracing::info!(%provider, "tunnel closed");
        Ok(())
    }
}

/// Starts keeping `record` open, from an already running CLI if given.
fn keep(records: &Arc<Records>, record: state::Tunnel, running: Option<(Running, Addr)>) -> Open {
    let (addr, receiver) = watch::channel(running.as_ref().map(|(_, addr)| addr.clone()));
    let record = Arc::new(Mutex::new(record));
    let task = tokio::spawn(supervise(
        Arc::clone(records),
        Arc::clone(&record),
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
    records: Arc<Records>,
    record: Arc<Mutex<state::Tunnel>>,
    mut running: Option<Running>,
    addr: watch::Sender<Option<Addr>>,
) {
    let current = || {
        record
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    };
    let provider = current().provider;
    let mut backoff = MIN_BACKOFF;
    loop {
        let mut cli = match running.take() {
            Some(cli) => cli,
            None => match tunnel::serve(provider, current().id.as_deref()).await {
                Ok((cli, opened)) => {
                    tracing::info!(addr = %opened, "tunnel open");
                    addr.send_replace(Some(opened));
                    cli
                }
                Err(err) => {
                    if replace(&records, &record).await {
                        continue;
                    }
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

/// Creates a new tunnel in place of one the provider no longer has, as when a Dev Tunnel expires unused, returning
/// whether it did. Paired hosts keep dialing the old address until moved to the new one.
async fn replace(records: &Records, record: &Mutex<state::Tunnel>) -> bool {
    let old = record
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    if !matches!(
        tunnel::exists(old.provider, old.id.as_deref()).await,
        Ok(false)
    ) {
        return false;
    }
    let new = match tunnel::create(old.provider).await {
        Ok(id) => state::Tunnel {
            provider: old.provider,
            id,
        },
        Err(err) => {
            tracing::warn!(provider = %old.provider, %err, "the tunnel is gone and cannot be recreated");
            return false;
        }
    };
    if let Err(err) = records.save(&new) {
        tracing::warn!(provider = %new.provider, %err, "cannot record the recreated tunnel");
        let _ = tunnel::delete(new.provider, new.id.as_deref()).await;
        return false;
    }
    tracing::warn!(
        provider = %new.provider,
        old = old.id.as_deref().unwrap_or_default(),
        new = new.id.as_deref().unwrap_or_default(),
        "the tunnel was gone, so a new one replaces it; `ovrd tunnel` shows the command that moves paired hosts to it"
    );
    *record.lock().unwrap_or_else(PoisonError::into_inner) = new;
    true
}
