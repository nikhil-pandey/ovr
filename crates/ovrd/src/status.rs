//! `ovrd status`: this host's state, read from its own files. Nothing is checked, contacted, started, or changed.

use ovr_proto::lock::Lock;
use ovr_proto::msg::Location;
use ovr_proto::{VERSION, format_age, paths, print_field as field};

use crate::agent::runs::{Store, now};
use crate::host::Connection;
use crate::tls::Identity;
use crate::{Error, agent, autostart, host, state};

/// Prints who this host is, the initiator it serves and whether it is connected, the login service, where its log
/// and checkouts are, and the runs here from every initiator.
///
/// # Errors
///
/// Returns an error when this host's own files cannot be read.
pub fn status() -> Result<(), Error> {
    let dir = paths::data_dir()?;
    let now = now();
    println!("ovrd {VERSION}");

    let hostname = agent::hostname();
    field(
        "host",
        [match Identity::load(&dir)? {
            Some(identity) => format!("{hostname}  {}", identity.fingerprint),
            None => hostname,
        }],
    );
    field(
        "initiator",
        [match state::initiator(&dir)? {
            Some(initiator) => format!("{}  {}", initiator.addr, initiator.fingerprint),
            None => {
                "not paired; run the `ovrd connect ADDR CODE` command `ovr pair` prints".to_owned()
            }
        }],
    );
    // `ovrd connect` holds the lock while it serves; this lets go of it at once. Without it, `connect.json` is
    // whatever an earlier one left behind.
    let running = Lock::try_exclusive(&dir.join("connect.lock"))?.is_none();
    let connection = if running { host::connection()? } else { None };
    field(
        "serving",
        [match (running, connection) {
            (false, _) => "not running".to_owned(),
            (true, None) => "running".to_owned(),
            (true, Some(Connection::Connected { since })) => {
                format!("connected for {}", format_age(now.saturating_sub(since)))
            }
            (true, Some(Connection::Retrying { since, error })) => format!(
                "not connected for {}{}",
                format_age(now.saturating_sub(since)),
                error.map_or_default(|error| format!(": {error}"))
            ),
        }],
    );
    field(
        "service",
        [match autostart::installed() {
            Ok(Some(service)) => service,
            Ok(None) => "not installed".to_owned(),
            Err(err) => format!("cannot tell: {err}"),
        }],
    );
    let log = paths::log_dir()?.join("connect.log");
    field("log", [Location::new("log", &log, "OVR_STATE_DIR")]);
    let checkouts = paths::checkouts_dir()?;
    field(
        "checkouts",
        [format!(
            "{}  {}",
            Location::new("checkouts", &checkouts, "OVR_CHECKOUTS"),
            ovr_proto::format_bytes(agent::clean::size(&checkouts))
        )],
    );

    let mut runs = Store::new(paths::runs_dir()?).all()?;
    runs.sort_by_key(|run| run.id);
    let header = ["RUN", "INITIATOR", "STATE", "AGE", "COMMAND"].map(str::to_owned);
    // A run whose files cannot be read is listed with `?` for what is missing.
    let rows: Vec<[String; 5]> = std::iter::once(header)
        .chain(runs.into_iter().map(|run| {
            let unknown = || "?".to_owned();
            let meta = run.meta().ok();
            [
                run.id.to_string(),
                meta.as_ref()
                    .map_or_else(unknown, |meta| meta.initiator.chars().take(12).collect()),
                run.peek()
                    .map_or_else(|_| unknown(), |state| state.to_string()),
                meta.as_ref()
                    .map_or_else(unknown, |meta| format_age(now.saturating_sub(meta.started))),
                meta.map_or_else(unknown, |meta| {
                    if meta.exec.argv.is_empty() {
                        "(shell)".to_owned()
                    } else {
                        meta.exec.argv.join(" ")
                    }
                }),
            ]
        }))
        .collect();
    // The header alone means no runs.
    field(
        "runs",
        if rows.len() > 1 {
            ovr_proto::columns(&rows)
        } else {
            Vec::new()
        },
    );
    Ok(())
}
