//! Managing who may dial in to this initiator: pairing codes, trust rules, and tunnels.

use ovr_proto::VERSION;
use ovr_proto::msg::{Op, Provider, Response};

use crate::{Error, daemon};

/// `ovr pair`: asks the server for a pairing code, through a tunnel from `tunnel` if given.
pub async fn pair(tunnel: Option<Provider>) -> Result<i32, Error> {
    if let Some(provider) = tunnel {
        eprintln!("opening a {provider} tunnel unless one is open...");
    }
    match daemon::request(Op::Pair { tunnel }).await? {
        (_, Response::Pair { addr, code }) => {
            println!("run this on the host within 10 minutes:\n  ovrd connect {addr} {code}");
            Ok(0)
        }
        (_, other) => Err(daemon::unexpected(other)),
    }
}

/// `ovr trust` and `ovr untrust`: changes the rules, if asked to, then lists them and how a workload dials in.
pub async fn trust(op: Op) -> Result<i32, Error> {
    let changed = match &op {
        Op::Trust { rule: Some(rule) } => Some(format!("trusting {rule}")),
        Op::Untrust { rule } => Some(format!("no longer trusting {rule}")),
        _ => None,
    };
    let (rules, initiator, addr) = match daemon::request(op).await? {
        (
            _,
            Response::Trust {
                rules,
                initiator,
                addr,
            },
        ) => (rules, initiator, addr),
        (_, other) => return Err(daemon::unexpected(other)),
    };
    if let Some(changed) = changed {
        println!("{changed}");
    }
    if rules.is_empty() {
        println!(
            "no workload is trusted; `ovr trust github:OWNER/REPO` trusts a repository's jobs"
        );
        return Ok(0);
    }
    println!("trusted:");
    for rule in &rules {
        println!("  {rule}");
    }
    let repo = rules
        .iter()
        .find_map(|rule| {
            rule.strip_prefix("github:")
                .filter(|spec| spec.contains('/'))
        })
        .unwrap_or("OWNER/REPO");
    println!(
        "\nstore where this machine is as repository variables (or organization variables, with --org):\n  \
         gh variable set OVR_ADDR --repo {repo} --body {addr}\n  \
         gh variable set OVR_INITIATOR --repo {repo} --body {initiator}\n\n\
         then add a step to a job with `permissions: id-token: write`:\n  \
         - if: failure() || runner.debug == '1'\n    \
         uses: nikhil-pandey/ovr@v{VERSION}\n    \
         with:\n      \
         addr: ${{{{ vars.OVR_ADDR }}}}\n      \
         initiator: ${{{{ vars.OVR_INITIATOR }}}}\n      \
         hold: 30m\n\n\
         or run `ovrd connect {addr} github:{initiator}` in any step"
    );
    if !addr.starts_with("devtunnel:") && !addr.starts_with("tailcat:") {
        println!(
            "\nGitHub-hosted runners reach {addr} only through a tunnel or your tailnet; \
             `ovr pair devtunnel` opens a tunnel whose address stays the same"
        );
    }
    Ok(0)
}

/// `ovr tunnel`: lists open tunnels and the command that points a paired host at each.
pub async fn tunnels() -> Result<i32, Error> {
    let tunnels = match daemon::request(Op::Tunnels).await? {
        (_, Response::Tunnels { tunnels }) => tunnels,
        (_, other) => return Err(daemon::unexpected(other)),
    };
    if tunnels.is_empty() {
        println!("no tunnels are open; `ovr pair PROVIDER` opens one");
    }
    for tunnel in tunnels {
        match tunnel.addr {
            Some(addr) => println!("{:<10}  ovrd connect {addr}", tunnel.provider),
            None => println!("{:<10}  (restarting)", tunnel.provider),
        }
    }
    Ok(0)
}

/// `ovr tunnel close PROVIDER`.
pub async fn close_tunnel(provider: Provider) -> Result<i32, Error> {
    match daemon::request(Op::CloseTunnel { provider }).await? {
        (_, Response::TunnelClosed) => {
            println!("closed the {provider} tunnel");
            Ok(0)
        }
        (_, other) => Err(daemon::unexpected(other)),
    }
}
