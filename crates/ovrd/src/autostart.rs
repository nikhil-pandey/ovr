//! Starting `ovrd connect --service` at login, as the logged-in user: a launchd agent on macOS, a systemd user
//! unit on Linux, a Scheduled Task on Windows.

use std::io;
use std::path::Path;
use std::process::Command;

#[cfg(not(target_os = "macos"))]
const NAME: &str = "ovrd";

/// Installs and starts the service, replacing an earlier one.
///
/// # Errors
///
/// Returns an error when the service manager is unavailable or rejects it.
pub fn install() -> io::Result<()> {
    let exe = std::env::current_exe()?;
    platform::install(&exe)
}

fn run(program: &str, args: &[&str]) -> io::Result<()> {
    let output = Command::new(program).args(args).output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{program} {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::{Path, io, run};

    const LABEL: &str = "dev.ovr.connect";

    pub fn install(exe: &Path) -> io::Result<()> {
        let dir = ovr_proto::home_dir()?.join("Library/LaunchAgents");
        std::fs::create_dir_all(&dir)?;
        let plist = dir.join(format!("{LABEL}.plist"));
        let log = ovr_proto::state_dir()?.join("connect.log");
        std::fs::write(
            &plist,
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{LABEL}</string>
  <key>ProgramArguments</key>
  <array><string>{exe}</string><string>connect</string><string>--service</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardErrorPath</key><string>{log}</string>
</dict>
</plist>
"#,
                exe = escape(&exe.to_string_lossy()),
                log = escape(&log.to_string_lossy()),
            ),
        )?;
        let domain = format!("gui/{}", nix::unistd::getuid());
        // Not loaded yet on first install.
        let _ = run("launchctl", &["bootout", &format!("{domain}/{LABEL}")]);
        run(
            "launchctl",
            &["bootstrap", &domain, &plist.to_string_lossy()],
        )
    }

    fn escape(value: &str) -> String {
        value
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
mod platform {
    use super::{NAME, Path, io, run};

    pub fn install(exe: &Path) -> io::Result<()> {
        let config = std::env::var_os("XDG_CONFIG_HOME")
            .map(std::path::PathBuf::from)
            .filter(|dir| dir.is_absolute())
            .map_or_else(
                || ovr_proto::home_dir().map(|home| home.join(".config")),
                Ok,
            )?;
        let dir = config.join("systemd/user");
        std::fs::create_dir_all(&dir)?;
        std::fs::write(
            dir.join(format!("{NAME}.service")),
            format!(
                "[Unit]\nDescription=ovr host agent\nAfter=network-online.target\n\n\
                 [Service]\nExecStart=\"{}\" connect --service\nRestart=always\nRestartSec=5\n\n\
                 [Install]\nWantedBy=default.target\n",
                exe.display()
            ),
        )?;
        run("systemctl", &["--user", "daemon-reload"])?;
        run("systemctl", &["--user", "enable", NAME])?;
        run("systemctl", &["--user", "restart", NAME])
    }
}

#[cfg(windows)]
mod platform {
    use super::{NAME, Path, io, run};

    pub fn install(exe: &Path) -> io::Result<()> {
        let command = format!("\"{}\" connect --service", exe.display());
        run(
            "schtasks",
            &[
                "/Create", "/F", "/SC", "ONLOGON", "/RL", "LIMITED", "/TN", NAME, "/TR", &command,
            ],
        )?;
        // Replace a running instance from an earlier install.
        let _ = run("schtasks", &["/End", "/TN", NAME]);
        run("schtasks", &["/Run", "/TN", NAME])
    }
}
