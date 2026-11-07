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

/// The installing shell's `PATH`, so the service finds tunnel CLIs installed by Homebrew or into the user's home.
#[cfg(unix)]
fn path() -> String {
    std::env::var_os("PATH")
        .map(|path| path.to_string_lossy().into_owned())
        .filter(|path| !path.is_empty())
        .unwrap_or_else(|| "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin".to_owned())
}

/// The installing shell's `PATH` and the directory overrides it sets, which the service keeps; service managers
/// otherwise start it with a minimal environment. A Scheduled Task already gets the user's.
#[cfg(unix)]
fn environment() -> Vec<(&'static str, String)> {
    let overrides = ovr_proto::paths::OVERRIDES.into_iter().filter_map(|var| {
        let value = std::env::var(var).ok()?;
        Path::new(&value).is_absolute().then_some((var, value))
    });
    std::iter::once(("PATH", path())).chain(overrides).collect()
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
    use std::fmt::Write;

    use super::{Path, environment, io, run};

    const LABEL: &str = "dev.ovr.connect";

    pub fn install(exe: &Path) -> io::Result<()> {
        let dir = ovr_proto::paths::home_dir()?.join("Library/LaunchAgents");
        std::fs::create_dir_all(&dir)?;
        let plist = dir.join(format!("{LABEL}.plist"));
        let log = ovr_proto::paths::log_dir()?.join("connect.log");
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
  <key>EnvironmentVariables</key>
  <dict>{environment}</dict>
  <key>StandardErrorPath</key><string>{log}</string>
</dict>
</plist>
"#,
                exe = escape(&exe.to_string_lossy()),
                log = escape(&log.to_string_lossy()),
                environment = environment()
                    .iter()
                    .fold(String::new(), |mut out, (key, value)| {
                        let _ = write!(out, "<key>{key}</key><string>{}</string>", escape(value));
                        out
                    }),
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
    use std::fmt::Write;

    use super::{NAME, Path, environment, io, run};

    pub fn install(exe: &Path) -> io::Result<()> {
        let dir = directories::BaseDirs::new()
            .ok_or_else(|| io::Error::other("cannot determine the home directory"))?
            .config_dir()
            .join("systemd/user");
        std::fs::create_dir_all(&dir)?;
        std::fs::write(
            dir.join(format!("{NAME}.service")),
            format!(
                "[Unit]\nDescription=ovr host agent\nAfter=network-online.target\n\n\
                 [Service]\nExecStart=\"{exe}\" connect --service\n{environment}\
                 Restart=always\nRestartSec=5\nKillMode=process\n\n\
                 [Install]\nWantedBy=default.target\n",
                exe = exe.display(),
                environment = environment()
                    .iter()
                    .fold(String::new(), |mut out, (key, value)| {
                        let value = value
                            .replace('\\', "\\\\")
                            .replace('"', "\\\"")
                            .replace('%', "%%");
                        let _ = writeln!(out, "Environment=\"{key}={value}\"");
                        out
                    }),
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
