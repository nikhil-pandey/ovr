//! Release targets, for picking the `ovrd` binary to install on an SSH host.

/// The release target this binary was built for.
#[must_use]
pub fn current() -> Option<&'static str> {
    triple(std::env::consts::OS, std::env::consts::ARCH)
}

/// The release target for `uname -sm` output such as `Linux x86_64`.
#[must_use]
pub fn from_uname(uname: &str) -> Option<&'static str> {
    let mut words = uname.split_whitespace();
    let os = match words.next()? {
        "Linux" => "linux",
        "Darwin" => "macos",
        _ => return None,
    };
    let arch = match words.next()? {
        "x86_64" | "amd64" => "x86_64",
        "aarch64" | "arm64" => "aarch64",
        _ => return None,
    };
    triple(os, arch)
}

fn triple(os: &str, arch: &str) -> Option<&'static str> {
    Some(match (os, arch) {
        ("linux", "x86_64") => "x86_64-unknown-linux-musl",
        ("linux", "aarch64") => "aarch64-unknown-linux-musl",
        ("macos", "x86_64") => "x86_64-apple-darwin",
        ("macos", "aarch64") => "aarch64-apple-darwin",
        ("windows", "x86_64") => "x86_64-pc-windows-gnu",
        ("windows", "aarch64") => "aarch64-pc-windows-gnullvm",
        _ => return None,
    })
}

/// Where the release archive for `target` is published.
#[must_use]
pub fn release_url(version: &str, target: &str) -> String {
    format!("https://github.com/nikhil-pandey/ovr/releases/download/v{version}/ovr-{target}.tar.gz")
}
