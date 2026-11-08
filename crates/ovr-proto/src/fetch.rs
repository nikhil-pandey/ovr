//! Copying files back from a host: what both sides must agree on. Which sources are globs and what they match,
//! which entry paths a client may write, and when two modification times are the same.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use globset::{GlobBuilder, GlobMatcher};

/// Characters that make a source a glob.
const GLOB_CHARS: [char; 4] = ['*', '?', '[', '{'];

/// Whether `source` is a glob, matched by the agent rather than named.
#[must_use]
pub fn is_glob(source: &str) -> bool {
    source.contains(GLOB_CHARS)
}

/// A glob source: the literal directory it starts from and the pattern below it. Both `/` and `\` separate
/// components, on every platform, so that host and client split a source the same way; `\` never escapes.
#[derive(Debug, Clone)]
pub struct Glob {
    /// The leading components without glob characters, with `/` for every separator; empty for none.
    pub prefix: String,
    matcher: GlobMatcher,
    /// How many components below the prefix a match can have; `None` with `**`.
    depth: Option<usize>,
}

impl Glob {
    /// Splits `source` into its literal prefix and pattern, or `None` when it is not a glob.
    ///
    /// # Errors
    ///
    /// Returns a description of the problem when the pattern is invalid.
    pub fn parse(source: &str, case_insensitive: bool) -> Result<Option<Self>, String> {
        let mut start = 0;
        let mut prefix_len = None;
        for (at, ch) in source.char_indices().chain([(source.len(), '/')]) {
            if ch != '/' && ch != '\\' {
                continue;
            }
            if is_glob(&source[start..at]) {
                prefix_len = Some(start);
                break;
            }
            start = at + ch.len_utf8();
        }
        let Some(prefix_len) = prefix_len else {
            return Ok(None);
        };
        let parts: Vec<&str> = source[prefix_len..]
            .split(['/', '\\'])
            .filter(|part| !part.is_empty())
            .collect();
        let depth = (!parts.iter().any(|part| part.contains("**"))).then_some(parts.len());
        let matcher = GlobBuilder::new(&parts.join("/"))
            .literal_separator(true)
            .backslash_escape(false)
            .case_insensitive(case_insensitive)
            .build()
            .map_err(|err| err.kind().to_string())?
            .compile_matcher();
        Ok(Some(Self {
            prefix: source[..prefix_len].replace('\\', "/"),
            matcher,
            depth,
        }))
    }

    /// Whether `path`, `/`-separated below the prefix, matches.
    #[must_use]
    pub fn matches(&self, path: &str) -> bool {
        self.matcher.is_match(path)
    }

    /// Whether a directory `depth` components below the prefix can contain a match.
    #[must_use]
    pub fn descends(&self, depth: usize) -> bool {
        self.depth.is_none_or(|max| depth < max)
    }
}

/// Splits an entry path from a host into its components, refusing anything but plain names: no empty component,
/// `.`, or `..`, nothing absolute, and, for a Windows client, no `\`, `:`, other characters Windows forbids, trailing
/// dots or spaces, or reserved device names. The empty path is the source itself and has no components.
///
/// # Errors
///
/// Returns why the path is refused.
pub fn components(path: &str, windows: bool) -> Result<Vec<&str>, String> {
    if path.is_empty() {
        return Ok(Vec::new());
    }
    path.split('/')
        .map(|part| match refusal(part, windows) {
            Some(why) => Err(format!("refused the path {path:?} from the host: {why}")),
            None => Ok(part),
        })
        .collect()
}

fn refusal(part: &str, windows: bool) -> Option<&'static str> {
    if part.is_empty() {
        return Some("it has an empty component or is absolute");
    }
    if part == "." || part == ".." {
        return Some("it has a . or .. component");
    }
    if part.contains('\0') {
        return Some("it contains a NUL character");
    }
    if !windows {
        return None;
    }
    if part.contains(|ch: char| "\\:<>\"|?*".contains(ch) || ch.is_ascii_control()) {
        return Some("it contains a character Windows does not allow in names");
    }
    if part.ends_with(['.', ' ']) {
        return Some("a name ends with a dot or space, which Windows drops");
    }
    let stem = part.split('.').next().unwrap_or_default().trim_end();
    let reserved = ["CON", "PRN", "AUX", "NUL"]
        .iter()
        .any(|name| stem.eq_ignore_ascii_case(name))
        || ["COM", "LPT"].iter().any(|name| {
            stem.len() == 4
                && stem.is_char_boundary(3)
                && stem[..3].eq_ignore_ascii_case(name)
                && stem[3..].bytes().all(|byte| byte.is_ascii_digit())
        });
    reserved.then_some("it names a Windows device")
}

/// Whether a name is `.git`, or what a filesystem may treat as `.git`: in any case, with trailing dots or spaces
/// (Windows), with characters HFS+ ignores (macOS), or as its 8.3 short name `git~1`. Such entries are never
/// fetched, as Git never checks them out, so a fetched tree cannot plant hooks in a repository.
#[must_use]
pub fn is_git(name: &str) -> bool {
    let mut name: String = name
        .chars()
        .filter(|&ch| !hfs_ignorable(ch))
        .collect::<String>()
        .to_lowercase();
    while name.ends_with(['.', ' ']) {
        name.pop();
    }
    name == ".git" || name == "git~1"
}

fn hfs_ignorable(ch: char) -> bool {
    matches!(ch, '\u{200c}'..='\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{206a}'..='\u{206f}' | '\u{feff}')
}

/// Nanoseconds since the Unix epoch, negative before it.
#[must_use]
pub fn mtime_ns(time: SystemTime) -> i64 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(after) => i64::try_from(after.as_nanos()).unwrap_or(i64::MAX),
        Err(before) => -i64::try_from(before.duration().as_nanos()).unwrap_or(i64::MAX),
    }
}

/// The time `ns` nanoseconds after the Unix epoch.
#[must_use]
pub fn system_time(ns: i64) -> SystemTime {
    let offset = Duration::from_nanos(ns.unsigned_abs());
    if ns >= 0 {
        UNIX_EPOCH.checked_add(offset)
    } else {
        UNIX_EPOCH.checked_sub(offset)
    }
    .unwrap_or(UNIX_EPOCH)
}

/// Timestamp resolutions a local filesystem may store below a host's nanoseconds, such as NTFS's 100 ns. Coarser
/// ones are not assumed: a time on a whole second says nothing about the filesystem, and a file changed within that
/// second would be skipped. Filesystems that keep only seconds have their files fetched again instead.
const RESOLUTIONS: [i64; 2] = [100, 1_000];

/// Whether a local copy stamped with the host's modification time `host` and now showing `local` still has it:
/// equal, or `host` truncated to the local filesystem's resolution.
#[must_use]
pub fn same_mtime(host: i64, local: i64) -> bool {
    host == local
        || RESOLUTIONS
            .iter()
            .any(|&step| local % step == 0 && host.div_euclid(step) * step == local)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn glob(source: &str) -> Option<Glob> {
        Glob::parse(source, false).ok().flatten()
    }

    #[test]
    fn globs_split_at_their_first_wildcard_component() {
        assert!(glob("target/doc").is_none());
        let testlogs = glob("bazel-testlogs/**/test.xml");
        assert_eq!(
            testlogs.as_ref().map(|glob| glob.prefix.as_str()),
            Some("bazel-testlogs/")
        );
        let testlogs = testlogs.as_ref();
        assert!(testlogs.is_some_and(|glob| glob.matches("pkg/target/test.xml")));
        assert!(testlogs.is_some_and(|glob| glob.matches("test.xml")));
        assert!(testlogs.is_some_and(|glob| !glob.matches("pkg/test.log") && glob.descends(9)));

        let logs = glob(r"C:\ProgramData\logs\*.txt");
        assert_eq!(
            logs.as_ref().map(|glob| glob.prefix.as_str()),
            Some("C:/ProgramData/logs/")
        );
        assert!(logs.as_ref().is_some_and(|glob| glob.matches("a.txt")
            && !glob.matches("sub/a.txt")
            && !glob.descends(1)));

        let top = glob("*/doc");
        assert_eq!(top.as_ref().map(|glob| glob.prefix.as_str()), Some(""));
        assert!(top.is_some_and(|glob| glob.matches("debug/doc") && glob.descends(1)));
        assert!(Glob::parse("logs/[", false).is_err());
        assert!(
            Glob::parse("LOGS/*.TXT", true)
                .ok()
                .flatten()
                .is_some_and(|glob| glob.matches("a.txt"))
        );
    }

    #[test]
    fn entry_paths_are_plain_names() {
        assert_eq!(components("", false), Ok(Vec::new()));
        assert_eq!(
            components("a/b c/d.txt", true),
            Ok(vec!["a", "b c", "d.txt"])
        );
        assert_eq!(components(r"a\b:c", false), Ok(vec![r"a\b:c"]));
        for path in [
            "/etc/passwd",
            "a//b",
            "a/",
            "../x",
            "a/./b",
            "a/../../b",
            "a\0b",
        ] {
            assert!(components(path, false).is_err(), "{path:?}");
        }
        for path in [
            r"a\..\b",
            "C:/x",
            "a/b:stream",
            "con",
            "Aux.txt",
            "lpt1",
            "COM9.log",
            "x.",
            "x ",
        ] {
            assert!(components(path, true).is_err(), "{path:?}");
            assert!(components(path, false).is_ok(), "{path:?}");
        }
        assert!(components("console/com10/lpt", true).is_ok());
    }

    #[test]
    fn git_directories_are_recognised_in_every_disguise() {
        for name in [
            ".git",
            ".GIT",
            ".git.",
            ".git . ",
            "GIT~1",
            ".g\u{200c}it",
            "\u{feff}.git",
        ] {
            assert!(is_git(name), "{name:?}");
        }
        for name in ["git", ".github", ".gitignore", "a.git", ".git~1"] {
            assert!(!is_git(name), "{name:?}");
        }
    }

    #[test]
    fn modification_times_survive_ntfs_but_not_a_second() {
        let host = 1_700_000_000_123_456_789;
        assert!(same_mtime(host, host));
        assert!(same_mtime(host, 1_700_000_000_123_456_700), "NTFS");
        assert!(!same_mtime(host, 1_700_000_000_123_456_788));
        // A second later on a host that keeps whole seconds is a change, not rounding.
        assert!(!same_mtime(
            1_700_000_001_000_000_000,
            1_700_000_000_000_000_000
        ));
        assert!(!same_mtime(host, 1_700_000_000_000_000_000));
        assert!(same_mtime(-1_550, -1_600), "before the epoch");
        for ns in [0, 1, -1, host, -host] {
            assert_eq!(mtime_ns(system_time(ns)), ns);
        }
    }
}
