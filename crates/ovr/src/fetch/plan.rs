//! Where each source of `ovr get` lands on this machine, and what is already there.

use std::collections::HashMap;
use std::fs::{self, Metadata};
use std::path::{Path, PathBuf};

use ovr_proto::fetch::{self as rules, Glob};
use ovr_proto::msg::Have;

use crate::Error;

/// Where the sources go, decided before connecting, much as `cp -R` decides, except that a directory always lands
/// inside DEST, so that the same command lands it in the same place every time.
#[derive(Debug, Clone)]
pub struct Plan {
    /// DEST as given.
    pub dest: PathBuf,
    /// DEST is a directory that sources go into, created if missing; otherwise the one source, if it is a file, is
    /// copied as DEST.
    pub into: bool,
    pub targets: Vec<Target>,
}

/// Where one source's entries go.
#[derive(Debug, Clone)]
pub struct Target {
    /// Components below DEST that the source's entry paths start from: its name, or none.
    pub below: Vec<String>,
    /// The source's name, as it lands inside DEST should a source copied as DEST turn out to be a directory.
    pub name: Option<String>,
    /// What a glob source matches below its literal prefix, so that only those files are listed as present; `None`
    /// for a named source, or a glob the host will reject.
    glob: Option<Glob>,
    globbed: bool,
}

impl Plan {
    /// Decides where `sources` go: into DEST when there are several, any is a glob, DEST ends with a separator, or
    /// DEST is a directory; otherwise the one source is copied as DEST if it is a file, or lands inside it if not.
    ///
    /// # Errors
    ///
    /// Returns a usage error when DEST must be a directory and is not, or a source would land in a `.git`.
    pub fn new(sources: &[String], dest: &str) -> Result<Self, Error> {
        if dest.is_empty() {
            return Err(usage(
                "DEST is empty",
                "name where the files go, such as `.`",
            ));
        }
        let path = PathBuf::from(dest);
        let globbed = sources.iter().any(|source| rules::is_glob(source));
        let into = sources.len() > 1
            || globbed
            || dest.ends_with(std::path::is_separator)
            || path.is_dir();
        if into && path.symlink_metadata().is_ok() && !path.is_dir() {
            return Err(usage(
                &format!("{dest} is not a directory"),
                "with several sources, a glob, or a DEST ending in /, DEST is a directory, created if missing",
            ));
        }
        let mut targets: Vec<Target> = Vec::with_capacity(sources.len());
        for source in sources {
            let globbed = rules::is_glob(source);
            let below = match name(source) {
                Some(name) if into && !globbed => vec![checked(source, name)?],
                _ => Vec::new(),
            };
            // Each would skip what the other just wrote as unchanged, and the last to arrive would win.
            if let Some(earlier) = targets.iter().position(|target| {
                !below.is_empty() && target.below.iter().map(key).eq(below.iter().map(key))
            }) {
                return Err(usage(
                    &format!(
                        "{} and {source} would both land in {}",
                        sources[earlier],
                        path.join(&below[0]).display()
                    ),
                    "fetch them into different directories, one command each",
                ));
            }
            targets.push(Target {
                name: (!into && !globbed)
                    .then(|| name(source).map(str::to_owned))
                    .flatten(),
                below,
                glob: Glob::parse(source, true).ok().flatten(),
                globbed,
            });
        }
        Ok(Self {
            dest: path,
            into,
            targets,
        })
    }

    /// Where `target`'s entries go.
    #[must_use]
    pub fn root(&self, target: &Target) -> PathBuf {
        target
            .below
            .iter()
            .fold(self.dest.clone(), |path, part| path.join(part))
    }

    /// The files already where the sources go, for the host to skip those it has unchanged.
    #[must_use]
    pub fn manifest(&self) -> Vec<Have> {
        let mut have = Vec::new();
        for (index, target) in self.targets.iter().enumerate() {
            let source = u32::try_from(index).unwrap_or(u32::MAX);
            let root = self.root(target);
            // DEST is the user's to name, symlink or not; nothing below it is followed.
            let meta = if target.below.is_empty() {
                fs::metadata(&root)
            } else {
                fs::symlink_metadata(&root)
            };
            match meta {
                Ok(meta) if meta.is_file() && !target.globbed => {
                    have.push(record(source, String::new(), &meta));
                }
                Ok(meta) if meta.is_dir() => {
                    let mut walk = Walk {
                        source,
                        target,
                        have: &mut have,
                    };
                    walk.dir(&root, "", 0, !target.globbed);
                }
                _ => {}
            }
        }
        // A file more than one source can write is always sent, so the last source wins every time, rather than
        // one skipping what another just replaced.
        let local = |have: &Have| {
            let target = &self.targets[have.source as usize];
            have.path
                .split('/')
                .filter(|part| !part.is_empty())
                .fold(self.root(target), |path, part| path.join(part))
        };
        let mut writers: HashMap<String, usize> = HashMap::new();
        for file in &have {
            *writers.entry(key(local(file))).or_default() += 1;
        }
        have.retain(|file| writers.get(&key(local(file))) == Some(&1));
        have
    }
}

struct Walk<'a> {
    source: u32,
    target: &'a Target,
    have: &'a mut Vec<Have>,
}

impl Walk<'_> {
    /// Lists the files in `dir`, `depth` components below the root, that the source can match: all of them once
    /// `matched`, as below a directory a glob matched.
    fn dir(&mut self, dir: &Path, rel: &str, depth: usize, matched: bool) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        let glob = self.target.glob.as_ref();
        for entry in entries.flatten() {
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            if rules::is_git(&name) {
                continue;
            }
            let rel = if rel.is_empty() {
                name
            } else {
                format!("{rel}/{name}")
            };
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            let matched = matched || glob.is_some_and(|glob| glob.matches(&rel));
            if meta.is_file() && matched {
                self.have.push(record(self.source, rel, &meta));
            } else if meta.is_dir()
                && (matched || glob.is_some_and(|glob| glob.descends(depth + 1)))
            {
                self.dir(&entry.path(), &rel, depth + 1, matched);
            }
        }
    }
}

/// What two local paths must differ in to be different files: on Windows and macOS, whose filesystems usually ignore
/// case, more than case.
fn key(path: impl AsRef<Path>) -> String {
    let path = path.as_ref().to_string_lossy();
    if cfg!(any(windows, target_os = "macos")) {
        path.to_lowercase()
    } else {
        path.into_owned()
    }
}

fn record(source: u32, path: String, meta: &Metadata) -> Have {
    Have {
        source,
        path,
        size: meta.len(),
        mtime_ns: meta.modified().map(rules::mtime_ns).unwrap_or_default(),
    }
}

/// The name a source lands under in a directory: its last component, whichever separator the host uses. A source
/// without one, such as `.`, `~`, `/`, or `C:\`, has its contents land in the directory instead.
#[must_use]
pub fn name(source: &str) -> Option<&str> {
    let name = source
        .trim_end_matches(['/', '\\'])
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default();
    let nameless =
        name.is_empty() || name == "." || name == ".." || name == "~" || name.ends_with(':');
    (!nameless).then_some(name)
}

/// `name` if this machine can write it, and it is not a `.git`.
fn checked(source: &str, name: &str) -> Result<String, Error> {
    if rules::is_git(name) {
        return Err(usage(
            &format!("{source} would land in a .git directory here"),
            "a fetched .git could run hooks in a repository; copy what is in it instead: `ovr get HOST .git/. dot-git/`",
        ));
    }
    match rules::components(name, cfg!(windows)) {
        Ok(_) => Ok(name.to_owned()),
        Err(_) => Err(usage(
            &format!("{name} is not a valid name on this machine"),
            "copy it under another name: `ovr get HOST SRC NEW-NAME`",
        )),
    }
}

fn usage(message: &str, help: &str) -> Error {
    Error::Usage {
        message: message.to_owned(),
        help: help.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(sources: &[&str], dest: &Path) -> Result<Plan, Error> {
        let sources: Vec<String> = sources.iter().map(|&source| source.to_owned()).collect();
        Plan::new(&sources, &dest.to_string_lossy())
    }

    fn roots(plan: &Plan) -> Vec<PathBuf> {
        plan.targets
            .iter()
            .map(|target| plan.root(target))
            .collect()
    }

    #[test]
    fn sources_land_like_cp() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let here = dir.path();
        let file = here.join("file");
        std::fs::write(&file, "")?;

        let into = plan(&["target/doc/"], here)?;
        assert!(into.into);
        assert_eq!(roots(&into), [here.join("doc")]);
        let renamed = plan(&["target/app"], &here.join("app-linux"))?;
        assert!(!renamed.into);
        assert_eq!(roots(&renamed), [here.join("app-linux")]);
        let created = plan(&["a", r"C:\x\crash.dmp"], &here.join("new"))?;
        assert!(created.into);
        assert_eq!(
            roots(&created),
            [
                here.join("new").join("a"),
                here.join("new").join("crash.dmp")
            ]
        );
        let trailing = Plan::new(&["a".to_owned()], &format!("{}/new/", here.display()))?;
        assert!(trailing.into);
        let globbed = plan(&["bazel-testlogs/**/test.xml"], &here.join("logs"))?;
        assert!(globbed.into);
        assert_eq!(roots(&globbed), [here.join("logs")]);
        let nameless = plan(&[".", "~", "/"], here)?;
        assert_eq!(roots(&nameless), [here, here, here]);

        for sources in [&["a", "b"][..], &["*.log"]] {
            let err = plan(sources, &file).err();
            assert!(
                matches!(&err, Some(Error::Usage { message, .. }) if message.ends_with("is not a directory")),
                "{err:?}"
            );
        }
        assert!(matches!(plan(&["x/.git"], here), Err(Error::Usage { .. })));
        let twice = plan(&["a/report.xml", "b/report.xml"], here).err();
        assert!(
            matches!(&twice, Some(Error::Usage { message, .. }) if message.starts_with("a/report.xml and b/report.xml would both land in")),
            "{twice:?}"
        );
        assert!(
            plan(&["x/.git"], &here.join("elsewhere")).is_ok(),
            "named by the user"
        );
        Ok(())
    }

    #[test]
    fn the_manifest_lists_only_what_a_source_can_overwrite() -> Result<(), Error> {
        let dir = tempfile::tempdir()?;
        let here = dir.path();
        for file in [
            "doc/a.html",
            "doc/sub/b.html",
            "doc/.git/HEAD",
            "p/t/test.xml",
            "p/t/test.log",
        ] {
            let path = here.join(file);
            std::fs::create_dir_all(path.parent().unwrap_or(here))?;
            std::fs::write(path, "x")?;
        }
        let paths = |sources: &[&str]| -> Result<Vec<(u32, String)>, Error> {
            let mut have: Vec<_> = plan(sources, here)?
                .manifest()
                .into_iter()
                .map(|have| (have.source, have.path))
                .collect();
            have.sort();
            Ok(have)
        };
        let owned = |paths: &[(u32, &str)]| -> Vec<(u32, String)> {
            paths
                .iter()
                .map(|&(source, path)| (source, path.to_owned()))
                .collect()
        };
        assert_eq!(
            paths(&["target/doc", "bazel-testlogs/*/t/*.xml"])?,
            owned(&[(0, "a.html"), (0, "sub/b.html"), (1, "p/t/test.xml")])
        );
        assert_eq!(
            paths(&["missing", "x/[p]"])?,
            owned(&[(1, "p/t/test.log"), (1, "p/t/test.xml")])
        );
        // Both globs can write p/t/test.xml: neither may skip it, or each would skip what the other just wrote.
        assert_eq!(
            paths(&["a/*/t/*.xml", "b/*/t/*.x*"])?,
            Vec::<(u32, String)>::new()
        );
        let file = Plan::new(
            &["x/a.html".to_owned()],
            &here.join("doc").join("a.html").to_string_lossy(),
        )?;
        let have = file.manifest();
        assert_eq!(
            have.iter()
                .map(|have| (have.path.as_str(), have.size))
                .collect::<Vec<_>>(),
            [("", 1)]
        );
        Ok(())
    }
}
