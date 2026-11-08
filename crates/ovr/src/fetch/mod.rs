//! `ovr get`: copy files and trees from a host, skipping what is already here.
//!
//! The client sends `GET` and the manifest of what it has in `HAVE` frames without waiting, so a fetch costs one
//! round trip plus the bytes. The host answers with `ENTRY` frames, each file's content as one zstd stream across
//! `DATA` frames, `NOTE`s for what it could not read, and `DONE`. `CONSUMED` tells the host how much has been
//! written, which it paces itself by.

mod plan;
mod report;
mod sink;

use std::io::IsTerminal;
use std::time::Instant;

use futures::SinkExt;
use ovr_proto::frame::{self, MAX_LEN};
use ovr_proto::msg::{Consumed, Get, Have};
use ovr_proto::{Frame, FrameKind};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::session::{self, INTERRUPTED};
use crate::snapshot::Repository;
use crate::{Error, daemon};
pub use plan::Plan;
pub use report::Report;
pub use sink::Stats;

/// Copies `sources` from `host` to `dest`, returning `0` when everything arrived and `1` when something did not.
///
/// # Errors
///
/// Returns an error when the arguments make no sense, or the host cannot be reached or stops answering.
pub async fn get(host: String, sources: Vec<String>, dest: String) -> Result<i32, Error> {
    let started = Instant::now();
    let plan = Plan::new(&sources, &dest)?;
    let cwd = std::env::current_dir()?;
    let open = async {
        let opened = daemon::open(&host).await;
        tracing::debug!(elapsed = ?started.elapsed(), "opened the host stream");
        opened
    };
    let prepare = async {
        let repository = Repository::locate(&cwd).await?;
        let walking = Instant::now();
        let listing = plan.clone();
        let have = tokio::task::spawn_blocking(move || listing.manifest())
            .await
            .map_err(std::io::Error::from)?;
        tracing::debug!(elapsed = ?walking.elapsed(), files = have.len(), "listed the destination");
        Ok::<_, Error>((repository, have))
    };
    let (opened, prepared) = tokio::join!(open, prepare);
    let (repository, have) = prepared?;
    let (stream, initiator) = opened?;
    let request = Get {
        checkout: repository.as_ref().map(|repo| repo.checkout(&initiator)),
        cwd: repository.map(|repo| repo.prefix).unwrap_or_default(),
        initiator,
        sources,
    };

    let stderr = std::io::stderr();
    let mut report = Report::new(stderr.lock(), stderr.is_terminal());
    let fetched = tokio::select! {
        fetched = fetch(stream, &request, &plan, have, &mut report) => Some(fetched),
        _ = tokio::signal::ctrl_c() => None,
    };
    let Some(fetched) = fetched else {
        report.note(
            "interrupted; the files already here are complete, and the same command fetches the rest",
        );
        return Ok(INTERRUPTED);
    };
    match fetched {
        Ok(stats) => {
            tracing::debug!(elapsed = ?started.elapsed(), ?stats, "fetched");
            report.summary(&stats, &plan.dest, started.elapsed());
            Ok(i32::from(stats.failed > 0))
        }
        Err(Error::Disconnected) => Err(Error::FetchCut),
        Err(err) => Err(err),
    }
}

/// Fetches on `stream` what `get` asks for into `plan`'s destination, given what is there already.
///
/// # Errors
///
/// Returns an error when the host fails, breaks the protocol, or the stream breaks; files already written stay.
pub async fn fetch<S, W>(
    stream: S,
    get: &Get,
    plan: &Plan,
    have: Vec<Have>,
    report: &mut Report<W>,
) -> Result<Stats, Error>
where
    S: AsyncRead + AsyncWrite,
    W: std::io::Write,
{
    let (mut frames, mut writer) = frame::split(stream);
    let listed = have.len();
    let asked = async {
        writer.feed(Frame::json(FrameKind::Get, get)?).await?;
        let mut batches = 0;
        for batch in batches_of(have) {
            writer.feed(Frame::json(FrameKind::Have, &batch)?).await?;
            batches += 1;
        }
        writer.send(Frame::json(FrameKind::Have, &[(); 0])?).await?;
        tracing::debug!(files = listed, frames = batches, "sent the manifest");
        Ok::<_, Error>(())
    };
    if let Err(err) = asked.await {
        return Err(broken(&mut frames, err).await);
    }

    let transfer = Instant::now();
    let mut sink = sink::Sink::new(plan, report)?;
    // A host that is done stops reading; what it sent decides how the fetch ends, not a credit it no longer needs.
    let mut crediting = true;
    loop {
        let frame = session::next(&mut frames).await?;
        match frame.kind {
            FrameKind::Err => sink.report.warn(&frame.payload),
            FrameKind::Entry => sink.entry(&frame.parse()?)?,
            FrameKind::Data => {
                sink.data(&frame.payload)?;
                if crediting && !frame.payload.is_empty() {
                    let consumed = Consumed {
                        bytes: sink.stats.wire,
                    };
                    let credit = Frame::json(FrameKind::Consumed, &consumed)?;
                    crediting = writer.send(credit).await.is_ok();
                }
                sink.report.progress(&sink.stats);
            }
            FrameKind::Note => sink.note(&frame.parse()?),
            FrameKind::Done => {
                let stats = sink.done(frame.parse()?)?;
                tracing::debug!(elapsed = ?transfer.elapsed(), ?stats, "received DONE");
                return Ok(stats);
            }
            kind => return Err(ovr_proto::Error::Unexpected(kind).into()),
        }
    }
}

/// Why writing to the host failed: what it said before closing the stream, if anything, or else that it is gone.
async fn broken<S: AsyncRead>(frames: &mut frame::Reader<S>, err: Error) -> Error {
    match session::reason(frames, err).await {
        Error::Protocol(ovr_proto::Error::Io(_)) => Error::Disconnected,
        other => other,
    }
}

/// Splits the manifest into `HAVE` payloads well under the frame limit.
fn batches_of(have: Vec<Have>) -> Vec<Vec<Have>> {
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    let mut size = 0;
    for file in have {
        // Field names, numbers, and quotes, generously, and the path, which escaping control characters can make six
        // times as long.
        let len = 96 + 6 * file.path.len();
        if size + len > MAX_LEN / 2 && !batch.is_empty() {
            batches.push(std::mem::take(&mut batch));
            size = 0;
        }
        size += len;
        batch.push(file);
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    batches
}

#[cfg(all(test, unix))]
mod tests {
    use std::path::Path;

    use futures::StreamExt;
    use ovr_proto::msg::{Done, Entry, EntryKind};
    use ovrd::agent::{Config, Shell};

    use super::*;

    /// Fetches `sources` into `dest` from an agent in this process serving `host`, with `checkout` as this
    /// repository's checkout and `cwd` the directory within it. Returns the stats and what was reported.
    async fn get_from(
        host: &Path,
        checkout: Option<&str>,
        cwd: &str,
        sources: &[&str],
        dest: &Path,
    ) -> Result<(Stats, String), Error> {
        let config = Config {
            root: host.join("checkouts"),
            runs: host.join("runs"),
            agents: host.join("agents"),
            base: host.join("home"),
            shell: Shell::new("/bin/sh"),
            keeper: None,
        };
        let (client, agent) = tokio::io::duplex(1 << 16);
        let agent = tokio::spawn(async move { ovrd::agent::session::serve(agent, &config).await });
        let sources: Vec<String> = sources.iter().map(|&source| source.to_owned()).collect();
        let plan = Plan::new(&sources, &dest.to_string_lossy())?;
        let get = Get {
            initiator: "me".to_owned(),
            checkout: checkout.map(str::to_owned),
            cwd: cwd.to_owned(),
            sources,
        };
        let mut said = Vec::new();
        let mut report = Report::new(&mut said, false);
        let stats = fetch(client, &get, &plan, plan.manifest(), &mut report).await;
        agent.await.map_err(|_| Error::Disconnected)?;
        Ok((stats?, String::from_utf8_lossy(&said).into_owned()))
    }

    fn write(path: &Path, content: &[u8]) -> Result<(), Error> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, content)?;
        Ok(())
    }

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap_or_else(|err| format!("{}: {err}", path.display()))
    }

    /// Every file below `dir`, relative to it, with what it holds or where it points.
    fn tree(dir: &Path) -> Vec<String> {
        let mut found = Vec::new();
        let mut pending = vec![dir.to_owned()];
        while let Some(at) = pending.pop() {
            for entry in std::fs::read_dir(&at).into_iter().flatten().flatten() {
                let path = entry.path();
                let rel = path
                    .strip_prefix(dir)
                    .unwrap_or(&path)
                    .display()
                    .to_string();
                match std::fs::read_link(&path) {
                    Ok(target) => found.push(format!("{rel} -> {}", target.display())),
                    Err(_) if path.is_dir() => pending.push(path),
                    Err(_) => found.push(format!("{rel}: {}", read(&path).trim_end())),
                }
            }
        }
        found.sort();
        found
    }

    #[tokio::test]
    async fn fetches_files_trees_and_globs_then_only_what_changed() -> Result<(), Error> {
        let host = tempfile::tempdir()?;
        let checkout = host.path().join("checkouts/me/repo-1");
        std::fs::create_dir_all(checkout.join(".git"))?;
        let engine = checkout.join("crates/engine");
        write(&engine.join("target/x.log"), b"log\n")?;
        write(&engine.join("target/doc/index.html"), b"index\n")?;
        write(&engine.join("target/doc/sub/page.html"), b"page\n")?;
        write(&engine.join("target/doc/.git/HEAD"), b"never\n")?;
        std::os::unix::fs::symlink("index.html", engine.join("target/doc/link"))?;
        std::fs::create_dir_all(engine.join("target/doc/empty"))?;
        write(&checkout.join("logs/a/t/test.xml"), b"a\n")?;
        write(&checkout.join("logs/b/t/test.xml"), b"b\n")?;
        write(&checkout.join("logs/b/t/test.log"), b"no\n")?;
        let big: Vec<u8> = (0..3u32 << 20).map(|i| (i % 251) as u8).collect();
        write(&engine.join("target/big"), &big)?;
        let app = engine.join("target/app");
        write(&app, b"#!/bin/sh\n")?;
        std::fs::set_permissions(&app, std::os::unix::fs::PermissionsExt::from_mode(0o755))?;

        let local = tempfile::tempdir()?;
        let out = local.path().join("out");
        let sources = [
            "target/x.log",
            "target/doc",
            "../../logs/**/test.xml",
            "target/big",
        ];
        let (first, said) = get_from(
            host.path(),
            Some("repo-1"),
            "crates/engine/",
            &sources,
            &out,
        )
        .await?;
        assert_eq!(
            (first.files, first.unchanged, first.failed),
            (7, 0, 0),
            "{said}"
        );
        assert!(first.wire * 10 < first.bytes, "compressed: {first:?}");
        assert!(said.contains("doc/.git: skipped"), "{said}");
        let mut want = vec![
            "a/t/test.xml: a".to_owned(),
            "b/t/test.xml: b".to_owned(),
            "doc/index.html: index".to_owned(),
            "doc/link -> index.html".to_owned(),
            "doc/sub/page.html: page".to_owned(),
            "x.log: log".to_owned(),
        ];
        let mut got = tree(&out);
        got.retain(|line| !line.starts_with("big"));
        assert_eq!(got, want);
        assert!(
            out.join("doc/empty").is_dir(),
            "empty directories exist too"
        );
        assert_eq!(std::fs::read(out.join("big"))?, big);

        // Unchanged files are skipped; a changed one is fetched again.
        write(&engine.join("target/doc/sub/page.html"), b"page two\n")?;
        let (second, _) = get_from(
            host.path(),
            Some("repo-1"),
            "crates/engine/",
            &sources,
            &out,
        )
        .await?;
        assert_eq!(
            (second.files, second.unchanged),
            (2, 5),
            "the page and the symlink"
        );
        want[4] = "doc/sub/page.html: page two".to_owned();
        got = tree(&out);
        got.retain(|line| !line.starts_with("big"));
        assert_eq!(got, want);

        // One file, renamed, keeping its executable bit.
        let renamed = local.path().join("app-linux");
        let (one, _) = get_from(
            host.path(),
            Some("repo-1"),
            "crates/engine/",
            &["target/app"],
            &renamed,
        )
        .await?;
        assert_eq!(one.files, 1);
        let mode =
            std::os::unix::fs::PermissionsExt::mode(&std::fs::metadata(&renamed)?.permissions());
        assert_eq!(
            (read(&renamed).as_str(), mode & 0o100),
            ("#!/bin/sh\n", 0o100)
        );
        Ok(())
    }

    #[tokio::test]
    async fn trees_land_in_one_place_and_files_keep_their_permissions() -> Result<(), Error> {
        use std::os::unix::fs::PermissionsExt;
        let host = tempfile::tempdir()?;
        write(&host.path().join("home/doc/index.html"), b"index\n")?;
        write(&host.path().join("home/x.log"), b"log\n")?;
        let local = tempfile::tempdir()?;

        // Whether DEST exists yet or not, so that the same command resumes.
        let copy = local.path().join("copy");
        for (files, unchanged) in [(1, 0), (0, 1)] {
            let (stats, said) = get_from(host.path(), None, "", &["doc"], &copy).await?;
            assert_eq!((stats.files, stats.unchanged), (files, unchanged), "{said}");
            assert_eq!(read(&copy.join("doc/index.html")), "index\n");
        }

        let log = local.path().join("x.log");
        get_from(host.path(), None, "", &["x.log"], &log).await?;
        std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o600))?;
        write(&host.path().join("home/x.log"), b"longer log\n")?;
        get_from(host.path(), None, "", &["x.log"], &log).await?;
        let mode = std::fs::metadata(&log)?.permissions().mode() & 0o777;
        assert_eq!((read(&log).as_str(), mode), ("longer log\n", 0o600));
        Ok(())
    }

    #[tokio::test]
    async fn says_where_it_looked_for_what_it_could_not_fetch() -> Result<(), Error> {
        let host = tempfile::tempdir()?;
        write(&host.path().join("home/notes.txt"), b"home\n")?;
        let local = tempfile::tempdir()?;
        let sources = ["notes.txt", "missing.txt", "*.log"];
        let (stats, said) =
            get_from(host.path(), Some("repo-1"), "sub/", &sources, local.path()).await?;
        assert_eq!((stats.files, stats.failed), (1, 2), "{said}");
        assert_eq!(read(&local.path().join("notes.txt")), "home\n");
        let home = host.path().join("home");
        assert!(
            said.contains(&format!(
                "ovr: this host has no checkout of repo-1 yet; reading relative paths from {}, not from sub\n",
                home.display()
            )),
            "{said}"
        );
        assert!(
            said.contains(&format!(
                "ovr: missing.txt: not found on the host at {}",
                home.join("missing.txt").display()
            )),
            "{said}"
        );
        assert!(
            said.contains(&format!(
                "ovr: *.log: matched nothing in {}",
                home.display()
            )),
            "{said}"
        );
        Ok(())
    }

    /// Serves one `GET` with `frames` after reading the request, as a hostile or failing host would.
    fn fake_host(frames: Vec<Frame>, end: bool) -> tokio::io::DuplexStream {
        let (client, host) = tokio::io::duplex(1 << 20);
        tokio::spawn(async move {
            let (mut requests, mut writer) = frame::split(host);
            while let Some(Ok(frame)) = requests.next().await {
                if frame.kind == FrameKind::Have && frame.payload.as_ref() == b"[]" {
                    break;
                }
            }
            for frame in frames {
                let _ = writer.send(frame).await;
            }
            if end {
                let _ = writer
                    .send(Frame::json(FrameKind::Done, &Done { unchanged: 0 })?)
                    .await;
            }
            // Closes only what the host sends, and keeps reading what the client sends, as a host does.
            let _ = writer.close().await;
            while requests.next().await.is_some() {}
            Ok::<_, ovr_proto::Error>(())
        });
        client
    }

    fn refused_both(said: &str) -> bool {
        said.contains("refused a symlink in place of the source itself")
            && said.contains("is not a directory here")
    }

    fn entry(path: &str, kind: EntryKind) -> Result<Frame, Error> {
        let entry = Entry {
            source: 0,
            path: path.to_owned(),
            kind,
        };
        Ok(Frame::json(FrameKind::Entry, &entry)?)
    }

    fn file(
        path: &str,
        content: &[u8],
        encoder: &mut zstd::stream::raw::Encoder<'_>,
    ) -> Result<Vec<Frame>, Error> {
        use zstd::stream::raw::{InBuffer, Operation, OutBuffer};
        let kind = EntryKind::File {
            size: content.len() as u64,
            mtime_ns: 1_000_000_000,
            exec: false,
        };
        let mut compressed = vec![0; 1024 + 2 * content.len()];
        let mut output = OutBuffer::around(&mut compressed[..]);
        encoder.run(&mut InBuffer::around(content), &mut output)?;
        while encoder.flush(&mut output)? > 0 {}
        let len = output.pos();
        compressed.truncate(len);
        Ok(vec![
            entry(path, kind)?,
            Frame::new(FrameKind::Data, compressed),
            Frame::empty(FrameKind::Data),
        ])
    }

    async fn fetch_from(
        stream: tokio::io::DuplexStream,
        sources: &[&str],
        dest: &Path,
    ) -> (Result<Stats, Error>, String) {
        let sources: Vec<String> = sources.iter().map(|&source| source.to_owned()).collect();
        let plan = match Plan::new(&sources, &dest.to_string_lossy()) {
            Ok(plan) => plan,
            Err(err) => return (Err(err), String::new()),
        };
        let get = Get {
            initiator: "me".to_owned(),
            checkout: None,
            cwd: String::new(),
            sources,
        };
        let mut said = Vec::new();
        let mut report = Report::new(&mut said, false);
        let stats = fetch(stream, &get, &plan, plan.manifest(), &mut report).await;
        (stats, String::from_utf8_lossy(&said).into_owned())
    }

    #[tokio::test]
    async fn a_hostile_host_cannot_write_outside_dest_or_into_git() -> Result<(), Error> {
        let local = tempfile::tempdir()?;
        let outside = local.path().join("outside");
        std::fs::create_dir(&outside)?;
        let dest = local.path().join("dest");
        let mut encoder = zstd::stream::raw::Encoder::new(3)?;
        let mut frames = vec![
            entry("", EntryKind::Dir)?,
            entry(
                "link",
                EntryKind::Symlink {
                    target: outside.display().to_string(),
                },
            )?,
        ];
        for path in [
            "link/planted",
            "../escaped",
            "/etc/planted",
            "a/.git/hooks/pre-commit",
            "a/./b",
            "ok",
        ] {
            frames.extend(file(path, b"payload\n", &mut encoder)?);
        }
        let (stats, said) = fetch_from(fake_host(frames, true), &["tree"], &dest).await;
        let stats = stats?;
        assert_eq!((stats.files, stats.failed), (2, 4), "{said}");
        assert_eq!(
            tree(&outside),
            Vec::<String>::new(),
            "nothing written through the symlink"
        );
        assert!(!local.path().join("escaped").exists());
        assert_eq!(
            tree(&dest),
            [
                format!("tree/link -> {}", outside.display()),
                "tree/ok: payload".to_owned()
            ]
        );
        assert!(
            said.contains("is a symlink here, and nothing is written through one"),
            "{said}"
        );
        assert!(
            said.contains(".git directories are never written"),
            "{said}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_hostile_host_cannot_turn_dest_into_a_symlink() -> Result<(), Error> {
        let local = tempfile::tempdir()?;
        let outside = local.path().join("outside");
        std::fs::create_dir(&outside)?;
        let dest = local.path().join("dest");
        let mut encoder = zstd::stream::raw::Encoder::new(3)?;
        let mut frames = vec![entry(
            "",
            EntryKind::Symlink {
                target: outside.display().to_string(),
            },
        )?];
        frames.extend(file("planted", b"payload\n", &mut encoder)?);
        let (stats, said) = fetch_from(fake_host(frames, true), &["tree"], &dest).await;
        assert_eq!((stats?.files, refused_both(&said)), (0, true), "{said}");
        assert!(dest.symlink_metadata().is_err(), "DEST was not created");
        assert_eq!(tree(&outside), Vec::<String>::new());
        Ok(())
    }

    #[tokio::test]
    async fn a_host_cannot_redirect_dest_through_an_alias_of_it() -> Result<(), Error> {
        let local = tempfile::tempdir()?;
        let work = local.path().join("work");
        let outside = local.path().join("outside");
        std::fs::create_dir_all(&work)?;
        std::fs::create_dir(&outside)?;
        // DEST is the user's symlink to its own parent, so `out` inside it is DEST itself.
        let dest = work.join("out");
        std::os::unix::fs::symlink(&work, &dest)?;
        let mut encoder = zstd::stream::raw::Encoder::new(3)?;
        let mut frames = vec![entry(
            "out",
            EntryKind::Symlink {
                target: outside.display().to_string(),
            },
        )?];
        frames.extend(file("planted", b"payload\n", &mut encoder)?);
        let (stats, said) = fetch_from(fake_host(frames, true), &["x/*"], &dest).await;
        assert_eq!(stats?.failed, 0, "{said}");
        assert_eq!(tree(&outside), Vec::<String>::new(), "{said}");
        assert_eq!(read(&work.join("planted")), "payload\n");
        Ok(())
    }

    #[tokio::test]
    async fn an_interrupted_fetch_keeps_whole_files_and_resumes() -> Result<(), Error> {
        let local = tempfile::tempdir()?;
        let dest = local.path().join("dest");
        std::fs::create_dir(&dest)?;
        let mut encoder = zstd::stream::raw::Encoder::new(3)?;
        let mut frames = vec![entry("", EntryKind::Dir)?];
        frames.extend(file("whole", b"whole\n", &mut encoder)?);
        // The second file's data never ends: the connection drops in the middle.
        let mut cut = file("cut", b"cut short\n", &mut encoder)?;
        cut.pop();
        frames.extend(cut);
        let (stats, _) = fetch_from(fake_host(frames, false), &["tree"], &dest).await;
        assert!(matches!(stats, Err(Error::Disconnected)), "{stats:?}");
        assert_eq!(
            tree(&dest),
            ["tree/whole: whole"],
            "no partial or temporary file is left"
        );

        // Whatever the host sends again for the file already here is skipped by the real agent's quick check.
        let host = tempfile::tempdir()?;
        write(&host.path().join("home/tree/whole"), b"whole\n")?;
        write(&host.path().join("home/tree/cut"), b"cut short\n")?;
        let mtime = std::fs::File::open(dest.join("tree/whole"))?
            .metadata()?
            .modified()?;
        std::fs::File::options()
            .write(true)
            .open(host.path().join("home/tree/whole"))?
            .set_modified(mtime)?;
        let (stats, said) = get_from(host.path(), None, "", &["tree"], &dest).await?;
        assert_eq!((stats.files, stats.unchanged), (1, 1), "{said}");
        assert_eq!(tree(&dest), ["tree/cut: cut short", "tree/whole: whole"]);
        Ok(())
    }

    #[tokio::test]
    async fn a_file_cut_short_on_the_host_is_discarded() -> Result<(), Error> {
        let local = tempfile::tempdir()?;
        let dest = local.path().join("dest");
        let mut encoder = zstd::stream::raw::Encoder::new(3)?;
        let mut frames = vec![entry("", EntryKind::Dir)?];
        let mut cut = file("cut", b"half", &mut encoder)?;
        cut.pop();
        frames.extend(cut);
        let note = ovr_proto::msg::Note {
            source: 0,
            message: "tree/cut: could not be read to the end: I/O error".to_owned(),
            failed: true,
        };
        frames.push(Frame::json(FrameKind::Note, &note)?);
        // The zstd stream stays in step: the next file decodes.
        frames.extend(file("next", b"next\n", &mut encoder)?);
        let (stats, said) = fetch_from(fake_host(frames, true), &["tree"], &dest).await;
        assert_eq!(
            (stats?.failed, tree(&dest)),
            (1, vec!["tree/next: next".to_owned()]),
            "{said}"
        );
        assert!(
            said.contains("ovr: tree/cut: could not be read to the end"),
            "{said}"
        );
        Ok(())
    }
}
