#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = ["rich==15.0.0", "psutil==7.2.2"]
# ///
"""Cross-runner e2e for ovr through a Cloudflare quick tunnel.

The initiator job prepares repositories, pairs a host on each OS through this run's artifacts, syncs every
repository to every host at once with `ovr`, times each step, and publishes a report. Host jobs publish their specs
and a key, then serve the initiator with `ovrd connect --foreground` until it uploads `finished`, streaming ovrd's
log and every run's output as they go.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import re
import secrets
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

import psutil
from rich.console import Console
from rich.markup import escape
from rich.table import Table

console = Console(force_terminal=True, width=200, highlight=False, soft_wrap=True)
OSES = ("linux", "macos", "windows")
COLORS = {"linux": "cyan", "macos": "magenta", "windows": "yellow", "initiator": "green"}
GIB = 1024**3
MIB = 1024**2


@dataclass
class Repo:
    name: str
    url: str | None  # None: generated
    tier: str
    workdir: str = ""  # where commands run, relative to the repository root
    build: list[str] = field(default_factory=list)
    build_gib: float = 0  # roughly what building leaves on the host's disk
    status: bool = False  # also time `git status` on the host, for large trees


REPOS = [
    Repo("ripgrep", "https://github.com/BurntSushi/ripgrep", "standard", build=["cargo", "build"], build_gib=1),
    Repo("cargo", "https://github.com/rust-lang/cargo", "standard", build=["cargo", "build"], build_gib=4),
    Repo("tokio", "https://github.com/tokio-rs/tokio", "standard", build=["cargo", "build"], build_gib=1),
    Repo(
        "examples",
        "https://github.com/bazelbuild/examples",
        "standard",
        workdir="cpp-tutorial/stage3",
        build=["bazel", "build", "//main:hello-world"],
        build_gib=1,
    ),
    Repo("llvm-project", "https://github.com/llvm/llvm-project", "large", status=True),
    Repo("chromium", "https://github.com/chromium/chromium", "large", status=True),
    Repo("synthetic", None, "large", status=True),
]
# Left free on every host, for the runner itself and for estimates that fall short.
RESERVE = 8 * GIB
STEPS = ["first sync", "sync, nothing changed", "sync, one file added", "git status on host", "build", "build again"]


def say(who: str, message: str) -> None:
    console.print(f"[{COLORS.get(who, 'white')}]{who:9}[/] {message}")


def raw(who: str, prefix: str, line: str) -> None:
    """Prints a line of command output verbatim, without interpreting it as markup."""
    console.print(f"[{COLORS.get(who, 'white')}]{who:9}[/] [dim]{escape(prefix)} │[/] {escape(line)}")


def gib(n: float) -> str:
    for unit, size in (("GiB", GIB), ("MiB", MIB), ("KiB", 1024)):
        if n >= size:
            return f"{n / size:.1f} {unit}"
    return f"{n:.0f} B"


def duration(s: float | None) -> str:
    if s is None:
        return "–"
    return f"{s:.1f} s" if s < 120 else f"{int(s // 60)}m {int(s % 60):02d}s"


def run(args: list[str], cwd: Path | None = None, check: bool = True) -> str:
    result = subprocess.run(args, check=False, cwd=cwd, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise SystemExit(f"{' '.join(args)} failed ({result.returncode}):\n{result.stdout}{result.stderr}")
    return result.stdout.strip()


async def arun(args: list[str], cwd: Path | None = None) -> str:
    child = await asyncio.create_subprocess_exec(
        *args, cwd=cwd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
    )
    out, _ = await child.communicate()
    if child.returncode != 0:
        raise SystemExit(f"{' '.join(args)} failed ({child.returncode}):\n{out.decode(errors='replace')}")
    return out.decode(errors="replace").strip()


# --- specs ------------------------------------------------------------------------------------------------------


def cpu_model() -> str:
    try:
        match sys.platform:
            case "linux":
                for line in Path("/proc/cpuinfo").read_text().splitlines():
                    if line.startswith("model name"):
                        return line.split(":", 1)[1].strip()
            case "darwin":
                return run(["sysctl", "-n", "machdep.cpu.brand_string"])
            case "win32":
                return run(["powershell", "-NoProfile", "(Get-CimInstance Win32_Processor).Name"])
    except (OSError, SystemExit):
        pass
    return platform.processor() or "unknown"


def os_version() -> str:
    match sys.platform:
        case "darwin":
            return f"macOS {platform.mac_ver()[0]}"
        case "linux":
            try:
                return platform.freedesktop_os_release().get("PRETTY_NAME", "Linux")
            except OSError:
                return "Linux"
    return f"Windows {platform.version()}"


def checkout_root() -> Path:
    """Where ovrd keeps checkouts: `~/ovr` on Unix, `C:\\ovr` on Windows."""
    if sys.platform == "win32":
        return Path(os.environ.get("SystemDrive", "C:") + "\\ovr")
    return Path.home() / "ovr"


def runs_dir() -> Path:
    """Where a host keeps run records, as `ovr_proto::paths::runs_dir` places them."""
    match sys.platform:
        case "darwin":
            return Path.home() / "Library/Application Support/ovr/runs"
        case "win32":
            return Path(os.environ["LOCALAPPDATA"]) / "ovr/data/runs"
    state = os.environ.get("XDG_STATE_HOME") or str(Path.home() / ".local/state")
    return Path(state) / "ovr/runs"


def cmd_specs(args: argparse.Namespace) -> None:
    disk = Path(args.disk)
    probe = next(p for p in (disk, *disk.parents) if p.exists())
    tools = {}
    for tool in ("git", "cargo", "bazel", "cloudflared"):
        try:
            tools[tool] = run([tool, "--version"]).splitlines()[0]
        except (OSError, SystemExit, IndexError):
            tools[tool] = "not found"
    usage = psutil.disk_usage(str(probe))
    specs = {
        "role": args.role,
        "runner": os.environ.get("RUNNER_NAME", ""),
        "label": os.environ.get("RUNS_ON", ""),
        "image": f"{os.environ.get('ImageOS', '?')} {os.environ.get('ImageVersion', '')}".strip(),
        "os": os_version(),
        "arch": platform.machine(),
        "cpu": cpu_model(),
        "cores_logical": psutil.cpu_count(),
        "cores_physical": psutil.cpu_count(logical=False),
        "memory_bytes": psutil.virtual_memory().total,
        "disk_path": str(disk),
        "disk_total_bytes": usage.total,
        "disk_free_bytes": usage.free,
        "tools": tools,
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(specs, indent=2))
    table = Table(title=f"{args.role} runner")
    table.add_column("spec")
    table.add_column("value")
    for key, value in specs.items():
        if key.endswith("_bytes"):
            value = gib(value)
        table.add_row(key, escape(json.dumps(value) if isinstance(value, dict) else str(value)))
    console.print(table)


# --- rendezvous through artifacts -------------------------------------------------------------------------------


async def await_artifact(who: str, name: str, dest: Path, timeout: float, alive=lambda: True) -> None:
    """Waits for another job in this run to upload artifact `name`, then downloads it into `dest`."""
    deadline = time.monotonic() + timeout
    say(who, f"waiting for artifact [bold]{name}[/]")
    while True:
        child = await asyncio.create_subprocess_exec(
            "gh",
            "run",
            "download",
            os.environ["GITHUB_RUN_ID"],
            "--repo",
            os.environ["GITHUB_REPOSITORY"],
            "--name",
            name,
            "--dir",
            str(dest),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        if await child.wait() == 0:
            say(who, f"artifact [bold]{name}[/] arrived")
            return
        if not alive():
            raise SystemExit(f"gave up waiting for artifact {name}: the process it was for has exited")
        if time.monotonic() > deadline:
            raise SystemExit(f"artifact {name} did not appear within {timeout:.0f}s")
        await asyncio.sleep(10)


def cmd_pair(args: argparse.Namespace) -> None:
    """Issues one pairing per host, encrypted to that host's key so that only it can read the address and code."""

    async def main() -> None:
        specs = Path(args.specs)
        await asyncio.gather(
            *(await_artifact("initiator", f"key-{os_name}", specs / os_name, args.timeout) for os_name in OSES)
        )
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        # One after another: `ovrd pair` opens the tunnel once and reuses it.
        for os_name in OSES:
            printed = await arun(["ovrd", "pair", "cloudflare"])
            found = re.search(r"^\s*ovrd connect (\S+) (\S+)\s*$", printed, re.MULTILINE)
            if not found:
                raise SystemExit(f"ovrd pair printed no connect command:\n{printed}")
            addr, code = found.groups()
            say("initiator", f"pairing [{COLORS[os_name]}]{os_name}[/] through {addr}")
            await asyncio.to_thread(
                subprocess.run,
                [
                    "openssl",
                    "pkeyutl",
                    "-encrypt",
                    "-pubin",
                    "-inkey",
                    str(specs / os_name / "key.pub"),
                    "-pkeyopt",
                    "rsa_padding_mode:oaep",
                    "-out",
                    str(out / os_name),
                ],
                input=f"{addr} {code}\n".encode(),
                check=True,
            )

    asyncio.run(main())


# --- host -------------------------------------------------------------------------------------------------------


class RunTail:
    """Follows one run's record: prints its command, its output as it is written, and how it ended."""

    def __init__(self, who: str, dir: Path) -> None:
        self.who, self.dir, self.id = who, dir, dir.name
        self.pos = {"out": 0, "err": 0}
        self.partial = {"out": "", "err": ""}
        self.announced = self.done = False

    def poll(self) -> None:
        if not self.announced:
            try:
                meta = json.loads((self.dir / "run.json").read_text())
            except (OSError, ValueError):
                return
            say(
                self.who,
                f"[bold]run {self.id}[/] in {escape(str(meta['cwd']))}: $ {escape(' '.join(meta['exec']['argv']))}",
            )
            self.announced = True
        for stream in ("out", "err"):
            self.drain(stream)
        ended = self.dir / "exit.json"
        if ended.exists():
            self.drain("out")
            self.drain("err")
            for stream, rest in self.partial.items():
                if rest:
                    raw(self.who, f"run {self.id} {stream}", rest)
            say(self.who, f"[bold]run {self.id}[/] ended: {escape(ended.read_text().strip())}")
            self.done = True

    def drain(self, stream: str) -> None:
        """Reads what was written since the last call. Segments are named by the offset of their first byte."""
        segments = sorted(
            int(p.name.split(".", 1)[1]) for p in self.dir.glob(f"{stream}.*") if p.name.split(".", 1)[1].isdigit()
        )
        data = b""
        for index, start in enumerate(segments):
            following = segments[index + 1] if index + 1 < len(segments) else None
            if following is not None and following <= self.pos[stream]:
                continue
            self.pos[stream] = max(self.pos[stream], start)
            try:
                with open(self.dir / f"{stream}.{start}", "rb") as handle:
                    handle.seek(self.pos[stream] - start)
                    chunk = handle.read()
            except OSError:
                return
            data += chunk
            self.pos[stream] += len(chunk)
        if not data:
            return
        text = self.partial[stream] + data.decode(errors="replace")
        *lines, self.partial[stream] = re.split(r"\r?\n|\r", text)
        for line in lines:
            raw(self.who, f"run {self.id} {stream}", line)


async def follow_runs(who: str, dir: Path) -> None:
    """Streams every run this host starts, as ovrd's keeper writes its output log."""
    existing = {p.name for p in dir.iterdir()} if dir.exists() else set()
    tails: dict[str, RunTail] = {}
    while True:
        if dir.exists():
            found = [p for p in dir.iterdir() if p.is_dir() and p.name.isdigit() and p.name not in existing]
            for path in sorted(found, key=lambda p: int(p.name)):
                if path.name not in tails:
                    tails[path.name] = RunTail(who, path)
        for tail in tails.values():
            if not tail.done:
                tail.poll()
        await asyncio.sleep(0.5)


async def pipe_lines(who: str, prefix: str, stream: asyncio.StreamReader, log: Path | None = None) -> None:
    handle = log.open("a") if log else None
    try:
        while line := await stream.readline():
            text = line.decode(errors="replace").rstrip("\r\n")
            if handle:
                handle.write(text + "\n")
                handle.flush()
            raw(who, prefix, text)
    finally:
        if handle:
            handle.close()


def cmd_serve(args: argparse.Namespace) -> None:
    """Pairs with the initiator and serves it, streaming ovrd's log and every run, until it uploads `finished`."""

    async def main() -> None:
        who = args.os
        await await_artifact(who, "pairs", Path("pairs"), args.timeout)
        decrypted = await asyncio.to_thread(
            subprocess.run,
            [
                "openssl",
                "pkeyutl",
                "-decrypt",
                "-inkey",
                args.key,
                "-pkeyopt",
                "rsa_padding_mode:oaep",
                "-in",
                f"pairs/{args.os}",
            ],
            capture_output=True,
            check=True,
        )
        plain = decrypted.stdout.decode().split()
        if len(plain) != 2:
            raise SystemExit(f"no pairing for {args.os}")
        addr, code = plain
        say(who, f"dialing the initiator at {addr}")
        ovrd = await asyncio.create_subprocess_exec(
            "ovrd",
            "connect",
            addr,
            code,
            "--foreground",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert ovrd.stdout is not None
        background = [
            asyncio.create_task(pipe_lines(who, "ovrd", ovrd.stdout, Path(args.log))),
            asyncio.create_task(follow_runs(who, runs_dir())),
        ]
        try:
            await await_artifact(who, "finished", Path("finished"), args.serve_timeout, lambda: ovrd.returncode is None)
        finally:
            await asyncio.sleep(1)  # let the tails catch up
            for task in background:
                task.cancel()
            if ovrd.returncode is None:
                ovrd.terminate()
                await ovrd.wait()
            else:
                raise SystemExit(f"ovrd exited early with {ovrd.returncode}")

    asyncio.run(main())


# --- initiator: repositories ------------------------------------------------------------------------------------


def tree_stats(root: Path) -> tuple[int, int]:
    """Bytes and files in the working tree, excluding `.git`."""
    total = files = 0
    for dirpath, dirnames, filenames in os.walk(root):
        if ".git" in dirnames:
            dirnames.remove(".git")
        for name in filenames:
            try:
                total += os.lstat(os.path.join(dirpath, name)).st_size
                files += 1
            except OSError:
                pass
    return total, files


def make_synthetic(root: Path, size: int) -> None:
    """Incompressible 4 MiB files in 256 directories, committed and repacked as `git gc` leaves a real repository."""
    root.mkdir(parents=True)
    run(["git", "init", "-q", "-b", "main"], cwd=root)
    chunk = 4 * MIB
    for index in range(size // chunk):
        path = root / f"assets/{index % 256:03d}/blob-{index:06d}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(os.urandom(chunk))
    run(["git", "-c", "core.looseCompression=0", "add", "-A"], cwd=root)
    run(["git", "-c", "user.name=e2e", "-c", "user.email=e2e@localhost", "commit", "-qm", "synthetic"], cwd=root)
    run(["git", "repack", "-adq"], cwd=root)


async def measure_pack(root: Path) -> tuple[int, float]:
    """The size of what a first sync sends, and how long `git pack-objects` takes to produce it, as `ovr` runs it."""
    start = time.monotonic()
    pack = await asyncio.create_subprocess_exec(
        "git",
        "pack-objects",
        "--stdout",
        "--revs",
        "--thin",
        "--delta-base-offset",
        "-q",
        cwd=root,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
    )
    assert pack.stdin is not None and pack.stdout is not None
    pack.stdin.write(b"HEAD\n")
    await pack.stdin.drain()
    pack.stdin.close()
    size = 0
    while block := await pack.stdout.read(1 << 20):
        size += len(block)
    if await pack.wait() != 0:
        raise SystemExit(f"pack-objects failed in {root}")
    return size, time.monotonic() - start


def drop_gitlinks(root: Path) -> int:
    """Commits the tree without submodules, which the host would otherwise clone from their own remotes, so that a
    sync measures ovr rather than those servers. Returns how many there were."""
    listing = run(["git", "ls-tree", "-r", "-z", "HEAD"], cwd=root)
    links = [entry.split("\t", 1)[1] for entry in listing.split("\0") if entry.startswith("160000 commit ")]
    if not links:
        return 0
    paths = [*links, ".gitmodules"] if (root / ".gitmodules").exists() else links
    for at in range(0, len(paths), 1000):
        run(["git", "rm", "-rq", "--cached", "--ignore-unmatch", "--", *paths[at : at + 1000]], cwd=root)
    identity = ["-c", "user.name=e2e", "-c", "user.email=e2e@localhost"]
    run(["git", *identity, "commit", "-qm", "e2e: drop submodules"], cwd=root)
    return len(links)


def cmd_prepare(args: argparse.Namespace) -> None:
    base = Path(args.dir)
    base.mkdir(parents=True, exist_ok=True)
    chosen = [repo for repo in REPOS if repo.tier == "standard" or args.tier == "large"]

    async def prepare(repo: Repo) -> dict:
        root = base / repo.name
        say("initiator", f"preparing [bold]{repo.name}[/]")
        start = time.monotonic()
        dropped = 0
        if repo.url:
            await arun(["git", "clone", "--quiet", "--depth", "1", repo.url, str(root)])
            if dropped := await asyncio.to_thread(drop_gitlinks, root):
                say("initiator", f"dropped {dropped:,} submodules from [bold]{repo.name}[/]")
        else:
            await asyncio.to_thread(make_synthetic, root, int(args.synthetic_gib * GIB))
        setup = time.monotonic() - start
        tree, files = await asyncio.to_thread(tree_stats, root)
        pack, pack_seconds = await measure_pack(root)
        say(
            "initiator",
            f"prepared [bold]{repo.name}[/] in {duration(setup)}: {gib(tree)} in {files:,} files; a first sync "
            f"sends {gib(pack)}, packed in {duration(pack_seconds)}",
        )
        return asdict(repo) | {
            "path": str(root),
            "tree_bytes": tree,
            "files": files,
            "pack_bytes": pack,
            "pack_seconds": pack_seconds,
            "submodules_dropped": dropped,
        }

    async def main() -> list[dict]:
        return list(await asyncio.gather(*(prepare(repo) for repo in chosen)))

    Path(args.out).write_text(json.dumps(asyncio.run(main()), indent=2))


# --- initiator: hosts and benchmark -----------------------------------------------------------------------------


def cmd_hosts(args: argparse.Namespace) -> None:
    """Waits until a host of every OS is online, and records each one's name."""
    deadline = time.monotonic() + args.timeout
    while True:
        listing = run(["ovr", "hosts"], check=False)
        hosts = {}
        for line in listing.splitlines():
            parts = line.split()
            if len(parts) == 3 and parts[1] == "online":
                hosts[parts[2].split("/")[0]] = parts[0]
        if all(os_name in hosts for os_name in OSES):
            say("initiator", f"every host is online:\n{escape(listing)}")
            Path(args.out).write_text(json.dumps(hosts, indent=2))
            return
        if time.monotonic() > deadline:
            raise SystemExit(f"hosts did not all dial in:\n{listing}")
        time.sleep(5)


async def ovr(
    os_name: str, host: str, repo: dict, step: str, argv: list[str], logs: Path, timeout: float
) -> tuple[int | str, float, str]:
    """Runs `ovr HOST -- argv` in the repository, streaming its output; returns its exit code, or `timeout` when it
    was killed for taking longer than `timeout` seconds, its time, and its output."""
    cwd = Path(repo["path"]) / repo["workdir"]
    prefix = f"{repo['name']}: {step}"
    say(os_name, f"[bold]{escape(prefix)}[/]: $ ovr {host} -- {escape(' '.join(argv))}")
    log = logs / f"{os_name}-{repo['name']}-{re.sub(r'[^a-z0-9]+', '-', step.lower()).strip('-')}.log"
    start = time.monotonic()
    child = await asyncio.create_subprocess_exec(
        "ovr",
        host,
        "--",
        *argv,
        cwd=cwd,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    assert child.stdout is not None
    lines = []

    async def relay() -> int:
        assert child.stdout is not None
        with log.open("w") as out:
            while line := await child.stdout.readline():
                text = line.decode(errors="replace").rstrip("\r\n")
                out.write(text + "\n")
                lines.append(text)
                raw(os_name, prefix, text)
        return await child.wait()

    code: int | str
    try:
        code = await asyncio.wait_for(relay(), timeout)
    except TimeoutError:
        child.kill()
        await child.wait()
        code = "timeout"
    elapsed = time.monotonic() - start
    style = "green" if code == 0 else "red"
    say(os_name, f"[bold]{escape(prefix)}[/]: [{style}]exit {code} after {duration(elapsed)}[/]")
    return code, elapsed, "\n".join(lines)


async def bench_host(
    os_name: str, host: str, repos: list[dict], free: int, logs: Path, results: list[dict], save, timeout: float
) -> None:
    free -= RESERVE
    for repo in repos:
        row = {"os": os_name, "host": host, "repo": repo["name"], "steps": {}, "ok": True}
        results.append(row)
        # The host keeps the pack, the checkout and its index with filesystem overhead, and what a build leaves.
        need = int(repo["pack_bytes"] * 1.1 + repo["tree_bytes"] * 1.2 + repo["files"] * 512 + repo["build_gib"] * GIB)
        if need > free:
            row["skipped"] = f"needs ~{gib(need)}, {gib(max(free, 0))} to spare"
            say(os_name, f"skipping [bold]{repo['name']}[/]: {row['skipped']}")
            save()
            continue
        free -= need

        async def step(name: str, argv: list[str], repo: dict = repo, row: dict = row) -> tuple[int, str]:
            code, elapsed, output = await ovr(os_name, host, repo, name, argv, logs, timeout)
            row["steps"][name] = {"seconds": elapsed, "code": code}
            row["ok"] &= code == 0
            save()
            return code, output

        await step("first sync", ["git", "--version"])
        await step("sync, nothing changed", ["git", "--version"])
        # A new untracked file, checked by hash, proves the working tree arrives, not just what is committed.
        marker = Path(repo["path"]) / repo["workdir"] / f"ovr-e2e-{os_name}.txt"
        marker.write_text(secrets.token_hex(32))
        want = run(["git", "hash-object", marker.name], cwd=marker.parent)
        code, output = await step("sync, one file added", ["git", "hash-object", marker.name])
        if code == 0 and want not in output:
            row["ok"] = False
            row["steps"]["sync, one file added"]["code"] = "content differs"
            save()
        if repo["status"]:
            await step("git status on host", ["git", "status", "--short", "--untracked-files=no"])
        if repo["build"]:
            await step("build", repo["build"])
            await step("build again", repo["build"])


async def follow_file(who: str, prefix: str, path: Path) -> None:
    """Streams lines appended to `path`, like `tail -F`."""
    pos = 0
    partial = ""
    while True:
        try:
            with path.open("rb") as handle:
                if handle.seek(0, 2) < pos:
                    pos = 0
                handle.seek(pos)
                data = handle.read()
                pos += len(data)
        except OSError:
            data = b""
        if data:
            *lines, partial = (partial + data.decode(errors="replace")).split("\n")
            for line in lines:
                raw(who, prefix, line)
        await asyncio.sleep(0.5)


def cmd_bench(args: argparse.Namespace) -> None:
    hosts = json.loads(Path(args.hosts).read_text())
    repos = json.loads(Path(args.repos).read_text())
    logs = Path(args.logs)
    logs.mkdir(parents=True, exist_ok=True)
    free = {
        os_name: json.loads((Path(args.specs) / os_name / "specs.json").read_text())["disk_free_bytes"]
        for os_name in OSES
    }

    out = Path(args.out)
    results: list[dict] = []

    def save() -> None:
        """Records every result so far, so a run cut short still has a report."""
        out.write_text(json.dumps(sorted(results, key=lambda row: OSES.index(row["os"])), indent=2))

    async def main() -> None:
        daemon_log = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state") / "ovr/ovrd.log"
        tail = asyncio.create_task(follow_file("initiator", "ovrd", daemon_log))
        try:
            await asyncio.gather(
                *(
                    bench_host(os_name, hosts[os_name], repos, free[os_name], logs, results, save, args.step_timeout)
                    for os_name in OSES
                )
            )
        finally:
            tail.cancel()

    asyncio.run(main())
    save()
    failed = [f"{row['os']}/{row['repo']}" for row in results if not row["ok"]]
    if failed:
        raise SystemExit(f"failed: {', '.join(failed)}")


# --- report -----------------------------------------------------------------------------------------------------


def cmd_report(args: argparse.Namespace) -> None:
    specs_dir = Path(args.specs)
    specs = {
        name: json.loads((specs_dir / name / "specs.json").read_text())
        for name in ("initiator", *OSES)
        if (specs_dir / name / "specs.json").exists()
    }
    repos = json.loads(Path(args.repos).read_text()) if Path(args.repos).exists() else []
    results = json.loads(Path(args.results).read_text()) if Path(args.results).exists() else []
    by_repo = {repo["name"]: repo for repo in repos}

    md = [
        "# ovr tunnel e2e",
        "",
        (
            "An initiator runner synced each repository to a host on each OS through one Cloudflare quick tunnel, "
            "all three hosts at once. A first sync covers snapshotting, packing, the transfer, indexing, and the "
            "checkout; its rate is the pack's size over that whole time, so it understates the link. Later syncs "
            "send only what changed."
        ),
    ]

    if specs:
        md += ["", "## Runners", "", "| | " + " | ".join(specs) + " |", "|---|" + "---|" * len(specs)]
        rows = [
            ("Label", lambda s: s["label"]),
            ("Image", lambda s: s["image"]),
            ("OS", lambda s: f"{s['os']} ({s['arch']})"),
            ("CPU", lambda s: s["cpu"]),
            ("Cores", lambda s: f"{s['cores_logical']} logical, {s['cores_physical']} physical"),
            ("Memory", lambda s: gib(s["memory_bytes"])),
            (
                "Disk free",
                lambda s: f"{gib(s['disk_free_bytes'])} of {gib(s['disk_total_bytes'])} at `{s['disk_path']}`",
            ),
            ("git", lambda s: s["tools"]["git"]),
            ("cargo", lambda s: s["tools"]["cargo"]),
            ("bazel", lambda s: s["tools"]["bazel"]),
            ("cloudflared", lambda s: s["tools"]["cloudflared"]),
        ]
        for label, get in rows:
            md.append(f"| {label} | " + " | ".join(str(get(s)) for s in specs.values()) + " |")

    md += [
        "",
        "## Repositories",
        "",
        "| Repository | Tree | Files | First sync sends | Packing it takes |",
        "|---|---|---|---|---|",
    ]
    for repo in repos:
        md.append(
            f"| {repo['name']} | {gib(repo['tree_bytes'])} | {repo['files']:,} | {gib(repo['pack_bytes'])} | "
            f"{duration(repo['pack_seconds'])} |"
        )
    if dropped := [
        f"{repo['name']} ({repo['submodules_dropped']:,})" for repo in repos if repo.get("submodules_dropped")
    ]:
        md += [
            "",
            (
                f"Submodules were dropped from {', '.join(dropped)}: hosts would clone them from their own remotes, "
                "which measures those servers rather than ovr."
            ),
        ]

    steps = [name for name in STEPS if any(name in row.get("steps", {}) for row in results)]
    for os_name in OSES:
        host_rows = [row for row in results if row["os"] == os_name]
        if not host_rows:
            continue
        md += [
            "",
            f"## {os_name} host",
            "",
            "| Repository | " + " | ".join(steps) + " | First sync rate |",
            "|---|" + "---|" * (len(steps) + 1),
        ]
        for row in host_rows:
            if "skipped" in row:
                md.append(f"| {row['repo']} | skipped: {row['skipped']} |" + " |" * len(steps))
                continue
            cells = []
            for name in steps:
                step = row["steps"].get(name)
                if step is None:
                    cells.append("–")
                elif step["code"] == 0:
                    cells.append(duration(step["seconds"]))
                else:
                    cells.append(f"❌ {step['code']} ({duration(step['seconds'])})")
            first = row["steps"].get("first sync")
            rate = "–"
            if first and first["code"] == 0 and first["seconds"] > 0:
                rate = f"{by_repo[row['repo']]['pack_bytes'] / first['seconds'] / MIB:.1f} MiB/s"
            md.append(f"| {row['repo']} | " + " | ".join(cells) + f" | {rate} |")

    synced = [
        (by_repo[row["repo"]]["pack_bytes"], row["steps"]["first sync"]["seconds"])
        for row in results
        if row["steps"].get("first sync", {}).get("code") == 0
    ]
    failed = [f"{row['os']}/{row['repo']}" for row in results if not row["ok"]]
    md += ["", "## Summary", ""]
    if synced:
        sent = sum(size for size, _ in synced)
        md.append(
            f"- First syncs sent {gib(sent)} in total; the largest went at "
            f"{max(size / seconds for size, seconds in synced) / MIB:.1f} MiB/s."
        )
    warm = [
        row["steps"]["sync, nothing changed"]["seconds"]
        for row in results
        if row["steps"].get("sync, nothing changed", {}).get("code") == 0
    ]
    if warm:
        md.append(f"- A sync with nothing changed took {min(warm):.1f}–{max(warm):.1f} s, including the command.")
    md.append(
        f"- **{'Failed: ' + ', '.join(failed) if failed else 'Every step passed.'}**"
        if results
        else "- **No results: the run failed before syncing.**"
    )
    text = "\n".join(md) + "\n"
    Path(args.out).write_text(text)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a") as handle:
            handle.write(text)
    console.print(escape(text))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(required=True)

    p = sub.add_parser("specs", help="record this runner's specs")
    p.add_argument("--role", required=True)
    p.add_argument(
        "--disk", default=os.environ.get("OVR_DISK") or str(checkout_root()), help="where to measure free disk"
    )
    p.add_argument("--out", required=True)
    p.set_defaults(func=cmd_specs)

    p = sub.add_parser("pair", help="initiator: pair a host on every OS through a quick tunnel")
    p.add_argument("--specs", default="specs", help="where hosts' keys and specs are downloaded")
    p.add_argument("--out", default="pairs")
    p.add_argument("--timeout", type=float, default=3600)
    p.set_defaults(func=cmd_pair)

    p = sub.add_parser("serve", help="host: pair, then serve the initiator until it has finished")
    p.add_argument("--os", required=True, choices=OSES)
    p.add_argument("--key", required=True)
    p.add_argument("--log", default="ovrd.log")
    p.add_argument("--timeout", type=float, default=2 * 3600)
    p.add_argument("--serve-timeout", type=float, default=6 * 3600)
    p.set_defaults(func=cmd_serve)

    p = sub.add_parser("prepare", help="initiator: clone or generate the repositories to sync")
    p.add_argument("--tier", choices=["standard", "large"], default="standard")
    p.add_argument("--synthetic-gib", type=float, default=10)
    p.add_argument("--dir", required=True)
    p.add_argument("--out", default="repos.json")
    p.set_defaults(func=cmd_prepare)

    p = sub.add_parser("hosts", help="initiator: wait for every host to dial in")
    p.add_argument("--timeout", type=float, default=900)
    p.add_argument("--out", default="hosts.json")
    p.set_defaults(func=cmd_hosts)

    p = sub.add_parser("bench", help="initiator: sync and run every repository on every host at once")
    p.add_argument("--hosts", default="hosts.json")
    p.add_argument("--repos", default="repos.json")
    p.add_argument("--specs", default="specs")
    p.add_argument("--logs", default="logs")
    p.add_argument("--out", default="results.json")
    p.add_argument("--step-timeout", type=float, default=3600, help="seconds before a step is killed")
    p.set_defaults(func=cmd_bench)

    p = sub.add_parser("report", help="initiator: write the report and the job summary")
    p.add_argument("--specs", default="specs")
    p.add_argument("--repos", default="repos.json")
    p.add_argument("--results", default="results.json")
    p.add_argument("--out", default="report.md")
    p.set_defaults(func=cmd_report)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
