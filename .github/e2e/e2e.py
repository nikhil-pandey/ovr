#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = ["rich==15.0.0", "psutil==7.2.2"]
# ///
"""Cross-runner e2e for ovr through a Cloudflare quick tunnel.

Hosts are an OS and a kind, such as `linux-paired`. A paired host dials in with a code the initiator issues through
this run's artifacts and serves with `ovrd connect --foreground`; a trusted host uses the action, which dials in as
this GitHub Actions job with an OIDC token and serves in the background. `KINDS` picks which kinds the run has, one
host of each per OS. Host jobs publish their specs, and paired ones a key, then serve the initiator until it uploads
`finished`, streaming ovrd's log and every run's output as they go. The initiator either checks that syncs reach
every host (`check`), or syncs real repositories to every host at once, builds them there, times each step, and
publishes a report (`bench`).
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
KINDS = tuple(os.environ.get("KINDS", "paired").split())
HOSTS = tuple(f"{os_name}-{kind}" for kind in KINDS for os_name in OSES)
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


def tag(who: str) -> str:
    """`who` in its OS's color, padded so that messages line up."""
    return f"[{COLORS.get(who.split('-')[0], 'white')}]{who:14}[/]"


def say(who: str, message: str) -> None:
    console.print(f"{tag(who)} {message}")


def raw(who: str, prefix: str, line: str) -> None:
    """Prints a line of command output verbatim, without interpreting it as markup."""
    console.print(f"{tag(who)} [dim]{escape(prefix)} │[/] {escape(line)}")


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


def absolute_env(var: str) -> Path | None:
    value = os.environ.get(var)
    return Path(value) if value and Path(value).is_absolute() else None


def checkout_root() -> Path:
    """Where ovrd keeps checkouts, as `ovr_proto::paths::checkouts_dir` places them."""
    if root := absolute_env("OVR_CHECKOUTS"):
        return root
    if data := absolute_env("OVR_DATA_DIR"):
        return data / "checkouts"
    match sys.platform:
        case "darwin":
            return Path.home() / "Library/Application Support/ovr/checkouts"
        case "win32":
            return Path(os.environ["LOCALAPPDATA"]) / "ovr/data/checkouts"
    data_home = absolute_env("XDG_DATA_HOME") or Path.home() / ".local/share"
    return data_home / "ovr/checkouts"


def runs_dir() -> Path:
    """Where a host keeps run records, as `ovr_proto::paths::runs_dir` places them."""
    if state := absolute_env("OVR_STATE_DIR"):
        return state / "runs"
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
    """Issues one pairing per paired host, encrypted to that host's key so that only it can read the address and
    code, and, when there are trusted hosts, trusts this repository's jobs and publishes where they dial."""

    async def main() -> None:
        specs = Path(args.specs)
        await asyncio.gather(
            *(await_artifact("initiator", f"host-{host}", specs / host, args.timeout) for host in HOSTS)
        )
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        # One after another: `ovrd pair` opens the tunnel once and reuses it.
        for host in (host for host in HOSTS if host.endswith("-paired")):
            printed = await arun(["ovrd", "pair", "cloudflare"])
            found = re.search(r"^\s*ovrd connect (\S+) (\S+)\s*$", printed, re.MULTILINE)
            if not found:
                raise SystemExit(f"ovrd pair printed no connect command:\n{printed}")
            addr, code = found.groups()
            say("initiator", f"pairing {tag(host)} through {addr}")
            await asyncio.to_thread(
                subprocess.run,
                [
                    "openssl",
                    "pkeyutl",
                    "-encrypt",
                    "-pubin",
                    "-inkey",
                    str(specs / host / "key.pub"),
                    "-pkeyopt",
                    "rsa_padding_mode:oaep",
                    "-out",
                    str(out / host),
                ],
                input=f"{addr} {code}\n".encode(),
                check=True,
            )
        if "trusted" in KINDS:
            # Trusted jobs need no pairing, and the address and fingerprint they dial are not secret. Trusting before
            # any pairing would leave the tunnel unopened, so it opens one first.
            if "paired" not in KINDS:
                await arun(["ovrd", "pair", "cloudflare"])
            printed = await arun(["ovrd", "trust", f"github:{os.environ['GITHUB_REPOSITORY']}"])
            values = dict(re.findall(r"gh variable set (OVR_ADDR|OVR_INITIATOR) .*--body (\S+)", printed))
            if len(values) != 2:
                raise SystemExit(f"ovrd trust printed no address and fingerprint:\n{printed}")
            (out / "trust").write_text(f"{values['OVR_ADDR']} {values['OVR_INITIATOR']}\n")
            say("initiator", f"trusting this repository's jobs through {values['OVR_ADDR']}")

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


def echo(who: str, prefix: str, lines: list[str], log: Path | None) -> None:
    """Prints `lines` as `who`'s output, and appends them to `log` if given."""
    if log and lines:
        with log.open("a") as handle:
            handle.writelines(line + "\n" for line in lines)
    for line in lines:
        raw(who, prefix, line)


async def pipe_lines(who: str, prefix: str, stream: asyncio.StreamReader, log: Path | None = None) -> None:
    while line := await stream.readline():
        echo(who, prefix, [line.decode(errors="replace").rstrip("\r\n")], log)


async def follow_file(who: str, prefix: str, path: Path, log: Path | None = None) -> None:
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
            *lines, partial = re.split(r"\r?\n", partial + data.decode(errors="replace"))
            echo(who, prefix, lines, log)
        await asyncio.sleep(0.5)


@dataclass
class Serving:
    """An `ovrd connect` serving the initiator, however it dialed in, and the task streaming its log."""

    process: psutil.Process
    log: asyncio.Task

    def alive(self) -> bool:
        try:
            return self.process.status() != psutil.STATUS_ZOMBIE
        except psutil.NoSuchProcess:
            return False

    async def stop(self) -> None:
        await asyncio.sleep(1)  # let the log catch up
        self.log.cancel()
        if not self.alive():
            return
        self.process.terminate()
        try:
            await asyncio.to_thread(self.process.wait, 30)
        except psutil.TimeoutExpired:
            self.process.kill()


async def connect_paired(who: str, key: str, log: Path) -> Serving:
    """Pairs with the code the initiator encrypted to this host's key, and serves in the foreground."""
    decrypted = await asyncio.to_thread(
        subprocess.run,
        ["openssl", "pkeyutl", "-decrypt", "-inkey", key, "-pkeyopt", "rsa_padding_mode:oaep", "-in", f"pairs/{who}"],
        capture_output=True,
        check=True,
    )
    plain = decrypted.stdout.decode().split()
    if len(plain) != 2:
        raise SystemExit(f"no pairing for {who}")
    addr, code = plain
    say(who, f"pairing with the initiator at {addr}")
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
    return Serving(psutil.Process(ovrd.pid), asyncio.create_task(pipe_lines(who, "ovrd", ovrd.stdout, log)))


def trusted_state() -> Path:
    """Where the action this job used keeps ovrd's state and log."""
    return Path(os.environ["ovr_action_log"]).parent


async def attach_trusted(who: str, log: Path) -> Serving:
    """Follows the ovrd this job's uses of the action started in the background. Only the first dials in, so there
    is exactly one."""
    fingerprint = Path("pairs/trust").read_text().split()[1]
    background = [
        process
        for process in psutil.process_iter(["cmdline"])
        if f"github:{fingerprint}" in (process.info["cmdline"] or []) and "--foreground" in process.info["cmdline"]
    ]
    if len(background) != 1:
        raise SystemExit(f"expected one ovrd serving in the background, found {len(background)}")
    say(who, f"serving as {escape(' '.join(background[0].info['cmdline'][1:]))}")
    tail = asyncio.create_task(follow_file(who, "ovrd", Path(os.environ["ovr_action_log"]), log))
    return Serving(background[0], tail)


def cmd_await(args: argparse.Namespace) -> None:
    asyncio.run(await_artifact(args.host, args.name, Path(args.name), args.timeout))


def cmd_serve(args: argparse.Namespace) -> None:
    """Dials in as a paired host, or follows the trusted job's connection, and serves the initiator, streaming
    ovrd's log and every run, until it uploads `finished`."""

    async def main() -> None:
        who = args.host
        if who.endswith("-paired"):
            if not args.key:
                raise SystemExit("a paired host needs --key")
            serving = await connect_paired(who, args.key, Path(args.log))
            runs = runs_dir()
        else:
            serving = await attach_trusted(who, Path(args.log))
            runs = trusted_state() / "runs"
        following = asyncio.create_task(follow_runs(who, runs))
        try:
            await await_artifact(who, "finished", Path("finished"), args.serve_timeout, serving.alive)
        finally:
            following.cancel()
            alive = serving.alive()
            await serving.stop()
        if not alive:
            raise SystemExit("ovrd exited before the initiator finished; its log is above")

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
    """Waits until every host is online, and records each one's name. A paired host is named after its machine; a
    trusted one after its job, `host`, which every host job shares, and the OS it names itself after."""
    job = f"{os.environ['GITHUB_REPOSITORY']}/host"
    deadline = time.monotonic() + args.timeout
    while True:
        listing = run(["ovr", "hosts"], check=False)
        hosts = {}
        for line in listing.splitlines():
            parts = line.split()
            if len(parts) != 3 or parts[1] != "online":
                continue
            name, os_name = parts[0], parts[2].split("/")[0]
            if name.startswith(f"{job}/"):
                # A trusted job names itself after its OS; any other name means it dialed in twice.
                hosts[f"{os_name}-trusted" if name == f"{job}/{os_name}" else name] = name
            elif "/" not in name:
                hosts[f"{os_name}-paired"] = name
        if extra := set(hosts) - set(HOSTS):
            raise SystemExit(f"unexpected hosts {', '.join(sorted(extra))}:\n{listing}")
        if set(hosts) == set(HOSTS):
            say("initiator", f"every host is online:\n{escape(listing)}")
            Path(args.out).write_text(json.dumps({host: hosts[host] for host in HOSTS}, indent=2))
            return
        if time.monotonic() > deadline:
            raise SystemExit(f"hosts did not all dial in as {', '.join(HOSTS)}:\n{listing}")
        time.sleep(5)


def cmd_check(args: argparse.Namespace) -> None:
    """Checks on every host at once that the working tree arrives, that a change to it does too, and that a
    command's exit code comes back."""
    hosts = json.loads(Path(args.hosts).read_text())
    root, logs = Path(args.dir), Path(args.logs)
    root.mkdir(parents=True, exist_ok=True)
    logs.mkdir(parents=True, exist_ok=True)
    run(["git", "init", "-q"], cwd=root)
    (root / "README").write_text("e2e\n")
    run(["git", "add", "README"], cwd=root)
    run(["git", "-c", "user.name=e2e", "-c", "user.email=e2e@localhost", "commit", "-qm", "init"], cwd=root)
    repo = {"path": str(root), "workdir": "", "name": "check"}

    async def check(host: str) -> list[str]:
        failed = []

        async def step(name: str, argv: list[str], want_code: int, want_output: str = "") -> None:
            code, _, output = await ovr(host, hosts[host], repo, name, argv, logs, args.step_timeout)
            if code != want_code:
                failed.append(f"{host} {name}: exit {code}, not {want_code}")
            elif want_output not in output:
                failed.append(f"{host} {name}: content differs")

        # An untracked file, checked by hash, proves the working tree arrives, not just what is committed.
        marker = root / f"marker-{host}.txt"
        for name in ("first sync", "sync, one file changed"):
            marker.write_text(secrets.token_hex(32))
            await step(name, ["git", "hash-object", marker.name], 0, run(["git", "hash-object", marker.name], cwd=root))
        await step("exit code", ["git", "hash-object", "missing"], 128)
        return failed

    async def main() -> list[str]:
        return [failure for failures in await asyncio.gather(*(check(host) for host in HOSTS)) for failure in failures]

    failed = asyncio.run(main())
    if failed:
        raise SystemExit("failed:\n" + "\n".join(failed))
    say("initiator", f"every check passed on {', '.join(HOSTS)}")


async def ovr(
    host: str, name: str, repo: dict, step: str, argv: list[str], logs: Path, timeout: float
) -> tuple[int | str, float, str]:
    """Runs `ovr NAME -- argv` on `host` in the repository, streaming its output; returns its exit code, or
    `timeout` when it was killed for taking longer than `timeout` seconds, its time, and its output."""
    cwd = Path(repo["path"]) / repo["workdir"]
    prefix = f"{repo['name']}: {step}"
    say(host, f"[bold]{escape(prefix)}[/]: $ ovr {name} -- {escape(' '.join(argv))}")
    log = logs / f"{host}-{repo['name']}-{re.sub(r'[^a-z0-9]+', '-', step.lower()).strip('-')}.log"
    start = time.monotonic()
    child = await asyncio.create_subprocess_exec(
        "ovr",
        name,
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
                raw(host, prefix, text)
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
    say(host, f"[bold]{escape(prefix)}[/]: [{style}]exit {code} after {duration(elapsed)}[/]")
    return code, elapsed, "\n".join(lines)


def plan(repos: list[dict], free: int) -> dict[str, str]:
    """Why each repository that does not fit on a host with `free` bytes is skipped."""
    free -= RESERVE
    skipped = {}
    for repo in repos:
        # The host keeps the pack, the checkout and its index with filesystem overhead, and what a build leaves.
        need = int(repo["pack_bytes"] * 1.1 + repo["tree_bytes"] * 1.2 + repo["files"] * 512 + repo["build_gib"] * GIB)
        if need > free:
            skipped[repo["name"]] = f"needs ~{gib(need)}, {gib(max(free, 0))} to spare"
        else:
            free -= need
    return skipped


async def bench_host(
    host: str,
    name: str,
    repos: list[dict],
    skipped: dict[str, str],
    unchanged: dict[str, asyncio.Barrier],
    logs: Path,
    results: list[dict],
    save,
    timeout: float,
) -> None:
    for repo in repos:
        row = {"host": host, "name": name, "repo": repo["name"], "steps": {}, "ok": True}
        results.append(row)
        if repo["name"] in skipped:
            row["skipped"] = skipped[repo["name"]]
            say(host, f"skipping [bold]{repo['name']}[/]: {row['skipped']}")
            save()
            continue

        async def step(step_name: str, argv: list[str], repo: dict = repo, row: dict = row) -> tuple[int, str]:
            code, elapsed, output = await ovr(host, name, repo, step_name, argv, logs, timeout)
            row["steps"][step_name] = {"seconds": elapsed, "code": code}
            row["ok"] &= code == 0
            save()
            return code, output

        await step("first sync", ["git", "--version"])
        await step("sync, nothing changed", ["git", "--version"])
        # Every host shares the working tree, so none adds its file until all have synced it unchanged.
        await unchanged[repo["name"]].wait()
        # A new untracked file, checked by hash, proves the working tree arrives, not just what is committed.
        marker = Path(repo["path"]) / repo["workdir"] / f"ovr-e2e-{host}.txt"
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


def cmd_bench(args: argparse.Namespace) -> None:
    hosts = json.loads(Path(args.hosts).read_text())
    repos = json.loads(Path(args.repos).read_text())
    logs = Path(args.logs)
    logs.mkdir(parents=True, exist_ok=True)
    skipped = {
        host: plan(repos, json.loads((Path(args.specs) / host / "specs.json").read_text())["disk_free_bytes"])
        for host in HOSTS
    }

    out = Path(args.out)
    results: list[dict] = []

    def save() -> None:
        """Records every result so far, so a run cut short still has a report."""
        out.write_text(json.dumps(sorted(results, key=lambda row: HOSTS.index(row["host"])), indent=2))

    async def main() -> None:
        daemon_log = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state") / "ovr/ovrd.log"
        tail = asyncio.create_task(follow_file("initiator", "ovrd", daemon_log))
        unchanged = {
            repo["name"]: asyncio.Barrier(max(1, sum(repo["name"] not in skipped[host] for host in HOSTS)))
            for repo in repos
        }
        try:
            await asyncio.gather(
                *(
                    bench_host(
                        host,
                        hosts[host],
                        repos,
                        skipped[host],
                        unchanged,
                        logs,
                        results,
                        save,
                        args.step_timeout,
                    )
                    for host in HOSTS
                )
            )
        finally:
            tail.cancel()

    asyncio.run(main())
    save()
    failed = [f"{row['host']}/{row['repo']}" for row in results if not row["ok"]]
    if failed:
        raise SystemExit(f"failed: {', '.join(failed)}")


# --- report -----------------------------------------------------------------------------------------------------


def cmd_report(args: argparse.Namespace) -> None:
    specs_dir = Path(args.specs)
    specs = {
        name: json.loads((specs_dir / name / "specs.json").read_text())
        for name in ("initiator", *HOSTS)
        if (specs_dir / name / "specs.json").exists()
    }
    repos = json.loads(Path(args.repos).read_text()) if Path(args.repos).exists() else []
    results = json.loads(Path(args.results).read_text()) if Path(args.results).exists() else []
    by_repo = {repo["name"]: repo for repo in repos}

    md = [
        "# ovr tunnel bench",
        "",
        (
            f"An initiator runner synced each repository to every host ({', '.join(HOSTS)}) at once through one "
            "Cloudflare quick tunnel. A first sync covers snapshotting, packing, the transfer, indexing, and the "
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
    for host in HOSTS:
        host_rows = [row for row in results if row["host"] == host]
        if not host_rows:
            continue
        md += [
            "",
            f"## {host} host",
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
    failed = [f"{row['host']}/{row['repo']}" for row in results if not row["ok"]]
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
    p.add_argument("--disk", default=str(checkout_root()), help="where to measure free disk")
    p.add_argument("--out", required=True)
    p.set_defaults(func=cmd_specs)

    p = sub.add_parser("pair", help="initiator: let every host dial in through a quick tunnel")
    p.add_argument("--specs", default="specs", help="where hosts' keys and specs are downloaded")
    p.add_argument("--out", default="pairs")
    p.add_argument("--timeout", type=float, default=3600)
    p.set_defaults(func=cmd_pair)

    p = sub.add_parser("await", help="host: download an artifact the initiator uploads, into a directory of its name")
    p.add_argument("--host", required=True, choices=HOSTS)
    p.add_argument("name")
    p.add_argument("--timeout", type=float, default=2 * 3600)
    p.set_defaults(func=cmd_await)

    p = sub.add_parser("serve", help="host: serve the initiator until it has finished")
    p.add_argument("--host", required=True, choices=HOSTS)
    p.add_argument("--key", help="a paired host's private key, whose public half it published")
    p.add_argument("--log", default="ovrd.log")
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

    p = sub.add_parser("check", help="initiator: check that syncs and exit codes reach every host")
    p.add_argument("--hosts", default="hosts.json")
    p.add_argument("--dir", default=str(Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "check"))
    p.add_argument("--logs", default="logs")
    p.add_argument("--step-timeout", type=float, default=300, help="seconds before a step is killed")
    p.set_defaults(func=cmd_check)

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
