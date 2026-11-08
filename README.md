# ovr

Run your builds over there.

```sh
ovr bigbox -- cargo test
ovr winbox -- dotnet build
```

`ovr HOST -- CMD...` runs `CMD` on another machine against your local, uncommitted working tree and streams the output
back. No configuration required.

Status: early development. See [docs/DESIGN.md](docs/DESIGN.md).

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/nikhil-pandey/ovr/main/install.sh | sh
```

On Windows:

```powershell
irm https://raw.githubusercontent.com/nikhil-pandey/ovr/main/install.ps1 | iex
```

`.deb` and `.rpm` packages are on the [releases](https://github.com/nikhil-pandey/ovr/releases) page.

## Usage

Any SSH host works as is; `ovr` uploads its agent on first use:

```sh
ovr user@bigbox -- cargo build --release   # run against your working tree, exit with its code
ovr bigbox                                 # a login shell in the synced checkout
ovr -d bigbox -- cargo test                # in the background; prints the run number
ovr ps                                     # runs on every connected host
ovr logs bigbox 12                         # follow a run's output
ovr attach bigbox 12                       # take a run's terminal back
ovr kill bigbox 12
```

Hosts you cannot SSH to dial in instead. Pair one by running the command `ovrd pair` prints on it:

```sh
ovrd pair               # on your machine: prints `ovrd connect ADDR CODE`
ovrd connect ADDR CODE  # on the host: pairs, and serves from then on, at login too
ovr hosts               # dialed-in hosts, online or not
ovr winbox -- dotnet build
```

When the host cannot reach your machine, pair through a tunnel; Cloudflare's needs no account:

```sh
ovrd pair cloudflare    # or devtunnel
ovrd tunnel             # open tunnels; `ovrd tunnel close cloudflare` closes one
```

GitHub Actions jobs of repositories you trust dial in without pairing, through
[the action](docs/DESIGN.md#trusted-workloads):

```sh
ovrd trust github:OWNER/REPO   # on your machine: prints the variables and the step to add
ovr OWNER/REPO/WORKFLOW/RUN/JOB -- make test
```

Remove checkouts and finished runs, keeping anything in use:

```sh
ovr clean bigbox        # yours on bigbox
ovr clean               # everything on this machine
ovr clean --all         # this machine, and yours on every connected host
```

## License

[MIT](LICENSE)
