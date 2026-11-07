# ovr

Run your builds over there.

```sh
ovr bigbox -- cargo test
ovr winbox -- dotnet build
```

`ovr HOST -- CMD...` runs `CMD` on another machine against your local, uncommitted working tree and streams the output
back. No configuration.

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

## License

[MIT](LICENSE)
