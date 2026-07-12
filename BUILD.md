# Building bson2csv for multiple architectures

`bson2csv` is a pure-Rust CLI. This document explains how to produce release
binaries for:

| Target                        | Rust triple                    | Notes                             |
| ----------------------------- | ------------------------------ | --------------------------------- |
| macOS Apple Silicon           | `aarch64-apple-darwin`         | Native on M1/M2/M3 machines       |
| macOS Intel                   | `x86_64-apple-darwin`          | Cross-compiled on Apple Silicon   |
| Linux amd64 (Intel/AMD)       | `x86_64-unknown-linux-musl`    | Static musl, runs in any container|
| Linux arm64 (ARM servers, M1) | `aarch64-unknown-linux-musl`   | Static musl, runs in any container|

Linux builds are produced with [`cross`](https://github.com/cross-rs/cross),
which runs the correct C/Rust toolchain inside a Docker container. You do not
need to install any Linux cross-compiler on your host.

## Prerequisites

- Rust (1.70+): <https://rustup.rs>
- Docker Desktop (or any Docker daemon) — required by `cross` and for Docker images
- The `cross` binary:

```sh
make install-cross
# equivalent to:
#   cargo install cross --git https://github.com/cross-rs/cross
```

For macOS Intel builds from an Apple Silicon Mac, add the rustup target once:

```sh
make add-targets
```

## Common commands

Everything is exposed through the `Makefile`. Run `make help` for the full
list.

Every build target copies its final binary into `./dist/` with an unambiguous
name, so you can just `cp` it wherever you need.

```sh
# macOS
make build-mac-arm          # -> dist/bson2csv-macos-arm64
make build-mac-x86          # -> dist/bson2csv-macos-x86_64
make build-mac-universal    # -> dist/bson2csv-macos-universal (fat binary)

# Linux (via cross + Docker)
make build-linux-amd64      # -> dist/bson2csv-linux-amd64
make build-linux-arm64      # -> dist/bson2csv-linux-arm64
make build-linux            # both of the above

# Everything at once
make dist-all
```

Then copy the binary manually wherever it should live, for example:

```sh
cp dist/bson2csv-macos-arm64 /usr/local/bin/bson2csv
# or, to send it to a Linux server:
scp dist/bson2csv-linux-amd64 user@host:/usr/local/bin/bson2csv
```

The Linux binaries are statically linked against musl, so they run in any
Linux container, including `scratch`, `distroless` and Alpine.

## Docker

A minimal runtime `Dockerfile` is provided. It does **not** compile Rust: it
simply copies the binary produced by `cross` into an Alpine image. This keeps
the image around ~5 MB and the build under a second.

```sh
# Build the amd64 image (works on any host thanks to buildx + qemu)
make docker-amd64
# -> bson2csv:<version>-amd64

# Build the arm64 image
make docker-arm64
# -> bson2csv:<version>-arm64
```

Custom image name/tag:

```sh
make docker-amd64 IMAGE=ghcr.io/ptitcoutu/bson2csv TAG=latest
```

### Running the container

Mount your data directory into `/data` (the container's working directory):

```sh
docker run --rm -v "$PWD:/data" bson2csv:<version>-amd64 \
    --input /data/dump.bson --output /data/out.csv
```

Force amd64 execution on an Apple Silicon host (uses qemu emulation):

```sh
docker run --rm --platform linux/amd64 -v "$PWD:/data" \
    bson2csv:<version>-amd64 --help
```

## Troubleshooting

- **`cross` fails with a Docker permission error** — ensure Docker Desktop is
  running and that your user can talk to the Docker socket.
- **`error: linker cc not found`** when building without `cross` — that means
  you tried `cargo build --target x86_64-unknown-linux-musl` directly. Use
  `make build-linux-amd64` (which delegates to `cross`) instead.
- **`exec format error` when running the container** — the image architecture
  does not match the host. Use the `-amd64` / `-arm64` tag that matches, or
  pass `--platform` to `docker run`.
