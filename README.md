# bson2csv

Convert large BSON dump files (e.g. produced by `mongodump`) to CSV, with
field selection and filtering. Written in Rust, designed to stream files of
arbitrary size (tens or hundreds of GB) while making use of every CPU core.

## Features

- **Streaming reader**: documents are decoded one by one, memory usage stays
  bounded regardless of the input file size.
- **Parallel processing**: a dedicated reader thread feeds bounded channels;
  worker chunks are processed in parallel with [rayon](https://github.com/rayon-rs/rayon),
  preserving document order in the output.
- **Field selection with dotted paths**: `user.address.city` walks nested
  documents, `tags.0` indexes into arrays.
- **Filtering**:
  - `--filter key=value` for a simple equality filter.
  - `--filter-in key=path/to/file.txt` for set-membership filtering
    (the file is loaded once as an in-memory hash set). Can be repeated
    (AND semantics).
- **Configurable CSV output**: custom delimiter, optional header row,
  output to a file or stdout.
- **Backpressure**: bounded channels cap peak memory at roughly
  `chunk_size × 4` documents.
- **Multi-arch builds**: native macOS builds via `cargo`, static musl Linux
  binaries (amd64 / arm64) via [`cross`](https://github.com/cross-rs/cross),
  ~5 MB Alpine Docker images.

## Installation

### From source

Requires Rust 1.70+.

```sh
cargo build --release
# binary at ./target/release/bson2csv
```

### Pre-built binaries

The `Makefile` produces release binaries for macOS (arm64 / x86_64 /
universal) and Linux (amd64 / arm64, static musl):

```sh
make dist-all
ls dist/
```

See [BUILD.md](./BUILD.md) for the complete multi-architecture build guide,
including Docker image production.

## Usage

```
bson2csv --input <FILE> --field <FIELD> [--field <FIELD>...] [OPTIONS]
```

### Options

| Flag                       | Description                                                                    |
| -------------------------- | ------------------------------------------------------------------------------ |
| `-i, --input <FILE>`       | Path to the input BSON file (concatenated documents, `mongodump` format).      |
| `-o, --output <FILE>`      | Output CSV file. Defaults to stdout.                                           |
| `-f, --field <FIELD>`      | Field to extract, in dotted notation. Repeat or comma-separate. **Required.** |
| `--filter <KEY=VALUE>`     | Keep only documents where `KEY` equals `VALUE` (string comparison).            |
| `--filter-in <KEY=PATH>`   | Keep only documents whose `KEY` value belongs to the given file (one per line). Repeatable. |
| `--delimiter <CHAR>`       | CSV delimiter (default: `,`).                                                  |
| `--no-header`              | Do not write the header row.                                                   |
| `--chunk-size <N>`         | Documents processed per chunk (default: `10000`).                              |
| `--threads <N>`            | Worker thread count (default: number of CPU cores).                            |
| `--progress-every <N>`     | Report progress on stderr every N documents (default: `1000000`, `0` to disable). |

### Examples

Extract three fields to a CSV file:

```sh
bson2csv \
    --input dump.bson \
    --output users.csv \
    --field _id \
    --field user.name \
    --field user.age
```

Combined equality filter and set-membership filter:

```sh
bson2csv \
    -i dump.bson \
    -o active_selected.csv \
    -f _id -f user.name -f status \
    --filter status=active \
    --filter-in _id=./ids_to_keep.txt
```

Pipe to stdout with a tab delimiter and no header:

```sh
bson2csv -i dump.bson -f _id -f user.name \
    --delimiter $'\t' --no-header > out.tsv
```

### Nested paths

Given a document like:

```json
{
  "_id": 42,
  "user": { "name": "alice", "age": 30 },
  "tags": ["admin", "beta"]
}
```

- `_id` -> `42`
- `user.name` -> `alice`
- `tags.0` -> `admin`

Non-scalar values (sub-documents, arrays) that are selected directly are
serialized as JSON in a single CSV cell, so no information is lost.

## Docker

A minimal Alpine-based runtime image is provided. It only wraps the
pre-built static musl binary (no Rust toolchain inside), so images stay
around 5 MB.

```sh
make docker-amd64                       # bson2csv:<version>-amd64
docker run --rm -v "$PWD:/data" \
    bson2csv:<version>-amd64 \
    --input /data/dump.bson --output /data/out.csv --field _id
```

See [BUILD.md](./BUILD.md) for details.

## Generating a sample BSON file

A small example is included for smoke tests:

```sh
cargo run --release --example gen_sample -- sample.bson 10000
bson2csv -i sample.bson -f _id -f user.name -f status
```

## Performance notes

- The reader thread uses a 16 MB `BufReader` and pushes chunks through a
  bounded channel (capacity 2), so disk I/O stays overlapped with CPU work.
- Rayon processes each chunk with `par_iter().collect()`, which preserves
  input order in the output.
- Peak memory is roughly `chunk_size × 4` documents. Increase
  `--chunk-size` for higher throughput on machines with abundant RAM,
  decrease it for tight memory budgets.
- `--filter-in` files are loaded eagerly and shared read-only across
  workers via `Arc<HashSet<String>>`, so lookups are O(1) and allocation-free.

## License

See [LICENSE](./LICENSE).
