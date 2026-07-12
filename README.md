# bson2csv

Convert large BSON dump files (e.g. produced by `mongodump`) to CSV or
Parquet, with field selection, filtering, output splitting and an optional
post-chunk upload hook (GCS, S3, ...). Written in Rust, designed to stream
files of arbitrary size (tens or hundreds of GB) while making use of every
CPU core.

## Features

- **Streaming reader**: documents are decoded one by one, memory usage stays
  bounded regardless of the input file size.
- **Parallel processing**: a dedicated reader thread feeds bounded channels;
  worker chunks are processed in parallel with [rayon](https://github.com/rayon-rs/rayon),
  preserving document order in the output.
- **Field selection with dotted paths**: `user.address.city` walks nested
  documents, `tags.0` indexes into arrays. Optional column aliasing with
  `path=alias`.
- **Filtering**:
  - `--filter key=value` for a simple equality filter.
  - `--filter-in key=path/to/file.txt` for set-membership filtering
    (the file is loaded once as an in-memory hash set). Can be repeated
    (AND semantics).
- **Multiple output formats**:
  - **CSV** with a custom delimiter, optional header row, file or stdout.
  - **Parquet** with a typed schema and configurable compression
    (snappy, zstd, gzip, lz4, none).
- **Typed columns**: `--field-type path:type` coerces values into
  `string` (default), `int32`, `int64`, `double`, `bool`, or `timestamp`
  (ms since epoch, UTC).
- **Output splitting**: `--rows-per-file N` writes multiple chunk files
  (`prefix-00000.csv`, `prefix-00001.csv`, ...) instead of a single output.
- **Post-chunk upload hook**: `--upload-cmd 'gsutil cp {file} gs://bucket/'`
  is executed after each chunk file is closed. Combined with
  `--remove-after-upload`, this streams a large export directly to GCS / S3
  / any command-driven backend without ever needing local space for the
  full result.
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

| Flag                        | Description                                                                    |
| --------------------------- | ------------------------------------------------------------------------------ |
| `-i, --input <FILE>`        | Path to the input BSON file (concatenated documents, `mongodump` format).      |
| `-o, --output <FILE>`       | Output file. With `--rows-per-file > 0` it is treated as a prefix. Defaults to stdout (CSV only). Required for Parquet or when rotating. |
| `--format <FMT>`            | Output format: `csv` (default) or `parquet`.                                   |
| `-f, --field <PATH[=ALIAS]>` | Field to extract, in dotted notation. Optional `=alias` renames the column. Repeat or comma-separate. **Required.** |
| `--field-type <NAME:TYPE>`  | Type coercion for a column. Types: `string` (default), `int32`, `int64`, `double`, `bool`, `timestamp`. Repeatable. |
| `--filter <KEY=VALUE>`      | Keep only documents where `KEY` equals `VALUE` (string comparison).            |
| `--filter-in <KEY=PATH>`    | Keep only documents whose `KEY` value belongs to the given file (one per line). Repeatable. |
| `--delimiter <CHAR>`        | CSV delimiter (default: `,`).                                                  |
| `--no-header`               | Do not write the header row (CSV only).                                        |
| `--compression <CODEC>`     | Parquet compression: `snappy` (default), `zstd`, `gzip`, `lz4`, `none`.        |
| `--rows-per-file <N>`       | Split output into files of at most N kept rows. `0` disables rotation.         |
| `--upload-cmd <CMD>`        | Shell command run after each chunk is closed. Placeholders: `{file}`, `{index}`. |
| `--remove-after-upload`     | Delete the local chunk file after `--upload-cmd` returns successfully.         |
| `--chunk-size <N>`          | Documents processed per chunk (default: `10000`).                              |
| `--threads <N>`             | Worker thread count (default: number of CPU cores).                            |
| `--progress-every <N>`      | Report progress on stderr every N documents (default: `1000000`, `0` to disable). |

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

Export to a single Parquet file with a typed schema:

```sh
bson2csv \
    -i dump.bson -o users.parquet --format parquet \
    -f _id \
    -f 'user.name=user_name' \
    -f 'user.age=user_age' \
    -f created_at \
    --field-type _id:int64,user_age:int32,created_at:timestamp \
    --compression zstd
```

Split into 1M-row Parquet chunks and stream them straight to a GCS bucket
(no full local copy is ever kept):

```sh
bson2csv \
    -i dump.bson -o users --format parquet \
    -f _id -f 'user.name=user_name' -f status \
    --field-type _id:int64 \
    --rows-per-file 1000000 \
    --upload-cmd 'gsutil cp {file} gs://my-bucket/exports/' \
    --remove-after-upload
```

Same thing to S3 via the AWS CLI:

```sh
bson2csv \
    -i dump.bson -o users --format parquet \
    -f _id -f status --field-type _id:int64 \
    --rows-per-file 1000000 \
    --upload-cmd 'aws s3 cp {file} s3://my-bucket/exports/' \
    --remove-after-upload
```

### Output splitting and upload hook

When `--rows-per-file` is greater than zero, `--output` is used as a prefix
and each chunk file gets a zero-padded index and the format extension:
`users-00000.parquet`, `users-00001.parquet`, ...

After each chunk file is fully written and closed, the string given to
`--upload-cmd` is executed via the system shell (`sh -c` on Unix,
`cmd /C` on Windows). Two placeholders are substituted:

- `{file}` -> the chunk path
- `{index}` -> the zero-padded chunk index (e.g. `00042`)

A non-zero exit status aborts the run. Because the reader / worker /
writer pipeline uses bounded channels, memory stays flat even when the
upload command is slow: the whole export naturally paces itself against
the upload throughput.

### Column names for Parquet

Dotted field paths (`user.name`) are perfectly valid Parquet column names
at the storage level, but some downstream tools (Avro-based readers,
BigQuery ingestion, ...) reject dots in column names. Use the `path=alias`
form of `--field` to rename columns:

```sh
-f 'user.address.city=city' -f 'tags.0=first_tag'
```

`--field-type` accepts either the path or the alias.

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

For Parquet output, non-string columns are best-effort coerced according to
`--field-type`. Uncoercible or missing values become Parquet nulls, so a
single malformed document does not abort a multi-hour export.

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
