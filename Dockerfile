# Minimal runtime image that wraps a pre-built static musl binary.
#
# The binary is expected to be built beforehand via `make build-linux-amd64`
# or `make build-linux-arm64` (which invoke `cross`). This keeps the image
# tiny (~5 MB) and the Docker build very fast: no Rust toolchain inside.
#
# Usage:
#   make docker-amd64          # builds and tags bson2csv:<version>-amd64
#   make docker-arm64          # builds and tags bson2csv:<version>-arm64
#
# Or manually:
#   docker buildx build --platform linux/amd64 \
#       --build-arg TARGET=x86_64-unknown-linux-musl \
#       -t bson2csv:latest --load .
#
# Run:
#   docker run --rm -v "$PWD:/data" bson2csv:latest \
#       --input /data/dump.bson --output /data/out.csv

ARG TARGET=x86_64-unknown-linux-musl

FROM alpine:3.20 AS runtime

# Non-root user for safer execution
RUN addgroup -S app && adduser -S -G app app

ARG TARGET
COPY target/${TARGET}/release/bson2csv /usr/local/bin/bson2csv

# Sanity-check that the binary is actually executable on this platform
RUN chmod +x /usr/local/bin/bson2csv && /usr/local/bin/bson2csv --help >/dev/null

USER app
WORKDIR /data

ENTRYPOINT ["/usr/local/bin/bson2csv"]
CMD ["--help"]
