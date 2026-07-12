## bson2csv - Multi-architecture build helpers
##
## Prerequisites (install once):
##   cargo install cross --git https://github.com/cross-rs/cross
##   Docker running (required by `cross` for Linux targets)
##
## Native macOS builds use `cargo` directly (no Docker needed).
## Linux builds use `cross` to produce fully-static musl binaries that
## run in any Docker container (distroless, alpine, scratch, ...).

BIN         := bson2csv
VERSION     := $(shell awk -F\" '/^version/ {print $$2; exit}' Cargo.toml)
DIST_DIR    := dist

# Targets
TARGET_MAC_ARM   := aarch64-apple-darwin
TARGET_MAC_X86   := x86_64-apple-darwin
TARGET_LINUX_AMD := x86_64-unknown-linux-musl
TARGET_LINUX_ARM := aarch64-unknown-linux-musl

# Docker image name/tag
IMAGE       ?= bson2csv
TAG         ?= $(VERSION)

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":[^#]*## "} /^[a-zA-Z0-9_-]+:[^#]*## / {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

## --- Tooling -----------------------------------------------------------------

.PHONY: install-cross
install-cross: ## Install the `cross` cross-compilation helper
	cargo install cross --git https://github.com/cross-rs/cross

.PHONY: add-targets
add-targets: ## Add rustup targets for native macOS builds
	rustup target add $(TARGET_MAC_ARM) $(TARGET_MAC_X86)

## Every build target below drops its final binary into $(DIST_DIR)/ with an
## unambiguous name, so you can just `cp dist/bson2csv-<platform> <somewhere>`.

## --- Native (macOS) builds ---------------------------------------------------

.PHONY: build-mac-arm
build-mac-arm: ## Build macOS Apple Silicon (arm64) -> dist/bson2csv-macos-arm64
	cargo build --release --target $(TARGET_MAC_ARM)
	@mkdir -p $(DIST_DIR)
	@cp target/$(TARGET_MAC_ARM)/release/$(BIN) $(DIST_DIR)/$(BIN)-macos-arm64
	@echo ""
	@echo "  Binary ready: $(DIST_DIR)/$(BIN)-macos-arm64"
	@ls -lh $(DIST_DIR)/$(BIN)-macos-arm64

.PHONY: build-mac-x86
build-mac-x86: ## Build macOS Intel (x86_64) -> dist/bson2csv-macos-x86_64
	cargo build --release --target $(TARGET_MAC_X86)
	@mkdir -p $(DIST_DIR)
	@cp target/$(TARGET_MAC_X86)/release/$(BIN) $(DIST_DIR)/$(BIN)-macos-x86_64
	@echo ""
	@echo "  Binary ready: $(DIST_DIR)/$(BIN)-macos-x86_64"
	@ls -lh $(DIST_DIR)/$(BIN)-macos-x86_64

.PHONY: build-mac-universal
build-mac-universal: build-mac-arm build-mac-x86 ## Build a universal macOS binary -> dist/bson2csv-macos-universal
	@mkdir -p $(DIST_DIR)
	lipo -create -output $(DIST_DIR)/$(BIN)-macos-universal \
		target/$(TARGET_MAC_ARM)/release/$(BIN) \
		target/$(TARGET_MAC_X86)/release/$(BIN)
	@echo ""
	@echo "  Binary ready: $(DIST_DIR)/$(BIN)-macos-universal"
	@ls -lh $(DIST_DIR)/$(BIN)-macos-universal

## --- Linux builds via `cross` (Docker required) ------------------------------

.PHONY: build-linux-amd64
build-linux-amd64: ## Build static Linux amd64 (musl) -> dist/bson2csv-linux-amd64
	cross build --release --target $(TARGET_LINUX_AMD)
	@mkdir -p $(DIST_DIR)
	@cp target/$(TARGET_LINUX_AMD)/release/$(BIN) $(DIST_DIR)/$(BIN)-linux-amd64
	@echo ""
	@echo "  Binary ready: $(DIST_DIR)/$(BIN)-linux-amd64"
	@ls -lh $(DIST_DIR)/$(BIN)-linux-amd64

.PHONY: build-linux-arm64
build-linux-arm64: ## Build static Linux arm64 (musl) -> dist/bson2csv-linux-arm64
	cross build --release --target $(TARGET_LINUX_ARM)
	@mkdir -p $(DIST_DIR)
	@cp target/$(TARGET_LINUX_ARM)/release/$(BIN) $(DIST_DIR)/$(BIN)-linux-arm64
	@echo ""
	@echo "  Binary ready: $(DIST_DIR)/$(BIN)-linux-arm64"
	@ls -lh $(DIST_DIR)/$(BIN)-linux-arm64

.PHONY: build-linux
build-linux: build-linux-amd64 build-linux-arm64 ## Build Linux amd64 + arm64

## --- Packaging ---------------------------------------------------------------

.PHONY: dist-all
dist-all: build-mac-universal build-linux ## Build every target into dist/
	@echo ""
	@echo "  All binaries in $(DIST_DIR):"
	@ls -lh $(DIST_DIR)

## --- Docker ------------------------------------------------------------------

.PHONY: docker-amd64
docker-amd64: build-linux-amd64 ## Build a Docker image for linux/amd64
	docker buildx build \
		--platform linux/amd64 \
		--build-arg TARGET=$(TARGET_LINUX_AMD) \
		-t $(IMAGE):$(TAG)-amd64 \
		--load .

.PHONY: docker-arm64
docker-arm64: build-linux-arm64 ## Build a Docker image for linux/arm64
	docker buildx build \
		--platform linux/arm64 \
		--build-arg TARGET=$(TARGET_LINUX_ARM) \
		-t $(IMAGE):$(TAG)-arm64 \
		--load .

.PHONY: docker
docker: docker-amd64 ## Alias: build the amd64 image (most common target)

## --- Cleanup -----------------------------------------------------------------

.PHONY: clean
clean: ## Remove build artefacts
	cargo clean
	rm -rf $(DIST_DIR)
