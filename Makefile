# Top-level Makefile containing make targets that automate build, release,
# deployment and local testing tasks.

GIT_VERSION ?= $(shell git describe --tags --always --dirty || echo "unknown")
GIT_COMMIT ?= $(shell git rev-parse HEAD)

BIN_DIR ?= $(shell pwd)/bin
$(BIN_DIR):
	@mkdir -p "$(BIN_DIR)"

include mk/build.mk
include mk/go.mk
include mk/test.mk
include mk/help.mk
