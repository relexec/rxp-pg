# ==== Build targets ====

## Location to put temporary build artifacts
BUILD_DIR ?= $(shell pwd)/build
$(BUILD_DIR):
	@mkdir -p "$(BUILD_DIR)"

BUILD_DATE ?= $(shell date +%Y-%m-%dT%H:%M)
BUILD_GIT_VERSION ?= $(GIT_VERSION)
BUILD_GIT_COMMIT ?= $(GIT_COMMIT)

##@ Build

.PHONY: build-clean
build-clean: ## Removes all temporary build artifacts.
	@rm -rf build/
