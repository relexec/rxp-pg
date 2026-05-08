# ==== Go targets and helper functions ====

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] && [ "$$(readlink -- "$(1)" 2>/dev/null)" = "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
rm -f "$(1)" ;\
GOBIN="$(BIN_DIR)" go install $${package} ;\
mv "$(BIN_DIR)/$$(basename "$(1)")" "$(1)-$(3)" ;\
} ;\
ln -sf "$$(realpath "$(1)-$(3)")" "$(1)"
endef

##@ Go development

.PHONY: go-fmt
go-fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: go-vet
go-vet: ## Run go vet against code.
	go vet ./...
