# ==== Test targets ====

TEST_CLUSTER_NAME ?= rxp-pg-test
TEST_KUBE_CONTEXT ?= kind-$(TEST_CLUSTER_NAME)

TEST_BUILD_DIR ?= $(BUILD_DIR)/$(TEST_CLUSTER_NAME)
$(TEST_BUILD_DIR):
	@mkdir -p "$(TEST_BUILD_DIR)"

TEST_RUN_DIR ?= $(BUILD_DIR)/$(TEST_CLUSTER_NAME)/run
$(TEST_RUN_DIR):
	@mkdir -p "$(TEST_RUN_DIR)"

KIND ?= $(BIN_DIR)/kind
KIND_VERSION ?= v0.31.0
$(KIND): | $(BIN_DIR)
	@echo -n "installing kind@$(KIND_VERSION) ... "
	@$(call go-install-tool,$(KIND),sigs.k8s.io/kind,$(KIND_VERSION))
	@echo "ok."

GOOSE ?= $(BIN_DIR)/goose
GOOSE_VERSION ?= latest
$(GOOSE): | $(BIN_DIR)
	@echo -n "installing goose@$(GOOSE_VERSION) ... "
	@$(call go-install-tool,$(GOOSE),github.com/pressly/goose/v3/cmd/goose,$(GOOSE_VERSION))
	@echo "ok."

TEST_PG_NAMESPACE ?= postgresql
TEST_PG_SERVICE ?= postgresql
TEST_PG_DB ?= rxptest

##@ Test

.PHONY: test
test: test-unit ## Run all tests.

.PHONY: test-unit
test-unit: ## Run all unit tests.
	@go test -race -count=1 -v ./...

.PHONY: test-cluster-status
test-cluster-status: ## Show status of test cluster.
	@echo "============================== Local testing environment ========================================"
	@if ! command -v helm >/dev/null 2>&1; then \
		echo "> helm installed? 		NO"; \
		echo "Please install Helm v3+"; \
		echo "https://helm.sh/docs/v4/intro/install"; \
		exit 1; \
	fi
	@echo "> helm installed? 			YES (`helm version --short`)"
	@if ! kind get clusters | grep -q "${TEST_CLUSTER_NAME}" >/dev/null 2>&1; then \
		echo "> kind cluster running? 		NO"; \
		echo "Run \`make test-cluster-create\` to create the test cluster."; \
		exit 1; \
	fi
	@echo "> kind cluster running? 		YES ('${TEST_CLUSTER_NAME}')"
	@PG_IP=$(shell kubectl --context "${TEST_KUBE_CONTEXT}" get service --namespace "${TEST_PG_NAMESPACE}" "${TEST_PG_SERVICE}" -o=jsonpath='{.spec.clusterIP}'); \
	if [ -z "$$PG_IP" ]; then \
		echo "> postgres running? 			NO"; \
		echo "Run \`make test-postgres-install\` to install postgres in the test cluster."; \
		exit 1; \
	else \
		echo "> postgres running? 			YES ($$PG_IP)"; \
	fi

.PHONY: test-cluster-delete
test-cluster-delete: $(KIND) ## Delete the test cluster.
	@echo -n "deleting '${TEST_CLUSTER_NAME}' kind cluster ... "
	@$(KIND) delete cluster -q -n "${TEST_CLUSTER_NAME}"
	@echo "ok."

.PHONY: test-cluster-create
test-cluster-create: $(KIND) ## Create the test cluster.
	@echo -n "creating '${TEST_CLUSTER_NAME}' kind cluster ... "
	@$(KIND) create cluster -q -n "${TEST_CLUSTER_NAME}"
	@echo "ok."
	@sleep 5

.PHONY: test-cluster-reset
test-cluster-reset: test-cluster-delete test-cluster-create test-postgres-install ## Reset the test cluster entirely and reinstall PostgreSQL.

# ==== PostgreSQL-related administration ====

.PHONY: test-postgres-install
test-postgres-install: ## Install PostgreSQL in test cluster.
	@echo -n "installing postgresql in '${TEST_CLUSTER_NAME}' kind cluster ... "
	@helm install postgresql bitnami/postgresql \
		--kube-context "kind-${TEST_CLUSTER_NAME}" \
		--namespace "$(TEST_PG_NAMESPACE)" \
		--create-namespace \
		--set commonLabels.app=postgresql \
		--set auth.postgresPassword=postgres \
		--set image.repository=bitnamilegacy/postgresql \
		--set global.security.allowInsecureImages=true >/dev/null 2>&1
	@echo "ok."
	@echo -n "waiting for postgresql to be ready ... "
	@sleep 10
	@kubectl wait --for=condition=Ready pod -l app=postgresql -n "$(TEST_PG_NAMESPACE)" --timeout=60s >/dev/null 2>&1
	@echo "ok."

.PHONY: test-postgres-cluster-ip
test-postgres-cluster-ip: ## Show PostgreSQL service IP in test cluster.
	@PG_IP=$(shell kubectl --context "${TEST_KUBE_CONTEXT}" get service --namespace "${TEST_PG_NAMESPACE}" "${TEST_PG_SERVICE} -o=jsonpath='{.spec.clusterIP}'); \
		echo "postgresql running on $$PG_IP"

POSTGRES_PIDFILE=$(TEST_RUN_DIR)/.postgres-portforward.pid
.PHONY: test-postgres-port-forward
test-postgres-port-forward: $(TEST_RUN_DIR) test-postgres-port-forward-stop ## Port-forward PostgreSQL service to localhost.
	@echo -n "forwarding postgres service ... "
	@kubectl -n postgresql port-forward service/$(TEST_PG_SERVICE) 5432:5432 >/dev/null 2>&1 & echo $$! > $(POSTGRES_PIDFILE)
	@echo "ok."
	@echo "connect to the postgresql server with:"
	@echo "  psql -h localhost -p 5432 -U postgres -d temporal"

.PHONY: test-postgres-port-forward-stop
test-postgres-port-forward-stop: $(TEST_RUN_DIR) ## Stop port-forwarding PostgreSQL service to localhost.
	@if [ -f $(POSTGRES_PIDFILE) ]; then \
		echo -n "stopping postgres port-forward ... "; \
		kill $$(cat $(POSTGRES_PIDFILE)) >/dev/null 2>&1; \
		rm -f $(POSTGRES_PIDFILE); \
		echo "ok."; \
	else \
		echo "postgres not being port-forwarded."; \
	fi

.PHONY: test-postgres-db-ensure
test-postgres-db-ensure: ## Ensure test DB exists.
	@echo -n "ensuring '${TEST_PG_DB}' database exists ... "
	@PGPASSWORD=postgres psql -U postgres -h localhost -tc "SELECT 1 FROM pg_database WHERE datname = '${TEST_PG_DB}'" | \
    grep -q 1 || \
    PGPASSWORD=postgres psql -U postgres -h localhost -c "CREATE DATABASE ${TEST_PG_DB}" >/dev/null 2>&1
	@echo "ok."

.PHONY: test-postgres-db-create
test-postgres-db-create: ## Create the test DB.
	@echo -n "creating '${TEST_PG_DB}' database ... "
	@PGPASSWORD=postgres psql -U postgres -h localhost -c "CREATE DATABASE ${TEST_PG_DB}" >/dev/null 2>&1
	@echo "ok."

.PHONY: test-postgres-db-delete
test-postgres-db-delete: ## Delete the test DB.
	@echo -n "deleting '${TEST_PG_DB}' database ... "
	@PGPASSWORD=postgres psql -U postgres -h localhost -c "DROP DATABASE IF EXISTS ${TEST_PG_DB}" >/dev/null 2>&1
	@echo "ok."

.PHONY: test-postgres-db-reset
test-postgres-db-reset: test-postgres-db-delete test-postgres-db-up ## Recreate the test DB.

.PHONY: test-postgres-db-up
test-postgres-db-up: test-postgres-db-ensure ## Run test DB schema migrations.
	@echo -n "running schema migrations for '${TEST_PG_DB}' database ... "
	@GOOSE_DRIVER=postgres \
	GOOSE_DBSTRING=postgres://postgres:postgres@localhost:5432/${TEST_PG_DB} \
	GOOSE_MIGRATION_DIR=migrations \
	goose up >/dev/null 2>&1
	@echo "ok."
