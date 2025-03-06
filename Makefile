BINARY ?= binwatch
IMG_REG ?= freepik-company

# Image URL to use all building/pushing image targets
IMG ?= ghcr.io/$(IMG_REG)/$(BINARY):latest

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

##@ Build

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	# sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	sed -e 's/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/g' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name project-builder
	$(CONTAINER_TOOL) buildx use project-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm project-builder
	rm Dockerfile.cross

docker-kind-build:
	docker build --tag '$(BINARY):test' .
	kind load docker-image $(BINARY):test

container-build:
	$(CONTAINER_TOOL) build --no-cache --tag ${IMG} -f Dockerfile .

container-push: container-build
	$(CONTAINER_TOOL) push ${IMG}

.PHONY: build
build: fmt vet ## Build manager binary.
	go build -o bin/binwatch cmd/main.go

# Watcher config src path
WATCHER_CONFIG?=
# Watcher subcommand flags
WATCHER_FLAGS?=--config $(WATCHER_CONFIG)

.PHONY: run-watcher
run-watcher: fmt vet ## Run a command from your host (define WATCHER_FLAGS to custom run watcher).
	go run cmd/main.go watcher $(WATCHER_FLAGS)

.PHONY: run-example
run-example:
	@docker compose --file docs/samples/mysql-binlog-watch-row/docker-compose.yml up -d --wait
	@docker exec -i mysql-binlog-watch-row mysql -uroot -pMiContrasenaSegura -e " \
		CREATE DATABASE IF NOT EXISTS test; \
		USE test; \
		CREATE TABLE IF NOT EXISTS test (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100));"

	@(HOSTNAME=test go run cmd/main.go watch --config docs/samples/mysql-binlog-watch-row/config.yaml & echo $$! > /tmp/go_pid.tmp)
	@(HOSTNAME=test2 go run cmd/main.go watch --config docs/samples/mysql-binlog-watch-row/config.yaml & echo $$! > /tmp/go_pid.tmp)

	@sleep 6
	@for i in $$(seq 1 20); do \
		echo "INSERT new row $$i..."; \
		docker exec -i mysql-binlog-watch-row mysql -uroot -pMiContrasenaSegura -e " \
			USE test; \
			INSERT INTO test (name) VALUES ('Example_$$i');" ; \
		echo "UPDATE the row $$i..."; \
		docker exec -i mysql-binlog-watch-row mysql -uroot -pMiContrasenaSegura -e " \
			USE test; \
			UPDATE test SET name='Modified_$$i' WHERE id=$$i;" ; \
		echo "DELETE the row $$i..."; \
		docker exec -i mysql-binlog-watch-row mysql -uroot -pMiContrasenaSegura -e " \
			USE test; \
			DELETE FROM test WHERE id=$$i;" ; \
	done

	@sleep 5
	@if [ -f /tmp/go_pid.tmp ]; then \
		kill -9 `cat /tmp/go_pid.tmp` && rm -f /tmp/go_pid.tmp; \
	fi; \
	ps aux | grep '[g]o-build' | cut -f2 -w | xargs -r kill -9 || true

	@docker compose --file docs/samples/mysql-binlog-watch-row/docker-compose.yml down --volumes --remove-orphans


