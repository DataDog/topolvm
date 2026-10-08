# Datadog-only Go module. Independent of the upstream TopoLVM Makefile; the
# parent Dockerfile.dd and .gitlab-ci.yml invoke it with `make -C datadog`.

SHELL = /bin/bash
.SHELLFLAGS = -e -o pipefail -c

BINDIR := $(CURDIR)/bin
BUILDDIR := $(CURDIR)/build

GOLANGCI_LINT_VERSION := v2.11.4
GOLANGCI_LINT := $(BINDIR)/golangci-lint

GOARCH ?= $(shell go env GOARCH)
VERSION ?= devel
LDFLAGS := -w -s -X github.com/DataDog/topolvm/datadog/cmd/topolvm-capacity-template-controller/app.Version=$(VERSION)

# Consumed by the envtest tests, which download kube-apiserver/etcd themselves.
ENVTEST_KUBERNETES_VERSION ?= 1.35.0
ENVTEST_ASSETS_DIR ?= $(CURDIR)/testbin
export ENVTEST_KUBERNETES_VERSION
export ENVTEST_ASSETS_DIR

BINARIES := topolvm-capacity-template-controller

.PHONY: all
all: lint test build

.PHONY: build
build: $(addprefix $(BUILDDIR)/,$(BINARIES))

# Always invoke go build; its cache decides whether anything changed.
$(BUILDDIR)/%: FORCE
	mkdir -p $(BUILDDIR)
	GOARCH=$(GOARCH) CGO_ENABLED=0 go build -o $@ -ldflags "$(LDFLAGS)" ./cmd/$*

.PHONY: FORCE
FORCE:

.PHONY: test
test:
	go test -count=1 -race --timeout=300s ./...

.PHONY: lint
lint: $(GOLANGCI_LINT)
	test -z "$$(gofmt -s -l . | tee /dev/stderr)"
	$(GOLANGCI_LINT) run
	go vet ./...

.PHONY: lint-fix
lint-fix: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --fix

$(GOLANGCI_LINT):
	mkdir -p $(BINDIR)
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(BINDIR) $(GOLANGCI_LINT_VERSION)

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)

.PHONY: distclean
distclean: clean
	rm -rf $(BINDIR) $(ENVTEST_ASSETS_DIR)
