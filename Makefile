VERSION=$(shell git describe --tags 2>/dev/null)

GOCMD=go
TAGS="sqlite_math_functions"
GOGET=$(GOCMD) get
GOCLEAN=$(GOCMD) clean
ifeq ($(strip $(VERSION)),)
  LDFLAGS=""
else
  LDFLAGS="-X github.com/noborus/trdsql.Version=$(VERSION)"
endif
GOVERSION=$(shell go version)
BUILDFLAG=-tags $(TAGS) -ldflags=$(LDFLAGS)
GOBUILD=$(GOCMD) build $(BUILDFLAG)
GOTEST=$(GOCMD) test -tags $(TAGS) ./...
GOINSTALL=$(GOCMD) install $(BUILDFLAG)
GOOS=$(word 1,$(subst /, ,$(lastword $(GOVERSION))))
GOARCH ?= $(word 2,$(subst /, ,$(lastword $(GOVERSION))))

ifeq ($(GOOS), windows)
  SUFFIX=.exe
else
  SUFFIX=

endif
DIST_BIN=dist
PKG_NAME=trdsql_$(VERSION)_$(GOOS)_$(GOARCH)
BUILD_DIR=$(DIST_BIN)/$(PKG_NAME)

BINARY_NAME := trdsql$(SUFFIX)
SRCS := $(shell git ls-files '*.go')
ZIP_NAME=trdsql_$(VERSION)_$(GOOS)_$(GOARCH).zip

all: test build

.PHONY: test
test: $(SRCS)
	$(GOTEST)

.PHONY: build
build: trdsql

$(BINARY_NAME): $(SRCS)
	$(GOBUILD) -o $(BINARY_NAME) ./cmd/trdsql

.PHONY: install
install:
	$(GOINSTALL) ./cmd/trdsql

.PHONY: clean
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -rf dist

pkg:
	CGO_ENABLED=1 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/trdsql
	$(DIST_DIRS) cp README.md $(BUILD_DIR) && \
	$(DIST_DIRS) cp LICENSE $(BUILD_DIR) && \
	$(DIST_DIRS) cp config.json.sample $(BUILD_DIR) && \
	cd $(DIST_BIN) && \
	$(DIST_DIRS) zip -r $(ZIP_NAME) $(PKG_NAME) && \
	cp $(ZIP_NAME) ../ && \
	cd ..
