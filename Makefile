VERSION=$(shell git describe --tags 2>/dev/null)

GOCMD=go
TAGS="sqlite_json"
GOGET=$(GOCMD) get
GOCLEAN=$(GOCMD) clean
ifeq ($(strip $(VERSION)),)
  LDFLAGS=""
else
  LDFLAGS="-X github.com/noborus/trdsql.Version=$(VERSION)"
endif
GOVERSION="1.16.x"
BUILDFLAG=-tags $(TAGS) -ldflags=$(LDFLAGS)
GOBUILD=$(GOCMD) build $(BUILDFLAG)
GOTEST=$(GOCMD) test -tags $(TAGS) ./...
GOINSTALL=$(GOCMD) install $(BUILDFLAG)

XGOCMD=xgo -go $(GOVERSION) $(BUILDFLAG)

DIST_BIN=dist/bin

BINARY_NAME := trdsql
SRCS := $(shell git ls-files '*.go')

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

dist-clean:
	rm -Rf dist/trdsql_*

build-all:
	-mkdir dist
	-mkdir dist/tmp
	-mkdir dist/bin
	$(XGOCMD) -dest dist/tmp github.com/noborus/trdsql/cmd/trdsql
	find dist/tmp -type f -exec cp {} $(DIST_BIN) \;

DIST_DIRS := find trdsql* -type d -exec

dist: dist-clean build-all linux-amd64 linux-386 linux-arm-5 linux-arm-6 linux-arm-7 linux-arm64 linux-mips linux-mips64 linux-mipsle windows-386 windows-amd64 darwin-amd64 darwin-arm64
	cd dist && \
	$(DIST_DIRS) cp ../README.md {} \; && \
	$(DIST_DIRS) cp ../LICENSE {} \; && \
	$(DIST_DIRS) cp ../config.json.sample {} \; && \
	$(DIST_DIRS) zip -r {}.zip {} \; && \
	cd ..

linux-amd64:
	mkdir dist/trdsql_$(VERSION)_linux_amd64
	cp $(DIST_BIN)/$(BINARY_NAME)-linux-amd64 dist/trdsql_$(VERSION)_linux_amd64/$(BINARY_NAME)

linux-386:
	mkdir dist/trdsql_$(VERSION)_linux_386
	cp $(DIST_BIN)/$(BINARY_NAME)-linux-386 dist/trdsql_$(VERSION)_linux_386/$(BINARY_NAME)

linux-arm-5:
	mkdir dist/trdsql_$(VERSION)_linux_arm5
	cp $(DIST_BIN)/$(BINARY_NAME)-linux-arm-5 dist/trdsql_$(VERSION)_linux_arm5/$(BINARY_NAME)

linux-arm-6:
	mkdir dist/trdsql_$(VERSION)_linux_arm6
	cp $(DIST_BIN)/$(BINARY_NAME)-linux-arm-6 dist/trdsql_$(VERSION)_linux_arm6/$(BINARY_NAME)

linux-arm-7:
	mkdir dist/trdsql_$(VERSION)_linux_arm7
	cp $(DIST_BIN)/$(BINARY_NAME)-linux-arm-7 dist/trdsql_$(VERSION)_linux_arm7/$(BINARY_NAME)

linux-arm64:
	mkdir dist/trdsql_$(VERSION)_linux_arm64
	cp $(DIST_BIN)/$(BINARY_NAME)-linux-arm64 dist/trdsql_$(VERSION)_linux_arm64/$(BINARY_NAME)

linux-mips:
	mkdir dist/trdsql_$(VERSION)_linux_mips
	cp $(DIST_BIN)/$(BINARY_NAME)-linux-mips dist/trdsql_$(VERSION)_linux_mips/$(BINARY_NAME)

linux-mips64:
	mkdir dist/trdsql_$(VERSION)_linux_mips64
	cp $(DIST_BIN)/$(BINARY_NAME)-linux-mips64 dist/trdsql_$(VERSION)_linux_mips64/$(BINARY_NAME)

linux-mipsle:
	mkdir dist/trdsql_$(VERSION)_linux_mipsle
	cp $(DIST_BIN)/$(BINARY_NAME)-linux-mipsle dist/trdsql_$(VERSION)_linux_mipsle/$(BINARY_NAME)

windows-386:
	mkdir dist/trdsql_$(VERSION)_windows_386
	cp $(DIST_BIN)/$(BINARY_NAME)-windows-4.0-386.exe dist/trdsql_$(VERSION)_windows_386/$(BINARY_NAME).exe

windows-amd64:
	mkdir dist/trdsql_$(VERSION)_windows_amd64
	cp $(DIST_BIN)/$(BINARY_NAME)-windows-4.0-amd64.exe dist/trdsql_$(VERSION)_windows_amd64/$(BINARY_NAME).exe

darwin-amd64:
	mkdir dist/trdsql_$(VERSION)_darwin_amd64
	cp $(DIST_BIN)/$(BINARY_NAME)-darwin-10.12-amd64 dist/trdsql_$(VERSION)_darwin_amd64/$(BINARY_NAME)

darwin-arm64:
	mkdir dist/trdsql_$(VERSION)_darwin_arm64
	cp $(DIST_BIN)/$(BINARY_NAME)-darwin-10.12-arm64 dist/trdsql_$(VERSION)_darwin_arm64/$(BINARY_NAME)

