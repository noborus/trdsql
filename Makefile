TARGET_NAME := trdsql
VERSION=$(shell git describe --tags 2>/dev/null)

GOCMD=go
TAGS="sqlite_math_functions"
GOGET=$(GOCMD) get
GOCLEAN=$(GOCMD) clean
ifeq ($(strip $(VERSION)),)
  LDFLAGS=""
else
  LDFLAGS="-X github.com/noborus/$(TARGET_NAME).Version=$(VERSION)"
endif
GOVERSION ?= "1.21.x"
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
BINARY_NAME := $(TARGET_NAME)$(SUFFIX)
SRCS := $(shell git ls-files '*.go')

all: test build

.PHONY: test
test: $(SRCS)
	$(GOTEST)

.PHONY: build
build: $(TARGET_NAME)

$(BINARY_NAME): $(SRCS)
	$(GOBUILD) -o $(BINARY_NAME) ./cmd/$(TARGET_NAME)

.PHONY: install
install:
	$(GOINSTALL) ./cmd/$(TARGET_NAME)

.PHONY: clean
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -rf dist

.PHONY: pkg

## Build package
## Usage:
## make pkg GOOS=linux GOARCH=amd64
## cross build
## CC="zig cc -target x86_64-windows" make pkg GOOS=windows GOARCH=amd64
PKGGOVERSION=$(shell go version)
GOOS=$(word 1,$(subst /, ,$(lastword $(PKGGOVERSION))))
GOARCH=$(word 2,$(subst /, ,$(lastword $(PKGGOVERSION))))
ZIPOS=$(GOOS)
ZIP_NAME=trdsql_$(VERSION)_$(ZIPOS)_$(GOARCH).zip

pkg:
	CGO_ENABLED=1 GOOS=$(GOOS) GOARCH=$(GOARCH) $(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/$(TARGET_NAME)
	$(DIST_DIRS) cp README.md $(BUILD_DIR) && \
	$(DIST_DIRS) cp LICENSE $(BUILD_DIR) && \
	$(DIST_DIRS) cp config.json.sample $(BUILD_DIR) && \
	cd $(DIST_BIN) && \
	$(DIST_DIRS) zip -r $(ZIP_NAME) $(PKG_NAME) && \
	cp $(ZIP_NAME) ..

## XGO build
.PHONY: dist-clean dist build-xgo dist-zip

dist: dist-clean build-xgo $(BINARY_FILES) dist-zip 

XGOCMD=xgo -go $(GOVERSION) $(BUILDFLAG)
XGO_TARGETS=linux/amd64,linux/386,linux/arm-5,linux/arm-6,linux/arm-7,linux/arm64,linux/mips,linux/mips64,linux/mipsle,windows/amd64,windows/386

build-xgo:
	-mkdir dist
	-mkdir dist/bin
	$(XGOCMD) --targets=$(XGO_TARGETS) -dest $(DIST_BIN) github.com/noborus/trdsql/cmd/trdsql

DIST_BIN=dist/bin
BINARY_FILE := $(TARGET_NAME)
BINARY_FILES := $(wildcard $(DIST_BIN)/$(BINARY_FILE)-*)
.PHONY: $(BINARY_FILES)

dist-clean:
	rm -Rf dist/trdsql_*

$(BINARY_FILES): 
	@OS_ARCH=`echo $@ | sed -e 's/.*-\(.*\)-\(.*\)/\1-\2/' -e 's/\.exe//'`; \
	BINSUFFIX=`echo $@ | sed -n -e 's/.*\(\.exe\)$$/\1/p'`; \
	OS=`echo $$OS_ARCH | cut -d '-' -f 1`; \
	ARCH=`echo $$OS_ARCH | cut -d '-' -f 2`; \
	DIST_DIR=dist/trdsql_$(VERSION)_$${OS}_$${ARCH}; \
	mkdir -p $$DIST_DIR; \
	cp $@ $$DIST_DIR/$(BINARY_FILE)$${BINSUFFIX}; \

dist-zip: $(BINARY_FILES)
	cd dist && \
	$(DIST_ZIP_DIRS) cp ../README.md {} \; && \
	$(DIST_ZIP_DIRS) cp ../LICENSE {} \; && \
	$(DIST_ZIP_DIRS) cp ../config.json.sample {} \; && \
	$(DIST_ZIP_DIRS) zip -r {}.zip {} \; && \
	cd ..

DIST_ZIP_DIRS := find trdsql* -type d -exec
