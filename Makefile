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
GOVERSION ?= "latest"
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
.PHONY: dist-clean dist build-xgo dist-zip dist-bin

dist: dist-clean build-xgo dist-bin dist-zip

XGOCMD=xgo -go $(GOVERSION) $(BUILDFLAG)
XGO_TARGETS=linux/amd64,linux/386,linux/arm-5,linux/arm-6,linux/arm-7,linux/arm64,linux/mips,linux/mips64,linux/mipsle,windows/amd64,windows/386
DIST_BIN=dist/bin
BINARY_FILE := $(TARGET_NAME)

dist-clean:
	rm -Rf dist/trdsql_*

build-xgo:
	mkdir -p dist dist/tmp dist/bin
	$(XGOCMD) --targets=$(XGO_TARGETS) -dest dist/tmp -pkg cmd/$(TARGET_NAME) .
	find dist/tmp -type f -exec cp {} $(DIST_BIN) \;

dist-bin:
	for file in $(wildcard $(DIST_BIN)/$(BINARY_FILE)-*); do \
        OS_ARCH=`echo $$file | sed -E -e 's/\.exe$$//' -e 's#^.*/$(BINARY_FILE)-([^-]+)-(4\.0-)?(.*)$$#\1-\3#'`; \
        OS=`echo $${OS_ARCH} | cut -d '-' -f 1`; \
        ARCH=`echo $${OS_ARCH} | cut -d '-' -f 2-`; \
        DIST_DIR=dist/trdsql_$(VERSION)_$${OS}_$${ARCH}; \
        mkdir -p $${DIST_DIR}; \
        if [ "$$OS" = "windows" ]; then \
                cp $$file $${DIST_DIR}/$(BINARY_FILE).exe; \
                zip -j $(DIST_BIN)/$(BINARY_FILE)-$${OS}-$${ARCH}.zip $$file; \
                rm -f $$file; \
        else \
                cp $$file $${DIST_DIR}/$(BINARY_FILE); \
        fi; \
    done

dist-zip: dist-bin
	cd dist && \
	$(DIST_ZIP_DIRS) cp ../README.md {} \; && \
	$(DIST_ZIP_DIRS) cp ../LICENSE {} \; && \
	$(DIST_ZIP_DIRS) cp ../config.json.sample {} \; && \
	$(DIST_ZIP_DIRS) zip -r {}.zip {} \; && \
	cd ..

DIST_ZIP_DIRS := find trdsql* -type d -exec
