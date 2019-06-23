GOCMD=go
TAGS="json1"
GOGET=$(GOCMD) get
GOCLEAN=$(GOCMD) clean
GOBUILD=$(GOCMD) build -tags $(TAGS)
GOTEST=$(GOCMD) test -tags $(TAGS)
GOINSTALL=$(GOCMD) install -tags $(TAGS)

GOXCMD=gox -cgo -tags $(TAGS)

TARGET="dist/trdsql_{{.OS}}_{{.Arch}}/{{.Dir}}"

BINARY_NAME := trdsql
SRCS := $(shell git ls-files '*.go')

all: test build

test: $(SRCS)
	$(GOTEST)

build: trdsql

$(BINARY_NAME): $(SRCS)
	$(GOBUILD) -o $(BINARY_NAME) ./cmd/trdsql

pkg: linux_pkg window_pkg

linux_pkg:
	$(GOXCMD) -os "linux" -arch "amd64" -output $(TARGET) ./cmd/trdsql

window_pkg:
	CC=x86_64-w64-mingw32-gcc $(GOXCMD) -os "windows" -arch "amd64" -output $(TARGET) ./cmd/trdsql

pkg_macOS:
	$(GOXCMD) -os "darwin" -arch "amd64" -output ${TARGET} ./cmd/trdsql

install:
	$(GOINSTALL) ./cmd/trdsql

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -rf dist

.PHONY: all test build pkg install clean
