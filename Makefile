GOCMD=go
TAGS="json1"
GOGET=$(GOCMD) get
GOCLEAN=$(GOCMD) clean
GOBUILD=$(GOCMD) build -tags $(TAGS)
GOTEST=$(GOCMD) test -tags $(TAGS)
GOINSTALL=$(GOCMD) install -tags $(TAGS)
DEP=dep

GOXCMD=gox -cgo -tags $(TAGS)

TARGET="dist/trdsql_{{.OS}}_{{.Arch}}/{{.Dir}}"

BINARY_NAME := trdsql
SRCS := $(shell git ls-files '*.go')

all: dep test build

dep:
	$(GOGET) github.com/golang/dep/cmd/dep
	$(GOGET) github.com/mitchellh/gox
	$(DEP) ensure

test: $(SRCS)
	$(GOTEST)

build: trdsql

$(BINARY_NAME): $(SRCS)
	$(GOBUILD) -o $(BINARY_NAME)

pkg: linux_pkg window_pkg

linux_pkg:
	$(GOXCMD) -os "linux" -arch "386 amd64" -output $(TARGET)

window_pkg:
	CC=x86_64-w64-mingw32-gcc $(GOXCMD) -os "windows" -arch "amd64" -output $(TARGET)
	CC=i686-w64-mingw32-gcc $(GOXCMD) -os "windows" -arch "386" -output $(TARGET)

pkg_macOS:
	$(GOXCMD) -os "darwin" -arch "386 amd64" -output ${TARGET}

install:
	$(GOCMD) install

clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)

.PHONY: all dep test build pkg install clean
