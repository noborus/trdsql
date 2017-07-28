#! /bin/sh

TARGET="dist/trdsql_{{.OS}}_{{.Arch}}/{{.Dir}}"
gox -cgo -os "linux" -arch "386 amd64" -output ${TARGET}

CC=x86_64-w64-mingw32-gcc gox -cgo -os "windows" -arch "amd64" -output ${TARGET}
CC=i686-w64-mingw32-gcc gox -cgo -os "windows" -arch "386" -output ${TARGET}
