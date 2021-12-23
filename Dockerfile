FROM golang:1.17-alpine as build-dev

RUN set -ex; \
	# build dependencies
	apk add --no-cache --update --virtual .build-deps \
		gcc \
		make \
		git \
		musl-dev \
	; \
  mkdir -p ${GOPATH}/src ${GOPATH}/bin; \
	# download
	git clone --depth 1 https://github.com/noborus/trdsql $GOPATH/src/github.com/noborus/trdsql; \
	cd $GOPATH/src/github.com/noborus/trdsql; \
	# install
	make; \
	make install; \
	cp /go/bin/trdsql /usr/local/bin/trdsql; \
	# cleanup
	rm -rf /go; \
	apk del .build-deps

FROM alpine:latest
COPY --from=build-dev /usr/local/bin/trdsql /usr/local/bin/trdsql

ENTRYPOINT ["trdsql"]
