FROM alpine:latest

# Configure Go
ENV GOROOT /usr/lib/go
ENV GOPATH /go
ENV PATH /go/bin:$PATH

RUN set -ex; \
	# build dependencies
	apk add --no-cache --update --virtual .build-deps \
		go \
		make \
		git \
		musl-dev \
	; \
  mkdir -p ${GOPATH}/src ${GOPATH}/bin; \
	# download
	go get -d github.com/noborus/trdsql; \
	cd $GOPATH/src/github.com/noborus/trdsql; \
	# install
	make; \
	make install; \
	cp /go/bin/trdsql /usr/local/bin/trdsql; \
	# cleanup
	rm -rf /go; \
	apk del .build-deps


ENTRYPOINT ["trdsql"]
