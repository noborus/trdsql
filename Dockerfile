FROM alpine:3.13

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
	git clone --depth 1 https://github.com/noborus/trdsql $GOPATH/src/github.com/noborus/trdsql; \
	cd $GOPATH/src/github.com/noborus/trdsql; \
	# install
	make; \
	make install; \
	cp /go/bin/trdsql /usr/local/bin/trdsql; \
	# cleanup
	rm -rf /go; \
	apk del .build-deps


ENTRYPOINT ["trdsql"]
