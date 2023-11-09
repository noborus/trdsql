#!/bin/bash

template_file=".github/trdsql.template.rb"

output_file=".github/trdsql.rb"

VERSION=$(git describe --tags 2>/dev/null)

get_url_and_sha256() {
    local file_path="$1"
    local file_name=$(basename "$file_path")
    local url="https://github.com/noborus/trdsql/releases/download/$VERSION/$file_name"
    local sha256=$(sha256sum "$file_path" | awk '{print $1}')
    echo "$url $sha256"
}

darwin_arm64=$(get_url_and_sha256 "dist/trdsql_${VERSION}_darwin_arm64.zip")
darwin_arm64_url=$(echo "${darwin_arm64% *}" | sed 's/\//\\\//g')
darwin_arm64_sha256=${darwin_arm64#* }

darwin_amd64=$(get_url_and_sha256 "dist/trdsql_${VERSION}_darwin_amd64.zip")
darwin_amd64_url=$(echo "${darwin_amd64% *}" | sed 's/\//\\\//g')
darwin_amd64_sha256=${darwin_amd64#* }

linux_arm64=$(get_url_and_sha256 "dist/trdsql_${VERSION}_linux_arm64.zip")
linux_arm64_url=$(echo "${linux_arm64% *}" | sed 's/\//\\\//g')
linux_arm64_sha256=${linux_arm64#* }

linux_amd64=$(get_url_and_sha256 "dist/trdsql_${VERSION}_linux_amd64.zip")
linux_amd64_url=$(echo "${linux_amd64% *}" | sed 's/\//\\\//g')
linux_amd64_sha256=${linux_amd64#* }

ver=$(echo "$VERSION" | sed 's/^v//g')
sed -e "s/{{ version }}/$ver/g" \
    -e "s/{{ DARWIN_ARM64_URL }}/$darwin_arm64_url/g" \
    -e "s/{{ DARWIN_ARM64_SHA256 }}/$darwin_arm64_sha256/g" \
    -e "s/{{ DARWIN_AMD64_URL }}/$darwin_amd64_url/g" \
    -e "s/{{ DARWIN_AMD64_SHA256 }}/$darwin_amd64_sha256/g" \
    -e "s/{{ LINUX_ARM64_URL }}/$linux_arm64_url/g" \
    -e "s/{{ LINUX_ARM64_SHA256 }}/$linux_arm64_sha256/g" \
    -e "s/{{ LINUX_AMD64_URL }}/$linux_amd64_url/g" \
    -e "s/{{ LINUX_AMD64_SHA256 }}/$linux_amd64_sha256/g" \
    "$template_file" > "$output_file"