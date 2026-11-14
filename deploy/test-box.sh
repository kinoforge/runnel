#!/bin/sh
set -eu

image=${BOX_IMAGE:-golang:1.26.11-alpine}
cpus=${BOX_CPUS:-1}
memory=${BOX_MEMORY:-2g}
iops=${BOX_IOPS:-150}
pkg=${PKG:-./...}
run=${RUN:-.}

disk=${BOX_DISK:-}
if [ -z "$disk" ] && command -v lsblk >/dev/null 2>&1; then
	src=$(findmnt -no SOURCE /var/lib/docker 2>/dev/null || findmnt -no SOURCE /)
	parent=$(lsblk -no PKNAME "$src" 2>/dev/null | head -n 1)
	[ -n "$parent" ] && disk=/dev/$parent
fi

set -- --rm --cpus "$cpus" --memory "$memory" --memory-swap "$memory" --pids-limit 1024
if [ -n "$disk" ]; then
	set -- "$@" --device-write-iops "$disk:$iops" --device-read-iops "$disk:$iops"
fi

exec docker run "$@" \
	-v "$(pwd)":/src:ro -w /src \
	-v runnel-gocache:/root/.cache/go-build -v runnel-gomod:/go/pkg/mod \
	-e CGO_ENABLED=0 -e GOFLAGS=-buildvcs=false \
	"$image" go test -p 1 -count=1 -run "$run" $pkg
