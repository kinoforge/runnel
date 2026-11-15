#!/bin/sh
set -eu

image=${BOX_IMAGE:-golang:1.27.4-alpine}
cpus=${BOX_CPUS:-1}
memory=${BOX_MEMORY:-2g}
iops=${BOX_IOPS:-150}
pkg=${PKG:-./...}
run=${RUN:-.}

tags=${BOX_TAGS:-}
tmpfs=${BOX_TMPFS:-}
timeout=${BOX_TIMEOUT:-10m}
count=${BOX_COUNT:-1}
race=${BOX_RACE:-}
cgo=0
if [ -n "$race" ]; then
	image=${BOX_IMAGE:-golang:1.27.4}
	cgo=1
	race=-race
fi

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

if [ -n "$tmpfs" ]; then
	set -- "$@" --tmpfs "$tmpfs" -e RUNNEL_DRILL_DISK="${tmpfs%%:*}"
fi

exec docker run "$@" \
	-v "$(pwd)":/src:ro -w /src \
	-v runnel-gocache:/root/.cache/go-build -v runnel-gomod:/go/pkg/mod \
	-e CGO_ENABLED="$cgo" -e GOFLAGS=-buildvcs=false \
	"$image" go test $race -p 1 -count="$count" -timeout "$timeout" -tags "$tags" -run "$run" $pkg
