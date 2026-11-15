-include .env

RUNNEL_TEST_POSTGRES ?= postgres://runnel:runnel@localhost:55432/runnel?sslmode=disable
RUNNEL_TEST_MYSQL ?= root:runnel@tcp(localhost:53306)/runnel
RUNNEL_TEST_REDIS ?= redis://localhost:56379/0
RUNNEL_TEST_VALKEY ?= redis://localhost:56380/0
RUNNEL_TEST_DRAGONFLY ?= redis://localhost:56381/0
INTEGRATION = RUNNEL_TEST_POSTGRES='$(RUNNEL_TEST_POSTGRES)' RUNNEL_TEST_MYSQL='$(RUNNEL_TEST_MYSQL)' \
	RUNNEL_TEST_REDIS='$(RUNNEL_TEST_REDIS)' RUNNEL_TEST_VALKEY='$(RUNNEL_TEST_VALKEY)' \
	RUNNEL_TEST_DRAGONFLY='$(RUNNEL_TEST_DRAGONFLY)'

FUZZTIME ?= 30s
GOLANGCI_LINT = go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0

.PHONY: build test test-all test-box drill up down lint fmt vuln fuzz bench docker release-check

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=$$(git describe --tags --always --dirty 2>/dev/null || echo dev)" -o bin/runnel ./cmd/runnel

test:
	go test -race -count=1 ./...

test-box:
	PKG='$(PKG)' RUN='$(RUN)' BOX_CPUS='$(BOX_CPUS)' BOX_MEMORY='$(BOX_MEMORY)' BOX_IOPS='$(BOX_IOPS)' \
		BOX_DISK='$(BOX_DISK)' BOX_IMAGE='$(BOX_IMAGE)' BOX_TAGS='$(BOX_TAGS)' BOX_TMPFS='$(BOX_TMPFS)' \
		BOX_TIMEOUT='$(BOX_TIMEOUT)' BOX_COUNT='$(BOX_COUNT)' BOX_RACE='$(BOX_RACE)' sh deploy/test-box.sh

drill:
	$(MAKE) test-box PKG=./internal/drill RUN='$(RUN)' BOX_TAGS=drill BOX_TMPFS=/drill:size=96m BOX_TIMEOUT=30m

test-all:
	$(INTEGRATION) go test -race -count=1 -timeout 15m ./...

up:
	docker compose -f deploy/compose.yml up -d

down:
	docker compose -f deploy/compose.yml down -v

lint:
	$(GOLANGCI_LINT) run ./...

fmt:
	$(GOLANGCI_LINT) fmt ./...

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...

fuzz:
	for f in FuzzDurationJSON FuzzParseAPIKey FuzzScheduleSpec FuzzCallbackURL FuzzDecide; do \
		go test -run '^$$' -fuzz "^$$f$$" -fuzztime $(FUZZTIME) . || exit 1; done
	go test -run '^$$' -fuzz '^FuzzPreferWait$$' -fuzztime $(FUZZTIME) ./server

bench: build
	./bin/runnel bench --store "$${RUNNEL_STORE:-sqlite:bench.db}"

docker:
	docker build -f deploy/Dockerfile --build-arg VERSION=$$(git describe --tags --always --dirty 2>/dev/null || echo dev) -t runnel:dev .

release-check:
	go run github.com/goreleaser/goreleaser/v2@v2.12.7 check
