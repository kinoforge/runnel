INTEGRATION = RUNNEL_TEST_POSTGRES='postgres://runnel:runnel@localhost:55432/runnel?sslmode=disable' \
	RUNNEL_TEST_MYSQL='root:runnel@tcp(localhost:53306)/runnel' \
	RUNNEL_TEST_REDIS=redis://localhost:56379/0 \
	RUNNEL_TEST_VALKEY=redis://localhost:56380/0 \
	RUNNEL_TEST_DRAGONFLY=redis://localhost:56381/0

FUZZTIME ?= 30s

.PHONY: build test test-all up down lint vuln fuzz bench docker release-check

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=$$(git describe --tags --always --dirty 2>/dev/null || echo dev)" -o bin/runnel ./cmd/runnel

test:
	go test -race -count=1 ./...

test-all:
	$(INTEGRATION) go test -race -count=1 -timeout 15m ./...

up:
	docker compose -f deploy/compose.yml up -d

down:
	docker compose -f deploy/compose.yml down -v

lint:
	test -z "$$(gofmt -l .)"
	go vet ./...

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...

fuzz:
	for f in FuzzDurationJSON FuzzParseAPIKey FuzzScheduleSpec FuzzWebhookURL FuzzDecide; do \
		go test -run '^$$' -fuzz "^$$f$$" -fuzztime $(FUZZTIME) . || exit 1; done
	go test -run '^$$' -fuzz '^FuzzPreferWait$$' -fuzztime $(FUZZTIME) ./server

bench: build
	./bin/runnel bench --store "$${RUNNEL_STORE:-sqlite:bench.db}"

docker:
	docker build -f deploy/Dockerfile --build-arg VERSION=$$(git describe --tags --always --dirty 2>/dev/null || echo dev) -t runnel:dev .

release-check:
	go run github.com/goreleaser/goreleaser/v2@v2.12.7 check
