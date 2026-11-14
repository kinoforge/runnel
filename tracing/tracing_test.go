package tracing_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/internal/spec"
	"github.com/kinoforge/runnel/tracing"
)

func provider() (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	rec := tracetest.NewSpanRecorder()
	return sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)), rec
}

func TestJobCarriesTraceToWorker(t *testing.T) {
	spec.Rule(t, "R09")
	tp, rec := provider()
	ctx, parent := tp.Tracer("t").Start(context.Background(), "enqueue")
	j := &runnel.Job{ID: "j1", Namespace: "app", Queue: "gen", Kind: "k", Attempt: 1, Pool: "acct-1"}
	tracing.Job(ctx)(j)
	parent.End()
	if j.Meta[runnel.TraceParent] == "" {
		t.Fatalf("meta %v", j.Meta)
	}
	run := tracing.Middleware(tp)(func(ctx context.Context, _ *runnel.Job) error {
		if !trace.SpanContextFromContext(ctx).IsValid() {
			t.Error("handler has no span")
		}
		return runnel.Fail(errors.New("bad input"))
	})
	_ = run(context.Background(), j)
	spans := rec.Ended()
	if len(spans) != 2 {
		t.Fatalf("%d spans", len(spans))
	}
	work := spans[1]
	if work.Name() != "process gen" || work.Parent().SpanID() != parent.SpanContext().SpanID() ||
		work.SpanKind() != trace.SpanKindConsumer || work.Status().Code != codes.Error {
		t.Fatalf("span %s parent %v kind %v status %+v", work.Name(), work.Parent().SpanID(), work.SpanKind(), work.Status())
	}
}

func TestHandlerNamesSpansByRoute(t *testing.T) {
	spec.Rule(t, "R09")
	tp, rec := provider()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/ns/{ns}/jobs/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	srv := httptest.NewServer(tracing.Handler(mux, tp))
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/v1/ns/app/jobs/42", nil)
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	spans := rec.Ended()
	if len(spans) != 1 || spans[0].Name() != "GET /v1/ns/{ns}/jobs/{id}" || spans[0].Parent().TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("spans %v", spans)
	}
}
