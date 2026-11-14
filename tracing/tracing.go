// Package tracing joins Runnel to OpenTelemetry. A job carries the W3C trace
// context in its meta, under traceparent and tracestate, so one trace follows
// a request from the producer through Runnel to the worker and the job's
// callback. Nothing here runs unless it is used: the core and the stores keep
// no tracing state.
package tracing

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/kinoforge/runnel"
)

const instrumentation = "github.com/kinoforge/runnel/tracing"

var w3c = propagation.TraceContext{}

// Job returns an option that puts the trace context of ctx in a job's meta,
// for jobs enqueued from Go. Producers over HTTP send a traceparent header
// instead, and the server copies it into the job.
func Job(ctx context.Context) runnel.Option {
	carrier := propagation.MapCarrier{}
	w3c.Inject(ctx, carrier)
	return runnel.Meta(carrier)
}

// Middleware starts a consumer span for every attempt a worker runs, as a
// child of the trace the job carries.
func Middleware(tp trace.TracerProvider) runnel.Middleware {
	tracer := tp.Tracer(instrumentation)
	return func(next runnel.HandlerFunc) runnel.HandlerFunc {
		return func(ctx context.Context, j *runnel.Job) error {
			parent := w3c.Extract(ctx, propagation.MapCarrier(j.Meta))
			op := "process"
			if j.Polling() {
				op = "poll"
			}
			ctx, span := tracer.Start(parent, op+" "+j.Queue, trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(jobAttributes(j)...))
			defer span.End()
			err := next(ctx, j)
			o := runnel.OutcomeOf(err, nil)
			span.SetAttributes(attribute.String("runnel.outcome", string(o.Kind)))
			if o.Kind == runnel.OutcomeRetry || o.Kind == runnel.OutcomeFail {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			return err
		}
	}
}

func jobAttributes(j *runnel.Job) []attribute.KeyValue {
	kv := []attribute.KeyValue{
		attribute.String("messaging.system", "runnel"),
		attribute.String("messaging.operation.type", "process"),
		attribute.String("messaging.destination.name", j.Queue),
		attribute.String("messaging.message.id", j.ID),
		attribute.String("runnel.namespace", j.Namespace),
		attribute.String("runnel.kind", j.Kind),
		attribute.Int("runnel.attempt", j.Attempt),
	}
	if j.Pool != "" {
		kv = append(kv, attribute.String("runnel.pool", j.Pool))
	}
	if j.Group != "" {
		kv = append(kv, attribute.String("runnel.group", j.Group))
	}
	return kv
}

// Handler starts a server span for every request h serves, named after the
// route it matched, as a child of the caller's trace.
func Handler(h http.Handler, tp trace.TracerProvider) http.Handler {
	tracer := tp.Tracer(instrumentation)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := w3c.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.request.method", r.Method)))
		defer span.End()
		rec := &recorder{ResponseWriter: w, code: http.StatusOK}
		r = r.WithContext(ctx)
		h.ServeHTTP(rec, r)
		// The mux sets the pattern on the request it was given.
		if r.Pattern != "" {
			span.SetName(r.Pattern)
			span.SetAttributes(attribute.String("http.route", r.Pattern))
		}
		span.SetAttributes(attribute.Int("http.response.status_code", rec.code))
		if rec.code >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(rec.code))
		}
	})
}

type recorder struct {
	http.ResponseWriter
	code int
}

func (r *recorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
