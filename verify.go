package runnel

import (
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const maxCallbackBody = 16 << 20

// DefaultSignatureAge is how far a callback's signing time may be from now
// when VerifySignature is given no age.
const DefaultSignatureAge = 5 * time.Minute

// ErrSignature means a callback's Runnel-Signature header is missing,
// malformed, too old, or signed with another secret.
var ErrSignature = errors.New("runnel: bad callback signature")

// CallbackBody is the body of a callback request.
type CallbackBody struct {
	Event    string     `json:"event"`
	Delivery int64      `json:"delivery"`
	Job      JobSummary `json:"job"`
}

// VerifySignature checks header, the value of a Runnel-Signature header,
// against body. A signature from any of secrets passes, so a secret can be
// rotated: give the new one and the old one until every delivery signed with
// the old one is done. A signing time more than maxAge from now fails, so a
// captured request cannot be replayed later; maxAge 0 means
// DefaultSignatureAge. Deliveries can still arrive twice: dedupe them by
// Runnel-Delivery.
func VerifySignature(header string, body []byte, maxAge time.Duration, now time.Time, secrets ...string) error {
	if maxAge <= 0 {
		maxAge = DefaultSignatureAge
	}
	ts, sigs, err := parseSignature(header)
	if err != nil {
		return err
	}
	if d := now.Sub(time.Unix(ts, 0)); d > maxAge || d < -maxAge {
		return fmt.Errorf("%w: signed %s from now", ErrSignature, d.Round(time.Second))
	}
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		want := []byte(Sign(secret, ts, body))
		for _, sig := range sigs {
			if hmac.Equal([]byte("t="+strconv.FormatInt(ts, 10)+",v1="+sig), want) {
				return nil
			}
		}
	}
	return fmt.Errorf("%w: no secret matches", ErrSignature)
}

// parseSignature reads t=<unix seconds> and every v1=<hex> of a header.
func parseSignature(header string) (int64, []string, error) {
	var ts int64
	var sigs []string
	for part := range strings.SplitSeq(header, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(part), "=")
		switch k {
		case "t":
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return 0, nil, fmt.Errorf("%w: bad time", ErrSignature)
			}
			ts = n
		case "v1":
			sigs = append(sigs, v)
		}
	}
	if ts == 0 || len(sigs) == 0 {
		return 0, nil, fmt.Errorf("%w: missing t or v1", ErrSignature)
	}
	return ts, sigs, nil
}

// ReadCallback reads a callback request, verifies its signature with
// VerifySignature and decodes it. Answer 2xx once it is handled; any error
// here should be answered 400 or 401, which Runnel does not retry.
func ReadCallback(r *http.Request, maxAge time.Duration, secrets ...string) (CallbackBody, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxCallbackBody+1))
	if err != nil {
		return CallbackBody{}, err
	}
	if len(body) > maxCallbackBody {
		return CallbackBody{}, fmt.Errorf("%w: callback body over %d bytes", ErrInvalid, maxCallbackBody)
	}
	if err := VerifySignature(r.Header.Get("Runnel-Signature"), body, maxAge, time.Now(), secrets...); err != nil {
		return CallbackBody{}, err
	}
	var c CallbackBody
	if err := json.Unmarshal(body, &c); err != nil {
		return CallbackBody{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return c, nil
}
