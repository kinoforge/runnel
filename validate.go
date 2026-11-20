package runnel

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

const (
	maxPriority      = 1000
	maxAttemptsLimit = 1000
)

var (
	namePattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	namespacePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	groupPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$`)
)

// ValidName reports whether s can name a queue, kind, pool or schedule: 1 to 128
// letters, digits, dots, dashes or underscores, starting with a letter or digit.
func ValidName(s string) bool { return namePattern.MatchString(s) }

// ValidNamespace reports whether s can name a namespace: 1 to 63 lowercase
// letters, digits, dashes or underscores.
func ValidNamespace(s string) bool { return namespacePattern.MatchString(s) }

func ValidGroup(s string) bool { return groupPattern.MatchString(s) }

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

func checkName(field, v string, optional bool) error {
	if v == "" && optional {
		return nil
	}
	if !ValidName(v) {
		return invalid("%s %q must match %s", field, v, namePattern)
	}
	return nil
}

func (j *Job) validate(l Limits) error {
	for _, check := range []func(Limits) error{j.validateNames, j.validateSettings, j.validateExtras} {
		if err := check(l); err != nil {
			return err
		}
	}
	return nil
}

func (j *Job) validateNames(Limits) error {
	if err := checkName("queue", j.Queue, false); err != nil {
		return err
	}
	if err := checkName("kind", j.Kind, false); err != nil {
		return err
	}
	if err := checkName("pool", j.Pool, true); err != nil {
		return err
	}
	if j.Group != "" && !ValidGroup(j.Group) {
		return invalid("group %q must match %s", j.Group, groupPattern)
	}
	return nil
}

func (j *Job) validateSettings(l Limits) error {
	if len(j.Payload) > l.MaxPayload {
		return invalid("payload is %d bytes, limit %d", len(j.Payload), l.MaxPayload)
	}
	if len(j.Payload) > 0 && !json.Valid(j.Payload) {
		return invalid("payload must be valid JSON")
	}
	if j.Priority < -maxPriority || j.Priority > maxPriority {
		return invalid("priority must be between %d and %d", -maxPriority, maxPriority)
	}
	if j.MaxAttempts < 1 || j.MaxAttempts > maxAttemptsLimit {
		return invalid("max_attempts must be between 1 and %d", maxAttemptsLimit)
	}
	if j.Timeout < 0 || j.StartTimeout < 0 || j.UniqueFor < 0 {
		return invalid("durations must not be negative")
	}
	if err := j.Backoff.Validate(); err != nil {
		return err
	}
	return j.validateDeadline()
}

func (j *Job) validateDeadline() error {
	if j.Deadline.IsZero() {
		return nil
	}
	left := j.Deadline.Sub(j.RunAt)
	if left <= 0 {
		return invalid("deadline must be after run_at")
	}
	if j.StartTimeout.D() > left || j.Timeout.D() > left {
		return invalid("start_timeout and timeout must not reach past the deadline")
	}
	return nil
}

func (j *Job) validateExtras(l Limits) error {
	if len(j.DependsOn) > l.MaxDependsOn {
		return invalid("at most %d dependencies", l.MaxDependsOn)
	}
	for _, d := range j.DependsOn {
		if !ValidID(d) {
			return invalid("dependency %q is not a job id", d)
		}
	}
	if len(j.UniqueKey) > l.MaxUniqueKey || strings.ContainsAny(j.UniqueKey, "|\x00") {
		return invalid("unique key must be at most %d bytes without '|'", l.MaxUniqueKey)
	}
	if err := ValidMeta(j.Meta, l); err != nil {
		return err
	}
	if j.CallbackURL != "" {
		return ValidCallbackURL(j.CallbackURL, l.MaxCallbackURL)
	}
	return nil
}

// ValidMeta checks meta against the limits: at most MaxMeta entries, keys of
// 1 to MaxMetaKey bytes and values of at most MaxMetaValue.
func ValidMeta(meta map[string]string, l Limits) error {
	if len(meta) > l.MaxMeta {
		return invalid("at most %d meta entries", l.MaxMeta)
	}
	for k, v := range meta {
		if len(k) == 0 || len(k) > l.MaxMetaKey || len(v) > l.MaxMetaValue {
			return invalid("meta keys must be 1-%d bytes and values at most %d", l.MaxMetaKey, l.MaxMetaValue)
		}
	}
	return nil
}

// ValidCallbackURL checks that raw is an absolute http or https URL without
// credentials, at most maxLen bytes.
func ValidCallbackURL(raw string, maxLen int) error {
	if len(raw) > maxLen {
		return invalid("callback url too long")
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil {
		return invalid("callback must be an absolute http(s) url without credentials")
	}
	return nil
}
