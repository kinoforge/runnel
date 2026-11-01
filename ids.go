package runnel

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
)

// NewID returns a new job id: a time-ordered UUID v7.
func NewID() string { return uuid.Must(uuid.NewV7()).String() }

// ValidID reports whether s is a UUID in its canonical form.
func ValidID(s string) bool {
	if len(s) != 36 {
		return false
	}
	_, err := uuid.Parse(s)
	return err == nil
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func defaultWorkerID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "worker"
	}
	if len(host) > 64 {
		host = host[:64]
	}
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), randomHex(3))
}

var specParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

func parseSpec(spec, tz string) (cron.Schedule, error) {
	if tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			return nil, err
		}
		spec = "CRON_TZ=" + tz + " " + spec
	}
	return specParser.Parse(spec)
}
