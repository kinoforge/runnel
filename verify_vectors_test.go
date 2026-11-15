package runnel

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/kinoforge/runnel/internal/spec"
)

func TestCallbackSignatureVectors(t *testing.T) {
	spec.Rule(t, "H05")
	b, err := os.ReadFile("sdk-contract/callback-signatures.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Vectors []struct {
			Name    string   `json:"name"`
			Secrets []string `json:"secrets"`
			Body    string   `json:"body"`
			Now     int64    `json:"now"`
			MaxAge  int64    `json:"max_age_seconds"`
			Header  string   `json:"header"`
			Valid   bool     `json:"valid"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(b, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Vectors) < 10 {
		t.Fatalf("only %d vectors", len(file.Vectors))
	}
	for _, v := range file.Vectors {
		err := VerifySignature(v.Header, []byte(v.Body), time.Duration(v.MaxAge)*time.Second, time.Unix(v.Now, 0), v.Secrets...)
		if (err == nil) != v.Valid {
			t.Errorf("%s: valid %v, got %v", v.Name, v.Valid, err)
		}
	}
}
