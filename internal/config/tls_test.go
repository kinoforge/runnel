package config

import (
	"crypto/tls"
	"slices"
	"testing"

	"github.com/kinoforge/runnel/internal/testcert"
)

func TestTLSFromEnv(t *testing.T) {
	ca := testcert.New(t)
	cert, key := ca.Server(t)
	cfg, err := Load("", env(map[string]string{
		"RUNNEL_SERVER_TLS_CERT":                cert,
		"RUNNEL_SERVER_TLS_KEY":                 key,
		"RUNNEL_SERVER_TLS_CLIENT_CA":           ca.File,
		"RUNNEL_SERVER_TLS_CLIENT_NAMES":        " dash, ops ,",
		"RUNNEL_SERVER_TLS_REQUIRE_CLIENT_CERT": "true",
		"RUNNEL_SERVER_TLS_MIN_VERSION":         "1.3",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(cfg.Server.TLS.ClientNames, []string{"dash", "ops"}) {
		t.Fatalf("client names %q", cfg.Server.TLS.ClientNames)
	}
	tc, err := cfg.Server.TLS.Build()
	if err != nil {
		t.Fatal(err)
	}
	if tc.ClientAuth != tls.RequireAndVerifyClientCert || tc.MinVersion != tls.VersionTLS13 || tc.ClientCAs == nil || len(tc.Certificates) != 1 {
		t.Fatalf("tls config %+v", tc)
	}
	cfg.Server.TLS.RequireClientCert = false
	if tc, _ = cfg.Server.TLS.Build(); tc.ClientAuth != tls.VerifyClientCertIfGiven {
		t.Fatalf("optional client certs: %v", tc.ClientAuth)
	}
}

func TestTLSBuildErrors(t *testing.T) {
	ca := testcert.New(t)
	cert, key := ca.Server(t)
	for name, c := range map[string]TLS{
		"off":         {MinVersion: "1.2"},
		"no files":    {Cert: "missing.pem", Key: "missing.key", MinVersion: "1.2"},
		"ca missing":  {Cert: cert, Key: key, ClientCA: "missing.pem", MinVersion: "1.2"},
		"ca is a key": {Cert: cert, Key: key, ClientCA: key, MinVersion: "1.2"},
	} {
		if _, err := c.Build(); err == nil {
			t.Errorf("%s: built", name)
		}
	}
}
