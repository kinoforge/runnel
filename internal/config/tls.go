package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	AuthRequired = "required"
	AuthNone     = "none"
)

type TLS struct {
	Cert              string   `yaml:"cert"`
	Key               string   `yaml:"key"`
	ClientCA          string   `yaml:"client_ca"`
	RequireClientCert bool     `yaml:"require_client_cert"`
	ClientNames       []string `yaml:"client_names"`
	MinVersion        string   `yaml:"min_version"`
}

var tlsVersions = map[string]uint16{"1.2": tls.VersionTLS12, "1.3": tls.VersionTLS13}

func (t TLS) Enabled() bool { return t.Cert != "" }

func (s Server) checkAuth() []error {
	var errs []error
	switch s.Auth {
	case AuthRequired:
	case AuthNone:
		if s.AdminToken != "" || s.TLS.ClientCA != "" {
			errs = append(errs, errors.New("server.auth none lets every request in; remove admin_token and tls.client_ca, or use auth required"))
		}
	default:
		errs = append(errs, fmt.Errorf("server.auth must be %s or %s, not %q", AuthRequired, AuthNone, s.Auth))
	}
	if s.AdminUser == "" || strings.ContainsAny(s.AdminUser, ": \t") {
		errs = append(errs, errors.New("server.admin_user must be set and hold no colon or spaces"))
	}
	return errs
}

func (t TLS) check() []error {
	var errs []error
	if (t.Cert == "") != (t.Key == "") {
		errs = append(errs, errors.New("server.tls.cert and server.tls.key go together"))
	}
	if t.ClientCA != "" && !t.Enabled() {
		errs = append(errs, errors.New("server.tls.client_ca needs server.tls.cert and key"))
	}
	if t.ClientCA == "" && (t.RequireClientCert || len(t.ClientNames) > 0) {
		errs = append(errs, errors.New("server.tls.require_client_cert and client_names need server.tls.client_ca"))
	}
	for _, name := range t.ClientNames {
		if strings.TrimSpace(name) == "" {
			errs = append(errs, errors.New("server.tls.client_names must not contain empty names"))
			break
		}
	}
	if _, ok := tlsVersions[t.MinVersion]; !ok {
		errs = append(errs, fmt.Errorf("server.tls.min_version must be 1.2 or 1.3, not %q", t.MinVersion))
	}
	return errs
}

func (t TLS) Build() (*tls.Config, error) {
	if !t.Enabled() {
		return nil, errors.New("server.tls.cert is not set")
	}
	pair, err := tls.LoadX509KeyPair(t.Cert, t.Key)
	if err != nil {
		return nil, fmt.Errorf("server.tls: %w", err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tlsVersions[t.MinVersion]}
	if t.ClientCA == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(t.ClientCA)
	if err != nil {
		return nil, fmt.Errorf("server.tls.client_ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("server.tls.client_ca %s holds no PEM certificate", t.ClientCA)
	}
	cfg.ClientCAs = pool
	cfg.ClientAuth = tls.VerifyClientCertIfGiven
	if t.RequireClientCert {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}
