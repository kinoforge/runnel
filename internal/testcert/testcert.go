package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type Authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	Dir  string
	File string
}

func New(t testing.TB) *Authority {
	t.Helper()
	key := newKey(t)
	tmpl := template(t, "test ca")
	tmpl.IsCA, tmpl.BasicConstraintsValid = true, true
	tmpl.KeyUsage = x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	a := &Authority{cert: cert, key: key, Dir: t.TempDir()}
	a.File = write(t, a.Dir, "ca.pem", "CERTIFICATE", der)
	return a
}

func (a *Authority) Server(t testing.TB) (certFile, keyFile string) {
	t.Helper()
	tmpl := template(t, "localhost")
	tmpl.DNSNames, tmpl.IPAddresses = []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	return a.issue(t, "server", tmpl)
}

func (a *Authority) Client(t testing.TB, name string) (certFile, keyFile string) {
	t.Helper()
	tmpl := template(t, name)
	tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	return a.issue(t, "client-"+name, tmpl)
}

func (a *Authority) issue(t testing.TB, file string, tmpl *x509.Certificate) (certFile, keyFile string) {
	t.Helper()
	key := newKey(t)
	tmpl.KeyUsage = x509.KeyUsageDigitalSignature
	der, err := x509.CreateCertificate(rand.Reader, tmpl, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return write(t, a.Dir, file+".pem", "CERTIFICATE", der), write(t, a.Dir, file+".key", "EC PRIVATE KEY", kb)
}

func newKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func template(t testing.TB, cn string) *x509.Certificate {
	t.Helper()
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	return &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: cn}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
}

func write(t testing.TB, dir, name, kind string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
