package runnel

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"slices"
	"strings"
	"time"
)

// HashSecret returns the hex SHA-256 of a key secret.
func HashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// NewAPIKey makes a key for the namespace and returns its bearer token, which is
// not stored, and the key to store.
func NewAPIKey(ns, name string, roles []string, expires time.Time) (string, APIKey, error) {
	id, secret := randomHex(6), randomHex(32)
	k := APIKey{ID: id, Namespace: ns, Name: name, Hash: HashSecret(secret), Roles: slices.Clone(roles), CreatedAt: time.Now().UTC(), ExpiresAt: expires}
	if err := k.Validate(); err != nil {
		return "", APIKey{}, err
	}
	return "rnk_" + id + "_" + secret, k, nil
}

// ParseAPIKey splits a token of the form rnk_<id>_<secret>.
func ParseAPIKey(token string) (id, secret string, ok bool) {
	rest, found := strings.CutPrefix(token, "rnk_")
	if !found {
		return "", "", false
	}
	id, secret, found = strings.Cut(rest, "_")
	if !found || len(id) != 12 || len(secret) != 64 {
		return "", "", false
	}
	return id, secret, true
}

// Matches reports whether secret belongs to the key and the key has not expired
// at now. The comparison takes constant time.
func (k APIKey) Matches(secret string, now time.Time) bool {
	if !k.ExpiresAt.IsZero() && now.After(k.ExpiresAt) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(k.Hash), []byte(HashSecret(secret))) == 1
}

// Has reports whether the key has the role, or the admin role.
func (k APIKey) Has(role string) bool {
	return slices.Contains(k.Roles, role) || slices.Contains(k.Roles, RoleAdmin)
}
