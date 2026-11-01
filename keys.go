package runnel

import (
	"context"
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

// APIKey grants roles in one namespace. Only the SHA-256 hash of its secret is
// stored.
type APIKey struct {
	ID        string    `json:"id"`
	Namespace string    `json:"namespace"`
	Name      string    `json:"name,omitempty"`
	Hash      string    `json:"hash"`
	Roles     []string  `json:"roles"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at,omitzero"`
}

const (
	// RoleAdmin may do everything in its namespace, including changing
	// configuration and retrying dead jobs.
	RoleAdmin = "admin"
	// RoleProducer may enqueue and cancel jobs.
	RoleProducer = "producer"
	// RoleWorker may claim, heartbeat and finish jobs.
	RoleWorker = "worker"
	// RoleReader may read jobs, events and stats.
	RoleReader = "reader"
)

// Roles lists every role.
var Roles = []string{RoleAdmin, RoleProducer, RoleWorker, RoleReader}

// Validate checks the namespace, id, hash and roles.
func (k APIKey) Validate() error {
	if !ValidNamespace(k.Namespace) {
		return invalid("namespace %q must match %s", k.Namespace, namespacePattern)
	}
	if len(k.ID) != 12 || k.Hash == "" {
		return invalid("key id and hash are required")
	}
	if len(k.Roles) == 0 {
		return invalid("a key needs at least one role")
	}
	for _, r := range k.Roles {
		if !slices.Contains(Roles, r) {
			return invalid("unknown role %q", r)
		}
	}
	if len(k.Name) > 100 {
		return invalid("key name too long")
	}
	return nil
}

// PutKey stores an API key.
func PutKey(ctx context.Context, s Store, k APIKey) error {
	if err := k.Validate(); err != nil {
		return err
	}
	return PutTyped(ctx, s, "", ConfigKey, k.ID, k)
}

// Keys returns every API key by id.
func Keys(ctx context.Context, s Store) (map[string]APIKey, error) {
	return LoadTyped[APIKey](ctx, s, "", ConfigKey)
}

// NamespaceKeys returns the keys of one namespace, oldest first.
func NamespaceKeys(ctx context.Context, s Store, ns string) ([]APIKey, error) {
	all, err := Keys(ctx, s)
	if err != nil {
		return nil, err
	}
	out := []APIKey{}
	for _, k := range all {
		if k.Namespace == ns {
			out = append(out, k)
		}
	}
	slices.SortFunc(out, func(a, b APIKey) int { return a.CreatedAt.Compare(b.CreatedAt) })
	return out, nil
}

// DeleteKey revokes a key of the namespace.
func DeleteKey(ctx context.Context, s Store, ns, id string) error {
	all, err := Keys(ctx, s)
	if err != nil {
		return err
	}
	if k, ok := all[id]; !ok || k.Namespace != ns {
		return ErrNotFound
	}
	return s.DeleteConfig(ctx, "", ConfigKey, id)
}
