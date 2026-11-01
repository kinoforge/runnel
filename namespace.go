package runnel

import (
	"context"
	"time"
)

const (
	maxWebhookSecret = 256
)

// Namespace is one isolated queue system, usually one per service. WebhookSecret
// signs its webhooks.
type Namespace struct {
	Name          string    `json:"name"`
	WebhookSecret string    `json:"webhook_secret,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// Validate checks the name and secret length.
func (n Namespace) Validate() error {
	if !ValidNamespace(n.Name) {
		return invalid("namespace %q must match %s", n.Name, namespacePattern)
	}
	if len(n.WebhookSecret) > maxWebhookSecret {
		return invalid("webhook secret too long")
	}
	return nil
}

// PutNamespace creates or replaces a namespace.
func PutNamespace(ctx context.Context, s Store, n Namespace) error {
	if err := n.Validate(); err != nil {
		return err
	}
	if n.CreatedAt.IsZero() {
		n.CreatedAt = time.Now().UTC()
	}
	return PutTyped(ctx, s, "", ConfigNamespace, n.Name, n)
}

// Namespaces returns every namespace by name.
func Namespaces(ctx context.Context, s Store) (map[string]Namespace, error) {
	return LoadTyped[Namespace](ctx, s, "", ConfigNamespace)
}

// DeleteNamespace removes a namespace and revokes its keys. Its jobs stay until
// they are pruned.
func DeleteNamespace(ctx context.Context, s Store, name string) error {
	if err := s.DeleteConfig(ctx, "", ConfigNamespace, name); err != nil {
		return err
	}
	keys, err := NamespaceKeys(ctx, s, name)
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := s.DeleteConfig(ctx, "", ConfigKey, k.ID); err != nil {
			return err
		}
	}
	return nil
}
