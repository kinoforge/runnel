package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/kinoforge/runnel"
	"github.com/kinoforge/runnel/store/hybrid"
	"github.com/kinoforge/runnel/store/hybrid/wal"
	"github.com/kinoforge/runnel/stores"
)

// backupCommand writes a backup to a file: from a running server with
// --server, or from the store itself when no server holds it.
func backupCommand(ctx context.Context, args []string) error {
	c := newCmd("backup")
	out := c.fs.String("o", "", "file to write, - for stdout (required)")
	srv := c.fs.String("server", os.Getenv("RUNNEL_SERVER"), "server url, such as http://localhost:8700 (default RUNNEL_SERVER)")
	token := c.fs.String("admin-token", "", "admin token (default RUNNEL_ADMIN_TOKEN)")
	if _, err := c.parse(args, 0); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("backup needs -o FILE")
	}
	cfg, err := c.settings()
	if err != nil {
		return err
	}
	if *token == "" {
		*token = cfg.Server.AdminToken
	}
	write := func(w io.Writer) error {
		if *srv != "" {
			return fetchBackup(ctx, *srv, *token, w)
		}
		s, err := open(ctx, cfg)
		if errors.Is(err, wal.ErrLocked) {
			return errors.New("a server holds this store; back it up through it with --server URL")
		}
		if err != nil {
			return err
		}
		defer s.Close()
		b, ok := s.(runnel.Backuper)
		if !ok {
			return errors.New("this store has no backup; use the database's own backup tools")
		}
		return b.Backup(ctx, w)
	}
	if *out == "-" {
		return write(os.Stdout)
	}
	return writeAtomic(*out, write)
}

func fetchBackup(ctx context.Context, base, token string, w io.Writer) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/v1/backup", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("server answered %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

// writeAtomic writes path whole or leaves it untouched.
func writeAtomic(path string, write func(io.Writer) error) error {
	tmp := path + ".part"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	err = write(f)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	fmt.Fprintln(os.Stderr, "backup written to", path)
	return nil
}

// restoreCommand unpacks a backup into the empty directory of a hybrid store
// and opens it once to check it replays.
func restoreCommand(ctx context.Context, args []string) error {
	c := newCmd("restore")
	pos, err := c.parse(args, 1)
	if err != nil {
		return err
	}
	cfg, err := c.settings()
	if err != nil {
		return err
	}
	dir, err := stores.HybridDir(cfg.Store)
	if err != nil {
		return err
	}
	f, err := os.Open(pos[0])
	if err != nil {
		return err
	}
	defer f.Close()
	if err := hybrid.Restore(f, dir); err != nil {
		return err
	}
	s, err := open(ctx, cfg)
	if err != nil {
		return fmt.Errorf("restored files do not open: %w", err)
	}
	all, err := runnel.Namespaces(ctx, s)
	err = errors.Join(err, s.Close())
	if err != nil {
		return err
	}
	fmt.Printf("restored %d namespace(s) into %s\n", len(all), dir)
	return nil
}
