//go:build !unix

package wal

import "os"

func lockFile(*os.File) error { return nil }
