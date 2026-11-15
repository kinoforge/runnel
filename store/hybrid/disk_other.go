//go:build !linux && !darwin

package hybrid

func freeBytes(string) (uint64, bool) { return 0, false }
