// Package hybrid is a Runnel store that keeps every job in memory, indexed for
// claiming, and makes each change durable in a write-ahead log before the call
// returns. Snapshots keep the log short and restarts fast.
//
// One process owns the data directory. Workers in other processes reach it
// through the Runnel server. Memory returns the same store with no disk at all,
// for tests and throwaway queues.
package hybrid
