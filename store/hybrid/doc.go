// Package hybrid is a Runnel store that keeps a small fixed-size record of every
// job in memory, indexed for claiming, and logs each change in a write-ahead
// log. Job bodies (payload, result, meta, errors) are read back from the log and
// snapshots, with a cache of hot bodies sized from the container's memory limit.
// Snapshots keep the log short and restarts fast.
//
// One process owns the data directory. Workers in other processes reach it
// through the Runnel server. Memory returns the same store with no disk at all,
// for tests and throwaway queues.
package hybrid
