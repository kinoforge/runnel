// Package hybrid is a Runnel store with no database. It keeps a small record of
// each job in memory and writes every change to a log on disk. One process owns
// the data directory; other processes reach it through the Runnel server.
// Memory returns the same store without a disk, for tests.
package hybrid
