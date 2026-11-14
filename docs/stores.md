# Stores

Runnel keeps its jobs in a store. You pick one with a URL, through `--store`, `RUNNEL_STORE` or the `store` key in the config file. Every store runs the same rules and passes the same test suite, so you can start on one and move to another later.

```
postgres://user:pass@host/db
mysql://user:pass@host/db
sqlite:runnel.db
redis://host:6379/0          (also rediss://, valkey://, dragonfly://)
hybrid:/var/lib/runnel       (add ?sync=each, periodic or off)
memory:
```

## Which one to use

| | Postgres | MySQL 8 | SQLite | Redis, Valkey, Dragonfly | Hybrid |
|---|---|---|---|---|---|
| Every feature | yes | yes | yes | yes, except transactional enqueue | yes, except transactional enqueue |
| Workers wake at once | LISTEN/NOTIFY | polling (1s by default) | polling | pub/sub | in process |
| Waiters wake at once | LISTEN/NOTIFY | polling | polling | pub/sub | in process |
| Claims without limits | `SKIP LOCKED`, no lock | `SKIP LOCKED`, no lock | one writer | one Lua script | in memory, one lock per namespace |
| Good for | production | production | tests, tools, one machine | high throughput | the highest throughput on one machine, no database |

If you already run Postgres, start there. If you want speed and no database at all, use the hybrid store. Use SQLite for tests and small tools.

## SQL connections

Postgres and MySQL keep a pool of connections. By default it holds up to 20 open and 10 idle connections. If your database allows only a few connections, as many hosted free tiers do, set the pool in the store URL:

```
postgres://user:pass@host/db?sslmode=require&max_conns=4&max_idle_conns=2
```

| Parameter | Default | Meaning |
|---|---|---|
| `max_conns` | 20 | open connections at most |
| `max_idle_conns` | 10 | idle connections kept, never more than `max_conns` |
| `conn_max_lifetime` | 30m | a connection is replaced after this long |
| `conn_max_idle_time` | 5m | an idle connection is closed after this long |

Runnel removes these parameters before it connects, so the database never sees them. On Postgres, `runnel serve` also holds one more connection for LISTEN/NOTIFY.

## The hybrid store

`hybrid:/var/lib/runnel` needs no database. It keeps a small record of every active job in memory, about 290 bytes whatever the payload, and writes each change to a log on disk before it answers.

About a second after a job finishes, it moves to an embedded store on disk (Pebble, inside the same binary and data folder). So keeping finished jobs for days costs disk, not memory. Waiting jobs past a quarter of the memory limit go there too, in the order they will run, and come back in batches as workers reach them. A backlog is limited by your disk, not your memory.

Payloads, results, meta and errors are written once and read back through the OS page cache. Runnel keeps Go's heap under 70% of the container's memory limit and leaves the rest for that cache. Every size and timing is worked out from the memory limit. The `memory` and `hybrid` sections of the config file change them if you need to.

How safe each sync mode is:

| `?sync=` | Answers after | A crash of the process loses | A crash of the machine loses |
|---|---|---|---|
| `each` (default) | the disk flush | nothing | nothing |
| `periodic` | the write to the OS | nothing | the last 100 ms |
| `off` | memory | the last 100 ms | the last 100 ms |

One process owns the folder. Run it behind `runnel serve` and point your workers at the server. `memory:` is the same store with no disk, for tests and throwaway queues.

The folder has a `FORMAT` file. A Runnel version that cannot read the folder refuses to start instead of guessing. Back the store up while it runs with `runnel backup`, and see the [runbook](RUNBOOK.md) for restores.

## Redis, Valkey and Dragonfly

The rules run as Lua scripts. All keys of a namespace share one hash tag, so it works on Redis Cluster.

One limit to know: a claim looks at no more than 2,000 waiting jobs. If one group at its limit has more jobs than that at the front of the line, other groups wait until its jobs drain below that depth. The SQL stores skip full groups inside the query and have no such limit.

## Backups

| Store | How |
|---|---|
| Hybrid | `runnel backup` while it runs, `runnel restore` to bring it back |
| Postgres, MySQL | your database's own tools: dumps, snapshots, point in time recovery |
| Redis family | RDB snapshots or AOF, as for any Redis |
| SQLite | copy the file while nothing writes, or use `sqlite3 .backup` |

## Writing your own store

Implement `runnel.Store` and run `storetest.Run` against it. The [contributing guide](../.github/CONTRIBUTING.md) lists the optional interfaces that add features.
