# Benchmarks

These numbers give you a feel for each store. They are not a promise: your hardware, your disk and your handlers decide what you get. Run the benchmark on your own machine before you plan around it.

## How to run it

```
make bench                                   # SQLite by default
RUNNEL_STORE=postgres://... make bench       # any store URL
runnel bench --jobs 100000 --workers 4 --concurrency 50 --payload 1024
```

`runnel bench` enqueues jobs in batches, then runs them with workers whose handler does nothing, so it measures Runnel and the store, not your code. It prints how fast jobs were enqueued and run, and how long one job takes from enqueue to result.

| Flag | Default | What it sets |
|---|---|---|
| `--jobs` | 20000 | how many jobs |
| `--workers` | 4 | workers, all running inside the benchmark process |
| `--concurrency` | 25 | slots per worker |
| `--batch` | 100 | jobs per enqueue call |
| `--groups` | 0 | spread jobs over this many groups |
| `--group-max-running` | 0 | cap per group, to measure the fair path |
| `--latency` | 200 | single jobs timed from enqueue to result |
| `--payload` | 0 | payload size in bytes |

The benchmark creates real jobs. Point it at a throwaway database, not production.

## Results on a desktop

The test: 20,000 jobs in batches of 100, then 4 workers with 25 slots each. "Fair" runs 10,000 jobs over 50 groups with `group_max_running: 2`, which takes the slower path that checks limits. The last two columns are the time for one job from enqueue to finished result, through `Worker.Do` (direct start) or `EnqueueAndWait`.

| Store | Enqueue | Run | Run, fair | Direct start p50 | Enqueue and wait p50 |
|---|---|---|---|---|---|
| Postgres 17 | 3,300/s | 1,160/s | 675/s | 58 ms | 25 ms |
| MySQL 8.4 | 1,070/s | 270/s | 240/s | 81 ms | 75 ms |
| SQLite (WAL) | 16,200/s | 2,700/s | 1,390/s | 0.8 ms | 51 ms |
| Redis 7.4 | 17,000/s | 6,060/s | 3,110/s | 1.1 ms | 1.1 ms |
| Valkey 8.1 | 16,800/s | 5,640/s | 2,900/s | 1.0 ms | 1.2 ms |
| Dragonfly 1.32 | 8,500/s | 2,290/s | 1,520/s | 2.0 ms | 1.6 ms |

The machine: a 2014 desktop (Intel i5-4460, 4 cores, 16 GB, a consumer SSD). The benchmark and every database ran in Docker on the same machine, all with their default durability.

A few things to keep in mind:

- The SQL numbers are limited by how fast the disk flushes a commit, about 8 ms here. Server disks flush much faster, so expect a lot more there.
- Numbers move by about 20% from one run to the next.
- MySQL ran with `--skip-log-bin`. With the binary log on, every commit pays for a second flush.
- SQLite and MySQL have no wake-up signal, so waiting for a result there means polling.

## The hybrid store

The hybrid store was measured on a different machine, a laptop with a SATA SSD that takes about 70 ms to flush. Compare it with SQLite on the same laptop, not with the table above.

| Store (laptop) | Enqueue | Run | Direct start p50 |
|---|---|---|---|
| SQLite (WAL) | 5,350/s | 635/s | 3.2 ms |
| Redis 7.4 | 17,900/s | 5,950/s | 0.9 ms |
| Hybrid, `sync=each` | 1,850/s | 560/s | 67 ms |
| Hybrid, `sync=each`, 4 workers with 200 slots | | 3,000 to 4,800/s | |
| Hybrid, `sync=periodic` | 215,000/s | 110,000 to 130,000/s | 0.03 ms |
| Hybrid, `sync=off` | 240,000/s | 127,000 to 130,000/s | 0.03 ms |
| Memory only | 300,000/s | 160,000 to 166,000/s | 0.02 ms |

With `sync=each` every enqueue and every result waits for the disk. So the speed grows with how many jobs are in flight at once, not with how fast the CPU is. With `sync=periodic` the disk is flushed every 100 ms instead, and a crash of the whole machine can lose that last 100 ms.

The store on its own (`go test -bench Cycle ./store/hybrid`) claims and finishes 190,000 jobs a second in memory, and 115,000 to 135,000 with `sync=periodic`.

## Small containers and big backlogs

These runs check that Runnel stays within its memory when there is far more work than memory:

| Container | Jobs | Enqueue | Run | Memory |
|---|---|---|---|---|
| 500 MB, 4 CPUs, SSD, `sync=periodic` | 500,000 | all before the first ran | 72,000/s | flat |
| 500 MB, 2 CPUs, 7200 rpm hard disk | 2,000,000 empty | 22,000/s | 9,000/s | flat |
| 500 MB, 2 CPUs, 7200 rpm hard disk | 1,000,000 of 1 KB | 7,500/s | 4,000/s | flat |

When the backlog is bigger than memory, waiting jobs live on disk and come back in batches. Those jobs run as fast as the disk allows, so a hard disk is much slower than an SSD here.
