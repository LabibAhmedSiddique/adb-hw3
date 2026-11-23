# ADB Homework 3 — Homework 2 revisited

Throughput benchmark for a realistic transaction mix on PostgreSQL, a serial baseline, and the bottleneck fixes.

## Files
- `Workload.java` — Part 1.1: dices each transaction (60% multi-point read, 10% insert, 10% full scan, 20% write of 3 single tuples); creates `blocking_data` with 50,000 rows (dropping the table removes earlier inserts)
- `StreamBenchmark.java` — Part 1.2/1.3: continuous stream with N worker threads; each worker starts a new transaction as soon as its last one commits
- `SerialBenchmark.java` — Part 2.1: the same stream on one thread, one transaction after the other
- `TxnRunner.java` — executes one transaction on one connection; retry on 40P01/40001 with backoff and a fair gate (from Homework 2), so every transaction commits
- `Options.java` — Part 2.3: switches each optimization on or off (`baseline`, `optimized`, or a comma list)
- `ConsoleOutput.java` — direct `println` or buffered background output
- `Stats.java` — commits, latency per transaction type, retries
- `postgres_analysis.sql` — Part 2.2: pg_stat_statements, EXPLAIN, table growth, lock waits
- `ConnectionFactory.java` — not in the repo (holds the password)

## Running (Windows, from the `src` folder)
```
javac -cp ".;..\lib\postgresql-42.7.4.jar" *.java
java -cp ".;..\lib\postgresql-42.7.4.jar" StreamBenchmark --threads 4 --seconds 30 --opts baseline
java -cp ".;..\lib\postgresql-42.7.4.jar" StreamBenchmark --threads 4 --seconds 30 --opts optimized
java -cp ".;..\lib\postgresql-42.7.4.jar" SerialBenchmark --count 20000 --opts baseline
java -cp ".;..\lib\postgresql-42.7.4.jar" StreamBenchmark --threads 4 --count 20000 --opts optimized
```
Arguments: `--threads N`, `--seconds S` (continuous, after `--warmup 5`), `--count N` (fixed list, same list for serial and concurrent), `--opts baseline|optimized|prepare,output,async-commit,read-autocommit,deadlock-timeout`, `--queries 3`, `--seed 42`, `--progress`.

Query results go to stdout, the summary to stderr. To benchmark without filling the terminal: `... > out.txt`.

Live demo (30 s): `java ... StreamBenchmark --threads 4 --seconds 30 --warmup 2 --opts optimized --progress > out.txt`

`deadlock-timeout` uses `SET deadlock_timeout`, which needs the `postgres` superuser.

