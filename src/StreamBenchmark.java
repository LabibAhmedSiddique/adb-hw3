import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Homework 3: a continuous stream of transactions, executed concurrently (Part 1.2, 1.3, 2.3).
 *
 * Usage:
 *   java StreamBenchmark --threads 4 --seconds 30 --opts baseline      continuous stream
 *   java StreamBenchmark --threads 4 --count 20000 --opts optimized    fixed list (compare with serial)
 *   options: --warmup 5  --queries 3  --seed 42  --progress
 *
 * Continuous stream: every worker thread runs a loop
 *     dice a new transaction -> execute it until it commits -> dice the next one
 * so exactly <threads> transactions are in flight at any moment. A new transaction only
 * starts when one has committed: arrivals = commits, which is the equilibrium the task asks for.
 */
public class StreamBenchmark {

    static final class Config {
        int threads = 4;
        int seconds = 30;
        int warmup = 5;
        int count = 0;             // > 0: fixed list of transactions instead of a time limit
        long seed = 42;
        boolean progress = false;
        boolean serial = false;    // set by SerialBenchmark: 1 thread, no gate
        String opts = "baseline";
    }

    public static void main(String[] args) throws Exception {
        run(parse(args));
    }

    static Config parse(String[] args) {
        Config c = new Config();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--threads": c.threads = Integer.parseInt(args[++i]); break;
                case "--seconds": c.seconds = Integer.parseInt(args[++i]); break;
                case "--warmup": c.warmup = Integer.parseInt(args[++i]); break;
                case "--count": c.count = Integer.parseInt(args[++i]); break;
                case "--queries": Workload.queriesPerTask = Integer.parseInt(args[++i]); break;
                case "--seed": c.seed = Long.parseLong(args[++i]); break;
                case "--opts": c.opts = args[++i]; break;
                case "--progress": c.progress = true; break;
                default: throw new IllegalArgumentException("Unknown argument " + args[i]);
            }
        }
        return c;
    }

    static void run(Config c) throws Exception {
        Options opt = Options.parse(c.opts);
        int threads = c.serial ? 1 : c.threads;
        String label = (c.serial ? "SERIAL" : "CONCURRENT threads=" + threads) + " opts=" + opt;

        Workload.setupDatabase();
        Stats stats = new Stats();
        ReentrantReadWriteLock gate = c.serial ? null : new ReentrantReadWriteLock(true);
        ConsoleOutput out = new ConsoleOutput(opt.asyncOutput);
        AtomicInteger txnCounter = new AtomicInteger();

        final List<Workload.Txn> fixed = c.count > 0 ? Workload.fixedStream(c.count, c.seed) : null;
        final AtomicInteger nextIndex = new AtomicInteger();
        final Workload workload = new Workload();
        final long warmupNanos = c.count > 0 ? 0 : c.warmup * 1_000_000_000L;
        final long start = System.nanoTime();
        final long stopAt = start + warmupNanos + c.seconds * 1_000_000_000L;
        if (fixed != null) stats.measuring = true;

        List<Thread> workers = new ArrayList<>();
        List<Throwable> failures = new ArrayList<>();
        for (int w = 0; w < threads; w++) {
            final int workerId = w;
            Thread t = new Thread(() -> {
                try (TxnRunner runner = new TxnRunner(opt, out, stats, gate)) {
                    Random rnd = new Random(c.seed * 1000 + workerId);
                    while (true) {
                        Workload.Txn txn;
                        if (fixed != null) {
                            int i = nextIndex.getAndIncrement();
                            if (i >= fixed.size()) break;
                            txn = fixed.get(i);
                        } else {
                            if (System.nanoTime() >= stopAt) break;
                            txn = workload.next(rnd);   // restart: a fresh transaction replaces the committed one
                        }
                        long t0 = System.nanoTime();
                        int attempts = runner.execute(txn, txnCounter.incrementAndGet());
                        stats.committed(txn.kind, System.nanoTime() - t0, attempts);
                    }
                } catch (Throwable e) {
                    synchronized (failures) { failures.add(e); }
                }
            }, "worker-" + w);
            workers.add(t);
            t.start();
        }

        // Warm-up: let caches and the JIT settle, then start counting.
        long measureStart = start;
        if (fixed == null) {
            Thread.sleep(c.warmup * 1000L);
            stats.measuring = true;
            measureStart = System.nanoTime();
        }

        // Optional once-per-second progress line (used for the live demo).
        Thread reporter = null;
        if (c.progress) {
            reporter = new Thread(() -> {
                long last = 0;
                int second = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    try { Thread.sleep(1000); } catch (InterruptedException e) { return; }
                    long now = stats.totalCommits();
                    System.err.printf("[t=%2ds] %5d commits/s  (in flight: %d)%n", ++second, now - last, threads);
                    last = now;
                }
            });
            reporter.setDaemon(true);
            reporter.start();
        }

        for (Thread t : workers) t.join();
        double seconds = (System.nanoTime() - measureStart) / 1e9;
        if (reporter != null) reporter.interrupt();
        out.close();

        if (!failures.isEmpty()) {
            System.err.println("[ERROR] " + failures.size() + " worker(s) failed: " + failures.get(0));
            System.exit(1);
        }
        if (fixed != null && stats.totalCommits() != fixed.size()) {
            System.err.println("[ERROR] only " + stats.totalCommits() + " of " + fixed.size() + " committed");
            System.exit(1);
        }
        System.err.print(stats.report(label, seconds));
    }
}
