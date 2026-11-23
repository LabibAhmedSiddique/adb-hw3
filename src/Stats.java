import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/** Thread-safe counters for one benchmark run. Only counts while measuring is on (after warm-up). */
public final class Stats {
    private final int kinds = Workload.Kind.values().length;
    private final LongAdder[] commits = new LongAdder[kinds];
    private final LongAdder[] nanos = new LongAdder[kinds];
    final LongAdder retries = new LongAdder();
    final LongAdder deadlocks = new LongAdder();
    final AtomicInteger maxAttempts = new AtomicInteger();
    volatile boolean measuring;

    public Stats() {
        for (int i = 0; i < kinds; i++) { commits[i] = new LongAdder(); nanos[i] = new LongAdder(); }
    }

    void committed(Workload.Kind kind, long elapsedNanos, int attempts) {
        if (!measuring) return;
        commits[kind.ordinal()].increment();
        nanos[kind.ordinal()].add(elapsedNanos);
        maxAttempts.accumulateAndGet(attempts, Math::max);
    }

    void retried(SQLException e) {
        if (!measuring) return;
        retries.increment();
        if ("40P01".equals(e.getSQLState())) deadlocks.increment();
    }

    long totalCommits() {
        long sum = 0;
        for (LongAdder a : commits) sum += a.sum();
        return sum;
    }

    /** One summary line plus a per-kind breakdown (count and mean latency). */
    String report(String label, double seconds) {
        long total = totalCommits();
        StringBuilder b = new StringBuilder();
        b.append(String.format("[RESULT] %s | %d commits in %.1f s | throughput %.0f txn/s | retries %d (deadlocks %d) | max attempts %d%n",
                label, total, seconds, total / seconds, retries.sum(), deadlocks.sum(), maxAttempts.get()));
        for (Workload.Kind k : Workload.Kind.values()) {
            long c = commits[k.ordinal()].sum();
            double meanMs = c == 0 ? 0 : nanos[k.ordinal()].sum() / 1e6 / c;
            double shareOfTime = 0;
            long allNanos = 0;
            for (LongAdder a : nanos) allNanos += a.sum();
            if (allNanos > 0) shareOfTime = 100.0 * nanos[k.ordinal()].sum() / allNanos;
            b.append(String.format("         %-16s %7d commits  mean %6.2f ms  share of worker time %5.1f%%%n",
                    k, c, meanMs, shareOfTime));
        }
        return b.toString();
    }
}
