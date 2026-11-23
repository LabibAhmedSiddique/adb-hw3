/**
 * Which optimizations (Part 2.3) are switched on. Each one can be toggled on its own,
 * so every bottleneck fix can be measured separately.
 *
 *   baseline   = none of them (the straightforward solution)
 *   optimized  = all of them
 *   or a comma list, e.g.  prepare,async-commit
 */
public final class Options {
    /** Prepare each SQL statement once per connection and reuse it (instead of once per transaction). */
    boolean prepareOnce;
    /** Print query results through a buffered background writer instead of System.out.println. */
    boolean asyncOutput;
    /** SET synchronous_commit = off: COMMIT does not wait for the WAL flush to disk. */
    boolean asyncCommit;
    /** Run read-only transactions in autocommit mode: saves the extra COMMIT round trip. */
    boolean readAutocommit;
    /** SET deadlock_timeout to this many ms (0 = keep the server default of 1 s). */
    int deadlockTimeoutMs;

    static Options parse(String spec) {
        Options o = new Options();
        if (spec == null || spec.equals("baseline")) return o;
        if (spec.equals("optimized")) spec = "prepare,output,async-commit,read-autocommit,deadlock-timeout";
        for (String s : spec.split(",")) {
            switch (s.trim()) {
                case "prepare": o.prepareOnce = true; break;
                case "output": o.asyncOutput = true; break;
                case "async-commit": o.asyncCommit = true; break;
                case "read-autocommit": o.readAutocommit = true; break;
                case "deadlock-timeout": o.deadlockTimeoutMs = 50; break;
                default: throw new IllegalArgumentException("Unknown option: " + s);
            }
        }
        return o;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        if (prepareOnce) b.append("prepare ");
        if (asyncOutput) b.append("output ");
        if (asyncCommit) b.append("async-commit ");
        if (readAutocommit) b.append("read-autocommit ");
        if (deadlockTimeoutMs > 0) b.append("deadlock-timeout=").append(deadlockTimeoutMs).append("ms ");
        return b.length() == 0 ? "baseline" : b.toString().trim();
    }
}
