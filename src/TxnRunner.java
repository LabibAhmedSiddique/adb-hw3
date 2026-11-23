import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Runs transactions on ONE connection (one TxnRunner per worker thread).
 *
 * execute() is the restart logic from Homework 2 (RestartBlocking + NoLiveLocks):
 *   - roll back and retry on deadlock (40P01) / serialization failure (40001)
 *   - exponential backoff with jitter between attempts
 *   - after 3 aborts the transaction takes the fair gate exclusively and runs alone,
 *     so every transaction commits after at most 4 attempts (no starvation)
 */
public final class TxnRunner implements AutoCloseable {
    static final String READ_SQL   = "SELECT data_value FROM blocking_data WHERE data_id = ?";
    static final String INSERT_SQL = "INSERT INTO blocking_data VALUES (?, ?, ?)";
    static final String SCAN_SQL   = "SELECT sum(data_value) FROM blocking_data";
    static final String UPDATE_SQL = "UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?";

    static final int MAX_SHARED_ATTEMPTS = 3;
    static final int BACKOFF_BASE_MS = 10;

    private final Connection con;
    private final Options opt;
    private final ConsoleOutput out;
    private final Stats stats;
    private final ReentrantReadWriteLock gate;   // null in serial mode: nothing to coordinate

    // Only used when opt.prepareOnce: prepared once, reused for every transaction.
    private PreparedStatement readPs, insertPs, scanPs, updatePs;

    public TxnRunner(Options opt, ConsoleOutput out, Stats stats, ReentrantReadWriteLock gate) throws SQLException {
        this.opt = opt;
        this.out = out;
        this.stats = stats;
        this.gate = gate;
        this.con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
        con.setAutoCommit(false);
        con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        try (Statement st = con.createStatement()) {
            if (opt.asyncCommit) st.execute("SET synchronous_commit = off");
            if (opt.deadlockTimeoutMs > 0) {
                try {
                    st.execute("SET deadlock_timeout = '" + opt.deadlockTimeoutMs + "ms'");
                } catch (SQLException e) {   // needs a superuser; keep going without it
                    System.err.println("[WARN] could not set deadlock_timeout: " + e.getMessage());
                    con.rollback();
                }
            }
        }
        con.commit();
        if (opt.prepareOnce) {
            readPs = con.prepareStatement(READ_SQL);
            insertPs = con.prepareStatement(INSERT_SQL);
            scanPs = con.prepareStatement(SCAN_SQL);
            updatePs = con.prepareStatement(UPDATE_SQL);
        }
    }

    /** Run t until it commits. Returns the number of attempts it needed. */
    public int execute(Workload.Txn t, int txnNo) {
        int failures = 0;
        while (true) {
            Lock side = null;
            if (gate != null) side = failures >= MAX_SHARED_ATTEMPTS ? gate.writeLock() : gate.readLock();
            if (side != null) side.lock();
            String result = null;
            boolean committed = false;
            try {
                result = attempt(t, txnNo);
                committed = true;
            } catch (SQLException e) {
                rollbackQuietly();                       // release all row locks
                if (!isRetryable(e)) {
                    throw new IllegalStateException("Non-retryable failure in " + t.kind + ": " + e.getMessage(), e);
                }
                stats.retried(e);
            } finally {
                if (side != null) side.unlock();
            }
            if (committed) {
                if (result != null) out.print(result);  // output "at completion", outside the gate
                return failures + 1;
            }
            failures++;
            backoff(failures);
        }
    }

    /** One attempt of the whole transaction, ending with COMMIT. Returns the line to print, if any. */
    private String attempt(Workload.Txn t, int txnNo) throws SQLException {
        boolean readOnly = t.kind == Workload.Kind.MULTI_POINT_READ || t.kind == Workload.Kind.FULL_SCAN;
        boolean autocommit = opt.readAutocommit && readOnly;
        if (autocommit) con.setAutoCommit(true);
        try {
            switch (t.kind) {
                case MULTI_POINT_READ: {
                    PreparedStatement ps = statement(READ_SQL, readPs);
                    try {
                        double sum = 0;
                        for (int id : t.ids) {   // one SQL statement per tuple
                            ps.setInt(1, id);
                            try (ResultSet rs = ps.executeQuery()) {
                                if (rs.next()) sum += rs.getDouble(1);
                            }
                        }
                        if (!autocommit) con.commit();
                        return "Read " + Arrays.toString(t.ids) + " sum=" + sum;
                    } finally { release(ps); }
                }
                case INSERT: {
                    PreparedStatement ps = statement(INSERT_SQL, insertPs);
                    try {
                        ps.setInt(1, t.ids[0]);
                        ps.setDouble(2, t.values[0]);
                        ps.setInt(3, txnNo);
                        ps.executeUpdate();
                        con.commit();
                        return null;
                    } finally { release(ps); }
                }
                case FULL_SCAN: {
                    PreparedStatement ps = statement(SCAN_SQL, scanPs);
                    try {
                        double sum;
                        try (ResultSet rs = ps.executeQuery()) {
                            rs.next();
                            sum = rs.getDouble(1);
                        }
                        if (!autocommit) con.commit();
                        return "Scan sum=" + sum;
                    } finally { release(ps); }
                }
                case WRITE: {
                    PreparedStatement ps = statement(UPDATE_SQL, updatePs);
                    try {
                        for (int i = 0; i < t.ids.length; i++) {  // same order as generated
                            ps.setDouble(1, t.values[i]);
                            ps.setInt(2, txnNo);
                            ps.setInt(3, t.ids[i]);            // primary key: exactly one tuple
                            ps.executeUpdate();
                        }
                        con.commit();
                        return null;
                    } finally { release(ps); }
                }
                default:
                    throw new IllegalArgumentException("Unknown kind " + t.kind);
            }
        } finally {
            if (autocommit) con.setAutoCommit(false);
        }
    }

    private PreparedStatement statement(String sql, PreparedStatement cached) throws SQLException {
        return opt.prepareOnce ? cached : con.prepareStatement(sql);
    }

    private void release(PreparedStatement ps) throws SQLException {
        if (!opt.prepareOnce) ps.close();
    }

    static boolean isRetryable(SQLException e) {
        String state = e.getSQLState();
        return "40P01".equals(state) || "40001".equals(state);
    }

    private void rollbackQuietly() {
        try {
            if (!con.getAutoCommit()) con.rollback();
        } catch (SQLException ignored) {
        }
    }

    private static void backoff(int failures) {
        long max = (long) BACKOFF_BASE_MS << Math.min(failures - 1, 10);
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(max + 1));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        try { con.close(); } catch (SQLException ignored) { }
    }
}
