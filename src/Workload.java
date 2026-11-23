import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Homework 3, Part 1.1: the realistic transaction mix.
 *
 * For every transaction we dice p in [0,1) (i.i.d.):
 *   p <  0.6  MULTI_POINT_READ  read data_value of 3 random distinct tuples, print the sum
 *   p <  0.7  INSERT            insert one new tuple
 *   p <  0.8  FULL_SCAN         SELECT sum(data_value) FROM blocking_data, print the result
 *   p >= 0.8  WRITE             3 UPDATEs, each changes exactly one tuple (by primary key)
 *
 * The table, its 50,000 rows and the seed are the same as in Blocking.java.
 */
public final class Workload {
    public static final int NUM_TUPLES = 50000;            // do not increase (task hint)
    public static final double MAX_VALUE = 50.0;
    public static int queriesPerTask = 3;                   // num_queries_per_task

    public enum Kind { MULTI_POINT_READ, INSERT, FULL_SCAN, WRITE }

    /** One transaction, fully decided before it runs, so a retry repeats exactly the same work. */
    public static final class Txn {
        public final Kind kind;
        public final int[] ids;        // tuples to read / update, or the new id for an INSERT
        public final double[] values;  // new data_value per UPDATE, or the value for an INSERT

        Txn(Kind kind, int[] ids, double[] values) {
            this.kind = kind;
            this.ids = ids;
            this.values = values;
        }
    }

    /** New tuples get ids 50000, 50001, ... (unique across all threads). */
    private final AtomicInteger nextInsertId = new AtomicInteger(NUM_TUPLES);

    /** Dice one new transaction. */
    public Txn next(Random r) {
        double p = r.nextDouble();
        if (p < 0.6) {
            return new Txn(Kind.MULTI_POINT_READ, distinctIds(r, queriesPerTask), null);
        } else if (p < 0.7) {
            return new Txn(Kind.INSERT, new int[] { nextInsertId.getAndIncrement() },
                    new double[] { r.nextDouble() * MAX_VALUE });
        } else if (p < 0.8) {
            return new Txn(Kind.FULL_SCAN, new int[0], null);
        } else {
            int[] ids = distinctIds(r, queriesPerTask);  // random order: we do NOT sort them (task hint)
            double[] values = new double[ids.length];
            for (int i = 0; i < ids.length; i++) values[i] = (float) (r.nextDouble() * MAX_VALUE);
            return new Txn(Kind.WRITE, ids, values);
        }
    }

    /** n distinct tuple ids from the original 50,000 rows. */
    static int[] distinctIds(Random r, int n) {
        int[] out = new int[n];
        int k = 0;
        while (k < n) {
            int candidate = r.nextInt(NUM_TUPLES);
            boolean duplicate = false;
            for (int j = 0; j < k; j++) if (out[j] == candidate) duplicate = true;
            if (!duplicate) out[k++] = candidate;
        }
        return out;
    }

    /** A fixed, reproducible stream: the same seed always gives the same transactions. */
    public static List<Txn> fixedStream(int count, long seed) {
        Workload w = new Workload();
        Random r = new Random(seed);
        List<Txn> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) list.add(w.next(r));
        return list;
    }

    /**
     * (Re)create blocking_data with the original 50,000 rows. Dropping the table also
     * removes all tuples inserted by an earlier run ("remove them before restarting").
     */
    public static void setupDatabase() throws SQLException {
        try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
            try (Statement st = con.createStatement()) {
                st.execute("DROP TABLE IF EXISTS blocking_data");
                st.execute("CREATE TABLE blocking_data (data_id integer primary key, data_value float, modified_by integer)");
            }
            con.setAutoCommit(false);
            Random rand = new Random(123456);  // same seed as Blocking.java
            try (PreparedStatement ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)")) {
                for (int i = 0; i < NUM_TUPLES; i++) {
                    ps.setInt(1, i);
                    ps.setDouble(2, rand.nextDouble() * MAX_VALUE);
                    ps.setInt(3, 0);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            con.commit();
            con.setAutoCommit(true);
            try (Statement st = con.createStatement()) {
                st.execute("VACUUM ANALYZE blocking_data");  // every run starts from the same clean state
            }
        }
    }
}
