/**
 * Homework 3, Part 2.1: trivial serial execution.
 *
 * One thread, one connection, one transaction after the other. Nothing runs concurrently,
 * so there are no lock waits, no deadlocks and no restarts. Same workload, same statements
 * and same options as StreamBenchmark, so the two are directly comparable.
 *
 * Usage:
 *   java SerialBenchmark --count 20000 --opts baseline     fixed list (same list as StreamBenchmark --count)
 *   java SerialBenchmark --seconds 30 --opts baseline      continuous stream
 */
public class SerialBenchmark {
    public static void main(String[] args) throws Exception {
        StreamBenchmark.Config c = StreamBenchmark.parse(args);
        c.serial = true;
        StreamBenchmark.run(c);
    }
}
