import java.io.BufferedWriter;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Read results must be printed to the console (task 1.1). Two ways:
 *
 *  - direct: System.out.println in the worker thread. println is synchronized and flushes
 *    on every line, so all workers queue up on one lock and pay one system call per line.
 *  - async:  workers only put the line into a queue; one background thread writes the
 *    lines in large buffered blocks. Every line is still printed.
 */
public final class ConsoleOutput implements AutoCloseable {
    private final boolean async;
    private final LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();
    private volatile boolean running = true;
    private final Thread writer;

    public ConsoleOutput(boolean async) {
        this.async = async;
        if (async) {
            writer = new Thread(this::drain, "console-writer");
            writer.setDaemon(true);
            writer.start();
        } else {
            writer = null;
        }
    }

    public void print(String line) {
        if (async) queue.offer(line);
        else System.out.println(line);
    }

    private void drain() {
        try (BufferedWriter out = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(FileDescriptor.out)), 1 << 16)) {
            List<String> batch = new ArrayList<>(1024);
            while (running || !queue.isEmpty()) {
                String first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) { out.flush(); continue; }
                batch.add(first);
                queue.drainTo(batch, 1023);
                for (String s : batch) { out.write(s); out.newLine(); }
                batch.clear();
            }
            out.flush();
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws InterruptedException {
        if (async) {
            running = false;
            writer.join();
        } else {
            System.out.flush();
        }
    }
}
