package es.ulpgc.bigdata.a4;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Control experiment, shared-memory side: a thread pool with the SAME kernel and the SAME task
 * granularity as the Spark runs (contiguous row blocks, tasks = 4 x threads), sequential and parallel.
 * Timer: output allocation + executor creation + tasks + termination (as in A3).
 * Flags: --n [--warmup 3] [--reps 10] [--threads 1,2,4,8,12,16] [--kernels blocked|ikj,blocked]
 *        [--tasks-per-thread 4] [--kb 64] [--jb 256] [--out file.csv]
 */
public final class ControlBaseline {
    private static final String HEADER =
        "timestamp_utc,method,kernel,n,threads,tasks,phase,rep,time_seconds,checksum,kb,jb,java_version,os";
    private static volatile double sink = 0.0;

    private static Map<String, String> parse(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) m.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        return m;
    }

    static double[][] sequential(double[][] a, double[][] b, int n, boolean blocked, int kb, int jb) {
        double[][] c = new double[n][n];
        Matrices.multiplyRowsWith(blocked, kb, jb, a, b, c);
        return c;
    }

    static double[][] chunked(double[][] a, double[][] b, int n, int threads, int tasks,
                              boolean blocked, int kb, int jb) throws Exception {
        double[][] c = new double[n][n];
        ExecutorService ex = Executors.newFixedThreadPool(threads);
        int[] bounds = Matrices.splitBounds(n, tasks);
        Future<?>[] fs = new Future<?>[bounds.length - 1];
        try {
            for (int p = 0; p + 1 < bounds.length; p++) {
                final int s = bounds[p];
                final int e = bounds[p + 1];
                fs[p] = ex.submit(() -> {
                    double[][] ar = Arrays.copyOfRange(a, s, e);   // row references only, no data copy
                    double[][] cr = Arrays.copyOfRange(c, s, e);   // writes go to the rows of c
                    Matrices.multiplyRowsWith(blocked, kb, jb, ar, b, cr);
                });
            }
            ex.shutdown();
            for (Future<?> f : fs) f.get();
            if (!ex.awaitTermination(60, TimeUnit.SECONDS)) {
                ex.shutdownNow();
                throw new InterruptedException("Parallel execution timed out.");
            }
        } catch (Exception e) {
            ex.shutdownNow();
            throw e;
        }
        return c;
    }

    private static void consume(double[][] c) { sink = Matrices.checksum(c); }

    private static boolean selfCheck(int kb, int jb) throws Exception {
        int n = 53;
        double[][] a = Matrices.random(n, 42L);
        double[][] b = Matrices.random(n, 84L);
        double[][] ref = Matrices.multiplySequential(a, b);
        boolean ok = Matrices.maxAbsDiff(ref, sequential(a, b, n, true, 7, 11)) == 0.0;
        ok &= Matrices.maxAbsDiff(ref, sequential(a, b, n, true, kb, jb)) == 0.0;
        ok &= Matrices.maxAbsDiff(ref, chunked(a, b, n, 3, 8, true, 7, 11)) == 0.0;
        ok &= Matrices.maxAbsDiff(ref, chunked(a, b, n, 2, 5, false, kb, jb)) == 0.0;
        return ok;
    }

    /** A configuration: kernel + sequential (threads == 0) or thread pool. */
    private static final class Cfg {
        final boolean blocked;
        final int threads;
        Cfg(boolean blocked, int threads) { this.blocked = blocked; this.threads = threads; }
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> o = parse(args);
        int n = Integer.parseInt(o.get("n"));
        int warmup = Integer.parseInt(o.getOrDefault("warmup", "3"));
        int reps = Integer.parseInt(o.getOrDefault("reps", "10"));
        int tpt = Integer.parseInt(o.getOrDefault("tasks-per-thread", "4"));
        int kb = Integer.parseInt(o.getOrDefault("kb", String.valueOf(Matrices.DEFAULT_KB)));
        int jb = Integer.parseInt(o.getOrDefault("jb", String.valueOf(Matrices.DEFAULT_JB)));
        String out = o.getOrDefault("out", "results/raw_control_threads.csv");
        String[] tl = o.getOrDefault("threads", "1,2,4,8,12,16").split(",");
        String[] kl = o.getOrDefault("kernels", "blocked").split(",");

        if (!selfCheck(kb, jb)) {
            System.err.println("ERROR: self-check failed (blocked/chunked kernels must equal i-k-j exactly)");
            System.exit(1);
        }
        System.out.println("Self-check passed. availableProcessors=" + Runtime.getRuntime().availableProcessors()
                           + " java=" + System.getProperty("java.version"));

        List<Cfg> cfgs = new ArrayList<>();
        for (String k : kl) {
            boolean bl = k.trim().equals("blocked");
            if (!bl && !k.trim().equals("ikj")) throw new IllegalArgumentException("kernel: " + k);
            cfgs.add(new Cfg(bl, 0));
            for (String t : tl) cfgs.add(new Cfg(bl, Integer.parseInt(t.trim())));
        }

        Path outPath = Paths.get(out);
        if (outPath.getParent() != null) Files.createDirectories(outPath.getParent());
        if (!Files.exists(outPath) || Files.size(outPath) == 0) {
            Files.write(outPath, (HEADER + "\n").getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        String jv = System.getProperty("java.version");
        String os = System.getProperty("os.name");

        double[][] aw = Matrices.random(n, 1L);
        double[][] bw = Matrices.random(n, 2L);
        for (Cfg c : cfgs) {
            for (int w = 0; w < warmup; w++) {
                long t0 = System.nanoTime();
                double[][] r = run(c, aw, bw, n, tpt, kb, jb);
                double t = (System.nanoTime() - t0) / 1e9;
                consume(r);
                record(outPath, c, n, tpt, "warmup", 0, t, Matrices.checksum(r), kb, jb, jv, os);
            }
        }

        for (int rep = 1; rep <= reps; rep++) {
            double[][] a = Matrices.random(n, Matrices.seedA(n, rep));
            double[][] b = Matrices.random(n, Matrices.seedB(n, rep));
            int offset = (rep - 1) % cfgs.size();
            double ref = Double.NaN;
            for (int idx = 0; idx < cfgs.size(); idx++) {
                Cfg c = cfgs.get((idx + offset) % cfgs.size());
                long t0 = System.nanoTime();
                double[][] r = run(c, a, b, n, tpt, kb, jb);
                double t = (System.nanoTime() - t0) / 1e9;
                consume(r);                                         // outside the timed region
                double cs = Matrices.checksum(r);
                if (Double.isNaN(ref)) ref = cs;
                else if (Math.abs(cs - ref) > 1e-9 * Math.abs(ref)) {
                    System.err.println("ERROR: checksum mismatch between configurations (rep " + rep + ")");
                    System.exit(3);
                }
                record(outPath, c, n, tpt, "measured", rep, t, cs, kb, jb, jv, os);
                System.out.println("n=" + n + " " + (c.blocked ? "blocked" : "ikj") + " "
                                   + (c.threads == 0 ? "seq" : "pool" + c.threads) + " rep=" + rep
                                   + " t=" + String.format(Locale.ROOT, "%.6f", t) + " s");
            }
        }
    }

    private static double[][] run(Cfg c, double[][] a, double[][] b, int n, int tpt, int kb, int jb)
            throws Exception {
        if (c.threads == 0) return sequential(a, b, n, c.blocked, kb, jb);
        return chunked(a, b, n, c.threads, Math.min(n, tpt * c.threads), c.blocked, kb, jb);
    }

    private static void record(Path p, Cfg c, int n, int tpt, String phase, int rep, double t,
                               double checksum, int kb, int jb, String jv, String os) throws Exception {
        String line = String.join(",", Instant.now().toString(),
            c.threads == 0 ? "Sequential" : "ThreadPoolChunked", c.blocked ? "blocked" : "ikj",
            String.valueOf(n), String.valueOf(c.threads == 0 ? 1 : c.threads),
            String.valueOf(c.threads == 0 ? 1 : Math.min(n, tpt * c.threads)), phase, String.valueOf(rep),
            String.format(Locale.ROOT, "%.9f", t), String.format(Locale.ROOT, "%.6f", checksum),
            c.blocked ? String.valueOf(kb) : "", c.blocked ? String.valueOf(jb) : "", jv, os);
        Files.write(p, (line + "\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
