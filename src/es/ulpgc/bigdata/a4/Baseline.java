package es.ulpgc.bigdata.a4;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Sequential and shared-memory baselines (from Assignment 3) re-measured on the SAME machine,
 * JDK and inputs (seeds 1000n+rep / 2000n+rep) as the Spark experiments.
 * Protocol as in A3: warm-up runs, measured repetitions, configuration order rotated per repetition.
 * Timer: output allocation + multiplication (+ executor creation/termination for parallel).
 * Flags: --n [--warmup 3] [--reps 10] [--threads 1,2,4,8,12,16] [--out file.csv]
 */
public final class Baseline {
    private static final String HEADER =
        "timestamp_utc,method,n,threads,phase,rep,time_seconds,checksum,java_version,os";

    private static Map<String, String> parse(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) m.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        return m;
    }

    private static boolean correctnessChecks() throws InterruptedException {
        int n = 50;
        double[][] a = Matrices.random(n, 42L);
        double[][] b = Matrices.random(n, 84L);
        boolean ok = A3Kernels.matricesEqual(A3Kernels.multiplySequential(a, b, n),
                                             A3Kernels.multiplyParallel(a, b, n, 2), n, 1e-9, 1e-9);
        double[][] id = Matrices.identity(n);
        double[][] z = Matrices.zeros(n);
        ok &= A3Kernels.matricesEqual(a, A3Kernels.multiplyParallel(a, id, n, 2), n, 1e-9, 1e-9);
        ok &= A3Kernels.matricesEqual(z, A3Kernels.multiplyParallel(a, z, n, 2), n, 1e-9, 1e-9);
        ok &= A3Kernels.matricesEqual(a, A3Kernels.multiplySequential(a, id, n), n, 1e-9, 1e-9);
        return ok;
    }

    private static String f9(double v) { return String.format(Locale.ROOT, "%.9f", v); }
    private static String f6(double v) { return String.format(Locale.ROOT, "%.6f", v); }

    public static void main(String[] args) throws Exception {
        Map<String, String> o = parse(args);
        int n = Integer.parseInt(o.get("n"));
        int warmup = Integer.parseInt(o.getOrDefault("warmup", "3"));
        int reps = Integer.parseInt(o.getOrDefault("reps", "10"));
        String[] tl = o.getOrDefault("threads", "1,2,4,8,12,16").split(",");
        String out = o.getOrDefault("out", "results/raw_baseline.csv");

        // configuration 0 = sequential, otherwise number of threads
        int[] configs = new int[tl.length + 1];
        for (int i = 0; i < tl.length; i++) configs[i + 1] = Integer.parseInt(tl[i].trim());

        if (!correctnessChecks()) {
            System.err.println("ERROR: correctness checks failed");
            System.exit(1);
        }
        System.out.println("Correctness checks passed. availableProcessors="
            + Runtime.getRuntime().availableProcessors() + " java=" + System.getProperty("java.version"));

        Path outPath = Paths.get(out);
        if (outPath.getParent() != null) Files.createDirectories(outPath.getParent());
        if (!Files.exists(outPath) || Files.size(outPath) == 0) {
            Files.write(outPath, (HEADER + "\n").getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        String jv = System.getProperty("java.version");
        String os = System.getProperty("os.name");

        // Warm-up (same inputs as A3: seeds 1 and 2), recorded with phase=warmup
        double[][] aw = Matrices.random(n, 1L);
        double[][] bw = Matrices.random(n, 2L);
        for (int cfg : configs) {
            for (int w = 0; w < warmup; w++) {
                long t0 = System.nanoTime();
                double[][] c = (cfg == 0) ? A3Kernels.multiplySequential(aw, bw, n)
                                          : A3Kernels.multiplyParallel(aw, bw, n, cfg);
                double t = (System.nanoTime() - t0) / 1e9;
                A3Kernels.consumeResult(c, n);
                record(outPath, cfg == 0 ? "Sequential" : "Parallel", n, cfg == 0 ? 1 : cfg,
                       "warmup", 0, t, Matrices.checksum(c), jv, os);
            }
        }

        for (int rep = 1; rep <= reps; rep++) {
            double[][] a = Matrices.random(n, Matrices.seedA(n, rep));
            double[][] b = Matrices.random(n, Matrices.seedB(n, rep));
            int offset = (rep - 1) % configs.length;
            double refChecksum = Double.NaN;
            for (int idx = 0; idx < configs.length; idx++) {
                int cfg = configs[(idx + offset) % configs.length];
                long t0 = System.nanoTime();
                double[][] c = (cfg == 0) ? A3Kernels.multiplySequential(a, b, n)
                                          : A3Kernels.multiplyParallel(a, b, n, cfg);
                double t = (System.nanoTime() - t0) / 1e9;
                A3Kernels.consumeResult(c, n);                 // outside the timed region
                double cs = Matrices.checksum(c);
                if (Double.isNaN(refChecksum)) refChecksum = cs;
                else if (Math.abs(cs - refChecksum) > 1e-9 * Math.abs(refChecksum)) {
                    System.err.println("ERROR: checksum mismatch between configurations (rep " + rep + ")");
                    System.exit(3);
                }
                record(outPath, cfg == 0 ? "Sequential" : "Parallel", n, cfg == 0 ? 1 : cfg,
                       "measured", rep, t, cs, jv, os);
                System.out.println("n=" + n + " " + (cfg == 0 ? "seq" : "par" + cfg) + " rep=" + rep
                                   + " t=" + f6(t) + " s");
            }
        }
    }

    private static void record(Path p, String method, int n, int threads, String phase, int rep,
                               double t, double checksum, String jv, String os) throws Exception {
        String line = String.join(",", Instant.now().toString(), method, String.valueOf(n),
                                  String.valueOf(threads), phase, String.valueOf(rep),
                                  f9(t), f6(checksum), jv, os);
        Files.write(p, (line + "\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
