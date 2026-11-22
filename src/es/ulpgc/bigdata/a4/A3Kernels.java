package es.ulpgc.bigdata.a4;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Sequential and parallel kernels REUSED from Assignment 3 (MatrixOperations.java).
 * The algorithms are unchanged (i-k-j loops, fixed thread pool, one task per output row);
 * only the package and the removal of unused helpers differ.
 */
public final class A3Kernels {
    private A3Kernels() {}

    private static volatile double resultSink = 0.0;

    // Sequential matrix multiplication (A3)
    public static double[][] multiplySequential(double[][] A, double[][] B, int n) {
        double[][] C = new double[n][n];
        for (int i = 0; i < n; i++) {
            for (int k = 0; k < n; k++) {
                double a = A[i][k];
                for (int j = 0; j < n; j++) {
                    C[i][j] += a * B[k][j];
                }
            }
        }
        return C;
    }

    // Parallel matrix multiplication using row distribution (A3)
    public static double[][] multiplyParallel(double[][] A, double[][] B, int n, int numThreads)
            throws InterruptedException {
        double[][] C = new double[n][n];
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        Future<?>[] futures = new Future<?>[n];
        try {
            for (int i = 0; i < n; i++) {
                final int row = i;
                futures[i] = executor.submit(() -> {
                    for (int k = 0; k < n; k++) {
                        double a = A[row][k];
                        for (int j = 0; j < n; j++) {
                            C[row][j] += a * B[k][j];
                        }
                    }
                });
            }
            executor.shutdown();
            for (Future<?> future : futures) {
                future.get();
            }
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                throw new InterruptedException("Parallel execution timed out.");
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            executor.shutdownNow();
            throw new RuntimeException("A parallel task failed.", e);
        }
        return C;
    }

    // Consume the result outside the timed region (A3)
    public static void consumeResult(double[][] C, int n) {
        double checksum = 0.0;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                checksum += C[i][j];
            }
        }
        resultSink = checksum;
    }

    // Numerical comparison with absolute and relative tolerance (A3)
    public static boolean matricesEqual(double[][] A, double[][] B, int n, double absTol, double relTol) {
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                double a = A[i][j];
                double b = B[i][j];
                if (!Double.isFinite(a) || !Double.isFinite(b)) return false;
                if (Math.abs(a - b) > absTol + relTol * Math.abs(a)) return false;
            }
        }
        return true;
    }
}
