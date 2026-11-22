package es.ulpgc.bigdata.a4;

import java.util.Random;

/** Pure-Java helpers (no Spark): input generation, sequential kernel, checks. */
public final class Matrices {
    private Matrices() {}

    /** Same seed convention as Assignment 3. */
    public static long seedA(int n, int rep) { return 1000L * n + rep; }
    public static long seedB(int n, int rep) { return 2000L * n + rep; }

    /**
     * Uniform [0,1) values from java.util.Random, filled row by row.
     * NOTE: keep identical to the generator used in Assignment 3 (MatrixOperations.java)
     * so that the logical input matrices are the same.
     */
    public static double[][] random(int n, long seed) {
        Random r = new Random(seed);
        double[][] m = new double[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                m[i][j] = r.nextDouble();
            }
        }
        return m;
    }

    public static double[][] identity(int n) {
        double[][] m = new double[n][n];
        for (int i = 0; i < n; i++) m[i][i] = 1.0;
        return m;
    }

    public static double[][] zeros(int n) { return new double[n][n]; }

    /**
     * i-k-j kernel over a block of rows of A. cRows must be zero-initialised and
     * have the same number of rows as aRows; b is n x n.
     */
    public static void multiplyRows(double[][] aRows, double[][] b, double[][] cRows) {
        int n = b.length;
        for (int i = 0; i < aRows.length; i++) {
            double[] ai = aRows[i];
            double[] ci = cRows[i];
            for (int k = 0; k < n; k++) {
                double aik = ai[k];
                double[] bk = b[k];
                for (int j = 0; j < n; j++) {
                    ci[j] += aik * bk[j];
                }
            }
        }
    }


    public static final int DEFAULT_KB = 64;    // rows of B per tile
    public static final int DEFAULT_JB = 256;   // columns of B per tile (tile = 64 x 256 doubles = 128 KiB)

    /**
     * Cache-blocked version of the i-k-j kernel (control experiment). B is processed in tiles of
     * kb x jb that are reused for every row of the block. For every C[i][j] the contributions are
     * still added in increasing k, so the result is bit-identical to multiplyRows.
     */
    public static void multiplyRowsBlocked(double[][] aRows, double[][] b, double[][] cRows, int kb, int jb) {
        int n = b.length;
        for (int kk = 0; kk < n; kk += kb) {
            int kEnd = Math.min(kk + kb, n);
            for (int jj = 0; jj < n; jj += jb) {
                int jEnd = Math.min(jj + jb, n);
                for (int i = 0; i < aRows.length; i++) {
                    double[] ai = aRows[i];
                    double[] ci = cRows[i];
                    for (int k = kk; k < kEnd; k++) {
                        double aik = ai[k];
                        double[] bk = b[k];
                        for (int j = jj; j < jEnd; j++) {
                            ci[j] += aik * bk[j];
                        }
                    }
                }
            }
        }
    }

    public static void multiplyRowsWith(boolean blocked, int kb, int jb,
                                        double[][] aRows, double[][] b, double[][] cRows) {
        if (blocked) multiplyRowsBlocked(aRows, b, cRows, kb, jb);
        else multiplyRows(aRows, b, cRows);
    }

    public static double[][] multiplySequential(double[][] a, double[][] b) {
        int n = b.length;
        double[][] c = new double[a.length][n];
        multiplyRows(a, b, c);
        return c;
    }

    public static double maxAbsDiff(double[][] x, double[][] y) {
        double max = 0.0;
        for (int i = 0; i < x.length; i++) {
            for (int j = 0; j < x[i].length; j++) {
                double d = Math.abs(x[i][j] - y[i][j]);
                if (d > max || Double.isNaN(d)) max = d;
            }
        }
        return max;
    }

    /** Sum of all elements; consumed so the computation cannot be discarded. */
    public static double checksum(double[][] m) {
        double s = 0.0;
        for (double[] row : m) for (double v : row) s += v;
        return s;
    }

    /** Row boundaries for splitting n rows in at most 'blocks' contiguous blocks. */
    public static int[] splitBounds(int n, int blocks) {
        int b = Math.max(1, Math.min(blocks, n));
        int base = n / b;
        int rem = n % b;
        int[] starts = new int[b + 1];
        for (int i = 0; i < b; i++) {
            starts[i + 1] = starts[i] + base + (i < rem ? 1 : 0);
        }
        return starts;
    }
}
