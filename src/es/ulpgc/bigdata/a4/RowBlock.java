package es.ulpgc.bigdata.a4;

import java.io.Serializable;

/** A contiguous block of rows of a matrix: rows [start, start + rows.length). */
public final class RowBlock implements Serializable {
    private static final long serialVersionUID = 1L;
    public final int start;
    public final double[][] rows;

    public RowBlock(int start, double[][] rows) {
        this.start = start;
        this.rows = rows;
    }
}
