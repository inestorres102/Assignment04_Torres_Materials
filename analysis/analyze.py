#!/usr/bin/env python3
"""
Assignment 4 - analysis of the Spark (strategy A) and baseline measurements.

Inputs  (results/): raw_spark.csv, raw_baseline.csv, config_status.csv, baseline_status.csv
Outputs (results/): summary_spark.csv, summary_baseline.csv, scaling_table.csv, memory_table.csv,
                    crosscheck.txt, analysis_environment.txt
        (figures/): fig1..fig6 (PNG)

Statistics: median and interquartile range (IQR) over the measured repetitions; warm-up rows are
excluded (they stay in the raw files). Speedup S(p) = Tseq / Tp and efficiency E(p) = S(p) / p use the
medians. 'p' is the number of workers (Spark, 1 core each) or threads (thread pool).
"""
import pathlib
import platform
import sys

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parent.parent
RES = ROOT / "results"
FIG = ROOT / "figures"
FIG.mkdir(exist_ok=True)

P_LIST = [1, 2, 4, 8, 12, 16]
EXPECTED_REPS = 10
COLORS = {250: "#1f77b4", 500: "#ff7f0e", 750: "#2ca02c", 1000: "#d62728", 2000: "#9467bd", 4000: "#8c564b"}


def q1(x):
    return x.quantile(0.25)


def q3(x):
    return x.quantile(0.75)


def load():
    spark = pd.read_csv(RES / "raw_spark.csv")
    base = pd.read_csv(RES / "raw_baseline.csv")
    cfg = pd.read_csv(RES / "config_status.csv")
    bst = pd.read_csv(RES / "baseline_status.csv")
    return spark, base, cfg, bst


def check_counts(df, keys, label):
    c = df.groupby(keys).size()
    bad = c[c != EXPECTED_REPS]
    if len(bad):
        print(f"WARNING ({label}): configurations without {EXPECTED_REPS} measured repetitions:\n{bad}")


def summarise(spark, base):
    sm = spark[spark.phase == "measured"].copy()
    bm = base[base.phase == "measured"].copy()
    check_counts(sm, ["n", "workers_expected"], "spark")
    check_counts(bm, ["n", "method", "threads"], "baseline")

    s = sm.groupby(["n", "workers_expected"]).agg(
        reps=("rep", "count"),
        e2e_median_s=("t_e2e_s", "median"), e2e_q1_s=("t_e2e_s", q1), e2e_q3_s=("t_e2e_s", q3),
        bcast_median_s=("t_bcast_create_s", "median"), job_median_s=("t_job_s", "median"),
        kernel_sum_median_s=("kernel_sum_s", "median"),
    ).reset_index().rename(columns={"workers_expected": "workers"})
    s["e2e_iqr_s"] = s.e2e_q3_s - s.e2e_q1_s

    b = bm.groupby(["n", "method", "threads"]).agg(
        reps=("rep", "count"),
        median_s=("time_seconds", "median"), q1_s=("time_seconds", q1), q3_s=("time_seconds", q3),
    ).reset_index()
    b["iqr_s"] = b.q3_s - b.q1_s
    return sm, bm, s, b


def scaling_table(sm, bm, s, b):
    seq = b[b.method == "Sequential"].set_index("n")
    par = b[b.method == "Parallel"].set_index(["n", "threads"])
    sp = s.set_index(["n", "workers"])
    seq_rep = bm[bm.method == "Sequential"].set_index(["n", "rep"]).time_seconds
    sm_idx = sm.set_index(["n", "workers_expected", "rep"]).t_e2e_s

    rows = []
    for n in sorted(s.n.unique()):
        tseq = seq.loc[n, "median_s"]
        for p in P_LIST:
            r = {"n": n, "p": p, "Tseq_s": tseq}
            if (n, p) in par.index:
                r["Tthreads_s"] = par.loc[(n, p), "median_s"]
            if (n, p) in sp.index:
                r["Tspark_s"] = sp.loc[(n, p), "e2e_median_s"]
                r["Tspark_iqr_s"] = sp.loc[(n, p), "e2e_iqr_s"]
                # paired speedup per repetition (same input matrices in each repetition)
                ratios = [seq_rep.loc[(n, rep)] / sm_idx.loc[(n, p, rep)] for rep in range(1, EXPECTED_REPS + 1)
                          if (n, p, rep) in sm_idx.index]
                r["S_spark_paired_median"] = float(np.median(ratios))
                r["S_spark_paired_iqr"] = float(np.quantile(ratios, .75) - np.quantile(ratios, .25))
            rows.append(r)
    t = pd.DataFrame(rows)
    t["S_threads"] = t.Tseq_s / t.Tthreads_s
    t["E_threads"] = t.S_threads / t.p
    t["S_spark"] = t.Tseq_s / t.Tspark_s
    t["E_spark"] = t.S_spark / t.p
    t["Spark_vs_threads_time_ratio"] = t.Tspark_s / t.Tthreads_s     # >1: Spark slower than the thread pool
    # self-relative strong scaling of Spark (1 worker as reference)
    t1 = t[t.p == 1].set_index("n").Tspark_s
    t["S_spark_self"] = t.n.map(t1) / t.Tspark_s
    t["E_spark_self"] = t.S_spark_self / t.p
    # derived quantities
    ks = s.set_index(["n", "workers"]).kernel_sum_median_s
    jb = s.set_index(["n", "workers"]).job_median_s
    bc = s.set_index(["n", "workers"]).bcast_median_s
    t["bcast_s"] = [bc.get((n, p), np.nan) for n, p in zip(t.n, t.p)]
    t["job_s"] = [jb.get((n, p), np.nan) for n, p in zip(t.n, t.p)]
    t["kernel_sum_s"] = [ks.get((n, p), np.nan) for n, p in zip(t.n, t.p)]
    t["kernel_mean_per_executor_s"] = t.kernel_sum_s / t.p
    t["kernel_inflation_vs_1w"] = [ks.get((n, p), np.nan) / ks.get((n, 1), np.nan) for n, p in zip(t.n, t.p)]
    # indicative B-read rate if B were never served from cache: n rows x 8n^2 bytes = 8 n^3 bytes
    # Only meaningful when B (8 n^2 bytes) clearly exceeds the last-level cache, i.e. n >= 2000; NaN below.
    big = t.n >= 2000
    t["Bread_rate_spark_GBps"] = (8.0 * t.n ** 3 / 1e9 / t.Tspark_s).where(big)
    t["Bread_rate_threads_GBps"] = (8.0 * t.n ** 3 / 1e9 / t.Tthreads_s).where(big)
    return t


def memory_table(cfg, bst):
    m = cfg.copy()
    m["dense_storage_mib"] = 3 * 8 * m.n.astype(float) ** 2 / 1024 ** 2
    m = m.merge(bst[["n", "peak_working_set_mb"]].rename(columns={"peak_working_set_mb": "baseline_peak_ws_mb"}),
                on="n", how="left")
    return m


def crosscheck(sm, bm):
    a = sm.groupby(["n", "rep"]).checksum.agg(["min", "max"])
    b = bm.groupby(["n", "rep"]).checksum.agg(["min", "max"])
    j = a.join(b, lsuffix="_spark", rsuffix="_base")
    rel = lambda x, y: (x - y).abs() / y.abs()
    out = [
        "Checksum cross-check (sum of all elements of C, measured repetitions)",
        f"(n, rep) pairs compared: {len(j)}",
        f"max relative spread across Spark configurations : {rel(j.max_spark, j.min_spark).max():.3e}",
        f"max relative spread across baseline configurations: {rel(j.max_base, j.min_base).max():.3e}",
        f"max relative difference Spark vs baseline        : {rel(j.min_spark, j.min_base).max():.3e}",
    ]
    ok = max(rel(j.max_spark, j.min_spark).max(), rel(j.max_base, j.min_base).max(),
             rel(j.min_spark, j.min_base).max()) <= 1e-9
    out.append("RESULT: " + ("PASS (all configurations computed the same product)" if ok else "FAIL"))
    return "\n".join(out)


# ----------------------------------------------------------------------------- figures
def _log2_axis(ax):
    ax.set_xscale("log", base=2)
    ax.set_xticks(P_LIST)
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.minorticks_off()


def fig1_time(s, b):
    sizes = sorted(s.n.unique())
    fig, axes = plt.subplots(2, 3, figsize=(14, 8), sharex=True)
    for ax, n in zip(axes.ravel(), sizes):
        seq = b[(b.method == "Sequential") & (b.n == n)].iloc[0]
        ax.axhline(seq.median_s, color="gray", ls="--", label="Sequential (A3 kernel)")
        ax.axhspan(seq.q1_s, seq.q3_s, color="gray", alpha=0.15)
        pt = b[(b.method == "Parallel") & (b.n == n)].sort_values("threads")
        ax.errorbar(pt.threads, pt.median_s, yerr=[pt.median_s - pt.q1_s, pt.q3_s - pt.median_s],
                    marker="s", color="tab:green", capsize=3, label="Thread pool (A3), p threads")
        ss = s[s.n == n].sort_values("workers")
        ax.errorbar(ss.workers, ss.e2e_median_s, yerr=[ss.e2e_median_s - ss.e2e_q1_s, ss.e2e_q3_s - ss.e2e_median_s],
                    marker="o", color="tab:red", capsize=3, label="Spark standalone, p workers")
        ax.set_yscale("log")
        _log2_axis(ax)
        ax.set_title(f"n = {n}")
        ax.grid(True, which="both", alpha=0.3)
    for ax in axes[1]:
        ax.set_xlabel("Parallelism p (threads or Spark workers)")
    for ax in axes[:, 0]:
        ax.set_ylabel("Time (s), median with IQR")
    axes[0, 0].legend(fontsize=8)
    fig.suptitle("Execution time vs parallelism (10 repetitions; Spark: end-to-end time; n=4000 not run for p>8)")
    fig.tight_layout()
    fig.savefig(FIG / "fig1_time_vs_parallelism.png", dpi=200)
    plt.close(fig)


def fig2_speedup(t):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)
    for ax, (col, ttl) in zip(axes, [("S_spark", "Spark standalone (p workers, end-to-end)"),
                                     ("S_threads", "Thread pool, A3 (p threads)")]):
        for n in sorted(t.n.unique()):
            d = t[(t.n == n)].dropna(subset=[col])
            ax.plot(d.p, d[col], marker="o", color=COLORS[n], label=f"n = {n}")
        ax.plot(P_LIST, P_LIST, "k--", lw=1, label="Ideal S = p")
        ax.axhline(1, color="gray", lw=1)
        ax.set_yscale("log")
        _log2_axis(ax)
        ax.set_xlabel("Parallelism p")
        ax.set_title(ttl)
        ax.grid(True, which="both", alpha=0.3)
    axes[0].set_ylabel("Speedup vs sequential (median times)")
    axes[0].legend(fontsize=8)
    fig.suptitle("Speedup relative to the sequential i-k-j baseline (same machine, JDK 21)")
    fig.tight_layout()
    fig.savefig(FIG / "fig2_speedup.png", dpi=200)
    plt.close(fig)


def fig3_efficiency(t):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5), sharey=True)
    for ax, (col, ttl) in zip(axes, [("E_spark", "Spark standalone"), ("E_threads", "Thread pool (A3)")]):
        for n in sorted(t.n.unique()):
            d = t[(t.n == n)].dropna(subset=[col])
            ax.plot(d.p, d[col], marker="o", color=COLORS[n], label=f"n = {n}")
        ax.axhline(1, color="k", ls="--", lw=1, label="Ideal E = 1")
        ax.set_yscale("log")
        _log2_axis(ax)
        ax.set_xlabel("Parallelism p")
        ax.set_title(ttl)
        ax.grid(True, which="both", alpha=0.3)
    axes[0].set_ylabel("Efficiency E(p) = S(p) / p")
    axes[0].legend(fontsize=8)
    fig.suptitle("Parallel efficiency relative to the sequential baseline")
    fig.tight_layout()
    fig.savefig(FIG / "fig3_efficiency.png", dpi=200)
    plt.close(fig)


def fig4_breakdown(s, t):
    sizes = [500, 1000, 2000, 4000]
    fig, axes = plt.subplots(1, 4, figsize=(16, 4.8))
    for ax, n in zip(axes, sizes):
        d = s[s.n == n].sort_values("workers")
        x = np.arange(len(d))
        ax.bar(x, d.bcast_median_s, color="tab:orange", label="Broadcast creation (driver)")
        ax.bar(x, d.job_median_s, bottom=d.bcast_median_s, color="tab:blue",
               label="Job: split + tasks + collect + assembly")
        ax.plot(x, d.kernel_sum_median_s / d.workers, "k*", ms=9,
                label="Mean kernel time per executor (derived)")
        ax.set_xticks(x)
        ax.set_xticklabels(d.workers.astype(int))
        ax.set_xlabel("Workers")
        ax.set_title(f"n = {n}")
        ax.grid(True, axis="y", alpha=0.3)
    axes[0].set_ylabel("Time (s), medians")
    axes[0].legend(fontsize=7, loc="upper left")
    fig.suptitle("Spark end-to-end time = broadcast creation + job (consecutive intervals). "
                 "Kernel time is a derived estimate and is not part of the stack")
    fig.tight_layout()
    fig.savefig(FIG / "fig4_spark_breakdown.png", dpi=200)
    plt.close(fig)


def fig5_inflation(t):
    fig, ax = plt.subplots(figsize=(7, 5))
    for n in [500, 750, 1000, 2000, 4000]:
        d = t[t.n == n].dropna(subset=["kernel_inflation_vs_1w"])
        ax.plot(d.p, d.kernel_inflation_vs_1w, marker="o", color=COLORS[n], label=f"n = {n}")
    ax.axhline(1, color="k", ls="--", lw=1, label="No slowdown of the kernel")
    ax.set_yscale("log")
    _log2_axis(ax)
    ax.set_xlabel("Spark workers p")
    ax.set_ylabel("Total in-task kernel time / value with 1 worker")
    ax.set_title("Same total arithmetic, more concurrent executors: kernel time inflation")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(FIG / "fig5_kernel_inflation.png", dpi=200)
    plt.close(fig)


def fig6_memory(m):
    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    ok = m[m.status == "OK"]
    for n in [1000, 2000, 4000]:
        d = ok[ok.n == n].sort_values("workers")
        axes[0].plot(d.workers, d.peak_total_exec_ws_mb / 1024, marker="o", color=COLORS[n], label=f"n = {n}")
    axes[0].set_xlabel("Spark workers p")
    axes[0].set_ylabel("Peak working set, sum of executors (GiB)")
    axes[0].set_title("Executor memory vs number of workers")
    axes[0].set_xticks(P_LIST)
    axes[0].grid(True, alpha=0.3)
    axes[0].legend(fontsize=8)

    sizes = sorted(ok.n.unique())
    w1 = ok[ok.workers == 1].set_index("n")
    axes[1].plot(sizes, [3 * 8 * n * n / 1024 ** 2 for n in sizes], "k--", marker="x", label="Dense A, B, C (3 x 8 n^2 B)")
    axes[1].plot(sizes, [w1.loc[n, "baseline_peak_ws_mb"] for n in sizes], marker="s", color="tab:green",
                 label="Baseline JVM (thread-pool runs), peak working set")
    axes[1].plot(sizes, [w1.loc[n, "peak_driver_ws_mb"] for n in sizes], marker="o", color="tab:blue",
                 label="Spark driver, peak working set (1 worker)")
    axes[1].plot(sizes, [w1.loc[n, "peak_total_exec_ws_mb"] for n in sizes], marker="^", color="tab:red",
                 label="Spark executor, peak working set (1 worker)")
    axes[1].set_xscale("log")
    axes[1].set_yscale("log")
    axes[1].set_xticks(sizes)
    axes[1].get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    axes[1].minorticks_off()
    axes[1].set_xlabel("Matrix size n")
    axes[1].set_ylabel("Memory (MiB)")
    axes[1].set_title("Memory vs n")
    axes[1].grid(True, which="both", alpha=0.3)
    axes[1].legend(fontsize=8)
    fig.suptitle("Windows working set (comparable to RSS), sampled every 300 ms / polled every 200 ms")
    fig.tight_layout()
    fig.savefig(FIG / "fig6_memory.png", dpi=200)
    plt.close(fig)


def main():
    spark, base, cfg, bst = load()
    sm, bm, s, b = summarise(spark, base)
    t = scaling_table(sm, bm, s, b)
    m = memory_table(cfg, bst)

    s.to_csv(RES / "summary_spark.csv", index=False, float_format="%.6f")
    b.to_csv(RES / "summary_baseline.csv", index=False, float_format="%.6f")
    t.to_csv(RES / "scaling_table.csv", index=False, float_format="%.6f")
    m.to_csv(RES / "memory_table.csv", index=False, float_format="%.3f")
    cc = crosscheck(sm, bm)
    (RES / "crosscheck.txt").write_text(cc + "\n")
    (RES / "analysis_environment.txt").write_text(
        f"python {platform.python_version()}\npandas {pd.__version__}\nnumpy {np.__version__}\n"
        f"matplotlib {matplotlib.__version__}\n")

    fig1_time(s, b)
    fig2_speedup(t)
    fig3_efficiency(t)
    fig4_breakdown(s, t)
    fig5_inflation(t)
    fig6_memory(m)

    pd.set_option("display.width", 220)
    pd.set_option("display.max_columns", 30)
    print(cc, "\n")
    cols = ["n", "p", "Tseq_s", "Tthreads_s", "Tspark_s", "S_threads", "S_spark", "E_spark", "S_spark_self",
            "kernel_inflation_vs_1w", "Bread_rate_spark_GBps", "Bread_rate_threads_GBps"]
    print(t[cols].round(3).to_string(index=False))
    print("\nFigures written to", FIG)


if __name__ == "__main__":
    sys.exit(main())
