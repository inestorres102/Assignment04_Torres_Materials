#!/usr/bin/env python3
"""
Assignment 4 - analysis of the CONTROL experiment (unblocked i-k-j kernel vs cache-blocked kernel).

Question: is the plateau of the Spark scaling (and the growth of the in-task kernel time with the number
of executors) caused by memory traffic of the unblocked i-k-j kernel?

Decision criteria fixed BEFORE running the experiment (n = 4000, p = 8 workers):
  H supported : kernel inflation of the blocked kernel (kernel_sum(8)/kernel_sum(1)) is clearly smaller than
                that of the i-k-j kernel (<= 1.5x vs about 4x) AND the self-speedup T(1)/T(8) of Spark with
                the blocked kernel is clearly larger than with i-k-j (about 1.8x).
  H rejected  : the blocked kernel shows similar inflation and a similar plateau.
  Inconclusive: anything in between; report it as such.

Inputs  (results/): raw_control_spark.csv, raw_control_threads.csv, raw_baseline.csv (reference only)
Outputs (results/): control_summary.csv, control_table.csv ; (figures/): fig7_control_time.png, fig8_control_scaling.png
"""
import pathlib
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


def q1(x):
    return x.quantile(0.25)


def q3(x):
    return x.quantile(0.75)


def log2_axis(ax):
    ax.set_xscale("log", base=2)
    ax.set_xticks(P_LIST)
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.minorticks_off()


def main():
    sp = pd.read_csv(RES / "raw_control_spark.csv")
    th = pd.read_csv(RES / "raw_control_threads.csv")
    base = pd.read_csv(RES / "raw_baseline.csv")
    sp = sp[sp.phase == "measured"]
    th = th[th.phase == "measured"]
    base = base[base.phase == "measured"]

    # ---- summaries
    s = sp.groupby(["n", "kernel", "workers_expected"]).agg(
        reps=("rep", "count"), e2e=("t_e2e_s", "median"), q1=("t_e2e_s", q1), q3=("t_e2e_s", q3),
        job=("t_job_s", "median"), ksum=("kernel_sum_s", "median")).reset_index().rename(
        columns={"workers_expected": "p"})
    s["platform"] = "Spark"
    t = th.groupby(["n", "method", "kernel", "threads"]).agg(
        reps=("rep", "count"), e2e=("time_seconds", "median"), q1=("time_seconds", q1),
        q3=("time_seconds", q3)).reset_index().rename(columns={"threads": "p"})
    t["platform"] = np.where(t.method == "Sequential", "Sequential", "ThreadPoolChunked")
    summary = pd.concat([s.drop(columns=["job", "ksum"]), t.drop(columns=["method"])], ignore_index=True)
    summary.to_csv(RES / "control_summary.csv", index=False, float_format="%.6f")

    bad = summary[summary.reps != 10]
    if len(bad):
        print("WARNING: configurations without 10 measured repetitions:\n", bad[["n", "platform", "kernel", "p", "reps"]])

    # checksum cross-check among control files and the main baseline
    cs = pd.concat([sp[["n", "rep", "checksum"]], th[["n", "rep", "checksum"]], base[["n", "rep", "checksum"]]])
    g = cs.groupby(["n", "rep"]).checksum.agg(["min", "max"])
    rel = ((g["max"] - g["min"]).abs() / g["min"].abs()).max()
    print(f"Checksum cross-check (control + main baseline, {len(g)} (n, rep) pairs): max relative spread {rel:.3e} ->",
          "PASS" if rel <= 1e-9 else "FAIL")

    # ---- wide table
    seq_blocked = t[(t.platform == "Sequential") & (t.kernel == "blocked")].set_index("n").e2e
    seq_ikj_main = base[base.method == "Sequential"].groupby("n").time_seconds.median()
    rows = []
    for n in sorted(s.n.unique()):
        for p in P_LIST:
            r = {"n": n, "p": p}
            for k in ("ikj", "blocked"):
                d = s[(s.n == n) & (s.kernel == k) & (s.p == p)]
                if len(d):
                    r[f"Tspark_{k}"] = d.e2e.iloc[0]
                    r[f"ksum_{k}"] = d.ksum.iloc[0]
            d = t[(t.n == n) & (t.platform == "ThreadPoolChunked") & (t.kernel == "blocked") & (t.p == p)]
            if len(d):
                r["Tpool_blocked"] = d.e2e.iloc[0]
            r["Tseq_blocked"] = seq_blocked.get(n, np.nan)
            r["Tseq_ikj_main"] = seq_ikj_main.get(n, np.nan)
            rows.append(r)
    w = pd.DataFrame(rows)
    for k in ("ikj", "blocked"):
        w[f"inflation_{k}"] = w[f"ksum_{k}"] / w.n.map(w[w.p == 1].set_index("n")[f"ksum_{k}"])
        w[f"selfS_spark_{k}"] = w.n.map(w[w.p == 1].set_index("n")[f"Tspark_{k}"]) / w[f"Tspark_{k}"]
    w["selfS_pool_blocked"] = w.n.map(w[w.p == 1].set_index("n")["Tpool_blocked"]) / w["Tpool_blocked"]
    w["blocked_vs_ikj_spark_time_ratio"] = w.Tspark_blocked / w.Tspark_ikj
    w["spark_vs_pool_time_ratio_blocked"] = w.Tspark_blocked / w.Tpool_blocked
    w.to_csv(RES / "control_table.csv", index=False, float_format="%.6f")

    pd.set_option("display.width", 230)
    pd.set_option("display.max_columns", 30)
    cols = ["n", "p", "Tspark_ikj", "Tspark_blocked", "Tpool_blocked", "inflation_ikj", "inflation_blocked",
            "selfS_spark_ikj", "selfS_spark_blocked", "selfS_pool_blocked", "spark_vs_pool_time_ratio_blocked"]
    print(w[cols].round(3).to_string(index=False))

    # ---- figure 7: time vs p
    sizes = sorted(s.n.unique())
    fig, axes = plt.subplots(1, len(sizes), figsize=(7 * len(sizes), 5), squeeze=False)
    for ax, n in zip(axes[0], sizes):
        def line(df, label, color, marker, ls="-"):
            if len(df) == 0:
                return
            df = df.sort_values("p")
            ax.errorbar(df.p, df.e2e, yerr=[df.e2e - df.q1, df.q3 - df.e2e], marker=marker, color=color,
                        ls=ls, capsize=3, label=label)
        line(s[(s.n == n) & (s.kernel == "ikj")], "Spark, i-k-j kernel", "tab:red", "o")
        line(s[(s.n == n) & (s.kernel == "blocked")], "Spark, blocked kernel", "tab:orange", "o", "--")
        line(t[(t.n == n) & (t.platform == "ThreadPoolChunked") & (t.kernel == "blocked")],
             "Thread pool (same kernel and tasks as Spark), blocked", "tab:green", "s")
        if n in seq_blocked.index:
            ax.axhline(seq_blocked[n], color="tab:green", ls=":", label="Sequential, blocked kernel")
        if n in seq_ikj_main.index:
            ax.axhline(seq_ikj_main[n], color="gray", ls=":", label="Sequential, i-k-j (main baseline)")
        ax.set_yscale("log")
        log2_axis(ax)
        ax.set_xlabel("Parallelism p (threads or Spark workers)")
        ax.set_title(f"n = {n}")
        ax.grid(True, which="both", alpha=0.3)
    axes[0][0].set_ylabel("Time (s), median with IQR")
    axes[0][0].legend(fontsize=8)
    fig.suptitle("Control experiment: only the multiplication kernel changes (10 repetitions)")
    fig.tight_layout()
    fig.savefig(FIG / "fig7_control_time.png", dpi=200)
    plt.close(fig)

    # ---- figure 8: self speedup and kernel inflation
    fig, axes = plt.subplots(2, len(sizes), figsize=(7 * len(sizes), 9), squeeze=False)
    for j, n in enumerate(sizes):
        d = w[w.n == n]
        ax = axes[0][j]
        for col, lab, c, m in [("selfS_spark_ikj", "Spark, i-k-j", "tab:red", "o"),
                               ("selfS_spark_blocked", "Spark, blocked", "tab:orange", "o"),
                               ("selfS_pool_blocked", "Thread pool, blocked", "tab:green", "s")]:
            dd = d.dropna(subset=[col])
            ax.plot(dd.p, dd[col], marker=m, color=c, label=lab)
        ax.plot(P_LIST, P_LIST, "k--", lw=1, label="Ideal S = p")
        ax.set_yscale("log")
        log2_axis(ax)
        ax.set_title(f"n = {n}: self-speedup T(1) / T(p)")
        ax.set_ylabel("Speedup relative to p = 1 of the same platform and kernel")
        ax.grid(True, which="both", alpha=0.3)
        ax.legend(fontsize=8)
        ax = axes[1][j]
        for col, lab, c in [("inflation_ikj", "Spark, i-k-j", "tab:red"), ("inflation_blocked", "Spark, blocked", "tab:orange")]:
            dd = d.dropna(subset=[col])
            ax.plot(dd.p, dd[col], marker="o", color=c, label=lab)
        ax.axhline(1, color="k", ls="--", lw=1, label="No slowdown of the kernel")
        ax.set_yscale("log")
        log2_axis(ax)
        ax.set_xlabel("Spark workers p")
        ax.set_ylabel("Total in-task kernel time / value with 1 worker")
        ax.set_title(f"n = {n}: kernel time inflation")
        ax.grid(True, which="both", alpha=0.3)
        ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(FIG / "fig8_control_scaling.png", dpi=200)
    plt.close(fig)
    print("Figures written to", FIG)


if __name__ == "__main__":
    sys.exit(main())
