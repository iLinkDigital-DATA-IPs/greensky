"""Over drift_run.py's pickles: every 13-week window's net rise / entries and monotonicity, for
established overdue PMs and LDAR outstanding, and the daily overdue share. The margins quoted
above 03d's NET_RISE_MAX_SHARE and OVERDUE_SHARE_MAX come from this output.

    python thresh_analyse.py [DIR]      # DIR holds drift_*.pkl; default "."
"""
import glob, os, pickle, sys
import numpy as np

N = 91                                                       # 13 weeks
W = lambda d: np.array([d[7 * i:7 * i + 7].mean() for i in range(len(d) // 7)])   # noqa: E731
runs = [pickle.load(open(f, "rb")) for f in sorted(glob.glob(os.path.join(sys.argv[1] if len(sys.argv) > 1 else ".", "drift_*.pkl")))]
print(f"{len(runs)} runs, seeds {[r['seed'] for r in runs]}, {len(runs[0]['est'])} days each\n")


def windows(count, entries, first):
    out = []
    for s in range(first, len(count) - N + 1):
        net = int(count[s + N - 1]) - int(count[s])
        ent = int(entries[s + 1:s + N].sum())
        wk = W(count[s:s + N])
        out.append((net / max(ent, 1), net, ent, bool(np.all(np.diff(wk) > 0))))
    return out


for lbl, first in (("every window", 0), ("windows after Q1", 91)):
    allr = []
    print(f"== established overdue PMs, 13-week windows, {lbl}")
    for r in runs:
        w = windows(r["est"], r["entries"], first)
        q = np.array([x[0] for x in w])
        allr += list(q)
        print(f"  seed {r['seed']:>3}  entries/window {np.mean([x[2] for x in w]):6.0f}  net rise "
              f"min {min(x[1] for x in w):+4d} max {max(x[1] for x in w):+4d}  ratio max {q.max():+.3f}  "
              f"p99 {np.percentile(q, 99):+.3f}  monotonic windows {sum(x[3] for x in w)}")
    a = np.array(allr)
    print(f"  ALL: ratio max {a.max():+.3f}  p99 {np.percentile(a, 99):+.3f}  p99.9 {np.percentile(a, 99.9):+.3f}  "
          f"sd {a.std():.3f}; margin of 0.5 over max {0.5 / a.max():.1f}x, {(0.5 - a.max()) / a.std():.1f} sd\n")

print("== LDAR outstanding, 13-week windows after the 45-day warm-up")
allr = []
for r in runs:
    det, out = np.asarray(r["ldar_det"]), np.asarray(r["ldar_out"])
    ent = np.diff(det, prepend=0)
    w = windows(out, ent, 45)
    q = np.array([x[0] for x in w])
    allr += list(q)
    print(f"  seed {r['seed']:>3}  detected/window {np.mean([x[2] for x in w]):5.0f}  net min "
          f"{min(x[1] for x in w):+4d} max {max(x[1] for x in w):+4d}  ratio max {q.max():+.3f}  "
          f"monotonic {sum(x[3] for x in w)}")
a = np.array(allr)
print(f"  ALL: ratio max {a.max():+.3f}  p99 {np.percentile(a, 99):+.3f}; margin of 0.5 {0.5 / a.max():.1f}x\n")

print("== overdue share (daily)")
mx_e, mx_a = [], []
for r in runs:
    se = r["est"] / r["n_est"]
    sa = r["all"] / r["n_all"]
    mx_e.append(se.max()); mx_a.append(sa.max())
    print(f"  seed {r['seed']:>3}  established mean {se.mean():.2%} max {se.max():.2%}   "
          f"all-assets mean {sa.mean():.2%} max {sa.max():.2%}  last {sa[-1]:.2%}")
print(f"  max across seeds: established {max(mx_e):.2%}, all assets {max(mx_a):.2%}; "
      f"mean of seed means {np.mean([(r['est'] / r['n_est']).mean() for r in runs]):.2%}, "
      f"sd of seed means {np.std([(r['est'] / r['n_est']).mean() for r in runs]):.2%}")
