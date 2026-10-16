"""Over drift_run.py's pickles: every 13-week window's net rise / entries and monotonicity, for
established overdue PMs and LDAR outstanding, the daily overdue share, and LDAR aged outstanding
with planted drain defects. The margins quoted above 03d's NET_RISE_MAX_SHARE, OVERDUE_SHARE_MAX
and AGED_OUTSTANDING_MAX come from this output.

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

# ---- LDAR aged outstanding: the share of leaks detected 30-91 days before the horizon that are
# ---- still unrepaired at it. The healthy null at every daily horizon, then three drain defects
# ---- planted at day 180 of each seed. The margins above 03d's AGED_OUTSTANDING_MAX come from here.
if "leak_det_ns" not in runs[0]:
    print("== LDAR aged outstanding: these pickles predate per-leak instants; rerun drift_run.py")
    sys.exit(0)
AGE, WIN, D0 = 30, 91, 180
DNS = 86400 * 10 ** 9


def aged_series(det, rep, h0, n_days, first):
    """[(day index, share, n)] for horizons at the end of each day from `first`."""
    out = []
    for j in range(first, n_days):
        h = h0 + (j + 1) * DNS
        m = (det >= h - WIN * DNS) & (det < h - AGE * DNS)
        k = int(m.sum())
        out.append((j, (rep[m] >= h).sum() / k if k else np.nan, k))
    return out


def defects(det, rep, h0):
    cut = h0 + D0 * DNS
    new = det >= cut
    never = np.int64(2 ** 62)
    return {"no repairs": np.where(new, never, rep),
            "half never repaired": np.where(new & (np.arange(len(det)) % 2 == 0), never, rep),
            "repairs 3x slower": np.where(new, det + 3 * (rep - det), rep)}


print(f"== LDAR aged outstanding: leaks detected {AGE}-{WIN} d before the horizon, share still "
      "unrepaired at it")
first = 45 + 14 - 1                     # 03d asserts from BACKLOG_CHECK_MIN_DAYS
healthy, late, dfx = [], [], {}
for r in runs:
    h0 = int(r["days"][0].value)
    det, rep = r["leak_det_ns"], r["leak_rep_ns"]
    s = aged_series(det, rep, h0, len(r["days"]), first)
    v = np.array([x[1] for x in s])
    healthy += list(v)
    late += [x[1] for x in s if x[0] >= 120]
    print(f"  seed {r['seed']:>3}  leaks {len(det):>5}  per window {np.mean([x[2] for x in s]):5.0f}  "
          f"share mean {np.nanmean(v):.1%}  p99 {np.nanpercentile(v, 99):.1%}  max {np.nanmax(v):.1%}")
    for lbl, rep_d in defects(det, rep, h0).items():
        sd = aged_series(det, rep_d, h0, len(r["days"]), D0)
        dfx.setdefault(lbl, []).append(sd)
a = np.array(healthy)
b = np.array(late)
print(f"  HEALTHY, every checked horizon, 8 seeds pooled ({len(a):,}): mean {np.nanmean(a):.1%}  "
      f"sd {np.nanstd(a):.1%}  p99 {np.nanpercentile(a, 99):.1%}  p99.9 {np.nanpercentile(a, 99.9):.1%}  "
      f"MAX {np.nanmax(a):.1%}")
print(f"  healthy after day 120 (shutdown tail filled): mean {np.nanmean(b):.1%}  max {np.nanmax(b):.1%}")
print(f"  defects planted at day {D0}; once the window is wholly after it (day {D0 + WIN}+):")
for lbl, per_seed in dfx.items():
    full = np.array([x[1] for sd in per_seed for x in sd if x[0] >= D0 + WIN])
    print(f"    {lbl:<22} min {np.nanmin(full):.1%}  p1 {np.nanpercentile(full, 1):.1%}  mean {np.nanmean(full):.1%}")
for thr in (0.15, 0.20, 0.25):
    lat = {lbl: [next((x[0] - D0 for x in sd if x[1] > thr), None) for sd in per_seed]
           for lbl, per_seed in dfx.items()}
    fp = int((a > thr).sum())
    print(f"  threshold {thr:.0%}: healthy horizons over it {fp} of {len(a):,} (max {np.nanmax(a):.1%} = "
          f"{thr / np.nanmax(a):.1f}x under); days from defect to first firing, per seed: "
          + "; ".join(f"{k} {v}" for k, v in lat.items()))
