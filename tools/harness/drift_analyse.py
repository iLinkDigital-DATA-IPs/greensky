"""Over drift_run.py's pickles: whether the established overdue level drifts after the first
quarter, the lag-1 autocorrelation of weekly means, and the null distribution of the longest
run of weekly rises (empirical windows, AR(1) at the measured phi, iid). This is the evidence
that retired 03d's consecutive-rise check.

    python drift_analyse.py [DIR]       # DIR holds drift_*.pkl; default "."
"""
import glob, os, pickle, sys
import numpy as np

W = lambda d: np.array([d[7 * i:7 * i + 7].mean() for i in range(len(d) // 7)])   # noqa: E731


def longest(v):
    best = run = 0
    for a, b in zip(v[:-1], v[1:]):
        run = run + 1 if b > a else 0
        best = max(best, run)
    return best


runs = [pickle.load(open(f, "rb")) for f in sorted(glob.glob(os.path.join(sys.argv[1] if len(sys.argv) > 1 else ".", "drift_*.pkl")))]
print(f"{len(runs)} independent 540-day runs\n")

# ---- 1. drift: is the established level still climbing after the first months? -----------------
print("established weekly mean, by quarter (13 weeks) -- a drift keeps rising, noise does not")
slopes = []
for r in runs:
    w = W(r["est"])
    q = [w[13 * i:13 * i + 13].mean() for i in range(len(w) // 13)]
    x = np.arange(len(w))
    late = w[13:]                      # after the first quarter
    b = np.polyfit(np.arange(len(late)), late, 1)[0]
    slopes.append(b / late.mean() * 52)
    print(f"  seed {r['seed']:>3}  n={r['n_est']}  quarters " + "  ".join(f"{v:6.1f}" for v in q)
          + f"   slope after Q1 {b:+.2f}/wk = {b / late.mean() * 52:+.1%}/yr of level")
print(f"  mean slope after Q1 across seeds {np.mean(slopes):+.1%}/yr (sd {np.std(slopes):.1%})\n")

# ---- 2. autocorrelation of weekly means (detrended, after Q1) ---------------------------------------
phis = []
for r in runs:
    w = W(r["est"])[13:]
    e = w - np.polyval(np.polyfit(np.arange(len(w)), w, 1), np.arange(len(w)))
    phis.append(np.corrcoef(e[:-1], e[1:])[0, 1])
phi = float(np.mean(phis))
print(f"lag-1 autocorrelation of weekly means: " + " ".join(f"{p:.2f}" for p in phis) + f"  mean {phi:.2f}\n")

# ---- 3. null distribution of the longest run of weekly rises ---------------------------------------
rng = np.random.default_rng(0)
for n in (13, 25):
    # (a) empirical: every n-week window after Q1 in every run (overlapping windows share data,
    #     so this is a sample of windows, not independent trials)
    emp = [longest(W(r["est"])[s:s + n]) for r in runs for s in range(13, len(W(r["est"])) - n + 1)]
    # (b) AR(1) with the measured phi; (c) iid for reference
    ar = []
    for _ in range(20000):
        x = np.empty(n)
        x[0] = rng.normal()
        for t in range(1, n):
            x[t] = phi * x[t - 1] + np.sqrt(1 - phi ** 2) * rng.normal()
        ar.append(longest(x))
    iid = [longest(rng.normal(size=n)) for _ in range(20000)]
    for lbl, d in (("empirical windows", emp), (f"AR(1) phi={phi:.2f}", ar), ("iid", iid)):
        d = np.array(d)
        print(f"  {n} weeks  {lbl:<20} P(run>=5) {np.mean(d >= 5):6.1%}  P(run>=6) {np.mean(d >= 6):6.1%}  "
              f"P(run>=7) {np.mean(d >= 7):6.1%}  P(>=8) {np.mean(d >= 8):6.1%}  p99 {np.percentile(d, 99):.0f}")
    print()
