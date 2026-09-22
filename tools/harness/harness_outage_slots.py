"""Why OUTAGE_SLOTS_EXPECTED undercounts what the suppression join removes.

Ground truth is explicit enumeration of the grid slots the join would suppress:
    k*cad in [max(out_start, install, WINDOW_START), min(out_end, WINDOW_END))
Everything else is compared against that.
"""
import numpy as np
import pandas as pd
import harness_model as M

AS_OF = pd.Timestamp("2026-09-15")
WINDOW_START, WINDOW_END = AS_OF - pd.Timedelta(days=30), AS_OF
_EPOCH_TS = pd.Timestamp("1970-01-01")


def window_slots_current(install_date, cadence_s, lo=None, hi=None):
    """02b as it stands: ceil on the low end, FLOOR on the high end."""
    lo = WINDOW_START if lo is None else lo
    hi = WINDOW_END if hi is None else hi
    start = max(pd.Timestamp(install_date), pd.Timestamp(lo))
    hi = pd.Timestamp(hi)
    if start >= hi:
        return 0
    cad = int(cadence_s)
    first = int(np.ceil((start - _EPOCH_TS).total_seconds() / cad))
    last = int((hi - _EPOCH_TS).total_seconds() // cad)
    return max(0, last - first)


def window_slots_fixed(install_date, cadence_s, lo=None, hi=None):
    """ceil on BOTH ends: count of k with start <= k*cad < hi is ceil(hi/c) - ceil(start/c)."""
    lo = WINDOW_START if lo is None else lo
    hi = WINDOW_END if hi is None else hi
    start = max(pd.Timestamp(install_date), pd.Timestamp(lo))
    hi = pd.Timestamp(hi)
    if start >= hi:
        return 0
    cad = int(cadence_s)
    first = int(np.ceil((start - _EPOCH_TS).total_seconds() / cad))
    last = int(np.ceil((hi - _EPOCH_TS).total_seconds() / cad))
    return max(0, last - first)


def enumerate_slots(install_date, cadence_s, out_start, out_end):
    """Ground truth: what build_day's filter + outage join actually suppress."""
    cad = int(cadence_s)
    lo = max(pd.Timestamp(install_date), pd.Timestamp(out_start), WINDOW_START)
    hi = min(pd.Timestamp(out_end), WINDOW_END)
    if lo >= hi:
        return 0
    k0 = int((WINDOW_START - _EPOCH_TS).total_seconds()) // cad
    k1 = int((WINDOW_END - _EPOCH_TS).total_seconds()) // cad
    k = np.arange(k0, k1)
    ts = pd.to_datetime(k * cad, unit="s")
    return int(((ts >= lo) & (ts < hi)).sum())


def duration_over_cadence(install_date, cadence_s, out_start, out_end):
    """The approach the notebook does NOT use -- shown for comparison."""
    lo = max(pd.Timestamp(install_date), pd.Timestamp(out_start), WINDOW_START)
    hi = min(pd.Timestamp(out_end), WINDOW_END)
    if lo >= hi:
        return 0
    return int(round((hi - lo).total_seconds() / int(cadence_s)))


# --- build a realistic outage population ---------------------------------------------------
rng = np.random.default_rng(20260915)
N_TAGS = 3965
status = rng.choice(["Active", "Faulty", "Decommissioned"], N_TAGS, p=[.97, .02, .01])
cadence = np.where(rng.random(N_TAGS) < 0.25, 300, 900)
# install dates: 12% land inside the window (matching the estate's age profile)
install = []
for i in range(N_TAGS):
    r = rng.random()
    if r < 0.87:
        install.append(WINDOW_START - pd.Timedelta(days=float(rng.uniform(1, 900))))
    elif r < 0.99:
        install.append(WINDOW_START + pd.Timedelta(seconds=float(rng.uniform(0, 30 * 86400))))
    else:
        install.append(WINDOW_END)


def merge(iv):
    if not iv:
        return []
    out, cur = [], None
    for s, e in sorted(iv):
        if cur is not None and s <= cur[1]:
            cur = (cur[0], max(cur[1], e)); continue
        if cur is not None:
            out.append(cur)
        cur = (s, e)
    out.append(cur)
    return out


rows = []
for i in range(N_TAGS):
    if status[i] == "Decommissioned" or install[i] >= WINDOW_END:
        continue
    tid = f"GS-{i//26+1:04d}.A{i%6+1}.PT-{100+i%9}"
    for s, e in merge(M.outage_intervals(tid, status[i], WINDOW_START, WINDOW_END)):
        rows.append({"tag": tid, "i": i, "cad": int(cadence[i]), "inst": install[i],
                     "status": status[i], "s": s, "e": e})
ivs = pd.DataFrame(rows)

truth = np.array([enumerate_slots(r.inst, r.cad, r.s, r.e) for r in ivs.itertuples()])
cur_clip = np.array([window_slots_current(r.inst, r.cad, max(r.s, WINDOW_START),
                                          min(r.e, WINDOW_END)) for r in ivs.itertuples()])
cur_noclip = np.array([window_slots_current(WINDOW_START, r.cad, max(r.s, WINDOW_START),
                                            min(r.e, WINDOW_END)) for r in ivs.itertuples()])
fix_clip = np.array([window_slots_fixed(r.inst, r.cad, max(r.s, WINDOW_START),
                                        min(r.e, WINDOW_END)) for r in ivs.itertuples()])
dur = np.array([duration_over_cadence(r.inst, r.cad, r.s, r.e) for r in ivs.itertuples()])

print("=" * 78)
print("OUTAGE_SLOTS_EXPECTED -- four ways, against enumerated ground truth")
print("=" * 78)
print(f"  merged outage intervals in window   {len(ivs):,}")
print()
print(f"  {'method':<44}{'slots':>12}{'vs truth':>11}{'per iv':>9}")
print("  " + "-" * 76)
print(f"  {'ENUMERATED (what the join removes)':<44}{truth.sum():>12,}{0:>11,}{0.0:>9.3f}")
print(f"  {'current: ceil(lo), FLOOR(hi), install-clipped':<44}{cur_clip.sum():>12,}"
      f"{cur_clip.sum()-truth.sum():>11,}{(cur_clip.sum()-truth.sum())/len(ivs):>9.3f}")
print(f"  {'current, NO install clip':<44}{cur_noclip.sum():>12,}"
      f"{cur_noclip.sum()-truth.sum():>11,}{(cur_noclip.sum()-truth.sum())/len(ivs):>9.3f}")
print(f"  {'fixed: ceil(lo), CEIL(hi), install-clipped':<44}{fix_clip.sum():>12,}"
      f"{fix_clip.sum()-truth.sum():>11,}{(fix_clip.sum()-truth.sum())/len(ivs):>9.3f}")
print(f"  {'duration / cadence (not used by 02b)':<44}{dur.sum():>12,}"
      f"{dur.sum()-truth.sum():>11,}{(dur.sum()-truth.sum())/len(ivs):>9.3f}")

# --- where the current error comes from -----------------------------------------------------
err = truth - cur_clip
on_boundary = np.array([int((min(r.e, WINDOW_END) - _EPOCH_TS).total_seconds()) % r.cad == 0
                        for r in ivs.itertuples()])
print()
print(f"  per-interval undercount: min {err.min()}  max {err.max()}  "
      f"mean {err.mean():.4f}")
print(f"  intervals undercounted by exactly 1 : {int((err == 1).sum()):,}")
print(f"  intervals with no error             : {int((err == 0).sum()):,}")
print(f"    of which hi lands on a slot boundary (clipped to WINDOW_END): "
      f"{int((on_boundary & (err == 0)).sum()):,}")
print(f"    of which the interval yields 0 slots: {int(((truth == 0) & (err == 0)).sum()):,}")

# --- the window-wide call must be unaffected -------------------------------------------------
same = all(window_slots_current(d, c) == window_slots_fixed(d, c)
           for d, c in zip(install, cadence))
print()
print(f"  window-wide call (hi = WINDOW_END, a slot boundary) identical under both: {same}")
print("  -> SLOTS_TOTAL == LIVE_SLOTS keeps passing; only the outage call changes")

# --- both-sides window clipping --------------------------------------------------------------
pre = int((ivs["s"] < WINDOW_START).sum())
post = int((ivs["e"] > WINDOW_END).sum())
print()
print(f"  intervals starting before WINDOW_START : {pre:,}  (clipped by max(s, WINDOW_START))")
print(f"  intervals ending after WINDOW_END      : {post:,}  (clipped by min(e, WINDOW_END))")
print(f"  both sides clip identically in expectation and in the join: "
      f"{bool((truth == fix_clip).all())}")

assert (fix_clip == truth).all(), "the fixed formula still disagrees with enumeration"
print("\nOK  ceil on both ends reproduces the enumerated join result exactly, "
      f"all {len(ivs):,} intervals")
