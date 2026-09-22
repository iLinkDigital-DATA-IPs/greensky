"""Model the JOIN CARDINALITY of 02b's gap layer, not just the interval lists.

The earlier harness generated outage and freeze intervals and measured their total
duration. It never modelled the left joins that apply them, so it could not see that two
overlapping intervals make one reading match twice. This closes that hole:

  1. per-day emitted-row count against the dimension's expected count, before and after
     merge_intervals -- this is the assertion that fired on 2026-08-16
  2. the suppressed-slot SET for a day, computed from a 30-day window and from a 1-day
     window, must be identical -- merging must not introduce window dependence
"""
import numpy as np
import pandas as pd
import harness_model as M

AS_OF = pd.Timestamp("2026-09-15")
WIN_START, WIN_END = AS_OF - pd.Timedelta(days=30), AS_OF
EPOCH = pd.Timestamp("1970-01-01")

N_TAGS = 3965
rng = np.random.default_rng(20260915)
status = rng.choice(["Active", "Faulty", "Decommissioned"], size=N_TAGS, p=[0.97, 0.02, 0.01])
cadence = np.where(rng.random(N_TAGS) < 0.25, 300, 900)
TAG_IDS = [f"GS-{i//26+1:04d}.A{i%6+1}.PT-{100+i%9}" for i in range(N_TAGS)]
LIVE = [i for i in range(N_TAGS) if status[i] != "Decommissioned"]


def merge_intervals(iv):
    """The notebook's merge_intervals, for a single key."""
    if not iv:
        return []
    out, cur = [], None
    for s, e in sorted(iv):
        if cur is not None and s <= cur[1]:
            cur = (cur[0], max(cur[1], e))
            continue
        if cur is not None:
            out.append(cur)
        cur = (s, e)
    out.append(cur)
    return out


def slots_of(day, cad):
    k0 = int(day.timestamp()) // cad
    k1 = int((day + pd.Timedelta(days=1)).timestamp()) // cad
    return np.arange(k0, k1)


def matches(idx, cad, iv):
    """How many intervals each slot falls inside, under a half-open predicate."""
    ts = pd.to_datetime(idx * cad, unit="s")
    c = np.zeros(len(idx), dtype=int)
    for s, e in iv:
        c += ((ts >= s) & (ts < e)).astype(int)
    return c


print("=" * 78)
print("JOIN CARDINALITY -- per-day emitted rows against expected")
print("=" * 78)
print(f"  {'day':<12}{'expected':>11}{'raw emitted':>13}{'extra':>8}{'merged':>12}{'extra':>8}")
print("  " + "-" * 64)

days = [WIN_START + pd.Timedelta(days=i) for i in range(30)]
cache_raw, cache_mrg = {}, {}
for i in LIVE:
    iv = M.outage_intervals(TAG_IDS[i], status[i], WIN_START, WIN_END)
    fz = M.freeze_interval(TAG_IDS[i], WIN_START, WIN_END)
    cache_raw[i] = (iv, fz)
    cache_mrg[i] = (merge_intervals(iv), merge_intervals(fz))

tot_raw = tot_mrg = tot_exp = 0
for d in days:
    exp = raw = mrg = 0
    for i in LIVE:
        cad = int(cadence[i])
        idx = slots_of(d, cad)
        exp += len(idx)
        o, f = cache_raw[i]
        raw += int((np.maximum(matches(idx, cad, o), 1)
                    * np.maximum(matches(idx, cad, f), 1)).sum())
        o, f = cache_mrg[i]
        mrg += int((np.maximum(matches(idx, cad, o), 1)
                    * np.maximum(matches(idx, cad, f), 1)).sum())
    tot_exp += exp; tot_raw += raw; tot_mrg += mrg
    flag = "" if mrg == exp else "   <-- STILL WRONG"
    if raw != exp or d == days[0]:
        print(f"  {str(d.date()):<12}{exp:>11,}{raw:>13,}{raw-exp:>8,}{mrg:>12,}"
              f"{mrg-exp:>8,}{flag}")

print("  " + "-" * 64)
print(f"  {'30-day':<12}{tot_exp:>11,}{tot_raw:>13,}{tot_raw-tot_exp:>8,}"
      f"{tot_mrg:>12,}{tot_mrg-tot_exp:>8,}")
assert tot_mrg == tot_exp, "merged intervals still emit duplicate readings"
print("\nOK  after merge_intervals every day emits exactly the expected row count")

# --- window independence of the gap layer -------------------------------------------------
print("\nWINDOW INDEPENDENCE of the suppressed-slot set")
bad = 0
for d in days:
    d_end = d + pd.Timedelta(days=1)
    for i in LIVE:
        cad = int(cadence[i])
        idx = slots_of(d, cad)
        # as a backfill sees it: intervals drawn over the whole 30-day window
        o30 = merge_intervals(M.outage_intervals(TAG_IDS[i], status[i], WIN_START, WIN_END))
        # as an incremental run sees it: intervals drawn over that one day only
        o1 = merge_intervals(M.outage_intervals(TAG_IDS[i], status[i], d, d_end))
        if not np.array_equal(matches(idx, cad, o30) > 0, matches(idx, cad, o1) > 0):
            bad += 1
print(f"  tag-days compared : {len(days) * len(LIVE):,}")
print(f"  mismatches        : {bad}")
assert bad == 0, "the gap layer is window-dependent -- backfill would differ from incremental"
print("OK  a 1-day window suppresses exactly the slots the 30-day window suppresses")
print("    (merging is a pure function of the intervals covering the day, and the 45-day")
print("     slot grid with a one-slot lookback always yields all of them)")
