"""Does 02d's validation section 1 FAIL on an alarm with no telemetry behind it?

Section 1 exists to prove every process alarm has at least ALARM_DEBOUNCE_SAMPLES breaching
readings at or before its raised_ts. Its first formulation left-joined alarm -> telemetry on
the upper time bound only and applied the lower bound as a filter afterwards. The filter
discards the unmatched, all-null row, so an alarm with no readings in its window vanished
from the aggregation and the check passed -- on exactly the failure it was written to catch.

The fix has two parts, and this harness requires both:
  1. both time bounds in the join condition, so the unmatched alarm survives the join
  2. coalesce(sum(breaching), 0), because sum() over an all-null group is null, not 0, and
     `null < N` is not true, so the filter would drop the alarm a second way

This mirrors the check's formulation in pandas with SQL null semantics (nullable dtypes,
Kleene AND, sum with min_count=1, a WHERE that keeps only true). Same limitation as the rest
of harness/: it verifies the formulation, not Spark's execution of it.
"""
import pandas as pd

DEBOUNCE = 3
WINDOW = pd.Timedelta(hours=DEBOUNCE * 2)     # the notebook's INTERVAL {DEBOUNCE * 2} HOURS
CAD = pd.Timedelta(minutes=15)
R = pd.Timestamp("2026-09-10 12:00:00")
LIMIT = 100.0


def readings(tag, n, value, end=R, suppressed=False):
    """n readings at cadence ending at `end`, all at `value`."""
    ts = [end - CAD * i for i in range(n)][::-1]
    return [{"tag_sk": tag, "rts": t, "value_num": value,
             "suppressed": suppressed, "neutral": False} for t in ts]


# One tag per alarm, so each case is isolated. Expected: flagged or not.
CASES = {
    #  alarm_sk: (description, telemetry rows, should_flag)
    1: ("backed: 3 breaching readings in window", readings(1, 3, 150.0), False),
    2: ("no telemetry at all for the tag", [], True),
    3: ("telemetry only older than the window",
        readings(3, 4, 150.0, end=R - WINDOW - CAD), True),
    4: ("readings in window, none breaching", readings(4, 4, 50.0), True),
    5: ("breaching in window but all suppressed", readings(5, 4, 150.0, suppressed=True), True),
    6: ("only 2 breaching readings in window",
        readings(6, 4, 50.0, end=R - 2 * CAD) + readings(6, 2, 150.0), True),
}

alarms = pd.DataFrame([{"alarm_sk": k, "tag_sk": k, "raised_ts": R,
                        "is_upper": True, "limit_value": LIMIT} for k in CASES])
tel = pd.DataFrame([r for _, rows, _ in CASES.values() for r in rows],
                   columns=["tag_sk", "rts", "value_num", "suppressed", "neutral"])
tel = tel.astype({"value_num": "Float64", "suppressed": "boolean", "neutral": "boolean"})


def left_join(left, right, cond):
    """SQL LEFT JOIN on tag_sk AND cond: unmatched left rows survive with nulls."""
    m = left.merge(right, on="tag_sk", how="inner")
    m = m[cond(m).fillna(False).astype(bool)]
    miss = left[~left["alarm_sk"].isin(m["alarm_sk"])]
    out = pd.concat([m, miss], ignore_index=True)
    return out.astype({"value_num": "Float64", "suppressed": "boolean", "neutral": "boolean"})


def n_breach(joined, coalesce):
    up = joined["value_num"] > joined["limit_value"]
    dn = joined["value_num"] < joined["limit_value"]
    br = up.where(joined["is_upper"].astype(bool), dn)
    br = br & ~joined["suppressed"] & ~joined["neutral"]          # Kleene: NA stays NA
    s = (br.astype("Int64").groupby(joined["alarm_sk"]).sum(min_count=1))
    return s.fillna(0) if coalesce else s


def flagged(n):
    return set(n[(n < DEBOUNCE).fillna(False).astype(bool)].index)


upper = lambda m: m["rts"] <= m["raised_ts"]
lower = lambda m: m["rts"] >= m["raised_ts"] - WINDOW

# --- the first formulation: upper bound in the join, lower bound as a filter afterwards -----
j_old = left_join(alarms, tel, upper)
j_old = j_old[lower(j_old).fillna(False).astype(bool)]           # WHERE: null -> dropped
old = flagged(n_breach(j_old, coalesce=False))

# --- both bounds in the join, but no coalesce ------------------------------------------------
j_new = left_join(alarms, tel, lambda m: upper(m) & lower(m))
no_coalesce = flagged(n_breach(j_new, coalesce=False))

# --- the notebook as it now stands -----------------------------------------------------------
n_fixed = n_breach(j_new, coalesce=True)
fixed = flagged(n_fixed)

expect = {k for k, (_, _, f) in CASES.items() if f}
no_backing = {2, 3}

print("=" * 84)
print("VALIDATION SECTION 1: IS AN ALARM WITH NO BACKING READINGS CAUGHT?")
print("=" * 84)
print(f"  {'alarm':<7}{'case':<44}{'old':>8}{'no coal.':>10}{'fixed':>8}   n_breach")
print("  " + "-" * 86)
for k, (desc, _, _) in CASES.items():
    mark = lambda s: "FLAG" if k in s else "pass"
    print(f"  {k:<7}{desc:<44}{mark(old):>8}{mark(no_coalesce):>10}{mark(fixed):>8}"
          f"   {n_fixed.get(k)}")

# The harness must reproduce the defect, or it would not have caught it.
assert no_backing.isdisjoint(old), (
    "the old formulation caught a no-backing alarm -- this harness is not reproducing the "
    "defect and would not have caught it")
assert no_backing.isdisjoint(no_coalesce), (
    "without coalesce the no-backing alarms were still caught -- the null semantics are not "
    "being mirrored, so this harness cannot show the coalesce is load-bearing")
assert fixed == expect, f"fixed check flagged {sorted(fixed)}, expected {sorted(expect)}"
assert all(n_fixed[k] == 0 for k in no_backing), "a no-backing alarm did not reach n_breach = 0"

print(f"\nOK  the old formulation passes alarms {sorted(no_backing)} with no backing readings")
print("OK  moving the bound into the join alone still passes them: sum() over nulls is null")
print(f"OK  the fixed check flags exactly {sorted(expect)}; no-backing alarms reach n_breach = 0")
