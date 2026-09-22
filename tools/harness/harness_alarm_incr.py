"""Does 02d's incremental path reproduce the backfill exactly?

02d derives alarms with a debounce machine, so its output depends on where each breach RUN
began -- not merely on whether an alarm was open at the window edge. This script checks the
design that shipped, and pins the design that was rejected so nobody re-introduces it.

  SHIPPED   every run scans the full telemetry retention and rewrites every partition.
            Backfill and incremental are identical by construction.
  REJECTED  a bounded lookback scan. It disagrees with the backfill, and no lookback length
            fixes it, because any scan edge can land in the middle of a run.

Both are asserted: the first that it works, the second that it does NOT. If a future change
makes the lookback version pass, this file is the place that says why that would be a
surprise worth understanding rather than a win.
"""
import numpy as np
from harness_sessionise import sessionise

DEBOUNCE = 3
LOOKBACK_DAYS = 7
DAYS = 30
SLOTS_PER_DAY = 96                      # 900s cadence
N = DAYS * SLOTS_PER_DAY


def derive(actions, lo, hi, debounce):
    """Alarms from a scan of [lo, hi), as (raised_slot, cleared_slot)."""
    return [(r + lo, None if c is None else c + lo)
            for r, c in sessionise(list(actions[lo:hi]), debounce)]


def run_full_retention(actions, debounce):
    """02d as shipped: each run scans everything and replaceWhere covers every partition."""
    table = {}
    for _day in range(DAYS):
        table = {}
        for r, c in derive(actions, 0, N, debounce):
            table.setdefault(r // SLOTS_PER_DAY, []).append((r, c))
    return sorted(x for rows in table.values() for x in rows)


def run_lookback(actions, debounce):
    """The rejected design: scan back LOOKBACK_DAYS, rewrite only the partitions scanned."""
    table = {}
    for d in range(DAYS):
        win_lo, win_hi = d * SLOTS_PER_DAY, (d + 1) * SLOTS_PER_DAY
        scan_lo = max(0, win_lo - LOOKBACK_DAYS * SLOTS_PER_DAY)
        got = derive(actions, scan_lo, win_hi, debounce)
        for p in range(scan_lo // SLOTS_PER_DAY, d + 1):
            table[p] = []
        for r, c in got:
            table.setdefault(r // SLOTS_PER_DAY, []).append((r, c))
    return sorted(x for rows in table.values() for x in rows)


if __name__ == "__main__":
    rng = np.random.default_rng(4242)
    bad_full = bad_look = 0
    TRIALS = 400
    for _t in range(TRIALS):
        p = rng.choice([[.02, .88, .10], [.08, .82, .10], [.20, .70, .10], [.45, .45, .10]])
        acts = rng.choice([1, -1, 0], size=N, p=p)
        back = sorted(derive(acts, 0, N, DEBOUNCE))
        bad_full += (run_full_retention(acts, DEBOUNCE) != back)
        bad_look += (run_lookback(acts, DEBOUNCE) != back)

    print(f"  {TRIALS} trials of {DAYS} days x {SLOTS_PER_DAY} slots")
    print(f"    shipped  (full retention scan) disagreements : {bad_full}")
    print(f"    rejected ({LOOKBACK_DAYS}-day lookback)      disagreements : {bad_look}")
    assert bad_full == 0, "the full-retention scan no longer reproduces the backfill"
    assert bad_look > 0, (
        "the lookback scan now agrees with the backfill, which contradicts the reason 02d "
        "scans the whole retention. Understand why before simplifying 02d."
    )
    print("\nOK  backfill and 30 incremental runs are identical under the shipped design")
    print("OK  the rejected lookback design still disagrees, as documented in 02d")

    # --- same input, same output ------------------------------------------------------------
    acts = rng.choice([1, -1, 0], size=N, p=[.08, .82, .10])
    assert derive(acts, 0, N, DEBOUNCE) == derive(acts, 0, N, DEBOUNCE)
    print("OK  the derivation carries no hidden state")

    # --- the minimal shape of the failure ----------------------------------------------------
    # A run of 8 breaches from slot 10, debounce 3: the full scan raises at 12, and a scan
    # starting at 11, 12 or 13 raises later. No alarm is open at any of those edges.
    demo = [-1] * 10 + [1] * 8 + [-1] * 10
    full = sessionise(demo, DEBOUNCE)
    print(f"\n  why no lookback length works -- run of 8 breaches from slot 10, debounce 3:")
    print(f"    scan from 0  -> {full}")
    for edge in (11, 12, 13):
        sub = [(r + edge, None if c is None else c + edge)
               for r, c in sessionise(demo[edge:], DEBOUNCE)]
        print(f"    scan from {edge} -> {sub}   {'same' if sub == full else 'DIFFERENT'}")
    print("    the run simply started earlier than the scan; any edge can land mid-run")
