"""Replicate the install-date chain 01a -> 01b -> 01d and size the partial-window tag effect.

Can partial-window tags account for the ~6.7% slot deficit 02b is seeing, or not?
The draws below are copied from the notebooks, not invented.
"""
import numpy as np
import pandas as pd

TOPOLOGY_SEED = 20260915
AS_OF = pd.Timestamp("2026-09-15")
HISTORY_YEARS = 15
N_FACILITIES = 150
WINDOW_DAYS = 30
WIN_START = AS_OF - pd.Timedelta(days=WINDOW_DAYS)

rng = np.random.default_rng(TOPOLOGY_SEED)

# --- 01a: commission uniform over 15 years ------------------------------------------------
commission = AS_OF - pd.to_timedelta(rng.integers(0, 365 * HISTORY_YEARS, N_FACILITIES), "D")

# --- 01b: asset age, legacy vs newer cohort -----------------------------------------------
# instrumentation caps at 6 assets per facility, and selection is on criticality, which is
# drawn independently of age -- so the age distribution of instrumented assets matches the
# population and 6 per facility is the right number to simulate.
ASSETS_PER_FAC = 6
TAGS_PER_ASSET = 6          # estate mean: 3,965 tags / 662 instrumented assets

asset_install = []
for c in commission:
    max_age = max(1, (AS_OF - c).days // 365)
    for _ in range(ASSETS_PER_FAC):
        if rng.random() < 0.35 and max_age >= 9:
            left = min(8, max_age - 1)
            right = max_age
            mode = max(left, min(int(max_age * 0.7), right))
            age = min(max_age, int(rng.triangular(left, mode, right)))
        else:
            right = max(1, int(max_age * 0.6))
            mode = max(0, min(int(max_age * 0.3), right))
            age = int(rng.triangular(0, mode, right))
        inst = AS_OF - pd.Timedelta(days=int(age * 365 + rng.integers(0, 365)))
        asset_install.append(max(inst, c))

asset_install = pd.DatetimeIndex(asset_install)

# --- 01d: tag install = asset install + U{0..min(365, age_days)} ---------------------------
tag_install = []
for ai in asset_install:
    max_off = max(0, (AS_OF - ai).days)
    for _ in range(TAGS_PER_ASSET):
        off = int(rng.integers(0, min(365, max_off) + 1)) if max_off else 0
        tag_install.append(ai + pd.Timedelta(days=off))
tag_install = pd.DatetimeIndex(tag_install)
N = len(tag_install)

# --- classify ------------------------------------------------------------------------------
dark_after = tag_install >= AS_OF                       # install_date >= WINDOW_END
partial = (tag_install > WIN_START) & (tag_install < AS_OF)
full = tag_install <= WIN_START

cover = np.ones(N)
cover[dark_after] = 0.0
cover[partial] = ((AS_OF - tag_install[partial]).days / WINDOW_DAYS)

print("=" * 76)
print("INSTALL-DATE CHAIN -- can partial-window tags explain a 6.7% slot deficit?")
print("=" * 76)
print(f"  simulated tags                    {N:,}")
print(f"  asset age: min {((AS_OF-asset_install).days/365.25).min():.2f}y  "
      f"median {np.median((AS_OF-asset_install).days/365.25):.1f}y  "
      f"max {((AS_OF-asset_install).days/365.25).max():.1f}y")
print()
print(f"  {'group':<38}{'tags':>8}{'share':>9}{'mean cover':>12}{'slot loss':>11}")
print("  " + "-" * 78)
for name, m in (("install <= window start (full)", full),
                ("install inside window (partial)", partial),
                ("install >= window end (dark)", dark_after)):
    n = int(m.sum())
    mc = cover[m].mean() if n else 0.0
    loss = (1 - cover[m]).sum() / N if n else 0.0
    print(f"  {name:<38}{n:>8,}{n/N:>9.3%}{mc:>12.3f}{loss:>11.4%}")
print("  " + "-" * 78)
print(f"  {'TOTAL slot deficit from install dates':<38}{'':>8}{'':>9}{'':>12}"
      f"{(1-cover).sum()/N:>11.4%}")

print()
print(f"  partial-window tags alone         {(1 - cover[partial]).sum() / N:.4%} of all slots")
print(f"  observed unexplained deficit      ~6.74% of all slots")
ratio = 0.0674 / max((1 - cover[partial]).sum() / N, 1e-12)
print(f"  observed / simulated              {ratio:.1f}x")
print()
if (1 - cover[partial]).sum() / N < 0.02:
    print("  VERDICT  partial-window tags are real but an order of magnitude too small.")
    print("           They do not account for the deficit on their own.")
else:
    print("  VERDICT  partial-window tags are the right order of magnitude.")

# --- old vs new denominator ---------------------------------------------------------------
print()
print("=" * 76)
print("OFFLINE SHARE: naive denominator vs install-clipped denominator")
print("=" * 76)
cad = np.where(rng.random(N) < 0.25, 300, 900)
status = rng.choice(["Active", "Faulty", "Decommissioned"], size=N, p=[.97, .02, .01])
per_day = 86400 // cad

naive = (per_day * WINDOW_DAYS).astype(float)
potential = naive * cover                       # install-clipped
potential[status == "Decommissioned"] = naive[status == "Decommissioned"] * cover[
    status == "Decommissioned"]

emitting = (status != "Decommissioned") & (~dark_after)
slots_total = potential[emitting].sum()
outage_share = 0.0083                           # lambda * E[D] from the knobs
outage_removed = slots_total * outage_share

for label, denom, allslots in (
    ("naive  (WINDOW_DAYS x cadence)", naive.sum(), naive.sum()),
    ("clipped (max(start, install))",  potential.sum(), potential.sum()),
):
    off = (allslots - slots_total + outage_removed) / allslots
    band = "inside 1-3%" if 0.01 <= off <= 0.03 else "OUTSIDE the 1-3% band"
    print(f"  {label:<34}{off:>9.4%}   {band}")

print()
print(f"  decommissioned slots        {potential[status=='Decommissioned'].sum()/potential.sum():>9.4%}")
print(f"  outage slots                {outage_removed/potential.sum():>9.4%}")
print(f"  partial-window tags charged {(naive.sum()-potential.sum())/naive.sum():>9.4%}"
      "   <- removed by the fix")
