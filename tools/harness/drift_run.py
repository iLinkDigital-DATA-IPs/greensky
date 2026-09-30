"""One long 03d backfill on harness_maintenance's synthetic estate under one seed. Pickles the
daily overdue series (established and first-PM cohorts, with entries into overdue) and LDAR
detected / repaired / outstanding, for drift_analyse.py and thresh_analyse.py.

    python drift_run.py SEED DAYS [OUT_DIR]      # SEED 0 = the repo's own seed; OUT_DIR default "."

This is the multi-seed null behind 03d's NET_RISE_MAX_SHARE, OVERDUE_SHARE_MAX and the
strict-monotonicity check. Run several seeds (the thresholds were set from 8, at 540 days:
0 11 23 37 41 53 67 79) as separate processes, then run the two analyses on OUT_DIR.

A non-zero SEED must change BOTH seeds together: 03d's draws (g["TOPOLOGY_SEED"]) and the
harness's copy of 02a's state machine (harness_model.TOPOLOGY_SEED). 03d copies its PM
calendar from 02a; if only one moves, 03d's due dates stop landing on 02a's Scheduled PM
stops, PMs are never recorded as done, and the overdue share reads 35-43% against ~12%. That
looks like a drift finding and is not one. The guards below fail such a run instead.
"""
import contextlib
import io
import os
import pickle
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pandas as pd                    # noqa: E402
import harness_maintenance as HM      # noqa: E402
import harness_model as M             # noqa: E402

seed, n_days = int(sys.argv[1]), int(sys.argv[2])
out_dir = sys.argv[3] if len(sys.argv) > 3 else "."
g = HM.load_model()
if seed:
    g["TOPOLOGY_SEED"] = seed
    M.TOPOLOGY_SEED = seed
assert g["TOPOLOGY_SEED"] == M.TOPOLOGY_SEED, (
    f"03d seed {g['TOPOLOGY_SEED']} != harness 02a seed {M.TOPOLOGY_SEED}: the PM calendars desync")
H0 = pd.Timestamp("2026-06-17")
end = H0 + n_days * HM.DAY
with contextlib.redirect_stdout(io.StringIO()):
    U = HM.upstream(g, H0, end)
m, i, l, pm = HM.run(g, U, H0, end, H0)
assets = g["asset_views"](U["eq"], U["fac"])
days = pd.date_range(H0, end - HM.DAY, freq="D")
est, young = g["split_cohorts"](assets, H0)
fe = g["overdue_flags"](est, m, days, H0, U["calib"])
oe, ee = fe.sum(axis=0), g["overdue_entries"](fe)
oy = g["overdue_trajectory"](young, m, days, H0, U["calib"])
# The desync signature: healthy runs sit at ~11-13% overdue. A mean over 25% means PMs are not
# landing on 02a's stops, and nothing downstream of this run measures the model.
_share = float(oe.mean()) / len(est)
assert _share < 0.25, (f"seed {seed}: established overdue share averages {_share:.0%} (healthy ~12%). "
                       "The PM calendar is out of step with 02a's stops -- check both seeds moved")
st, _ = HM.as_of(g, U, end)
by_fac = {}
for a in assets.values():
    by_fac.setdefault(a["facility_sk"], []).append(a)
sv = g["ldar_surveys_in"](g["state_index"](st), by_fac, U["fac"], H0, end, H0)
det, rep_, out = g["ldar_cumulative"](sv, days)
os.makedirs(out_dir, exist_ok=True)
with open(os.path.join(out_dir, f"drift_{seed}.pkl"), "wb") as f:
    pickle.dump({"seed": seed, "days": days, "est": oe, "entries": ee, "young": oy, "n_est": len(est),
                 "n_all": len(assets), "all": oe + oy, "ldar_det": det, "ldar_rep": rep_, "ldar_out": out}, f)
wk = g["weekly_means"](oe)
print(seed, len(est), "weekly:", " ".join(f"{v:.0f}" for v in wk))
