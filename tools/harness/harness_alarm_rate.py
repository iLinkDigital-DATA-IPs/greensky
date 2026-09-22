"""Alarm rate per facility-month, using 02a's ACTUAL state machine.

The first estimate used a synthetic state trace roughly 8x churnier than 02a, which matters
enormously: essentially every alarm the telemetry can produce comes from a Standby interval
suppressing a pressure or temperature below its low limit, so the alarm rate is set by the
Standby ARRIVAL RATE, not by process variance.
"""
import numpy as np
import pandas as pd
import harness_model as M
from harness_alarms import TEMPLATES, F

AS_OF = pd.Timestamp("2026-09-15")
WIN_START, WIN_END = AS_OF - pd.Timedelta(days=30), AS_OF
DEBOUNCE = 3

# --- 01_topology_config constants, verbatim ------------------------------------------------
EQUIPMENT_TYPES = {
    "Compressor": {"life": 20, "insp_days": 90, "leak_propensity": 1.00},
    "Separator": {"life": 25, "insp_days": 180, "leak_propensity": 0.55},
    "Storage Tank": {"life": 30, "insp_days": 365, "leak_propensity": 0.85},
    "Flare": {"life": 20, "insp_days": 180, "leak_propensity": 0.60},
    "Pump": {"life": 15, "insp_days": 120, "leak_propensity": 0.75},
    "Metering Station": {"life": 20, "insp_days": 180, "leak_propensity": 0.45},
}
STATE_DUTY_FACTOR = {"Compressor": 1.00, "Pump": 0.85, "Flare": 0.55, "Separator": 0.40,
                     "Metering Station": 0.30, "Storage Tank": 0.20}
BASE_MTBF_DAYS = 26.0
SPURIOUS_TRIP_SHARE = 0.30
STATE_DWELL_HOURS = {"Startup": (0.25, 1.0), "Shutdown": (0.25, 1.0),
                     "Down": (2.0, 14.0), "Standby": (12.0, 96.0)}
MAINTENANCE_DWELL_HOURS = {"Scheduled PM": (6.0, 24.0), "Corrective": (12.0, 72.0)}
UNPLANNED_CAUSE_WEIGHTS = {"Trip": 0.55, "Corrective": 0.30, "Market": 0.10, "Unknown": 0.05}
CAUSE_NAMES = list(UNPLANNED_CAUSE_WEIGHTS)
CAUSE_P = [UNPLANNED_CAUSE_WEIGHTS[n] for n in CAUSE_NAMES]


def mtbf_days(et, age_years):
    ev = EQUIPMENT_TYPES[et]
    life = max(ev["life"], 1)
    age_factor = 0.65 + 0.70 * min(max(age_years, 0.0) / life, 1.5)
    return BASE_MTBF_DAYS / max(STATE_DUTY_FACTOR[et] * ev["leak_propensity"] * age_factor, 1e-6)


def simulate(et, eq_id, age_years, install, start, until):
    """02a's step(), reproduced."""
    rows, state, cause, ts = [], "Running", None, start
    insp = EQUIPMENT_TYPES[et]["insp_days"]
    phase = float(M.get_rng("pm_phase", eq_id).uniform(0.0, insp))
    while ts < until:
        rng = M.get_rng("state", eq_id, state, ts.isoformat())
        if state == "Running":
            anchor = install + pd.Timedelta(days=phase)
            if ts < anchor:
                t_pm = anchor
            else:
                el = (ts - anchor) / pd.Timedelta(days=insp)
                t_pm = anchor + pd.Timedelta(days=insp * (np.floor(el) + 1))
            t_fail = ts + pd.Timedelta(days=float(rng.exponential(mtbf_days(et, age_years))))
            if t_pm <= t_fail:
                dur, nxt, ncause = t_pm - ts, "Shutdown", "Scheduled PM"
            else:
                picked = str(rng.choice(CAUSE_NAMES, p=CAUSE_P))
                dur = t_fail - ts
                nxt = "Down" if picked == "Trip" else "Shutdown"
                ncause = picked
        elif state == "Shutdown":
            lo, hi = STATE_DWELL_HOURS["Shutdown"]
            dur = pd.Timedelta(hours=float(rng.uniform(lo, hi)))
            nxt = "Standby" if cause == "Market" else "Maintenance"
            ncause = cause
        elif state == "Down":
            lo, hi = STATE_DWELL_HOURS["Down"]
            dur = pd.Timedelta(hours=float(rng.uniform(lo, hi)))
            if rng.random() < SPURIOUS_TRIP_SHARE:
                nxt, ncause = "Startup", cause
            else:
                nxt, ncause = "Maintenance", "Corrective"
        elif state == "Maintenance":
            lo, hi = MAINTENANCE_DWELL_HOURS.get(cause, MAINTENANCE_DWELL_HOURS["Corrective"])
            dur, nxt, ncause = pd.Timedelta(hours=float(rng.uniform(lo, hi))), "Startup", cause
        elif state == "Standby":
            lo, hi = STATE_DWELL_HOURS["Standby"]
            dur, nxt, ncause = pd.Timedelta(hours=float(rng.uniform(lo, hi))), "Startup", cause
        else:  # Startup
            lo, hi = STATE_DWELL_HOURS["Startup"]
            dur, nxt, ncause = pd.Timedelta(hours=float(rng.uniform(lo, hi))), "Running", None
        rows.append((state, ts, ts + dur))
        ts, state, cause = ts + dur, nxt, ncause
    return pd.DataFrame(rows, columns=["state", "start_ts", "end_ts"])


SUPPRESS = {"Down", "Maintenance", "Startup", "Shutdown"}
CAD = 900
N_ASSETS_PER_TYPE = 12

if __name__ == "__main__":
    print("=" * 96)
    print("ALARM RATE with 02a's REAL state machine")
    print("=" * 96)
    print(f"  {'equipment':<18}{'MTBF(d)':>9}{'Standby %':>11}{'Standby ivals':>15}"
          f"{'alarms/asset/30d':>18}{'by type':>22}")
    print("  " + "-" * 94)

    by_type = {}
    for et, tmpls in TEMPLATES.items():
        tot_alarms, tot_standby, tot_sb_iv, n = 0, 0, 0, 0
        kinds = {}
        for a in range(N_ASSETS_PER_TYPE):
            eq_id = f"GS-{a+1:04d}-E{a:03d}"
            fac_id = f"GS-{a+1:04d}"
            age = 1.5 + 4.0 * float(M.get_rng("age", eq_id).random())
            install = AS_OF - pd.Timedelta(days=age * 365)
            tr = simulate(et, eq_id, age, install, AS_OF - pd.Timedelta(days=120), WIN_END)
            k0, k1 = int(WIN_START.timestamp()) // CAD, int(WIN_END.timestamp()) // CAD
            idx = np.arange(k0, k1)
            ts = pd.DatetimeIndex(pd.Timestamp("1970-01-01") + pd.to_timedelta(idx * CAD, unit="s"))
            i = np.clip(np.searchsorted(tr["start_ts"].values, ts.values, "right") - 1, 0, len(tr) - 1)
            st = tr["state"].values[i]
            s0, s1 = tr["start_ts"].values[i], tr["end_ts"].values[i]
            span = (s1 - s0) / np.timedelta64(1, "s")
            phi = (ts.values - s0) / np.timedelta64(1, "s") / np.maximum(span, 1)
            la, lp = M.series_params("asset", eq_id); fa, fp = M.series_params("facility", fac_id)
            tsec = idx.astype(float) * CAD
            load, fac = M.spectral(la, lp, tsec), M.spectral(fa, fp, tsec)
            tot_standby += int((st == "Standby").sum()); n += len(st)
            tot_sb_iv += int((tr["state"] == "Standby").sum())
            for j, t in enumerate(tmpls):
                tg = dict(zip(F, t)); tg["tag_id"] = f"{fac_id}.A1.XT-{101+j}"
                v, _ = M.value_model(tg, ts, st, phi, load, fac, idx, AS_OF - pd.Timedelta(days=90))
                elig = ~np.isin(st, list(SUPPRESS))
                if tg["measurement_type"] in ("flow", "rpm"):
                    elig &= (st != "Standby")
                for lim, up, kind in ((tg["alarm_hi"], 1, "Hi"), (tg["alarm_hihi"], 1, "HiHi"),
                                      (tg["alarm_lo"], 0, "Lo"), (tg["alarm_lolo"], 0, "LoLo")):
                    if lim is None:
                        continue
                    br = ((v > lim) if up else (v < lim)) & elig
                    d = np.diff(np.concatenate(([0], br.view(np.int8), [0])))
                    r = np.flatnonzero(d == -1) - np.flatnonzero(d == 1)
                    c = int((r >= DEBOUNCE).sum())
                    tot_alarms += c
                    kinds[kind] = kinds.get(kind, 0) + c
        per_asset = tot_alarms / N_ASSETS_PER_TYPE
        by_type[et] = per_asset
        ks = " ".join(f"{k}:{v}" for k, v in sorted(kinds.items())) or "-"
        print(f"  {et:<18}{mtbf_days(et, 3.5):>9.0f}{tot_standby/max(n,1):>11.2%}"
              f"{tot_sb_iv/N_ASSETS_PER_TYPE:>15.2f}{per_asset:>18.2f}{ks:>22}")

    # --- weight by the instrumented mix ---------------------------------------------------------
    # INSTRUMENT_PRIORITY orders Compressor, Separator, Storage Tank, Metering, Flare, Pump and
    # caps 6 per facility, so the instrumented population skews hard to the first few.
    MIX = {"Compressor": 0.34, "Separator": 0.26, "Storage Tank": 0.17,
           "Metering Station": 0.12, "Flare": 0.07, "Pump": 0.04}
    INSTRUMENTED, FACILITIES = 662, 150
    weighted = sum(by_type[k] * v for k, v in MIX.items())
    per_fac = weighted * INSTRUMENTED / FACILITIES
    print("  " + "-" * 94)
    print(f"  weighted mean alarms per instrumented asset per 30 days : {weighted:.2f}")
    print(f"  estate: {weighted:.2f} x {INSTRUMENTED} / {FACILITIES} facilities "
          f"= {per_fac:.1f} alarms per facility-month")
    print(f"  target band 10 - 60")
    print()
    if per_fac < 10:
        print(f"  VERDICT  BELOW band. Alarms come almost entirely from Standby intervals, whose")
        print(f"           arrival rate is Market cause ({UNPLANNED_CAUSE_WEIGHTS['Market']:.0%} of "
              "unplanned stops) in 02a.")
    elif per_fac > 60:
        print("  VERDICT  ABOVE band.")
    else:
        print("  VERDICT  inside the 10-60 band.")
