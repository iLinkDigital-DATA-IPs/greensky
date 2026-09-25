"""02e's CH4 detector telemetry: shared code identical to 02b, and the model's properties.

Three things are checked here.

1. 02e's copy of 02b's helpers is character-for-character identical (shared_defs.py). This is
   what makes "no second spectral formulation" a checked property rather than a promise.

2. A pandas mirror of 02e's generator -- same spectral series (harness_model's mirror of 02b),
   same time stretch, same onset/risk/state mechanism, same outage grid, same row hash --
   run over a SYNTHETIC registry and state history. The registry (equipment types, ages,
   criticality) and fact_asset_state live in OneLake, so they are simulated here with the
   right shape, not reproduced. What this verifies is the mechanism: the rates below are
   what the notebook should land near, not what it will print to the digit.

3. Determinism: two runs identical, and a 30-day backfill equal to 30 one-day runs.
"""
import numpy as np
import pandas as pd
import harness_model as M
import shared_defs as S
from harness_final_rate import load_config

C = load_config()
AS_OF = pd.Timestamp("2026-09-15")
START, END = AS_OF - pd.Timedelta(days=30), AS_OF
CAD = C["CH4_INTERVAL_HOURS"] * 3600
EPOCH = pd.Timestamp("2026-01-01")
STATES = C["STATES"]


# --- 1. the shared definitions ----------------------------------------------------------------
def check_shared():
    a, b = S.extract(S.NB_02B), S.extract(S.NB_02E)
    diff = [k for k in S.SHARED if a[k] != b[k]]
    assert not diff, (f"02e's copy of {diff} differs from 02b. Re-copy them: 02e must not carry a "
                      "second formulation of 02b's hash, spectral, slot or state-join code.")
    print(f"OK  {len(S.SHARED)} definitions in 02e are identical to 02b's")


# --- 2. synthetic registry, shaped like 01b's --------------------------------------------------
def registry(n_fac=150, per=4):
    rng = M.get_rng("h_ch4_registry")
    types = list(C["EQUIPMENT_TYPES"])
    # instrumented assets are the highest-criticality ones, so crit_bias weights the draw
    w = np.array([C["EQUIPMENT_TYPES"][t]["crit_bias"] for t in types]); w /= w.sum()
    rows, sid = [], 0
    for f in range(1, n_fac + 1):
        for _ in range(per):
            sid += 1
            s = f"SNS-{sid:05d}"
            et = types[rng.choice(len(types), p=w)]
            ev = C["EQUIPMENT_TYPES"][et]
            age = float(np.clip(rng.lognormal(np.log(1.5), 0.9), 0.05, 15.0))
            crit = "Critical" if rng.random() < 0.55 else "High"
            stype = str(rng.choice(list(C["SENSOR_TYPES"]), p=list(C["SENSOR_TYPES"].values())))
            # 01b's derivations, mirrored
            tier = "critical" if crit == "Critical" else "standard"
            sigma = C["CH4_SIGMA_PPM"][stype] * (1.0 + C["CH4_SIGMA_JITTER"] * (
                2.0 * M.get_rng("sensor_sigma", s).random() - 1.0))
            thr = C["CH4_AMBIENT_REF_PPM"] + C["CH4_EXCEEDANCE_SIGMAS"] * sigma * (
                C["CH4_CRITICAL_THRESHOLD_FACTOR"] if tier == "critical" else 1.0)
            inst = (START + pd.Timedelta(hours=int(rng.integers(0, 600))) if rng.random() < 0.03
                    else START - pd.Timedelta(days=int(rng.integers(30, 3000))))
            rows.append(dict(sensor_id=s, equipment_sk=sid, facility_id=f"GS-{f:04d}",
                             equipment_type=et, sensor_type=stype, tier=tier,
                             sigma_ppm=round(sigma, 4), exceedance_threshold_ppm=round(thr, 4),
                             leak_propensity=ev["leak_propensity"],
                             age_ratio=min(age / ev["life"], 1.0), install_ts=inst.floor("h")))
    r = pd.DataFrame(rows)
    rank = r["sensor_id"].map(lambda s: M._seed("sensor_status", s)).rank(method="first")
    nd = int(round(C["SENSOR_DECOMMISSIONED_SHARE"] * len(r)))
    nf = int(round(C["SENSOR_FAULTY_SHARE"] * len(r)))
    r["status"] = np.where(rank <= nd, "Decommissioned",
                           np.where(rank <= nd + nf, "Faulty", "Active"))
    r["risk"] = r["leak_propensity"] * (1.0 + C["CH4_AGE_WEIGHT"] * r["age_ratio"])
    r["risk_rel"] = r["risk"] / r["risk"].mean()
    return r


def asset_states(eq, lo=pd.Timestamp("2026-06-01"), hi=AS_OF + pd.Timedelta(days=1)):
    """A renewal process tuned to the estate's measured state mix (Running 95.75%, Maintenance
    3.29%, Standby 0.44%, Down 0.38%, Startup 0.08%, Shutdown 0.05%); window-independent."""
    rng = M.get_rng("h_ch4_state", eq)
    out, t = [], lo
    while t < hi:
        d = pd.Timedelta(hours=float(rng.lognormal(np.log(700), 0.6)))
        out.append(("Running", t, t + d)); t += d
        u = rng.random()
        if u < 0.35:
            seq = [("Standby", 9)]
        elif u < 0.85:
            seq = [("Shutdown", 0.6), ("Maintenance", 42)]
        else:
            seq = [("Shutdown", 0.6), ("Down", 20)]
        for st, med in seq + [("Startup", 0.8)]:
            d = pd.Timedelta(hours=float(rng.lognormal(np.log(med), 0.5)))
            out.append((st, t, t + d)); t += d
    return out


def state_of(eq, ts):
    iv = asset_states(eq)
    s = np.array([a[1].value for a in iv]); names = np.array([a[0] for a in iv])
    return names[np.searchsorted(s, ts.values.astype("int64"), "right") - 1]


def outages(sid, faulty, lo, hi):
    slot = C["CH4_OUTAGE_MEAN_DAYS"] * 86400.0
    k0 = int(np.floor((lo - EPOCH).total_seconds() / slot)) - 1
    k1 = int(np.floor((hi - EPOCH).total_seconds() / slot))
    out = []
    for k in range(k0, k1 + 1):
        rng = M.get_rng("ch4_outage", sid, k)
        n = int(rng.poisson(C["CH4_FAULTY_OUTAGE_MULT"] if faulty else 1.0))
        if not n:
            continue
        offs = rng.random(n) * slot
        durs = np.minimum(C["CH4_OUTAGE_MEDIAN_H"] * np.exp(C["CH4_OUTAGE_SIGMA"]
                                                            * rng.standard_normal(n)),
                          C["CH4_OUTAGE_MAX_H"])
        for o, d in zip(offs, durs):
            s = EPOCH + pd.Timedelta(seconds=float(k * slot + o))
            e = s + pd.Timedelta(hours=float(d))
            if e > lo and s < hi:
                out.append((s, e))
    return out


def spec(kind, sid, t):
    return M.spectral(*M.series_params(kind, sid), t)


def generate(reg, lo, hi):
    """02e for one window, mirrored."""
    k0, k1 = int(np.ceil(lo.timestamp() / CAD)), int(np.ceil(hi.timestamp() / CAD))
    idx_all = np.arange(k0, k1)
    ts_all = pd.to_datetime(idx_all * CAD, unit="s")
    t_all = idx_all.astype(float) * CAD
    reg_t = {k: spec(k, "PERMIAN", t_all) for k in ("ch4_wind_speed", "ch4_wind_dir")}
    rg = spec("ch4_region", "PERMIAN", t_all / C["CH4_BASELINE_TIME_STRETCH"])
    local_h = np.mod(t_all / 3600.0 + C["CH4_LOCAL_UTC_OFFSET_H"], 24.0)
    base_common = (C["CH4_AMBIENT_REF_PPM"]
                   + C["CH4_DIURNAL_PPM"] * np.cos(2 * np.pi / 24.0
                                                  * (local_h - C["CH4_DIURNAL_PEAK_LOCAL_H"]))
                   + C["CH4_TREND_PPM_PER_YEAR"] * (t_all - EPOCH.timestamp()) / (365.25 * 86400)
                   + C["CH4_REGIONAL_PPM"] * rg)
    fac_cache, parts, removed = {}, [], 0
    for r in reg[reg["status"] != "Decommissioned"].itertuples():
        keep = ts_all >= r.install_ts
        for s, e in outages(r.sensor_id, r.status == "Faulty", lo, hi):
            removed += int((keep & (ts_all >= s) & (ts_all < e)).sum())
            keep &= ~((ts_all >= s) & (ts_all < e))
        idx, ts, t = idx_all[keep], ts_all[keep], t_all[keep]
        if not len(idx):
            continue
        u = M.row_uniforms(r.sensor_id, idx, 1)[:, 0]
        m = u >= C["CH4_DROPOUT_RATE"]
        idx, ts, t = idx[m], ts[m], t[m]
        sel = np.flatnonzero(keep)[m]
        if r.facility_id not in fac_cache:
            fac_cache[r.facility_id] = (
                spec("ch4_site", r.facility_id, t_all / C["CH4_BASELINE_TIME_STRETCH"]),
                spec("ch4_wind_speed", r.facility_id, t_all),
                spec("ch4_wind_dir", r.facility_id, t_all))
        st_f, ws_f, wd_f = fac_cache[r.facility_id]
        base = base_common[sel] + C["CH4_SITE_PPM"] * st_f[sel]
        state = state_of(r.equipment_sk, ts)
        z = spec("ch4_leak", r.sensor_id, t / C["CH4_LEAK_TIME_STRETCH"])
        onset = (C["CH4_LEAK_ONSET"] - C["CH4_RISK_SHIFT"] * (r.risk_rel - 1.0)
                 - np.array([C["CH4_STATE_SHIFT"][s] for s in state]))
        sup = np.isin(state, C["CH4_SUPPRESSED_STATES"])
        mult = np.array([C["CH4_STATE_LEAK_MULT"][s] for s in state])
        leak = np.where(sup, 0.0, r.sigma_ppm * mult * np.maximum(0.0, z - onset))
        bg = C["CH4_BG_PPM"] * np.clip((z + C["CH4_SPECTRAL_BOUND"])
                                       / (2 * C["CH4_SPECTRAL_BOUND"]), 0, 1)
        ch4 = np.round(base + bg + leak, 4)
        wsp = np.round(C["CH4_WIND_MEAN_MS"] * np.exp(
            C["CH4_WIND_LOG_SD"] * (0.6 * reg_t["ch4_wind_speed"][sel] + 0.8 * ws_f[sel])
            - C["CH4_WIND_LOG_SD"] ** 2 / 2), 2)
        wdr = np.mod(np.round(C["CH4_WIND_DIR_PREVAILING"] + C["CH4_WIND_DIR_SPREAD"]
                              * (0.6 * reg_t["ch4_wind_dir"][sel] + 0.8 * wd_f[sel]), 1), 360.0)
        parts.append(pd.DataFrame(dict(
            sensor_id=r.sensor_id, facility_id=r.facility_id, reading_ts=ts, state=state,
            ch4_ppm=ch4, baseline_ppm=np.round(base, 4),
            exceedance_flag=ch4 > r.exceedance_threshold_ppm, wind_speed_ms=wsp,
            wind_dir_deg=wdr,
            sensor_status=np.where(sup, "Calibration",
                                   "Fault" if r.status == "Faulty" else "OK"),
            risk_rel=r.risk_rel)))
    df = pd.concat(parts, ignore_index=True)
    return df.sort_values(["sensor_id", "reading_ts"]).reset_index(drop=True), removed


def slots(reg, lo, hi):
    n = 0
    for r in reg.itertuples():
        a = max(r.install_ts, lo)
        n += max(0, int(np.ceil(hi.timestamp() / CAD)) - int(np.ceil(a.timestamp() / CAD)))
    return n


def runs_of(df):
    f = df[df["exceedance_flag"]]
    gap = f.groupby("sensor_id")["reading_ts"].diff() != pd.Timedelta(seconds=CAD)
    return f.assign(run=gap.cumsum()).groupby("run").size()


if __name__ == "__main__":
    print("=" * 84)
    print("02e CH4 DETECTOR TELEMETRY")
    print("=" * 84)
    check_shared()
    reg = registry()
    print(f"  synthetic registry: {len(reg)} sensors; "
          + "  ".join(f"{k} {v}" for k, v in reg["status"].value_counts().items()))

    df, removed = generate(reg, START, END)
    n = len(df)
    all_slots = slots(reg, START, END)
    live_slots = slots(reg[reg["status"] != "Decommissioned"], START, END)
    proj = (live_slots - removed) * (1 - C["CH4_DROPOUT_RATE"])
    print(f"  rows {n:,}   projection {proj:,.0f} ({n / proj - 1:+.2%})")
    print("  synthetic state mix  " + "  ".join(
        f"{k} {v:.2%}" for k, v in df["state"].value_counts(normalize=True).items()))
    assert abs(n / proj - 1) <= 0.10

    # --- determinism ------------------------------------------------------------------------------
    df2, _ = generate(reg, START, END)
    pd.testing.assert_frame_equal(df, df2, check_exact=True)
    daily = pd.concat([generate(reg, START + pd.Timedelta(days=d),
                                START + pd.Timedelta(days=d + 1))[0] for d in range(30)])
    daily = daily.sort_values(["sensor_id", "reading_ts"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(df, daily, check_exact=True)
    print("\nOK  two runs identical; the 30-day backfill equals 30 one-day runs, bitwise")

    # --- values -------------------------------------------------------------------------------------
    lo_b, hi_b = C["CH4_BASELINE_BOUNDS"]
    assert df["baseline_ppm"].between(lo_b, hi_b).all() and (df["ch4_ppm"] >= df["baseline_ppm"]).all()
    assert (df["wind_speed_ms"] >= 0).all() and df["wind_dir_deg"].between(0, 359.999).all()
    fac_sd = df.groupby("reading_ts")["wind_speed_ms"].std().median()
    print(f"OK  baseline {df['baseline_ppm'].min():.3f}..{df['baseline_ppm'].max():.3f} ppm; "
          f"ch4 >= baseline; wind varies across facilities (median cross-basin sd "
          f"{fac_sd:.2f} m/s at an instant)")

    # --- exceedances ---------------------------------------------------------------------------------
    rate = df["exceedance_flag"].mean()
    lo_e, hi_e = C["CH4_EXCEEDANCE_BAND"]
    day_rate = df.groupby(df["reading_ts"].dt.date)["exceedance_flag"].mean()
    per = df.groupby("sensor_id")["exceedance_flag"].sum().sort_values(ascending=False)
    live = int((reg["status"] != "Decommissioned").sum())
    print(f"\n  exceedance rate {rate:.2%}  (band {lo_e:.1%}-{hi_e:.1%}); by day "
          f"{day_rate.min():.2%}..{day_rate.max():.2%}")
    print(f"  on {int((per > 0).sum())} of {live} sensors; top 10% hold "
          f"{per.head(live // 10).sum() / per.sum():.0%}")
    by_state = df.groupby("state")["exceedance_flag"].mean()
    print("  by state   " + "  ".join(f"{k} {v:.2%}" for k, v in by_state.items()))
    q = pd.cut(df["risk_rel"], [0, 0.8, 1.0, 1.2, 9], labels=["<0.8", "0.8-1", "1-1.2", ">=1.2"])
    by_risk = df.groupby(q, observed=True)["exceedance_flag"].mean()
    print("  by risk    " + "  ".join(f"{k} {v:.2%}" for k, v in by_risk.items()))
    assert lo_e <= rate <= hi_e, f"exceedance rate {rate:.2%} outside the band"
    assert by_state.get("Maintenance", 0) == 0, "an exceedance during Maintenance"
    assert by_state["Standby"] > by_state["Running"], "Standby does not raise the rate"
    assert by_risk.is_monotonic_increasing, "exceedance rate does not rise with relative risk"
    rl = runs_of(df)
    print(f"  runs: {len(rl):,}, median {rl.median():.0f} h, p90 {rl.quantile(.9):.0f} h, "
          f"single-reading {np.mean(rl == 1):.0%}")
    assert rl.median() >= 3
    print("OK  rate in band; concentrated by risk and state; none in Maintenance; runs cluster")

    # --- autocorrelation ------------------------------------------------------------------------------
    ac = []
    for sid, g in df.groupby("sensor_id"):
        ok = g["reading_ts"].diff() == pd.Timedelta(seconds=CAD)
        v = g["ch4_ppm"].values
        if ok.sum() >= 20:
            ac.append(np.corrcoef(v[1:][ok.values[1:]], v[:-1][ok.values[1:]])[0, 1])
    ac = np.array(ac)
    assert (ac > 0.7).all(), f"min lag-1 autocorrelation {ac.min():.3f}"
    print(f"OK  lag-1 autocorrelation on all {len(ac)} sensors: min {ac.min():.3f}, "
          f"median {np.median(ac):.3f}")

    # --- offline ----------------------------------------------------------------------------------------
    dark = all_slots - live_slots
    off = (dark + removed) / all_slots
    lo_o, hi_o = C["CH4_OFFLINE_BAND"]
    assert lo_o <= off <= hi_o, f"offline share {off:.2%} outside the band"
    print(f"OK  offline share {off:.2%} (band {lo_o:.0%}-{hi_o:.0%})")
    for f in (0.15, 0.35, 0.55, 0.75, 0.95):
        p = (START + (END - START) * f).floor("h")
        seen = set(df[(df["reading_ts"] <= p) & (df["reading_ts"] > p - pd.Timedelta(hours=8))]
                   ["sensor_id"])
        ex = reg[reg["install_ts"] <= p - pd.Timedelta(hours=8)]
        o = ex[~ex["sensor_id"].isin(seen)]
        nd = int((o["status"] == "Decommissioned").sum())
        print(f"    {p:%Y-%m-%d %H:%M}  8-hour KPI {len(o)} offline  ({nd} decommissioned, "
              f"{len(o) - nd} in an outage)")
        assert len(o) > 0
    print("OK  the 8-hour offline KPI is non-zero at every sampled instant")
