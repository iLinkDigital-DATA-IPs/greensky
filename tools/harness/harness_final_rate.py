"""Realised alarm rate and type mix with the ACTUAL new TAG_TEMPLATES from the repo.

Loads 01_topology_config as written, uses 02a's real state machine and 02b's value model at
the new TELEMETRY_PROCESS_SD_FRACTION, and reports what 02d will see.
"""
import io, re, contextlib, pathlib
from datetime import date, timedelta
import numpy as np
import pandas as pd
import harness_model as M
from harness_alarm_rate import simulate

REPO = pathlib.Path("C:/Users/prutha.annadate/Desktop/Green Sky/greensky_v2/greensky/"
                    "Methane Emissions/Accelerator/Planetary Computer/Varon et al Approach")
MARK = re.compile(r"^# (CELL|MARKDOWN|METADATA) \*{20,}$")


def load_config():
    p = REPO / "notebooks/01_topology/01_topology_config.Notebook/notebook-content.py"
    code, kind = [], None
    for ln in p.read_text(encoding="utf-8").splitlines():
        m = MARK.match(ln)
        if m:
            kind = m.group(1); continue
        if kind == "CELL":
            code.append("pass  # " + ln if ln.startswith("%") else ln)
    g = {"np": np, "pd": pd, "date": date, "timedelta": timedelta,
         "BBOX": {"min_lat": 30.5, "max_lat": 33.5, "min_lon": -105.0, "max_lon": -101.0},
         "__name__": "__main__"}
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile("\n".join(code), "01_topology_config", "exec"), g)
    return g


CFG = load_config()
TAG_TEMPLATES = CFG["TAG_TEMPLATES"]
FIELDS = CFG["TAG_TEMPLATE_FIELDS"]
M.PROCESS_SD_FRACTION = CFG["TELEMETRY_PROCESS_SD_FRACTION"]

AS_OF = pd.Timestamp("2026-09-15")
WIN_START, WIN_END = AS_OF - pd.Timedelta(days=30), AS_OF
DEBOUNCE, FROZEN_SAMPLES = 3, 8
SUPPRESS = {"Down", "Maintenance", "Startup", "Shutdown"}
N_PER_TYPE, HOT_SHARE = 10, 0.25
INSTRUMENTED, FACILITIES, N_TAGS = 662, 150, 3965
MIX = {"Compressor": 0.34, "Separator": 0.26, "Storage Tank": 0.17,
       "Metering Station": 0.12, "Flare": 0.07, "Pump": 0.04}
TRIP_KINDS = {"HiHi", "LoLo"}

if __name__ == "__main__":
    print("=" * 88)
    print(f"REALISED ALARM RATE -- new limits, PROCESS_SD_FRACTION = {M.PROCESS_SD_FRACTION}")
    print("=" * 88)

    per_type = {et: {300: 0.0, 900: 0.0} for et in TAG_TEMPLATES}
    kinds, crit_mix = {}, {"Critical": 0, "other": 0}
    env_num = env_den = 0
    ac_vals = []

    for et, tmpls in TAG_TEMPLATES.items():
        for a in range(N_PER_TYPE):
            eq_id, fac_id = f"GS-{a+1:04d}-E{a:03d}", f"GS-{a+1:04d}"
            age = 1.5 + 4.0 * float(M.get_rng("age", eq_id).random())
            tr = simulate(et, eq_id, age, AS_OF - pd.Timedelta(days=age * 365),
                          AS_OF - pd.Timedelta(days=120), WIN_END)
            for cad in (300, 900):
                k0, k1 = int(WIN_START.timestamp()) // cad, int(WIN_END.timestamp()) // cad
                idx = np.arange(k0, k1)
                ts = pd.DatetimeIndex(pd.Timestamp("1970-01-01")
                                      + pd.to_timedelta(idx * cad, unit="s"))
                i = np.clip(np.searchsorted(tr["start_ts"].values, ts.values, "right") - 1,
                            0, len(tr) - 1)
                st = tr["state"].values[i]
                s0, s1 = tr["start_ts"].values[i], tr["end_ts"].values[i]
                span = (s1 - s0) / np.timedelta64(1, "s")
                phi = (ts.values - s0) / np.timedelta64(1, "s") / np.maximum(span, 1)
                la, lp = M.series_params("asset", eq_id)
                fa, fp = M.series_params("facility", fac_id)
                tsec = idx.astype(float) * cad
                load, fac = M.spectral(la, lp, tsec), M.spectral(fa, fp, tsec)
                for j, t in enumerate(tmpls):
                    tg = dict(zip(FIELDS, t)); tg["tag_id"] = f"{fac_id}.A1.XT-{101+j}"
                    v, _ = M.value_model(tg, ts, st, phi, load, fac, idx,
                                         AS_OF - pd.Timedelta(days=90))
                    run = st == "Running"
                    H = .5 * (tg["normal_max"] - tg["normal_min"])
                    if H > 0 and run.any():
                        lo = tg["normal_min"] - 3 * tg["noise_sigma"]
                        hi = tg["normal_max"] + 3 * tg["noise_sigma"]
                        env_num += int(((v[run] >= lo) & (v[run] <= hi)).sum())
                        env_den += int(run.sum())
                        # lag-1 autocorrelation on contiguous Running rows
                        pos = np.flatnonzero(run)
                        runs = np.split(pos, np.where(np.diff(pos) != 1)[0] + 1)
                        num = den = 0.0
                        for r in runs:
                            if len(r) < 50:
                                continue
                            x = v[r] - v[r].mean()
                            num += float((x[:-1] * x[1:]).sum()); den += float((x * x).sum())
                        if den:
                            ac_vals.append(num / den)
                    if H == 0:
                        continue
                    elig = ~np.isin(st, list(SUPPRESS))
                    if tg["measurement_type"] in ("flow", "rpm"):
                        elig &= (st != "Standby")
                    for kind, lim, up in (("Hi", tg["alarm_hi"], 1), ("HiHi", tg["alarm_hihi"], 1),
                                          ("Lo", tg["alarm_lo"], 0), ("LoLo", tg["alarm_lolo"], 0)):
                        if lim is None:
                            continue
                        br = ((v > lim) if up else (v < lim)) & elig
                        d = np.diff(np.concatenate(([0], br.view(np.int8), [0])))
                        r = np.flatnonzero(d == -1) - np.flatnonzero(d == 1)
                        n = int((r >= DEBOUNCE).sum())
                        per_type[et][cad] += n
                        kinds[kind] = kinds.get(kind, 0) + n

    weighted = sum((HOT_SHARE * per_type[et][300] / N_PER_TYPE
                    + (1 - HOT_SHARE) * per_type[et][900] / N_PER_TYPE) * w
                   for et, w in MIX.items())
    per_fac = weighted * INSTRUMENTED / FACILITIES
    total = sum(kinds.values())

    print(f"\n  alarms per instrumented asset per 30 days : {weighted:.2f}")
    print(f"  ALARMS PER FACILITY-MONTH                : {per_fac:.1f}   (band 10 - 60)")
    print(f"  verdict                                  : "
          f"{'IN BAND' if 10 <= per_fac <= 60 else 'OUT OF BAND'}")

    print(f"\n  alarm type mix ({total:,} alarms in the sample):")
    print(f"    {'type':<10}{'count':>8}{'share':>9}{'priority':>11}")
    for k in ("HiHi", "Hi", "Lo", "LoLo"):
        n = kinds.get(k, 0)
        pr = "P1" if k in TRIP_KINDS else "P2 / P3"
        print(f"    {k:<10}{n:>8,}{n/max(total,1):>9.1%}{pr:>11}")
    p1 = sum(kinds.get(k, 0) for k in TRIP_KINDS)
    print(f"    {'-> P1':<10}{p1:>8,}{p1/max(total,1):>9.1%}")
    print(f"    {'-> P2/P3':<10}{total-p1:>8,}{(total-p1)/max(total,1):>9.1%}")

    print(f"\n  02b regression at the new process width:")
    print(f"    envelope: {env_num/max(env_den,1):.3%} of Running readings inside "
          f"band +/- 3 sigma_noise   (02b asserts >= 99%)")
    print(f"    lag-1 autocorrelation on Running rows: min {min(ac_vals):.3f}  "
          f"median {np.median(ac_vals):.3f}   (02b asserts > 0.70)")
    assert env_num / max(env_den, 1) >= 0.99, "02b's envelope check would now fail"
    assert min(ac_vals) > 0.70, "02b's autocorrelation check would now fail"
    print("    both 02b checks still pass")
