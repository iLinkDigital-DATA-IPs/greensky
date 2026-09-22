"""Are 02d's surrogate keys unique at the grain the table actually has?

The gap this closes: harness_sessionise.py exercises the debounce machine for ONE
(tag, alarm_type) stream at a time, so it never sees two alarm types on the same tag raising
at the same reading -- which is exactly when alarm_sk collides if alarm_type is not in the
key. Uniqueness is checkable in pandas with no JVM; nothing here needs Spark.

A simultaneous crossing is not a corner case in this estate. The dominant alarm source is a
Standby interval, where 02b relaxes a compressor's discharge pressure from ~1000 psig to
0.45 x centre in one sample -- straight past alarm_lo AND alarm_lolo between one reading and
the next, so both machines reach the debounce count on the same reading.
"""
import hashlib
import numpy as np
import pandas as pd
import harness_model as M
from harness_alarms import F
from harness_alarm_rate import simulate
from harness_final_rate import load_config

AS_OF = pd.Timestamp("2026-09-15")
WIN_START, WIN_END = AS_OF - pd.Timedelta(days=30), AS_OF
DEBOUNCE = 3
SUPPRESS = {"Down", "Maintenance", "Startup", "Shutdown"}
CFG = load_config()
ET_ORD = {e: n + 1 for n, e in enumerate(sorted(load_config()["TAG_TEMPLATES"]))}
TAG_TEMPLATES, FIELDS = CFG["TAG_TEMPLATES"], CFG["TAG_TEMPLATE_FIELDS"]
M.PROCESS_SD_FRACTION = CFG["TELEMETRY_PROCESS_SD_FRACTION"]
SEED = CFG["TOPOLOGY_SEED"]
MASK = 0x7FFF_FFFF_FFFF_FFFF


def stable_key(*parts):
    h = hashlib.sha256("|".join(map(str, (SEED, *parts))).encode()).hexdigest()
    return int(h[:16], 16) & MASK


def runs_of(flag, debounce):
    """Start indices of debounced raises in a boolean breach series."""
    d = np.diff(np.concatenate(([0], flag.view(np.int8), [0])))
    starts, ends = np.flatnonzero(d == 1), np.flatnonzero(d == -1)
    return [s + debounce - 1 for s, e in zip(starts, ends) if e - s >= debounce]


def derive_alarms(n_assets=8):
    """Every (tag, alarm_type) alarm across a sample of assets, as the notebook would."""
    out = []
    for et, tmpls in TAG_TEMPLATES.items():
        for a in range(n_assets):
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
                    tg = dict(zip(FIELDS, t))
                    # tag_id must be unique across equipment types too: in the real
                    # estate 01d asserts it, and a synthetic id that collides would
                    # manufacture duplicates this check would wrongly blame on the key
                    tg["tag_id"] = f"{fac_id}.A{ET_ORD[et]}.XT-{101 + j}.{cad}"
                    if tg["normal_max"] == tg["normal_min"]:
                        continue
                    v, _ = M.value_model(tg, ts, st, phi, load, fac, idx,
                                         AS_OF - pd.Timedelta(days=90))
                    elig = ~np.isin(st, list(SUPPRESS))
                    if tg["measurement_type"] in ("flow", "rpm"):
                        elig &= (st != "Standby")
                    for kind, lim, up in (("Hi", tg["alarm_hi"], 1),
                                          ("HiHi", tg["alarm_hihi"], 1),
                                          ("Lo", tg["alarm_lo"], 0),
                                          ("LoLo", tg["alarm_lolo"], 0)):
                        if lim is None:
                            continue
                        br = ((v > lim) if up else (v < lim)) & elig
                        for r in runs_of(br, DEBOUNCE):
                            out.append({"tag_id": tg["tag_id"], "alarm_type": kind,
                                        "raised_ts": str(ts[r])})
    return pd.DataFrame(out)


if __name__ == "__main__":
    al = derive_alarms()
    print("=" * 84)
    print("ALARM KEY UNIQUENESS")
    print("=" * 84)
    print(f"  alarms derived                       {len(al):,}")

    # --- how often do two alarm types raise on the same reading? ----------------------------
    g = al.groupby(["tag_id", "raised_ts"]).size()
    clash = g[g > 1]
    print(f"  (tag, raised_ts) pairs with >1 type  {len(clash):,}"
          f"   covering {int(clash.sum()):,} alarms")
    if len(clash):
        ex = al.merge(clash.rename("n").reset_index(), on=["tag_id", "raised_ts"]).head(6)
        print("\n  examples:")
        for r in ex.itertuples():
            print(f"    {r.tag_id:<26}{r.raised_ts}  {r.alarm_type}")

    # --- old key: (alarm, tag_id, raised_ts) ------------------------------------------------
    al["sk_old"] = [stable_key("alarm", t, r)
                    for t, r in zip(al["tag_id"], al["raised_ts"])]
    dup_old = int(len(al) - al["sk_old"].nunique())
    # --- new key: (alarm, tag_id, alarm_type, raised_ts) ------------------------------------
    al["sk_new"] = [stable_key("alarm", t, k, r)
                    for t, k, r in zip(al["tag_id"], al["alarm_type"], al["raised_ts"])]
    dup_new = int(len(al) - al["sk_new"].nunique())

    print(f"\n  {'key':<44}{'distinct':>10}{'collisions':>12}")
    print("  " + "-" * 66)
    print(f"  {'stable_key(alarm, tag_id, raised_ts)':<44}"
          f"{al['sk_old'].nunique():>10,}{dup_old:>12,}")
    print(f"  {'stable_key(alarm, tag_id, alarm_type, raised_ts)':<44}"
          f"{al['sk_new'].nunique():>10,}{dup_new:>12,}")

    assert dup_old > 0, ("no collision reproduced -- this harness is not generating "
                         "simultaneous crossings and would not have caught the defect")
    assert dup_new == 0, "alarm_type in the key does not make alarm_sk unique"
    print(f"\nOK  the old key collides ({dup_old:,} alarms lost), the new key does not")
    print("    the table's grain is (tag, alarm_type, raised_ts) and the key now matches it")

    # --- event_sk in fact_sensor_status_event ------------------------------------------------
    # Natural grain is (tag_id, event_ts, to_status). Checked rather than argued.
    print()
    print("=" * 84)
    print("SENSOR STATUS KEY UNIQUENESS")
    print("=" * 84)

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

    rng = np.random.default_rng(SEED)
    N_TAGS = 3000
    st_mix = rng.choice(["Active", "Faulty", "Decommissioned"], N_TAGS, p=[.97, .02, .01])
    cads = np.where(rng.random(N_TAGS) < .25, 300, 900)
    rows = []
    for i in range(N_TAGS):
        tid = f"GS-{i//26+1:04d}.A{i%6+1}.PT-{101+i}"      # unique, as 01d asserts
        if st_mix[i] == "Decommissioned":
            rows.append((tid, str(WIN_START), "Decommissioned")); continue
        c = int(cads[i])
        k0, k1 = int(WIN_START.timestamp()) // c, int(WIN_END.timestamp()) // c
        idx = np.arange(k0, k1)
        ts = pd.to_datetime(idx * c, unit="s")
        alive = np.ones(len(idx), bool)
        for s, e in merge(M.outage_intervals(tid, st_mix[i], WIN_START, WIN_END)):
            alive &= ~((ts >= s) & (ts < e))
        alive &= M.row_uniforms(tid, idx, 6)[:, 2] >= M.DROPOUT_RATE
        present = np.flatnonzero(alive)
        if len(present) < 2:
            continue
        pts = ts[present]
        for j in np.flatnonzero((pts[1:] - pts[:-1]).total_seconds() > 2 * c):
            rows.append((tid, str(pts[j] + pd.Timedelta(seconds=c)),
                         "Faulty" if st_mix[i] == "Faulty" else "Offline"))
            rows.append((tid, str(pts[j + 1]), "Online"))

    ev = pd.DataFrame(rows, columns=["tag_id", "event_ts", "to_status"])
    ev["event_sk"] = [stable_key("sensor_event", t, e, s) for t, e, s in rows]
    nat = int(ev.duplicated(subset=["tag_id", "event_ts", "to_status"]).sum())
    sur = int(ev.duplicated(subset=["event_sk"]).sum())
    both = int((ev.groupby(["tag_id", "event_ts"]).size() > 1).sum())
    print(f"  status events                       {len(ev):,}")
    print(f"  duplicate natural key (t, ts, st)   {nat}")
    print(f"  duplicate event_sk                  {sur}")
    print(f"  (tag, event_ts) carrying 2 events   {both}")
    assert nat == 0 and sur == 0, "event_sk is not unique"
    print("\nOK  event_sk is unique at (tag_id, event_ts, to_status)")
    print("    A tag cannot leave and return at one instant by construction: an Offline")
    print("    event is stamped at prev_reading + cadence, an Online event at a reading_ts,")
    print("    and if a reading existed at prev + cadence there would be no gap at all.")
    print("    to_status stays in the key because it is the natural grain, not because it")
    print("    is currently load-bearing -- it costs nothing and the grain may widen.")
