"""Run the offline harness checks for 02b_gen_scada_telemetry."""
import numpy as np
import pandas as pd
import harness_model as M

WIN_END = pd.Timestamp("2026-09-15")
WIN_START = WIN_END - pd.Timedelta(days=30)

# --- the compressor template from 01_topology_config, verbatim -------------------------
COMP = [
    ("suction_pressure",   "pressure",       "psig",  40.0,  120.0,  20.0,  175.0,  0.1,   0.8,   1.5),
    ("discharge_pressure", "pressure",       "psig", 800.0, 1200.0, 600.0, 1450.0,  1.0,   5.0,  12.0),
    ("suction_temp",       "temperature",    "degF",  60.0,  110.0,  20.0,  150.0,  0.1,   0.5,   1.0),
    ("discharge_temp",     "temperature",    "degF", 180.0,  280.0, 120.0,  350.0,  0.1,   1.5,   2.0),
    ("rpm",                "rpm",            "rpm",  900.0, 1200.0, 700.0, 1320.0,  1.0,   4.0,   3.0),
    ("vibration",          "vibration",      "in/s",   0.05,   0.25, None,    0.60, 0.001, 0.010, 0.020),
    ("flow",               "flow",           "mscfd",1500.0, 4500.0, 400.0, 6000.0, 1.0,  35.0,  60.0),
    ("seal_gas_pressure",  "pressure",       "psig",  45.0,   90.0,  25.0,  130.0,  0.1,   0.6,   1.2),
    ("air_fuel_ratio",     "air_fuel_ratio", "ratio",  14.0,   17.5,  12.0,   21.0,  0.01,  0.08,  0.15),
]
FIELDS = ("tag_name", "measurement_type", "uom", "normal_min", "normal_max",
          "alarm_lolo", "alarm_hihi", "resolution", "noise_sigma", "drift_per_year")

EQ_ID, FAC_ID = "GS-0007-E003", "GS-0007"
TAGS = []
for i, t in enumerate(COMP):
    d = dict(zip(FIELDS, t))
    d["tag_id"] = f"{FAC_ID}.A2.XT-{101 + i}"
    TAGS.append(d)


def build_state_trace(cadence_s, start, end):
    """A plausible 02a-style chain: Running with PM and trip excursions."""
    rows, ts, state, rng = [], start, "Running", M.get_rng("state_trace", EQ_ID)
    chain = {"Running": ("Shutdown", 26 * 24), "Shutdown": ("Maintenance", 0.6),
             "Maintenance": ("Startup", 30.0), "Startup": ("Running", 0.6),
             "Down": ("Maintenance", 8.0), "Standby": ("Startup", 40.0)}
    while ts < end:
        if state == "Running":
            dur = float(rng.exponential(5.0 * 24))
            nxt = str(rng.choice(["Shutdown", "Down", "Standby"], p=[0.5, 0.35, 0.15]))
        else:
            nxt, dur = chain[state]
            dur = float(dur) * float(rng.uniform(0.6, 1.4))
        rows.append((state, ts, ts + pd.Timedelta(hours=dur)))
        ts = ts + pd.Timedelta(hours=dur)
        state = nxt
    return pd.DataFrame(rows, columns=["state", "start_ts", "end_ts"])


def resolve(trace, ts):
    """State name and ramp fraction per reading."""
    i = np.searchsorted(trace["start_ts"].values, ts.values, side="right") - 1
    i = np.clip(i, 0, len(trace) - 1)
    st = trace["state"].values[i]
    s0 = trace["start_ts"].values[i]
    s1 = trace["end_ts"].values[i]
    span = (s1 - s0) / np.timedelta64(1, "s")
    phi = np.where(span > 0, (ts.values - s0) / np.timedelta64(1, "s") / np.maximum(span, 1), 0.0)
    return st, phi


def generate(cadence_s, start, end, trace):
    """Generate the 9 compressor tags over [start, end) on a fixed global slot grid."""
    k0 = int(np.ceil((start - pd.Timestamp("1970-01-01")).total_seconds() / cadence_s))
    k1 = int((end - pd.Timestamp("1970-01-01")).total_seconds() // cadence_s)
    idx = np.arange(k0, k1)
    ts = pd.DatetimeIndex(pd.Timestamp("1970-01-01") + pd.to_timedelta(idx * cadence_s, unit="s"))
    t_sec = idx.astype(float) * cadence_s

    la, lp = M.series_params("asset", EQ_ID)
    fa, fp = M.series_params("facility", FAC_ID)
    load = M.spectral(la, lp, t_sec)
    fac = M.spectral(fa, fp, t_sec)

    st, phi = resolve(trace, ts)
    drift_ref = pd.Timestamp("2026-06-01")
    out = {}
    for tag in TAGS:
        v, u = M.value_model(tag, ts, st, phi, load, fac, idx, drift_ref)
        out[tag["tag_name"]] = v
    return pd.DataFrame(out, index=ts), idx, st, load, fac


def lag1(x):
    x = np.asarray(x, dtype=float)
    if x.std() == 0:
        return np.nan
    return float(np.corrcoef(x[:-1], x[1:])[0, 1])


if __name__ == "__main__":
    print("=" * 78)
    print("OFFLINE HARNESS -- 02b_gen_scada_telemetry deterministic core")
    print("=" * 78)

    # --- 1. hash stream quality -----------------------------------------------------------
    idx = np.arange(0, 20000)
    u = M.row_uniforms(TAGS[0]["tag_id"], idx)
    print("\n1. sha256-derived uniform stream (20,000 draws, 6 chunks)")
    print(f"   mean {u.mean():.5f} (expect 0.5)   sd {u.std():.5f} (expect {1/np.sqrt(12):.5f})")
    print(f"   lag-1 autocorr of chunk 0 across idx: {lag1(u[:, 0]):+.5f} (expect ~0)")
    cc = np.corrcoef(u.T)
    print(f"   max |corr| between chunks: {np.abs(cc - np.eye(6)).max():.5f} (expect ~0)")
    z = np.sqrt(-2 * np.log(u[:, 0])) * np.cos(2 * np.pi * u[:, 1])
    print(f"   Box-Muller z: mean {z.mean():+.4f} sd {z.std():.4f} (expect 0, 1)")

    # --- 2. spectral series -----------------------------------------------------------------
    print("\n2. spectral series (unit variance, lag-1 autocorr by cadence)")
    for cad in (300, 900):
        r1, sds = [], []
        for n in range(40):
            a, p = M.series_params("tag", f"probe-{n}")
            t = np.arange(0, 30 * 86400, cad, dtype=float)
            s = M.spectral(a, p, t)
            r1.append(lag1(s)); sds.append(s.std())
        print(f"   cadence {cad:>4}s   lag-1 min {min(r1):.4f} mean {np.mean(r1):.4f}   "
              f"sd of series {np.mean(sds):.3f} (expect ~1.0)")

    # --- 3. full value model ----------------------------------------------------------------
    print("\n3. full value model, one compressor, 30 days")
    for cad, label in ((300, "hot 300s"), (900, "standard 900s")):
        trace = build_state_trace(cad, WIN_START - pd.Timedelta(days=60), WIN_END)
        df, idxs, st, load, fac = generate(cad, WIN_START, WIN_END, trace)
        print(f"\n   --- {label}: {len(df):,} slots/tag ---")
        print(f"   {'tag':<20}{'lag-1':>8}{'sd':>12}{'mean':>12}{'min':>12}{'max':>12}")
        for c in df.columns:
            print(f"   {c:<20}{lag1(df[c]):>8.3f}{df[c].std():>12.3f}{df[c].mean():>12.2f}"
                  f"{df[c].min():>12.2f}{df[c].max():>12.2f}")
        if cad == 900:
            print("\n   cross-tag correlation on the same asset:")
            corr = df.corr()
            pairs = [("rpm", "flow"), ("discharge_pressure", "suction_pressure"),
                     ("discharge_temp", "flow"), ("vibration", "rpm"),
                     ("air_fuel_ratio", "flow"), ("seal_gas_pressure", "discharge_pressure")]
            for a, b in pairs:
                print(f"     {a:<20} {b:<20} {corr.loc[a, b]:+.3f}")
            off = corr.values[~np.eye(len(corr), dtype=bool)]
            print(f"     pairs with |corr| > 0.5: {(np.abs(off) > 0.5).sum() // 2} of "
                  f"{len(corr)*(len(corr)-1)//2}")
            print("\n   state composition of the window:")
            for s, n in pd.Series(st).value_counts().items():
                print(f"     {s:<14}{n:>8,}  {n/len(st):>7.2%}")
            print("\n   Down/Maintenance-state readings (should read at rest):")
            stopped = np.isin(st, ["Down", "Maintenance"])
            if stopped.any():
                for c in ("flow", "rpm", "discharge_temp", "discharge_pressure", "vibration"):
                    sub = df[c].values[stopped]
                    print(f"     {c:<20} mean {sub.mean():>10.2f}  max {sub.max():>10.2f}")

    # --- 4. envelope compliance on Running rows ---------------------------------------------
    print("\n4. Running-state values inside [normal_min - 3s, normal_max + 3s]")
    trace = build_state_trace(900, WIN_START - pd.Timedelta(days=60), WIN_END)
    df, idxs, st, load, fac = generate(900, WIN_START, WIN_END, trace)
    run = st == "Running"
    worst = 1.0
    for tag in TAGS:
        v = df[tag["tag_name"]].values[run]
        lo = tag["normal_min"] - 3 * tag["noise_sigma"]
        hi = tag["normal_max"] + 3 * tag["noise_sigma"]
        share = float(((v >= lo) & (v <= hi)).mean())
        worst = min(worst, share)
        flag = "" if share >= 0.99 else "   <-- under 99%"
        print(f"   {tag['tag_name']:<20}{share:>9.4%}{flag}")
    print(f"   worst tag: {worst:.4%}  (threshold 99%)")

    # --- 5. window independence: backfill vs incremental ------------------------------------
    print("\n5. window independence (the backfill == incremental proof)")
    full, fidx, _, _, _ = generate(900, WIN_START, WIN_END, trace)
    chunks = []
    d = WIN_START
    while d < WIN_END:
        nxt = d + pd.Timedelta(days=1)
        part, _, _, _, _ = generate(900, d, nxt, trace)
        chunks.append(part)
        d = nxt
    inc = pd.concat(chunks)
    same_index = full.index.equals(inc.index)
    identical = bool((full.values == inc.values).all()) and same_index
    print(f"   30-day sweep rows {len(full):,}   30 x 1-day rows {len(inc):,}")
    print(f"   index identical: {same_index}   values bit-identical: {identical}")
    assert identical, "window dependence detected"

    # --- 6. determinism ---------------------------------------------------------------------
    again, _, _, _, _ = generate(900, WIN_START, WIN_END, trace)
    print(f"\n6. same-seed rerun bit-identical: {bool((full.values == again.values).all())}")

    # --- 7. outage / freeze model ------------------------------------------------------------
    print("\n7. gap model realised rates")
    rng = np.random.default_rng(7)
    n_tags = 1200
    statuses = rng.choice(["Active", "Faulty", "Decommissioned"], size=n_tags, p=[0.97, 0.02, 0.01])
    win_s = (WIN_END - WIN_START).total_seconds()
    offline_s, n_out, frozen = 0.0, 0, 0
    for i in range(n_tags):
        tid = f"probe.tag-{i}"
        if statuses[i] == "Decommissioned":
            offline_s += win_s
            continue
        for s, e in M.outage_intervals(tid, statuses[i], WIN_START, WIN_END):
            lo, hi = max(s, WIN_START), min(e, WIN_END)
            if hi > lo:
                offline_s += (hi - lo).total_seconds()
                n_out += 1
        if M.freeze_interval(tid, WIN_START, WIN_END):
            frozen += 1
    print(f"   tags probed              {n_tags:,}")
    print(f"   outage intervals in win  {n_out:,}  ({n_out/n_tags:.3f} per tag per 30 days)")
    print(f"   offline share of tag-time{offline_s/(n_tags*win_s):>9.3%}  (target band 1-3%)")
    print(f"   frozen tags              {frozen:,}  ({frozen/n_tags:.3%}, target 0.300%)")
    u = M.row_uniforms("probe.tag-0", np.arange(200000), n=6)
    print(f"   dropout rate realised    {(u[:,2] < M.DROPOUT_RATE).mean():.4%} (target 0.2000%)")
    print(f"   bad-quality rate         {(u[:,3] < M.BAD_RATE).mean():.4%} (target 0.5000%)")
    print(f"   uncertain rate           {(u[:,4] < M.UNCERTAIN_RATE).mean():.4%} (target 1.0000%)")

    # --- 8. golden hash vectors for the notebook --------------------------------------------
    print("\n8. golden hash vectors (embed in the notebook, asserted against Spark sha2)")
    import hashlib
    for tid, k in (("GS-0001.A1.PT-101", 0), ("GS-0001.A1.PT-101", 1782432),
                   ("GS-0042.A3.FT-104", 5347296)):
        h = hashlib.sha256(f"{M.TOPOLOGY_SEED}|{tid}|{k}".encode()).hexdigest()
        print(f'   ("{tid}", {k}, "{h[:16]}"),')
    print("\nall harness assertions passed")
