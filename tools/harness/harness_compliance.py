"""03c's compliance events: the notebook's own model cell, run against 01_topology_config.

1. Static. fact_emission_episode is named in 03c's code only inside GROUND_TRUTH_TABLES, and
   every table read goes through read_input() (or the write's own row-count check).

2. Determinism, on a synthetic upstream -- a plume catalogue shaped like the real one (49
   plumes a month, median 19.7 t/h, ~86% attributed), CH4 detector flags, and flare alarms --
   with every upstream table cut to what it would hold at each run's horizon:
     - two backfills identical
     - a 30-day backfill equals 1 backfill day + 29 incremental days, bitwise, each writing
       through the notebook's own merge_for_write() and replaceWhere semantics
     - rerunning the last day changes nothing
   Negative controls, so the checks can fail: a case plan re-drawn per run, and a repetition
   rule that looks FORWARD as well as back (a detection escalated by one that has not happened
   yet), each break backfill = incremental.

3. Steady state over 300 days, far longer than the ~60 days state cases need to level: open
   non-stalled cases inside [0.4, 2.5] x lambda x T and not trending; stall shares near
   their targets; every violation a state case, fined inside its regulation's range.
   Counts here are synthetic; the real ones are what 03c prints in Fabric.
"""
import ast
import contextlib
import io
import numpy as np
import pandas as pd
import shared_defs as S
import harness_episodes as HE

NB_03C = S.NB / "03_enterprise/03c_gen_compliance.Notebook/notebook-content.py"
MODEL_MARKER = "# ---- 03c compliance model (pure"
NB_00 = S.NB / "00_prereqs/00_config.Notebook/notebook-content.py"
DAY = pd.Timedelta(days=1)


def load_model():
    g = HE.load_config()
    # 03c reads CONFIG["persistence_match_radius_km"] (06's site radius); 00_config's own dict,
    # not a copy, so the harness and the notebook use the same radius
    g["CONFIG"] = ast.literal_eval(S.extract(NB_00, ["CONFIG"])["CONFIG"].split("=", 1)[1])
    cell = [c for c in HE.code_cells(NB_03C) if MODEL_MARKER in c]
    assert len(cell) == 1, f"expected one 03c model cell, found {len(cell)}"
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(cell[0], "03c_model_cell", "exec"), g)
    return g


def check_static():
    code = "\n".join(("pass  # " + ln) if ln.lstrip().startswith("%") else ln
                     for c in HE.code_cells(NB_03C) for ln in c.split("\n"))
    tree = ast.parse(code)
    refusal = [n for n in tree.body if isinstance(n, ast.Assign)
               and any(isinstance(t, ast.Name) and t.id == "GROUND_TRUTH_TABLES" for t in n.targets)]
    assert len(refusal) == 1 and ast.literal_eval(refusal[0].value) == ("fact_emission_episode",)
    inside = {id(x) for x in ast.walk(refusal[0])}
    leaked = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)
              and isinstance(n.value, str) and id(n) not in inside and "emission_episode" in n.value]
    assert not leaked, f"03c names the ground-truth table outside the refusal list: {leaked}"
    reads = [ast.get_source_segment(code, n) for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and n.func.attr in ("table", "sql", "load", "parquet", "csv")
             and not (n.func.attr == "table" and isinstance(n.func.value, ast.Attribute)
                      and n.func.value.attr == "catalog")]
    bad = [r for r in reads if r not in {"spark.table(name)", "spark.table(CE_TABLE)"}]
    assert not bad, f"03c reads a table other than through read_input(): {bad}"
    print("OK  fact_emission_episode named only in GROUND_TRUTH_TABLES; every read through "
          "read_input()")


# --- a synthetic upstream -------------------------------------------------------------------------
def upstream(g, start, end):
    rng = g["get_rng"]("h_ce_upstream")
    fac = [f"GS-{i:04d}" for i in range(1, 151)]
    fsk = {f: i for i, f in enumerate(fac, 1)}
    # attribution concentrates on some facilities (nearest-facility geometry), so repeats occur
    w = rng.pareto(1.5, len(fac)) + 0.2
    w /= w.sum()
    plumes = []
    for d in pd.date_range(start - pd.Timedelta(days=40), end - DAY, freq="D"):
        if rng.random() > 14 / 31:                    # ~14 scene days a month
            continue
        for _ in range(int(rng.poisson(3.5))):
            t = d + pd.Timedelta(hours=float(rng.uniform(17.5, 21.0)))   # afternoon overpass
            rate = float(np.clip(rng.lognormal(np.log(19700), 0.7), 3200, 72000))
            att = fac[int(rng.choice(len(fac), p=w))] if rng.random() < 42 / 49 else None
            sig = "Incomplete Combustion" if rng.random() < 2 / 49 else "Fugitive Leak"
            pid = f"{g['stable_key']('h_plume', t.isoformat(), rate):016x}"[:12]
            # a source somewhere around its facility: one facility collects distinct sources,
            # and only some detections repeat a location within 5 km
            base = fsk[att] if att else 0
            lat = 31.0 + (base % 15) * 0.15 + float(rng.normal(0, 0.12))
            lon = -104.0 + (base // 15) * 0.25 + float(rng.normal(0, 0.12))
            plumes.append((pid, t.floor("s"), rate, lat, lon, att, sig))
    pl = pd.DataFrame(plumes, columns=["plume_id", "detect_ts", "emission_rate_kg_h", "source_lat",
                                       "source_lon", "attributed_facility_id", "signature"])
    sens, flags = [], []
    for f in fac:
        for k in range(4):
            sid = f"SNS-{fsk[f] * 10 + k:05d}"
            sens.append((sid, fsk[f], fsk[f] * 100 + k))
            for _ in range(int(rng.poisson(0.85 * (end - start + 40 * DAY) / (30 * DAY)))):
                t0 = (start - pd.Timedelta(days=40) + (end - start + 40 * DAY) * float(rng.random())).floor("h")
                n = max(1, int(rng.lognormal(np.log(6), 0.8)))
                flags += [(sid, t0 + pd.Timedelta(hours=i)) for i in range(n)]
    sen = pd.DataFrame(sens, columns=["sensor_id", "facility_sk", "equipment_sk"])
    fl = (pd.DataFrame(flags, columns=["sensor_id", "reading_ts"]).drop_duplicates()
          .query("reading_ts < @end").sort_values(["sensor_id", "reading_ts"]).reset_index(drop=True))
    alarms = []
    for f in fac:
        for _ in range(int(rng.poisson(1.2 * (end - start) / (30 * DAY)))):
            tn, at = [("stack_temperature", "LoLo"), ("flow", "HiHi"), ("flow", "Hi")][int(rng.integers(0, 3))]
            r = (start + (end - start) * float(rng.random())).floor("5min")
            dur = pd.Timedelta(hours=float(rng.lognormal(np.log(1.5), 1.6)))
            alarms.append((g["stable_key"]("h_alarm", f, tn, at, r.isoformat()), fsk[f] * 100 + 9,
                           fsk[f], "Flare", tn, at, r, r + dur))
    al = pd.DataFrame(alarms, columns=["alarm_sk", "equipment_sk", "facility_sk", "equipment_type",
                                       "tag_name", "alarm_type", "raised_ts", "cleared_ts"])
    return dict(plumes=pl, sensors=sen, flags=fl, alarms=al, fac_sk=fsk,
                fac_id={v: k for k, v in fsk.items()})


def sources_as_of(g, U, horizon, source_start):
    pl = U["plumes"][U["plumes"]["detect_ts"] < horizon]
    fl = U["flags"][U["flags"]["reading_ts"] < horizon]
    al = U["alarms"][U["alarms"]["raised_ts"] < horizon].copy()
    al.loc[al["cleared_ts"] >= horizon, "cleared_ts"] = pd.NaT
    runs = g["facility_runs"](fl, U["sensors"], 3600)
    src, skipped = g["build_sources"](pl, runs, al, U["fac_sk"], horizon)
    return src[src["event_ts"] >= source_start].reset_index(drop=True), skipped


def norm(g, ce):
    c = ce[g["CE_COLS"]].copy()
    c["equipment_sk"] = c["equipment_sk"].astype("Int64")
    for k in ("measured_value", "threshold_value", "fine_usd"):
        c[k] = c[k].astype("float64")
    for k in ("is_violation", "is_synthetic"):
        c[k] = c[k].astype(bool)
    return c.sort_values("compliance_id", kind="mergesort").reset_index(drop=True)


def backfill(g, U, start, end):
    src, _ = sources_as_of(g, U, end, start)
    rows, events = g["run_window"]([], 1, src, start, end, U["fac_id"])
    return norm(g, g["to_frame"](rows)), events


def incremental_day(g, U, ce, day, start):
    end = day + DAY
    src, _ = sources_as_of(g, U, end, start)
    prior, nxt = g["prior_from_table"](ce, day)
    rows, _ = g["run_window"](prior, nxt, src, day, end, U["fac_id"])
    ch = g["to_frame"](rows)
    ws = int(day.strftime("%Y%m%d"))
    lo = min([ws] + ch["date_sk"].tolist())
    ex = ce[(ce["date_sk"] >= lo) & (ce["date_sk"] <= ws)]
    w = g["merge_for_write"](ex, ch, day)
    return norm(g, pd.concat([ce[(ce["date_sk"] < lo) | (ce["date_sk"] > ws)], w], ignore_index=True))


def sequence(g, U, start, n_days):
    ce, _ = backfill(g, U, start, start + DAY)
    for d in range(1, n_days):
        ce = incremental_day(g, U, ce, start + d * DAY, start)
    return ce


def check_determinism(g, U, start, n_days):
    end = start + n_days * DAY
    a, _ = backfill(g, U, start, end)
    b, _ = backfill(g, U, start, end)
    pd.testing.assert_frame_equal(a, b, check_exact=True)
    print(f"OK  two backfills identical ({len(a):,} events, "
          + ", ".join(f"{k} {v}" for k, v in a['event_type'].value_counts().items()) + ")")
    inc = sequence(g, U, start, n_days)
    pd.testing.assert_frame_equal(inc, a, check_exact=True)
    print(f"OK  the {n_days}-day backfill equals 1 backfill day + {n_days - 1} incremental days, bitwise")
    again = incremental_day(g, U, inc, end - DAY, start)
    pd.testing.assert_frame_equal(again, inc, check_exact=True)
    print("OK  rerunning the last day reproduces the table exactly")

    # negative control 1: the plan re-drawn per run
    real = g["get_rng"]
    key = {"k": None}
    g["get_rng"] = lambda *p: real(*p, key["k"]) if p and p[0] == "compliance" else real(*p)
    try:
        key["k"] = "backfill"
        x, _ = backfill(g, U, start, end)
        try:
            ce, _ = backfill(g, U, start, start + DAY)
            for d in range(1, n_days):
                key["k"] = f"day-{d}"
                ce = incremental_day(g, U, ce, start + d * DAY, start)
            differs = not ce.equals(x)
        except AssertionError:
            differs = True             # prior_from_table refused the mismatched plan
    finally:
        g["get_rng"] = real
    assert differs, "negative control failed: a per-run plan matched the backfill"
    print("OK  negative control: a case plan re-drawn per run does not reproduce the backfill")

    # negative control 2: repetition judged with hindsight (+/- the window, not trailing)
    orig = g["plume_sources"]
    code = HE.code_cells(NB_03C)
    cell = [c for c in code if MODEL_MARKER in c][0]
    hind = cell.replace('(att["detect_ts"] < t) & (att["detect_ts"] >= t - win)',
                        '(att["detect_ts"] != t) & ((att["detect_ts"] - t).abs() <= win)')
    assert hind != cell, "negative control 2 could not patch the repetition rule"
    h = dict(g)
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(hind, "03c_hindsight", "exec"), h)
    hb, _ = backfill(h, U, start, end)
    hi = sequence(h, U, start, n_days)
    assert not hi.equals(hb), "negative control failed: hindsight repetition still matched"
    g["plume_sources"] = orig
    print(f"OK  negative control: repetition with hindsight does not reproduce the backfill "
          f"({len(hb) - len(hi):+d} events)")


def check_steady_state(g, U, start, n_days):
    end = start + n_days * DAY
    ce, events = backfill(g, U, start, end)
    stalled = {e["compliance_id"] for e in events if e["plan"]["is_stalled"]}
    ns = ce[~ce["compliance_id"].isin(stalled)]
    days = pd.date_range(start, end - DAY, freq="D")
    traj = g["open_trajectory"](ns, days)
    tw = days[-g["TREND_WINDOW_DAYS"]:]
    lam = int(((ns["event_ts"] >= tw[0]) & (ns["event_ts"] < end)).sum()) / len(tw)
    by_id = {e["compliance_id"]: e for e in events}
    T = np.mean([(by_id[c]["plan"]["report_s"] + by_id[c]["plan"]["review_s"]) / 86400.0
                 for c in ns["compliance_id"]])
    real = traj[-len(tw):].mean()
    x = np.arange(len(tw), dtype=float)
    rise = float(np.polyfit(x, traj[-len(tw):].astype(float), 1)[0]) * (len(tw) - 1)
    print(f"  {n_days}-day run: {len(ce):,} events, {int(ce['is_violation'].sum())} violations, "
          f"fines ${ce['fine_usd'].sum():,.0f}")
    print("  open by 30 days: " + "  ".join(str(int(traj[i:i + 30].mean())) for i in range(0, len(traj), 30)))
    print(f"  arrivals {lam:.2f}/day, T {T:.1f} d, predicted open {lam * T:.1f}, realised {real:.1f}, "
          f"rise {rise:+.1f}")
    lo, hi = g["STEADY_BAND"]
    assert lo * lam * T <= real <= hi * lam * T, "open cases outside the steady-state band"
    assert rise <= g["TREND_MAX_RISE"] * real, "open cases trending upward"
    for t in g["EVENT_TYPES"]:
        n = int((ce["event_type"] == t).sum())
        if n >= 40:
            sh = sum(1 for c in ce.loc[ce["event_type"] == t, "compliance_id"] if c in stalled) / n
            assert abs(sh - g["STALL_SHARE"][t]) < 0.06, f"{t} stall share {sh:.1%}"
    v = ce[ce["is_violation"]]
    assert v["event_type"].isin(["Venting", "Flaring"]).all()
    assert (ce.loc[~ce["is_violation"], "fine_usd"] == 0).all()
    reg = {g["REG_SK"][r["regulation_id"]]: r for r in g["REGULATIONS"]}
    assert all(reg[s]["fine_min_usd"] <= f <= reg[s]["fine_max_usd"]
               for s, f in zip(v["regulation_sk"], v["fine_usd"]))
    cases = ce[ce["event_type"].isin(["Venting", "Flaring"])]
    dec = cases[cases["status"].isin(["Closed", "Violation"])]
    share = (dec["status"] == "Violation").mean() if len(dec) else float("nan")
    print(f"  state cases {len(cases)}, decided {len(dec)}, violation share {share:.0%} "
          f"(target {g['VIOLATION_AFFIRM_SHARE']:.0%}); OLRE {int((ce['event_type'] == 'OLRE').sum())}")
    print("OK  steady state: open cases in band, not trending; stall shares near target; "
          "violations only on state cases, fined inside their range")

    # the stalled exit: nothing open past it, and the stalled pool levels off after it
    exit_d = g["CE_CANCEL_AFTER_DAYS"]
    opn = ce[ce["status"].isin(g["CE_OPEN_STATUSES"])]
    assert (opn["event_ts"] + pd.Timedelta(days=exit_d) >= end).all(), "a case open past the exit"
    cx = ce[ce["status"] == "Cancelled"]
    assert len(cx) and cx["compliance_id"].isin(stalled).all(), "cancelled must be exactly stalled"
    st = ce[ce["compliance_id"].isin(stalled)]
    so = g["open_trajectory"](st, days)
    L = 60
    rate = len(st[st["event_ts"] >= end - pd.Timedelta(days=exit_d)]) / exit_d
    srise = float(np.polyfit(np.arange(L, dtype=float), so[-L:].astype(float), 1)[0]) * (L - 1)
    print("  stalled open by 30 days: " + "  ".join(str(int(so[i:i + 30].mean())) for i in range(0, len(so), 30)))
    print(f"  stalled open last {L} d {so[-L:].mean():.1f} vs {rate:.3f}/day x {exit_d} d = "
          f"{rate * exit_d:.1f}; rise {srise:+.1f}; cancelled {len(cx)}")
    assert 0.5 * rate * exit_d <= so[-L:].mean() <= 1.5 * rate * exit_d, "stalled pool did not converge"
    # The pool is ~10 cases, so a percentage trend test cannot tell growth from Poisson noise
    # (the last 60 days drift by several cases either way). Bound it instead: once the exit
    # is live, the pool never exceeds twice its predicted level, and it sits well below the
    # no-exit counterfactual -- every stalled case ever raised, which only grows.
    no_exit = np.array([int((st["event_ts"] < d + DAY).sum()) for d in days])
    assert so[exit_d:].max() <= 2.0 * rate * exit_d, "stalled pool exceeded twice its steady level"
    assert so[-1] <= 0.6 * no_exit[-1], "the exit is not draining the stalled pool"
    print(f"  without the exit the stalled pool would be {no_exit[-1]} and still growing; with it, "
          f"max {so[exit_d:].max()} after day {exit_d}")
    print(f"OK  stalled exit: nothing open past {exit_d} days; the stalled pool levels off at "
          "stall rate x exit; Cancelled is exactly the stalled cases that reached it")


if __name__ == "__main__":
    print("=" * 84)
    print("03c COMPLIANCE EVENTS")
    print("=" * 84)
    g = load_model()
    check_static()
    START = pd.Timestamp("2026-08-16")
    U = upstream(g, START, START + 300 * DAY)
    print(f"  synthetic upstream: {len(U['plumes'])} plumes "
          f"({int(U['plumes']['attributed_facility_id'].isna().sum())} unattributed), "
          f"{len(U['flags']):,} flagged CH4 readings, {len(U['alarms']):,} flare alarms")
    check_determinism(g, U, START, 30)
    print("  -- again with CE_CANCEL_AFTER_DAYS = 10, so cancellations cross run boundaries")
    _exit = g["CE_CANCEL_AFTER_DAYS"]
    g["CE_CANCEL_AFTER_DAYS"] = 10
    try:
        check_determinism(g, U, START, 30)
    finally:
        g["CE_CANCEL_AFTER_DAYS"] = _exit
    check_steady_state(g, U, START, 300)
