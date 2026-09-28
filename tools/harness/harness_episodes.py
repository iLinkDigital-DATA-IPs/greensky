"""03a's emission episodes: the notebook's own model cell, run against 01_topology_config.

Four things are checked here.

1. Static. 03a reads tables only through read_input(); no observation table name appears in
   its code outside the OBSERVATION_TABLES refusal list; 00_config and 01_topology_config
   (which 03a %runs) read no table at all. And every root cause 03a allows on an instrumented
   equipment type has a signature in 02b's EPISODE_SIGNATURE that touches at least one of
   that type's tags in TAG_TEMPLATES -- otherwise 02b's overlay could never corroborate it.

2. Calibration. The model cell (extracted from the notebook, not copied) runs over a
   SYNTHETIC estate shaped like 01b's -- 150 facilities, the real facility and equipment
   mixes, 01b's age cohorts -- with fact_asset_state from 02a's real state machine
   (harness_alarm_rate.simulate). The estate lives in OneLake, so the counts below are what
   the notebook should land near, not what it will print to the digit. The rate figures
   depend on the rate model alone and should match closely.

3. Invariants on that output: no overlap with Maintenance or Down, root cause consistent
   with equipment type, mass and duration consistent with the rates.

4. Determinism. Two runs identical; a 90-day backfill equal to the sequence of incremental
   days, where each incremental run sees state only up to its own horizon and regenerates
   the 30-day lookback. And the negative control: incremental runs WITHOUT the lookback
   do not reproduce the backfill, which is why the lookback exists.
"""
import ast
import io
import contextlib
import re
import numpy as np
import pandas as pd
import shared_defs as S
from harness_final_rate import load_config
import harness_alarm_rate as AR
from harness_alarm_rate import simulate

NB_03A = S.NB / "03_enterprise/03a_gen_emission_episodes.Notebook/notebook-content.py"
NB_00 = S.NB / "00_prereqs/00_config.Notebook/notebook-content.py"
NB_01 = S.NB / "01_topology/01_topology_config.Notebook/notebook-content.py"
MARK = re.compile(r"^# (CELL|MARKDOWN|METADATA) \*{20,}$")
MODEL_MARKER = "# ---- 03a episode model (pure"


def code_cells(path):
    cells, cur, kind = [], [], None
    for ln in path.read_text(encoding="utf-8").replace("\r\n", "\n").split("\n"):
        m = MARK.match(ln)
        if m:
            if kind == "CELL":
                cells.append("\n".join(cur))
            kind, cur = m.group(1), []
            continue
        cur.append(ln)
    if kind == "CELL":
        cells.append("\n".join(cur))
    return cells


def load_model():
    """01_topology_config, then 03a's model cell executed verbatim on top of it."""
    g = load_config()
    cell = [c for c in code_cells(NB_03A) if MODEL_MARKER in c]
    assert len(cell) == 1, f"expected one 03a model cell, found {len(cell)}"
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(cell[0], "03a_model_cell", "exec"), g)
    return g


# --- 1. static ---------------------------------------------------------------------------------
def check_static(g):
    code = "\n".join(("pass  # " + ln) if ln.lstrip().startswith("%") else ln
                     for c in code_cells(NB_03A) for ln in c.split("\n"))
    tree = ast.parse(code)
    refusal = [n for n in tree.body if isinstance(n, ast.Assign)
               and any(isinstance(t, ast.Name) and t.id == "OBSERVATION_TABLES"
                       for t in n.targets)]
    assert len(refusal) == 1, "03a must define OBSERVATION_TABLES exactly once"
    obs = set(ast.literal_eval(refusal[0].value))
    for must in ("gold_plume_catalog", "gold_multi_gas_signatures", "scada_telemetry",
                 "sensor_telemetry"):
        assert must in obs, f"OBSERVATION_TABLES does not refuse {must}"
    inside = {id(x) for x in ast.walk(refusal[0])}
    leaked = sorted({n.value for n in ast.walk(tree)
                     if isinstance(n, ast.Constant) and isinstance(n.value, str)
                     and id(n) not in inside and any(o in n.value for o in obs)})
    assert not leaked, f"03a's code names observation table(s) outside the refusal list: {leaked}"

    reads = []
    for n in ast.walk(tree):
        if (isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                and n.func.attr in ("table", "sql", "load", "parquet", "csv")
                and not (n.func.attr == "table" and isinstance(n.func.value, ast.Attribute)
                         and n.func.value.attr == "catalog")):
            reads.append(ast.get_source_segment(code, n))
    allowed = {"spark.table(name)", "spark.table(TABLE)"}
    bad = [r for r in reads if r not in allowed]
    assert not bad, (f"03a reads a table other than through read_input() or its own output: "
                     f"{bad}")
    for p in (NB_00, NB_01):
        src = p.read_text(encoding="utf-8")
        assert "spark.table(" not in src and "spark.sql(" not in src, (
            f"{p.parent.name} reads a table, and 03a %runs it -- that read would bypass "
            "read_input()")
    print(f"OK  03a reads only through read_input() (+ its own output table); no observation "
          f"table named in code outside the refusal list; 00_config and 01_topology_config "
          f"read no table")

    sig = eval(S.extract(S.NB_02B, ["EPISODE_SIGNATURE"])["EPISODE_SIGNATURE"]
               .split("=", 1)[1])
    assert set(g["ROOT_CAUSES"]) == set(sig), (
        f"03a's root-cause vocabulary {sorted(g['ROOT_CAUSES'])} differs from 02b's "
        f"EPISODE_SIGNATURE keys {sorted(sig)}; 02b's overlay joins on root_cause")
    tags = {et: {t[0] for t in rows} for et, rows in g["TAG_TEMPLATES"].items()}
    dead = []
    for et, w in g["ROOT_CAUSE_WEIGHTS"].items():
        if et not in tags:
            continue          # uninstrumented: no telemetry to corroborate with, any cause
        for rc in w:
            if rc == "Unknown":
                continue      # no signature by design
            if not {tn for tn, _, _ in sig[rc]} & tags[et]:
                dead.append((et, rc))
    assert not dead, (f"root cause(s) allowed on a type none of whose tags the 02b signature "
                      f"touches -- the overlay would be a no-op: {dead}")
    n_pairs = sum(len(w) for et, w in g["ROOT_CAUSE_WEIGHTS"].items() if et in tags)
    print(f"OK  root-cause vocabulary matches 02b; every one of {n_pairs} (instrumented type, "
          f"cause) pairs has a signature tag on that type")


# --- 2. synthetic estate, shaped like 01b's -----------------------------------------------------
def estate(g):
    rng = g["get_rng"]("h_episode_estate")
    ftypes = list(g["FACILITY_TYPE_WEIGHTS"])
    fw = [g["FACILITY_TYPE_WEIGHTS"][t] for t in ftypes]
    as_of = pd.Timestamp(g["TOPOLOGY_AS_OF"])
    mfrs = list(g["MANUFACTURERS"])
    rows, eq = [], 0
    for f in range(1, 151):
        ft = str(rng.choice(ftypes, p=fw))
        commission = as_of - pd.Timedelta(days=int(rng.integers(365, g["HISTORY_YEARS"] * 365)))
        lat, lon = 31.5 + rng.normal(0, 0.4), -102.5 + rng.normal(0, 0.5)
        lo, hi = g["EQUIP_COUNT_BY_TYPE"][ft]
        for _ in range(int(rng.integers(lo, hi + 1))):
            eq += 1
            et = str(rng.choice(g["EQUIPMENT_TYPE_NAMES"], p=g["equipment_weights"](ft)))
            ev = g["EQUIPMENT_TYPES"][et]
            max_age = max(1, (as_of - commission).days // 365)
            if rng.random() < 0.35 and max_age >= 9:          # 01b's legacy cohort
                left, right = min(8, max_age - 1), max_age
                age = min(max_age, int(rng.triangular(left, max(left, min(int(max_age * .7),
                                                                         right)), right)))
            else:                                               # 01b's newer cohort
                right = max(1, int(max_age * 0.6))
                age = int(rng.triangular(0, max(0, min(int(max_age * 0.3), right)), right))
            install = max(as_of - pd.Timedelta(days=age * 365 + int(rng.integers(0, 365))),
                          commission)
            crit = str(rng.choice(["Low", "Medium", "High", "Critical"],
                                  p=[.4, .3, .2, .1] if ev["crit_bias"] < 0.6
                                  else [.2, .3, .3, .2]))
            mfr = mfrs[int(rng.integers(0, len(mfrs)))]
            rows.append(dict(equipment_sk=eq, equipment_id=f"EQ-{eq:05d}", equipment_type=et,
                             install_date=install, expected_life_years=ev["life"],
                             inspection_frequency_days=ev["insp_days"],
                             reliability_index=g["MANUFACTURERS"][mfr],
                             leak_propensity=ev["leak_propensity"], criticality=crit,
                             area_sk=eq // 5 + 1, facility_sk=f, facility_id=f"GS-{f:04d}",
                             area_lat=lat, area_lon=lon))
    return pd.DataFrame(rows)


def bind_state_machine(g):
    """harness_alarm_rate carries 02a's constants for the six instrumented types only. Check
    its copy agrees with 01_topology_config where they overlap, then bind it to the config's
    values so simulate() covers all eight types -- Valve and Pipeline Segment leak too."""
    for et, ev in AR.EQUIPMENT_TYPES.items():
        for k, v in ev.items():
            assert g["EQUIPMENT_TYPES"][et][k] == v, f"harness_alarm_rate drifted: {et}.{k}"
        assert g["STATE_DUTY_FACTOR"][et] == AR.STATE_DUTY_FACTOR[et], f"duty drifted: {et}"
    for name in ("BASE_MTBF_DAYS", "SPURIOUS_TRIP_SHARE", "STATE_DWELL_HOURS",
                 "MAINTENANCE_DWELL_HOURS", "UNPLANNED_CAUSE_WEIGHTS"):
        assert getattr(AR, name) == g[name], f"harness_alarm_rate drifted: {name}"
    AR.EQUIPMENT_TYPES = g["EQUIPMENT_TYPES"]
    AR.STATE_DUTY_FACTOR = g["STATE_DUTY_FACTOR"]


def full_state(g, assets, history_start, until):
    """02a's chain per asset, from the history anchor (or install) to `until`."""
    bind_state_machine(g)
    out = []
    for a in assets.itertuples():
        age = (pd.Timestamp(g["TOPOLOGY_AS_OF"]) - a.install_date).days / 365.25
        start = max(history_start, a.install_date)
        tr = simulate(a.equipment_type, a.equipment_id, age, a.install_date, start, until)
        tr["equipment_sk"] = a.equipment_sk
        out.append(tr)
    return pd.concat(out, ignore_index=True)


def cut_state(st, horizon):
    """fact_asset_state as 02a would have written it with its horizon at `horizon`."""
    s = st[st["start_ts"] < horizon].copy()
    s.loc[s["end_ts"] >= horizon, "end_ts"] = pd.NaT
    return s


# --- 3. invariants ------------------------------------------------------------------------------
def check_invariants(g, ep, st):
    assert ep["episode_sk"].is_unique
    assert (ep["end_ts"] > ep["start_ts"]).all()
    assert np.allclose((ep["end_ts"] - ep["start_ts"]).dt.total_seconds() / 3600,
                       ep["duration_hours"], atol=1e-9)
    assert np.allclose(ep["mean_rate_kg_h"] * ep["duration_hours"], ep["total_mass_kg"],
                       rtol=1e-12)
    for et, rc in zip(ep["equipment_type"], ep["root_cause"]):
        assert rc in g["ROOT_CAUSE_WEIGHTS"][et], (et, rc)
    stops = st[st["state"].isin(g["NON_EMITTING_STATES"])]
    by = {k: (v["start_ts"].values, v["end_ts"].fillna(pd.Timestamp.max).values)
          for k, v in stops.groupby("equipment_sk")}
    bad = 0
    for r in ep.itertuples():
        s = by.get(r.equipment_sk)
        if s is not None and np.any((s[0] < np.datetime64(r.end_ts))
                                    & (s[1] > np.datetime64(r.start_ts))):
            bad += 1
    assert bad == 0, f"{bad} episode(s) overlap Maintenance or Down"
    print(f"OK  {len(ep):,} episodes: keys unique, durations and mass consistent, root cause "
          f"allowed for type, none overlapping Maintenance or Down")


def top_share(mass, share=0.05):
    s = np.sort(np.asarray(mass))[::-1]
    return s[:max(1, int(len(s) * share))].sum() / s.sum()


def report(g, assets, ep, days):
    above = ep[ep["above_tropomi_floor"]]
    print(f"    episodes              {len(ep):,}  ({len(ep) / (len(assets) * days / 365.25):.2f}"
          f" per asset-year, {len(assets):,} assets, {days} days)")
    print(f"    above 3 t/h           {len(above)}  ({len(above) / len(ep):.3%})")
    if len(above):
        print(f"      median / max        {above['peak_rate_kg_h'].median() / 1000:.1f} / "
              f"{above['peak_rate_kg_h'].max() / 1000:.1f} t/h")
    print(f"    top 5% by mass        {top_share(ep['total_mass_kg']):.1%} of total mass")
    w0, w1 = pd.Timestamp("2026-08-16"), pd.Timestamp("2026-09-16")
    act = above[(above["start_ts"] < w1) & (above["end_ts"] > w0)]
    act_days = {d for r in act.itertuples()
                for d in pd.date_range(max(r.start_ts, w0).normalize(), min(r.end_ts, w1), freq="D")
                if d < w1}
    print(f"    active 08-16..09-15   {len(act)} above-floor episode(s), on {len(act_days)} of 31 days")
    mix = ep.groupby("root_cause").agg(n=("episode_sk", "size"), sup=("_is_super", "mean"))
    print("    cause mix             " + ", ".join(f"{c} {r.n} ({r.sup:.0%} super)"
                                                     for c, r in mix.iterrows()))
    print(f"    truncated by a stop   {ep['_truncated'].mean():.1%};  intermittent "
          f"{ep['is_intermittent'].mean():.1%}")
    fac = ep.groupby("facility_id").size().reindex(
        assets["facility_id"].unique(), fill_value=0)
    print(f"    per facility          median {fac.median():.0f}  p90 {fac.quantile(.9):.0f}  "
          f"max {fac.max()}  none {int((fac == 0).sum())}")
    rng = g["get_rng"]("h_episode_model_check")
    types = ep["equipment_type"].value_counts(normalize=True)
    mc = np.array([g["draw_episode"](rng, t)["peak_rate_kg_h"]
                   for t in rng.choice(types.index.values, 200_000, p=types.values)])
    mca = mc[mc > 3000]
    mx = np.median([rng.choice(mca, 49).max() for _ in range(500)])
    print(f"    model (200k draws)    {len(mca) / len(mc):.3%} above floor; above-floor median "
          f"{np.median(mca) / 1000:.1f} t/h, p5 {np.percentile(mca, 5) / 1000:.1f}, "
          f"p95 {np.percentile(mca, 95) / 1000:.1f}, typical max of 49 {mx / 1000:.1f} t/h")
    print("    observed TROPOMI      median 19.7 t/h, max 71.8 t/h (n=49); CAMS median 48 t/h")
    return mca


if __name__ == "__main__":
    g = load_model()
    AS_OF = pd.Timestamp(g["TOPOLOGY_AS_OF"])
    H0 = AS_OF - pd.Timedelta(days=g["STATE_HISTORY_DAYS"])
    DAY, L = pd.Timedelta(days=1), pd.Timedelta(days=g["EPISODE_MAX_DURATION_DAYS"])

    print("=" * 88)
    print("03a EMISSION EPISODES -- the notebook's model cell, verbatim")
    print("=" * 88)
    check_static(g)

    assets = estate(g)
    st_full = full_state(g, assets, H0, AS_OF + pd.Timedelta(days=40))
    st_asof = cut_state(st_full, AS_OF)
    print(f"\nsynthetic estate: {len(assets):,} assets on 150 facilities; "
          f"{len(st_asof):,} state intervals over {g['STATE_HISTORY_DAYS']} days")

    ep = g["generate_episodes"](assets, st_asof, H0, AS_OF, H0)
    check_invariants(g, ep, st_asof)
    print("\ncalibration (90-day backfill, synthetic estate):")
    report(g, assets, ep, g["STATE_HISTORY_DAYS"])

    # --- 4. determinism ---------------------------------------------------------------------
    print()
    g2 = load_model()
    ep2 = g2["generate_episodes"](assets, st_asof, H0, AS_OF, H0)
    pd.testing.assert_frame_equal(ep, ep2, check_exact=True)
    print(f"OK  two runs from the same seed identical ({len(ep):,} rows, fresh namespace)")

    sub = assets[assets["equipment_sk"] % 12 == 0].reset_index(drop=True)
    key = ["equipment_sk", "start_ts"]

    def norm(df):
        return df.sort_values(key, kind="mergesort").reset_index(drop=True)

    def incremental(lookback):
        table = g["generate_episodes"](sub, cut_state(st_full, H0 + DAY), H0, H0 + DAY, H0)
        d = H0 + 2 * DAY
        while d <= AS_OF:
            ws = d - DAY
            gs = max(H0, ws - L) if lookback else ws
            new = g["generate_episodes"](sub, cut_state(st_full, d), gs, d, H0)
            lo, hi = int(gs.strftime("%Y%m%d")), int(ws.strftime("%Y%m%d"))
            keep = table[(table["date_sk"] < lo) | (table["date_sk"] > hi)]
            table = pd.concat([f for f in (keep, new) if len(f)] or [new], ignore_index=True)
            d += DAY
        return norm(table)

    back = norm(g["generate_episodes"](sub, st_asof, H0, AS_OF, H0))
    inc = incremental(lookback=True)
    pd.testing.assert_frame_equal(back, inc, check_exact=True)
    print(f"OK  backfill == {g['STATE_HISTORY_DAYS'] - 1} incremental days, each seeing state "
          f"only to its own horizon ({len(sub)} assets, {len(back):,} episodes, bitwise)")

    noback = incremental(lookback=False)
    m = back.merge(noback, on=key, suffixes=("", "_nl"))
    moved = int((m["end_ts"] != m["end_ts_nl"]).sum())
    assert len(noback) == len(back) and moved > 0, (
        "negative control failed: incremental runs without the lookback matched the backfill, "
        "so this check cannot tell whether the lookback matters")
    print(f"OK  negative control: without the {g['EPISODE_MAX_DURATION_DAYS']}-day lookback, "
          f"{moved} episode end(s) differ from the backfill -- an episode running past its "
          "day's horizon is never truncated by the stop that arrives later")
