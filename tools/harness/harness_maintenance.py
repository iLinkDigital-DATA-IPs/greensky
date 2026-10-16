"""03d's maintenance, inspections and LDAR: the notebook's own model cell, verbatim.

1. Static. fact_emission_episode is named only in GROUND_TRUTH_TABLES; every read goes through
   read_input() (or the write's own table probes); pm_phase_days is character-for-character
   02a's, so PM due dates agree with 02a's Maintenance intervals; TEAM_DISCIPLINES is 03b's.

2. Determinism, on a synthetic estate with 02a's real state machine (harness_alarm_rate.
   simulate) and synthetic closed work orders, every upstream cut to each run's horizon:
     - two backfills identical, all four tables
     - a 30-day backfill equals 1 backfill day + 29 incremental days, bitwise, each run
       reading its prior state back from the tables it wrote (prior_state) and writing
       through replaceWhere semantics, LDAR through the notebook's merge_for_write()
     - rerunning the last day changes nothing
   Negative control: a PM completion re-drawn per run instead of keyed on the calendar index
   does not reproduce the backfill.

3. Backlogs over 180 days: established overdue PMs pass the net-rise, monotonicity and
   share-ceiling checks, with negative controls that each one fires; PM visit completion near its target;
   LDAR cumulative repaired >= 70% of cumulative detected 30 days earlier, and outstanding passes the
   net-rise and monotonicity checks, with planted drain defects that each fire. No slope test on
   either backlog: 03d's constants cell records why it was removed; Leak Found rate rising with condition; every preventive record sitting exactly
   on a Maintenance interval.
"""
import ast
import contextlib
import io
import numpy as np
import pandas as pd
import shared_defs as S
import harness_episodes as HE
import harness_alarm_rate as AR

NB_03D = S.NB / "03_enterprise/03d_gen_maintenance.Notebook/notebook-content.py"
NB_02A = S.NB / "02_scada/02a_build_asset_state.Notebook/notebook-content.py"
NB_03B = S.NB / "03_enterprise/03b_gen_work_orders.Notebook/notebook-content.py"
MODEL_MARKER = "# ---- 03d maintenance model (pure"
DAY = pd.Timedelta(days=1)


def load_model():
    g = HE.load_config()
    cell = [c for c in HE.code_cells(NB_03D) if MODEL_MARKER in c]
    assert len(cell) == 1, f"expected one 03d model cell, found {len(cell)}"
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(cell[0], "03d_model_cell", "exec"), g)
    return g


def check_static(g):
    code = "\n".join(("pass  # " + ln) if ln.lstrip().startswith("%") else ln
                     for c in HE.code_cells(NB_03D) for ln in c.split("\n"))
    tree = ast.parse(code)
    ref = [n for n in tree.body if isinstance(n, ast.Assign)
           and any(isinstance(t, ast.Name) and t.id == "GROUND_TRUTH_TABLES" for t in n.targets)]
    assert len(ref) == 1 and ast.literal_eval(ref[0].value) == ("fact_emission_episode",)
    inside = {id(x) for x in ast.walk(ref[0])}
    leaked = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)
              and isinstance(n.value, str) and id(n) not in inside and "emission_episode" in n.value]
    assert not leaked, f"03d names the ground-truth table outside the refusal list: {leaked}"
    reads = [ast.get_source_segment(code, n) for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and n.func.attr in ("table", "sql", "load", "parquet", "csv")
             and not (n.func.attr == "table" and isinstance(n.func.value, ast.Attribute)
                      and n.func.value.attr == "catalog")]
    bad = [r for r in reads if r not in {"spark.table(name)", "spark.table(_tbl)"}]
    assert not bad, f"03d reads a table other than through read_input(): {bad}"
    a = S.extract(NB_02A, ["pm_phase_days"])["pm_phase_days"]
    b = S.extract(NB_03D, ["pm_phase_days"])["pm_phase_days"]
    assert a == b, "03d's pm_phase_days is not 02a's -- PM due dates would drift from 02a's stops"
    t3b = S.extract(NB_03B, ["TEAM_DISCIPLINES"])["TEAM_DISCIPLINES"]
    t3d = S.extract(NB_03D, ["TEAM_DISCIPLINES"])["TEAM_DISCIPLINES"]
    assert t3b == t3d, "03d's team roster is not 03b's"
    print("OK  fact_emission_episode named only in the refusal list; reads through read_input(); "
          "pm_phase_days identical to 02a's; team roster identical to 03b's")


# --- a synthetic upstream ------------------------------------------------------------------------
def upstream(g, history_start, end):
    assets = HE.estate(g)
    fr = assets.drop_duplicates("facility_sk")[["facility_sk", "facility_id"]].copy()
    rng = g["get_rng"]("h_maint_fac")
    fr["sub_basin"] = [list(g["ANCHORS"])[i % 4] for i in range(len(fr))]
    fr["facility_type"] = [list(g["FACILITY_TYPE_WEIGHTS"])[int(rng.integers(0, 5))] for _ in range(len(fr))]
    fr["commission_date"] = [pd.Timestamp("2008-01-01") + pd.Timedelta(days=int(rng.integers(0, 6000)))
                             for _ in range(len(fr))]
    eq = assets.rename(columns={})
    st = HE.full_state(g, eq, history_start, end + DAY)
    # 02a's cause, recovered: a Scheduled PM Maintenance interval starts 15-60 minutes after a
    # calendar point (the PM Shutdown's dwell); every other Maintenance interval is corrective.
    ph = {r.equipment_sk: (pd.Timestamp(r.install_date)
                           + pd.Timedelta(days=g["pm_phase_days"](r.equipment_id,
                                                                  int(r.inspection_frequency_days))),
                           int(r.inspection_frequency_days))
          for r in eq.itertuples()}
    cause = []
    for r in st.itertuples():
        c = None
        if r.state == "Maintenance":
            anchor, f = ph[r.equipment_sk]
            k = np.floor((r.start_ts - anchor) / pd.Timedelta(days=f))
            gap = (r.start_ts - (anchor + pd.Timedelta(days=f * k))) / pd.Timedelta(hours=1)
            c = "Scheduled PM" if 0.24 <= gap <= 1.01 else "Corrective"
        cause.append(c)
    st["cause"] = cause
    # synthetic work orders: created at random, closed after a lognormal life
    wos = []
    wr = g["get_rng"]("h_maint_wo")
    days = (end - history_start) / DAY
    for i in range(int(days * 12)):
        e = eq.iloc[int(wr.integers(0, len(eq)))]
        c = history_start + pd.Timedelta(seconds=int(wr.integers(0, int(days * 86400))))
        life = pd.Timedelta(hours=float(wr.lognormal(np.log(40), 0.8)))
        wos.append({"work_order_id": f"WO-{i + 1:06d}", "equipment_sk": int(e.equipment_sk),
                    "source": ["Alarm", "Exceedance", "SensorStatus", "Compliance", "Trip"][int(wr.integers(0, 5))],
                    "created_ts": c, "close_at": c + life, "cost_usd": round(float(wr.lognormal(7.5, 0.8)), 2),
                    "assigned_team_sk": g["team_sk"]("Midland Basin", "Mechanical")})
    wo = pd.DataFrame(wos)
    si_full = g["state_index"](st)
    wo["downtime_full"] = [down(si_full, e, c, x) for e, c, x in
                           zip(wo["equipment_sk"], wo["created_ts"], wo["close_at"])]
    calib = g["seed_calibration"](g["asset_views"](eq, fr), st, history_start)
    return dict(eq=eq, fac=fr, state=st, wo=wo, calib=calib)


def down(si, esk, lo, hi):
    x = si.get(int(esk))
    if x is None:
        return 0.0
    m = np.isin(x[2], ["Down", "Maintenance"])
    a = np.maximum(x[0][m], lo.value)
    b = np.minimum(x[1][m], hi.value)
    return float(np.clip(b - a, 0, None).sum() / 3600e9)


def as_of(g, U, horizon):
    st = HE.cut_state(U["state"], horizon)
    w = U["wo"].copy()
    closed = w["close_at"] < horizon
    w["status"] = np.where(closed, "Closed", "Open")
    w["closed_ts"] = w["close_at"].where(closed)
    w["downtime_hours"] = w["downtime_full"].where(closed)
    return st, w[w["created_ts"] < horizon]


def run(g, U, lo, hi, history_start, stored=None):
    st, wo = as_of(g, U, hi)
    assets = g["asset_views"](U["eq"], U["fac"])
    si = g["state_index"](st)
    if stored is None:
        prior = {"last_completed": {k: g["pm_seed"](a, history_start, U["calib"]) for k, a in assets.items()},
                 "parents": [], "surveys": []}
    else:
        prior = g["prior_state"](assets, U["fac"], si, stored["maint"], stored["insp"], lo, history_start,
                                 U["calib"])
    m, i, l, last = g["run_window"](assets, U["fac"], si, st, wo, set(), prior, lo, hi, history_start)
    pm = g["pm_snapshot"](assets, last, hi)
    return (g["to_frame"](m, g["MAINT_COLS"], nullable_int=("contractor_sk",)),
            g["to_frame"](i, g["INSP_COLS"]), g["to_frame"](l, g["LDAR_COLS"]),
            g["to_frame"](pm, g["PM_COLS"]))


def norm(df, key):
    d = df.copy()
    for c in d.columns:
        if d[c].dtype == object and d[c].map(lambda v: isinstance(v, pd.Timestamp) or v is None).all():
            d[c] = pd.to_datetime(d[c])
    return d.sort_values(key, kind="mergesort").reset_index(drop=True)


def replace_window(old, new, lo_sk, hi_sk):
    if old is None or not len(old):
        return new
    keep = old[(old["date_sk"] < lo_sk) | (old["date_sk"] > hi_sk)]
    return pd.concat([keep, new], ignore_index=True)


def incremental_day(g, U, T, day, history_start):
    m, i, l, pm = run(g, U, day, day + DAY, history_start, stored=T)
    sk = int(day.strftime("%Y%m%d"))
    lo = min([sk] + l["date_sk"].tolist())
    ex = T["ldar"][(T["ldar"]["date_sk"] >= lo) & (T["ldar"]["date_sk"] <= sk)]
    lw = g["merge_for_write"](ex, l, "survey_sk", "survey_ts", day, g["LDAR_COLS"])
    return {"maint": replace_window(T["maint"], m, sk, sk), "insp": replace_window(T["insp"], i, sk, sk),
            "ldar": replace_window(T["ldar"], lw, lo, sk), "pm": pm}


KEYS = {"maint": "maintenance_sk", "insp": "inspection_sk", "ldar": "survey_sk", "pm": "pm_schedule_sk"}


def tables(m, i, l, pm):
    return {"maint": m, "insp": i, "ldar": l, "pm": pm}


def assert_same(a, b, what):
    for k in KEYS:
        pd.testing.assert_frame_equal(norm(a[k], KEYS[k]), norm(b[k], KEYS[k]), check_exact=True,
                                      check_dtype=False, obj=f"{what}:{k}")


def check_determinism(g, U, history_start, start, n_days):
    end = start + n_days * DAY
    # the prefix [history_start, start) as a backfill, then the window both ways
    base = tables(*run(g, U, history_start, start, history_start))
    full = tables(*run(g, U, history_start, end, history_start))
    again = tables(*run(g, U, history_start, end, history_start))
    assert_same(full, again, "two backfills")
    print(f"OK  two backfills identical ({len(full['maint'])} maintenance, {len(full['insp'])} "
          f"inspections, {len(full['ldar'])} surveys, {len(full['pm'])} PM rows)")
    T = base
    for d in range(n_days):
        T = incremental_day(g, U, T, start + d * DAY, history_start)
    assert_same(T, full, "backfill vs incremental")
    print(f"OK  backfill equals a {(start - history_start).days}-day prefix + {n_days} incremental "
          "days, all four tables, bitwise")
    T2 = incremental_day(g, U, T, end - DAY, history_start)
    assert_same(T2, T, "rerun")
    print("OK  rerunning the last day reproduces all four tables exactly")

    real = g["get_rng"]
    key = {"k": None}
    g["get_rng"] = lambda *p: real(*p, key["k"]) if p and p[0] == "pm_complete" else real(*p)
    try:
        key["k"] = "backfill"
        x = tables(*run(g, U, history_start, end, history_start))
        key["k"] = "prefix"
        T3 = tables(*run(g, U, history_start, start, history_start))
        for d in range(n_days):
            key["k"] = f"day-{d}"
            T3 = incremental_day(g, U, T3, start + d * DAY, history_start)
        differs = not norm(T3["maint"], "maintenance_sk").equals(norm(x["maint"], "maintenance_sk"))
    finally:
        g["get_rng"] = real
    assert differs, "negative control failed: a per-run PM completion matched the backfill"
    print("OK  negative control: a PM completion re-drawn per run does not reproduce the backfill")

    # the Fabric unit bug: Spark's toPandas yields datetime64[us]. The rebuild must give the
    # same series from microsecond-unit records as from nanosecond ones, and end at the snapshot.
    days = pd.date_range(history_start, end - DAY, freq="D")
    assets = g["asset_views"](U["eq"], U["fac"])
    m_ns = full["maint"]
    m_us = m_ns.assign(maintenance_ts=pd.to_datetime(m_ns["maintenance_ts"]).astype("datetime64[us]"))
    a_ns = g["overdue_trajectory"](assets, m_ns, days, history_start, U["calib"])
    a_us = g["overdue_trajectory"](assets, m_us, days, history_start, U["calib"])
    assert (a_ns == a_us).all(), "the overdue rebuild depends on the timestamp unit"
    assert int(a_us[-1]) == int(full["pm"]["is_overdue"].sum()), "the rebuild does not end at the snapshot"
    print(f"OK  the overdue rebuild is unit-proof: microsecond and nanosecond records give the same series, "
          f"ending at the snapshot's {int(a_us[-1])}")


def check_backlogs(g, U, history_start, n_days):
    end = history_start + n_days * DAY
    m, i, l, pm = run(g, U, history_start, end, history_start)
    st, wo = as_of(g, U, end)
    assets = g["asset_views"](U["eq"], U["fac"])
    si = g["state_index"](st)
    days = pd.date_range(history_start, end - DAY, freq="D")
    od = g["overdue_trajectory"](assets, m, days, history_start, U["calib"])
    assert int(od[-1]) == int(pm["is_overdue"].sum()), "the daily rebuild does not end at the snapshot"
    est, young = g["split_cohorts"](assets, history_start)
    fe = g["overdue_flags"](est, m, days, history_start, U["calib"])
    oe, ee = fe.sum(axis=0), g["overdue_entries"](fe)
    oy = g["overdue_trajectory"](young, m, days, history_start, U["calib"])
    assert (oe + oy == od).all()
    fmt = lambda v: " ".join(f"{x:.0f}" for x in g["weekly_means"](v))   # noqa: E731
    share, net, ent = g["net_rise_share"](oe, ee)
    print(f"  overdue PMs weekly, all:            {fmt(od)}")
    print(f"  established ({len(est)}):          {fmt(oe)}")
    print(f"  first PM in window ({len(young)}):  {fmt(oy)}")
    print(f"  established, last {g['NET_WINDOW_DAYS']} d: net {net:+d} against {ent} entries "
          f"= {share:+.3f} (max {g['NET_RISE_MAX_SHARE']}); peak share {oe.max() / len(est):.2%} "
          f"(ceiling {g['OVERDUE_SHARE_MAX']:.0%})")
    assert share < g["NET_RISE_MAX_SHARE"], "established overdue PMs: net rise over half the entries"
    assert not g["strictly_rising"](g["weekly_means"](oe[-g["NET_WINDOW_DAYS"]:])), "monotonic rise"
    assert oe.max() / len(est) <= g["OVERDUE_SHARE_MAX"] and od.max() / len(assets) <= g["OVERDUE_SHARE_MAX"]
    assert (oy <= g["young_bound"](young, days)).all(), "first-PM cohort above its bound"

    # negative controls, one per check, each on the real series with a real defect put in:
    # (1) a leak: from day 90 half the established assets never complete a PM again, so
    #     they enter overdue and stay. The net rise must reach half the entries.
    ks = sorted(est)[::2]
    cut = history_start + 90 * DAY
    leak = m[~(m["equipment_sk"].isin(ks) & (m["maintenance_type"] == "Preventive")
               & (pd.to_datetime(m["maintenance_ts"]) >= cut))]
    fl = g["overdue_flags"](est, leak, days, history_start, U["calib"])
    lsh, lnet, lent = g["net_rise_share"](fl.sum(axis=0), g["overdue_entries"](fl))
    assert lsh >= g["NET_RISE_MAX_SHARE"], f"net-rise check misses a leak ({lsh:+.3f})"
    # (2) a slow steady climb of 0.3/day with noise: every weekly mean above the last
    climb = np.arange(91) * 0.3 + 300 + 2 * np.sin(np.arange(91) * 0.9)
    assert g["strictly_rising"](g["weekly_means"](climb)), "monotonicity check cannot fire"
    assert not g["strictly_rising"]([1.0, 2.0, 2.0, 3.0]) and not g["strictly_rising"]([1.0]),         "strictly_rising is not strict"
    # (3) every PM visit fails: the share passes the ceiling
    fz = g["overdue_flags"](est, m[m["maintenance_type"] != "Preventive"], days, history_start, U["calib"])
    assert fz.sum(axis=0).max() / len(est) > g["OVERDUE_SHARE_MAX"], "share ceiling cannot fire"
    print(f"OK  negative controls: half the estate stops completing PMs from day 90 -> net {lnet:+d} "
          f"against {lent} entries = {lsh:+.3f}, fails; a 0.3/day noisy climb is strictly rising, fails; "
          f"no PM ever completed -> share {fz.sum(axis=0).max() / len(est):.0%}, fails")
    by_fac = {}
    for a in assets.values():
        by_fac.setdefault(a["facility_sk"], []).append(a)
    sv = g["ldar_surveys_in"](si, by_fac, U["fac"], history_start, end, history_start)
    det, rep, out = g["ldar_cumulative"](sv, days)
    mi = st[(st["state"] == "Maintenance") & st["end_ts"].notna()]
    p = m[m["maintenance_type"] == "Preventive"].merge(
        mi, left_on=["equipment_sk", "maintenance_ts"], right_on=["equipment_sk", "end_ts"], how="left")
    assert p["end_ts"].notna().all(), "a preventive record not on a Maintenance interval"
    sched = p[p["cause"] == "Scheduled PM"]
    comp = sched["is_completed"].mean()
    print(f"  {n_days} days: {len(m)} maintenance ({len(sched)} PM visits, completed {comp:.1%}), "
          f"{len(i)} inspections, {len(sv)} surveys")
    print("  overdue PMs by 15 days: " + "  ".join(str(int(od[k:k + 15].mean())) for k in range(0, len(od), 15)))
    print("  LDAR outstanding by 15 days: " + "  ".join(str(int(out[k:k + 15].mean())) for k in range(0, len(out), 15)))
    print(f"  cumulative detected {det[-1]}, repaired {rep[-1]}")
    assert abs(comp - g["PM_ON_TIME_COMPLETION"]) < 0.05, f"PM completion {comp:.1%}"
    lag, lo = g["LDAR_REPAIR_LAG_DAYS"], g["LDAR_REPAIR_BAND"][0]
    assert all(rep[k] >= lo * det[k - lag] for k in range(g["WARMUP_DAYS"], len(days))), "repairs lag"
    assert (rep <= det).all()
    w0 = g["WARMUP_DAYS"]
    ls, ln, le = g["net_rise_share"](out[w0:], np.diff(det, prepend=0)[w0:])
    print(f"  LDAR outstanding after warm-up: net {ln:+d} against {le} detected = {ls:+.3f}")
    assert ls < g["NET_RISE_MAX_SHARE"], "LDAR outstanding: net rise over half the detections"
    assert not g["strictly_rising"](g["weekly_means"](out[w0:][-g["NET_WINDOW_DAYS"]:])), "LDAR monotonic"
    ag, an, ao = g["aged_outstanding_share"](sv, end)
    print(f"  LDAR aged outstanding: {ao} of {an} leaks aged {g['AGED_MIN_DAYS']}-{g['NET_WINDOW_DAYS']} d "
          f"still open = {ag:.1%} (max {g['AGED_OUTSTANDING_MAX']:.0%})")
    assert an > 0 and ag <= g["AGED_OUTSTANDING_MAX"], "LDAR aged outstanding over its maximum"

    # LDAR negative controls: the same surveys, with the drain broken from day 90. Each must fail
    # a check that remains; together they are what the removed slope test claimed to catch.
    never = pd.Timestamp("2100-01-01")
    cut = history_start + 90 * DAY

    def ldar_fails(repair_of):
        ends = [d + DAY for d in days]
        dt = [(s["survey_ts"], repair_of(s["survey_ts"], ts, i)) for s in sv
              for i, (ts, _) in enumerate(s["_repairs"])]
        d_ = np.array([sum(1 for a, _ in dt if a < h) for h in ends])
        r_ = np.array([sum(1 for _, b in dt if b < h) for h in ends])
        o_ = d_ - r_
        fired = []
        ratio = min(r_[k] / max(d_[k - lag], 1) for k in range(w0, len(days)))
        if not all(r_[k] >= lo * d_[k - lag] for k in range(w0, len(days))):
            fired.append("repaired>=70% lag")
        nr = g["net_rise_share"](o_[w0:], np.diff(d_, prepend=0)[w0:])[0]
        if nr >= g["NET_RISE_MAX_SHARE"]:
            fired.append("net rise")
        if g["strictly_rising"](g["weekly_means"](o_[w0:][-g["NET_WINDOW_DAYS"]:])):
            fired.append("monotonic")
        a_lo, a_hi = end - g["NET_WINDOW_DAYS"] * DAY, end - g["AGED_MIN_DAYS"] * DAY
        aged = [b for a, b in dt if a_lo <= a < a_hi]
        ag_ = sum(1 for b in aged if b >= end) / max(len(aged), 1)
        if ag_ > g["AGED_OUTSTANDING_MAX"]:
            fired.append("aged outstanding")
        w = o_[-14:].astype(float)
        sl = float(np.polyfit(np.arange(14.0), w, 1)[0]) * 13 > 0.25 * max(w.mean(), 1.0)
        print(f"    {'':<4}min repaired/detected-30d {ratio:.2f} (floor {lo}); net rise share {nr:+.3f} "
              f"(max {g['NET_RISE_MAX_SHARE']}); aged {ag_:.1%} (max {g['AGED_OUTSTANDING_MAX']:.0%}); weekly {' '.join(f'{v:.0f}' for v in g['weekly_means'](o_[w0:][-g['NET_WINDOW_DAYS']:]))}; "
              f"removed slope would {'FIRE' if sl else 'pass'}")
        return fired, int(o_[-1])

    ctl = {"no repairs from day 90": lambda a, b, i: never if a >= cut else b,
           "half never repaired from day 90": lambda a, b, i: never if a >= cut and i % 2 == 0 else b,
           "repairs 3x slower from day 90": lambda a, b, i: a + (b - a) * 3 if a >= cut else b}
    # NOT CAUGHT BY CHOICE, asserted so any change that starts catching it is noticed: 03d's LDAR
    # checks catch a drain that loses leaks (none repaired; half never repaired, through aged
    # outstanding), not one that is merely slow. Repairs 3x slower read ~16% aged outstanding
    # against a healthy ~4% and a 20% limit. The comment beside 03d's AGED_OUTSTANDING_MAX says why.
    must_fire = {"no repairs from day 90", "half never repaired from day 90"}
    for lbl, fn in ctl.items():
        print(f"  LDAR control: {lbl}")
        fired, o_end = ldar_fails(fn)
        if lbl in must_fire:
            assert fired, f"no remaining LDAR check fires when {lbl}"
            print(f"OK  caught: {', '.join(fired)} (outstanding {o_end} against {int(out[-1])})")
        else:
            assert not fired, (f"{lbl} is now caught ({', '.join(fired)}), which 03d deliberately does "
                               "not do -- see the comment beside AGED_OUTSTANDING_MAX before accepting it")
            print(f"NOT CAUGHT (by design: slow, not lost)  no check fires (outstanding {o_end} against {int(out[-1])})")
    ins = pd.DataFrame(i if isinstance(i, list) else [])
    ii = g["inspections_in"](si, assets, history_start, end, end, history_start, set(), [])
    c = np.array([e["_cond"] for e in ii])
    lf = np.array([e["result"] == "Leak Found" for e in ii])
    qs = np.quantile(c, [0.2, 0.8])
    lo_rate, hi_rate = lf[c <= qs[0]].mean(), lf[c >= qs[1]].mean()
    print(f"  Leak Found rate: bottom condition quintile {lo_rate:.1%}, top {hi_rate:.1%}")
    assert hi_rate > lo_rate, "Leak Found does not rise with condition"
    print("OK  backlogs: overdue PMs and LDAR outstanding drain (net rise, monotonicity, share ceiling, "
          "repairs keep pace with a 30-day lag); PM completion on target; Leak Found rises with "
          "condition; every PM on a stop")


if __name__ == "__main__":
    print("=" * 84)
    print("03d MAINTENANCE, INSPECTIONS AND LDAR")
    print("=" * 84)
    g = load_model()
    check_static(g)
    H0 = pd.Timestamp(g["TOPOLOGY_AS_OF"]) - pd.Timedelta(days=g["STATE_HISTORY_DAYS"])  # 03d's HISTORY_START
    U = upstream(g, H0, H0 + 180 * DAY)
    print(f"  synthetic upstream: {len(U['eq'])} assets, {len(U['state']):,} state intervals, "
          f"{int((U['state']['cause'] == 'Scheduled PM').sum())} Scheduled PM stops, {len(U['wo'])} work orders")
    check_determinism(g, U, H0, H0 + 60 * DAY, 30)
    check_backlogs(g, U, H0, 180)
