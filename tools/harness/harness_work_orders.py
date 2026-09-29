"""03b's work orders: the notebook's own model cell, run against 01_topology_config.

1. Static. fact_emission_episode is named in 03b's code only inside GROUND_TRUTH_TABLES, the
   refusal list; every table read goes through read_input() (or the write's own row-count
   check). SENSOR_OFFLINE_MULTIPLE equals 02d's, and the outage cap 03b's ongoing-outage scan
   relies on equals 02b's TELEMETRY_OUTAGE_MAX_H.

2. Determinism. Over a synthetic estate (harness_episodes.estate, 02a's real state machine)
   and a synthetic source stream, where every upstream table is cut to what it would hold at
   each run's horizon -- alarms raised by then, flagged readings by then, outages whose
   returning reading exists by then, outages still running at the horizon seen only through
   the tag's last reading:
     - two backfills are identical
     - a 30-day backfill equals 30 one-day incremental runs, both tables, bitwise, with each
       run writing through the notebook's own merge_for_write() and replaceWhere semantics
     - rerunning the last day changes nothing
   Negative controls, so the checks can fail: pass 1 reading `status != 'Closed'` loses the
   rerun day's closures; a plan re-drawn per run instead of fixed at creation makes the
   incremental sequence disagree with the backfill.

3. Steady state, on a 120-day synthetic run: realised open within [0.4, 2.5] x lambda x T,
   no upward trend over the last 14 days, stall share near 5%, breach share by priority in
   band. And the stalled-ticket exit: nothing open past WO_CANCEL_AFTER_DAYS, and once 60
   days of history exist the stalled-and-open population levels off at about
   stall arrivals x 60 days instead of growing without bound.
   The determinism checks also run with a 7-day exit, so cancellations fall inside the
   30-day run and cross incremental-run boundaries. The synthetic stream is sized roughly to fact_scada_alarm (about 7,500 alarms and
   1,060 P1 per 30 days, against 7,415 and 1,043; P2 is light at ~2,560 against 3,099) but not
   to its clustering, so arrival counts here are NOT the calibration -- that is the notebook's
   calibration note, measured on the realistic stream.
"""
import ast
import contextlib
import io
import numpy as np
import pandas as pd
import shared_defs as S
import harness_episodes as HE

NB_03B = S.NB / "03_enterprise/03b_gen_work_orders.Notebook/notebook-content.py"
NB_02D = S.NB / "02_scada/02d_gen_alarms.Notebook/notebook-content.py"
MODEL_MARKER = "# ---- 03b work-order model (pure"
DAY = pd.Timedelta(days=1)


def load_model():
    g = HE.load_config()
    cell = [c for c in HE.code_cells(NB_03B) if MODEL_MARKER in c]
    assert len(cell) == 1, f"expected one 03b model cell, found {len(cell)}"
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(cell[0], "03b_model_cell", "exec"), g)
    return g


# --- 1. static -------------------------------------------------------------------------------------
def check_static(g):
    code = "\n".join(("pass  # " + ln) if ln.lstrip().startswith("%") else ln
                     for c in HE.code_cells(NB_03B) for ln in c.split("\n"))
    tree = ast.parse(code)
    refusal = [n for n in tree.body if isinstance(n, ast.Assign)
               and any(isinstance(t, ast.Name) and t.id == "GROUND_TRUTH_TABLES"
                       for t in n.targets)]
    assert len(refusal) == 1, "03b must define GROUND_TRUTH_TABLES exactly once"
    assert ast.literal_eval(refusal[0].value) == ("fact_emission_episode",)
    inside = {id(x) for x in ast.walk(refusal[0])}
    leaked = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)
              and isinstance(n.value, str) and id(n) not in inside
              and "emission_episode" in n.value]
    assert not leaked, f"03b's code names the ground-truth table outside the refusal list: {leaked}"
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    assert "fact_emission_episode" not in names

    reads = [ast.get_source_segment(code, n) for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and n.func.attr in ("table", "sql", "load", "parquet", "csv")
             and not (n.func.attr == "table" and isinstance(n.func.value, ast.Attribute)
                      and n.func.value.attr == "catalog")]
    bad = [r for r in reads if r not in {"spark.table(name)", "spark.table(table)"}]
    assert not bad, f"03b reads a table other than through read_input(): {bad}"

    d = S.extract(NB_02D, ["SENSOR_OFFLINE_MULTIPLE"])["SENSOR_OFFLINE_MULTIPLE"]
    assert float(d.split("=", 1)[1].split("#")[0]) == g["SENSOR_OFFLINE_MULTIPLE"], \
        "SENSOR_OFFLINE_MULTIPLE drifted from 02d"
    b = S.extract(S.NB_02B, ["TELEMETRY_OUTAGE_MAX_H"])["TELEMETRY_OUTAGE_MAX_H"]
    assert float(b.split("=", 1)[1].split("#")[0]) == g["CH4_OUTAGE_MAX_H"], \
        "02b's outage cap is no longer CH4_OUTAGE_MAX_H; OFFLINE_SCAN_DAYS is sized on it"
    print("OK  fact_emission_episode named only in GROUND_TRUTH_TABLES; every read through "
          "read_input(); SENSOR_OFFLINE_MULTIPLE and the outage cap agree with 02d / 02b")


# --- 2. a synthetic upstream -----------------------------------------------------------------------
def upstream(g, start, end):
    """Everything 03b reads, over [start, end), uncut. Shaped to fact_scada_alarm's totals."""
    rng = g["get_rng"]("h_wo_upstream")
    assets = HE.estate(g)
    inst = assets[assets["equipment_type"].isin(g["INSTRUMENTABLE_EQUIPMENT"])
                  & assets["criticality"].isin(["Critical", "High"])]
    inst = inst.sort_values(["facility_sk", "equipment_sk"]).groupby("facility_sk").head(12)
    days = (end - start) / DAY
    grid = lambda t, cad: t.floor(f"{cad}s")                      # noqa: E731

    tags, alarms, outages = [], [], []
    for a in inst.itertuples():
        crit = a.criticality == "Critical"
        for j in range(int(rng.integers(4, 10))):
            tag_sk = a.equipment_sk * 100 + j
            tag_id = f"{a.facility_id}.A1.XT-{101 + j}"
            cad = 300 if crit else 900
            tags.append((tag_sk, tag_id, a.equipment_sk, cad))
            # bursts: a tag sitting near a limit alarms several times in a few hours
            for _ in range(int(rng.poisson(0.74 * days / 30))):
                t = start + (end - start) * float(rng.random())
                for _k in range(1 + int(rng.geometric(0.55)) - 1):
                    pr = ("P1" if rng.random() < 0.14 else ("P2" if crit else "P3"))
                    raised = grid(t, cad)
                    dur = pd.Timedelta(minutes=float(rng.lognormal(np.log(25), 1.2)))
                    alarms.append((g["stable_key"]("alarm", tag_id, pr, raised.isoformat()),
                                   tag_sk, tag_id, a.equipment_sk, "Hi" if pr != "P1" else "HiHi",
                                   pr, raised, raised + dur))
                    t = t + pd.Timedelta(hours=float(rng.exponential(3.0)))
            for _ in range(int(rng.poisson((8.0 if rng.random() < 0.02 else 1.0) * days / 45))):
                off = grid(start + (end - start) * float(rng.random()), cad)
                h = min(6.0 * np.exp(0.894 * rng.standard_normal()), 120.0)
                outages.append((tag_sk, tag_id, a.equipment_sk, off,
                                grid(off + pd.Timedelta(hours=h), cad) + pd.Timedelta(seconds=cad)))
    al = pd.DataFrame(alarms, columns=["alarm_sk", "tag_sk", "tag_id", "equipment_sk",
                                       "alarm_type", "priority", "raised_ts", "cleared_ts"])
    al = al.drop_duplicates("alarm_sk").reset_index(drop=True)
    out = pd.DataFrame(outages, columns=["tag_sk", "tag_id", "equipment_sk", "off_ts", "back_ts"])

    # CH4: 4 detectors per facility, runs of consecutive flagged hours, median ~6 h
    sens, flags = [], []
    for f, grp in assets.groupby("facility_sk"):
        for k, a in enumerate(grp.head(4).itertuples()):
            sid = f"SNS-{f * 10 + k:05d}"
            sens.append((sid, a.equipment_sk))
            for _ in range(int(rng.poisson(0.87 * days / 30))):
                t0 = (start + (end - start) * float(rng.random())).floor("h")
                n = max(1, int(rng.lognormal(np.log(6), 0.8)))
                hours = [t0 + pd.Timedelta(hours=i) for i in range(n)]
                if n > 8 and rng.random() < 0.3:            # a dropout splits the run
                    hours.pop(int(rng.integers(1, n - 1)))
                flags += [(sid, h) for h in hours if h < end]
    sen = pd.DataFrame(sens, columns=["sensor_id", "equipment_sk"])
    fl = (pd.DataFrame(flags, columns=["sensor_id", "reading_ts"])
          .drop_duplicates().sort_values(["sensor_id", "reading_ts"]).reset_index(drop=True))

    st = HE.full_state(g, assets, start - pd.Timedelta(days=60), end + DAY)
    sub = pd.read_csv(io.StringIO("\n".join(["facility_sk,sub_basin"] + [
        f"{f},{list(g['ANCHORS'])[f % 4]}" for f in assets["facility_sk"].unique()])))
    ax = assets.merge(sub, on="facility_sk")
    ctx = {"equipment": {int(r.equipment_sk): {"facility_sk": int(r.facility_sk),
                                                "facility_id": r.facility_id,
                                                "area_sk": int(r.area_sk),
                                                "equipment_type": r.equipment_type,
                                                "sub_basin": r.sub_basin}
                         for r in ax.itertuples()},
           "team_name": g["TEAM_ROSTER"]}
    return dict(alarms=al, outages=out, sensors=sen, flags=fl, state=st, ctx=ctx,
                tags=pd.DataFrame(tags, columns=["tag_sk", "tag_id", "equipment_sk", "cad"]))


def as_of(g, U, horizon, source_start):
    """03b's sources and stops as the notebook would build them at this horizon."""
    al = U["alarms"][U["alarms"]["raised_ts"] < horizon].copy()
    al.loc[al["cleared_ts"] >= horizon, "cleared_ts"] = pd.NaT
    fl = U["flags"][U["flags"]["reading_ts"] < horizon]
    o = U["outages"]
    done = o[o["back_ts"] < horizon]
    leaves = done.rename(columns={"off_ts": "event_ts"}).assign(
        duration_hours=(done["back_ts"] - done["off_ts"]).dt.total_seconds() / 3600.0)
    run = o[(o["off_ts"] < horizon) & (o["back_ts"] >= horizon)].merge(
        U["tags"][["tag_sk", "cad"]], on="tag_sk")
    # the notebook sees an ongoing outage only through the tag's last reading, off_ts - cad
    run = run[(horizon - (run["off_ts"] - pd.to_timedelta(run["cad"], unit="s")))
              .dt.total_seconds() > g["SENSOR_OFFLINE_MULTIPLE"] * run["cad"]]
    st = HE.cut_state(U["state"], horizon)
    maint = st[st["state"] == "Maintenance"][["equipment_sk", "start_ts", "end_ts"]]
    src = g["build_sources"](al, fl, U["sensors"], 3600, leaves, run[["tag_sk", "tag_id",
                             "equipment_sk", "off_ts"]], maint, horizon)
    src = src[src["trigger_ts"] >= source_start].reset_index(drop=True)
    stops = {}
    for k, gg in st[st["state"].isin(["Down", "Maintenance"])].groupby("equipment_sk"):
        e = gg["end_ts"].values.astype("datetime64[ns]")
        stops[int(k)] = (gg["start_ts"].values.astype("datetime64[ns]").astype("int64"),
                         np.where(np.isnat(e), np.iinfo("int64").max, e.astype("int64")))
    return src, stops


def backfill(g, U, start, end):
    src, stops = as_of(g, U, end, start)
    rows, ev, ab, _ = g["run_window"]([], 1, src, start, end, U["ctx"], stops)
    return g["to_frames"](rows, ev) + (ab,)


def incremental_day(g, U, wo, ev, day, start, prior_fn=None):
    """One incremental run for [day, day + 1), writing like the notebook does."""
    end = day + DAY
    src, stops = as_of(g, U, end, start)
    if prior_fn is None:
        prior, nxt = g["prior_from_table"](wo, day, U["ctx"])
    else:
        prior, nxt = prior_fn(wo, day)
    rows, events, _, _ = g["run_window"](prior, nxt, src, day, end, U["ctx"], stops)
    ch, epdf = g["to_frames"](rows, events)
    ws, we = int(day.strftime("%Y%m%d")), int(day.strftime("%Y%m%d"))
    lo = min([ws] + ch["date_sk"].tolist())
    ex = wo[(wo["date_sk"] >= lo) & (wo["date_sk"] <= we)]
    w = g["merge_for_write"](ex, ch, day)
    wo = pd.concat([wo[(wo["date_sk"] < lo) | (wo["date_sk"] > we)], w], ignore_index=True)
    ev = pd.concat([ev[(ev["date_sk"] < ws) | (ev["date_sk"] > we)], epdf], ignore_index=True)
    return norm(g, wo), norm_ev(ev)


def norm(g, wo):
    w = wo[g["WO_COLS"]].copy()
    w["tag_sk"] = w["tag_sk"].astype("Int64")
    for c in ("resolution_hours", "downtime_hours", "cost_usd"):
        w[c] = w[c].astype("float64")
    for c in ("is_breached", "is_stalled", "is_synthetic"):
        w[c] = w[c].astype(bool)
    w["closed_ts"] = pd.to_datetime(w["closed_ts"])
    return w.sort_values("work_order_id", kind="mergesort").reset_index(drop=True)


def norm_ev(ev):
    return ev.sort_values(["event_ts", "work_order_id", "to_status"],
                          kind="mergesort").reset_index(drop=True)


def check_determinism(g, U, start, n_days):
    end = start + n_days * DAY
    wo_b, ev_b, _ = backfill(g, U, start, end)
    wo_b2, ev_b2, _ = backfill(g, U, start, end)
    pd.testing.assert_frame_equal(norm(g, wo_b), norm(g, wo_b2), check_exact=True)
    pd.testing.assert_frame_equal(norm_ev(ev_b), norm_ev(ev_b2), check_exact=True)
    print(f"OK  two backfills identical ({len(wo_b):,} tickets, {len(ev_b):,} transitions)")

    # one-day backfill of day 1, then n_days - 1 incremental days
    wo, ev, _ = backfill(g, U, start, start + DAY)
    wo, ev = norm(g, wo), norm_ev(ev)
    snapshots = {}
    for d in range(1, n_days):
        day = start + d * DAY
        snapshots[d] = (wo, ev)
        wo, ev = incremental_day(g, U, wo, ev, day, start)
    pd.testing.assert_frame_equal(wo, norm(g, wo_b), check_exact=True)
    pd.testing.assert_frame_equal(ev, norm_ev(ev_b), check_exact=True)
    print(f"OK  the {n_days}-day backfill equals 1 backfill day + {n_days - 1} incremental "
          "days, both tables, bitwise")

    last = start + (n_days - 1) * DAY
    wo_r, ev_r = incremental_day(g, U, wo, ev, last, start)
    pd.testing.assert_frame_equal(wo_r, wo, check_exact=True)
    pd.testing.assert_frame_equal(ev_r, ev, check_exact=True)
    print("OK  rerunning the last day reproduces both tables exactly")

    # negative control 1: pass 1 reads status != 'Closed' on the stored (later) rows
    def naive(wo_rows, ws):
        tk, nxt = g["prior_from_table"](wo_rows, ws, U["ctx"])
        still = set(wo_rows.loc[wo_rows["status"] != "Closed", "work_order_id"])
        return [t for t in tk if t["work_order_id"] in still], nxt
    wo_n, ev_n = incremental_day(g, U, wo, ev, last, start, prior_fn=naive)
    lost = len(ev) - len(ev_n)
    assert lost > 0, "negative control failed: the naive pass-1 filter lost nothing on a rerun"
    print(f"OK  negative control: pass 1 on status != 'Closed' loses {lost} transition(s) when "
          "the last day is rerun")

    # negative control 2: the plan re-drawn per run instead of fixed at creation
    real = g["get_rng"]
    run_key = {"k": None}
    g["get_rng"] = lambda *p: real(*p, run_key["k"]) if p and p[0] == "work_order" else real(*p)
    try:
        run_key["k"] = "backfill"
        wo_x, _, _ = backfill(g, U, start, end)
        run_key["k"] = "day-1"
        wo2, ev2, _ = backfill(g, U, start, start + DAY)
        wo2, ev2 = norm(g, wo2), norm_ev(ev2)
        for d in range(1, n_days):
            run_key["k"] = f"day-{d}"
            day = start + d * DAY
            try:
                wo2, ev2 = incremental_day(g, U, wo2, ev2, day, start)
            except AssertionError:
                break               # prior_from_table catches the re-drawn plan and refuses
        differs = not norm(g, wo_x).equals(wo2)
    finally:
        g["get_rng"] = real
    assert differs, "negative control failed: a per-run plan still matched the backfill"
    print("OK  negative control: a plan re-drawn each run does not reproduce the backfill "
          "(and prior_from_table refuses the mismatched rows)")


# --- 3. steady state ------------------------------------------------------------------------------
def check_steady_state(g, U, start, n_days):
    end = start + n_days * DAY
    wo, ev, ab = backfill(g, U, start, end)
    days = pd.date_range(start, end - DAY, freq="D")
    traj = g["open_trajectory"](wo, days)
    live = wo[~wo["is_stalled"]]
    plans = [g["ticket_plan"](r.work_order_id, r.priority, r.source,
                              U["ctx"]["equipment"][int(r.equipment_sk)]["equipment_type"])
             for r in live.itertuples()]
    planned = np.array([p["resolution_s"] / 3600.0 for p in plans])
    tw = days[-g["TREND_WINDOW_DAYS"]:]
    lam = int(((live["created_ts"] >= tw[0]) & (live["created_ts"] < tw[-1] + DAY)).sum()) / len(tw)
    T = planned.mean() / 24.0
    realised = traj[-len(tw):].mean()
    x = np.arange(len(tw), dtype=float)
    rise = float(np.polyfit(x, traj[-len(tw):].astype(float), 1)[0]) * (len(tw) - 1)
    print(f"  {n_days}-day synthetic run: {len(wo):,} tickets, {len(ab):,} candidates absorbed")
    print(f"  arrivals {lam:.1f}/day (non-stalled), T {T:.2f} d, predicted open {lam * T:.1f}, "
          f"realised {realised:.1f} (ratio {realised / (lam * T):.2f})")
    print(f"  open by week: " + "  ".join(str(int(traj[i:i + 7].mean()))
                                          for i in range(0, len(traj), 7)))
    print(f"  fitted rise over the last {len(tw)} days {rise:+.1f} ({rise / realised:+.1%})")
    lo, hi = g["STEADY_BAND"]
    assert lo * lam * T <= realised <= hi * lam * T, "open count outside the steady-state band"
    assert rise <= g["TREND_MAX_RISE"] * realised, "open count trending upward"
    stall = wo["is_stalled"].mean()
    assert 0.03 <= stall <= 0.07, f"stall share {stall:.1%}"
    for p in g["WO_PRIORITIES"]:
        t = live[live["priority"] == p]
        pl = np.array([g["ticket_plan"](r.work_order_id, r.priority, r.source,
                                        U["ctx"]["equipment"][int(r.equipment_sk)]
                                        ["equipment_type"])["resolution_s"] / 3600.0
                       for r in t.itertuples()])
        if len(pl) < g["WO_BREACH_MIN_TICKETS"]:
            print(f"  {p}: {len(pl)} tickets -- band not checked")
            continue
        share = float((pl > g["WO_SLA_HOURS"][p]).mean())
        b = g["WO_BREACH_BAND"][p]
        print(f"  {p}: {len(pl):>5} tickets, mean resolution {pl.mean():6.1f} h "
              f"(target {g['WO_MEAN_RESOLUTION_HOURS'][p]:.0f}), breach {share:.1%} "
              f"(target {g['WO_BREACH_TARGET'][p]:.0%})")
        assert b[0] <= share <= b[1], f"{p} breach share {share:.1%} outside {b}"
    print(f"OK  steady state: open within the band, no upward trend, stall share {stall:.1%}, "
          "breach shares in band")

    # the stalled-ticket exit
    exit_d = g["WO_CANCEL_AFTER_DAYS"]
    so = g["open_trajectory"](wo, days, stalled=True)
    cx = wo[wo["status"] == "Cancelled"]
    opn = wo[wo["status"].isin(g["WO_OPEN_STATUSES"])]
    assert (opn["created_ts"] + pd.Timedelta(days=exit_d) >= end).all(), "open past the exit"
    assert len(cx) > 0, "no cancellation in a run longer than the exit"
    s_rate = int(wo.loc[wo["created_ts"] >= tw[0], "is_stalled"].sum()) / len(tw)
    s_rise = float(np.polyfit(x, so[-len(tw):].astype(float), 1)[0]) * (len(tw) - 1)
    print(f"  stalled open by week: " + "  ".join(str(int(so[i:i + 7].mean()))
                                                  for i in range(0, len(so), 7)))
    print(f"  stalled open last 14 d {so[-len(tw):].mean():.1f} vs {s_rate:.2f}/day x {exit_d} d "
          f"= {s_rate * exit_d:.1f}; rise {s_rise:+.1f}; cancelled {len(cx):,}")
    assert 0.5 * s_rate * exit_d <= so[-len(tw):].mean() <= 1.5 * s_rate * exit_d,         "stalled-and-open did not converge to stall rate x exit"
    assert s_rise <= g["TREND_MAX_RISE"] * so[-len(tw):].mean(), "stalled population still growing"
    print(f"OK  stalled exit: nothing open past {exit_d} days; stalled-and-open levels off at "
          "stall rate x exit; cancelled tickets never counted open")

    # the dashboard's two measures are OBSERVABLE (within / past SLA), not the stalled split:
    # disjoint, together exactly the open rows, each equal to its own trajectory at the horizon
    act, bkl = g["open_measures"](wo)
    gns, gst = g["generator_split"](wo)
    is_open = wo["status"].isin(g["WO_OPEN_STATUSES"])
    assert not (act & bkl).any(), "a ticket counted as both active open and stalled backlog"
    assert ((act | bkl) == is_open).all() and ((gns | gst) == is_open).all()
    oa, ob = g["observable_trajectory"](wo, days)
    assert int(act.sum()) == int(oa[-1]) and int(bkl.sum()) == int(ob[-1])
    assert int(gns.sum()) == int(traj[-1]) and int(gst.sum()) == int(so[-1])
    assert not wo.loc[act | bkl, "status"].eq("Cancelled").any()
    wk = lambda a: "  ".join(f"{int(a[i:i + 7].mean()):>3}" for i in range(0, len(a), 7))  # noqa
    print("  week by week (mean open at day end):")
    print("    observable  active open (within SLA)  " + wk(oa))
    print("    observable  backlog (past SLA)        " + wk(ob))
    print("    generator   non-stalled open          " + wk(traj))
    print("    generator   stalled open              " + wk(so))
    L = len(tw)
    print(f"  last {L} days: active {oa[-L:].mean():.1f} vs non-stalled {traj[-L:].mean():.1f}; "
          f"backlog {ob[-L:].mean():.1f} vs stalled {so[-L:].mean():.1f}")
    print(f"  cross-tab at the horizon: within SLA & not stalled {int((act & gns).sum())}, "
          f"within SLA & stalled {int((act & gst).sum())}, past SLA & not stalled "
          f"{int((bkl & gns).sum())}, past SLA & stalled {int((bkl & gst).sum())}")
    b_rise = float(np.polyfit(x, ob[-L:].astype(float), 1)[0]) * (L - 1)
    # The splits disagree in both directions -- a stalled ticket still inside its SLA, a
    # normal ticket past it -- so neither is a relabelling of the other. The backlog is NOT
    # asserted larger than the stalled count: the backlog counts ticket-time past SLA, and a
    # normal breach is past SLA for hours while a stalled ticket is for weeks.
    assert int((act & gst).sum()) > 0 and int((bkl & gns).sum()) > 0,         "the observable and generator splits agree exactly -- one is relabelling the other"
    assert b_rise <= g["TREND_MAX_RISE"] * ob[-L:].mean(), "observable backlog still growing"
    print(f"OK  observable measures: disjoint, cover exactly the open tickets, match their "
          f"trajectories; backlog levels off (rise {b_rise:+.1f}); Cancelled in neither")


def check_compliance_mapping(g):
    """Violations raise a ticket at the finding, P1 Critical / P2 Major; everything else raises
    nothing; a case with no asset goes to the facility's responsible asset."""
    ce = pd.DataFrame({
        "compliance_sk": [11, 12, 13, 14], "compliance_id": [f"CE-00000{i}" for i in range(1, 5)],
        "facility_sk": [1, 2, 3, 4], "equipment_sk": pd.array([None, 7, None, None], dtype="Int64"),
        "event_type": ["Venting", "Flaring", "OLRE", "Venting"],
        "severity": ["Critical", "Major", "Minor", "Major"],
        "status": ["Violation", "Violation", "Under Review", "Closed"],
        "status_ts": pd.to_datetime(["2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04"])})
    c = g["compliance_sources"](ce, {1: 101, 2: 102, 3: 103, 4: 104}, pd.Timestamp("2026-09-15"))
    assert list(c["source_ref"]) == [11, 12] and list(c["priority"]) == ["P1", "P2"]
    assert list(c["equipment_sk"]) == [101, 7] and (c["trigger_ts"] == ce["status_ts"][:2]).all()
    later = g["compliance_sources"](ce, {1: 101, 2: 102}, pd.Timestamp("2026-09-02"))
    assert list(later["source_ref"]) == [11], "a violation found after the horizon raised a ticket"
    print("OK  compliance mapping: violations only, at the finding, P1 Critical / P2 Major; "
          "reports, closed and undecided cases raise nothing")


if __name__ == "__main__":
    print("=" * 84)
    print("03b WORK ORDERS")
    print("=" * 84)
    g = load_model()
    check_static(g)
    check_compliance_mapping(g)
    START = pd.Timestamp("2026-08-16")
    U = upstream(g, START, START + 120 * DAY)
    print(f"  synthetic upstream: {len(U['alarms']):,} alarms over 120 days "
          + str(U["alarms"]["priority"].value_counts().to_dict())
          + f", {len(U['flags']):,} flagged CH4 readings, {len(U['outages']):,} outages")
    check_determinism(g, U, START, 30)
    print(f"  -- again with WO_CANCEL_AFTER_DAYS = 7, so cancellations cross run boundaries")
    _exit = g["WO_CANCEL_AFTER_DAYS"]
    g["WO_CANCEL_AFTER_DAYS"] = 7
    try:
        check_determinism(g, U, START, 30)
    finally:
        g["WO_CANCEL_AFTER_DAYS"] = _exit
    check_steady_state(g, U, START, 120)
