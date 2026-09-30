"""03e's production, financial and snapshot model: the notebook's own model cell, verbatim.

1. Static. fact_emission_episode is named only in GROUND_TRUTH_TABLES; every read goes through
   read_input(). Every definition 03e copies is character-for-character its owner's:
   open_measures / observable_trajectory from 03b, open_trajectory from 03c, and 03d's PM
   state, asset condition and LDAR repair constants. The constants 03e shares by value agree
   with their owners: 03c's fine scenario and social cost, 02e's 8-hour offline KPI, 03c's
   state-case event types, 03d's LDAR repair median and sigma.

2. The pure model. The risk score stays inside [0, 100] with every component inside its
   weight, at the extremes and on random inputs. An emission row's attributed mass is
   rate x duration x allocation, which is the IME, and its impact band is ordered. A fine row
   carries the notice's fine and nothing else. Social cost is zero with the switch off.

The whole-notebook runs (03c -> 03b -> 03d -> 03e on the offline estate, backfill against a
sequence of incremental days with every source cut to each horizon) need the pandas Spark
stub and are not part of this file.
"""
import ast
import contextlib
import io
import numpy as np
import pandas as pd
import shared_defs as S
import harness_episodes as HE

NB_03E = S.NB / "03_enterprise/03e_gen_financial_snapshots.Notebook/notebook-content.py"
NB_03B = S.NB / "03_enterprise/03b_gen_work_orders.Notebook/notebook-content.py"
NB_03C = S.NB / "03_enterprise/03c_gen_compliance.Notebook/notebook-content.py"
NB_03D = S.NB / "03_enterprise/03d_gen_maintenance.Notebook/notebook-content.py"
NB_02E = S.NB / "02_scada/02e_gen_ch4_telemetry.Notebook/notebook-content.py"
MODEL_MARKER = "# ---- 03e financial model (pure"

COPIED = {
    NB_03B: ["WO_OPEN_STATUSES", "WO_CANCEL_AFTER_DAYS", "open_measures", "observable_trajectory"],
    NB_03C: ["CE_DECIDED", "open_trajectory"],
    NB_03D: ["PM_ON_TIME_COMPLETION", "PM_CATCHUP_SHARE", "PM_SEED_CYCLES", "SEED_CALIBRATION_DAYS",
             "LDAR_CADENCE_DAYS", "LDAR_EPOCH", "LDAR_REPAIR_MAX_DAYS", "LDAR_DELAY_SHARE",
             "LDAR_DELAY_DAYS", "LDAR_REPAIR_COST", "FEDERAL_FROM", "pm_phase_days", "asset_views",
             "ldar_program", "state_index", "days_since_service", "condition", "pm_completes",
             "seed_calibration", "pm_seed", "next_due", "overdue_flags"],
}


def _model_cell(path, marker):
    cell = [c for c in HE.code_cells(path) if marker in c]
    assert len(cell) == 1, f"expected one model cell in {path.parent.name}, found {len(cell)}"
    return cell[0]


def load_model(path=NB_03E, marker=MODEL_MARKER):
    g = HE.load_config()
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(_model_cell(path, marker), path.parent.name, "exec"), g)
    return g


def check_static():
    code = "\n".join(("pass  # " + ln) if ln.lstrip().startswith("%") else ln
                     for c in HE.code_cells(NB_03E) for ln in c.split("\n"))
    tree = ast.parse(code)
    ref = [n for n in tree.body if isinstance(n, ast.Assign)
           and any(isinstance(t, ast.Name) and t.id == "GROUND_TRUTH_TABLES" for t in n.targets)]
    assert len(ref) == 1 and ast.literal_eval(ref[0].value) == ("fact_emission_episode",)
    inside = {id(x) for x in ast.walk(ref[0])}
    leaked = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)
              and isinstance(n.value, str) and id(n) not in inside and "emission_episode" in n.value]
    assert not leaked, f"03e names the ground-truth table outside the refusal list: {leaked}"
    reads = [ast.get_source_segment(code, n) for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and n.func.attr in ("table", "sql", "load", "parquet", "csv")
             and not (n.func.attr == "table" and isinstance(n.func.value, ast.Attribute)
                      and n.func.value.attr == "catalog")]
    assert reads == ["spark.table(name)"], f"03e reads a table other than through read_input(): {reads}"

    mine = S.extract(NB_03E, [n for names in COPIED.values() for n in names])
    for src, names in COPIED.items():
        theirs = S.extract(src, names)
        bad = [n for n in names if mine[n] != theirs[n]]
        assert not bad, f"03e's copy of {src.parent.name}'s {bad} is not identical to the original"

    import harness_compliance as HC
    import harness_maintenance as HM
    e, c, d = load_model(), HC.load_model(), HM.load_model()
    assert e["FINE_SCENARIO"] == c["ACTIVE_FINE_SCENARIO"], \
        f"03e's FINE_SCENARIO {e['FINE_SCENARIO']!r} is not 03c's {c['ACTIVE_FINE_SCENARIO']!r}"
    assert e["SOCIAL_COST_USD_PER_T_CH4"] == c["FINE_SCENARIOS"]["social_cost"]["per_tonne_usd"], \
        "03e's social cost is not 03c's social_cost scenario"
    assert set(e["STATE_CASE_TYPES"]) == {t for t, r in c["REG_OF_EVENT"].items() if r == "TX_RRC_SWR32"}, \
        "03e's state-case types are not 03c's SWR 32 events"
    assert (e["LDAR_REPAIR_MEDIAN_DAYS"], e["LDAR_REPAIR_SIGMA"]) == \
        (d["LDAR_REPAIR_MEDIAN_DAYS"], d["LDAR_REPAIR_SIGMA"]), "LDAR repair median / sigma differ from 03d's"
    kpi = S.extract(NB_02E, ["KPI_HOURS"])["KPI_HOURS"]
    assert kpi == f"KPI_HOURS = {e['OFFLINE_KPI_HOURS']}", f"02e's offline KPI is {kpi!r}"
    n = sum(len(v) for v in COPIED.values())
    print(f"OK  fact_emission_episode named only in the refusal list; one read path, read_input(); "
          f"{n} copied definitions identical to 03b / 03c / 03d; fine scenario, social cost, state-case "
          "types, LDAR repair constants and the 8-hour offline KPI agree with their owners")
    return e


def check_model(g):
    W = g["RISK_WEIGHTS"]
    top = g["risk_points"](1.0, 99, 99, 99, 1.0, 999, 999)
    assert top == W and abs(sum(top.values()) - g["RISK_RANGE"][1]) < 1e-9, "the maximum is not 100"
    assert sum(g["risk_points"](0.0, 0, 0, 0, 0.0, 0, 0).values()) == 0.0
    rng = np.random.default_rng(3)
    for _ in range(5000):
        p = g["risk_points"](float(rng.uniform(0, 1.2)), int(rng.integers(0, 6)), int(rng.integers(0, 3)),
                             int(rng.integers(0, 4)), float(rng.uniform(0, 0.6)), int(rng.integers(0, 12)),
                             int(rng.integers(0, 12)))
        assert all(0.0 <= p[c] <= W[c] for c in W) and 0.0 <= sum(p.values()) <= 100.0

    t = pd.Timestamp("2026-09-01 19:30:00")
    pl = pd.DataFrame([{"plume_id": "00000000000a", "detect_ts": t, "emission_rate_kg_h": 20000.0,
                        "emission_rate_p5_kg_h": 9000.0, "emission_rate_p50_kg_h": 19000.0,
                        "emission_rate_p95_kg_h": 45000.0, "t_mix_s": 2700.0,
                        "attributed_facility_id": "GS-0001"},
                       {"plume_id": "00000000000b", "detect_ts": t, "emission_rate_kg_h": 5000.0,
                        "emission_rate_p5_kg_h": 1.0, "emission_rate_p50_kg_h": 2.0,
                        "emission_rate_p95_kg_h": 3.0, "t_mix_s": 1.0, "attributed_facility_id": None}])
    rows = g["emission_rows"](pl, t.normalize(), t.normalize() + pd.Timedelta(days=1), {"GS-0001": 1},
                              {1: "GS-0001"}, {"00000000000a": (11, 22)})
    assert len(rows) == 1, "an unattributed plume was charged"
    r = rows[0]
    assert abs(r["attributed_emissions_kg"] - 20000.0 * 0.75) < 1e-6, "attributed mass != rate x t_mix"
    assert r["total_impact_usd_p5"] <= r["total_impact_usd_p50"] <= r["total_impact_usd_p95"]
    assert abs(r["lost_gas_value_usd"] - 15000.0 / g["KG_CH4_PER_MCF"] * g["GAS_PRICE_USD_PER_MCF"]) < 0.01
    assert r["social_cost_usd"] == 0.0 and not g["SOCIAL_COST_ON"], "a social cost with the switch off"
    assert (r["compliance_sk"], r["regulation_sk"]) == (11, 22) and r["violation_fine_usd"] == 0.0
    ce = pd.DataFrame([{"compliance_sk": 5, "facility_sk": 1, "regulation_sk": 33, "status": "Violation",
                        "status_ts": t, "fine_usd": 12345.678, "source_ref": "00000000000a"},
                       {"compliance_sk": 6, "facility_sk": 1, "regulation_sk": 33, "status": "Closed",
                        "status_ts": t, "fine_usd": 0.0, "source_ref": "123"}])
    f = g["fine_rows"](ce, t.normalize(), t.normalize() + pd.Timedelta(days=1), {"00000000000a"}, {1: "GS-0001"})
    assert len(f) == 1 and f[0]["violation_fine_usd"] == 12345.68 and f[0]["plume_id"] == "00000000000a"
    assert f[0]["total_impact_usd_p5"] == f[0]["total_impact_usd_p95"] == 12345.68
    assert f[0]["attributed_emissions_kg"] == 0.0
    print("OK  risk score inside [0, 100] with every component inside its weight (5,000 draws and the "
          "extremes); attributed mass = rate x t_mix; band ordered; unattributed plumes not charged; "
          "a fine row carries the notice's fine only; social cost off")


if __name__ == "__main__":
    print("=" * 84)
    print("03e PRODUCTION, FINANCIAL IMPACT AND FACILITY DAILY SNAPSHOTS")
    print("=" * 84)
    check_model(check_static())
