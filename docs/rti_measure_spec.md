# Green Sky RTI dashboard: measure specification

A living document. It defines what every number on the RTI board means, before any tile is built. It holds
no tiles, no layout and no board JSON. A tile query is correct when it matches a row of Part 1 and passes
every rule in Part 3; a reviewer checks it against this page and nothing else.

**Status, 2026-10-14.** Parts 1–3 are the proposed definitions. Part 4 holds three decisions that are **not
made**. Part 5 lists what code and data could not settle.

**Update, 2026-10-15.**
- **Decisions.** Decisions A, B and C are now made (Part 4).
- **The catalog was re-derived on 2026-10-14 ~12:00Z** (05 wind fix, commit c93dd7a). Its figures are
  re-read live and marked *(2026-10-15)*.
- **What changed in the catalog:**
  - It holds **50** plumes.
  - The external table shows **41** columns.
  - The re-derivation moved the attribution: **31** facilities have a detection, and **1,445.8 t** is
    attributed in the common window.
- **The enterprise layer has not been regenerated against that attribution** (Part 5, Q12). Figures read
  from `fact_facility_daily_snapshot` are therefore still pre-fix, and are labelled so.
- **Superseded figures.** The 2026-10-14 figures are kept beside the new ones where the change matters.
  They are struck through where they no longer hold.
- **Board design.** The board's tile design is in `rti_board_design.md`.

**Release, 2026-10-15 (afternoon).**
- **The run.** `greensky_enterprise` was re-run as a backfill with `pipeline_date` empty (run `59dedf66`,
  06:31–07:15Z).
- **The gate.** The snapshot now matches the catalog on all 42 plume facility-days in the common window.
- **Q12 is closed.** Snapshot figures below are re-read and marked *(released)*.
- **The release record** is `rti_board_design.md` §14.

**Sources read.** Notebooks: `00_config`, `01_topology_config`, `01a`, `02c`, `03e`, `04`, `05`.
Prior work: `rti_investigation_2026-10-12.md`, `rti_investigation_2_2026-10-12.md`,
`rti_serving_namespace_2026-10-14.md`. Data: read-only KQL against the serving database, run on
2026-10-14 between 10:40 and 11:05 UTC. Every number below was read in that session unless it carries a
citation to a document.

Line references are to `notebook-content.py` under
`Methane Emissions/Accelerator/Planetary Computer/Varon et al Approach/notebooks/`, written
`03e:868` for "03e, line 868". `05` is the folder `05_facility_attribution/05_attribute_facilities .Notebook`
(trailing space).

---

## 0. Serving layer as found

Eventhouse **GreenSky_V2_EH** (`aa842896-5385-40b0-ad0b-d40424ce2a9d`), database **greensky_v2_rti**
(`de7dfd60-0593-4e7c-b654-07065f5a2d6c`), cluster `https://trd-1rwzmf8wysaj2u0zj5.z4.kusto.fabric.microsoft.com`.
Four external tables, all Delta, all over `greensky_v2_lakehouse` `Tables/dbo/<name>`.

| table | `\| count` | `\| summarize count()` | columns | query acceleration (read back) |
|---|---|---|---|---|
| gold_plume_catalog | ~~49~~ **50** *(2026-10-15)* | ~~49~~ **50** | ~~39~~ **41** *(2026-10-15; see R6c)* | `IsEnabled: false` (re-read 2026-10-15) |
| fact_facility_daily_snapshot | 4,500 | 4,500 | 27 | `IsEnabled: false` |
| dim_facility | 150 | 150 | 21 | `IsEnabled: false` |
| scada_telemetry_hourly | 3,265,025 | 3,265,025 | 30 | `IsEnabled: true`, Hot 365000 d |

The two counts agree on every table today. `plume_id` and `scene_id` read as `string` with 0 nulls in 49 rows,
and all 11 attribution columns are present.

*(2026-10-15)* The catalog now has 50 rows and 41 columns.
- **The two new columns** are `era5_u10` and `era5_v10`, both `real`. All 50 rows have values.
- **Column check.** The other three tables were compared with their Delta logs column by column on
  2026-10-15. They match: 27, 21 and 30 columns.
- **The 50th plume** is PL-bfdb1e16e469 (GS-0060, 8,436 kg/h). It is real given its pixels. It comes
  from a granule-assignment non-determinism in 03's dedup tie-break, not from new data.

Acceleration matches the plan in `rti_serving_namespace_2026-10-14.md`:
- **OFF** on the three small tables. Re-read on 2026-10-14 with
  `.show external table <name> policy query_acceleration` for `fact_facility_daily_snapshot` and
  `dim_facility`.
- **ON** for `scada_telemetry_hourly` only.

An earlier read the same day found the snapshot and `dim_facility` ON. They had been left at the default
because their two alters were never run. The alters have since been applied.

---

## Part 1. Measure table

Notation used in the tables:
- **W** is the reporting window. **H_det** and **H_ent** are the horizons defined in R1.
- "Stock" means a value read at one day's end. It is never summed over days (R1c).
- "Flow" means a value that happens on a day. It sums over days.

### 1a. Executive Overview (portfolio)

| # | measure | definition (one sentence) | source table.column | aggregation | units | grain | time anchor | computed at |
|---|---|---|---|---|---|---|---|---|
| E1 | Plumes detected | Number of plumes in the catalog whose `detection_date` falls in W. | `gold_plume_catalog.plume_id`, `.detection_date` | `summarize count()`, one row per plume | plumes | plume | `detection_date`, H_det | 04:1064 (`plume_id = plume_key(scene, source_lat, source_lon)`); 04:1056 (`detection_date` = datetime of the cluster's **first pixel row**, i.e. overpass time) |
| E2 | Plumes attributed / unattributed | E1 split by whether 05 found any facility within 50 km. | `gold_plume_catalog.attribution_method` (`wind_aware` / `no_match`) | count by method; the two must sum to E1 | plumes | plume | `detection_date`, H_det | 05:232–245, 05:253–266; radius 00_config:93 (`50`; the `# 30 km` comment at 05:154 is stale) |
| E3 | Observed plume methane | Methane mass the satellite saw in the plume at overpass (the IME), summed over plumes. It is a **floor on the release, not an emission total**. | catalog: `gold_plume_catalog.ime_kg` (all plumes). Snapshot: `fact_facility_daily_snapshot.total_emissions_kg` (**attributed only**) | sum, then ÷ 1,000 | t CH₄ | plume or facility-day | `detection_date` / `snapshot_date` (R1) | 04:1034 (`ime_kg = Σ enhancement × PPB_TO_KG`); 03e:661 (`kg = rate × t_mix/3600 × 1.0`), 03e:838, 865; 03e asserts `rate × t_mix = ime_kg` at 03e:1087–1089 |
| E4 | Median detected emission rate | Median, across the window's plumes, of 04's point-estimate rate `IME / t_mix`. | `gold_plume_catalog.emission_rate_kg_h` | `percentile(…, 50)` across plumes, never a sum | t/h (kg/h ÷ 1,000) | plume | `detection_date`, H_det | 04:1146, 04:1155 |
| E5 | Largest detected emission rate | The highest point-estimate rate in W, shown with **that plume's** P5–P95. | `.emission_rate_kg_h`, `.emission_rate_p5_kg_h`, `.emission_rate_p95_kg_h` | `arg_max(emission_rate_kg_h, …)` | t/h | plume | `detection_date`, H_det | 04:1155; 04:1243–1245 (Monte Carlo, 500 draws, seeded per plume) |
| E6 | Financial impact (point estimate) | Lost-gas value of the observed methane at $2.75/mcf, plus fines from notices of violation dated at the notice. Social cost is off. | `fact_facility_daily_snapshot.daily_financial_impact` | sum over facility-days in W | USD | facility-day (flow) | `snapshot_date`, H_ent | 03e:839–840, 867; components 03e:635–636 (lost gas), 03e:692 (fine), 03e:639–640 (social, off at 03e:282) |
| E7 | Notices of violation | Compliance cases that reached `Violation` on the day, counted on the notice date. | `.compliance_violations` | sum over facility-days | notices | facility-day (flow) | `snapshot_date`, H_ent | 03e:841–842, 870 |
| E8 | Open work orders | Tickets open at the end of the anchor day, split into within-SLA (`open_tickets`) and past-SLA (`stalled_backlog`). | `.open_tickets` + `.stalled_backlog` | sum over facilities **on one day** | tickets | facility-day (stock) | the anchor day, ≤ H_ent | 03e:370–374 (masks), 03e:377–392 (as-of-day trajectory), 03e:797–798, 868 |
| E9 | Open compliance cases (all types) | Cases of **every type** in Reported / Under Review at the anchor day's end. **Not** the input to the compliance risk component, which counts open **state** cases only (see the note below the table and Decision C). | `.open_compliance_cases` | sum over facilities, one day | cases (all types) | stock | anchor day | 03e:399–407, 799–800, 871 |
| E10 | Overdue preventive maintenance | Installed assets whose PM next-due time has passed at the anchor day's end. | `.overdue_pms` | sum over facilities, one day | assets | stock | anchor day | 03e:578–605, 802–808, 871 |
| E11 | LDAR leaks outstanding | Leaks detected by surveys up to the anchor day's end, minus leaks repaired by then. | `.ldar_leaks_outstanding` | sum over facilities, one day | leaks | stock | anchor day | 03e:851–853, 859, 872 |
| E12 | Facilities with a detection | Distinct facilities with at least one attributed plume in W. | `gold_plume_catalog.attributed_facility_id` | exact distinct count (R7) | facilities | facility | `detection_date`, H_det | 05:234 |
| E13 | Daily observed methane | E3 by day. | `fact_facility_daily_snapshot.total_emissions_kg` by `snapshot_date` (attributed); **plus** the unattributed series from the catalog (R3) | sum by day | t CH₄ per day of detection | day | per source (R1) | 03e:838, 865; 04:1034 |
| E14 | Facility map | Each facility at its `dim_facility` coordinates, sized by E3. Unattributed plumes are drawn at their own `source_lat/lon` with a distinct marker. | `dim_facility.facility_lat/lon`; `gold_plume_catalog.source_lat/lon` | sum per facility | t CH₄ | facility / plume | as E3 | 01a:118, 145–146; 04:1051–1052 (source = the cluster's peak pixel) |
| E15 | Facility risk | **Not defined.** See Decision C. | `.risk_score`, `.risk_pts_*` | — | points (0–100) | facility-day (stock) | anchor day | 03e:764–773, 860–861, 876–877 |

Values in W = [2026-08-16, 2026-09-15), the common window of R1:

| measure | value | how it reconciles |
|---|---|---|
| E1 | ~~48~~ **49** *(2026-10-15)* | Plus 1 plume on 2026-09-15, outside the common window. ~~49~~ **50** in the catalog. |
| E2 | ~~41~~ **42** attributed + 7 unattributed *(2026-10-15)* | 42 + 7 = 49. Of the 42, 41 are `wind_aware` and 1 is `distance_only`. |
| E3 | *(2026-10-15)* **1,445.8 t attributed + 275.2 t unattributed = 1,721.1 t** | The catalog's 43 attributed plumes (to H_det) sum to 1,483.1 t; the 09-15 plume (PL-d6024ee3f97a, 37.3 t) is outside W. *(Released)* The snapshot's attributed total is also 1,445.822 t; it agrees with the catalog for all 150 facilities to within 0.0008 kg. ~~The snapshot held 1,452.0 t under the pre-fix attribution.~~ |
| E6 | **$268,504.28** *(released)*. ~~$285,469.31 (pre-fix)~~ | $206,438.77 lost gas (1,445,822.072 kg ÷ 19.26 × 2.75), plus $62,065.51 in fines from 3 notices. The derived split is 1 cent off the notice days' own sum, $62,065.50, because of 03e's 2-dp rounding. ~~$207,324.86 + $78,144.45.~~ |
| E7 | **3** *(released; different notices: GS-0070 09-06, GS-0061 09-13, GS-0014 08-31, none at a facility with a plume)* | — |
| E12 | **31** *(2026-10-15)* | Exact distinct `attributed_facility_id`, excluding `""`. `count_distinct` counts the empty string as a value, so an unfiltered query returns 32. |

Values read at H_ent's last day (2026-09-14), *(released)*, after the enterprise backfill. Pre-fix values are
struck through. 03b raises tickets from 03c's violations, so E8 moved. E10 and E11 did not.

| measure | value |
|---|---|
| E8 | **74 open within SLA + 52 stalled = 126** ~~77 + 56 = 133~~ |
| E9 | **22** ~~21~~ |
| E10 | 299 (unchanged) |
| E11 | 51 (unchanged) |

**Where the code does what the name does not suggest.**
- **`total_emissions_kg` is not total emissions.** It is the IME, the methane in the plume at the moment of
  overpass, because 03e sets the duration to the plume's own mixing time (03e:59–65, 03e:319–322, 03e:660–661).
  V1's `× 3600` was an implicit one-hour release. V2 claims only the observed mass. A tile titled
  "Total CH₄ Emissions" over this column overstates what was measured; title it "Observed plume methane".
- **`active_plumes` counts only attributed plumes detected that UTC day.** It is built from `plumes_att`
  (03e:814, 03e:832–833, 03e:1090). Unattributed plumes never reach the snapshot (03e:654–655,
  03e:1155–1159). "Active" claims a duration that TROPOMI cannot observe: it sees each place once a day.
- **`open_compliance_cases` (E9) is not what drives `risk_pts_compliance`.** *(2026-10-15)* The two are
  computed from different case sets:
  - **`open_compliance_cases`** counts open cases of **every** type (`ncase`, 03e:800). This is E9 and F11.
  - **`risk_pts_compliance`** uses `min(violations_30d + 0.5 × open_state_cases, 1) × 20` (03e:769, 860).
    `open_state_cases` counts only open cases whose `event_type` is a **state** case type, `Venting` or
    `Flaring` (`STATE_CASE_TYPES`, 03e:346; `nstate`, 03e:801).
  - **So the two disagree on the same facility-day.** Example: GS-0043, 2026-09-06–08, shows **2 open
    compliance cases and 0 compliance points**. Its open cases are not state cases, and it has no Violation
    in the trailing 30 days.
  - **Labelling rule.**
    - E9 is labelled "open compliance cases (all types)".
    - The risk component is labelled "compliance points (violations in 30 days + open state cases)".
    - Neither is presented as the input to the other.
    - A tile that shows both must not imply that more open cases means more compliance points.
- **`open_tickets` excludes tickets past SLA.** It is `open & ~past_sla` (03e:370–374). An "open work orders"
  figure is `open_tickets + stalled_backlog`. V1 showed both 290 and 29,023 under one title; V2 defines one
  measure, E8, and shows the split.
- **`daily_financial_impact` is a point estimate and carries no band.** It is lost gas plus fines plus social
  cost (03e:839–840). Its P5/P50/P95 counterparts exist only in `fact_financial_impact` (03e:677–679), which
  is not a serving table (Part 2).
- **`emission_rate_p50_kg_h` is the Monte Carlo median, not the point estimate** (04:1244). On the 50 plumes
  (2026-10-15; unchanged from 49), p50 sits between −8.7 % and +2.1 % of `emission_rate_kg_h` (median
  −2.6 %). E4 and E5 use the point estimate.
- **The catalog's minimum, 418 kg/h, is an artefact.** PL-8a46231dbd67 had `U_eff_ms` = 0.36 m/s, below
  `min_wind_speed_ms` 0.5 (00_config:86). Its mixing time therefore fell back to a fixed 48 h (04:1138–1143,
  00_config:85), which divides the IME by 172,800 s. The minimum over the ~~48~~ **49** wind-dependent plumes
  is ~~5,439~~ **5,324** kg/h *(2026-10-15)*.
  - **This plume's attribution after the fix.** It is the one `distance_only` row: its wind is below
    0.5 m/s, so 05 no longer scores it on direction.

### 1b. Facility Operations (one facility, `f`)

Every measure is E-row logic filtered to `f`. The rows below are the ones that exist only at facility grain,
or that need a different rule there.

| # | measure | definition (one sentence) | source table.column | aggregation | units | grain | time anchor | computed at |
|---|---|---|---|---|---|---|---|---|
| F1 | Plumes attributed to f | Plumes whose `attributed_facility_id = f`, in W. | `gold_plume_catalog.attributed_facility_id` | count | plumes | plume | `detection_date`, H_det | 05:234, 05:238 |
| F2 | Observed methane at f | E3 for f. | `fact_facility_daily_snapshot.total_emissions_kg` | sum over days | t CH₄ | facility-day (flow) | `snapshot_date`, H_ent | 03e:838, 865 |
| F3 | Plume list | One row per plume at f: detection time, point rate, P5–P95, IME, `t_mix_method`, `confidence`. | `gold_plume_catalog` row | none (row-level) | t/h, t | plume | `detection_date` | 04:1155, 1243–1245, 1034, 1153; `confidence` 04:1276–1307 |
| F4 | Attribution evidence | Per plume: distance, number of candidates in range, the runner-up facility and its share. | `.attribution_distance_km`, `.facilities_in_range`, `.second_facility_id`, `.second_facility_probability` | row-level | km, count, share | plume | — | 05:238–244 |
| F5 | Open work orders | E8 for f. | `.open_tickets`, `.stalled_backlog` | value on the anchor day | tickets | stock | anchor day | 03e:868 |
| F6 | Mean time to close, trailing 30 days | Mean hours from creation to closure, over f's tickets closed in the 30 days before the anchor day's end. NULL when none closed. | `.mean_time_to_repair_hr` | the anchor day's value only. Not averaged over days, not over facilities. | hours | stock (trailing) | anchor day | 03e:845–847, 869 |
| F7 | Active CH₄ exceedances | f's CH₄ sensors whose last reading in [day end − 8 h, day end) carried `exceedance_flag`. | `.active_exceedances` | value on the anchor day | sensors | stock | anchor day | 03e:825–829, 872 |
| F8 | Sensors offline | f's CH₄ sensors installed ≥ 8 h before day end with no reading in the last 8 h. | `.sensors_offline` | value on the anchor day | sensors | stock | anchor day | 03e:830–831, 873 |
| F9 | Alarms raised | SCADA alarms raised on the day at f's assets. | `.alarms_raised` | sum over days | alarms | flow | `snapshot_date` | 03e:848, 873 |
| F10 | Alarms standing | Alarms raised before day end and not cleared by then. | `.alarms_active` | value on the anchor day | alarms | stock | anchor day | 03e:849–850, 874 |
| F11 | Overdue PMs, LDAR outstanding, open compliance cases | E9–E11 for f. | as E9–E11 | anchor-day value | as E9–E11 | stock | anchor day | as E9–E11 |
| F12 | Equipment condition | 1 − the mean `equipment_condition_index` of f's installed assets at day end. | `.equipment_health_score` | anchor-day value; trend by day | index in [0, 1] | stock | anchor day | 03e:857, 875; the index itself is 01_topology_config's `equipment_condition_index`, called at 03e:491–494 |
| F13 | Tag value, hourly | Mean of a tag's `Good` readings in one UTC hour. | `scada_telemetry_hourly.value_avg` (+ `value_min`, `value_max`) | none at tag-hour. To roll up, **re-weight**: `sum(value_sum_e6) / sum(good_count) / 1e6`, never `avg(value_avg)` | the row's `uom` | tag × hour | `bucket_ts`, H_ent | 02c:340–356, 02c:318–336; weighting rule 02c:74–82 |
| F14 | Tag data completeness | Share of expected readings that arrived, and the share of those that were `Good`. | `.sample_count`, `.expected_count`, `.good_count` | ratio of sums: `sum(sample_count)/sum(expected_count)`, `sum(good_count)/sum(sample_count)` | share (display %) | tag × hour | `bucket_ts` | 02c:344–345, 360, 330 |
| F15 | Running share | Share of a tag's readings taken while its asset was `Running`. | `.n_running`, `.sample_count` | `sum(n_running)/sum(sample_count)` | share | tag × hour | `bucket_ts` | 02c:349–350, 331 |

**Where the code does what the name does not suggest.**
- **`mean_time_to_repair_hr` measures time to close, not time to repair.** It is `closed_ts − created_ts` over
  tickets closed in the trailing 30 days (03e:845–847). It is NULL on 1,659 of 4,500 facility-days, where
  nothing closed. A portfolio MTTR cannot be built from this column, because the snapshot does not carry the
  number of closures behind each mean. It is a Facility Operations measure only.
- **`equipment_health_score` is an index, not a health percentage.** It is `1 − mean condition index`
  (03e:857). Observed range 0.763–1.0, p50 0.901. A value of 0.90 does not mean "90 % healthy".
- **`active_exceedances` and `sensors_offline` describe midnight, not now.** Both look at the 8 hours before
  the day's end (03e:821–831).
- **`pct_good` and `pct_time_running` are shares in [0, 1], despite the `pct_` prefix** (02c:330–331).
- **`confidence` (catalog) is a points score, not a probability.** It adds 0–2 for wind alignment,
  0–2 for uncertainty ratio and 1 for ≥ 5 pixels (04:1276–1307). Alignment is folded to [0, 90°] and graded
  `high` at ≤ 75° (04:901–912, 00_config:82), so it can only be `high` or `medium`. Observed: ~~44~~ **45**
  high, 5 medium, 0 low *(2026-10-15, 50 plumes)*. A known issue in CLAUDE.md.

### 1c. Real and synthetic

- **Real.** The satellite side of `gold_plume_catalog`: detection, IME, rate, band and wind. It has no
  `is_synthetic` column.
- **Synthetic.** Everything with facility identity:
  - `dim_facility`, from `topology_seed` 20260915. It has **no `is_synthetic` column**.
  - The attribution columns of the catalog: a real plume matched against invented facilities.
  - Every column of `fact_facility_daily_snapshot`. `is_synthetic` is true on all 4,500 rows
    (03e:876), including `total_emissions_kg`, which carries real IME through a synthetic attribution.
  - `scada_telemetry_hourly`. `is_synthetic` is true on all 3,265,025 rows (02c:336).

**Rule.** Any measure that names, ranks, maps or filters by a facility is synthetic, and must be visibly
marked as such where it is shown. How it is marked is set by R8. *(Revised 2026-10-15: the marker no longer
sits in each tile's title.)* Only E1, E4, E5 and a plume map at `source_lat/lon` are free of it. Do not derive
"synthetic" from the `is_synthetic` column alone: `dim_facility` and the catalog's attribution columns
lack it.

---

## Part 2. What V1 had that V2 cannot support

V1's tiles are T02–T26 of round-2 §B1. "Replaced" means a Part 1 row answers the same question under its own
definition. "Dropped" means no V2 source exists and none should be invented. "Decision" means a fifth table
or a policy call is needed. No near-match is substituted silently: where the V2 row answers a narrower
question, the table says how.

| V1 tile | V1 measure | status | V2 answer, or what is missing |
|---|---|---|---|
| T02 | Total CH₄ emissions (t), current 90 d vs previous 90 d | **Replaced, narrower** | E3 "Observed plume methane", which is a floor, not an emission total. The period comparison is **dropped**: the data spans 30 days (2026-08-16 → 09-14/15), so no previous period exists. |
| T03 | Methane intensity % | **Decision** | Needs `fact_production_daily` (a fifth table; 03e:44–46 says that is what it exists for). The numerator is the observed-mass floor, so the ratio is a floor too. The 19.26 divisor: see "Also settle". |
| T04 | Financial impact P10/P50/P90, lost gas, fines | **Replaced in part** | E6 is the point-estimate total. The **band** needs `fact_financial_impact` and must obey R4. It holds P5/P95, not P10/P90 (03e:85–89). Lost gas vs fines split: the snapshot cannot separate them on a day with both, so this needs `fact_financial_impact`. |
| T05 | Active plumes | **Replaced** | E1 / E2, renamed "Plumes detected". "Active" is dropped (see 1a). |
| T06, T17 | Open work orders (two cards, 290 and 29,023) | **Replaced** | E8 / F5, one definition. V1's "breached" ≈ `stalled_backlog` (open and past SLA, 03e:374). The `priority == "Critical"` count is **dropped**: no priority in the serving layer, and V1's filter never matched (values P1–P3). |
| T07 | Compliance violations, total / critical / major | **Replaced in part** | E7 (notices issued). Severity is **dropped**: not in the serving layer. Restoring it needs `fact_compliance_event` (a fifth table). |
| T08 | Detections table: rate, severity P1–P4, signature, confidence, distance, allocation, basin | **Replaced in part** | F3 / F4 rows from the catalog; basin = `dim_facility.sub_basin`. Severity P1–P4 (thresholds 100/250/500 kg/h) puts all 49 plumes in P1; **Decision A**. Signature needs `gold_multi_gas_signatures`, which is not served (**Decision**; that table is the frozen one in Operations_KQL). Allocation is always 1.0 (03e:284): **dropped**. Confidence: **Decision B**. |
| T09 | Facility map with avg risk and health | **Replaced** | E14. Health = F12, with no `coalesce(…, 0.7)`. Avg risk: **Decision C**. |
| T10, T23 | Daily emissions trend with target lines 12,000 / 50,000 / 4,500 | **Replaced** | E13. Every target line is **dropped**: none has a source. |
| T11 | Top 10 facilities by P50 | **Replaced** | Rank by E6 point estimate, labelled "point estimate". A sum of P50s is not a P50 (R4). |
| T12, T24 | Financial composition, lost gas vs fines, by day / "week" | **Decision** | Needs `fact_financial_impact`. The snapshot gives only the total. |
| T13 | Emission signatures breakdown | **Decision** | Needs `gold_multi_gas_signatures` (not served). |
| T15 | Active exceedances "now" | **Replaced** | F7, at the anchor day's end, not now. |
| T16 | Highest active emission rate | **Replaced** | E5 / F3 for f, renamed "Largest detected rate". V1's "no-data" 0.0 row is not reproduced: no plume means no row and a "no detection" message. |
| T18 | Plumes detected D-7, high-confidence, OLRE | **Replaced in part** | F1 over the last 7 days before H_det. "High confidence" = `confidence = 'high'` (see 1b for what that means). The OLRE flag (> 100 kg/h) is **dropped**: all 49 plumes exceed it, so it carries no information. |
| T19 | Sensors offline | **Replaced** | F8. |
| T20 | Overdue PM count, critical overdue, avg days overdue | **Replaced in part** | F11 (count). Critical and days-overdue are **dropped**; they need `dim_equipment` / `fact_pm_schedule`. V1's `9999`-day sentinel is not reproduced. |
| T21 | LDAR detected / repaired / cost / repair rate | **Replaced in part** | E11 / F11 (outstanding). Detected, repaired, cost and rate need `fact_ldar_survey`: **Decision**. |
| T22 | Compliance heatmap, monthly | **Dropped** | 3 notices in 30 days, in one month. A month × facility heatmap would show three cells. E7 covers it. |
| T25 | Work-order trail around a plume | **Decision** | Needs `fact_work_order` (a fifth table). |
| T26 | Top 20 high-risk equipment | **Decision** | Needs `dim_equipment` and `fact_maintenance`. The snapshot is facility-grain only. |

**Fifth tables named above, and why each.**
- `fact_financial_impact`: impact band and component split (T04, T12, T24).
- `fact_production_daily`: intensity (T03).
- `gold_multi_gas_signatures`: signature (T08, T13).
- `fact_compliance_event`: severity (T07).
- `fact_work_order`: trail (T25).
- `fact_ldar_survey`: repair performance (T21).
- `dim_equipment` / `fact_maintenance` / `fact_pm_schedule`: equipment grain (T20, T26).

None is assumed by Part 1. Each is a serving-layer decision. Each also brings the R6 acceleration question
with it.

---

## Part 3. Rules that apply to every measure

Each rule is written so that a reviewer can read a query and say pass or fail.

### R1. Time anchor

**The horizons, read 2026-10-14:**

| source | horizon column | first | last | horizon name |
|---|---|---|---|---|
| gold_plume_catalog | `detection_date` | 2026-08-16 19:45:22Z | **2026-09-15 20:17:52.5Z** | H_det |
| fact_facility_daily_snapshot | `snapshot_date` | 2026-08-16 | **2026-09-14** | H_ent |
| scada_telemetry_hourly | `bucket_ts` | 2026-08-16 00:00Z | **2026-09-14 23:00Z** | H_ent |
| dim_facility | `effective_from` | 2026-09-15 (all 150 rows, all `is_current`) | — | topology as-of |

`TOPOLOGY_AS_OF = date(2026, 9, 15)` (01_topology_config:343) is the **exclusive** end of the enterprise and
SCADA windows. 03e and 02c both treat every horizon as half-open (03e:964–970; 02c:199–200). The last
enterprise day is therefore 2026-09-14.

- **R1a. No wall clock.** A query must not contain `now()`, `ago()` or `datetime_diff(…, now(), …)`. Today is
  2026-10-14, neither pipeline is scheduled, and every such window selects nothing. V1 failed this on
  T02, T08, T16, T18, T19, T20 and T26. Every window is a literal or a parameter, stated relative to a named
  horizon.
- **R1b. One source, its own horizon. Two sources, the earlier horizon.** A measure that reads only the
  catalog may run to H_det. Any measure or page that combines detection with enterprise or SCADA data ends
  at H_ent: the common window is **[2026-08-16, 2026-09-15)**.
  - **Why not one `max()` across sources.** It returns 2026-09-15 20:17. On that day the snapshot and SCADA
    hold nothing. A "today" tile would then show zero emissions, zero tickets and zero alarms: values that
    were never computed, presented as measured.
  - **The concrete case.** The catalog has 42 attributed plumes and the snapshot has 41.
    PL-d6024ee3f97a (GS-0072, 2026-09-15 20:17, 37.3 t) is in one and not the other. A page that mixes the
    two horizons does not reconcile.
  - *(2026-10-15)* The catalog now has **43** attributed plumes (42 in the common window). The snapshot
    still has 41, under the pre-fix attribution.
    - **Horizon still applies.** The horizon argument above is unchanged.
    - **A second mismatch, independent of the horizon.** Snapshot and catalog disagreed on 66 of 74 plume
      facility-days (Q12).
    - **Closed by the release.** The snapshot now has 42 attributed plumes in the common window, matching
      the catalog.
- **R1c. Stocks are read on one day; flows sum over days.**
  - Flows: E1–E3, E6, E7, E13, F1, F2, F9.
  - Stocks: E8–E11, F5–F8, F10–F12, `risk_*`.
  - A query that sums a stock column without first restricting to a single `snapshot_date` fails.
- **R1d. The as-of date is printed once per page, on the freshness strip, and on any tile whose anchor differs
  from the page default.** *(Amended 2026-10-15. It previously required every tile to print its own as-of,
  which forced KPI figures into tables with an `as_of` column.)*
  - **The page default** is the window W, capped at H_ent. Flows are summed over W; stocks are read at the
    end of W's last day. The freshness strip (T01's board row) prints that date.
  - **Tiles with a different anchor print their own:** a literal window (the consistency gate), the topology
    date (facility identity), or a source's own maximum (each freshness row).
  - **A tile on the page default** need not repeat the date, and charts need not put it in the legend.
- **R1e. Filter on `snapshot_date` / `bucket_ts`, not `date_sk`.** `date_sk` is `long` in Delta (03e:1261;
  02c:335) but reads as **`string`** through these external tables (`gettype(date_sk)` = `string`). A
  numeric comparison on it is wrong.

### R2. Zero rows vs missing

**The snapshot writes a row for every facility-day, including days when nothing happened** (03e:1349–1354
asserts it). By 03e's own definition of zero activity (03e:1550–1552: no plume, no cost, no open ticket, no
stalled ticket, no notice, no alarm raised or standing, no exceedance), **1,723 of 4,500 facility-days
are zero rows: 38.29 %.** Verified on the serving table. For plumes alone, 4,459 of 4,500 facility-days
(99.1 %) have none.

**What that does to an average.**

| statistic | value |
|---|---|
| `avg(total_emissions_kg)` over all facility-days | 322.7 kg |
| the same over facility-days with a plume | 35,415 kg |
| `avg(daily_financial_impact)` over all facility-days | $63.44 |
| the same over facility-days with a cost | $6,487.94 |

Each figure is correct for a different question. A tile that says "average" without saying over what is wrong
by up to two orders of magnitude.

The SCADA table uses **the opposite convention: an hour with no readings has no row** (02c:68–72). Of 4,955
tags, 2,739 have fewer than the 720 hourly rows that 30 days would give. Three active facilities (GS-0077,
GS-0115, GS-0119) have no SCADA rows at all, and GS-0119 has an attributed plume (PL-771b55350b6c).
*(2026-10-15: no longer. After the wind fix, PL-771b55350b6c goes to GS-0035; none of the three now has an
attributed plume.)*

Rules:
- **R2a. Every average names its denominator** in the tile label: "per facility-day (zeros included)" or
  "per detection". A per-detection average is computed from the catalog (one row per plume), never from
  snapshot rows filtered to `> 0`.
- **R2b. Sums are unaffected** (E3, E6, E7, F2, F9). Counts of "facilities with X" state `X > 0` explicitly.
- **R2c. NULL is not zero.** Exclude NULLs; never `coalesce` them to 0 or to any constant (V1's T09 used 0.7).
  - `mean_time_to_repair_hr`: NULL on 1,659 rows.
  - `value_avg` / `value_min` / `value_max`: NULL when `good_count = 0`.
  - `value_stddev`: NULL when `good_count < 2` (02c:62–66).
- **R2d. SCADA coverage is a ratio of sums.** Completeness is `sum(sample_count)/sum(expected_count)` (F14).
  Coverage over the window is hours present ÷ (24 × days). A mean over hourly rows silently skips missing
  hours, so it must be shown beside its coverage.
- **R2e. No rows is a state, not a zero.** A facility with no SCADA rows shows "no SCADA data", never 0.
  A facility with no plume shows "no detection", never a 0.0 kg/h rate (V1's T16).
- **R2f. Which facilities count.** The snapshot includes the 7 facilities with `active_flag = false` (210
  facility-days, 0 plumes). 03e reads `dim_facility` by `is_current`, not `active_flag` (03e:1038). Whether
  portfolio "per facility" denominators use 150 or 143 is Part 5, Q4. Until it is settled, the tile states
  which.

### R3. Unattributed plumes

7 of ~~49~~ **50** plumes have `attribution_method = 'no_match'` *(2026-10-15; the same 7 plumes)*. For each:
- `facilities_in_range = 0`, and every attribution attribute is NULL (05:246–266, asserted at 05:462–471).
- Together, the 7 carry **275.2 t** of observed methane. That is ~~15.6 % of the catalog's 1,764.6 t~~
  **15.7 % of the catalog's 1,758.4 t** *(2026-10-15)*.
- The plume is absent from the snapshot and from `fact_financial_impact`. 03e skips it "no facility to charge"
  (03e:654–655, 1155–1159).

Their detection dates (08-20 ×2, 08-21 ×3, 08-25, 08-26) all lie inside the common window.

- **R3a. Portfolio totals of plume count or observed methane are computed with an explicit "Unattributed"
  bucket.** Either compute them from the catalog, or as snapshot totals plus catalog `no_match` rows, in the
  same window.
- **R3b. Reconciliation is a test.** Σ over facilities + unattributed = catalog total, for the same window.
  - Counts *(2026-10-15)*: **43 + 7 = 50** to H_det; **42 + 7 = 49** in the common window.
    ~~42 + 7 = 49; 41 + 7 = 48.~~
  - Tonnes *(2026-10-15, catalog only)*: **1,483.1 + 275.2 = 1,758.4 t** to H_det; **1,445.8 + 275.2 =
    1,721.1 t** in the common window. ~~1,489.3 + 275.2 = 1,764.6 t; 1,452.0 + 275.2 = 1,727.3 t.~~
  - ~~Until Q12 is closed, facility totals must come from the catalog.~~ *(Released)* The snapshot's
    `total_emissions_kg` now passes this test. 1,445.822 t + 275.2 t unattributed = 1,721.1 t, the
    catalog's common-window total.
  - A tile whose facility total does not reconcile fails.
- **R3c. Financial totals cover attributed plumes only, and say so.** Unattributed methane is not priced.
- **R3d. Mapping.** Unattributed plumes are drawn at `source_lat/lon` with their own marker. They never sit
  under a facility symbol.

### R4. Percentiles do not add

**What exists.**
- The catalog carries `emission_rate_p5/p50/p95` per plume: the 5th, 50th and 95th percentile of 500 Monte
  Carlo draws over retrieval noise and wind (04:1193–1245).
- `fact_financial_impact` carries `total_impact_usd_p5/p50/p95` per row (03e:677–679). It is not served.
- No Monte Carlo samples are stored anywhere.

**On this catalog:**

| sum over plumes | kg/h, 49 plumes (2026-10-14) | kg/h, 50 plumes (2026-10-15) |
|---|---|---|
| Σ point | 1,204,076 | 1,210,962 |
| Σ p50 | 1,174,983 | 1,181,694 |
| Σ p5 | 543,788 | 546,278 |
| Σ p95 | 1,946,348 | 1,958,855 |

**Legitimate:**
- **R4a.** A per-plume band, shown on that plume's row (F3, E5).
- **R4b.** A total as the **sum of point estimates**, labelled "point estimate". Expectations add; point
  estimates are what 03e sums for E3 and E6.
- **R4c.** Σp5 … Σp95, labelled **"range if every plume's error moved together"**, never "P5–P95". The sum
  of quantiles equals the quantile of the sum only when the errors are perfectly correlated
  (comonotonic). Under independence the true band is narrower. Plumes in one scene share a wind field, so
  neither extreme is known to hold (Part 5, Q5).
- **R4d.** The median of point rates **across** plumes (E4). It describes the catalog's spread, not any
  plume's uncertainty, and is labelled "median across plumes".

**Not legitimate:**
- Any `sum(…pNN…)` labelled pNN (V1's T04, T11).
- An average of p50s labelled "median".
- A ratio of percentile sums.
- P10/P90 labels on P5/P95 columns (03e:85–89).
- Mixing the Monte Carlo p50 with point estimates in one total. `total_impact_usd_p50` is not the sum of its
  point components (03e:90–93).

### R5. Unit discipline

- **R5a.** Every displayed number names its unit in the tile: t, t/h, kg/h, USD, $ k, $ M, plumes, tickets,
  sensors, share or %.
- **R5b. Every divisor or multiplier in a query is one of the constants below**, and its result carries the
  matching label. A constant not on this list fails review. A comment like V1's
  "adjust until numbers look realistic" fails review.

| conversion | constant | source |
|---|---|---|
| kg → t | ÷ 1,000 | SI |
| kg/s → kg/h | × 3,600 | 04:1155 |
| kg/h → t/h | ÷ 1,000 | SI; the catalog's `emission_rate_t_h` = kg/s × 3.6 (04:1156) |
| USD → $ k / $ M | ÷ 1e3 labelled "k", ÷ 1e6 labelled "M". One divisor per label, never chained | — |
| kg CH₄ → mcf | ÷ `KG_CH4_PER_MCF` = 19.26 | 03e:279, basis 03e:293–297 (see "Also settle") |
| mcf → USD (lost gas) | × `GAS_PRICE_USD_PER_MCF` = 2.75 | 03e:278, basis 03e:286–292 (V1's value, ~10 % low for the window by 03e's own check) |
| t CH₄ → t CO₂e | × `GWP_CH4_100` = 28 | 03e:280, basis 03e:298–302 |
| share → % | × 100 | applies to `pct_good`, `pct_time_running` (02c:330–331) |

- **R5c. No rate becomes a mass by assumption.** Never multiply an hourly rate by 24, or by any duration other
  than the stored `t_mix_s`, to make "daily emissions". TROPOMI sees one instant a day (03e:59–65).
- **R5d. SCADA values aggregate only within one measurand.** Never across tags with different `uom`, and
  not by `uom` alone either: `percent` is used by `LT` level tags (254) and `ZT` position tags (128), and
  `mscfd` / `bpd` both come from `FT` flow tags. The measurand is the tag prefix plus `uom`. A proper tag
  type needs `dim_scada_tag` (Part 5, Q8).
- **R5e.** `equipment_health_score` and `risk_*` are unitless indices. Label them as such, never as %.
- **R5f. No displayed number is abbreviated, and no label is cut below its unit.** *(Added 2026-10-15.)*
  - **What failed in V1.** It showed "5.1M" for $5.13 billion: a card's automatic compaction, with the
    magnitude suffix and the unit both lost.
  - **Cards.** A card shows the exact value with thousands separators, and the unit in its label. For
    example, "Financial impact (point estimate), USD — 268,504.28" or "Observed plume methane at
    facilities, t CH₄ — 1,445.8".
  - **How the board does it.** The query formats the value as text, so the visual has nothing to compact.
  - **Charts** carry the unit on the axis title.

### R6. Count with `summarize count()`

The frozen-catalog failure (`| count` = 109, `| summarize count()` = 49 on Operations_KQL) is a property of a
stale query-acceleration catalog. Acceleration is ON for `scada_telemetry_hourly` (section 0), and any table
can be switched on later.

- **R6a.** Every count in a tile or a check uses `summarize count()`, never bare `| count`.
- **R6b.** A health check compares the two daily and alerts on any disagreement.
- **R6c. A schema change to a served table requires recreating its shortcut by hand in the portal.**
  - **AutoUpdateSchema does not update a shortcut's external-table schema, even with query acceleration
    OFF.**
  - **What happened** (2026-10-14 → 15):
    - Delta `gold_plume_catalog` gained `era5_u10` and `era5_v10` at v27 (41 columns).
    - Acceleration was OFF throughout (`IsEnabled: false`, re-read 2026-10-15).
    - The external table still reported 39 columns the next morning. Any query naming the new columns
      returned 400.
    - Only recreating the shortcut in the portal brought the 41 columns.
  - **Do not recreate it through the REST API.** On 2026-10-14, API-created shortcuts on this KQL
    database returned 201 but never surfaced as external tables (`rti_serving_namespace_2026-10-14.md`).
  - **Rules:**
    - Any notebook change that adds, drops, renames or retypes a column of the four served tables
      carries a portal step: recreate that shortcut.
    - After the portal step, run a check: `external_table('<t>') | getschema | summarize count()` must
      equal the column count of the Delta log's latest `metaData`. Compare names too, not only the count.
    - `date_sk` is the partition column and reads as `string` (R1e). The check expects it.
  - **This cost a day and is not guessable from the documentation.**

### R7. Exact distinct counts

`dcount()` is an estimate:

| query | result |
|---|---|
| `dcount(tag_sk)` | 4,962 |
| `summarize by tag_sk \| count` | 4,955 (exact) |

**R7.** A displayed distinct count uses `count_distinct()` or `summarize by … | count`, never `dcount()`.

### R8. The real/synthetic marker

A measure listed as synthetic in 1c is marked as synthetic where it is shown (see 1c for which measures). The
marker is carried in three places. **Tile titles do not carry it.** *(Revised 2026-10-15. The earlier wording
put "SYNTHETIC ESTATE" in every facility-keyed title. On the board that repeated one phrase across 19 titles
and crowded out the words that tell the tiles apart.)*

- **R8a. Page banner.** Every page that shows any synthetic measure opens with a banner stating the
  real/synthetic boundary for that page:
  - which parts are real: plume detection, rate, band, observed methane;
  - which parts are synthetic: facilities, attribution, tickets, sensors, SCADA.
- **R8b. In-tile marking where real and synthetic content mix.** A table whose rows mix real and synthetic
  content carries a per-row `estate` (or `marker`) column. A chart whose series mix them names the
  synthetic part in the series label, for example "attributed (synthetic attribution)" or "condition
  (synthetic, max 30)".
  - A tile that is wholly synthetic, on a bannered page, needs no in-tile marker.
- **R8c. README.** The README states the boundary in full. It also says that titles do not repeat it.

A reviewer checks R8 per tile in two steps. First, does the tile sit on a bannered page (R8a)? Second, if
the tile mixes real and synthetic content, does it mark them row by row or series by series (R8b)? The
board's rule table (`rti_board_design.md` §3) records how each tile meets R8.

---

## Part 4. Three decisions (made 2026-10-15)

The options and evidence below are kept as written on 2026-10-14. The decision is stated under each
heading. Figures re-read on 2026-10-15 are marked.

### Decision A. Severity banding — DECIDED 2026-10-15: no banding

**Decision.** Option 3, no banding.
- **What shows:** the rate in t/h with its P5–P95, sorted by rate.
- **What never shows:** no P1–P4, no SUPER_EMITTER tier, no colour by magnitude.
- **The one exception:** one reference line at 25 t/h labelled "Lauvaux et al. 2022 ultra-emitter
  threshold". It is drawn on the distribution tile only, and never as a per-plume badge.
- **Why:** the median (~19.7 t/h) sits near that threshold, and detection density runs ~16× CAMS with
  striping unresolved. Banding would present a probable artefact as a world-class event.
- **This supersedes** the recommendation's P5-based flag below.

*(2026-10-15, n = 50, point estimates.)*
- **Rate distribution, kg/h:**

  | statistic | min | p10 | p25 | median | mean | p75 | p90 | max |
  |---|---|---|---|---|---|---|---|---|
  | kg/h | 418 | 11,345 | 14,669 | 19,655 | 24,219 | 35,522 | 43,713 | 71,751 |

- **Plumes at or above each threshold (point estimate):**

  | threshold | 1,000 kg/h | 10,000 kg/h | 25,000 kg/h | 50,000 kg/h |
  |---|---|---|---|---|
  | plumes | 49 | 46 | 17 | 3 |

- **At 25 t/h by band edge:** 2 at P5, 36 at P95.
- **`uncertainty_ratio`:** median 1.67, max 5.61.

**The distribution as read 2026-10-14** (`gold_plume_catalog.emission_rate_kg_h`, n = 49, point estimates):

| statistic | value |
|---|---|
| min | 418 (the 48 h-fallback artefact, see 1a) |
| min of the 48 wind-dependent plumes | 5,439 |
| p10 | 11,674 |
| p25 | 14,751 |
| median | 19,716 |
| mean | 24,573 |
| p75 | 35,522 |
| p90 | 43,891 |
| max | 71,751 |

`uncertainty_ratio` (p95/p50) has median 1.66 and max 5.61 (the fallback plume). Among wind-dependent plumes
the max is 2.12.

| threshold | plumes at or above (point) | at P5 | at P95 |
|---|---|---|---|
| 10 kg/h | 49 | — | — |
| 100 kg/h | 49 | — | — |
| 1,000 kg/h | 48 | — | — |
| 10,000 kg/h | 46 | — | — |
| 25,000 kg/h | 17 | **2** | **36** |
| 50,000 kg/h | 3 | — | — |

The catalog's floor is TROPOMI's detection floor. A 7 km pixel sees only large releases, so the estate's
small leaks are absent rather than low.

**Options.**

1. **Absolute thresholds from the literature.**
   - **Thresholds.** I cite these from memory; they were **not re-verified in this session**. Check each
     against the paper before adopting it.
     - EPA's Super Emitter Program (NSPS OOOOb, 2024): 100 kg/h.
     - Duren et al. 2019, *Nature*, California super-emitters: ≥ 10 kg/h point sources.
     - Lauvaux et al. 2022, *Science*, "ultra-emitters" from TROPOMI: > 25 t/h.
   - **For.** Externally defensible and comparable across operators.
   - **Against.** The first two put all 49 plumes in the top tier. That is the band reproducing the
     instrument's floor, the failure the brief warns about. The 25 t/h tier does discriminate, but it is
     fragile: 17 plumes at the point estimate, 2 at P5, 36 at P95. One threshold can show 2 or 36
     "ultra-emitters" depending on which edge of a 90 % interval is read.
2. **Distribution-relative bands** (terciles or quartiles of this catalog).
   - **For.** Always discriminates.
   - **Against.**
     - "High" would mean "high among what TROPOMI saw here", which is easily read as an absolute severity.
     - The edges move whenever 04 reruns, and 04 overwrites the whole catalog (CLAUDE.md known issues).
     - n = 49 makes the edges noisy.
     - It is a ranking dressed as a severity.
3. **No banding.**
   - **What it shows.** The rate in t/h with its P5–P95 bar, sorted by rate.
   - **For.** It claims nothing the data does not hold. With 49 plumes, sorting is triage enough.
   - **Against.** No at-a-glance colour, and stakeholders used to V1's P1–P4 will ask for one.

**Recommendation (not a decision): option 3.** If a tier is required, add one flag only: "> 25 t/h (Lauvaux
et al. 2022)", raised when **P5** exceeds 25 t/h (2 plumes today), with the citation in the tooltip. The flag
must not be raised on the point estimate. V1's P1–P4 (100/250/500 kg/h) must not return in any form.

### Decision B. Attribution confidence — DECIDED 2026-10-15: option 1 with the hard rule

**Decision.** Show `attribution_probability`, always beside `facilities_in_range` and
`attribution_distance_km`.
- **Single candidate.** When `facilities_in_range = 1`, display "only candidate within 50 km", never a
  number.
- **Facility identity.** Show `facility_id` beside `facility_name`, including in the facility selector's
  label.
- **Method.** Surface `attribution_method`, which is now accurate on every row.
- **Display rule.** The board also shows a multi-candidate share of ≥ 0.995 as "> 0.99" so that 1.00 never
  appears.

**Post-fix figures** (2026-10-15, 43 attributed plumes):
- **Probability:** median 0.69, min 0.22, and 18 of 43 below 0.5.
- **Distance:** median 23.0 km, max 48.3 km.
- **Candidates in range:** median 13. Median angular offset 21.4°.
- **Single candidates:** 7 plumes have `facilities_in_range = 1`, at 30.3–47.6 km:
  - PL-d6024ee3f97a and PL-d7f86d52a89e → GS-0072
  - PL-62f209476b54 → GS-0133
  - PL-4aa750c5e496 → GS-0051
  - PL-ee07d05aa443 and PL-fc6e6c832a11 → GS-0038
  - PL-140fdf382e6b → GS-0125
- **Methods:** `wind_aware` 42, `distance_only` 1, `no_match` 7.
- **Worked example: PL-771b55350b6c, GS-0119 → GS-0035.**
  - Wind toward 350.9° at 4.29 m/s.
  - GS-0035 sits 15.8 km upwind, 15.0° off the wind axis.
  - GS-0119 is 9.5 km away but lies downwind of the plume, 153° off.
  - The full evidence is in `rti_board_design.md` §10.

**How 05 computes `attribution_probability`, confirmed from code:**
- Candidates are facilities within `attribution_search_radius_km` = 50 (05:186–190; 00_config:93).
- Each candidate scores `w_dist × w_wind`.
  - `w_dist = exp(−d² / (2 · 15²))`. The σ = 15 km is a literal at 05:193, not in CONFIG.
  - `w_wind = exp(−θ² / (2 · 30²))` (05:196–201, 00_config:94).
- **Probability = score ÷ Σ scores over the candidates** (05:222–225). It is a share among the candidates
  in range, not a likelihood that the facility is the source. With one candidate it is 1.0 whatever the score.

**What the data shows.**
- **7 plumes, not 5,** have `facilities_in_range = 1` and probability 1.0. Their distances are
  30.3–47.6 km. Five go to `outside`-band facilities (round-2 C4). The other two are PL-62f209476b54 → GS-0133
  (33.2 km) and PL-140fdf382e6b → GS-0125 (45.8 km).
- On those 7, the raw score says "poor match": `w_dist` is 0.0065–0.13 and `w_wind` is 7.5e-8 to 1.2e-4.
  The probability says "certain".
- Over all 42 attributed plumes:
  - probability: median 0.65, min 0.10; 16 of 42 are below 0.5.
  - distance: median 19.2 km, max 49.1 km.
  - candidates in range: median 13.

**New finding (2026-10-14): the "wind_aware" term does not use the wind.**
*Resolved 2026-10-14 (commit c93dd7a). The catalog now carries `era5_u10/v10`. 05 compares the
facility→plume bearing with the direction the wind blows **toward**. When the wind is ≤ 0.5 m/s it records
`distance_only`. The analysis below describes the pre-fix state.*
- 05 reads the wind from `plume["era5_u10"]` and `plume["era5_v10"]`, falling back to 0 when they are absent
  (05:176–179).
- The catalog has no such columns. 04's schema (04:1331–1360) writes `U_eff_ms` and `wind_alignment_deg`,
  not the u/v components.
- So `atan2(0, 0) = 0` and `upwind_dir = 180°` for every plume. The guard at 05:172 passes, because
  `U_eff_ms > 0` on all 42.
- **Verified.** For **42 of 42** attributed plumes, the stored `attribution_angular_offset_deg` equals the
  angle between the facility→plume bearing and 180°, to 1.5 × 10⁻¹² degrees
  (scratchpad `wind_check.py`, recomputed with 05's own bearing and angle functions).

The angular weight therefore favours facilities due north of each plume, regardless of the actual wind. This
shapes every `attribution_probability`, every `second_facility_*`, and possibly **which** facility wins on
multi-candidate plumes. The label `attribution_method = 'wind_aware'` is wrong on every row.

Also: facility names are decorative. The town is drawn independently of the coordinates (01a:125), and
14 of 20 place names span three or more sub-basins (round-2 C6).

**Options.**
1. Show `attribution_probability` as stored, beside `facilities_in_range` and `attribution_distance_km`.
2. Show a derived confidence: probability, but capped (for example at 0.5) and labelled "single candidate"
   when `facilities_in_range = 1`.
3. Show no probability. Show the evidence: distance, number of candidates within 50 km, and the runner-up
   with its share. Label the assignment "nearest-candidate assignment (provisional)".

**Recommendation (not a decision): option 3 until 05's wind input is fixed** (Part 5, Q1). Today's number
measures distance plus a fixed north preference, so any display of it as confidence misleads.

After a fix, option 1 with one hard rule: when `facilities_in_range = 1`, display "only candidate within
50 km", never a number. Option 2's cap would be a new invented constant.

In every option:
- Show `facility_id` beside `facility_name`.
- Never present the name as a location.

### Decision C. Risk score presentation — DECIDED 2026-10-15: option 2

**Decision.** Never show a bare `risk_score`.
- **Ranking.** Rank facilities on observed methane: E3 in the window first, then plume count.
- **Context.** Show the `risk_pts_*` components beside the rank, each labelled, with the synthetic ones
  marked.
- **Secondary sort.** Where one is needed, use a single named component, not the sum.
- **All five components are now on the board,** released 2026-10-15. They were withheld while Q12 was
  open.

**The definition.**
- Weights at 03e:347–348: condition 30, emissions 25, compliance 20, maintenance 15, open work 10.
- Saturations at 03e:351–356. `RISK_PLUMES_AT_MAX = 2.0` means one attributed plume in 30 days scores 12.5 and
  two score 25.
- Computed in `risk_points()`, 03e:764–773.

**Distribution over all 4,500 facility-days.**

| score | value |
|---|---|
| mean | 27.6 |
| p5 | 13.0 |
| p25 | 18.6 |
| p50 | 25.9 |
| p75 | 34.5 |
| p95 | 49.2 |
| p99 | 60.1 |
| max | 69.3 |
| facility-days ≥ 50 | 201 |
| facility-days ≥ 70 | 0 |

03e's markdown quotes p50 25 and max 73 from the offline estate (03e:246). Fabric's max is 69.3, and
compliance is non-zero on 2.8 % of facility-days against the 0.4 % quoted offline (03e:253–254).

**Released distribution (2026-10-15, after the enterprise backfill).** The table above is pre-fix.

| score | mean | p5 | p25 | p50 | p75 | p95 | p99 | max | facility-days ≥ 50 | ≥ 70 |
|---|---|---|---|---|---|---|---|---|---|---|
| released | 27.7 | 13.0 | 18.2 | 25.1 | 34.7 | 50.1 | 68.9 | **80.0** | 225 | **34** |

| component (weight) | mean | p5 | p50 | p95 | max | non-zero facility-days | at weight | sd |
|---|---|---|---|---|---|---|---|---|
| condition (30) | 17.64 | 9.63 | 15.65 | 29.47 | 30 | 4,456 (99.0 %) | 178 | 6.74 |
| emissions (25) | 2.12 | 0 | 0 | 12.5 | 25 | 630 (14.0 %) | 132 | 5.58 |
| compliance (20) | 0.44 | 0 | 0 | 0 | 20 | 155 (3.4 %) | 43 | 2.47 |
| maintenance (15) | 4.70 | 0 | 4.24 | 10.57 | 15 | 3,980 (88.4 %) | 4 | 3.09 |
| open work (10) | 2.78 | 0 | 0 | 10 | 10 | 1,748 (38.8 %) | 757 | 3.82 |

Total score sd 12.58. Notes on the released tables:
- **The percentiles** come from KQL `percentile()`, which is an estimate. Condition's p50 reads 15.65 here
  and 15.56 above, but condition is not attribution-dependent (02a). Its mean, non-zero count, at-weight
  count and sd are identical, and so are its values wherever compared (`rti_board_design.md` §14).
- **The emissions column** equals `round(25 × min(trailing-30-day attributed plumes ÷ 2, 1), 2)` on 4,500
  of 4,500 facility-days.
- **The ceiling moved.** The max rose from 69.3 to 80.0, and 34 facility-days now score ≥ 70, where there
  were none.
  - All 34 are at facilities with two or more attributed plumes, so emissions is at 25: GS-0051 (18 days),
    GS-0043 (9), GS-0091 (4) and GS-0064 (3).
  - Most also carry 10 compliance points.

| component (weight) | mean | p5 | p50 | p95 | max | non-zero facility-days | at weight |
|---|---|---|---|---|---|---|---|
| condition (30) | **17.64** | 9.63 | 15.56 | 29.47 | 30 | 4,456 (99.0 %) | 178 |
| emissions (25) | 2.14 | 0 | 0 | 12.5 | 25 | 657 (14.6 %) | 112 |
| compliance (20) | 0.35 | 0 | 0 | 0 | 20 | 125 (2.8 %) | 34 |
| maintenance (15) | 4.70 | 0 | 4.24 | 10.58 | 15 | 3,980 (88.4 %) | 4 |
| open work (10) | 2.79 | 0 | 0 | 10 | 10 | 1,761 (39.1 %) | 749 |

Standard deviations: condition 6.74, emissions 5.47, open work 3.81, maintenance 3.09, compliance 2.22.
Total score 11.52.

**What that means.**
- **Condition supplies 64 % of the mean score.** Emissions are zero on 3,843 of 4,500 facility-days (85.4 %).
  *(Released: still 64 %; emissions zero on 3,870 of 4,500, 86.0 %.)*
- **92 % of the average score (25.5 of 27.6 points) comes from the four synthetic components.** *(Released:
  92 %, 25.6 of 27.7.)* Only the
  emissions component is driven by real satellite data, and it arrives through the attribution in
  Decision B.
- **The facility with the highest 30-day mean risk has never had a detection.** GS-0014 (63.0) scores on
  compliance 20 and condition. Of the top 25 facilities by 30-day mean, 13 have no detection.
  - *(2026-10-15)* **GS-0014 is still first and still has no detection.** It stays first with the
    emissions component recomputed under the current attribution (63.03). That recompute uses 03e's own
    formula: 25 points × min(trailing-30-day attributed plumes ÷ 2, 1).
  - **Top 25 by 30-day mean, facilities with no detection:**
    - With the stored (pre-fix) scores, 14 have no detection *under the current attribution*.
    - With the emissions component recomputed, 10 do.
  - ~~**Caveat on these figures.** The compliance component is still pre-fix, so all of them are
    provisional until 03c/03e re-run (Q12).~~
  - *(Released)* **GS-0014 is now second, not first.** GS-0051 leads at 63.32: two plumes, emissions
    19.17, compliance 6.67, condition 24.62. GS-0014 follows at 63.03, still with no detection, scoring on
    compliance 18.0 and condition 26.71.
    - **The finding stands in weaker form.** A facility with no methane sits 0.29 points from the top, on
      synthetic components alone.
    - **Top 25 by 30-day mean:** 10 facilities have no detection.
- **GS-0001 vs GS-0002** *(written 2026-10-14 under the pre-fix attribution; see the update after this list)*.
  - **Over 30 days.**
    - GS-0001 (one plume, 72.4 t) averages 33.07.
    - GS-0002 (no plume) averages 27.58.
    - GS-0002's condition points average 21.3 against GS-0001's 10.1.
  - **Day by day.**
    - The gap runs from −7.97 to +13.23.
    - The two are within 1.1 points on 3 days (08-17, 09-07, 09-11).
    - **GS-0002 outranks GS-0001 on 7 of 30 days.** Four of those (09-07 → 09-10) fall while GS-0001 was
      carrying 12.5 emission points.
  - **Correction to the brief.** "Within 1.1 points" holds on some days, not as a summary. The stronger
    finding is that the facility with no methane outranks the one with methane on 7 days.
- **GS-0001 vs GS-0002, after the wind fix (2026-10-15).** The example no longer holds.
  - **GS-0001 no longer has a plume.**
    - Its only plume was PL-f8ec764f9cdf, on the re-assigned scene SCN-20260819T202934.
    - Pre-fix, GS-0001 had won it among 35 candidates with a share of only 0.137.
    - After the fix it goes to **GS-0102**, at 8.4 km with a 0.29 share; the runner-up is GS-0144 at 0.26.
    - Its IME is now 60.5 t, against the 72.4 t the snapshot still carries for GS-0001 on 08-19.
    - So neither facility has methane in the window.
  - **Stored (pre-fix) snapshot:**
    - GS-0001 averages 33.07, with 11.25 of it emissions points from that plume.
    - GS-0002 averages 27.58.
  - **Emissions component recomputed under the current attribution** (projection, 2026-10-15 morning):
    - GS-0001 falls to **21.82**: condition 10.09, maintenance 4.06, open work 7.67, compliance 0.
    - GS-0002 stays at 27.58: condition 21.33, maintenance 3.75, open work 2.50.
    - GS-0002 then outranks GS-0001 on 28 of 30 days, by up to 20.5 points.
  - *(Released)* **The projection held for GS-0001: 21.82, exactly.**
    - **GS-0002 moved to 26.75.** Its open work fell from 2.50 to 1.67 when the tickets were regenerated.
    - **The ordering is unchanged.** GS-0002 still outranks GS-0001 on 28 of 30 days, by up to 18.37 points.
  - **What it shows now.** With methane removed from both, the ordering is set entirely by synthetic
    condition, maintenance and open-work points.
  - **The point stands, more sharply.** A risk sum can rank a facility above another for reasons unrelated
    to methane, and one attribution change moved GS-0001's score by 11 points.
  - ~~**Where to look instead.** Choosing a replacement pair is left until 03e is re-run.~~ Chosen below.
- **Replacement pair (released 2026-10-15): GS-0102 vs GS-0003.** It replaces GS-0001 vs GS-0002.
  - **Why this pair.**
    - Both are Tank Batteries on the Central Basin Platform, both active, so they are comparable estates.
    - GS-0102 now holds PL-f8ec764f9cdf, the 60.5 t plume that left GS-0001. It is the direct successor of
      the original example.
    - GS-0003 has no detection.
  - **Over 30 days:**

    | facility | 30-day mean | emissions | compliance | condition | maintenance | open work | methane in W | notices |
    |---|---|---|---|---|---|---|---|---|
    | GS-0102 | **23.45** | 11.25 | 0 | 11.89 | 0.31 | 0 | 60.5 t, 1 plume (08-19, share 0.29 of 35) | 0 |
    | GS-0003 | **25.50** | 0 | 0 | 17.64 | 7.03 | 0.83 | none | 0 |

  - **Day by day.**
    - GS-0003, with no methane, outranks GS-0102 on **25 of 30 days**. Those include 22 of the 27 days on
      which GS-0102 carries 12.5 emissions points (08-19 → 09-14).
    - GS-0102 is ahead only on 08-19 → 08-24, the first days after its plume.
    - The gap runs from −11.74 to +5.44, and the two are within 1.1 points on 6 days.
  - **What it shows.** One detected plume's 12.5 points do not lift GS-0102 above a methane-free neighbour.
    The neighbour carries 5.75 more condition points and 6.72 more maintenance points, all synthetic.
  - **A second, starker example: GS-0127 vs GS-0061.** Both are Compression Stations in the Delaware Basin.
    - GS-0127 has one plume: PL-3df7b6aaabdf, 13.6 t, 08-20, "> 0.99 share of 2 candidates".
    - GS-0061 has none, yet outranks GS-0127 on **all 30 days**: 44.97 against 33.15, gap −32.83 to −0.66.
    - GS-0061 scores through compliance (9.67 mean, one notice on 09-13, $31,645.49) and condition (22.06
      against 16.81).
  - **Attribution caveat (Decision B).** GS-0102's plume is a 0.29 share among 35 candidates. "The facility
    with methane" is itself a weak attribution. That is one more reason the board ranks on observed methane
    and shows the share, rather than folding either into a score.
- **The compliance component is not E9** *(2026-10-15)*.
  - **What it counts.** `risk_pts_compliance` counts Violations in the trailing 30 days plus **open state
    cases** (Venting / Flaring) at 0.5 each, capped at 20 points (03e:346, 769, 801, 860). E9
    (`open_compliance_cases`) counts open cases of **all** types (03e:800).
  - **Example.** GS-0043 has 2 open cases on 09-06 to 09-08 and 0 compliance points.
  - **Labels on the board.**
    - The component is labelled "compliance points (violations in 30 days + open state cases)".
    - E9 is labelled "open compliance cases (all types)".
  - **Do not read one as the input to the other.** See also the note under Part 1a.
- **The 80-point ceiling (2026-10-15).** The released maximum is 80.01 (GS-0091, 09-14), against 69.3 pre-fix.
  34 facility-days now score ≥ 70, against none pre-fix.
  - **Which components drive the 34 rows.** Four facilities account for them: GS-0051 (18 days), GS-0043
    (9), GS-0091 (4) and GS-0064 (3).

    | component (max) | mean over the 34 rows | how many of the 34 |
    |---|---|---|
    | emissions (25) | 25.00 | all 34 at the cap (2+ attributed plumes in 30 days) |
    | condition (30) | 26.21 | 26 at ≥ 24; GS-0043 at 30 on all 9 |
    | maintenance (15) | 8.48 | none at the cap |
    | compliance (20) | 7.35 | 25 at 10 (one open state case); none at 20 |
    | open work (10) | 6.76 | 0, 5 or 10 |

    - **70 is unreachable without emissions at 25.** The other four sum to at most 75, and these rows sit
      well below that.
  - **No component changed behaviour, and the saturation counts are unchanged** (facility-days at the cap,
    pre-fix → released):
    - condition 178 → 178;
    - maintenance 4 → 4;
    - open work 749 → 757;
    - emissions 112 → 132, the same rule (25 points at 2 plumes), held by 7 facilities pre-fix and 8 now;
    - compliance 34 → 43, though none of the 34 high rows is at 20.
    - **Method.** The pre-fix emissions column was rebuilt from `attribution_baseline_pre_wind_fix.csv`. It
      reproduces the pre-fix figures above exactly (112 at the cap, 657 non-zero), and the same method
      reproduces the released column on 4,500 of 4,500 facility-days.
  - **What moved is where the repeat plumes land.**
    - **Pre-fix,** the facilities holding repeat plumes averaged 16.4 condition points on their capped days:
      GS-0026, GS-0010, GS-0058, GS-0070, GS-0127, GS-0118 and GS-0038. Their highest emissions + condition
      + maintenance total was 60.15.
    - **After the fix,** the repeat plumes sit on GS-0100, GS-0125, GS-0051, GS-0064, GS-0056, GS-0043,
      GS-0091 and GS-0038, which average 22.2 condition points on their capped days. Their highest such
      total is 67.19. GS-0038 is the only facility in both sets.
    - **Compliance adds 10 points** on every capped day at GS-0051, GS-0064 and GS-0091. That is consistent
      with 03c opening a state case on repetition (03c:432–450). It cannot be confirmed from the served
      tables, because `fact_compliance_event` is not served. GS-0100 has 3 plumes and no compliance points.
  - **This is an argument for the decision, not against it.**
    - **The 69.3 maximum was never a property of the score.** Its cap is 100. The 69.3 reflected which
      facilities the broken attribution happened to pick.
    - **The new top is co-location.** It is set by a real signal (two plumes) landing on facilities that
      already carry high **synthetic** condition points. Condition is drawn independently of plume
      geography, so the shift toward high-condition facilities (n = 7 → 8) is most likely chance.
    - **What a bare `risk_score` would show.** A one-time 11-point jump in the maximum, presented as a
      change in methane risk. It was produced by re-attributing eight facilities' plumes onto a different
      draw of a synthetic index.
    - **What ranking on observed methane shows.** It is unaffected by the condition draw. The stacked
      breakdown shows exactly which part of a high score is methane and which part is synthetic.

**Options.**
1. **Reweight** (for example raise emissions, lower condition).
   - **For.** Keeps a single sortable number.
   - **Against.**
     - Any new weights are a fresh modelling choice with no calibration target. 03e already calls every
       weight that (03e:267).
     - It still blends one real signal with four synthetic ones into one number.
     - With emissions zero on 85 % of facility-days, no weight on that component can separate facilities on
       most days.
2. **Never show a bare `risk_score`.**
   - **How it works.** Rank facilities primarily on emissions (E3 per facility in W, then plume count), and
     show the stacked `risk_pts_*` breakdown beside the rank as context, each component labelled, synthetic
     ones marked.
   - **For.** The rank rests on the one measured signal. Every point stays traceable to its component. No
     new constant is introduced.
   - **Against.** Facilities with no detection (~~118~~ **119** of 150 *(2026-10-15)*) tie on the primary key and fall back to the
     breakdown.

**Recommendation (not a decision): option 2.** The stored score is internally consistent: it is asserted to
equal its parts (03e:1415–1418). But a bare number would present a mostly synthetic condition index as
methane risk. If a secondary sort is needed for the 118 tied facilities, use a single named component
(for example open work or maintenance), not the sum.

---

## Part 5. Open questions

Each is something code and data could not settle. None is chosen here.

1. ~~**05's wind input.**~~ **Resolved 2026-10-14** (commit c93dd7a; verified 2026-10-15).
   - **What changed.** The catalog carries `era5_u10/v10`, and 05 scores against the toward direction.
   - **As expected,** attributions moved: 32 of 42 winners changed.
   - **Follow-on.** The enterprise layer has not followed (Q12).
   - **Context.** Attribution uses a fixed 180° upwind for every plume (Decision B). The fix is to pass the
     ERA5 u/v (or a direction) into the catalog, or to read it in 05 from `bronze_era5_wind`.
   - **To decide.** Whether to fix this before any facility-level tile ships. Who owns it.
   - **What to expect.** After a fix, attributions may move between facilities. Every facility measure in
     Part 1 would change.
2. ~~**Who turned acceleration on** for `fact_facility_daily_snapshot` and `dim_facility`?~~
   **Resolved.** Nobody turned it on: the two alters that set it OFF had never been run. Both now read
   `IsEnabled: false` (section 0).
3. **19.26 vs 19.2 kg/mcf.** See "Also settle".
4. **Portfolio denominator: 150 facilities or 143 active?** The snapshot carries all 150 (03e:1038 filters
   `is_current`, not `active_flag`). The 7 inactive facilities have 0 plumes.
5. **Portfolio uncertainty band.**
   - **What exists.** No Monte Carlo samples are stored. A true P5/P95 of a total would need 04 to keep its
     draws, or a re-run of `mc_rates_for` with the stored seeds (04:1193, 04:1236).
   - **What is unknown.** How correlated the per-plume errors are. Plumes in one scene share ERA5 wind.
     Until it is known, R4c's "fully correlated" range is the only defensible total band.
6. **Methane intensity.**
   - **Context.** Its numerator is an observed-mass floor from a 30-day window. Its denominator
     (`fact_production_daily`) is synthetic, with V1's base volumes that 03e calls "small for real Permian
     plants" (03e:326–328).
   - **To decide.** Whether a floor-over-synthetic ratio belongs on the Executive page at all.
7. **What `equipment_health_score` should be called.** It is 1 − a synthetic condition index, observed range
   0.763–1.0. Whether to show it at all, and under what name, is not a code question.
8. **SCADA measurand.** The hourly table has `tag_id` and `uom` but no tag type or description.
   - Telling a level from a position (both `percent`) relies on parsing the `tag_id` prefix.
   - A proper Facility Operations SCADA panel needs `dim_scada_tag`, a fifth table.
9. **Three active facilities have no SCADA** (GS-0077, GS-0115, GS-0119). Is that intended by 01d/02b, or a
   gap? ~~GS-0119 has a plume.~~ *(2026-10-15: none of the three has an attributed plume after the wind fix.)*
10. **Pipelines unscheduled.**
    - All horizons are fixed in the past: detection 2026-09-15, enterprise and SCADA 2026-09-14.
    - If the pipelines are scheduled later, R1's horizons become moving values.
    - The board then needs a declared refresh contract: which horizon each page waits for.
11. **Corrections to the brief, recorded so they are not re-used.**
    - Probability-1.0 single-candidate plumes number **7**, not 5.
    - GS-0002 is within 1.1 points of GS-0001 on 3 of 30 days, not generally. It outranks GS-0001 on 7.
    - `TOPOLOGY_AS_OF` is 2026-09-15, the exclusive end. 2026-09-14 is the last enterprise day.
    - `KG_CH4_PER_MCF` **does** exist in V2 (below).
    - *(2026-10-15)* The catalog holds 50 plumes, not 49. Do not carry 49 forward.
    - *(2026-10-15)* Facilities with a detection in the common window: 31.
12. ~~**The enterprise layer predates the wind fix** *(2026-10-15)*.~~ **Closed 2026-10-15.**
    - **The fix.** `greensky_enterprise` backfill, `pipeline_date` empty, run `59dedf66`.
    - **The gate.** 42 of 42 plume facility-days match.
    - **Release record:** `rti_board_design.md` §14. The original text follows.
    - **Evidence.** `fact_facility_daily_snapshot` matches the **pre-fix** attribution on 38 of its 41 plume
      facility-days, and the current catalog on 8.
      - The 3 that miss the pre-fix file are on the re-assigned scene SCN-20260819T202934.
      - In the common window, 33 facility-days appear only in the catalog, 32 only in the snapshot, and 1
        in both with different values.
    - **What it taints:**
      - Snapshot columns: `active_plumes`, `total_emissions_kg`, `emission_rate_kg_s_sum`,
        `daily_financial_impact`, `compliance_violations`, `open_compliance_cases`, `risk_pts_emissions`,
        `risk_pts_compliance` and `risk_score`.
      - **Through 03c → 03b → 03d,** potentially the ticket and maintenance stocks as well. 03b raises
        P1/P2 tickets from 03c's violations (03b:1224–1231), and 03d reads `fact_work_order`.
    - **The remedy.** A `greensky_enterprise` backfill.
    - **The release test.** Gate G1 in `rti_board_design.md` §7.
    - **Preventing a recurrence:** `rti_board_design.md` §11.

---

## Also settle

### The 19.26 constant

**V2 does store it, as a named constant with a stated source, in 03e and nowhere else.**
- `KG_CH4_PER_MCF = 19.26` (03e:279).
- Kind `"physical constant"`, with this basis (03e:293–297, verbatim):
  > "V1's value, and the divisor the dashboard's Methane Intensity tile hard-codes. GHGRP Subpart W uses
  > 0.0192 kg/scf (19.2 kg/mcf) at 60 F and 14.7 psia (40 CFR 98.233); the 0.3% difference is kept so this
  > table and that tile agree."
- 03e uses it for lost gas (03e:636) and in its own intensity printout (03e:1522–1523).
- `00_config` and `01_topology_config` hold no CH₄ density or kg-per-volume constant. `00_config` has only
  the molar mass `M_CH4` (00_config:163).

**Consequence.** A V2 intensity measure must reference `KG_CH4_PER_MCF` from 03e by name, never a bare 19.26,
so that intensity and lost-gas value use one figure.

**The decision that remains.** The value itself is V1's, and its cited source says 19.2. Adopting 19.2 would
move every lost-gas dollar by 0.3 % and requires a 03e change and backfill. That is Part 5, Q3, not a copy.

### Freshness

| table | as-of column | current value | notes |
|---|---|---|---|
| gold_plume_catalog | `max(detection_date)` | 2026-09-15 20:17:52.5Z | No run timestamp column. Whole-table overwrite by 04 then 05. Last re-derived 2026-10-14 ~12:00Z (greensky_detection backfill, `pipeline_date` 2026-09-15). 50 plumes, 41 columns. |
| — | — | — | *(released)* `fact_facility_daily_snapshot` was last written by `greensky_enterprise` run `59dedf66` (03e 07:10–07:15Z, 2026-10-15), **after** that re-derivation. 4,500 rows, last day 2026-09-14, 27 columns. ~~Previously the 2026-10-12 07:03Z backfill, before it (Q12).~~ |
| fact_facility_daily_snapshot | `max(snapshot_date)` | 2026-09-14 | `date_sk` 20260914 (a string in KQL). 30 days × 150 facilities. |
| scada_telemetry_hourly | `max(bucket_ts)` | 2026-09-14 23:00Z | 30 days. 147 of 150 facilities. |
| dim_facility | `effective_from` | 2026-09-15 on all 150 rows | `topology_seed` 20260915. No `is_synthetic` column. |

**Rule.** Each page shows the horizon of every source it reads (R1d). A page that reads two sources shows both.
