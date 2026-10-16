# RTI dashboard investigation — 2026-10-12

Read-only. Nothing was created, written, run or changed in Fabric. All facts below come from
calls made on 2026-10-12 as `prutha.annadate@ilink-systems.com` (Azure CLI token). The
methods and the exact queries are under each section and in the Methods appendix. CANNOT REACH
means that the call was made and refused, or that the object was not found; nothing is inferred
in its place.

---

## 0. Access

| Target | Reachable? | Mechanism | Proof |
|---|---|---|---|
| Eventhouse **GreenSky_EH**, KQL DB `c0d0aefe-9ec0-4bfb-ac7d-fe002b4f1060` | **CANNOT REACH** | — | See below |
| Lakehouse **greensky_v2_lakehouse** (`5d5c8002-…`, workspace `640876ea-…`): schemas and row counts | **Yes**, all 41 tables | (a) **OneLake DFS REST** (`onelake.dfs.fabric.microsoft.com`, token for `https://storage.azure.com`): Delta `_delta_log` replay, Parquet footers and row-group statistics by HTTP range read. (b) **SQL analytics endpoint** via ODBC Driver 18 with an Entra token, but it only exposes 15 of the 41 tables | Below |
| KQL execution in general | **Yes** | **Kusto REST API** (`/v1/rest/mgmt`, `/v1/rest/query`) on the two Eventhouses that exist in Green Sky - Dev, token for `https://kusto.kusto.windows.net` | Below |

**Why GreenSky_EH is CANNOT REACH.**
- Listing eventhouses and KQL databases in every workspace this account can see (11
  workspaces) found no item named GreenSky_EH and no item with id `c0d0aefe-…`. Listing KQL
  databases in *Customer Demos* returned HTTP 500, but none of its eventhouses' `databasesItemIds`
  contains `c0d0aefe-…`.
- `GET /v1/workspaces/640876ea-…/items/c0d0aefe-9ec0-4bfb-ac7d-fe002b4f1060` → `404 EntityNotFound`.
- The only definition that names GreenSky_EH is the **repo** file
  `Methane Emissions/Accelerator/GreenSky_RTI.KQLDashboard/RealTimeDashboard.json`, data source
  `{"name": "GreenSky_EH", "clusterUri": "", "databaseArtifactId": "c0d0aefe-9ec0-4bfb-ac7d-fe002b4f1060"}`.
  Its only commit is `d1d786d Committing 60 items from workspace 060ba34b-f1a3-4509-a6e2-36d1e736a8eb`.
- `GET /v1/workspaces/060ba34b-f1a3-4509-a6e2-36d1e736a8eb` → `404 WorkspaceNotFound`. The
  workspace is either deleted or not shared with this account; the API does not say which.

**What is reachable instead.** Green Sky - Dev holds two eventhouses. The board deployed there,
*Methane Intelligence - PC v1* (`dc4fd2af-…`), reads **Operations_KQL**, not GreenSky_EH. Its only
data source is:
```
{"kind": "kusto-trident", "name": "Operations_KQL", "clusterUri": "https://trd-9k8b5vackx1a66j3jm.z3.kusto.fabric.microsoft.com",
 "databaseArtifactId": "85d85e9c-2342-460b-8085-5883c8266286", ...}
```
Section 1 inventories Operations_KQL and GreenSky_Analytics, labelled as such. They are **not**
GreenSky_EH.

KQL proof, `.show databases` on each (first lines, verbatim):
```
== GreenSky_Analytics
DatabaseName | PersistentStorage | Version | IsCurrent | DatabaseAccessMode | PrettyName | ...
fe23d680-d3d4-496d-9b0c-a28508734eea | https://zy3virtualengines.z7.blob.storage.azure.net/398unallocateddbmd202608301605041475 | v38.0 | True | ReadWrite | GreenSky_Analytics | ...
== Operations_KQL
85d85e9c-2342-460b-8085-5883c8266286 | https://3v1virtualengines.blob.core.windows.net/fax85d85e9c2342460b80855883c8266286md202606300517312332 | v1092.53 | True | ReadWrite | Operations_KQL | ...
```
`.show database schema | project DatabaseName, TableName, ColumnName, ColumnType | take 8` returns
one row with an empty table name on both: neither database has a native table.

Lakehouse proof, SQL endpoint, `SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES`
(dbo rows, verbatim):
```
dbo | bronze_ch4_pixels | BASE TABLE        dbo | gold_plume_catalog | BASE TABLE
dbo | bronze_era5_wind | BASE TABLE         dbo | gold_plume_site_mapping | BASE TABLE
dbo | bronze_methane_pixels | BASE TABLE    dbo | ref_facilities | BASE TABLE
dbo | bronze_no2_pixels | BASE TABLE        dbo | silver_plume_ready_pixels | BASE TABLE
dbo | bronze_weather | BASE TABLE           dbo | validation_cams_plumes | BASE TABLE
dbo | gold_emission_sites | BASE TABLE      dbo | validation_cams_plumes_global | BASE TABLE
dbo | gold_flagged_large_clusters | BASE TABLE   dbo | validation_carbon_mapper_plumes | BASE TABLE
dbo | gold_multi_gas_signatures | BASE TABLE
```
OneLake `Tables/dbo` holds **41** tables. **The SQL endpoint does not expose 26 of them**: every
`dim_*`, `fact_*` and `scada_*` table, plus `sensor_telemetry`, `gold_plume_id_runs` and
`gold_rejected_collinear`. `SELECT TOP 40 * FROM sys.dm_db_external_tables_log_status` returned
0 rows. Refreshing the endpoint's metadata would be an action on Fabric, so it was not done; any
SQL-endpoint or Power BI consumer currently cannot see the enterprise tables. All counts and
schemas below for those 26 come from OneLake.

---

## 1. Eventhouse inventory

### GreenSky_EH — CANNOT REACH
Not found (section 0). Its tables, shortcuts, materialized views, OneLake availability, and the
definitions of **`hourly_trends()`** and **`facility_metrics`** cannot be read. What *can* be
stated, from the repo file only: the GreenSky_RTI board calls both. `hourly_trends()` appears in
tile *Emission Trends* (`hourly_trends() | where hour_bin >= ago(300d) | project hour_bin,
total_emissions, super_emitters, avg_emission_per_event`), and `facility_metrics` in *High-Risk
Facilities* (`facility_metrics | where detection_count > 0 | … priority_score,
total_financial_impact`). That tells us what they return, not how they are defined. They have
no definition in the repo, and none was reachable. **Unresolved.**

### Operations_KQL (the database the deployed board reads)
Commands: `.show tables details`, `.show external tables`, `.show materialized-views`,
`.show functions`, `.show database Operations_KQL policy mirroring`. Shortcut targets come from
`GET /v1/workspaces/640876ea-…/items/85d85e9c-…/shortcuts`. Row counts come from
`external_table('<name>') | count`, run per table.

- Native (ingested) tables: **0**. Materialized views: **0**. Functions: **0**, so
  `hourly_trends()` and `facility_metrics` are **not defined here**.
- OneLake availability (mirroring policy): `MirroringPolicy | [85d85e9c-…] | null`, so **off**.
- External tables: **29**, all `TableType = Delta`, `"Shortcut": true`.

| External table (shortcut) | Rows (`count`) | Resolves to | Lakehouse |
|---|---:|---|---|
| bridge_plume_equipment | 3,795 | Operations_LH :: Tables/gold/bridge_plume_equipment | **V1** |
| bridge_plume_facility_attribution | 2,879 | Operations_LH :: Tables/gold/bridge_plume_facility_attribution | **V1** |
| ch4_plume_catalog | 291 | Planetary_computer_LH :: Tables/silver/ch4_plume_catalog | elsewhere |
| dim_contractor | 6 | Operations_LH :: Tables/gold/dim_contractor | **V1** |
| dim_date | 417 | Operations_LH :: Tables/gold/dim_date | **V1** |
| dim_equipment | 10,575 | Operations_LH :: Tables/gold/dim_equipment | **V1** |
| dim_equipment_type | 8 | Operations_LH :: Tables/gold/dim_equipment_type | **V1** |
| dim_facility | 350 | Operations_LH :: Tables/gold/dim_facility | **V1** |
| dim_manufacturer | 8 | Operations_LH :: Tables/gold/dim_manufacturer | **V1** |
| dim_operator | 12 | Operations_LH :: Tables/gold/dim_operator | **V1** |
| dim_regulation | 3 | Operations_LH :: Tables/gold/dim_regulation | **V1** |
| dim_sensor | 1,400 | Operations_LH :: Tables/gold/dim_sensor | **V1** |
| dim_team | 8 | Operations_LH :: Tables/gold/dim_team | **V1** |
| equipment_geo | 10,575 | Operations_LH :: Tables/silver/equipment_geo | **V1** |
| facility_kpi_snapshot_6h | 597,800 | Operations_LH :: Tables/gold/facility_kpi_snapshot_6h | **V1** |
| fact_compliance_event | 198,368 | Operations_LH :: Tables/gold/fact_compliance_event | **V1** |
| fact_facility_daily_snapshot | 2,506 | Operations_LH :: Tables/gold/fact_facility_daily_snapshot | **V1** |
| fact_financial_impact | 257,459 | Operations_LH :: Tables/gold/fact_financial_impact | **V1** |
| fact_inspection | 21,697 | Operations_LH :: Tables/gold/fact_inspection | **V1** |
| fact_ldar_survey | 1,626 | Operations_LH :: Tables/gold/fact_ldar_survey | **V1** |
| fact_maintenance | 21,755 | Operations_LH :: Tables/gold/fact_maintenance | **V1** |
| fact_plume_detection | 2,360 | Operations_LH :: Tables/gold/fact_plume_detection | **V1** |
| fact_production_daily | 145,950 | Operations_LH :: Tables/gold/fact_production_daily | **V1** |
| fact_work_order | 192,698 | Operations_LH :: Tables/gold/fact_work_order | **V1** |
| gold_multi_gas_signatures | 109 | greensky_v2_lakehouse :: Tables/dbo/gold_multi_gas_signatures | **V2** |
| plume_detection_hot | 2,360 | Operations_LH :: Tables/gold/plume_detection_hot | **V1** |
| sensor_telemetry | 3,586,800 | Operations_LH :: Tables/gold/sensor_telemetry | **V1** |
| weather | 84 | Planetary_computer_LH :: Tables/bronze/weather | elsewhere |
| workorder_event | 412,521 | Operations_LH :: Tables/gold/workorder_event | **V1** |

**26 of 29 point at V1 Operations_LH, 2 at Planetary_computer_LH, 1 at greensky_v2_lakehouse.**

Note on the V2 shortcut: through KQL, `gold_multi_gas_signatures` counts **109**, but the V2 table
itself has **49** rows (SQL `COUNT(*)` = 49, OneLake = 49, Delta version 4). The cause was not
investigated (it would need reading the shortcut's resolved files). It is recorded here as an
open discrepancy.

### GreenSky_Analytics (exists in Green Sky - Dev; not referenced by any board)
Same commands: 0 native tables, 0 external tables, 0 materialized views, 0 functions; mirroring
policy null (OneLake availability off). **Empty.**

---

## 2. Time anchor

MAX of every timestamp/date column, read from the **Parquet row-group statistics of every active
file** of each V2 table (OneLake). For the 15 SQL-visible tables it was cross-checked with
`SELECT MAX(...)`, and the two agree (e.g. `gold_plume_catalog`: `MAX(detection_date)` =
`2026-09-15 20:17:52.5` both ways).

Event-time column of each table the board could read:

| Table | Primary timestamp | MAX |
|---|---|---|
| gold_plume_catalog | detection_date | 2026-09-15 20:17:52.5 |
| gold_multi_gas_signatures | detect_ts | 2026-09-15 20:17:52.5 |
| gold_emission_sites | last_detection | 2026-09-15 20:17:52.5 |
| silver_plume_ready_pixels | datetime | 2026-09-15 20:28:46.5 |
| bronze_ch4_pixels | datetime | 2026-09-15 20:28:46.5 |
| bronze_no2_pixels | datetime | 2026-09-15 20:17:52.5 |
| bronze_weather | time | 2026-09-15 23:00:00 |
| scada_telemetry | reading_ts | 2026-09-14 23:55:00 |
| scada_telemetry_hourly | bucket_ts | 2026-09-14 23:00:00 |
| scada_telemetry_daily | bucket_ts | 2026-09-14 00:00:00 |
| sensor_telemetry | reading_ts | 2026-09-14 23:00:00 |
| fact_asset_state | start_ts | 2026-09-14 23:59:11.381 |
| fact_scada_alarm | raised_ts | 2026-09-14 23:45:00 |
| fact_sensor_status_event | event_ts | 2026-09-14 23:45:00 |
| fact_facility_daily_snapshot | snapshot_date | 2026-09-14 00:00:00 |
| fact_production_daily | prod_date | 2026-09-14 00:00:00 |
| fact_financial_impact | detect_ts | 2026-09-14 10:28:56 |
| fact_work_order | created_ts | 2026-09-14 23:56:08.353 |
| fact_work_order_event | event_ts | 2026-09-14 23:56:08.353 |
| fact_compliance_event | event_ts | 2026-09-14 20:00:00 |
| fact_maintenance | maintenance_ts | 2026-09-14 22:58:57 |
| fact_inspection | inspection_ts | 2026-09-14 22:10:46 |
| fact_ldar_survey | survey_ts | 2026-09-13 08:00:00 |
| fact_pm_schedule | as_of_ts | 2026-09-15 00:00:00 |
| fact_emission_episode | start_ts | 2026-09-14 23:38:56 |

**Anything later than 2026-09-15:**

| Table.column | MAX | What it is |
|---|---|---|
| bronze_era5_wind.era5_time | **2026-09-23 08:00** | Reanalysis input; 02b_ingest_era5 loads the whole covering file (`era5_permian_202608_202609.nc`), not the window |
| fact_emission_episode.end_ts | **2026-10-04 03:06:18** | Episodes running past the window; hidden ground truth, not a board source |
| fact_pm_schedule.next_due_ts | **2027-09-14** | Forward due dates, by design |
| dim_regulation.effective_from | **2027-01-22** | A regulation effective in the future |
| scada_telemetry.ingest_ts / sensor_telemetry.ingest_ts | 2026-10-12 07:25 / 07:08 | Load time of today's enterprise run, not event time |
| gold_plume_id_runs.run_ts | 2026-10-12 06:31 | 04's run log |
| `effective_to` (dim_*) | 2999-12-31 | SCD2 open-ended sentinel |

No **event-time** column of any board source is later than 2026-09-15. There are, however, **two
horizons, not one**. The detection tables end on **2026-09-15** (window `[08-16, 09-16)`); every
enterprise/SCADA table ends on **2026-09-14** (window `[08-16, 09-15)`, ending at
TOPOLOGY_AS_OF). A single data-derived `max()` across sources would therefore not be one anchor.
Each source's own max, or a parameter, is needed. Anchoring on `now()` would select nothing
(today is 2026-10-12).

---

## 3. Serving-layer evidence

The **row counts** in this section are counts of the stored rows. For every table they are the
sum of `num_rows` over the Parquet footers of the Delta snapshot's active files; no file carries
a deletion vector, so the sum is exact. They are equal to the sum of `add.stats.numRecords` in
every case. Where the SQL endpoint can see a table, `SELECT COUNT(*)` was also run and agrees.
The samples are the first three rows of the first active file.

### fact_facility_daily_snapshot — 4,500 rows, partitioned by `date_sk`
**Grain: facility × day** (150 facilities × 30 days, 2026-08-16 … 2026-09-14). Written by 03e.

Schema: `date_sk:long, facility_sk:long, facility_id:string, snapshot_date:timestamp,
active_plumes:long, total_emissions_kg:double, emission_rate_kg_s_sum:double,
daily_financial_impact:double, open_tickets:long, stalled_backlog:long,
mean_time_to_repair_hr:double, compliance_violations:long, open_compliance_cases:long,
overdue_pms:long, ldar_leaks_outstanding:long, active_exceedances:long, sensors_offline:long,
alarms_raised:long, alarms_active:long, equipment_health_score:double, risk_score:double,
risk_pts_condition:double, risk_pts_emissions:double, risk_pts_compliance:double,
risk_pts_maintenance:double, risk_pts_open_work:double, is_synthetic:boolean`

Measures it already carries: emissions (kg and summed rate), plumes, financial impact, tickets
and backlog, MTTR, compliance, overdue PMs, LDAR outstanding, CH4 exceedances, sensors offline,
alarms, health, and the risk score with its five component points.
```
{"facility_sk": 1, "facility_id": "GS-0001", "snapshot_date": "2026-08-27", "active_plumes": 0, "total_emissions_kg": 0.0, "emission_rate_kg_s_sum": 0.0, "daily_financial_impact": 0.0, "open_tickets": 0, "stalled_backlog": 1, "mean_time_to_repair_hr": 32.008, "compliance_violations": 0, "open_compliance_cases": 0, "overdue_pms": 2, "ldar_leaks_outstanding": 0, "active_exceedances": 0, "sensors_offline": 0, "alarms_raised": 3, "alarms_active": 0, "equipment_health_score": 0.946611, "risk_score": 29.88, "risk_pts_condition": 8.43, "risk_pts_emissions": 12.5, "risk_pts_compliance": 0.0, "risk_pts_maintenance": 3.95, "risk_pts_open_work": 5.0, "is_synthetic": true, "date_sk": 20260827}
{"facility_sk": 2, "facility_id": "GS-0002", "snapshot_date": "2026-08-27", "active_plumes": 0, "total_emissions_kg": 0.0, ..., "equipment_health_score": 0.863491, "risk_score": 28.76, "risk_pts_condition": 21.55, "risk_pts_emissions": 0.0, "risk_pts_compliance": 0.0, "risk_pts_maintenance": 2.21, "risk_pts_open_work": 5.0, ...}
{"facility_sk": 3, "facility_id": "GS-0003", "snapshot_date": "2026-08-27", ..., "mean_time_to_repair_hr": 102.286, "overdue_pms": 2, "equipment_health_score": 0.884112, "risk_score": 25.8, "risk_pts_condition": 18.3, "risk_pts_emissions": 0.0, "risk_pts_compliance": 0.0, "risk_pts_maintenance": 7.5, "risk_pts_open_work": 0.0, ...}
```

### gold_plume_catalog (after 05) — 49 rows (SQL `COUNT(*)` = 49), not partitioned
**Facility attribution is inline**: `attributed_facility_id:string, attributed_facility_name:string,
attributed_facility_lat:double, attributed_facility_lon:double, attribution_probability:double,
attribution_distance_km:double, attribution_angular_offset_deg:double, attribution_method:string,
facilities_in_range:long, second_facility_id:string, second_facility_probability:double`. It carries
the business key `facility_id` (`GS-xxxx`), **not `facility_sk`**. Joining any `facility_sk`-keyed
fact, or anything from dim_facility beyond name and coordinates, requires
`dim_facility.facility_id = attributed_facility_id`. `SELECT COUNT(*), COUNT(attributed_facility_id)`
→ **49 | 42**: 7 plumes are unattributed.

Full schema: `plume_id:string, scene_id:string, stac_id:string, detection_date:timestamp,
source_lat:double, source_lon:double, n_pixels:long, plume_area_km2:double,
peak_ch4_enhancement_ppb:double, mean_ch4_enhancement_ppb:double, ime_kg:double,
emission_rate_kg_s:double, emission_rate_kg_h:double, emission_rate_t_h:double,
emission_rate_p5_kg_s:double, emission_rate_p50_kg_s:double, emission_rate_p95_kg_s:double,
emission_rate_p5_kg_h:double, emission_rate_p50_kg_h:double, emission_rate_p95_kg_h:double,
t_mix_s:double, t_mix_method:string, U_eff_ms:double, L_m:double, wind_alignment_deg:double,
wind_confidence:string, uncertainty_ratio:double, confidence:string,` + the 11 attribution columns above.

Samples (SQL, `TOP 3 … ORDER BY detection_date DESC`, attribution columns):
```
PL-d6024ee3f97a | 2026-09-15 20:17:52.5 | GS-0072 | Kermit Tank Battery 4          | 30.3823 | -104.8631 | 1.0    | 30.317 | wind_aware
PL-f19b099ff53b | 2026-09-11 20:03:46.5 | GS-0035 | Imperial Collection Facility 2 | 31.7819 | -102.4656 | 0.5048 | 13.100 | wind_aware
PL-ee07d05aa443 | 2026-09-11 19:55:56   | GS-0038 | Monahans Tank Battery          | 30.2696 | -102.0633 | 1.0    | 43.796 | wind_aware
```
Samples (OneLake, measure columns):
```
{"plume_id": "PL-5e5dc7a6da41", "detection_date": "2026-09-04 20:28:17", "source_lat": 31.6707, "source_lon": -103.3089, "n_pixels": 6, "ime_kg": 39765.07, "emission_rate_kg_h": 22178.43, "emission_rate_p5_kg_h": 10974.50, "emission_rate_p50_kg_h": 22017.92, "emission_rate_p95_kg_h": 34973.56, "t_mix_method": "wind_dependent", ...}
{"plume_id": "PL-2c80a662469b", "detection_date": "2026-09-04 20:33:46.5", "ime_kg": 26884.93, "emission_rate_kg_h": 19499.82, ...}
{"plume_id": "PL-f52a4cb34f9a", "detection_date": "2026-09-10 20:23:46.5", "ime_kg": 23246.26, "emission_rate_kg_h": 14751.14, ...}
```

### fact_financial_impact — 44 rows, partitioned by `date_sk`
**Grain: one row per (plume, facility) impact**, with `impact_type` (the samples are all
`emission`). The key to plumes is **`plume_id`**; to facilities, **`facility_sk`** (and
`facility_id`); also `compliance_sk` and `regulation_sk`. The 44 rows span 16 distinct `date_sk`.

Schema: `financial_sk:long, facility_sk:long, facility_id:string, plume_id:string,
compliance_sk:long, regulation_sk:long, impact_type:string, detect_ts:timestamp,
period_month:string, emission_rate_kg_h:double, duration_h:double, allocation_factor:double,
attributed_emissions_kg:double, gwp_co2e_tonnes:double, lost_gas_value_usd:double,
violation_fine_usd:double, social_cost_usd:double, total_impact_usd_p5:double,
total_impact_usd_p50:double, total_impact_usd_p95:double, fine_scenario:string, date_sk:long,
is_synthetic:boolean`
```
{"financial_sk": 74207796234702006, "facility_sk": 46, "facility_id": "GS-0046", "plume_id": "PL-094e1a99c910", "impact_type": "emission", "detect_ts": "2026-08-30 20:22:22.5", "emission_rate_kg_h": 43713.37, "duration_h": 0.97729, "allocation_factor": 1.0, "attributed_emissions_kg": 42720.641, "gwp_co2e_tonnes": 1196.178, "lost_gas_value_usd": 6099.78, "violation_fine_usd": 0.0, "social_cost_usd": 0.0, "total_impact_usd_p5": 2949.64, "total_impact_usd_p50": 5770.42, "total_impact_usd_p95": 9904.4, "fine_scenario": "reporting_only", "date_sk": 20260830}
{"financial_sk": 5205950150261370661, "facility_sk": 144, "facility_id": "GS-0144", "plume_id": "PL-84d936fc5c16", ..., "attributed_emissions_kg": 8759.975, "lost_gas_value_usd": 1250.78, "total_impact_usd_p50": 1162.44, ...}
{"financial_sk": 8548392409650465120, "facility_sk": 12, "facility_id": "GS-0012", "plume_id": "PL-881dbf57ee04", ..., "attributed_emissions_kg": 41059.547, "lost_gas_value_usd": 5862.6, "total_impact_usd_p50": 5607.12, ...}
```

### dim_facility — 150 rows (`ref_facilities` SQL `COUNT(*)` = 150 as well)
Full column list (21): `facility_sk:long, facility_id:string, facility_name:string,
name_descriptor:string, facility_type:string, operator_sk:long, sub_basin:string,
region_band:string, facility_lat:double, facility_lon:double, bbox_excursion_deg:double,
country:string, state:string, commission_date:timestamp, active_flag:boolean,
in_detection_bbox:boolean, operator_name:string, effective_from:timestamp,
effective_to:timestamp, is_current:boolean, topology_seed:long`
```
{"facility_sk": 1, "facility_id": "GS-0001", "facility_name": "Sand Hills Compression Station", "facility_type": "Compression Station", "operator_sk": 9, "sub_basin": "Delaware Basin", "region_band": "inside", "facility_lat": 31.7306, "facility_lon": -103.8085, "country": "USA", "state": "TX", "commission_date": "2022-10-27", "active_flag": true, "in_detection_bbox": true, "operator_name": "Operator 09", "is_current": true, ...}
{"facility_sk": 2, "facility_id": "GS-0002", "facility_name": "Monahans Collection Facility", "facility_type": "Gathering System", "operator_sk": 12, "sub_basin": "Delaware Basin", ..., "operator_name": "Operator 12", ...}
{"facility_sk": 3, "facility_id": "GS-0003", "facility_name": "Sand Hills Tank Battery", "facility_type": "Tank Battery", "operator_sk": 7, "sub_basin": "Central Basin Platform", ..., "operator_name": "Operator 07", ...}
```

### scada_telemetry_hourly — 3,265,025 rows; scada_telemetry_daily — 137,092 rows (both partitioned by `date_sk`)
Hourly schema: `tag_sk:long, tag_id:string, equipment_sk:long, area_sk:long, facility_sk:long,
bucket_ts:timestamp, date_sk:long, value_avg:double, value_min:double, value_max:double,
value_stddev:double, sample_count:integer, expected_count:integer, good_count:integer,
bad_count:integer, uncertain_count:integer, substituted_count:integer, pct_good:double,
pct_time_running:double, dominant_state:string, n_running:integer, n_standby:integer,
n_down:integer, n_maintenance:integer, n_startup:integer, n_shutdown:integer,
value_sum_e6:long, value_sumsq_e12:decimal(38,0), uom:string, is_synthetic:boolean`.
Daily: the same columns plus `hours_present:integer`.
```
hourly: {"tag_id": "GS-0107.A1.TT-103", "facility_sk": 107, "bucket_ts": "2026-09-14 13:00", "value_avg": 93.425, "value_min": 88.9, "value_max": 96.9, "sample_count": 12, "good_count": 12, "pct_good": 1.0, "dominant_state": "Running", "uom": "degF", ...}
hourly: {"tag_id": "GS-0017.A1.PT-108", "facility_sk": 17, "bucket_ts": "2026-09-14 20:00", "value_avg": 62.54, ..., "uom": "psig"}
hourly: {"tag_id": "GS-0023.A5.FT-116", "facility_sk": 23, "bucket_ts": "2026-09-14 18:00", "value_avg": 3588.75, ..., "uom": "mscfd"}
daily:  {"tag_id": "GS-0087.A2.AT-104", "facility_sk": 87, "bucket_ts": "2026-09-14", "value_avg": 16.93, "sample_count": 95, "expected_count": 96, "hours_present": 24, "uom": "ratio", ...}
daily:  {"tag_id": "GS-0015.A2.TT-103", "facility_sk": 15, "bucket_ts": "2026-09-14", "value_avg": 109.20, "pct_good": 0.9895, "uom": "degF", ...}
daily:  {"tag_id": "GS-0034.A2.VT-115", "facility_sk": 34, "bucket_ts": "2026-09-14", "value_avg": 0.1101, "uom": "in/s", ...}
```

### Proposed minimum serving set (two pages)

| # | Table | Serves | Status | Owner |
|---|---|---|---|---|
| 1 | `fact_facility_daily_snapshot` | Both pages: emissions, financial, tickets, compliance, maintenance, sensors, risk (with components), per facility per day | **Present** | 03e_gen_financial_snapshots |
| 2 | `gold_plume_catalog` | Plume list, rates with the p5/p50/p95 band, map points, inline attribution | **Present** | 04_derive_emissions (05 adds attribution) |
| 3 | `dim_facility` | Names, type, operator, coordinates, the selector parameter, and `facility_id` ↔ `facility_sk` for the plume join | **Present** | 01a_build_facility_topology |
| 4 | `scada_telemetry_hourly` | Facility page: operating trends per tag | **Present** | 02c_rollup_telemetry |

No new notebook work is needed for the set. Two non-notebook gaps stand between it and a board:
- **Operations_KQL has no shortcut to any of the four.** Its `dim_facility` and
  `fact_facility_daily_snapshot` resolve to V1 Operations_LH (section 1).
- **The SQL endpoint cannot see 1, 3 and 4** (section 0).

`fact_financial_impact` (plume × facility) is optional for these two pages: the snapshot already
carries `daily_financial_impact` per facility-day. It is needed only for a per-plume financial
drill-down, keyed by `plume_id` / `facility_sk`.

---

## 4. V1 → V2 vocabulary

Where the V1 names are actually used: 13 of the 15 occur only in the **repo** dashboard
`GreenSky_RTI.KQLDashboard` (bound to the unreachable GreenSky_EH), mostly against
`external_table('gold_emission_events')` and `external_table('dim_facility')`. **None of the 15
is used by the deployed board's 25 queries.** A schema scan of every Operations_KQL external
table (`external_table('<t>') | getschema`) and of all 32 Delta tables in Operations_LH and
Planetary_computer_LH (`_delta_log` `metaData.schemaString`) finds **only `criticality`** in any
reachable table. `gold_emission_events` exists in neither lakehouse, so the other 14 V1 columns
could not be inspected at source.

| V1 column | V1 meaning (from the repo board's queries) | V2 source | Unit conversion / derivation |
|---|---|---|---|
| facility_key | join key, `gold_emission_events.facility_key = dim_facility.facility_key` | `dim_facility.facility_sk` (long) on dims and facts. Plumes carry **`gold_plume_catalog.attributed_facility_id`** (string business key), not an sk | Plume → facility_sk requires a join on `dim_facility.facility_id` |
| equipment_key | join key, events → `dim_equipment.equipment_key` | `dim_equipment.equipment_sk` on dims and facts. **Plume → equipment: NO EQUIVALENT** (V2 plumes carry no equipment attribution) | — |
| emission_kg_per_hour | per-event emission rate | `gold_plume_catalog.emission_rate_kg_h` (plus `_p5/_p50/_p95_kg_h`) | Same unit. V2 is per plume, with an uncertainty band |
| emission_severity | categorical (`SUPER_EMITTER`, `HIGH`, …) | **NO EQUIVALENT** | `gold_plume_catalog.confidence` is a confidence label, not severity. Defining severity bands is a design decision |
| financial_impact_usd | per-event dollar impact | **NO single equivalent**. `fact_financial_impact.total_impact_usd_p5 / _p50 / _p95` (per plume × facility), plus `lost_gas_value_usd`, `violation_fine_usd`, `social_cost_usd` | V2 is a band, not a point. Which percentile represents "impact" is a decision |
| compliance_risk_score | per-event score, thresholds 7 and 9 | **NO EQUIVALENT** | V2's `fact_facility_daily_snapshot.risk_score` is per facility-day on 0–100, a composite (compliance is 20 of the 100 points, in `risk_pts_compliance`). Different grain, scale and meaning |
| work_order_generated | per-event ticket link | **NO EQUIVALENT** | V2 work orders are not raised from plumes; `fact_work_order.source` records the actual source |
| total_methane_loss_mcf | per-event gas lost, mcf | **NO direct column**. Nearest: `fact_financial_impact.attributed_emissions_kg` | **Unit conversion required**: kg CH4 → mcf needs a density constant, which V2 does not store and has to be chosen |
| equipment_age_years | per-asset age | **NO stored column**. Derive from `dim_equipment.install_date` | **Derivation**: needs an as-of date. The estate is frozen at TOPOLOGY_AS_OF (2026-09-15), so `now()` would inflate ages |
| criticality | `dim_equipment.criticality`, filtered `== "CRITICAL"` | `dim_equipment.criticality` (also `fact_pm_schedule.criticality`) | Values: V2 `Critical 486 / High 791 / Medium 903 / Low 955`; V1 `Critical 1598 / High 2652 / Medium 3131 / Low 3194`. Title-case on both, so the V1 board's `"CRITICAL"` filter matches nothing in either |
| well_count | facility attribute | **NO EQUIVALENT** | — |
| daily_gas_mcf | facility attribute (single value) | **NO dim attribute**. `fact_production_daily.gross_gas_mcf` is per facility × day | **Derivation**: a facility-level number needs a rule (latest day, mean, …) |
| production_start_date | facility attribute | **NO EQUIVALENT** | `dim_facility.commission_date` exists; whether commissioning means production start is a decision, so it is not substituted here |
| operator | facility attribute | `dim_facility.operator_name` (and `operator_sk`) | — |
| active_status | facility attribute (label) | `dim_facility.active_flag` (boolean) | Boolean → label mapping |

**NO EQUIVALENT (design decisions):** emission_severity, compliance_risk_score,
work_order_generated, well_count, production_start_date, and plume → equipment for equipment_key.
**Not a single column:** financial_impact_usd (needs a percentile choice), total_methane_loss_mcf
(needs a unit conversion), equipment_age_years and daily_gas_mcf (need a derivation rule).

---

## 5. Coordinate columns (dim_facility)

| Version | Coordinate columns (Delta schema) |
|---|---|
| **V2** greensky_v2_lakehouse `dim_facility` | `facility_lat:double`, `facility_lon:double` |
| V1 Operations_LH `gold/dim_facility` | `latitude:double`, `longitude:double`, `facility_lat:double`, `facility_lon:double` |

**V2 has exactly one pair, and no second pair survives from V1.** `latitude` / `longitude` are
gone. The deployed board's *Facility Map* tile projects `latitude, longitude, basin,
anchor_site` from dim_facility. None of those four exist in V2 (`basin` is replaced by
`sub_basin`; `anchor_site` has no V2 column).

---

## 6. Freshness

| Source | Column a freshness indicator would read | Current value | Notes |
|---|---|---|---|
| Plumes | `gold_plume_catalog.detection_date` | 2026-09-15 20:17:52.5 | Satellite overpass time. No load/as-of column on the table |
| SCADA telemetry | `scada_telemetry.reading_ts` (event); `ingest_ts` (load) | 2026-09-14 23:55:00; ingest 2026-10-12 07:25:10 | Rollups: `scada_telemetry_hourly.bucket_ts` 2026-09-14 23:00, `_daily.bucket_ts` 2026-09-14 |
| CH4 sensor telemetry | `sensor_telemetry.reading_ts` (event); `ingest_ts` (load) | 2026-09-14 23:00:00; ingest 2026-10-12 07:08:14 | |
| Operations facts | `fact_facility_daily_snapshot.snapshot_date` | 2026-09-14 | The only explicit as-of column among the operations facts is `fact_pm_schedule.as_of_ts` = 2026-09-15 00:00 |

Only the two telemetry tables carry a load timestamp (`ingest_ts`). For every other source, event
time is the only freshness signal.

---

## 7. Risk score

Computed in **`03e_gen_financial_snapshots`** (repo HEAD `6543756`;
`Methane Emissions/Accelerator/Planetary Computer/Varon et al Approach/notebooks/03_enterprise/03e_gen_financial_snapshots.Notebook/notebook-content.py`).
`risk_points()` is at **lines 764–773**, the per-facility condition mean at **line 857**, and
the call at **lines 860–861**. Command: `grep -n "RISK_W\|risk_score\|def risk\|cond = \|pts = risk_points" …`
then `sed -n` on those ranges.

Weights and saturation levels, lines 347–356 (verbatim, comment lines 349–350 omitted):
```python
RISK_WEIGHTS = {"condition": 30.0, "emissions": 25.0, "compliance": 20.0, "maintenance": 15.0,
                "open_work": 10.0}
RISK_CONDITION_AT_MAX = 0.19        # condition index p95 0.187 (range 0.10-0.24)
RISK_PLUMES_AT_MAX = 2.0            # p99; a detection plus a repeat, 03c's repetition rule
RISK_OPEN_CASE_WEIGHT = 0.5
RISK_OVERDUE_SHARE_AT_MAX = 0.20    # p95 0.200; about twice the estate's ~11%
RISK_LDAR_AT_MAX = 2.0              # p95; non-zero on 20% of facility-days
RISK_TICKETS_AT_MAX = 2.0           # p95; non-zero on 39% of facility-days
```
`risk_points()`, lines 764–773 (verbatim):
```python
def risk_points(cond, plumes_30d, violations_30d, open_state_cases, overdue_share, ldar_out,
                tickets):
    """{component: points}; the points sum to the risk score."""
    s = {"condition": min(max(cond, 0.0) / RISK_CONDITION_AT_MAX, 1.0),
         "emissions": min(plumes_30d / RISK_PLUMES_AT_MAX, 1.0),
         "compliance": min(violations_30d + RISK_OPEN_CASE_WEIGHT * open_state_cases, 1.0),
         "maintenance": 0.5 * min(overdue_share / RISK_OVERDUE_SHARE_AT_MAX, 1.0)
                        + 0.5 * min(ldar_out / RISK_LDAR_AT_MAX, 1.0),
         "open_work": min(tickets / RISK_TICKETS_AT_MAX, 1.0)}
    return {c: round(RISK_WEIGHTS[c] * s[c], 2) for c in RISK_COMPONENTS}
```
**The condition component, traced in code:**
- Line 857: `cond = cond_sum[i, j] / n_assets[i, j] if n_assets[i, j] else 0.0`. This is the
  mean, over the facility's installed assets at the day's end, of
  `condition(si, a, h, history_start)`, accumulated at lines 810–812.
- 03e lines 491–494: `condition()` = `equipment_condition_index(age, life,
  days_since_service(...), freq, reliability, leak_propensity)`, with `age = max((t -
  install).days / 365.25, 0)`.
- `equipment_condition_index` is defined in `01_topology_config` at **line 1408**, reached by
  03e's `%run 01_topology_config` (03e line 154):
  ```python
  wear = np.clip(age_years / max(life_years, 1), 0, 1.4)
  service_gap = np.clip(days_since_service / (insp_days * 2.0), 0, 1.5)
  cond = (0.55 * wear + 0.30 * service_gap) * reliability_index * (0.6 + 0.4 * leak_propensity)
  return float(np.clip(cond, 0, 1))
  ```
- **Contribution:** `risk_pts_condition = round(30 × min(cond / 0.19, 1), 2)`, so 0–30 points,
  saturating at a mean condition index of 0.19. The row also stores
  `equipment_health_score = round(1 − cond, 6)` (line 875) and
  `risk_score = round(sum(pts.values()), 2)` (line 876).
- Cross-check against the data, from the sample rows: GS-0002's `equipment_health_score`
  0.863491 → cond 0.136509 → 30 × 0.136509 / 0.19 = 21.55, equal to its stored
  `risk_pts_condition` **21.55**. GS-0003's 0.884112 → cond 0.115888 → 18.30 = stored **18.3**.

---

## Methods appendix

Helper scripts (session scratchpad, not in the repo) and what each one calls. All are read-only.
- `fab.py`: Fabric REST `GET` / `POST getDefinition` with an Azure CLI token.
- `kql.py`: `POST {cluster}/v1/rest/mgmt` for `.`-commands and `/v1/rest/query` otherwise, body
  `{"db": …, "csl": …}`.
- `sqlq.py`: `pyodbc` with `ODBC Driver 18 for SQL Server`, server from
  `GET /v1/workspaces/{ws}/lakehouses/{id}` → `properties.sqlEndpointProperties.connectionString`,
  `ApplicationIntent=ReadOnly`, Entra access token (`attrs_before={1256: token}`).
- `onelake_tables.py`: `GET https://onelake.dfs.fabric.microsoft.com/{ws}?resource=filesystem&directory=…`
  to list; `GET` of `_delta_log/*.json` and `*.checkpoint.parquet` to replay adds and removes; for
  each active file, `HEAD` plus HTTP `Range` reads through `pyarrow.parquet.ParquetFile` (footer
  `num_rows`, row-group `statistics.max`). Samples come from `iter_batches(batch_size=3)` on the
  first active file.
- `find_cols.py`: the same Delta-log reader over every table in Operations_LH and
  Planetary_computer_LH, matching column names.

Queries run, beyond those quoted in the sections:
```
.show databases
.show database schema | project DatabaseName, TableName, ColumnName, ColumnType | take 8
.show tables details | project TableName, TotalRowCount, TotalExtentSize, HotRowCount
.show external tables | project TableName, TableType, Folder, ConnectionStrings, Properties
.show materialized-views | project Name, SourceTable, IsEnabled, IsHealthy
.show functions | project Name, Parameters, Folder
.show database <db> policy mirroring
external_table('<each of 29>') | count
external_table('<each of 29>') | getschema | project ColumnName, ColumnType
external_table('dim_equipment') | summarize n=count() by criticality
SELECT COUNT(*) AS n, COUNT(attributed_facility_id) AS n_attributed, MIN(detection_date), MAX(detection_date) FROM dbo.gold_plume_catalog
SELECT COUNT(*), MAX(detect_ts) FROM dbo.gold_multi_gas_signatures
SELECT COUNT(*), MAX(last_detection) FROM dbo.gold_emission_sites
SELECT COUNT(*) FROM dbo.ref_facilities
SELECT TOP 40 * FROM sys.dm_db_external_tables_log_status
```
Fabric REST calls: `GET /v1/workspaces`; `GET /v1/workspaces/{ws}/eventhouses | /kqlDatabases |
/lakehouses | /items?type=KQLDashboard`; `GET /v1/workspaces/{ws}/items/{kqlDb}/shortcuts`;
`POST /v1/workspaces/{ws}/items/dc4fd2af-…/getDefinition` (deployed board);
`GET /v1/workspaces/060ba34b-…` and `GET …/items/c0d0aefe-…` (both 404).

Two collection runs had transient network failures (`RemoteDisconnected`, DNS `getaddrinfo
failed`) on six tables. Those six were re-read on their own, and every figure here comes from a
successful read.
