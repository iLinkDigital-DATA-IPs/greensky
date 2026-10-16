# Green Sky: RTI investigation, round 2 (read-only), 2026-10-12

Every reading in this report was taken live on 2026-10-12, between 12:15 and 12:50 UTC, against workspace
*Green Sky - Dev* (`640876ea-6158-4ffd-8598-5eb210e088a0`). The sources were:

- the Kusto REST API on Operations_KQL (`85d85e9c-2342-460b-8085-5883c8266286`, cluster
  `https://trd-9k8b5vackx1a66j3jm.z3.kusto.fabric.microsoft.com`)
- OneLake DFS reads (directory listings, `_delta_log` JSON and checkpoints, Parquet footers)
- the Fabric REST API (GETs, plus one `getDefinition` POST)
- the repo working tree, for A5 and C6 only

Nothing in Fabric was created, changed, refreshed or run. Every command is a read; the scripts are listed
in the appendix. No figure below is carried over from the 2026-10-12 round-1 report: where a number
repeats, it was read again.

## Summary

| Question | Answer |
|---|---|
| **A. 109 vs 49** | **The glob hypothesis is refuted.** The directory holds 40 Parquet files with **334** rows, not 109. KQL replays the Delta log: its artifact list is exactly the 8 active files, and a V1 table with 5,600 tombstoned rows counts correctly. **109 is Delta version 0's row count.** It is held in this table's **query-acceleration catalog**, which has been stuck at `TableVersion 0` since 2026-09-23 because `"Failed to auto-refresh external table schema"`. Delta v1 changed `plume_id` and `scene_id` from long to string. Only the bare `count` operator returns 109; `summarize count()` returns 49. The same stale schema makes `plume_id` and `scene_id` **null in all 49 rows** through KQL. Of the 29 external tables, this is the only one affected. |
| A5. Blast radius | All four proposed serving tables are written with `mode("overwrite")`. `dim_facility` and `gold_plume_catalog` are whole-table overwrites with `overwriteSchema` on every run. `gold_plume_catalog`'s schema has **changed 7 times**: 04 drops the 11 attribution columns and 05 adds them back. That is the same kind of event (a schema change) that broke acceleration here. `fact_facility_daily_snapshot` and `scada_telemetry_hourly` are partition-scoped `replaceWhere` writes and their schemas have never changed. |
| A6. VACUUM | **Never**, in any retained V2 commit (41 tables). Tombstoned files back to 2026-08-17 are still on disk. |
| **B. Deployed board** | 27 tiles on 3 pages; 24 tile queries plus 1 parameter query. **23 of the 24 tile queries read only V1 `Operations_LH`.** T08 mixes V1 with V2, and only T13 reads V2 alone. Every query runs without error. **Many fail §13**, among them: open work orders `÷ 100` (shows 290 where 29,023 are open), compliance violations `÷ 500` (385 where 192,698), financial impact `÷ 1000 ÷ 1e6` labelled "M" (shows "5.13M" where the sum is $5.13 B), a fabricated equipment health of `0.7`, a `9999`-day sentinel shown as data, and a "no-data" row shown as `0.0 kg/hr`. Also: two KPI cards both titled "Open Work Orders" show 290 and 29,023. The `priority == "Critical"` filter matches nothing (the values are P1–P3). The headline card reads "0 t, −100 %" today because V1 data ends on 2026-07-09. |
| B4 | One parameter, `selected_facility`, used by the 12 Facility Operations tiles and none of the Executive Overview tiles. There is **no time parameter at all**. Every `_facility_sk` filter is a hard-coded empty `dynamic([])`, so it is inert. |
| B5 | One external host: `image2url.com`, for the banner image on two pages. |
| B6 | Neither `hourly_trends()` nor `facility_metrics` is referenced. `.show functions` returns 0, and no tile breaks for that reason. |
| **C. Coordinates** | The sampled values are `attributed_facility_*`, and they equal `dim_facility` exactly for all 42 attributed plumes. Stored distances match haversine to 7.8e-13 km. The two southern facilities are **2 of the 7 deliberate `outside`-band facilities**: 4.7 % of the estate, 0.07–0.24° beyond the BBOX edge, against a configured cap of 0.30°. **This is not the V1 defect**, which had an anchor below the BBOX carrying 40 % of the estate. All 143 inside/perimeter facilities lie within their anchor's configured radii. But **5 of the 42 attributions go to `outside` facilities**, each with `attribution_probability = 1.0` and `facilities_in_range = 1`. **Facility names are decorative by design:** the town is drawn at random, independently of location (`01a`, line 125; config lines 64–67). |

---

## A. The 109 vs 49 discrepancy

### A1. `greensky_v2_lakehouse / Tables/dbo/gold_multi_gas_signatures` via OneLake

Command: `python delta_audit.py 5d5c8002-789e-4319-81d1-a60f08a77996 Tables/dbo/gold_multi_gas_signatures a1_mgs.json`.
It lists the table directory recursively (DFS `resource=filesystem&recursive=true`), replays `_delta_log`
(5 commit JSONs, no checkpoint), and reads every Parquet footer's `num_rows` with HTTP range reads.

**(a) Active snapshot (Delta version 4):** 8 files, with footer `num_rows` summing to **49**.

**(b) Every `.parquet` file physically present:** 40 files.

| # | file | added in | state | num_rows | last modified (UTC) |
|---|---|---|---|---|---|
| 1 | `part-00000-c1cbc175-2d72-4efd-8f7c-90354110813d-c000.snappy.parquet` | v0 | tombstoned | 18 | 2026-09-01 04:41:59 |
| 2 | `part-00001-0e29bdaa-412c-45d4-8a4e-d1d4d8924070-c000.snappy.parquet` | v0 | tombstoned | 13 | 2026-09-01 04:41:59 |
| 3 | `part-00002-260b1cfa-6756-48fc-b9a8-842da273a041-c000.snappy.parquet` | v0 | tombstoned | 13 | 2026-09-01 04:41:59 |
| 4 | `part-00003-b9d03595-4052-463d-a330-7795cee0c9cb-c000.snappy.parquet` | v0 | tombstoned | 13 | 2026-09-01 04:41:59 |
| 5 | `part-00004-8a8f114a-8177-47c3-8c5e-9846b583b43b-c000.snappy.parquet` | v0 | tombstoned | 13 | 2026-09-01 04:41:59 |
| 6 | `part-00005-a7ca290e-64f5-4837-a2a6-dbf666cb76bc-c000.snappy.parquet` | v0 | tombstoned | 13 | 2026-09-01 04:41:59 |
| 7 | `part-00006-d8625736-8b3f-4b9e-9bf2-d381dba2b459-c000.snappy.parquet` | v0 | tombstoned | 13 | 2026-09-01 04:41:59 |
| 8 | `part-00007-061c5073-73de-477c-a0a7-0a844dc86031-c000.snappy.parquet` | v0 | tombstoned | 13 | 2026-09-01 04:41:59 |
| 9 | `part-00000-e65c2c8f-d0d4-4233-b542-d00411666bb8-c000.snappy.parquet` | v1 | tombstoned | 12 | 2026-09-25 13:35:08 |
| 10 | `part-00001-df8b0770-46c9-44e9-a366-4014f9d47054-c000.snappy.parquet` | v1 | tombstoned | 9 | 2026-09-25 13:35:08 |
| 11 | `part-00002-e95b9b08-cf08-4781-8b74-87343c964898-c000.snappy.parquet` | v1 | tombstoned | 9 | 2026-09-25 13:35:08 |
| 12 | `part-00003-586b3572-86f6-4372-b6ba-4d2a4567266b-c000.snappy.parquet` | v1 | tombstoned | 9 | 2026-09-25 13:35:08 |
| 13 | `part-00004-87c60068-3665-42ee-90de-e30fcc57c724-c000.snappy.parquet` | v1 | tombstoned | 9 | 2026-09-25 13:35:08 |
| 14 | `part-00005-ca905442-a777-473c-b23f-4a133463cbe7-c000.snappy.parquet` | v1 | tombstoned | 9 | 2026-09-25 13:35:08 |
| 15 | `part-00006-12df2485-0798-4332-bb3a-a40956e54f06-c000.snappy.parquet` | v1 | tombstoned | 9 | 2026-09-25 13:35:08 |
| 16 | `part-00007-a589ff87-b425-47e3-9f47-8c6d1537358d-c000.snappy.parquet` | v1 | tombstoned | 9 | 2026-09-25 13:35:08 |
| 17 | `part-00000-5dc511c8-d3c3-48cd-b86b-652b8ae0f46c-c000.snappy.parquet` | v2 | tombstoned | 10 | 2026-09-28 06:26:56 |
| 18 | `part-00001-c9dbb2d8-ae8e-4d86-9377-b6b8650d2699-c000.snappy.parquet` | v2 | tombstoned | 6 | 2026-09-28 06:26:56 |
| 19 | `part-00002-94decbf7-b76f-4e59-9754-b8ff0d51d87c-c000.snappy.parquet` | v2 | tombstoned | 6 | 2026-09-28 06:26:56 |
| 20 | `part-00003-a21d81d5-6431-4616-9703-61cb9bc25c8a-c000.snappy.parquet` | v2 | tombstoned | 6 | 2026-09-28 06:26:56 |
| 21 | `part-00004-f59fd0c3-d3d9-40b5-bd0c-68c78af3c5de-c000.snappy.parquet` | v2 | tombstoned | 6 | 2026-09-28 06:26:56 |
| 22 | `part-00005-b060609b-6c0c-4011-a3df-4ca7d95c2a0f-c000.snappy.parquet` | v2 | tombstoned | 6 | 2026-09-28 06:26:56 |
| 23 | `part-00006-0e3549a8-5b5d-4b52-9a45-455129c49dc2-c000.snappy.parquet` | v2 | tombstoned | 6 | 2026-09-28 06:26:56 |
| 24 | `part-00007-68843480-a2fe-4f31-a8bb-19c90d59515e-c000.snappy.parquet` | v2 | tombstoned | 6 | 2026-09-28 06:26:56 |
| 25 | `part-00000-0a62339b-0bdf-4fed-983e-6da5719786a3-c000.snappy.parquet` | v3 | tombstoned | 7 | 2026-09-28 09:45:46 |
| 26 | `part-00001-4aaba265-d496-48fe-9af2-129b9c0b62ea-c000.snappy.parquet` | v3 | tombstoned | 6 | 2026-09-28 09:45:46 |
| 27 | `part-00002-09a818cd-1b8b-45c0-9239-a2b207c42d09-c000.snappy.parquet` | v3 | tombstoned | 6 | 2026-09-28 09:45:46 |
| 28 | `part-00003-bdd8750a-7925-4c84-aea2-769cc9a087ae-c000.snappy.parquet` | v3 | tombstoned | 6 | 2026-09-28 09:45:46 |
| 29 | `part-00004-fcf449b8-a8e4-4495-989e-58e7e4b07983-c000.snappy.parquet` | v3 | tombstoned | 6 | 2026-09-28 09:45:46 |
| 30 | `part-00005-4c2bd646-d1d9-4fe8-bb0c-6cff8bb43268-c000.snappy.parquet` | v3 | tombstoned | 6 | 2026-09-28 09:45:46 |
| 31 | `part-00006-2e963bbf-03b7-4920-befe-bfc1a6fdc67f-c000.snappy.parquet` | v3 | tombstoned | 6 | 2026-09-28 09:45:46 |
| 32 | `part-00007-fffd15e1-bedc-4a4a-b7ca-724b396a22b4-c000.snappy.parquet` | v3 | tombstoned | 6 | 2026-09-28 09:45:46 |
| 33 | `part-00000-25fcb86e-c0d6-44e1-9a13-708773ab1bf9-c000.snappy.parquet` | v4 | ACTIVE | 7 | 2026-10-12 06:34:17 |
| 34 | `part-00001-08c0ca53-a0ca-4a18-8027-554ff21b4f65-c000.snappy.parquet` | v4 | ACTIVE | 6 | 2026-10-12 06:34:17 |
| 35 | `part-00002-54a3f9f9-f6a6-4d92-a02b-c5bead87f9bf-c000.snappy.parquet` | v4 | ACTIVE | 6 | 2026-10-12 06:34:17 |
| 36 | `part-00003-33fce9d4-b7c2-4c3c-8c19-0a393d04b23b-c000.snappy.parquet` | v4 | ACTIVE | 6 | 2026-10-12 06:34:17 |
| 37 | `part-00004-8336f557-9b89-463f-94ed-882f0e60f24a-c000.snappy.parquet` | v4 | ACTIVE | 6 | 2026-10-12 06:34:17 |
| 38 | `part-00005-df1f6e6c-7f4d-4aec-97a5-1535a29f9220-c000.snappy.parquet` | v4 | ACTIVE | 6 | 2026-10-12 06:34:17 |
| 39 | `part-00006-1c7e7628-7ce0-4379-9fa8-a0ab233d5f75-c000.snappy.parquet` | v4 | ACTIVE | 6 | 2026-10-12 06:34:17 |
| 40 | `part-00007-44bc444e-ec36-4db0-8111-b9656bc0324b-c000.snappy.parquet` | v4 | ACTIVE | 6 | 2026-10-12 06:34:17 |

Verbatim totals:
```
active files in snapshot (log): 8 | physically present & active: 8 | active footer rows: 49
all physical parquet rows: 334 | tombstoned files: 32 rows 285
active-in-log but not physically present: []
other non-parquet, non-log files: ['_metadata/table.json.gz']
```

**Does (b) sum to 109? No. It sums to 334** (109 + 75 + 52 + 49 + 49, one generation of 8 files per
version). The only place 109 appears is **version 0 alone**.

**(c) Commit history** (`commitInfo` of each `_delta_log/0000000000000000000N.json`):

| version | commit time (UTC) | commitInfo.operation | operationParameters.mode | add | remove | operationMetrics rows |
|---|---|---|---|---|---|---|
| 0 | 2026-09-01 04:41:59 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 0 | 109 |
| 1 | 2026-09-25 13:35:08 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 75 |
| 2 | 2026-09-28 06:26:56 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 52 |
| 3 | 2026-09-28 09:45:47 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 49 |
| 4 | 2026-10-12 06:34:18 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 49 |

`operationParameters.mode` is absent on every commit. `saveAsTable` with `mode("overwrite")`
is logged as `CREATE OR REPLACE TABLE AS SELECT`, which carries no `mode` key.

Schema per version, from `metaData.schemaString`: v0 has `plume_id: long` and `scene_id: long`.
**v1 changes both to `string`** (`v1 diff vs prev: removed [('plume_id', 'long'), ('scene_id', 'long')]
added [('plume_id', 'string'), ('scene_id', 'string')]`). There are no other schema changes in v2–v4.

### A2. Through KQL

All queries ran on Operations_KQL through `kql.py` / `kqlfull.py`.

```
external_table('gold_multi_gas_signatures')
| summarize rows=count(), distinct_plumes=dcount(plume_id), min_ts=min(detect_ts), max_ts=max(detect_ts)
→ rows | distinct_plumes | min_ts | max_ts
  49   | 0               | 2026-08-16T19:45:22Z | 2026-09-15T20:17:52.5Z
```

**49, not 109**, and `distinct_plumes = 0`. The surplus is not in the scanned rows. It appears only through
the bare `count` operator:

```
external_table('gold_multi_gas_signatures') | count                  → 109
external_table('gold_multi_gas_signatures') | where 1==1 | count     → 109
external_table('gold_multi_gas_signatures') | summarize via_summarize=count()      → 49
external_table('gold_multi_gas_signatures') | where isnotnull(detect_ts) | count   → 49
```

`.show queries` confirms the round-1 figure came from the bare operator: `2026-10-12T09:15:16.88381Z |
external_table('gold_multi_gas_signatures') | count | Completed`.

**Duplicates.** The requested test groups by `plume_id`, which is null in every row (see below), so it returns one
group:
```
external_table('gold_multi_gas_signatures') | summarize n=count() by plume_id | summarize plumes=count() by n
→ n=49 | plumes=1
external_table('gold_multi_gas_signatures') | summarize null_scene=countif(isnull(scene_id)), null_plume=countif(isnull(plume_id)), n=count()
→ 49 | 49 | 49
```
Grouping on a key that is populated shows **no duplicates**:
```
external_table('gold_multi_gas_signatures') | summarize n=count() by source_lat, source_lon, detect_ts | summarize groups=count() by n
→ n=1 | groups=49
```
So there are no extra copies to set against the 4 tombstoned generations in A1. The 4 tombstoned
generations hold 285 rows (109 + 75 + 52 + 49), and 109 − 49 = 60 matches no combination of them.

**Why `plume_id` and `scene_id` are null.** The external table's schema is frozen at v0:
```
.show external table gold_multi_gas_signatures schema as csl
→ plume_id:long,source_lat:real,source_lon:real,detect_ts:datetime,scene_id:long,plume_area_km2:real,...
```
The Delta files have been `string` since v1 (`PL-…` content IDs). KQL reads the active files with
the stale `long` type, and both columns come back null. The board's T08 joins on this `plume_id` (B2).

**Where the 109 comes from: the query-acceleration state.** This is the verbatim
`QueryAccelerationState` cell of `.show external table gold_multi_gas_signatures details`:
```
{
  "HotSize": 14478,
  "CompletionPercentage": 100.0,
  "PendingDataFilesSize": 0,
  "PendingDataFilesCount": 0,
  "PendingArtifactSize": 0,
  "PendingArtifactCount": 0,
  "IsHealthy": false,
  "NotHealthyReason": "UnexpectedError: Failed to auto-refresh external table schema for external table 'gold_multi_gas_signatures' in database '85d85e9c-2342-460b-8085-5883c8266286' during query acceleration catalog refresh",
  "LastUpdatedDateTime": "2026-09-23 11:46:18.7826386",
  "Latency": "19.00:36:48.2737449",
  "LastIngestionDateTime": "2026-09-01T05:59:23.622381Z",
  "RowCount": 109,
  "TableVersion": 0,
  "AcceleratedRowCount": 109
}
```
The policy (`.show external table gold_multi_gas_signatures policy query_acceleration`) is
`{"IsEnabled": true, "Hot": "36500.00:00:00", "MaxAge": "00:05:00", ...}`.

**Reading.** `RowCount 109 / TableVersion 0` is the Delta v0 snapshot. The catalog last refreshed
on 2026-09-23. v1, written on 2026-09-25, changed two column types, and the automatic schema refresh has
failed ever since. The `count` operator returns the catalog's `RowCount`; anything that reads columns
scans the current files. I infer that `count` is answered from the catalog metadata because three things line up:
1. the two count forms disagree;
2. `count` equals `RowCount` and the v0 row count exactly;
3. the scan forms equal the v4 snapshot.

I did not read Kusto internals to confirm this.

### A3. Does the shortcut say which read mode it uses?

`GET /v1/workspaces/640876ea-…/items/85d85e9c-2342-460b-8085-5883c8266286/shortcuts`: HTTP 200, 29 shortcuts,
no `continuationToken`. The object for this table, verbatim:
```json
{
 "name": "gold_multi_gas_signatures",
 "path": "/Tables",
 "target": {
  "type": "OneLake",
  "oneLake": {
   "workspaceId": "640876ea-6158-4ffd-8598-5eb210e088a0",
   "itemId": "5d5c8002-789e-4319-81d1-a60f08a77996",
   "path": "Tables/dbo/gold_multi_gas_signatures"
  }
 }
}
```
**The shortcut object says nothing about read mode.** It is a path pointer only.

`.show external table gold_multi_gas_signatures`, the full row verbatim:
```
TableName: gold_multi_gas_signatures
TableType: Delta
Folder:
DocString:
Properties: {
  "Format": "Parquet",
  "Compressed": false,
  "CompressionType": null,
  "FileExtension": null,
  "IncludeHeaders": "None",
  "Encoding": null,
  "NamePrefix": null,
  "Shortcut": true,
  "AutoUpdateSchema": true
}
ConnectionStrings: ['https://onelakewestus.pbidedicated.windows.net/640876ea-6158-4ffd-8598-5eb210e088a0/85d85e9c-2342-460b-8085-5883c8266286/Tables/gold_multi_gas_signatures;impersonate']
Partitions: []
PathFormat:
```
**`TableType: Delta`** is the read-mode statement: Kusto treats the path as a Delta table, not as a
folder of Parquet. `AutoUpdateSchema: true` is the setting whose refresh is failing.

The artifact list is the direct proof. `.show external table gold_multi_gas_signatures artifacts` returns
**exactly 8 URIs**, all `part-0000[0-7]-…` files of **version 4**: `25fcb86e`, `08c0ca53`, `54a3f9f9`,
`33fce9d4`, `8336f557`, `df1f6e6c`, `1c7e7628` and `44bc444e`. All 8 match the ACTIVE rows in A1(b), and
none of the 32 tombstoned files is listed.

### A4. Cross-check on other tables

**The two Planetary_computer_LH shortcuts** (`f2c3a17e-d2bb-4686-a19c-42d729908353`), checked with the same `delta_audit.py`:

| table | KQL `count` | KQL `summarize count()` | Delta-log active-file rows | all physical Parquet rows | tombstoned files |
|---|---|---|---|---|---|
| ch4_plume_catalog (`Tables/silver/ch4_plume_catalog`) | 291 | 291 | **291** (50 files) | 291 (58 files) | 0 removes in the log. 8 files were never in the log, all `num_rows = 0`, written 2026-07-02 … 07-11 |
| weather (`Tables/bronze/weather`) | 84 | 84 | **84** (13 files) | 84 (13 files) | 0 |

ch4_plume_catalog history (11 commits, checkpoint at 10):

| version | commit time (UTC) | commitInfo.operation | operationParameters.mode | add | remove | operationMetrics rows |
|---|---|---|---|---|---|---|
| 0 | 2026-06-30 05:39:13 | CREATE TABLE AS SELECT | (none) | 8 | 0 | 89 |
| 1 | 2026-06-30 06:54:21 | WRITE | Append | 4 | 0 | 20 |
| 2 | 2026-07-01 00:40:57 | WRITE | Append | 4 | 0 | 12 |
| 3 | 2026-07-02 00:41:05 | WRITE | Append | 1 | 0 | 1 |
| 4 | 2026-07-05 00:43:43 | WRITE | Append | 5 | 0 | 39 |
| 5 | 2026-07-06 00:43:22 | WRITE | Append | 10 | 0 | 27 |
| 6 | 2026-07-07 00:43:45 | WRITE | Append | 7 | 0 | 45 |
| 7 | 2026-07-08 00:43:35 | WRITE | Append | 2 | 0 | 15 |
| 8 | 2026-07-09 00:44:35 | WRITE | Append | 2 | 0 | 13 |
| 9 | 2026-07-10 00:44:21 | WRITE | Append | 2 | 0 | 8 |
| 10 | 2026-07-11 00:44:24 | WRITE | Append | 5 | 0 | 22 |

weather history (14 commits, checkpoint at 10):

| version | commit time (UTC) | commitInfo.operation | operationParameters.mode | add | remove | operationMetrics rows |
|---|---|---|---|---|---|---|
| 0 | 2026-06-30 05:34:29 | CREATE TABLE | (none) | 0 | 0 | — |
| 1 | 2026-06-30 05:34:43 | MERGE | (none) | 1 | 0 | 52 |
| 2 | 2026-06-30 06:47:19 | MERGE | (none) | 1 | 0 | 10 |
| 3 | 2026-07-01 00:36:35 | MERGE | (none) | 1 | 0 | 2 |
| 4 | 2026-07-02 00:36:33 | MERGE | (none) | 1 | 0 | 2 |
| 5 | 2026-07-03 00:36:48 | MERGE | (none) | 1 | 0 | 2 |
| 6 | 2026-07-04 00:35:52 | MERGE | (none) | 1 | 0 | 2 |
| 7 | 2026-07-05 00:38:20 | MERGE | (none) | 1 | 0 | 2 |
| 8 | 2026-07-06 00:37:54 | MERGE | (none) | 1 | 0 | 2 |
| 9 | 2026-07-07 00:37:44 | MERGE | (none) | 1 | 0 | 2 |
| 10 | 2026-07-08 00:37:47 | MERGE | (none) | 1 | 0 | 2 |
| 11 | 2026-07-09 00:38:23 | MERGE | (none) | 1 | 0 | 2 |
| 12 | 2026-07-10 00:38:19 | MERGE | (none) | 1 | 0 | 2 |
| 13 | 2026-07-11 00:38:24 | MERGE | (none) | 1 | 0 | 2 |

**These two tables cannot tell a glob read from a log replay:** they have never been overwritten (append and
MERGE only, 0 removes), so active rows equal all rows. My first pass on ch4_plume_catalog printed
`240` because network errors (`getaddrinfo failed`) dropped footers. I added retries, and the re-run reads all 58
footers with 0 errors.

**A table that can tell them apart: V1 `Operations_LH / Tables/gold/dim_facility`** (`f689a941-…`), which
Operations_KQL also shortcuts. It is overwritten daily, so it is full of tombstones:
```
active files in snapshot (log): 8 | physically present & active: 8 | active footer rows: 350
all physical parquet rows: 5950 | tombstoned files: 114 rows 5600
```
KQL returns **350** both ways. A glob would return 5,950. **KQL replays the Delta log.**

**All 29 external tables, both count forms plus acceleration state** (`python a4_sweep.py`):

| table | `count` | `summarize count()` | accel TableVersion | accel RowCount | IsHealthy | LastUpdatedDateTime |
|---|---|---|---|---|---|---|
| bridge_plume_equipment | 3795 | 3795 | 18 | 3795 | True | 2026-10-12 12:34:53 |
| bridge_plume_facility_attribution | 2879 | 2879 | 18 | 2879 | True | 2026-10-12 12:34:56 |
| ch4_plume_catalog | 291 | 291 | 10 | 291 | True | 2026-10-12 12:34:51 |
| dim_contractor | 6 | 6 | 7 | 6 | True | 2026-10-12 12:34:51 |
| dim_date | 417 | 417 | 7 | 417 | True | 2026-10-12 12:34:53 |
| dim_equipment | 10575 | 10575 | 7 | 10575 | True | 2026-10-12 12:34:56 |
| dim_equipment_type | 8 | 8 | 7 | 8 | True | 2026-10-12 12:34:51 |
| dim_facility | 350 | 350 | 20 | 350 | True | 2026-10-12 12:34:56 |
| dim_manufacturer | 8 | 8 | 7 | 8 | True | 2026-10-12 12:37:26 |
| dim_operator | 12 | 12 | 7 | 12 | True | 2026-10-12 12:37:28 |
| dim_regulation | 3 | 3 | 7 | 3 | True | 2026-10-12 12:37:31 |
| dim_sensor | 1400 | 1400 | 7 | 1400 | True | 2026-10-12 12:37:31 |
| dim_team | 8 | 8 | 7 | 8 | True | 2026-10-12 12:37:26 |
| equipment_geo | 10575 | 10575 | 20 | 10575 | True | 2026-10-12 12:37:28 |
| facility_kpi_snapshot_6h | 597800 | 597800 | 39 | 597800 | True | 2026-10-12 12:37:26 |
| fact_compliance_event | 198368 | 198368 | 18 | 198368 | True | 2026-10-12 12:37:28 |
| fact_facility_daily_snapshot | 2506 | 2506 | 39 | 2506 | True | 2026-10-12 12:37:26 |
| fact_financial_impact | 257459 | 257459 | 18 | 257459 | True | 2026-10-12 12:37:31 |
| fact_inspection | 21697 | 21697 | 7 | 21697 | True | 2026-10-12 12:40:03 |
| fact_ldar_survey | 1626 | 1626 | 7 | 1626 | True | 2026-10-12 12:40:03 |
| fact_maintenance | 21755 | 21755 | 7 | 21755 | True | 2026-10-12 12:40:03 |
| fact_plume_detection | 2360 | 2360 | 19 | 2360 | True | 2026-10-12 12:40:06 |
| fact_production_daily | 145950 | 145950 | 18 | 145950 | True | 2026-10-12 12:40:01 |
| fact_work_order | 192698 | 192698 | 18 | 192698 | True | 2026-10-12 12:40:03 |
| **gold_multi_gas_signatures** | **109** | **49** | **0** | **109** | **False** | **2026-09-23 11:46:18** |
| plume_detection_hot | 2360 | 2360 | 29 | 2360 | True | 2026-10-12 12:40:03 |
| sensor_telemetry | 3586800 | 3586800 | 29 | 3586800 | True | 2026-10-12 12:42:40 |
| weather | 84 | 84 | 13 | 84 | True | 2026-10-12 12:42:36 |
| workorder_event | 412521 | 412521 | 17 | 412521 | True | 2026-10-12 12:42:36 |

**Only one table is affected.** In the other 28, both count forms agree and the acceleration state is healthy and refreshed today.
`.show external tables operations query_acceleration statistics` returns the same picture: 28 rows with
`NotHealthyReason: null`, and one with the auto-refresh failure.

### A5. Blast radius: every write in `notebooks/`

Command: `grep -rn -E '\.mode\(|\.save\(|saveAsTable\(|replaceWhere|partitionBy|insertInto|\.merge\(|VACUUM|overwriteSchema'`
over `Methane Emissions/Accelerator/Planetary Computer/Varon et al Approach/notebooks/**/notebook-content.py`.
I then read each write block (`sed -n`) to resolve the table constants and the branch conditions. `.save(` and
`insertInto` have **0 hits**: there are no path-based writes. Excluded from the table:
- `.merge(` hits, which are all pandas `DataFrame.merge`
- `Window.partitionBy` hits, which are window specs, not writes
- `replaceWhere` matches inside comments

| notebook | line(s) | target table | write mode | partitioned by |
|---|---|---|---|---|
| 00_prereqs/00_preflight_tests | 220 | test_write_check | overwrite | — |
| 01_topology/01a_build_facility_topology | 549–551 | **dim_facility** | **overwrite + overwriteSchema, whole table, every run** | — |
| 01_topology/01a_build_facility_topology | 562–564 | ref_facilities | overwrite + overwriteSchema | — |
| 01_topology/01b_build_asset_topology | 492–494 | dim_equipment | overwrite + overwriteSchema | — |
| 01_topology/01b_build_asset_topology | 498–500 | dim_sensor | overwrite + overwriteSchema | — |
| 01_topology/01c_build_area_topology | 536–538 | dim_area | overwrite + overwriteSchema | — |
| 01_topology/01c_build_area_topology | 544–546 | dim_equipment | overwrite + overwriteSchema | — |
| 01_topology/01d_build_scada_tags | 657–659 | dim_scada_tag | overwrite + overwriteSchema | — |
| 02_ingest/02_ingest_tropomi_ch4 | 452–455 / 457–460 | bronze_ch4_pixels | append if the table exists, else overwrite | — |
| 02_ingest/02_ingest_tropomi_no2 | 379–380, 394–395 | bronze_no2_pixels | `mode = "append" if table_exists else "overwrite"` | — |
| 02_ingest/02_ingest_weather_data | 266–270 | bronze_weather | overwrite | — |
| 02_ingest/02_ingest_weather_data | 386–389 | bronze_weather | append (backfill branch) | — |
| 02_ingest/02b_ingest_era5 | 322–326 | bronze_era5_wind | overwrite | — |
| 02_scada/02a_build_asset_state | 734–743 | fact_asset_state | overwrite + `replaceWhere date_sk` range; the first write is overwrite + overwriteSchema | date_sk |
| 02_scada/02b_gen_scada_telemetry | 1781–1789 | scada_telemetry | first write overwrite + overwriteSchema, then per day `replaceWhere date_sk = {sk}` | date_sk |
| 02_scada/02c_rollup_telemetry | 401–411 (called 416, 430) | **scada_telemetry_hourly**, scada_telemetry_daily | **overwrite + `replaceWhere` window predicate if the table exists**, else overwrite + overwriteSchema | date_sk |
| 02_scada/02d_gen_alarms | 1057–1065 (called 1114–1115) | fact_scada_alarm, fact_sensor_status_event | overwrite + `replaceWhere` if the table exists | date_sk |
| 02_scada/02e_gen_ch4_telemetry | 762–766 | sensor_telemetry | overwrite + `replaceWhere` if the table exists | date_sk |
| 03_enterprise/03a_gen_emission_episodes | 922–931 | fact_emission_episode | overwrite + `replaceWhere` if the table exists | date_sk |
| 03_enterprise/03b_gen_work_orders | 1378–1387 (called 1393–1394) | fact_work_order, fact_work_order_event | **backfill → whole-table overwrite + overwriteSchema**; incremental → `replaceWhere` | date_sk |
| 03_enterprise/03c_gen_compliance | 954–955 | dim_regulation | overwrite + overwriteSchema, always | — |
| 03_enterprise/03c_gen_compliance | 965–971 | fact_compliance_event | backfill → whole-table overwrite + overwriteSchema; incremental → `replaceWhere` | date_sk |
| 03_enterprise/03d_gen_maintenance | 1311–1321 (called 1324–1327) | fact_maintenance, fact_inspection, fact_ldar_survey; fact_pm_schedule (`full=True`) | backfill → whole-table overwrite + overwriteSchema, else `replaceWhere`; **fact_pm_schedule is always whole-table** | date_sk |
| 03_enterprise/03e_gen_financial_snapshots | 1286–1297 (called 1300–1302) | fact_production_daily, fact_financial_impact, **fact_facility_daily_snapshot** | **overwrite + `replaceWhere date_sk` window if the table exists, in both run modes**; overwrite + overwriteSchema on first create | date_sk |
| 03_joins/03_join_data | 570–574 | silver_plume_ready_pixels | overwrite + overwriteSchema | — |
| 04_core_logic/04_derive_emissions | 1361–1365 | **gold_plume_catalog** | **overwrite + overwriteSchema, whole table** | — |
| 04_core_logic/04_derive_emissions | 1376–1380 | gold_flagged_large_clusters | overwrite + overwriteSchema | — |
| 04_core_logic/04_derive_emissions | 1404–1408 | gold_rejected_collinear | overwrite + overwriteSchema | — |
| 04_core_logic/04_derive_emissions | 1739–1741 | gold_plume_id_runs | append + mergeSchema | — |
| 04_core_logic/04b_Multi_Gas_Cross_Correlation | 384–388 | gold_multi_gas_signatures | overwrite + overwriteSchema | — |
| 05_facility_attribution/05_attribute_facilities | 411–415 | **gold_plume_catalog** | **overwrite + overwriteSchema, whole table** | — |
| 06_temporal/06_temporal_persistence | 359–363 | gold_emission_sites | overwrite + overwriteSchema | — |
| 06_temporal/06_temporal_persistence | 380–384 | gold_plume_site_mapping | overwrite + overwriteSchema | — |
| 07_validation/07_ingest_validation | 265–269 | validation_cams_plumes | overwrite + overwriteSchema | — |
| 07_validation/07_ingest_validation | 273–277 | validation_cams_plumes_global | overwrite + overwriteSchema | — |
| 07_validation/07_ingest_validation | 650–653 | validation_carbon_mapper_plumes | overwrite | — |

**The four proposed serving tables.** File counts come from `delta_audit.py` on each table; schema changes come
from `a6_history.py`, which diffs `metaData.schemaString` across commits.

| serving table | written by | mode | physical vs active | schema changes in the log |
|---|---|---|---|---|
| fact_facility_daily_snapshot | 03e | overwrite, partition-scoped `replaceWhere` | 90 files / 13,500 rows physical; **30 / 4,500 active**; 60 tombstoned | **none** (3 commits) |
| gold_plume_catalog | 04 **and** 05, both every detection run | overwrite + **overwriteSchema**, whole table | 216 files / 1,758 rows; **8 / 49 active**; 208 tombstoned | **7**: v13, v18, v21 and v24 *add* the 11 attribution columns; v14, v19 and v23 *drop* them |
| dim_facility | 01a | overwrite + **overwriteSchema**, whole table | 72 files / 1,350 rows; **8 / 150 active**; 64 tombstoned | **none** (9 commits) |
| scada_telemetry_hourly | 02c | overwrite, partition-scoped `replaceWhere` | 120 files / 11,647,578 rows; **30 / 3,265,025 active**; 90 tombstoned | **none** (4 commits) |

**What carries the risk.** A1–A4 refute the glob mechanism, so tombstoned files are **not** a correctness risk
for a KQL shortcut. The mechanism that did fail is **a schema change under an accelerated external
table**. On that test:
- **gold_plume_catalog carries it.** Each detection run first writes it *without* the 11 attribution columns (04, lines
  1357–1360 comment: *"It also drops 05's attribution columns until 05 runs again"*) and then *with* them (05).
- **dim_facility** is exposed by construction (overwriteSchema on every run) but has not changed schema to date.
- **fact_facility_daily_snapshot** and **scada_telemetry_hourly** are partition-scoped and have never changed schema.

Not tested: whether an added or dropped *column*, as opposed to the *type* change seen here, also breaks
acceleration auto-refresh. No V2 table with an add/drop history has a shortcut, so this is CANNOT REACH
without creating one, which is outside this brief.

### A6. VACUUM on V2

Command: `python a6_history.py a6_history.json`. It reads every `commitInfo.operation` in every retained
commit JSON of all 41 tables under `greensky_v2_lakehouse/Tables/dbo`.

Result: **0 VACUUM operations in any table.** The operations seen are `CREATE OR REPLACE TABLE AS SELECT`,
`WRITE` and `CREATE TABLE AS SELECT`. There is also no `OPTIMIZE`.

Commit JSONs no longer present: gold_plume_catalog v0–3, gold_flagged_large_clusters v0, and ref_facilities v0–1.
They were cleaned up after checkpoints 10 and 20. **For those versions the commitInfo is CANNOT REACH.** The physical evidence still rules VACUUM
out: gold_plume_catalog still holds **32 files that no retained commit added** (v0–3 generation), and its oldest
tombstoned file is dated `Mon, 17 Aug 2026 05:59:25 GMT`. VACUUM at the default 7-day retention would have
deleted them. Every V2 table audited still holds its full tombstone history (A1, A5).

For contrast, V1 `Operations_LH/Tables/gold/dim_facility` *does* log `OPTIMIZE` and `VACUUM START` / `VACUUM END`
(v2–4 and v7–9, `numDeletedFiles: 0`):

| version | commit time (UTC) | commitInfo.operation | operationParameters.mode | add | remove | operationMetrics rows |
|---|---|---|---|---|---|---|
| 0 | 2026-06-30 05:48:58 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 0 | 350 |
| 1 | 2026-06-30 05:52:47 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 350 |
| 2 | 2026-06-30 06:12:18 | OPTIMIZE | (none) | 1 | 8 | — |
| 3 | 2026-06-30 06:12:27 | VACUUM START | (none) | 0 | 0 | — |
| 4 | 2026-06-30 06:12:27 | VACUUM END | (none) | 0 | 0 | — |
| 5 | 2026-06-30 07:01:33 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 1 | 350 |
| 6 | 2026-06-30 07:06:03 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 350 |
| 7 | 2026-06-30 07:27:31 | OPTIMIZE | (none) | 1 | 8 | — |
| 8 | 2026-06-30 07:27:39 | VACUUM START | (none) | 0 | 0 | — |
| 9 | 2026-06-30 07:27:39 | VACUUM END | (none) | 0 | 0 | — |
| 10 | 2026-07-01 01:33:54 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 1 | 350 |
| 11 | 2026-07-02 01:33:56 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 350 |
| 12 | 2026-07-03 01:33:48 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 350 |
| 13 | 2026-07-04 01:33:58 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 350 |
| 14 | 2026-07-05 01:33:44 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 350 |
| 15 | 2026-07-06 01:33:47 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 350 |
| 16 | 2026-07-07 01:33:49 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 350 |
| 17 | 2026-07-08 01:33:46 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 350 |
| 18 | 2026-07-09 01:34:02 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 350 |
| 19 | 2026-07-10 01:34:06 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 350 |
| 20 | 2026-07-11 01:33:50 | CREATE OR REPLACE TABLE AS SELECT | (none) | 8 | 8 | 350 |

---

## B. Audit of the deployed board "Methane Intelligence - PC v1"

`GET /v1/workspaces/640876ea-…/items?type=KQLDashboard` returns the item
`dc4fd2af-e342-4648-9adc-47f3c8890b74 | Methane Intelligence - PC v1`.
`POST …/items/dc4fd2af-e342-4648-9adc-47f3c8890b74/getDefinition` (LRO, 200) returns `RealTimeDashboard.json`
(83,454 bytes) and `.platform`. Its structure:
- `schema_version` 82
- `autoRefresh {'enabled': True, 'minInterval': '2h'}`
- `changeDetection {'kind': 'liveUpdates', 'minRefreshRate': '2h', 'fallbackRefreshRate': '30s'}`
- one data source, `kusto-trident` **Operations_KQL** (`databaseArtifactId 85d85e9c-…`)
- 3 pages: *Executive Overview*, *Facility Operations* and *README.md*
- 27 tiles, 25 queries (24 bound to tiles, plus one parameter query) and 1 parameter
- `embeddedApps []`

The board was **not modified**.

### B1. Every tile and query, verbatim

Tiles are ordered by page, then layout y, then x. T-numbers are this report's labels. Query text is the
stored `text`, with CRLF normalised to LF.

#### T01. (untitled)

- page: **Executive Overview** · visualType: `markdownCard` · tile id `3fe7ad91-2828-4b11-b388-e759fe12fef4` · queryRef `{}`
- no query; `markdownText` verbatim:

````markdown
![](https://image2url.com/r2/default/images/1772774090163-e8f21834-9a7d-4392-97ef-64e0d4979f8b.png)
````

#### T02. Total Ch4 Emissions (t)

- page: **Executive Overview** · visualType: `card` · tile id `79525990-e562-4740-9065-9463b4755b75` · queryRef `{"kind": "query", "queryId": "41b97c61-b4f9-46a0-9c68-15586d3cddb1"}`
- usedVariables: `[]` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
let _facility_sk = dynamic([]);

let current_total =
    fact_facility_daily_snapshot
    | where snapshot_date between (ago(90d) .. now())
    | where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
    | summarize current_t = round(sum(total_emissions_kg) / 1000.0, 0);

let previous_total =
    fact_facility_daily_snapshot
    | where snapshot_date between (ago(180d) .. ago(90d))
    | where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
    | summarize previous_t = round(sum(total_emissions_kg) / 1000.0, 0);

current_total
| extend join_key = 1
| join kind=inner (
    previous_total
    | extend join_key = 1
) on join_key
| extend pct_change =
    iff(previous_t > 0,
        round(100.0 * (current_t - previous_t) / previous_t, 1),
        0.0)
| project
    Value = current_t,
    ComparisonValue = previous_t,
    ChangePct = pct_change
```

#### T03. Methane Intensity %

- page: **Executive Overview** · visualType: `card` · tile id `4bad3660-c3d1-41ad-8852-8efd7aef28b5` · queryRef `{"kind": "query", "queryId": "72675de1-eef7-4e21-9677-f10b2b20dec9"}`
- usedVariables: `[]` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 1.2: Methane Intensity % (KPI) test------------------------------
let _start = datetime(2025-05-10T00:00:00Z);
let _end = now();
let _facility_sk = dynamic([]);
let emissions =
    fact_facility_daily_snapshot
    | where snapshot_date between (_start .. _end)
    | where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
    | summarize emissions_kg = sum(total_emissions_kg);
let production =
    fact_production_daily
    | where prod_date between (_start .. _end)
    | where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
    | summarize prod_mcf = sum(gross_gas_mcf);
emissions
| extend prod_mcf = toscalar(production)
| extend intensity_pct = iff(prod_mcf > 0, round((emissions_kg / 19.26) / prod_mcf * 100.0, 3), real(0))
| project intensity_pct
```

#### T04. Financial Impact (Fines)

- page: **Executive Overview** · visualType: `card` · tile id `f73a572c-3b3d-4f9f-a09e-679b3cc21377` · queryRef `{"kind": "query", "queryId": "15c9d982-330b-4c15-a49b-469ec9f7061f"}`
- usedVariables: `[]` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
let _start = datetime(2026-05-01);
let _end = now();
let _facility_sk = dynamic([]);
let _scale = 1000.0;  // adjust until numbers look realistic
fact_financial_impact
| where detect_ts between (_start .. _end)
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| summarize
    p10_raw = sum(total_financial_impact_usd_p10) / _scale,
    p50_raw = sum(total_financial_impact_usd_p50) / _scale,
    p90_raw = sum(total_financial_impact_usd_p90) / _scale,
    lost_gas_raw = sum(lost_gas_value_usd) / _scale,
    fines_raw = sum(nsps_violation_fine_usd) / _scale
| extend
    p10 = strcat(tostring(round(p10_raw / 1000000, 2)), "M"),
    p50 = strcat(tostring(round(p50_raw / 1000000, 2)), "M"),
    p90 = strcat(tostring(round(p90_raw / 1000000, 2)), "M"),
    lost_gas = strcat(tostring(round(lost_gas_raw / 1000000, 2)), "M"),
    fines = strcat(tostring(round(fines_raw / 1000000, 2)), "M")
| project p10, p50, p90, lost_gas, fines
```

#### T05. Active Plumes

- page: **Executive Overview** · visualType: `card` · tile id `5c0fd148-1876-4772-bd27-8e507fe6441b` · queryRef `{"kind": "query", "queryId": "e5e882d4-90d7-46e6-a8eb-eb0ebc116e18"}`
- usedVariables: `[]` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 1.4: Active Plumes (KPI) ------------------------------------
let _facility_sk = dynamic([]);
let plume_facilities =
    bridge_plume_facility_attribution
    | where primary_flag == true
    | project plume_id = tostring(plume_id), facility_sk;
plume_detection_hot
| where detect_ts > ago(2200d)
| extend plume_id = tostring(plume_id)
| join kind=leftouter plume_facilities on plume_id
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| summarize active_plumes = dcount(plume_id)
;
```

#### T06. Open Work Orders

- page: **Executive Overview** · visualType: `card` · tile id `8a4ff18c-7544-4a62-b579-68fe77ebbb4e` · queryRef `{"kind": "query", "queryId": "c42534fa-c7fd-4e6f-817c-68ff0588d30d"}`
- usedVariables: `[]` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
let _facility_sk = dynamic([]);
let _raw = 
    fact_work_order
    | where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
    | where status != "Closed"
    | summarize raw_total = count(), raw_breached = countif(is_breached == true);
_raw
| project total = toint(raw_total / 100), breached = toint(raw_breached / 100)
```

#### T07. Compliance Violations

- page: **Executive Overview** · visualType: `card` · tile id `700a1d99-0449-4d1a-a4af-a2a76e90faa8` · queryRef `{"kind": "query", "queryId": "3f09dacb-d6fb-466e-8afe-ee000b7aaa07"}`
- usedVariables: `[]` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
let _data_start = datetime(2026-06-10);
let _data_end = datetime(2026-07-09);
let _facility_sk = dynamic([]);
fact_compliance_event
| where event_ts between (_data_start .. _data_end)
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| where is_violation == true
| summarize raw_total = count(),
            raw_critical = countif(severity == "Critical"),
            raw_major = countif(severity == "Major")
| extend _scale = 500.0
| project total = toint(raw_total / _scale),
          critical = toint(raw_critical / _scale),
          major = toint(raw_major / _scale)
```

#### T08. Detections Table

- page: **Executive Overview** · visualType: `table` · tile id `46cccd0b-25a0-4777-ac3c-2a8ed230e3ab` · queryRef `{"kind": "query", "queryId": "f6aa7a68-f3b3-4a75-9b5e-c03f3fc196fa"}`
- usedVariables: `[]` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
external_table('fact_plume_detection')
| where detect_ts >= ago(90d)
| extend plume_id = tostring(plume_id)
| summarize arg_max(detect_ts, *) by plume_id
| join kind=inner (
    external_table('bridge_plume_facility_attribution')
    | where primary_flag == true
    | extend plume_id = tostring(plume_id)
    | summarize arg_max(allocation_factor, *) by plume_id
    | project plume_id, facility_sk, distance_km, attribution_confidence, allocation_factor
) on plume_id
| join kind=inner (
    external_table('dim_facility')
    | where is_current == true
    | summarize arg_max(facility_sk, *) by facility_sk
    | project facility_sk, facility_name, basin
) on facility_sk
| join kind=leftouter (
    external_table('gold_multi_gas_signatures')
    | extend plume_id = tostring(plume_id)
    | project plume_id, emission_signature, no2_elevated
) on plume_id
| extend rate_kg_hr = round(emission_rate_kg_s * 3600.0, 1),
         sev = case(
             emission_rate_kg_s * 3600 > 500, "P1",
             emission_rate_kg_s * 3600 > 250, "P2",
             emission_rate_kg_s * 3600 > 100, "P3",
             "P4"),
         source_label = "satellite"
| extend emission = strcat(toint(rate_kg_hr))
| extend len = strlen(emission)
| extend formatted_rate =
    case(
        len <= 3, emission,
        len <= 6, strcat(substring(emission, 0, len-3), ",", substring(emission, len-3, 3)),
        len <= 9, strcat(substring(emission, 0, len-6), ",",
                         substring(emission, len-6, 3), ",",
                         substring(emission, len-3, 3)),
        emission
    )
| project
    ['Detection Time'] = format_datetime(detect_ts, 'yyyy-MM-dd HH:mm'),
    Facility = facility_name,
    Basin = basin,
    ['Emission Rate'] = strcat(formatted_rate, " kg/hr"),
    Severity = sev,
    Signature = coalesce(emission_signature, "—"),
    ['Confidence'] = round(attribution_confidence, 2),
    ['Distance (km)'] = round(distance_km, 2),
    Allocation = round(allocation_factor, 2),
    Source = source_label,
    Status = case(
        sev == "P1", "🔴 Critical",
        sev == "P2", "🟠 High",
        sev == "P3", "🟡 Medium",
        "🟢 Low")
| order by ['Detection Time'] desc
```

#### T09. Facility Map

- page: **Executive Overview** · visualType: `map` · tile id `afcec3cb-212d-4e92-ac05-f47d9572f3b9` · queryRef `{"kind": "query", "queryId": "90478806-d5d9-4b9e-b534-ef70c4796e7d"}`
- usedVariables: `[]` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
let _facility_sk = dynamic([]);
let recent_metrics =
    fact_facility_daily_snapshot
    | where snapshot_date > ago(2200d)
    | summarize emissions_kg = sum(total_emissions_kg),
                avg_risk = avg(risk_score),
                health = avg(equipment_health_score) by facility_sk;
dim_facility
| where is_current == true and active_flag == true
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| join kind=leftouter recent_metrics on facility_sk
| project facility_name, basin, anchor_site, latitude, longitude,
          emissions_kg = coalesce(emissions_kg, 0.0),
          avg_risk = coalesce(avg_risk, 0.0),
          health = coalesce(health, 0.7)
| top 25 by emissions_kg desc
;
```

#### T10. Daily Emissions Trend

- page: **Executive Overview** · visualType: `line` · tile id `593f12f2-9410-4990-bacc-b9294305b338` · queryRef `{"kind": "query", "queryId": "12b1f6af-9af1-46b1-b268-e2e46aebf00b"}`
- usedVariables: `[]` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 1.7: Daily Emissions Trend (line chart) ----------------------
let _start = datetime(2026-05-01);
let _end = now();
let _facility_sk = dynamic([]);
fact_facility_daily_snapshot
| where snapshot_date between (_start .. _end)
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| summarize daily_kg = sum(total_emissions_kg) by snapshot_date
| order by snapshot_date asc
| extend target_kg = 12000.0
;
```

#### T11. Top 10 Facilities by P50

- page: **Executive Overview** · visualType: `bar` · tile id `7efca995-351f-4f01-8e4e-db90edc4e54a` · queryRef `{"kind": "query", "queryId": "abdd3176-68e3-4437-848c-7c5009f85e67"}`
- usedVariables: `[]` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 1.9: Top 10 Facilities by P50 (bar chart) -------------------
let _start = datetime(2020-05-01);
let _end = now();
let _facility_sk = dynamic([]);
fact_financial_impact
| where detect_ts between (_start .. _end)
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| summarize p50 = sum(total_financial_impact_usd_p50) by facility_sk
| join kind=inner (dim_facility | where is_current == true) on facility_sk
| project facility_name, basin, p50
| top 10 by p50 desc
;
```

#### T12. Financial Impact Composition

- page: **Executive Overview** · visualType: `stackedarea` · tile id `213aed1b-464f-4e76-85b6-94821967f076` · queryRef `{"kind": "query", "queryId": "5a41b844-98e2-4571-92c4-b548c9aa8a63"}`
- usedVariables: `[]` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 1.10: Financial Impact Composition (stacked area) ------------
let _start = datetime(2024-03-01T00:00:00Z);
let _end = now();
let _facility_sk = dynamic([]);
fact_financial_impact
| where detect_ts between (_start .. _end)
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| summarize lost_gas = sum(lost_gas_value_usd),
            nsps_fines = sum(nsps_violation_fine_usd)
  by day = startofday(detect_ts)
| extend lost_gas_log = log10(lost_gas + 1),
         nsps_fines_log = log10(nsps_fines + 1)
| order by day asc
;
```

#### T13. Emission Signatures Breakdown

- page: **Executive Overview** · visualType: `bar` · tile id `f542703a-341f-4aa1-875d-bc0b5d707346` · queryRef `{"kind": "query", "queryId": "47bf0fb0-0bb1-4c40-a4d6-843f9cf87e20"}`
- usedVariables: `[]` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
external_table('gold_multi_gas_signatures')
| where detect_ts >= ago(90d)
| summarize plume_count = count() by emission_signature
| order by plume_count desc
```

#### T14. (untitled)

- page: **Facility Operations** · visualType: `markdownCard` · tile id `96633581-4e6a-4ad3-b2c9-1a6a419c75b0` · queryRef `{}`
- no query; `markdownText` verbatim:

````markdown
![](https://image2url.com/r2/default/images/1772774090163-e8f21834-9a7d-4392-97ef-64e0d4979f8b.png)

> ⚠️ **Note:** This page is fully functional only when a facility is selected from the main dashboard.  
````

#### T15. Active Exceedances

- page: **Facility Operations** · visualType: `card` · tile id `21841cb8-9c30-45ab-9d02-b0b7fd729795` · queryRef `{"kind": "query", "queryId": "06d38081-77f8-41f4-ac0b-b03dfee2e11b"}`
- usedVariables: `['selected_facility']` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 2.1: Active Exceedances Now (KPI) ----------------------------
let _facility_sk = dynamic([]);
let _selected = ['selected_facility'];
external_table('sensor_telemetry')
| where reading_ts > ago(2200d)
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| where exceedance_flag == true
| join kind=inner (dim_facility | where is_current == true
                   | project facility_sk, facility_name) on facility_sk
| where array_length(_selected) == 0 or facility_name in (_selected)
| summarize active = dcount(sensor_id),
            facilities_affected = dcount(facility_sk)
;
```

#### T16. Highest Active Emission Rate

- page: **Facility Operations** · visualType: `card` · tile id `ba3ab926-8c20-4793-8e4e-c886d8701d35` · queryRef `{"kind": "query", "queryId": "579b7d4a-12ac-4160-ac81-bf2a2f2d025c"}`
- usedVariables: `['selected_facility']` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 2.3: Highest Active Emission Rate (KPI) ---------------------
let _selected = ['selected_facility'];
let result =
    plume_detection_hot
    | extend plume_id = tostring(plume_id)
    | where detect_ts >= ago(7d)
    | join kind=inner (
        bridge_plume_facility_attribution
        | extend plume_id = tostring(plume_id)
        | join kind=inner (
            external_table('dim_facility')
            | where is_current == true
            | project facility_sk, facility_name
        ) on facility_sk
        | project plume_id, facility_name
    ) on plume_id
    | where array_length(_selected) == 0 or facility_name in (_selected)
    | top 1 by emission_rate_kg_s desc
    | extend rate_kg_hr = round(emission_rate_kg_s * 3600.0, 1)
    | project plume_id, rate_kg_hr,
              confidence = emission_rate_confidence,
              detect_ts, anchor_distance_km;
union result,
(datatable(plume_id:string, rate_kg_hr:real, confidence:real, detect_ts:datetime, anchor_distance_km:real)
    ["no-data", 0.0, 0.0, datetime(null), 0.0])
| where isnotempty(plume_id)
| top 1 by rate_kg_hr desc
```

#### T17. Open Work Orders

- page: **Facility Operations** · visualType: `card` · tile id `c0f7e23b-896b-4235-bdc5-379583505d64` · queryRef `{"kind": "query", "queryId": "90f9935a-6d70-40c3-9518-18a8894e3592"}`
- usedVariables: `['selected_facility']` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile: Open Work Orders (KPI) -------------------------------------
let _facility_sk = dynamic([]);
let _selected = ['selected_facility'];
fact_work_order
| where status != "Closed"
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| join kind=inner (dim_facility | where is_current == true
                   | project facility_sk, facility_name) on facility_sk
| where array_length(_selected) == 0 or facility_name in (_selected)
| summarize open_tickets = count(),
            critical = countif(priority == "Critical"),
            breached_sla = countif(is_breached == true),
            facilities_affected = dcount(facility_sk)
;
```

#### T18. Plume Detected (D-7)

- page: **Facility Operations** · visualType: `card` · tile id `54d001f5-d455-461f-be10-20c1ce09e567` · queryRef `{"kind": "query", "queryId": "06ec4b39-e39c-420d-804e-75a0ab6a2c6b"}`
- usedVariables: `['selected_facility']` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 2.2: Plumes Detected (Last 7 Days) (KPI) ----------------------------
let _selected = ['selected_facility'];
let result =
    plume_detection_hot
    | extend plume_id = tostring(plume_id)
    | extend emission_rate_confidence = toreal(emission_rate_confidence),
             emission_rate_kg_s = toreal(emission_rate_kg_s)
    | where detect_ts >= ago(7d)
    | join kind=inner (
        bridge_plume_facility_attribution
        | extend plume_id = tostring(plume_id)
        | join kind=inner (
            external_table('dim_facility')
            | where is_current == true
            | project facility_sk, facility_name
        ) on facility_sk
        | project plume_id, facility_name
    ) on plume_id
    | where array_length(_selected) == 0 or facility_name in (_selected)
    | summarize total = tolong(count()),
                high_conf = tolong(countif(emission_rate_confidence > 0.7)),
                olre = tolong(countif(emission_rate_kg_s * 3600 > 100));
union result,
(datatable(total:long, high_conf:long, olre:long)
    [long(0), long(0), long(0)])
| top 1 by total desc
```

#### T19. Sensors Offline

- page: **Facility Operations** · visualType: `card` · tile id `8374ed7a-d33d-42e1-91e7-748f248d08f6` · queryRef `{"kind": "query", "queryId": "f5d262b6-4930-4601-a43f-ec6bab26fdfa"}`
- usedVariables: `['selected_facility']` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 2.4: Sensors Offline (KPI) ----------------------------------
let _facility_sk = dynamic([]);
let _selected = ['selected_facility'];
external_table('sensor_telemetry')
| where reading_ts > ago(2200d)
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| join kind=inner (dim_facility | where is_current == true
                   | project facility_sk, facility_name) on facility_sk
| where array_length(_selected) == 0 or facility_name in (_selected)
| summarize last_reading = max(reading_ts) by sensor_id, facility_sk
| where last_reading < ago(8h)
| summarize offline = count(), facilities = dcount(facility_sk)
;
```

#### T20. Overdue PM Count

- page: **Facility Operations** · visualType: `card` · tile id `f1fecd57-afcd-413c-9edc-56c043b8f499` · queryRef `{"kind": "query", "queryId": "d1679652-274e-40d6-af8c-2cfbf270bae6"}`
- usedVariables: `['selected_facility']` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 4.2: Overdue PM Count (KPI) ---------------------------------
let _facility_sk = dynamic([]);
let _selected = ['selected_facility'];
let last_pm =
    fact_maintenance
    | where maintenance_type == "Preventive"
    | summarize last_ts = max(maintenance_ts) by equipment_sk;
dim_equipment
| where is_current == true
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| join kind=inner (dim_facility | where is_current == true
                   | project facility_sk, facility_name) on facility_sk
| where array_length(_selected) == 0 or facility_name in (_selected)
| join kind=leftouter last_pm on equipment_sk
| extend days_since = case(isnotnull(last_ts),
                           datetime_diff('day', now(), last_ts),
                           9999)
| where days_since > inspection_frequency_days
| summarize overdue_count = count(),
            critical_overdue = countif(criticality == "Critical"),
            avg_days_overdue = round(avg(todouble(days_since - inspection_frequency_days)), 0)
;
```

#### T21. LDAR Leaks Repaired

- page: **Facility Operations** · visualType: `card` · tile id `e9dd8fe3-e97a-4be4-a9c0-5ffe60317630` · queryRef `{"kind": "query", "queryId": "fd50844e-f4c6-4f92-8a97-022b94b2f9e6"}`
- usedVariables: `['selected_facility']` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 4.3: LDAR Leaks Repaired (KPI) ------------------------------
let _facility_sk = dynamic([]);
let _selected = ['selected_facility'];
fact_ldar_survey
| where survey_ts > ago(2200d)
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| join kind=inner (dim_facility | where is_current == true
                   | project facility_sk, facility_name) on facility_sk
| where array_length(_selected) == 0 or facility_name in (_selected)
| summarize detected = sum(leaks_detected),
            repaired = sum(leaks_repaired),
            cost = sum(repair_cost_usd),
            surveys = count()
| extend repair_rate_pct = round(100.0 * todouble(repaired) / todouble(detected), 1)
;
```

#### T22. Compliance Heatmap

- page: **Facility Operations** · visualType: `heatmap` · tile id `34a26539-f734-4aa2-80a0-9b46d03bd223` · queryRef `{"kind": "query", "queryId": "186280f9-a9bf-4767-865a-8569a79b3ca4"}`
- usedVariables: `['selected_facility']` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
let _start = datetime(2025-05-10T00:00:00Z);
let _end   = now();
let _facility_sk = dynamic([]);
let _selected = ['selected_facility'];   // dashboard parameter token

let top_facilities =
    fact_compliance_event
    | where event_ts between (_start .. _end) and is_violation == true
    | where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
    | summarize total_violations = count() by facility_sk
    | top 10 by total_violations desc
    | project facility_sk;

let fac =
    external_table('dim_facility')
    | where is_current == true
    | summarize arg_max(facility_sk, *) by facility_sk
    | project facility_sk, facility_name;

// Monthly violations for the top facilities, with names. Materialized because
// it's referenced several times below.
let base = materialize(
    fact_compliance_event
    | where event_ts between (_start .. _end) and is_violation == true
    | where facility_sk in (top_facilities)
    | summarize violations = count() by facility_sk, month = startofmonth(event_ts)
    | join kind=inner fac on facility_sk
    | project facility_name, month, violations
);

// Your original result: selection applied.
let primary =
    base
    | where array_length(_selected) == 0 or facility_name in (_selected);

// Fallback: top 5 facilities by total violations.
let fallback =
    base
    | where facility_name in (
        ( base
          | summarize total = sum(violations) by facility_name
          | top 5 by total desc
          | project facility_name )
      );

let has_data = toscalar(primary | count) > 0;

union
    (primary  | where has_data),
    (fallback | where not(has_data))
| project facility_name, month, violations
| order by violations desc
```

#### T23. Emissions Trend

- page: **Facility Operations** · visualType: `line` · tile id `da7d2867-b3ee-4c6e-823b-87f368ba4016` · queryRef `{"kind": "query", "queryId": "f97a1520-be7d-4abe-842d-0bc731cf5399"}`
- usedVariables: `['selected_facility']` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 1.7: Daily Emissions Trend (line chart) ----------------------
let _start = datetime(2026-05-01);
let _end = now();
let _facility_sk = dynamic([]);
let _selected = ['selected_facility'];
fact_facility_daily_snapshot
| where snapshot_date between (_start .. _end)
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| join kind=inner (dim_facility | where is_current == true
                   | project facility_sk, facility_name) on facility_sk
| where array_length(_selected) == 0 or facility_name in (_selected)
| summarize daily_kg = sum(total_emissions_kg) by snapshot_date
| order by snapshot_date asc
;
```

#### T24. Financial Loss Breakdown by Facility 

- page: **Facility Operations** · visualType: `stackedarea` · tile id `1ff6b100-4f77-4d99-82e1-bbf6428c2cec` · queryRef `{"kind": "query", "queryId": "8e0990f4-a2f8-4468-8f18-0ce101bea38c"}`
- usedVariables: `['selected_facility']` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 1.10: Financial Impact Composition (stacked area) ------------
let _start = datetime(2024-03-01T00:00:00Z);
let _end = now();
let _facility_sk = dynamic([]);
let _selected = ['selected_facility'];
fact_financial_impact
| where detect_ts between (_start .. _end)
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| join kind=inner (dim_facility | where is_current == true
                   | project facility_sk, facility_name) on facility_sk
| where array_length(_selected) == 0 or facility_name in (_selected)
| summarize lost_gas = sum(lost_gas_value_usd),
            nsps_fines = sum(nsps_violation_fine_usd)
  by week = startofday(detect_ts)
| extend lost_gas_log = log10(lost_gas + 1),
         nsps_fines_log = log10(nsps_fines + 1)
| order by week asc
;
```

#### T25. Work Order Trail

- page: **Facility Operations** · visualType: `table` · tile id `a7e005f7-6a33-4d5f-83bb-e8f48528e2bb` · queryRef `{"kind": "query", "queryId": "bb6afc86-dce7-438d-863a-811fba3d0c13"}`
- usedVariables: `['selected_facility']` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql
// -- Tile 3.8: Work Order Trail ---------------------------------------
let _selected = ['selected_facility'];
let _plume_id = toscalar(
    plume_detection_hot
    | extend plume_id = tostring(plume_id)
    | join kind=inner (
        bridge_plume_facility_attribution
        | extend plume_id = tostring(plume_id)
        | where primary_flag == true
        | join kind=inner (
            external_table('dim_facility')
            | where is_current == true
            | project facility_sk, facility_name
        ) on facility_sk
        | project plume_id, facility_name
    ) on plume_id
    | where isnull(_selected) or array_length(_selected) == 0 or facility_name in (_selected)
    | top 1 by detect_ts desc
    | project plume_id
);
let _plume_facility =
    bridge_plume_facility_attribution
    | where tostring(plume_id) == _plume_id and primary_flag == true
    | project facility_sk;
let _plume_ts =
    toscalar(
        plume_detection_hot
        | where tostring(plume_id) == _plume_id
        | project detect_ts
    );
fact_work_order
| where isnotnull(_plume_id)
| where facility_sk in (_plume_facility)
| where created_ts between ((_plume_ts - 30d) .. (_plume_ts + 30d))
| project ticket_id, created_ts, closed_ts, priority, status,
          sla_hours, resolution_time_hours, is_breached, source
| order by created_ts asc
```

#### T26. Top 20 High-Risk Equipment

- page: **Facility Operations** · visualType: `table` · tile id `7489e1c7-38b4-43f9-ae31-c0de6cce6875` · queryRef `{"kind": "query", "queryId": "32c19b1c-b213-4c01-8f02-e8d4188adb72"}`
- usedVariables: `['selected_facility']` · dataSource `{'kind': 'inline', 'dataSourceId': '45111dc0-5b37-4916-bed1-928c6df43884'}`

```kql

// -- Tile 4.7: Top 20 High-Risk Equipment (table) ---------------------
let _facility_sk = dynamic([]);
let _selected = ['selected_facility'];
let last_maint =
    fact_maintenance
    | summarize last_ts = max(maintenance_ts) by equipment_sk;
let leaks =
    bridge_plume_equipment
    | where primary_flag == true
    | summarize leak_count = count() by equipment_sk;
dim_equipment
| where is_current == true
| where array_length(_facility_sk) == 0 or facility_sk in (_facility_sk)
| join kind=leftouter last_maint on equipment_sk
| join kind=leftouter leaks on equipment_sk
| join kind=inner (dim_facility | where is_current == true
                   | project facility_sk, facility_name) on facility_sk
| where array_length(_selected) == 0 or facility_name in (_selected)
| extend age_yrs = round(todouble(datetime_diff('day', now(), install_date)) / 365.0, 1),
         days_since = case(isnotnull(last_ts),
                           datetime_diff('day', now(), last_ts),
                           9999),
         leak_count = coalesce(leak_count, tolong(0))
| extend overdue_days = days_since - inspection_frequency_days
| where overdue_days > 0 or criticality in ("High", "Critical") or leak_count > 0
| project equipment_id, facility_name, equipment_type, manufacturer,
          criticality, age_yrs, last_maint_ts = last_ts,
          days_since, overdue_days, leak_count
| order by overdue_days desc, criticality asc, leak_count desc
| take 20
;
```

#### T27. (untitled)

- page: **README.md** · visualType: `markdownCard` · tile id `c01f4b3b-4ba8-4f5e-a289-96bc9d2a739c` · queryRef `{}`
- no query; `markdownText` verbatim:

````markdown
# Methane Emissions Dashboard — README

## Overview

Two-page dashboard for methane emissions monitoring. Start on the **Executive Overview** for portfolio-wide performance, then drill through to the **Facility Operations** page to investigate a specific facility.

---

## Executive Overview

Portfolio-wide view for executives, sustainability leads, operations managers, and compliance teams.

### KPI Cards

| KPI | What it shows | How to interpret |
|-----|--------------|-----------------|
| **Total Methane Emissions** | Total tonnes CH₄, last 30 days  | Overview |
| **Methane Intensity (%)** | Emissions ÷ Gas Produced × 100 | Lower = more efficient; useful for comparing facilities of different sizes. |
| **Financial Impact (P10/P50/P90)** | Estimated financial exposure across  Fines | Use for planning |
| **Active Methane Plumes** | Count of active emission events across monitored facilities | Rising count may signal equipment failures; review alongside Work Orders and Violations. |
| **Open Work Orders** | Unresolved methane-related work orders | rising count alongside rising emissions = maintenance backlog. |
| **Compliance Violations** | Regulatory violations this period | Critical violations require immediate action; track trends alongside plume activity. |

### Visualizations

| Chart | Type | What it shows |
|-------|------|--------------|
| **Daily Methane Emissions Trend** | Line chart | Daily portfolio emissions vs. a target threshold; spikes indicate operational failures. |
| **Top Emitting Facilities Map** | Bubble map | Geographic emission concentration; bubble size = emissions volume; prioritize large bubbles with poor equipment health. |
| **Top 10 Facilities by Financial Impact (P50)** | Bar chart | Facilities ranked by expected financial loss; guides where to prioritize mitigation investment. |
| **Financial Impact Composition Trend** | Stacked area chart | Weekly Lost Gas vs. NSPS Fines; shifts in composition reveal whether costs are operational or compliance-driven. |
| **High-Risk Methane Leak Events** | Interactive table | Recent plume detections with severity (P1–P4), confidence, emission rate, and source type; focus on P1/P2 with high confidence. |

**Severity levels:** P1 = Critical, P2 = High, P3 = Medium, P4 = Low.

### Recommended Workflow

1. Review Total Methane Emissions trend.
2. Check Active Plumes and Open Work Orders.
3. Assess Compliance Violations.
4. Review Financial Impact exposure.
5. Identify top emitting facilities on the map.
6. Investigate High-Risk Methane Leak Events.
7. **Drill into a facility page** for root-cause analysis.

---

## Facility Operations

Detailed operational view scoped entirely to the **selected facility**. All metrics, charts, and tables on this page reflect that facility only. Used after drilling through from the Executive Overview.

Designed for operations teams, maintenance planners, environmental specialists, and reliability engineers.

### KPI Cards

| KPI | What it shows | How to interpret |
|-----|--------------|-----------------|
| **Active Exceedances Now** | Sensors currently above configured methane thresholds | Multiple exceedances at once may indicate a widespread issue; review alongside Active Plumes. |
| **Plumes Detected (Last 7 Days)** | Total plumes, high-confidence detections, and OLREs (Other Large Release Events) | Rising count = worsening leak activity; high-confidence detections should be prioritized. |
| **Highest Active Emission Rate** | Most severe plume in the last 7 days: rate (kg/hr),  |  investigate first. |
| **Open Work Orders** | Unresolved work orders  |  critical tickets = high-priority operational risk. |
| **Sensors Offline** | Sensors with no data in the last 8 hours | Offline sensors create monitoring blind spots; investigate communication, power, or hardware faults. |
| **Overdue Preventive Maintenance** | Assets past their PM schedule, including critical assets and avg. days overdue | Extended overdue periods correlate with elevated failure and leak risk; prioritize critical assets. |
| **LDAR Leak Repairs Performance** | Leaks detected vs. repaired,  | Large detected-vs-repaired gap = backlog; low repair rate = LDAR program underperforming. |

### Visualizations

| Chart | Type | What it shows |
|-------|------|--------------|
| **Daily Emissions Trend** | Line chart | Facility-level daily emissions over time; spikes correspond to equipment failures or large release events. |
| **Weekly Financial Impact Breakdown** | Stacked area chart | Lost Gas vs. NSPS Fines week-over-week; shifts indicate whether costs are operational or compliance-driven. |
| **Compliance Violations Heatmap** | Heatmap | Monthly violation counts across selected facility |
| **Work Order Trail Around Plume Event** | Investigation table | Work orders ±30 days around a selected plume detection; reveals whether maintenance preceded or followed a leak, and whether SLAs were met. |
| **High-Risk Equipment List** | Prioritized table | Top 20 assets ranked by risk (maintenance overdue, criticality, leak history, age, inspection compliance); deploy maintenance resources here first. |

### Recommended Workflow

1. Review Active Exceedances and Active Plumes.
2. Assess the Highest Active Emission Rate.
3. Verify sensor coverage using Sensors Offline.
4. Examine Daily Emissions Trend.
5. Review Open Work Orders and Maintenance Backlog.
6. Analyze Compliance Violation patterns.
7. Investigate Work Order Trail around significant plume events.
8. Prioritize corrective actions using the High-Risk Equipment List.
9. Monitor Financial Impact to validate mitigation effectiveness.
````

#### Query not bound to a tile: `12a7d6b5-b63f-49b3-8ee2-f6ff7bb727ad`
- usedVariables: `[]`

```kql
external_table('dim_facility')
| where is_current == true
| distinct facility_name
| order by facility_name asc
```


### B2. One row per tile

Each query was executed read-only, exactly as stored, with `python b_run.py`. The one parameter token was bound
to its default ("all" = empty array) by prepending
`declare query_parameters(['selected_facility']:dynamic = dynamic([]));`. **All 24 tile queries and the
parameter query ran without error.**

Bare table names (for example `fact_facility_daily_snapshot`) resolve to the shortcut external tables of the same
name. Lakehouse resolution comes from the shortcut dump in A3:
- 26 shortcuts point at V1 `Operations_LH` (`f689a941-…`)
- `ch4_plume_catalog` and `weather` point at Planetary_computer_LH
- `gold_multi_gas_signatures` points at V2

The live V1 horizons (KQL `summarize min/max`) set the context: `fact_facility_daily_snapshot`, `fact_financial_impact`,
`fact_compliance_event`, `fact_work_order`, `fact_plume_detection` and `plume_detection_hot` all end on **2026-07-09**;
`sensor_telemetry` ends on 2026-07-10 20:00; `fact_production_daily`, `fact_maintenance` and `fact_ldar_survey` end on 2026-06-30.

| tile | page | source table(s) | resolves to | result today (all facilities) | verdict |
|---|---|---|---|---|---|
| T01 banner | Executive Overview | — (markdown image) | external host (B5) | — | external dependency |
| T02 Total Ch4 Emissions (t) | Executive | fact_facility_daily_snapshot | V1 | `Value 0.0, ComparisonValue 153789.0, ChangePct -100.0` | **FAIL**: `ago(90d)` window has no V1 data, so the headline shows 0 t and −100 % |
| T03 Methane Intensity % | Executive | fact_facility_daily_snapshot, fact_production_daily | V1 | `1.759` | **FAIL**: same `_start` but different data spans (emissions 2026-06-10…07-09, production 2025-05-10…06-30), so the numerator and denominator cover different periods |
| T04 Financial Impact (Fines) | Executive | fact_financial_impact | V1 | `p50 "5.13M"`; the true sum is `5126763767.72` USD | **FAIL**: scale and units (B3) |
| T05 Active Plumes | Executive | plume_detection_hot, bridge_plume_facility_attribution | V1 | `22` | **FLAG**: "active" means any detection in 2,200 days |
| T06 Open Work Orders | Executive | fact_work_order | V1 | `total 290, breached 0`; the true open count is `29023` | **FAIL**: `÷ 100` |
| T07 Compliance Violations | Executive | fact_compliance_event | V1 | `385 / 239 / 133`; true `192698 / 119720 / 66730` | **FAIL**: `÷ 500` |
| T08 Detections Table | Executive | fact_plume_detection, bridge_plume_facility_attribution, dim_facility (V1) + gold_multi_gas_signatures | **V1 + V2** | **0 rows** | **FAIL**: `ago(90d)` is past the V1 horizon. It also joins V1 integer `plume_id` to V2 `plume_id`, a different ID scheme that is null in KQL (A2) |
| T09 Facility Map | Executive | fact_facility_daily_snapshot, dim_facility | V1 | 25 rows; top rows `avg_risk 100.0` | **FAIL**: `health = coalesce(health, 0.7)`. Reads V1-only `latitude/longitude/basin/anchor_site` |
| T10 Daily Emissions Trend | Executive | fact_facility_daily_snapshot | V1 | 24 rows, daily_kg 2.9–8.3 million | **FLAG**: `target_kg = 12000.0` literal, plus an unrelated chart line at `50000` |
| T11 Top 10 Facilities by P50 | Executive | fact_financial_impact, dim_facility | V1 | 10 rows; top value `171024389.4` USD | **FLAG**: sums per-event P50s and labels the total "P50" |
| T12 Financial Impact Composition | Executive | fact_financial_impact | V1 | 24 rows | **FLAG**: daily bins under a README that says "Weekly"; log columns computed but not plotted |
| T13 Emission Signatures Breakdown | Executive | gold_multi_gas_signatures | **V2** | `Fugitive Leak 47, Incomplete Combustion 2` | passes today; `ago(90d)` starts dropping V2 rows after 2026-11-14 (earliest `detect_ts` is 2026-08-16 19:45) |
| T14 banner + note | Facility Operations | — | external host | — | external dependency |
| T15 Active Exceedances | Facility Ops | sensor_telemetry, dim_facility | V1 | `0, 0` | **FLAG**: "Now" is computed over `ago(2200d)` |
| T16 Highest Active Emission Rate | Facility Ops | plume_detection_hot, bridge_plume_facility_attribution, dim_facility | V1 | `"no-data", 0.0, …` | **FAIL**: a placeholder row is shown as a 0.0 kg/hr rate |
| T17 Open Work Orders | Facility Ops | fact_work_order, dim_facility | V1 | `29023, critical 0, breached 0, 95` | **FAIL**: `priority == "Critical"` never matches (values are P1/P2/P3). Contradicts T06 |
| T18 Plume Detected (D-7) | Facility Ops | plume_detection_hot, bridge_plume_facility_attribution, dim_facility | V1 | `0, 0, 0` | **FLAG**: `ago(7d)` past the horizon |
| T19 Sensors Offline | Facility Ops | sensor_telemetry, dim_facility | V1 | `1400, 350` (every sensor) | **FLAG**: `ago(8h)` against data that ended 2026-07-10 |
| T20 Overdue PM Count | Facility Ops | fact_maintenance, dim_equipment, dim_facility | V1 | `6422, 969, avg 1330 days` | **FAIL**: `now()` arithmetic and a `9999` sentinel |
| T21 LDAR Leaks Repaired | Facility Ops | fact_ldar_survey, dim_facility | V1 | `1012 / 267 / 290524.39 / 1626 / 26.4 %` | passes (`ago(2200d)` window) |
| T22 Compliance Heatmap | Facility Ops | fact_compliance_event, dim_facility | V1 | 20 rows | **FLAG**: when the selection has no data it silently shows the top-5 *other* facilities |
| T23 Emissions Trend | Facility Ops | fact_facility_daily_snapshot, dim_facility | V1 | 24 rows | **FLAG**: chart line at `4500` literal |
| T24 Financial Loss Breakdown by Facility | Facility Ops | fact_financial_impact, dim_facility | V1 | 24 rows | **FLAG**: column `week = startofday(...)` |
| T25 Work Order Trail | Facility Ops | plume_detection_hot, bridge_plume_facility_attribution, dim_facility, fact_work_order | V1 | **28,801 rows** | **FLAG**: unbounded table; `_plume_ts` is `toscalar` over several rows of one plume |
| T26 Top 20 High-Risk Equipment | Facility Ops | fact_maintenance, bridge_plume_equipment, dim_equipment, dim_facility | V1 | top rows `days_since 9999, overdue_days 9909, age_yrs 0.3` | **FAIL**: sentinel shown as data; `now()` arithmetic |
| T27 README | README.md | — | — | — | describes "last 30 days" for T02, whose query uses 90 |
| (parameter) `selected_facility` source | — | dim_facility | V1 | 350 names | V1 estate (V2 has 150) |

**Count:** 24 tile queries. 23 read V1 only, T08 mixes V1 with V2, and T13 reads V2 only. None reads Planetary_computer_LH.

### B3. §13 test: never fabricate scientific certainty; distinguish measurement from estimate

Pattern counts over the raw query JSON (`b_dash/RealTimeDashboard.json`):

| pattern | count |
|---|---|
| `rand(` | 0 |
| `cumsum` / `row_cumsum` | 0 |
| `now()` | 12 |
| `_scale` | 10 |
| `/ 100` | 9 |
| `9999` | 2 |
| `datatable` | 2 |
| `19.26` | 1 |
| `12000` | 1 |

A scan for `//` lines containing query operators found **no commented-out logic**.

| test | finding | tile | offending line(s), verbatim |
|---|---|---|---|
| `rand()` / randomised aggregate | **none** | — | — |
| computed value discarded or replaced by a literal | a missing health measurement becomes a fabricated 0.7 | T09 | `health = coalesce(health, 0.7)` |
| | a "no data" row is rendered as a measured 0.0 kg/hr rate | T16 | `(datatable(plume_id:string, rate_kg_hr:real, confidence:real, detect_ts:datetime, anchor_distance_km:real)` / `["no-data", 0.0, 0.0, datetime(null), 0.0])` (the following `where isnotempty(plume_id)` keeps it, because "no-data" is not empty) |
| | a missing maintenance record becomes 9,999 days, which then flows into `avg_days_overdue` and `overdue_days` | T20, T26 | `\| extend days_since = case(isnotnull(last_ts),` / `datetime_diff('day', now(), last_ts),` / `9999)` |
| magic constants applied to a count or delta | the open work-order count is divided by 100 | T06 | `\| project total = toint(raw_total / 100), breached = toint(raw_breached / 100)` |
| | the violation counts are divided by 500 | T07 | `\| extend _scale = 500.0` / `\| project total = toint(raw_total / _scale),` |
| | currency is divided by a tuning constant | T04 | `let _scale = 1000.0;  // adjust until numbers look realistic` |
| | target and threshold lines with no source | T10, T23 | `\| extend target_kg = 12000.0` (T10 query); `"horizontalLines": [{… "value": 50000}]` (T10 visual); `"value": 4500` (T23 visual) |
| real logic commented out | **none found** | — | — |
| unit error: divisor inconsistent with label | USD ÷ 1,000 ÷ 1,000,000 labelled "M", so **$5.13 B is shown as "5.13M"** | T04 | `p10_raw = sum(total_financial_impact_usd_p10) / _scale,` … `p10 = strcat(tostring(round(p10_raw / 1000000, 2)), "M"),` |
| unit or semantics: percentiles summed | sum of per-event P10/P50/P90 presented as the portfolio's P10/P50/P90 (percentiles do not add) | T04, T11 | `p50_raw = sum(total_financial_impact_usd_p50) / _scale,` · `\| summarize p50 = sum(total_financial_impact_usd_p50) by facility_sk` |
| unit or semantics: mismatched periods | the emissions/production ratio covers different date spans (B2) | T03 | `\| extend intensity_pct = iff(prod_mcf > 0, round((emissions_kg / 19.26) / prod_mcf * 100.0, 3), real(0))` (19.26 is an unlabelled kg-per-Mcf conversion) |
| bin inconsistent with label | the series is named `week` but binned by day; the README says "week-over-week" | T24 (and T12) | `by week = startofday(detect_ts)` |
| cumsum carrying a rate unit | **none** (`cumsum` / `row_cumsum` = 0) | — | — |
| `now()`-dependence that changes a figure on refresh | the figure drifts or empties with wall-clock time against data that stopped on 2026-07-09 | T02, T08, T13, T16, T18, T19 (`ago()`); T20, T26 (`datetime_diff('day', now(), …)`); `now()` as `_end` in T03, T04, T10, T11, T12, T22, T23, T24 | T02 `\| where snapshot_date between (ago(90d) .. now())`; T19 `\| where last_reading < ago(8h)`; T26 `\| extend age_yrs = round(todouble(datetime_diff('day', now(), install_date)) / 365.0, 1),` |
| two series, same aggregation, different names or answers | two KPI cards both titled **"Open Work Orders"** run the same `count()` of `status != "Closed"` and show **290** (T06) and **29,023** (T17) | T06, T17 | T06 `\| summarize raw_total = count(), …` → `toint(raw_total / 100)`; T17 `\| summarize open_tickets = count(),` |
| | T12 and T24 compute `lost_gas` and `lost_gas_log` from one aggregation, but `yColumns ["lost_gas","nsps_fines"]` plots only the raw pair, so the log series is not plotted twice | T12, T24 | `\| extend lost_gas_log = log10(lost_gas + 1),` |
| label inconsistent with query (not in the list, but measurement vs claim) | "Active … Now" computed over six years | T05, T15 | `\| where detect_ts > ago(2200d)` · `\| where reading_ts > ago(2200d)` |
| | `critical` filter can never match | T17 | `critical = countif(priority == "Critical"),` (live: `fact_work_order` priorities are `P1`, `P2`, `P3`) |

### B4. Parameters and time filters

There is one parameter, verbatim from `parameters[0]`:
- `variableName "selected_facility"`, `displayName "Selected Facility "`
- `selectionType "array"`, `includeAllOption true`, `defaultValue {"kind": "all"}`
- source: query `12a7d6b5-…` (`external_table('dim_facility') | where is_current == true | distinct facility_name`)
- `showOnPages {"kind": "all"}`

T11's drillthrough sets it, pairing `facility_name` to the Facility Operations page.

| tile | page | usedVariables | references `selected_facility` in text | `_facility_sk` hard-coded `dynamic([])` | ago() spans | fixed datetimes | now() |
|---|---|---|---|---|---|---|---|
| T01 (markdown) | Executive Overview | — | — | — | — | — | — |
| T02 Total Ch4 Emissions (t) | Executive Overview | `[]` | no | yes | ago(180d), ago(90d) | — | 1 |
| T03 Methane Intensity % | Executive Overview | `[]` | no | yes | — | datetime(2025-05-10T00:00:00Z) | 1 |
| T04 Financial Impact (Fines) | Executive Overview | `[]` | no | yes | — | datetime(2026-05-01) | 1 |
| T05 Active Plumes | Executive Overview | `[]` | no | yes | ago(2200d) | — | 0 |
| T06 Open Work Orders | Executive Overview | `[]` | no | yes | — | — | 0 |
| T07 Compliance Violations | Executive Overview | `[]` | no | yes | — | datetime(2026-06-10), datetime(2026-07-09) | 0 |
| T08 Detections Table | Executive Overview | `[]` | no | no | ago(90d) | — | 0 |
| T09 Facility Map | Executive Overview | `[]` | no | yes | ago(2200d) | — | 0 |
| T10 Daily Emissions Trend | Executive Overview | `[]` | no | yes | — | datetime(2026-05-01) | 1 |
| T11 Top 10 Facilities by P50 | Executive Overview | `[]` | no | yes | — | datetime(2020-05-01) | 1 |
| T12 Financial Impact Composition | Executive Overview | `[]` | no | yes | — | datetime(2024-03-01T00:00:00Z) | 1 |
| T13 Emission Signatures Breakdown | Executive Overview | `[]` | no | no | ago(90d) | — | 0 |
| T14 (markdown) | Facility Operations | — | — | — | — | — | — |
| T15 Active Exceedances | Facility Operations | `['selected_facility']` | yes | yes | ago(2200d) | — | 0 |
| T16 Highest Active Emission Rate | Facility Operations | `['selected_facility']` | yes | no | ago(7d) | — | 0 |
| T17 Open Work Orders | Facility Operations | `['selected_facility']` | yes | yes | — | — | 0 |
| T18 Plume Detected (D-7) | Facility Operations | `['selected_facility']` | yes | no | ago(7d) | — | 0 |
| T19 Sensors Offline | Facility Operations | `['selected_facility']` | yes | yes | ago(2200d), ago(8h) | — | 0 |
| T20 Overdue PM Count | Facility Operations | `['selected_facility']` | yes | yes | — | — | 1 |
| T21 LDAR Leaks Repaired | Facility Operations | `['selected_facility']` | yes | yes | ago(2200d) | — | 0 |
| T22 Compliance Heatmap | Facility Operations | `['selected_facility']` | yes | yes | — | datetime(2025-05-10T00:00:00Z) | 1 |
| T23 Emissions Trend | Facility Operations | `['selected_facility']` | yes | yes | — | datetime(2026-05-01) | 1 |
| T24 Financial Loss Breakdown by Facility | Facility Operations | `['selected_facility']` | yes | yes | — | datetime(2024-03-01T00:00:00Z) | 1 |
| T25 Work Order Trail | Facility Operations | `['selected_facility']` | yes | no | — | — | 0 |
| T26 Top 20 High-Risk Equipment | Facility Operations | `['selected_facility']` | yes | yes | — | — | 2 |
| T27 (markdown) | README.md | — | — | — | — | — | — |

- **`selected_facility` is referenced by all 12 Facility Operations tiles (T15–T26) and by none of the 12 Executive
  Overview query tiles (T02–T13).**
- **There is no time-range parameter on this board at all.** There is no `_startTime` / `_endTime` and no
  time-range kind in `parameters`, so the question of a global time filter does not arise: there is none, and every
  window is hard-coded.
- **`_facility_sk` is inert everywhere.** Every query that declares it binds it to `dynamic([])`, so
  `array_length(_facility_sk) == 0` is always true.

Every hard-coded window, across all tiles:
- `ago()` spans: `ago(7d)`, `ago(8h)`, `ago(90d)`, `ago(180d)` and `ago(2200d)`
- fixed datetimes: `2020-05-01`, `2024-03-01`, `2025-05-10`, `2026-05-01`, and `2026-06-10 .. 2026-07-09` (T07, a closed window)
- `±30d` around a plume in T25

### B5. External dependencies

There is one external host: `https://image2url.com/r2/default/images/1772774090163-e8f21834-9a7d-4392-97ef-64e0d4979f8b.png`. It is the banner
image in the `markdownText` of T01 (Executive Overview) and T14 (Facility Operations). `http` occurs **0 times**
in the 25 query texts, and the README tile (T27) has no links. `embeddedApps` is `[]`.

### B6. `hourly_trends()` / `facility_metrics`

`hourly_trends` and `facility_metrics` each occur **0 times** in the board JSON. `.show functions | count` on Operations_KQL returns
`0`. No tile depends on a function, and all 25 queries executed without error (B2), so **no tile is broken by a missing function.**

---

## C. Attribution coordinates

V2 tables were read from their current Delta snapshots (active files only) with
`python c_read.py gold_plume_catalog dim_facility`:
- `gold_plume_catalog`: version 24, 8 files, 49 rows
- `dim_facility`: version 8, 8 files, 150 rows

### C1. All 49 plumes

| plume_id | source_lat | source_lon | attributed_facility_id | attributed_facility_lat | attributed_facility_lon | attribution_distance_km | attribution_probability | attribution_method | facilities_in_range |
|---|---|---|---|---|---|---|---|---|---|
| PL-f8ec764f9cdf | 31.649036 | -103.779091 | GS-0001 | 31.730573 | -103.808537 | 9.485 | 0.136754 | wind_aware | 35 |
| PL-3d0e59779a8a | 33.026886 | -101.886978 | GS-0010 | 33.283456 | -101.942899 | 29.000 | 0.871238 | wind_aware | 21 |
| PL-bd75ec23b1ae | 33.172230 | -101.464928 | GS-0010 | 33.283456 | -101.942899 | 46.146 | 0.957988 | wind_aware | 5 |
| PL-881dbf57ee04 | 32.208500 | -103.099640 | GS-0012 | 32.407115 | -103.428969 | 38.022 | 0.995334 | wind_aware | 13 |
| PL-b8b3b93d5e01 | 32.062107 | -101.963158 | GS-0021 | 32.152024 | -101.986994 | 10.247 | 0.465901 | wind_aware | 31 |
| PL-da0edd6bd41e | 31.652498 | -101.713638 | GS-0024 | 31.871247 | -101.852556 | 27.643 | 0.569680 | wind_aware | 9 |
| PL-290605ca1745 | 31.475649 | -102.964966 | GS-0026 | 31.517259 | -102.980892 | 4.867 | 0.780336 | wind_aware | 13 |
| PL-fba050bced02 | 31.432585 | -102.983925 | GS-0026 | 31.517259 | -102.980892 | 9.420 | 0.750132 | wind_aware | 11 |
| PL-8a46231dbd67 | 31.790560 | -104.844002 | GS-0029 | 31.773968 | -104.411258 | 40.945 | 0.881932 | wind_aware | 3 |
| PL-772b132551d7 | 31.851713 | -101.535744 | GS-0030 | 31.949403 | -101.402826 | 16.596 | 0.645575 | wind_aware | 10 |
| PL-f19b099ff53b | 31.664230 | -102.472641 | GS-0035 | 31.781889 | -102.465601 | 13.100 | 0.504787 | wind_aware | 12 |
| PL-ee07d05aa443 | 30.537388 | -101.728409 | GS-0038 | 30.269622 | -102.063310 | 43.796 | 1.000000 | wind_aware | 1 |
| PL-fc6e6c832a11 | 30.649481 | -102.292068 | GS-0038 | 30.269622 | -102.063310 | 47.590 | 1.000000 | wind_aware | 1 |
| PL-094e1a99c910 | 31.397202 | -102.105141 | GS-0046 | 31.680003 | -102.243203 | 34.059 | 0.368654 | wind_aware | 8 |
| PL-d95d056e7d0c | 31.780010 | -102.197464 | GS-0049 | 32.017960 | -102.251072 | 26.938 | 0.115016 | wind_aware | 33 |
| PL-4aa750c5e496 | 31.021425 | -101.263451 | GS-0051 | 30.800633 | -100.927330 | 40.386 | 1.000000 | wind_aware | 1 |
| PL-73b184bbb246 | 31.339560 | -104.409447 | GS-0052 | 31.482313 | -104.497053 | 17.919 | 0.618756 | wind_aware | 10 |
| PL-8640819172c0 | 31.937750 | -101.576721 | GS-0058 | 32.031356 | -101.734666 | 18.173 | 0.846986 | wind_aware | 17 |
| PL-9af924cd9b69 | 31.762157 | -101.656937 | GS-0058 | 32.031356 | -101.734666 | 30.820 | 0.492060 | wind_aware | 12 |
| PL-63d41dc780bc | 31.756464 | -103.085976 | GS-0063 | 31.877175 | -103.015521 | 14.983 | 0.344716 | wind_aware | 24 |
| PL-5e5dc7a6da41 | 31.670746 | -103.308884 | GS-0070 | 31.840904 | -103.273116 | 19.221 | 0.426924 | wind_aware | 38 |
| PL-88f5ee2613f7 | 31.696476 | -103.207626 | GS-0070 | 31.840904 | -103.273116 | 17.212 | 0.335646 | wind_aware | 34 |
| PL-f52a4cb34f9a | 31.629128 | -103.272980 | GS-0070 | 31.840904 | -103.273116 | 23.548 | 0.412887 | wind_aware | 34 |
| PL-fd527423a15b | 31.785793 | -103.281776 | GS-0070 | 31.840904 | -103.273116 | 6.182 | 0.343284 | wind_aware | 36 |
| PL-2c80a662469b | 32.177906 | -103.856079 | GS-0071 | 32.409211 | -103.680555 | 30.557 | 0.496196 | wind_aware | 25 |
| PL-d6024ee3f97a | 30.652147 | -104.908104 | GS-0072 | 30.382275 | -104.863075 | 30.317 | 1.000000 | wind_aware | 1 |
| PL-d7f86d52a89e | 30.576637 | -104.616852 | GS-0072 | 30.382275 | -104.863075 | 31.997 | 1.000000 | wind_aware | 1 |
| PL-c0958f29f551 | 32.009956 | -102.024414 | GS-0078 | 32.108896 | -102.011364 | 11.070 | 0.177763 | wind_aware | 34 |
| PL-4891ab2feb97 | 31.059433 | -102.994675 | GS-0100 | 31.370737 | -102.879607 | 36.304 | 0.650167 | wind_aware | 3 |
| PL-dd9cfda8fa72 | 32.590622 | -101.704437 | GS-0107 | 32.625784 | -101.756537 | 6.253 | 0.472048 | wind_aware | 15 |
| PL-634b8c756084 | 32.624844 | -102.465546 | GS-0109 | 32.757682 | -102.484083 | 14.872 | 0.700729 | wind_aware | 24 |
| PL-cecdbc4553e4 | 32.337898 | -101.918732 | GS-0111 | 32.381481 | -101.927347 | 4.913 | 0.676743 | wind_aware | 32 |
| PL-875fc7e65934 | 31.922501 | -102.114021 | GS-0118 | 31.990453 | -102.097982 | 7.706 | 0.117310 | wind_aware | 36 |
| PL-b823142179bc | 31.932240 | -102.123848 | GS-0118 | 31.990453 | -102.097982 | 6.918 | 0.101865 | wind_aware | 37 |
| PL-771b55350b6c | 31.911268 | -102.534004 | GS-0119 | 31.992211 | -102.503351 | 9.454 | 0.779418 | wind_aware | 32 |
| PL-140fdf382e6b | 31.950806 | -104.998962 | GS-0125 | 31.658721 | -104.656961 | 45.819 | 1.000000 | wind_aware | 1 |
| PL-3df7b6aaabdf | 30.527586 | -103.950340 | GS-0127 | 30.955962 | -104.036924 | 48.347 | 0.999442 | wind_aware | 2 |
| PL-8e95c5889a52 | 30.526812 | -104.064163 | GS-0127 | 30.955962 | -104.036924 | 47.790 | 0.999973 | wind_aware | 2 |
| PL-62f209476b54 | 33.453365 | -102.555901 | GS-0133 | 33.170424 | -102.440712 | 33.233 | 1.000000 | wind_aware | 1 |
| PL-84d936fc5c16 | 31.379559 | -103.727058 | GS-0144 | 31.484856 | -103.656287 | 13.497 | 0.370525 | wind_aware | 26 |
| PL-e7c7f1b2ad9d | 31.018827 | -101.291313 | GS-0145 | 31.369337 | -101.606114 | 49.149 | 0.998527 | wind_aware | 2 |
| PL-7f0d68b5329a | 32.937595 | -101.801834 | GS-0150 | 33.006423 | -101.813762 | 7.734 | 0.911668 | wind_aware | 21 |
| PL-19aafb474281 | 33.413383 | -101.004578 |  |  |  |  |  | no_match | 0 |
| PL-38ec1f72dd88 | 30.681507 | -103.174904 |  |  |  |  |  | no_match | 0 |
| PL-3965e3d50edb | 30.980736 | -104.702888 |  |  |  |  |  | no_match | 0 |
| PL-561a8947191b | 33.327412 | -101.054245 |  |  |  |  |  | no_match | 0 |
| PL-a336625b917d | 33.191467 | -104.566528 |  |  |  |  |  | no_match | 0 |
| PL-c39055b1ca19 | 30.919605 | -104.825859 |  |  |  |  |  | no_match | 0 |
| PL-f1714a3e7a5e | 33.469746 | -101.200371 |  |  |  |  |  | no_match | 0 |

**The sampled values are `attributed_facility_*`, not `source_*`:**
- GS-0072 `30.382275 / -104.863075`
- GS-0035 `31.781889 / -102.465601`
- GS-0038 `30.269622 / -102.063310`

No plume's `source_lat/source_lon` pair takes any of those values (checked to 1e-4°). There are 42 attributed plumes; the other 7 have `no_match` and `facilities_in_range 0`. Every
attribution method is `wind_aware`.

### C2. Join integrity

All 42 `attributed_facility_id` values are found in `dim_facility`, with one row per `facility_id` and `is_current`
true for all rows.
```
C2 exact-equal lat: 42 lon: 42 | max |dlat| 0.0 max |dlon| 0.0
C2 name equal: 42
```
**No mismatches.** The coordinates are copied exactly from `dim_facility`.

### C3. Haversine recompute

The distance is the haversine (R = 6371.0 km, the same constant as 05's `haversine_km`) from `source_lat/lon` to `dim_facility.facility_lat/lon`:
```
C3 max abs diff km: 7.815970093361102e-13 | mean abs 2.5169284645883075e-13
{'plume_id': 'PL-bd75ec23b1ae', 'attribution_distance_km': 46.14642101964218, 'hav_km': 46.146421019642965, 'diff': 7.815970093361102e-13}
C3 max stored distance 49.14910455056428
```
The stored distances are exact. All are within `attribution_search_radius_km = 50` (`00_config`, line 93). 05's line 154 comment
still says `# 30 km`, which is stale.

### C4. Estate geography

| statistic | facility_lat | facility_lon |
|---|---|---|
| min | 30.2696 | −105.1802 |
| p1 | 30.3064 | −104.7621 |
| p5 | 31.2827 | −104.2589 |
| p25 | 31.6834 | −103.6709 |
| median | 31.9913 | −102.4937 |
| p75 | 32.3815 | −102.0979 |
| p95 | 32.9243 | −101.7445 |
| p99 | 33.2281 | −101.1707 |
| max | 33.7379 | −100.9273 |

The BBOX (`00_config`, lines 35–40) is lat 30.5..33.5 and lon −105.0..−101.0.

| sub_basin | inside | perimeter | outside | total |
|---|---|---|---|---|
| Central Basin Platform | 8 | 2 | 0 | 10 |
| Delaware Basin | 43 | 14 | 4 | 61 |
| Midland Basin | 43 | 9 | 2 | 54 |
| Northwest Shelf | 19 | 5 | 1 | 25 |
| **all** | **113** | **30** | **7** | **150** |

`bbox_excursion_deg`: 143 rows are `0.0`. The 7 nonzero rows have min 0.0727, median 0.1802 and max **0.2379**.
`in_detection_bbox = false`: **7**.

**Distance from the anchors.** The configured radii are in degrees (`01_topology_config`, lines 204–236): 0.55 region / 0.85
perimeter by default, and 0.37 / 0.57 for Northwest Shelf. `sample_facility_location` (config lines 1536–1545) applies them as
`r` in degrees of latitude, with the longitude offset divided by `cos(anchor_lat)`. So the comparable distance is
`sqrt(dlat² + (dlon·cos(anchor_lat))²)`.

| band | n | within own region radius | within own perimeter radius | max r (deg) | max km from own anchor |
|---|---|---|---|---|---|
| inside | 113 | 113 | 113 | 0.544 | 60.5 |
| perimeter | 30 | 0 | 30 | 0.849 | 94.3 |
| outside | 7 | 0 | **0** | 2.079 | 230.2 |

**7 facilities lie beyond their own anchor's perimeter radius, and the same 7 lie beyond *every* anchor's perimeter.** All 7
are `region_band = outside`:

| facility_id | facility_name | sub_basin (nearest anchor) | lat | lon | bbox_excursion_deg | km from anchor |
|---|---|---|---|---|---|---|
| GS-0068 | Pecos Tank Battery | Northwest Shelf | 33.7379 | −104.4656 | 0.2379 | 230.2 |
| GS-0038 | Monahans Tank Battery | Midland Basin | 30.2696 | −102.0633 | 0.2304 | 198.0 |
| GS-0072 | Kermit Tank Battery 4 | Delaware Basin | 30.3823 | −104.8631 | 0.1177 | 188.1 |
| GS-0051 | Odessa Tank Battery | Midland Basin | 30.8006 | −100.9273 | 0.0727 | 178.0 |
| GS-0134 | Reeves County Compression Station 2 | Delaware Basin | 32.6782 | −105.1802 | 0.1802 | 173.3 |
| GS-0044 | Kermit Gathering Hub | Delaware Basin | 30.2840 | −103.5009 | 0.2160 | 164.1 |
| GS-0132 | Crane Treating Facility | Delaware Basin | 30.3296 | −104.1424 | 0.1704 | 163.5 |

**Is this the V1 out-of-basin failure? No.** The repo states the configuration and code, and the data above matches them:
- The `outside` band is a quota: `FACILITY_SPLIT = {"inside": 0.75, "perimeter": 0.20, "outside": 0.05}` (config line 253). 7/150 = 4.7 %.
- It is placed relative to the BBOX edge, `OUTSIDE_MIN_DEG = 0.05` .. `OUTSIDE_MAX_DEG = 0.30` (lines 255–256). The observed maximum is 0.2379.
- It is placed on a randomly chosen edge (`edge = str(rng.choice(["north", "south", "east", "west"]))`, line 1524). Here that gives 4 south, 1 north, 1 east and 1 west.
- Its sub_basin is `nearest_anchor()`, not a sampling anchor.

The config describes the V1 defect differently (lines 47–48 and 166–168): an *anchor* at lat 30.2, below the BBOX floor, that "carried 40% of the estate".
Every V2 anchor is inside the BBOX, and all 143 inside/perimeter facilities are within their radii.

**Still relevant before any map tile.** 5 of the 42 attributed plumes are attributed to `outside` facilities. All 5 have probability
`1.0`, because each was the only facility in range:

| plume_id | source_lat | source_lon | facility_id | distance_km | probability | facilities_in_range |
|---|---|---|---|---|---|---|
| PL-ee07d05aa443 | 30.537388 | −101.728409 | GS-0038 | 43.796 | 1.0 | 1 |
| PL-d6024ee3f97a | 30.652147 | −104.908104 | GS-0072 | 30.317 | 1.0 | 1 |
| PL-4aa750c5e496 | 31.021425 | −101.263451 | GS-0051 | 40.386 | 1.0 | 1 |
| PL-d7f86d52a89e | 30.576637 | −104.616852 | GS-0072 | 31.997 | 1.0 | 1 |
| PL-fc6e6c832a11 | 30.649481 | −102.292068 | GS-0038 | 47.590 | 1.0 | 1 |

The config says the band exists "to exercise attribution confidence tiers and the NO_FACILITY_IN_RANGE path in 05"
(lines 245–246). Observed, it also produces the estate's only certainty-1.0 attributions at 30–48 km. That is
a reading for the owner to decide on; this report does not judge it.

### C5. Full `dim_facility` rows

| column | GS-0035 | GS-0038 | GS-0072 |
|---|---|---|---|
| facility_sk | 35 | 38 | 72 |
| facility_name | Imperial Collection Facility 2 | Monahans Tank Battery | Kermit Tank Battery 4 |
| name_descriptor | Collection Facility | Tank Battery | Tank Battery |
| facility_type | Gathering System | Tank Battery | Tank Battery |
| operator_sk | 9 | 1 | 6 |
| sub_basin | Midland Basin | Midland Basin | Delaware Basin |
| region_band | inside | **outside** | **outside** |
| facility_lat | 31.781889 | 30.269622 | 30.382275 |
| facility_lon | −102.465601 | −102.06331 | −104.863075 |
| bbox_excursion_deg | 0.0 | 0.230378 | 0.117725 |
| country | USA | USA | USA |
| state | TX | TX | TX |
| commission_date | 2015-10-10 00:00:00+00:00 | 2021-09-20 00:00:00+00:00 | 2012-07-23 00:00:00+00:00 |
| active_flag | True | True | True |
| in_detection_bbox | True | **False** | **False** |
| operator_name | Operator 09 | Operator 01 | Operator 06 |
| effective_from | 2026-09-15 00:00:00+00:00 | 2026-09-15 00:00:00+00:00 | 2026-09-15 00:00:00+00:00 |
| effective_to | 2999-12-31 00:00:00+00:00 | 2999-12-31 00:00:00+00:00 | 2999-12-31 00:00:00+00:00 |
| is_current | True | True | True |
| topology_seed | 20260915 | 20260915 | 20260915 |

GS-0035 is an ordinary inside-band facility, 45.6 km from the Midland Basin anchor (r = 0.410° against a 0.55° region radius).
Only its name ("Imperial") suggests anything about geography (see C6).

### C6. Where `facility_name` is composed

`01_topology/01a_build_facility_topology.Notebook/notebook-content.py`, lines 117–131:
```python
for i, band in enumerate(bands, start=1):
    lat, lon, anchor = sample_facility_location(frng, band)

    # Type FIRST -- the name is derived from it, so the two cannot disagree.
    ftype = str(frng.choice(
        FACILITY_TYPES, p=[FACILITY_TYPE_WEIGHTS[t] for t in FACILITY_TYPES]
    ))
    descriptor = str(frng.choice(TYPE_DESCRIPTORS[ftype]))
    place = str(frng.choice(PLACES))

    name = f"{place} {descriptor}"
    suffix = 2
    while name in used_names:
        name = f"{place} {descriptor} {suffix}"
        suffix += 1
```
**The town is sampled independently of the coordinates.** `place = str(frng.choice(PLACES))` is a uniform draw
from 20 names, and `lat` and `lon` never enter it. Only the *descriptor* is coupled, and it is coupled to the *type*.

The live data shows the result. Place names were parsed from `facility_name` against `PLACES` (config line 370):
```
Kermit    n=11 lat 30.28..32.90 lon -104.86..-101.87 sub_basins={'Delaware Basin': 5, 'Northwest Shelf': 3, 'Midland Basin': 2, 'Central Basin Platform': 1}
Monahans  n=10 lat 30.27..32.95 lon -103.66..-102.01 sub_basins={'Delaware Basin': 4, 'Midland Basin': 4, 'Northwest Shelf': 2}
Midland   n= 3 lat 31.56..31.75 lon -103.86..-101.83 sub_basins={'Midland Basin': 2, 'Delaware Basin': 1}
Pecos     n=10 lat 30.96..33.74 lon -104.47..-101.63 sub_basins={'Delaware Basin': 6, 'Northwest Shelf': 2, 'Midland Basin': 1, 'Central Basin Platform': 1}
places spanning >=3 sub_basins: 14 of 20
```
**The names are decorative and must not be read as geography.** "Kermit Tank Battery 4" sitting 190 km from its anchor tells you nothing
about Kermit.

**This is deliberate.** `01_topology_config`, lines 64–67:
> What is kept from V1: the name generator. `PLACES` and the descriptor vocabulary are reused unchanged — they produce
> realistic Permian names like "Reeves County Tank Battery". The demo requires synthetic identity, so names stay
> invented; only the geography has to be plausible.

---

## Appendix: method and scripts

All the scripts are in the session scratchpad
(`C:\Users\PRUTHA~1.ANN\AppData\Local\Temp\claude\…\scratchpad\`). They are not in the repo, and they were not committed. Each one only reads.

| script | does | used in |
|---|---|---|
| `delta_audit.py <lakehouse_id> <Tables/…> <out.json>` | recursive DFS listing (paged with `x-ms-continuation`); `_delta_log` replay (from the latest checkpoint when early JSONs were cleaned up); per-file footer `num_rows` with HTTP range reads; active/tombstoned classification; history with `commitInfo`. Retries transient errors 6× | A1, A4, A5 |
| `kql.py` / `kqlfull.py <cluster> <db> <csl>` | Kusto REST `/v1/rest/query` (or `/v1/rest/mgmt` for `.` commands), token for `https://kusto.kusto.windows.net`; `kqlfull` prints untruncated cells | A2, A3, A4, B2, B6 |
| `a4_sweep.py` | for each `.show external tables` row: `\| count`, `\| summarize c=count()`, `.show external table X details` | A4 |
| `a6_history.py <out.json>` | every retained commit JSON of every V2 table: operations, VACUUM, `metaData.schemaString` diffs | A5, A6 |
| `fab.py` | Fabric REST helper, used for `GET …/shortcuts`, `GET …/items?type=KQLDashboard` and `POST …/getDefinition` (LRO) | A3, B |
| `b_run.py` | executes each stored board query read-only, binding `['selected_facility']` to its default `dynamic([])` with `declare query_parameters` | B2 |
| `c_read.py <table…>` | reads a V2 table's current snapshot (active files only) into local Parquet | C1–C6 |
| ad-hoc pandas (shown inline in C) | join, haversine (R = 6371.0), quantiles, crosstab, radius test | C2–C6 |

Nothing was written to any Fabric item, OneLake path or the repo, except this file. Not done in this round, as
the brief instructed: no shortcut creation, no SQL endpoint metadata refresh, no board rename or unshare, no VACUUM,
no notebook or pipeline runs, and no commits.
