# V2 RTI serving namespace — 2026-10-14

Fabric workspace `640876ea-6158-4ffd-8598-5eb210e088a0` (Green Sky - Dev). Created via Fabric REST (fab.py, az CLI token) and Kusto REST (kql.py). None of this lives in Git.

**Status: INCOMPLETE — stopped after step 3.** The four OneLake shortcuts were accepted by the Fabric API (201) but never surfaced as external tables in the KQL engine. Steps 4 (acceleration policy) and verification a–f could not run.

## What was created

| Item | Name | ID | Notes |
|---|---|---|---|
| Eventhouse | GreenSky_V2_EH | `aa842896-5385-40b0-ad0b-d40424ce2a9d` | owner: prutha.annadate |
| KQL database (auto-created default) | GreenSky_V2_EH | `a1afdbe0-9d0c-4f7e-a04b-e8121124cba3` | created by Fabric with the eventhouse; **not used**, left in place |
| KQL database | greensky_v2_rti | `de7dfd60-0593-4e7c-b654-07065f5a2d6c` | ReadWrite, parent `aa842896…` |
| Shortcut | gold_plume_catalog | in greensky_v2_rti, path `/Tables` | → greensky_v2_lakehouse `5d5c8002-789e-4319-81d1-a60f08a77996` `Tables/dbo/gold_plume_catalog` |
| Shortcut | fact_facility_daily_snapshot | same | → `Tables/dbo/fact_facility_daily_snapshot` |
| Shortcut | dim_facility | same | → `Tables/dbo/dim_facility` |
| Shortcut | scada_telemetry_hourly | same | → `Tables/dbo/scada_telemetry_hourly` |

- Query URI: `https://trd-1rwzmf8wysaj2u0zj5.z4.kusto.fabric.microsoft.com`
- Ingestion URI: `https://ingest-trd-1rwzmf8wysaj2u0zj5.z4.kusto.fabric.microsoft.com`
- Kusto database names are the item GUIDs; PrettyName carries the display name.

## Call log (UTC)

```
08:51:08Z POST C:/Users/.../Git/workspaces/640876ea-.../eventhouses -> HTTP 404   (malformed URL: Git Bash path conversion; request never reached the eventhouses endpoint)
08:51:32Z POST /workspaces/640876ea-.../eventhouses -> 201                           GreenSky_V2_EH created
08:51:52Z GET  /workspaces/640876ea-.../eventhouses/aa842896-... -> 200
08:52:00Z GET  /workspaces/640876ea-.../kqlDatabases -> 200                          default DB a1afdbe0 present
08:52:15Z POST /workspaces/640876ea-.../kqlDatabases -> 202                          greensky_v2_rti (LRO a40f00c7-24c7-4880-9345-325530961cfa)
08:52:29Z GET  .../operations/a40f00c7-... -> 200 (Succeeded)
08:52:38Z GET  .../operations/a40f00c7-.../result -> 200                             id de7dfd60-...
08:53:03Z POST /workspaces/.../items/de7dfd60-.../shortcuts -> 201                   gold_plume_catalog
08:53:14Z POST /workspaces/.../items/de7dfd60-.../shortcuts -> 201                   fact_facility_daily_snapshot
08:53:23Z POST /workspaces/.../items/de7dfd60-.../shortcuts -> 201                   dim_facility
08:53:32Z POST /workspaces/.../items/de7dfd60-.../shortcuts -> 201                   scada_telemetry_hourly
08:54:06Z KQL .show external table gold_plume_catalog policy query_acceleration -> HTTP 400 BadRequest_EntityNotFound
08:54:14Z KQL .show external table fact_facility_daily_snapshot policy query_acceleration -> HTTP 400 BadRequest_EntityNotFound
08:54:22Z KQL .show external table dim_facility policy query_acceleration -> HTTP 400 BadRequest_EntityNotFound
08:54:31Z KQL .show external table scada_telemetry_hourly policy query_acceleration -> HTTP 400 BadRequest_EntityNotFound
08:54:44Z KQL .show databases -> OK (2 rows: a1afdbe0 GreenSky_V2_EH, de7dfd60 greensky_v2_rti)
08:54:52Z KQL .show external tables -> OK (0 rows)
08:55:00Z KQL .show tables -> OK (0 rows)
08:55:10Z GET  /workspaces/.../items/85d85e9c-.../shortcuts -> 200                   Operations_KQL, read-only shape comparison
08:55:19Z GET  /workspaces/.../items/de7dfd60-.../shortcuts -> 200                   all 4 present, path "/Tables"
08:55:41Z–09:01:18Z KQL .show external tables (12 polls, 30 s apart) -> OK, 0 rows every time
09:02:02Z KQL external_table('dim_facility') | count -> HTTP 400 General_BadRequest
09:02:13Z KQL .show database greensky_v2_rti schema as json -> OK, ExternalTables: {}
```

## Verification

- **a.** `.show tables` → 0 rows. `.show external tables` → 0 rows (unchanged across ~6 min of polling).
- **b–e.** Not run: no external tables exist to query. `external_table('dim_facility') | count` → HTTP 400 General_BadRequest.
- **f.** Shortcut objects (GET .../items/de7dfd60-.../shortcuts): all four have `path: /Tables`, target type OneLake, `workspaceId 640876ea-…`, `itemId 5d5c8002-789e-4319-81d1-a60f08a77996` (greensky_v2_lakehouse), `path Tables/dbo/<name>`. None point at Operations_LH.

Database schema as read back:

```json
{"Databases":{"de7dfd60-0593-4e7c-b654-07065f5a2d6c":{"Name":"de7dfd60-0593-4e7c-b654-07065f5a2d6c","Tables":{},"PrettyName":"greensky_v2_rti","MajorVersion":11,"MinorVersion":0,"Functions":{},"DatabaseAccessMode":"ReadWrite","ExternalTables":{},"MaterializedViews":{},"EntityGroups":{},"Graphs":{},"StoredQueryResults":{}}}}
```

## Open issue

Fabric-level shortcut exists; Kusto-level external table does not. The shortcut bodies match the shape of Operations_KQL's working shortcuts. Not attempted (outside the brief): creating the external tables by hand with `.create external table … kind=delta`, or recreating the shortcuts through the portal. Neither was tried; nothing was deleted.

## Follow-up — shortcuts removed (same day)

The portal refused to create dim_facility ("shortcut with same name already exists"): the API shortcuts were registered at `/Tables` but were invisible to the KQL engine and Explorer. The portal wizard writes KQL-database shortcuts to "greensky_v2_rti > Shortcut", a different location. The Create Shortcut REST reference only says `path` must include "Files" or "Tables" and does not document KQL databases or a "Shortcut" location.

```
10:14:51Z GET    .../items/de7dfd60-.../shortcuts -> 200 (4 shortcuts, path "/Tables"; posted value was "Tables")
10:15:19Z DELETE .../items/de7dfd60-.../shortcuts/Tables/gold_plume_catalog -> 200
10:15:29Z DELETE .../items/de7dfd60-.../shortcuts/Tables/fact_facility_daily_snapshot -> 200
10:15:38Z DELETE .../items/de7dfd60-.../shortcuts/Tables/dim_facility -> 200
10:15:48Z DELETE .../items/de7dfd60-.../shortcuts/Tables/scada_telemetry_hourly -> 200
10:15:57Z GET    .../items/de7dfd60-.../shortcuts -> 200, {"value": []}
```

Current state: GreenSky_V2_EH, its default DB and greensky_v2_rti exist; greensky_v2_rti has no shortcuts, tables or external tables.

Not touched: Operations_EH, Operations_KQL (one read-only GET of its shortcut list), GreenSky_Analytics eventhouse/database, the "ARCHIVED - V1 - DO NOT DEMO" dashboard, the "Ch4 Plume Analysis" queryset, greensky_v2_lakehouse.
