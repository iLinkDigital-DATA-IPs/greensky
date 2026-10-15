# Green Sky RTI board V2: build sheet

This sheet is for building the board in the Fabric Real-Time Dashboard editor, in order. It is complete on
its own: every query, position, binding and setting is here.
- **What it records.** It implements `rti_board_design.md`, which holds the reasoning, the rule check (R1–R8)
  and the release record. Measure definitions are in `rti_measure_spec.md`.
- **The queries.** Each one was copied by script from the file that was run, not retyped.
- **The expected outputs.** Each is what that exact query returned read-only against `greensky_v2_rti` on
  2026-10-15, after the enterprise release (gate 42/42).
- **The test harness** added `let _startTime = datetime(2026-08-16); let _endTime = datetime(2026-09-15);`
  and, on the facility page, `let _facility_id = 'GS-0091';`. It inlined B1 as
  `let _snapshot_consistency = (…);`. Do **not** paste any of those `let` lines into the editor. The
  dashboard supplies all of them.

**Status, 2026-10-15.** The board has been built from this sheet through the Fabric REST API:
- **Item:** KQL Dashboard "Green Sky RTI", id `18f4788a-783f-4653-bf5d-df6bb99f714e`.
- **Commit state:** uncommitted.
- **Last update:** short titles and the T11b height fix, applied 08:59 UTC.

This sheet remains the reference for rebuilding the board, or checking it, in the editor.

---

## 1. Before you start

| check | expected | how |
|---|---|---|
| The four external tables exist in `greensky_v2_rti` | `gold_plume_catalog` 41 cols / 50 rows; `fact_facility_daily_snapshot` 27 / 4,500; `dim_facility` 21 / 150; `scada_telemetry_hourly` 30 / 3,265,025 | `external_table('<t>') \| getschema \| summarize count()` and `external_table('<t>') \| summarize count()` |
| Release gate passes | B1 returns `consistent = True`, 42/42 | Run B1 (§3) in a KQL queryset first |
| Do not touch V1 | "ARCHIVED - V1 - DO NOT DEMO" stays as it is | Build a **new** dashboard |

**What the conventions mean in this sheet:**
- **Positions** use V1's grid: 24 columns. `x, y` is the top-left cell; `w × h` is the width and height in
  grid units.
- **Titles** are short and carry no "synthetic" marker. R8 is met in three places:
  - **Page banners:** the T00 and T19 banners state the real/synthetic boundary on each page.
  - **Per-row marking:** the `estate` (or `marker`) column in the tables, and series labels, mark rows and
    series where real and synthetic content mix.
  - **README:** the README page.

  Keep the banners and those columns exactly as given.
- **Minimum tile sizes:** see the table below. Every tile in this sheet meets it, checked by script with no
  overlaps and no gaps on either page.

**Minimum tile sizes (width × height, grid units).** The editor refuses a tile below its visual type's
minimum, with "Current tile size (w, h) is smaller than the minimum supported tile size (…)". These sizes
are not documented anywhere I could find.

| visual | minimum used here | evidence |
|---|---|---|
| Multi Stat | **3 per card column × 3 per card row** of its card grid (`slot`). Grid 3×2 → 9 × 6; 4×1 → 12 × 3; 2×2 → 6 × 6; 1×3 → 3 × 9 | two editor errors (2026-10-15): grid 2×2 at 8×5 → "minimum (6, 6)"; grid 1×3 at 8×6 → "minimum (3, 9)" |
| Table | 5 × 3 | rendered at 5×4 and 8×3 on this board; **failed at 13×2** ("An error occurred") |
| Map | 11 × 7 | smallest seen rendering (V1 and this board) |
| Scatter, Column, Stacked column | 12 × 6 | smallest seen rendering on this board |
| Stacked bar | 11 × 8 | smallest seen rendering on this board |
| Text (markdown) | 24 × 3 | V1 and this board |

The Multi Stat rule is a real limit, inferred from two editor errors that fit it exactly. The others are the smallest sizes seen rendering, not
confirmed floors. Do not go below them without checking in the editor.

**Every tile against its minimum** (checked by script, 2026-10-15 09:43 UTC; no overlaps and no gaps on
either page):

| page | tile | visual | size (w × h) | minimum | margin |
| Executive | T00 | Text (markdown) | 24 × 3 | 24 × 3 | ok (+0, +0) |
| Executive | T00 | Text (markdown) | 24 × 3 | 24 × 3 | ok (+0, +0) |
| Executive | T01 | Table | 16 × 4 | 5 × 3 | ok (+11, +1) |
| Executive | T12 | Table | 8 × 4 | 5 × 3 | ok (+3, +1) |
| Executive | T30 | Multi Stat (grid 3×2) | 24 × 6 | 9 × 6 | ok (+15, +0) |
| Executive | T31 | Table | 12 × 9 | 5 × 3 | ok (+7, +6) |
| Executive | T07 | Map | 12 × 9 | 11 × 7 | ok (+1, +2) |
| Executive | T11a | Table | 24 × 5 | 5 × 3 | ok (+19, +2) |
| Executive | T11b | Table | 24 × 4 | 5 × 3 | ok (+19, +1) |
| Executive | T06 | Stacked column chart | 12 × 6 | 12 × 6 | ok (+0, +0) |
| Executive | T05 | Scatter chart | 12 × 6 | 12 × 6 | ok (+0, +0) |
| Executive | T09 | Table | 13 × 8 | 5 × 3 | ok (+8, +5) |
| Executive | T10 | Stacked bar chart | 11 × 8 | 11 × 8 | ok (+0, +0) |
| Facility | T19 | Text (markdown) | 24 × 3 | 24 × 3 | ok (+0, +0) |
| Facility | T01 | Table | 16 × 4 | 5 × 3 | ok (+11, +1) |
| Facility | T12 | Table | 8 × 4 | 5 × 3 | ok (+3, +1) |
| Facility | T20 | Table | 24 × 3 | 5 × 3 | ok (+19, +0) |
| Facility | T32 | Multi Stat (grid 4×1) | 24 × 4 | 12 × 3 | ok (+12, +1) |
| Facility | T33 | Table | 12 × 8 | 5 × 3 | ok (+7, +5) |
| Facility | T24 | Column chart | 12 × 8 | 12 × 6 | ok (+0, +2) |
| Facility | T22 | Table | 24 × 5 | 5 × 3 | ok (+19, +2) |
| Facility | T25 | Stacked column chart | 24 × 6 | 12 × 6 | ok (+12, +0) |
| Facility | T26 | Table | 24 × 6 | 5 × 3 | ok (+19, +3) |
- **No colour by magnitude anywhere** (Decision A). The only conditional formatting is the consistency gate on
  T12 (required).
- **Two levels on each page:**
  - **Headline cards (Multi Stat):** T30 on Executive Overview (six cards, grid 3 × 2) and T32 on Facility
    Operations (four cards, grid 4 × 1).
  - **Compact tables below them:** T31 (the splits and the R3 reconciliation) and T33 (operations and
    compliance, mostly zeros).
  - **Facility line:** on Facility Operations, the facility is one line (T20) above the headline.
  - **Empty headline values:** a headline card that would show 0 says "none in window".
- **Card labels must read in full.** A Multi Stat card label stays within **5 characters per grid unit of card width** (card width = tile width ÷ grid columns). This is a conservative heuristic, not a measured limit: T30's cards are 8 units wide, so the budget is 40 characters; T32's are 6 units, so 30. If a label will not fit, widen the card (fewer columns) or shorten the label. Never ship a truncated label beside a big number.
  - **Columns.** Each query returns `label` and `value`. The unit is in the **label**, and the value is the
    exact number as text with thousands separators, so the card cannot abbreviate it.
  - **The as-of date** is printed once per page, on T01's board row (R1d as amended 2026-10-15). It also
    appears on tiles whose anchor differs from the page default (T12, T20).
- **Header (T00)** is the Green Sky image, from the link given, plus a one-line caption that states the
  real/synthetic boundary (R8a).

---

## 2. Create the dashboard and the data source

1. In workspace **Green Sky - Dev**, create a new **Real-Time Dashboard**. Suggested name: `Green Sky RTI V2`.
2. **Data source.** Add a data source:
   - Type: **Eventhouse / KQL Database**.
   - Database: **`greensky_v2_rti`** (id `de7dfd60-0593-4e7c-b654-07065f5a2d6c`, in Eventhouse
     `GreenSky_V2_EH`).
   - Query URI: `https://trd-1rwzmf8wysaj2u0zj5.z4.kusto.fabric.microsoft.com`.
   - Every tile and query below uses this source.
3. **Auto refresh:** off. The data is static, because the pipelines are unscheduled (spec Part 5 Q10).
4. **Pages.** Rename the first page to **Executive Overview**. Add **Facility Operations**, then **README**,
   in that order.

---

## 3. Shared base query B1

Add a **base query** (Manage → Base queries → New):
- **Variable name:** `_snapshot_consistency`. It must be exact: seven tiles reference it by this name.
- **Data source:** `greensky_v2_rti`.
- **What it returns.** One row: whether every plume facility-day in the snapshot matches the catalog's
  current attribution. T12 displays it, and T30, T31, T10, T32, T33 and T25 use it to withhold
  attribution-dependent figures if it ever fails.
- **If the editor will not accept B1 as a base query,** paste B1's body into each of those queries as a
  first line, `let _snapshot_consistency = (` … `);`. The results are identical; that is how the queries
  were tested.


```kql
// B1 Base query `_snapshot_consistency` · release gate G1 as one row · as-of: common window [2026-08-16, 2026-09-15), literal (the snapshot's vintage is global, not windowed)
// Compares every plume facility-day in the snapshot with the catalog's current attribution: same plume count, same kg (to 0.01 kg).
// consistent = true only when every facility-day matches. Referenced by T12 and by every gated tile.
external_table('gold_plume_catalog')
| where detection_date >= datetime(2026-08-16) and detection_date < datetime(2026-09-15) and isnotempty(attributed_facility_id)
| summarize cat_n = count(), cat_kg = sum(ime_kg) by facility_id = attributed_facility_id, day = startofday(detection_date)
| join kind=fullouter (
    external_table('fact_facility_daily_snapshot')
    | where snapshot_date >= datetime(2026-08-16) and snapshot_date < datetime(2026-09-15) and (active_plumes > 0 or total_emissions_kg > 0)
    | project facility_id, day = snapshot_date, snap_n = active_plumes, snap_kg = total_emissions_kg) on facility_id, day
| summarize facility_days = count(),
            matched       = countif(isnotnull(cat_n) and isnotnull(snap_n) and cat_n == snap_n and abs(cat_kg - snap_kg) <= 0.01),
            catalog_only  = countif(isnull(snap_n)),
            snapshot_only = countif(isnull(cat_n))
| extend values_differ = facility_days - matched - catalog_only - snapshot_only
| extend consistent = (facility_days == matched)
```

Expected output:

1 rows (defaults):

| facility_days | matched | catalog_only | snapshot_only | values_differ | consistent |
|---|---|---|---|---|---|
| 42 | 42 | 0 | 0 | 0 | True |

---

## 4. Parameters

Create these two parameters, and no others. Every query that names a parameter filters on it (no inert
parameters).

### 4.1 Window

| setting | value |
|---|---|
| kind | **Time range** (the built-in one) |
| variables | `_startTime`, `_endTime` |
| default | **Custom fixed range: 2026-08-16 00:00 → 2026-09-15 00:00 UTC**. Not a relative range ("last N days"): today's date is outside the data, so a relative range selects nothing. |
| show on pages | Executive Overview, Facility Operations |

**How the queries treat the window.**
- **Days.** They snap it to whole UTC days.
- **Cap.** They cap it at 2026-09-15 (exclusive), the enterprise horizon. A range that runs past it is
  capped, and every tile then prints "as of 2026-09-14".

### 4.2 Facility

| setting | value |
|---|---|
| label | `Facility` |
| kind | **Single selection**. No "Select all", no multi-select. |
| variable | `_facility_id` |
| data type | string |
| source | **Query**, data source `greensky_v2_rti`, query P1 below |
| value column | **`facility_id`** (V1 bound on the name; that was wrong) |
| label column | **`facility_label`** (`GS-0091 · Orla Booster Station`: id beside name, Decision B) |
| default | **`GS-0091`** (a literal; rank 1 in the default window) |
| show on pages | **Facility Operations only** |

Query P1:


```kql
// P1 Facility parameter source · value = facility_id, label = "facility_id · facility_name" (Decision B: id beside name) · as-of: topology 2026-09-15 · SYNTHETIC ESTATE
external_table('dim_facility')
| project facility_id, facility_label = strcat(facility_id, " · ", facility_name)
| order by facility_id asc
```

Expected: 150 rows, first `GS-0001 | GS-0001 · Sand Hills Compression Station`.


---

## 5. Page 1: Executive Overview

Create the page **Executive Overview**, then add the tiles in this order.


#### 5.1 · T00 · (no title)

| field | value |
|---|---|
| visual | Text (markdown) |
| position x, y | 0, 0 |
| size w × h | 24 × 3 |
| data source | — |
| parameters used | none |
| conditional formatting | none |
| interactions | none |

**Visual settings.** Content (markdown, exactly):

```
![Green Sky: Real Time Methane Intelligence](https://image2url.com/r2/default/images/1772774090163-e8f21834-9a7d-4392-97ef-64e0d4979f8b.png)

*Real TROPOMI methane detections over the Permian · synthetic operating estate (facilities, attribution, operations, SCADA) · see README*
```


#### 5.2 · T01 · Source freshness

| field | value |
|---|---|
| visual | Table |
| position x, y | 0, 3 |
| size w × h | 16 × 4 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`) |
| conditional formatting | Optional: rows where `estate` == `synthetic`, colour amber (tone only; carries no magnitude). |
| interactions | none |

**Visual settings.** All columns, in query order.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T01 Source freshness · spec "Also settle: Freshness" + R1d · as-of: each source's own max; board end = window end capped at H_ent (last day 2026-09-14)
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let board   = strcat("[", format_datetime(W_start, "yyyy-MM-dd"), ", ", format_datetime(W_end, "yyyy-MM-dd"), ")");
let cat = external_table('gold_plume_catalog');
union
 (cat | summarize t0 = min(detection_date), t1 = max(detection_date), n = count(),
                  h0 = min(detection_date - startofday(detection_date)), h1 = max(detection_date - startofday(detection_date)),
                  after = countif(detection_date >= W_end)
      | project ord = 1, source = "TROPOMI CH4 plumes (gold_plume_catalog.detection_date)", estate = "real",
                as_of_utc = format_datetime(t1, "yyyy-MM-dd HH:mm:ss"),
                coverage = strcat(format_datetime(t0, "yyyy-MM-dd"), " → ", format_datetime(t1, "yyyy-MM-dd"), " · ", n, " plumes · overpasses ",
                                  format_timespan(h0, "HH:mm"), "–", format_timespan(h1, "HH:mm"), " UTC"),
                note = strcat("detection_date = first pixel row of the cluster (overpass time). ", after, " plume(s) after the board end are not shown; no run timestamp is stored.")),
 (cat | summarize n = count(), w = countif(isnotnull(era5_u10) and isnotnull(era5_v10)), fb = countif(t_mix_method != "wind_dependent")
      | project ord = 2, source = "ERA5 10 m wind (gold_plume_catalog.era5_u10/v10)", estate = "real",
                as_of_utc = "matched per plume; not stored",
                coverage = strcat(w, " of ", n, " plumes carry ERA5 u/v; ", fb, " fell back to the fixed 48 h mixing time"),
                note = "Per pixel: nearest ERA5 hour and grid point (03_join_data:421-453). ERA5 comes from a manually downloaded extract that must cover the window (02b:142-180); its file and latency are not in the serving layer."),
 (external_table('fact_facility_daily_snapshot') | summarize t0 = min(snapshot_date), t1 = max(snapshot_date), d = count_distinct(snapshot_date), f = count_distinct(facility_id)
      | project ord = 3, source = "Enterprise daily snapshot (fact_facility_daily_snapshot.snapshot_date)", estate = "synthetic",
                as_of_utc = format_datetime(t1, "yyyy-MM-dd"),
                coverage = strcat(format_datetime(t0, "yyyy-MM-dd"), " → ", format_datetime(t1, "yyyy-MM-dd"), " · ", d, " days × ", f, " facilities"),
                note = "Stocks read at end of the anchor day. Plume-derived columns predate the 05 wind fix and are not shown (see README)."),
 (external_table('scada_telemetry_hourly') | summarize t0 = min(bucket_ts), t1 = max(bucket_ts), f = count_distinct(facility_sk)
      | project ord = 4, source = "SCADA hourly (scada_telemetry_hourly.bucket_ts)", estate = "synthetic",
                as_of_utc = format_datetime(t1, "yyyy-MM-dd HH:mm"),
                coverage = strcat(format_datetime(t0, "yyyy-MM-dd HH:mm"), " → ", format_datetime(t1, "yyyy-MM-dd HH:mm"), " · ", f, " of 150 facilities have rows"),
                note = "An hour with no readings has no row (02c:68-72)."),
 (external_table('dim_facility') | summarize e = max(effective_from), n = count(), cur = countif(is_current), seed = max(topology_seed)
      | project ord = 5, source = "Facility topology (dim_facility.effective_from)", estate = "synthetic",
                as_of_utc = format_datetime(e, "yyyy-MM-dd"),
                coverage = strcat(n, " facilities, ", cur, " current · topology_seed ", seed),
                note = "Invented estate. Facility names are decorative: the town is drawn independently of the coordinates (01a:125)."),
 (print ord = 6, source = "THIS BOARD", estate = "—",
        as_of_utc = format_datetime(W_end - 1d, "yyyy-MM-dd"),
        coverage = strcat("window ", board, " UTC, capped at H_ent 2026-09-15 (exclusive)"),
        note = "Detection runs one day past enterprise/SCADA; every page reads both, so every tile ends at the earlier horizon (R1b).")
| order by ord asc
| project-away ord
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

6 rows (defaults):

| source | estate | as_of_utc | coverage | note |
|---|---|---|---|---|
| TROPOMI CH4 plumes (gold_plume_catalog.detection_date) | real | 2026-09-15 20:17:52 | 2026-08-16 → 2026-09-15 · 50 plumes · overpasses 19:35–20:35 UTC | detection_date = first pixel row of the cluster (overpass time). 1 plume(s) after the board end are not shown; no run timestamp is stored. |
| ERA5 10 m wind (gold_plume_catalog.era5_u10/v10) | real | matched per plume; not stored | 50 of 50 plumes carry ERA5 u/v; 1 fell back to the fixed 48 h mixing time | Per pixel: nearest ERA5 hour and grid point (03_join_data:421-453). ERA5 comes from a manually downloaded extract that must cover the window (02b:142-180); its file and latency are not in the serving layer. |
| Enterprise daily snapshot (fact_facility_daily_snapshot.snapshot_date) | synthetic | 2026-09-14 | 2026-08-16 → 2026-09-14 · 30 days × 150 facilities | Stocks read at end of the anchor day. Plume-derived columns predate the 05 wind fix and are not shown (see README). |
| SCADA hourly (scada_telemetry_hourly.bucket_ts) | synthetic | 2026-09-14 23:00 | 2026-08-16 00:00 → 2026-09-14 23:00 · 147 of 150 facilities have rows | An hour with no readings has no row (02c:68-72). |
| Facility topology (dim_facility.effective_from) | synthetic | 2026-09-15 | 150 facilities, 150 current · topology_seed 20260915 | Invented estate. Facility names are decorative: the town is drawn independently of the coordinates (01a:125). |
| THIS BOARD | — | 2026-09-14 | window [2026-08-16, 2026-09-15) UTC, capped at H_ent 2026-09-15 (exclusive) | Detection runs one day past enterprise/SCADA; every page reads both, so every tile ends at the earlier horizon (R1b). |


#### 5.3 · T12 · Data consistency

| field | value |
|---|---|
| visual | Table |
| position x, y | 16, 3 |
| size w × h | 8 × 4 |
| data source | greensky_v2_rti |
| parameters used | base query `_snapshot_consistency` |
| conditional formatting | **Required.** Rule 1: `gate` == `FAIL` → red, apply to the row. Rule 2: `gate` == `PASS` → green, apply to the row. |
| interactions | none |

**Visual settings.** All columns.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T12 Data consistency · release gate G1 (spec Q12; design §11 option 1) · as-of: common window [2026-08-16, 2026-09-15) · SYNTHETIC ESTATE (snapshot side)
// On both pages, beside freshness. Reads the base query _snapshot_consistency.
_snapshot_consistency
| project gate = iff(consistent, "PASS", "FAIL"),   // conditional-formatting key
          status = iff(consistent,
                       "CONSISTENT: enterprise data is built on the current attribution",
                       "INCONSISTENT: enterprise data predates the current attribution; dependent figures are withheld"),
          plume_facility_days_checked = facility_days, matched, catalog_only, snapshot_only, values_differ,
          checked = "plume facility-days, catalog vs fact_facility_daily_snapshot, 2026-08-16 → 2026-09-14",
          action = iff(consistent, "none",
                       "run greensky_enterprise with run_mode=backfill and pipeline_date EMPTY (not 2026-09-15)")
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

1 rows (defaults):

| gate | status | plume_facility_days_checked | matched | catalog_only | snapshot_only | values_differ | checked | action |
|---|---|---|---|---|---|---|---|---|
| PASS | CONSISTENT: enterprise data is built on the current attribution | 42 | 42 | 0 | 0 | 0 | plume facility-days, catalog vs fact_facility_daily_snapshot, 2026-08-16 → 2026-09-14 | none |


#### 5.4 · T30 · Headline

| field | value |
|---|---|
| visual | Multi Stat |
| position x, y | 0, 7 |
| size w × h | 24 × 6 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`), Facility (`_facility_id`), base query `_snapshot_consistency` |
| conditional formatting | none |
| interactions | none |

**Visual settings.** Label column: `label` (carries the unit). Value column: `value` (exact, formatted text; never abbreviated). Text size: auto. Orientation: horizontal. Card grid (slot): 3 wide × 2 high, so each card is 8 units wide. Minimum tile size for this grid: 9 × 6.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T30 Headline · E1, E3 (attributed), E4, E6, E8 (total), E7 · as-of: page default (printed on T01) · Multi Stat, 6 cards (grid 3 × 2)
// The six numbers to read first. Their splits and the R3 reconciliation are in T31, directly below.
// GATED on G1: E6, E8, E7 (enterprise figures built on the attribution).
// fmt(v, dp): the exact value as text with thousands separators, so nothing is abbreviated (R5f). Precision must be a literal.
let fmt = (v:real, dp:int) {
    let r  = round(v, dp);
    let s  = tostring(r);
    let ip = tostring(split(s, ".")[0]);
    let fp = iff(array_length(split(s, ".")) > 1, tostring(split(s, ".")[1]), "");
    let L  = strlen(ip);
    let g  = case(L > 9, strcat(substring(ip, 0, L - 9), ",", substring(ip, L - 9, 3), ",", substring(ip, L - 6, 3), ",", substring(ip, L - 3, 3)),
                  L > 6, strcat(substring(ip, 0, L - 6), ",", substring(ip, L - 6, 3), ",", substring(ip, L - 3, 3)),
                  L > 3, strcat(substring(ip, 0, L - 3), ",", substring(ip, L - 3, 3)),
                  ip);
    iff(dp <= 0, g, strcat(g, ".", substring(strcat(fp, "0000000000"), 0, dp)))
};
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let anchor  = W_end - 1d;                                            // stocks: end of the window's last day (R1c)
let ok = toscalar(_snapshot_consistency | project consistent);   // G1 (B1): false while the snapshot predates the current attribution
let withheld = "withheld: see Data consistency";
let p = external_table('gold_plume_catalog') | where detection_date >= W_start and detection_date < W_end;
let s = external_table('fact_facility_daily_snapshot') | where snapshot_date >= W_start and snapshot_date < W_end;
let a = s | where snapshot_date == anchor;
union
 (p | summarize n = count() | project ord = 1, label = "Plumes detected", value = tostring(n)),
 (p | where isnotempty(attributed_facility_id) | summarize kg = sum(ime_kg)
    | project ord = 2, label = "Plume methane at facilities, t CH₄", value = fmt(kg / 1000.0, 1)),            // kg → t
 (p | summarize m = percentile(emission_rate_kg_h, 50)
    | project ord = 3, label = "Median detected rate, t/h", value = fmt(m / 1000.0, 1)),                                 // kg/h → t/h
 (s | summarize v = sum(daily_financial_impact)
    | project ord = 4, label = "Financial impact (point estimate), USD", value = iff(ok, fmt(v, 2), withheld)),
 (a | summarize v = sum(open_tickets + stalled_backlog)
    | project ord = 5, label = "Open work orders, total", value = iff(ok, tostring(v), withheld)),
 (s | summarize v = sum(compliance_violations)
    | project ord = 6, label = "Notices of violation", value = iff(ok, tostring(v), withheld))
| order by ord asc
| project label, value
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

6 rows (defaults):

| label | value |
|---|---|
| Plumes detected | 49 |
| Plume methane at facilities, t CH₄ | 1,445.8 |
| Median detected rate, t/h | 19.7 |
| Financial impact (point estimate), USD | 268,504.28 |
| Open work orders, total | 126 |
| Notices of violation | 3 |


#### 5.5 · T31 · Supporting figures and reconciliation

| field | value |
|---|---|
| visual | Table |
| position x, y | 0, 13 |
| size w × h | 12 × 9 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`), Facility (`_facility_id`), base query `_snapshot_consistency` |
| conditional formatting | none |
| interactions | none |

**Visual settings.** All columns, in query order: `area`, `figure`, `value`, `unit`. Compact proof rows behind the headline, including the R3 reconciliation.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T31 Supporting figures and reconciliation · E2, E3 (+R3b), E5, E6 split, E8 split, E9, E10, E11, E12 · as-of: page default (printed on T01) · Table
// Proof, not headline: the splits behind T30 and the R3 reconciliation (facilities + unattributed = catalog total).
// GATED on G1: the financial, backlog and compliance rows.
// fmt(v, dp): the exact value as text with thousands separators, so nothing is abbreviated (R5f). Precision must be a literal.
let fmt = (v:real, dp:int) {
    let r  = round(v, dp);
    let s  = tostring(r);
    let ip = tostring(split(s, ".")[0]);
    let fp = iff(array_length(split(s, ".")) > 1, tostring(split(s, ".")[1]), "");
    let L  = strlen(ip);
    let g  = case(L > 9, strcat(substring(ip, 0, L - 9), ",", substring(ip, L - 9, 3), ",", substring(ip, L - 6, 3), ",", substring(ip, L - 3, 3)),
                  L > 6, strcat(substring(ip, 0, L - 6), ",", substring(ip, L - 6, 3), ",", substring(ip, L - 3, 3)),
                  L > 3, strcat(substring(ip, 0, L - 3), ",", substring(ip, L - 3, 3)),
                  ip);
    iff(dp <= 0, g, strcat(g, ".", substring(strcat(fp, "0000000000"), 0, dp)))
};
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let anchor  = W_end - 1d;                                            // stocks: end of the window's last day (R1c)
let ok = toscalar(_snapshot_consistency | project consistent);   // G1 (B1): false while the snapshot predates the current attribution
let withheld = "withheld: see Data consistency";
let p = external_table('gold_plume_catalog') | where detection_date >= W_start and detection_date < W_end;
let s = external_table('fact_facility_daily_snapshot') | where snapshot_date >= W_start and snapshot_date < W_end;
let a = s | where snapshot_date == anchor;
let fac_kg = toscalar(p | where isnotempty(attributed_facility_id) | summarize kg = sum(ime_kg) by attributed_facility_id | summarize sum(kg));
let una_kg = toscalar(p | where attribution_method == "no_match" | summarize sum(ime_kg));
let tot_kg = toscalar(p | summarize sum(ime_kg));
let usd  = toscalar(s | summarize sum(daily_financial_impact));
let lost = toscalar(s | summarize sum(total_emissions_kg)) / 19.26 * 2.75;   // kg CH4 → mcf → USD (R5b)
let top  = p | summarize arg_max(emission_rate_kg_h, plume_id, emission_rate_p5_kg_h, emission_rate_p95_kg_h) | where isnotempty(plume_id);
union
 (p | where attribution_method != "no_match" | summarize n = count()
    | project ord = 1, area = "Plumes", figure = "Attributed to a facility", value = tostring(n), unit = "plumes", gated = false),
 (p | where attribution_method == "no_match" | summarize n = count()
    | project ord = 2, area = "Plumes", figure = "Unattributed (no facility within 50 km)", value = tostring(n), unit = "plumes", gated = false),
 (p | where isnotempty(attributed_facility_id) | summarize n = count_distinct(attributed_facility_id)
    | project ord = 3, area = "Plumes", figure = "Facilities with a detection", value = tostring(n), unit = "facilities", gated = false),
 (print ord = 4, area = "Methane", figure = "At facilities", value = fmt(fac_kg / 1000.0, 1), unit = "t CH₄", gated = false),
 (print ord = 5, area = "Methane", figure = "Unattributed", value = fmt(una_kg / 1000.0, 1), unit = "t CH₄", gated = false),
 (print ord = 6, area = "Methane", figure = "Catalog total", value = fmt(tot_kg / 1000.0, 1), unit = "t CH₄", gated = false),
 (print ord = 7, area = "Methane", figure = "Reconciliation: facilities + unattributed − total", value = fmt((fac_kg + una_kg - tot_kg) / 1000.0, 1), unit = "t CH₄", gated = false),
 (top | project ord = 8, area = "Rate", figure = strcat("Largest rate (", plume_id, ")"), value = fmt(emission_rate_kg_h / 1000.0, 1), unit = "t/h", gated = false),
 (top | project ord = 9, area = "Rate", figure = "Largest plume's P5–P95", value = strcat(fmt(emission_rate_p5_kg_h / 1000.0, 1), " – ", fmt(emission_rate_p95_kg_h / 1000.0, 1)), unit = "t/h", gated = false),
 (print ord = 10, area = "Financial", figure = "Lost gas (derived)", value = fmt(lost, 2), unit = "USD", gated = true),
 (print ord = 11, area = "Financial", figure = "Fines (derived)", value = fmt(usd - lost, 2), unit = "USD", gated = true),
 (a | summarize v = sum(open_tickets)           | project ord = 12, area = "Backlog", figure = "Open work orders within SLA", value = tostring(v), unit = "tickets", gated = true),
 (a | summarize v = sum(stalled_backlog)        | project ord = 13, area = "Backlog", figure = "Open work orders past SLA", value = tostring(v), unit = "tickets", gated = true),
 (a | summarize v = sum(overdue_pms)            | project ord = 14, area = "Backlog", figure = "Overdue preventive maintenance", value = tostring(v), unit = "assets", gated = true),
 (a | summarize v = sum(ldar_leaks_outstanding) | project ord = 15, area = "Backlog", figure = "LDAR leaks outstanding", value = tostring(v), unit = "leaks", gated = true),
 (a | summarize v = count()                     | project ord = 16, area = "Backlog", figure = "Facilities summed (incl. 7 inactive)", value = tostring(v), unit = "facilities", gated = false),
 (a | summarize v = sum(open_compliance_cases)  | project ord = 17, area = "Compliance", figure = "Open compliance cases, all types", value = tostring(v), unit = "cases", gated = true)
| extend value = iff(gated and not(ok), withheld, value)
| order by ord asc
| project area, figure, value, unit
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

17 rows (defaults); first row: `Plumes | Attributed to a facility | 42 | plumes`


#### 5.6 · T07 · Plumes and facilities

| field | value |
|---|---|
| visual | Map |
| position x, y | 12, 13 |
| size w × h | 12 × 9 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`), Facility (`_facility_id`) |
| conditional formatting | none (no colour by magnitude). |
| interactions | none |

**Visual settings.** Define location by: **Latitude and longitude** → Latitude `lat`, Longitude `lon`. Label column: `label`. Size: `t_ch4_point_estimate`. If the map offers a series / colour-by column, use `marker` (three values: facility with plumes, facility with no detection, unattributed plume). If it does not, `marker` still shows in the tooltip.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T07 Plume and facility map · measure E14 (+ R3d) · as-of: last day of window, capped at H_ent (2026-09-14) · SYNTHETIC ESTATE
// Facilities at dim_facility coordinates sized by E3 (catalog ime_kg); unattributed plumes at their own source_lat/lon.
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let as_of   = strcat("as of ", format_datetime(W_end - 1d, "yyyy-MM-dd"));
let p = external_table('gold_plume_catalog') | where detection_date >= W_start and detection_date < W_end;
let dimf = external_table('dim_facility') | project facility_id, facility_name, facility_lat, facility_lon;
union
 (p | where isnotempty(attributed_facility_id)
    | summarize plumes = count(), kg = sum(ime_kg) by facility_id = attributed_facility_id
    | join kind=inner dimf on facility_id
    | project lat = facility_lat, lon = facility_lon, marker = "synthetic facility · attributed plume(s)",
              label = strcat(facility_id, " · ", facility_name), plumes, t_ch4_point_estimate = round(kg / 1000.0, 1)),   // kg → t
 (dimf
    | join kind=leftanti (p | where isnotempty(attributed_facility_id) | project facility_id = attributed_facility_id) on facility_id
    | project lat = facility_lat, lon = facility_lon, marker = "synthetic facility · no detection in window",
              label = strcat(facility_id, " · ", facility_name), plumes = 0, t_ch4_point_estimate = 0.0),
 (p | where attribution_method == "no_match"
    | project lat = source_lat, lon = source_lon, marker = "unattributed plume (real; no facility within 50 km)",
              label = plume_id, plumes = 1, t_ch4_point_estimate = round(ime_kg / 1000.0, 1))
| extend as_of
| order by marker asc, label asc   // deterministic row order (union order is not guaranteed)
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

157 rows (defaults); first row: `31.5961 | -102.411 | synthetic facility · attributed plume(s) | GS-0006 · Kermit Tank Battery | 1 | 18.8 | as of 2026-09-14`


#### 5.7 · T11a · Detected plumes - rate t/h

| field | value |
|---|---|
| visual | Table |
| position x, y | 0, 22 |
| size w × h | 24 × 5 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`), Facility (`_facility_id`) |
| conditional formatting | none. **No colour by rate** (Decision A). |
| interactions | **Drillthrough ON.** Visual tab → Interactions → Drillthrough: on → Create new. Destination page: **Facility Operations**. Column: **`facility_id`**. Parameter: **Facility**. (Both string.) Cross-filter: off. |

**Visual settings.** All columns, in query order (sorted by rate).

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T11a Detection table, attributed plumes · measures F3, F4 (portfolio rows), E5; Decisions A and B · as-of: last day of window, capped at H_ent (2026-09-14) · SYNTHETIC ESTATE (attribution columns)
// Sorted by point rate. No severity tier. Drill-through ON: facility_id → Facility Operations (_facility_id). Unattributed plumes are in T11b.
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
external_table('gold_plume_catalog')
| where detection_date >= W_start and detection_date < W_end
| where attribution_method != "no_match"
| join kind=leftouter (external_table('dim_facility') | project attributed_facility_id = facility_id, dim_name = facility_name) on attributed_facility_id
| order by emission_rate_kg_h desc, plume_id asc
| project detection_utc = format_datetime(detection_date, "yyyy-MM-dd HH:mm"),
          plume_id,
          rate_t_h = round(emission_rate_kg_h / 1000.0, 1),       // kg/h → t/h, point estimate
          p5_t_h   = round(emission_rate_p5_kg_h / 1000.0, 1),
          p95_t_h  = round(emission_rate_p95_kg_h / 1000.0, 1),
          ime_t    = round(ime_kg / 1000.0, 1),                   // kg → t
          t_mix_method,
          confidence_grade = confidence,
          facility_id = attributed_facility_id,
          facility_name_decorative = dim_name,
          attribution = case(facilities_in_range == 1, "only candidate within 50 km",
                             attribution_probability >= 0.995, strcat("> 0.99 share of ", facilities_in_range, " candidates"),
                             strcat(tostring(round(attribution_probability, 2)), " share of ", facilities_in_range, " candidates")),
          facilities_in_range,
          distance_km = round(attribution_distance_km, 1),
          attribution_method,
          estate = "real plume · synthetic facility"
| extend as_of = strcat("as of ", format_datetime(W_end - 1d, "yyyy-MM-dd"))
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

42 rows (defaults); first row: `2026-09-11 19:55 | PL-dd9cfda8fa72 | 71.8 | 36.4 | 107.9 | 110.7 | wind_dependent | high | GS-0091 | Orla Booster Station | 0.49 share of 15 candidates | 15 | 24.1 | wind_aware | real plume · synthetic facility | as of 2026-09-14`


#### 5.8 · T11b · Unattributed plumes - rate t/h

| field | value |
|---|---|
| visual | Table |
| position x, y | 0, 27 |
| size w × h | 24 × 4 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`) |
| conditional formatting | none |
| interactions | **Drillthrough OFF.** Cross-filter off. (No facility exists to drill to.) |

**Visual settings.** All columns.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T11b Detection table, unattributed plumes · measures F3 (portfolio rows), R3 bucket; Decision A · as-of: last day of window, capped at H_ent (2026-09-14) · real (no facility)
// Drill-through OFF on this visual: there is no facility to drill to. RTI drill-through is set per visual, not per row.
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
external_table('gold_plume_catalog')
| where detection_date >= W_start and detection_date < W_end
| where attribution_method == "no_match"
| order by emission_rate_kg_h desc, plume_id asc
| project detection_utc = format_datetime(detection_date, "yyyy-MM-dd HH:mm"),
          plume_id,
          rate_t_h = round(emission_rate_kg_h / 1000.0, 1),       // kg/h → t/h, point estimate
          p5_t_h   = round(emission_rate_p5_kg_h / 1000.0, 1),
          p95_t_h  = round(emission_rate_p95_kg_h / 1000.0, 1),
          ime_t    = round(ime_kg / 1000.0, 1),                   // kg → t
          t_mix_method,
          confidence_grade = confidence,
          source_lat = round(source_lat, 3), source_lon = round(source_lon, 3),
          attribution = "no facility within 50 km",
          attribution_method,
          estate = "real plume"
| extend as_of = strcat("as of ", format_datetime(W_end - 1d, "yyyy-MM-dd"))
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

7 rows (defaults):

| detection_utc | plume_id | rate_t_h | p5_t_h | p95_t_h | ime_t | t_mix_method | confidence_grade | source_lat | source_lon | attribution | attribution_method | estate | as_of |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-08-26 19:57 | PL-c39055b1ca19 | 44.8 | 20.1 | 68.1 | 61.8 | wind_dependent | high | 30.92 | -104.826 | no facility within 50 km | no_match | real plume | as of 2026-09-14 |
| 2026-08-21 19:51 | PL-38ec1f72dd88 | 37.2 | 15.4 | 58 | 34.4 | wind_dependent | high | 30.682 | -103.175 | no facility within 50 km | no_match | real plume | as of 2026-09-14 |
| 2026-08-21 19:51 | PL-a336625b917d | 35.1 | 16.5 | 57.7 | 36.9 | wind_dependent | high | 33.191 | -104.567 | no facility within 50 km | no_match | real plume | as of 2026-09-14 |
| 2026-08-21 19:51 | PL-561a8947191b | 23.7 | 11.4 | 37.4 | 48 | wind_dependent | high | 33.327 | -101.054 | no facility within 50 km | no_match | real plume | as of 2026-09-14 |
| 2026-08-20 20:10 | PL-3965e3d50edb | 19.9 | 9.2 | 32.9 | 46.6 | wind_dependent | high | 30.981 | -104.703 | no facility within 50 km | no_match | real plume | as of 2026-09-14 |
| 2026-08-20 20:16 | PL-f1714a3e7a5e | 17.8 | 7 | 30.5 | 12.8 | wind_dependent | medium | 33.47 | -101.2 | no facility within 50 km | no_match | real plume | as of 2026-09-14 |
| 2026-08-25 20:16 | PL-19aafb474281 | 16.4 | 7 | 27.7 | 34.8 | wind_dependent | high | 33.413 | -101.005 | no facility within 50 km | no_match | real plume | as of 2026-09-14 |


#### 5.9 · T06 · Daily observed methane - t CH₄

| field | value |
|---|---|
| visual | Stacked column chart |
| position x, y | 0, 31 |
| size w × h | 12 × 6 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`) |
| conditional formatting | none |
| interactions | none |

**Visual settings.** X: `detection_day`. Y: `t_ch4_point_estimate`. Series: `series` (2 values: attributed, unattributed).

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T06 Daily observed methane · measure E13 (+ R3 bucket) · as-of: last day of window, capped at H_ent (2026-09-14) · SYNTHETIC ESTATE (attribution split)
// Both series from the catalog (ime_kg by UTC detection day). Σ of all bars = T03 catalog total.
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
external_table('gold_plume_catalog')
| where detection_date >= W_start and detection_date < W_end
| extend series = iff(attribution_method == "no_match", "unattributed (no facility within 50 km)", "attributed (synthetic attribution)")
| summarize t_ch4_point_estimate = sum(ime_kg) / 1000.0 by detection_day = startofday(detection_date), series   // kg → t
| order by detection_day asc, series asc
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

17 rows (defaults); first row: `2026-08-16T00:00:00Z | attributed (synthetic attribution) | 87.379`


#### 5.10 · T05 · Emission rate by plume - t/h (line: 25 t/h, Lauvaux et al. 2022)

| field | value |
|---|---|
| visual | Scatter chart |
| position x, y | 12, 31 |
| size w × h | 12 × 6 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`) |
| conditional formatting | none. **No per-plume badge or tier** (Decision A). |
| interactions | none |

**Visual settings.** X: `plume_rank`. Y: `t_per_h`. Series: `series` (point, P5, P95). **Y Axis → Reference lines: 25.** The line is a visual setting, not a data series. The label lives in the title because the docs don't document labels on reference lines.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T05 Emission rate distribution · measures E4/E5 context, Decision A (no banding) · as-of: last day of window, capped at H_ent (2026-09-14) · real (no facility)
// Visual: Scatter chart (x = plume_rank). The 25 t/h line is the visual's native Y-axis "Reference lines" setting, not a data series:
// documented for Scatter, not for Line chart (learn.microsoft.com/fabric/real-time-intelligence/dashboard-visuals-customize). Decision A.
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let ranked = external_table('gold_plume_catalog')
    | where detection_date >= W_start and detection_date < W_end
    | order by emission_rate_kg_h desc, plume_id asc
    | extend plume_rank = row_number();
union
 (ranked | project plume_rank, series = "point estimate", t_per_h = emission_rate_kg_h / 1000.0),     // kg/h → t/h
 (ranked | project plume_rank, series = "P5 of that plume", t_per_h = emission_rate_p5_kg_h / 1000.0),
 (ranked | project plume_rank, series = "P95 of that plume", t_per_h = emission_rate_p95_kg_h / 1000.0)
| order by series asc, plume_rank asc
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

147 rows (defaults); first row: `1 | P5 of that plume | 36.3753`


#### 5.11 · T09 · Facilities by observed methane

| field | value |
|---|---|
| visual | Table |
| position x, y | 0, 37 |
| size w × h | 13 × 8 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`), Facility (`_facility_id`) |
| conditional formatting | none. |
| interactions | **Drillthrough OFF** (the UNATTRIBUTED and CATALOG TOTAL rows have no facility). |

**Visual settings.** All columns, in query order. The last two rows are UNATTRIBUTED and CATALOG TOTAL (reconciliation).

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T09 Facility ranking by observed methane · measures E3 + F1 per facility, E12; Decision C (rank on methane, never risk_score) · as-of: last day of window, capped at H_ent (2026-09-14) · SYNTHETIC ESTATE
// Rank: E3 in window desc, then plume count desc, then facility_id (deterministic tie-break, not a score).
// Source: catalog (current attribution). R3: Σ facility rows + UNATTRIBUTED row = CATALOG TOTAL row.
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let as_of   = strcat("as of ", format_datetime(W_end - 1d, "yyyy-MM-dd"));
let p = external_table('gold_plume_catalog') | where detection_date >= W_start and detection_date < W_end;
let ranked = p
    | where isnotempty(attributed_facility_id)
    | summarize plumes = count(), kg = sum(ime_kg), max_kg_h = max(emission_rate_kg_h) by facility_id = attributed_facility_id
    | join kind=inner (external_table('dim_facility') | project facility_id, facility_name, sub_basin) on facility_id
    | order by kg desc, plumes desc, facility_id asc
    | extend rank = row_number();
union
 (ranked | project ord = 1, rank_n = rank, rank = tostring(rank), facility_id, facility_name_decorative = facility_name, sub_basin, plumes,
                   t_ch4_point_estimate = round(kg / 1000.0, 1),          // kg → t
                   largest_point_rate_t_h = round(max_kg_h / 1000.0, 1)), // kg/h → t/h
 (p | where attribution_method == "no_match"
    | summarize plumes = count(), kg = sum(ime_kg), max_kg_h = max(emission_rate_kg_h)
    | project ord = 2, rank_n = 0, rank = "—", facility_id = "UNATTRIBUTED", facility_name_decorative = "no facility within 50 km", sub_basin = "",
              plumes, t_ch4_point_estimate = round(kg / 1000.0, 1), largest_point_rate_t_h = round(max_kg_h / 1000.0, 1)),
 (p | summarize plumes = count(), kg = sum(ime_kg), max_kg_h = max(emission_rate_kg_h)
    | project ord = 3, rank_n = 0, rank = "Σ", facility_id = "CATALOG TOTAL", facility_name_decorative = "check: = Σ facilities + unattributed", sub_basin = "",
              plumes, t_ch4_point_estimate = round(kg / 1000.0, 1), largest_point_rate_t_h = round(max_kg_h / 1000.0, 1))
| extend estate = iff(ord == 1, "synthetic facility", "real plumes"), as_of
| order by ord asc, rank_n asc
| project-away ord, rank_n
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

33 rows (defaults); first row: `1 | GS-0091 | Orla Booster Station | Midland Basin | 2 | 158.5 | 71.8 | synthetic facility | as of 2026-09-14`


#### 5.12 · T10 · Risk points by component

| field | value |
|---|---|
| visual | Stacked bar chart |
| position x, y | 13, 37 |
| size w × h | 11 × 8 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`), Facility (`_facility_id`), base query `_snapshot_consistency` |
| conditional formatting | none. **No total, no `risk_score`** (Decision C). |
| interactions | none |

**Visual settings.** X (category): `facility` (`#01 · GS-…`, same order as T09). Y: `points`. Series: `series` (five components). Legend on.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T10 Risk points beside the rank · Decision C context (risk_pts_* stocks) · as-of: anchor day = last day of window, capped at H_ent (2026-09-14) · SYNTHETIC ESTATE
// Same facilities and order as T09. Emissions points come from real plumes through the synthetic attribution; the other four are synthetic.
// All five components, each separately; emissions and compliance released 2026-10-15 after the enterprise backfill. No sum, no risk_score.
// GATED on G1: emissions, compliance, maintenance and open work depend on attribution (directly, or through 03c → 03b → 03d).
// While G1 fails only condition is drawn, and its series label says the other four are withheld.
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let anchor  = W_end - 1d;
let ok = toscalar(_snapshot_consistency | project consistent);   // G1 (B1): false while the snapshot predates the current attribution
let withheld = "withheld: enterprise data predates the current attribution (see Data consistency)";
let ranked = external_table('gold_plume_catalog')
    | where detection_date >= W_start and detection_date < W_end and isnotempty(attributed_facility_id)
    | summarize plumes = count(), kg = sum(ime_kg) by facility_id = attributed_facility_id
    | order by kg desc, plumes desc, facility_id asc
    | extend rank = row_number();
external_table('fact_facility_daily_snapshot')
| where snapshot_date == anchor
| join kind=inner ranked on facility_id
| extend facility = strcat("#", iff(rank < 10, "0", ""), rank, " · ", facility_id)
| mv-expand component = dynamic(["emissions (real plumes via synthetic attribution, max 25)", "compliance: violations in 30 days + open state cases (synthetic, max 20)",
                                 "condition (synthetic, max 30)", "maintenance (synthetic, max 15)", "open work (synthetic, max 10)"]) to typeof(string)
| extend points = case(component startswith "emissions",   risk_pts_emissions,
                       component startswith "compliance",  risk_pts_compliance,
                       component startswith "condition",   risk_pts_condition,
                       component startswith "maintenance", risk_pts_maintenance,
                       risk_pts_open_work)
| where ok or component startswith "condition"
| project rank, facility, series = strcat(component,
                                          iff(ok, "", " · emissions, compliance, maintenance and open work " ), iff(ok, "", withheld)), points
| order by rank asc, series asc
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

155 rows (defaults); first row: `1 | #01 · GS-0091 | compliance: violations in 30 days + open state cases (synthetic, max 20) | 10`


---

## 6. Page 2: Facility Operations

Create the page **Facility Operations**, then add the tiles in this order. The facility selector shows on this page only.


#### 6.1 · T19 · (no title)

| field | value |
|---|---|
| visual | Text (markdown) |
| position x, y | 0, 0 |
| size w × h | 24 × 3 |
| data source | — |
| parameters used | none |
| conditional formatting | none |
| interactions | none |

**Visual settings.** Content:

> **One synthetic facility, chosen above or by clicking a facility in the detected-plumes table.** Everything on this page is synthetic except each plume's detection, rate, band and observed methane. See README.


#### 6.2 · T01 · Source freshness

| field | value |
|---|---|
| visual | Table |
| position x, y | 0, 3 |
| size w × h | 16 × 4 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`) |
| conditional formatting | as on Executive Overview |
| interactions | none |

**Visual settings.** Same query as on Executive Overview (duplicate the tile, or paste the query).

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T01 Source freshness · spec "Also settle: Freshness" + R1d · as-of: each source's own max; board end = window end capped at H_ent (last day 2026-09-14)
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let board   = strcat("[", format_datetime(W_start, "yyyy-MM-dd"), ", ", format_datetime(W_end, "yyyy-MM-dd"), ")");
let cat = external_table('gold_plume_catalog');
union
 (cat | summarize t0 = min(detection_date), t1 = max(detection_date), n = count(),
                  h0 = min(detection_date - startofday(detection_date)), h1 = max(detection_date - startofday(detection_date)),
                  after = countif(detection_date >= W_end)
      | project ord = 1, source = "TROPOMI CH4 plumes (gold_plume_catalog.detection_date)", estate = "real",
                as_of_utc = format_datetime(t1, "yyyy-MM-dd HH:mm:ss"),
                coverage = strcat(format_datetime(t0, "yyyy-MM-dd"), " → ", format_datetime(t1, "yyyy-MM-dd"), " · ", n, " plumes · overpasses ",
                                  format_timespan(h0, "HH:mm"), "–", format_timespan(h1, "HH:mm"), " UTC"),
                note = strcat("detection_date = first pixel row of the cluster (overpass time). ", after, " plume(s) after the board end are not shown; no run timestamp is stored.")),
 (cat | summarize n = count(), w = countif(isnotnull(era5_u10) and isnotnull(era5_v10)), fb = countif(t_mix_method != "wind_dependent")
      | project ord = 2, source = "ERA5 10 m wind (gold_plume_catalog.era5_u10/v10)", estate = "real",
                as_of_utc = "matched per plume; not stored",
                coverage = strcat(w, " of ", n, " plumes carry ERA5 u/v; ", fb, " fell back to the fixed 48 h mixing time"),
                note = "Per pixel: nearest ERA5 hour and grid point (03_join_data:421-453). ERA5 comes from a manually downloaded extract that must cover the window (02b:142-180); its file and latency are not in the serving layer."),
 (external_table('fact_facility_daily_snapshot') | summarize t0 = min(snapshot_date), t1 = max(snapshot_date), d = count_distinct(snapshot_date), f = count_distinct(facility_id)
      | project ord = 3, source = "Enterprise daily snapshot (fact_facility_daily_snapshot.snapshot_date)", estate = "synthetic",
                as_of_utc = format_datetime(t1, "yyyy-MM-dd"),
                coverage = strcat(format_datetime(t0, "yyyy-MM-dd"), " → ", format_datetime(t1, "yyyy-MM-dd"), " · ", d, " days × ", f, " facilities"),
                note = "Stocks read at end of the anchor day. Plume-derived columns predate the 05 wind fix and are not shown (see README)."),
 (external_table('scada_telemetry_hourly') | summarize t0 = min(bucket_ts), t1 = max(bucket_ts), f = count_distinct(facility_sk)
      | project ord = 4, source = "SCADA hourly (scada_telemetry_hourly.bucket_ts)", estate = "synthetic",
                as_of_utc = format_datetime(t1, "yyyy-MM-dd HH:mm"),
                coverage = strcat(format_datetime(t0, "yyyy-MM-dd HH:mm"), " → ", format_datetime(t1, "yyyy-MM-dd HH:mm"), " · ", f, " of 150 facilities have rows"),
                note = "An hour with no readings has no row (02c:68-72)."),
 (external_table('dim_facility') | summarize e = max(effective_from), n = count(), cur = countif(is_current), seed = max(topology_seed)
      | project ord = 5, source = "Facility topology (dim_facility.effective_from)", estate = "synthetic",
                as_of_utc = format_datetime(e, "yyyy-MM-dd"),
                coverage = strcat(n, " facilities, ", cur, " current · topology_seed ", seed),
                note = "Invented estate. Facility names are decorative: the town is drawn independently of the coordinates (01a:125)."),
 (print ord = 6, source = "THIS BOARD", estate = "—",
        as_of_utc = format_datetime(W_end - 1d, "yyyy-MM-dd"),
        coverage = strcat("window ", board, " UTC, capped at H_ent 2026-09-15 (exclusive)"),
        note = "Detection runs one day past enterprise/SCADA; every page reads both, so every tile ends at the earlier horizon (R1b).")
| order by ord asc
| project-away ord
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

6 rows (defaults):

| source | estate | as_of_utc | coverage | note |
|---|---|---|---|---|
| TROPOMI CH4 plumes (gold_plume_catalog.detection_date) | real | 2026-09-15 20:17:52 | 2026-08-16 → 2026-09-15 · 50 plumes · overpasses 19:35–20:35 UTC | detection_date = first pixel row of the cluster (overpass time). 1 plume(s) after the board end are not shown; no run timestamp is stored. |
| ERA5 10 m wind (gold_plume_catalog.era5_u10/v10) | real | matched per plume; not stored | 50 of 50 plumes carry ERA5 u/v; 1 fell back to the fixed 48 h mixing time | Per pixel: nearest ERA5 hour and grid point (03_join_data:421-453). ERA5 comes from a manually downloaded extract that must cover the window (02b:142-180); its file and latency are not in the serving layer. |
| Enterprise daily snapshot (fact_facility_daily_snapshot.snapshot_date) | synthetic | 2026-09-14 | 2026-08-16 → 2026-09-14 · 30 days × 150 facilities | Stocks read at end of the anchor day. Plume-derived columns predate the 05 wind fix and are not shown (see README). |
| SCADA hourly (scada_telemetry_hourly.bucket_ts) | synthetic | 2026-09-14 23:00 | 2026-08-16 00:00 → 2026-09-14 23:00 · 147 of 150 facilities have rows | An hour with no readings has no row (02c:68-72). |
| Facility topology (dim_facility.effective_from) | synthetic | 2026-09-15 | 150 facilities, 150 current · topology_seed 20260915 | Invented estate. Facility names are decorative: the town is drawn independently of the coordinates (01a:125). |
| THIS BOARD | — | 2026-09-14 | window [2026-08-16, 2026-09-15) UTC, capped at H_ent 2026-09-15 (exclusive) | Detection runs one day past enterprise/SCADA; every page reads both, so every tile ends at the earlier horizon (R1b). |


#### 6.3 · T12 · Data consistency

| field | value |
|---|---|
| visual | Table |
| position x, y | 16, 3 |
| size w × h | 8 × 4 |
| data source | greensky_v2_rti |
| parameters used | base query `_snapshot_consistency` |
| conditional formatting | as on Executive Overview (**required**) |
| interactions | none |

**Visual settings.** Same query as on Executive Overview.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T12 Data consistency · release gate G1 (spec Q12; design §11 option 1) · as-of: common window [2026-08-16, 2026-09-15) · SYNTHETIC ESTATE (snapshot side)
// On both pages, beside freshness. Reads the base query _snapshot_consistency.
_snapshot_consistency
| project gate = iff(consistent, "PASS", "FAIL"),   // conditional-formatting key
          status = iff(consistent,
                       "CONSISTENT: enterprise data is built on the current attribution",
                       "INCONSISTENT: enterprise data predates the current attribution; dependent figures are withheld"),
          plume_facility_days_checked = facility_days, matched, catalog_only, snapshot_only, values_differ,
          checked = "plume facility-days, catalog vs fact_facility_daily_snapshot, 2026-08-16 → 2026-09-14",
          action = iff(consistent, "none",
                       "run greensky_enterprise with run_mode=backfill and pipeline_date EMPTY (not 2026-09-15)")
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

1 rows (defaults):

| gate | status | plume_facility_days_checked | matched | catalog_only | snapshot_only | values_differ | checked | action |
|---|---|---|---|---|---|---|---|---|
| PASS | CONSISTENT: enterprise data is built on the current attribution | 42 | 42 | 0 | 0 | 0 | plume facility-days, catalog vs fact_facility_daily_snapshot, 2026-08-16 → 2026-09-14 | none |


#### 6.4 · T20 · Facility

| field | value |
|---|---|
| visual | Table |
| position x, y | 0, 7 |
| size w × h | 24 × 3 |
| data source | greensky_v2_rti |
| parameters used | Facility (`_facility_id`) |
| conditional formatting | none |
| interactions | none |

**Visual settings.** One column, `facility`: a single identity line. Hide the column header if the editor allows it.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T20 Facility identity line · dim_facility attributes for _facility_id · as-of: topology 2026-09-15 (printed on T01's dim_facility row) · Table, one row
// One line above the headline. The decorative-name note lives in the README, not here.
external_table('dim_facility')
| where facility_id == _facility_id
| project facility = strcat(facility_id, " · ", facility_name, " · ", facility_type, " · ", operator_name, " · ", sub_basin, " · ",
                            round(facility_lat, 4), ", ", round(facility_lon, 4), " · commissioned ", format_datetime(commission_date, "yyyy-MM-dd"),
                            iff(active_flag, "", " · inactive"), " · synthetic")
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

1 rows (defaults, GS-0091):

| facility |
|---|
| GS-0091 · Orla Booster Station · Compression Station · Operator 04 · Midland Basin · 32.3814, -101.772 · commissioned 2014-08-21 · synthetic |


#### 6.5 · T32 · Headline

| field | value |
|---|---|
| visual | Multi Stat |
| position x, y | 0, 10 |
| size w × h | 24 × 4 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`), Facility (`_facility_id`), base query `_snapshot_consistency` |
| conditional formatting | none |
| interactions | none |

**Visual settings.** Label column: `label` (carries the unit). Value column: `value` (exact, formatted text; a zero shows as "none in window"). Text size: auto. Orientation: horizontal. Card grid (slot): 4 wide × 1 high, so each card is 6 units wide. Minimum tile size for this grid: 12 × 3.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T32 Facility headline · F1, F2, E5 for f, E6 for f · as-of: page default (printed on T01) · Multi Stat, 4 cards in one row
// A zero is shown as "none in window", never as a big 0: for most facilities these are empty, and a large 0 reads as a finding.
// GATED on G1: F2 and E6 (snapshot figures built on the attribution).
// fmt(v, dp): the exact value as text with thousands separators (R5f). Precision must be a literal.
let fmt = (v:real, dp:int) {
    let r  = round(v, dp);
    let s  = tostring(r);
    let ip = tostring(split(s, ".")[0]);
    let fp = iff(array_length(split(s, ".")) > 1, tostring(split(s, ".")[1]), "");
    let L  = strlen(ip);
    let g  = case(L > 9, strcat(substring(ip, 0, L - 9), ",", substring(ip, L - 9, 3), ",", substring(ip, L - 6, 3), ",", substring(ip, L - 3, 3)),
                  L > 6, strcat(substring(ip, 0, L - 6), ",", substring(ip, L - 6, 3), ",", substring(ip, L - 3, 3)),
                  L > 3, strcat(substring(ip, 0, L - 3), ",", substring(ip, L - 3, 3)),
                  ip);
    iff(dp <= 0, g, strcat(g, ".", substring(strcat(fp, "0000000000"), 0, dp)))
};
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let anchor  = W_end - 1d;                                            // stocks: end of the window's last day (R1c)
let ok = toscalar(_snapshot_consistency | project consistent);   // G1 (B1): false while the snapshot predates the current attribution
let withheld = "withheld: see Data consistency";
let p = external_table('gold_plume_catalog')
    | where detection_date >= W_start and detection_date < W_end and attributed_facility_id == _facility_id;
let s = external_table('fact_facility_daily_snapshot')
    | where facility_id == _facility_id and snapshot_date >= W_start and snapshot_date < W_end;
let n = toscalar(p | summarize count());
let kg = toscalar(s | summarize sum(total_emissions_kg));
let usd = toscalar(s | summarize sum(daily_financial_impact));
let top = toscalar(p | summarize max(emission_rate_kg_h));
union
 (print ord = 1, label = "Plumes attributed", value = iff(n == 0, "none in window", tostring(n))),
 (print ord = 2, label = "Observed plume methane, t CH₄", value = case(not(ok), withheld, n == 0, "none in window", fmt(kg / 1000.0, 1))),   // kg → t
 (print ord = 3, label = "Largest rate, t/h", value = iff(n == 0, "none in window", fmt(top / 1000.0, 1))),                                   // kg/h → t/h
 (print ord = 4, label = "Financial impact (est.), USD", value = case(not(ok), withheld, usd == 0, "none in window", fmt(usd, 2)))
| order by ord asc
| project label, value
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

4 rows (defaults, GS-0091):

| label | value |
|---|---|
| Plumes attributed | 2 |
| Observed plume methane, t CH₄ | 158.5 |
| Largest rate, t/h | 71.8 |
| Financial impact (est.), USD | 22,627.17 |


#### 6.6 · T33 · Operations and compliance

| field | value |
|---|---|
| visual | Table |
| position x, y | 0, 14 |
| size w × h | 12 × 8 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`), Facility (`_facility_id`), base query `_snapshot_consistency` |
| conditional formatting | none |
| interactions | none |

**Visual settings.** All columns: `figure`, `value`, `unit`. Most rows are 0 for most facilities, so they are a table, not cards.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T33 Facility operations and compliance · F5, F6, F7, F8, F10, F11 (PM, LDAR, open cases) at the anchor day; E7 for f over W; E5 band · as-of: page default (printed on T01) · Table
// Compact on purpose: most of these are zero for most facilities, so they sit in a table, not on cards.
// GATED on G1: F5, F6, F11 and notices (attribution-dependent through 03c → 03b → 03d); F7, F8, F10 always show.
// fmt(v, dp): the exact value as text with thousands separators (R5f). Precision must be a literal.
let fmt = (v:real, dp:int) {
    let r  = round(v, dp);
    let s  = tostring(r);
    let ip = tostring(split(s, ".")[0]);
    let fp = iff(array_length(split(s, ".")) > 1, tostring(split(s, ".")[1]), "");
    let L  = strlen(ip);
    let g  = case(L > 9, strcat(substring(ip, 0, L - 9), ",", substring(ip, L - 9, 3), ",", substring(ip, L - 6, 3), ",", substring(ip, L - 3, 3)),
                  L > 6, strcat(substring(ip, 0, L - 6), ",", substring(ip, L - 6, 3), ",", substring(ip, L - 3, 3)),
                  L > 3, strcat(substring(ip, 0, L - 3), ",", substring(ip, L - 3, 3)),
                  ip);
    iff(dp <= 0, g, strcat(g, ".", substring(strcat(fp, "0000000000"), 0, dp)))
};
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let anchor  = W_end - 1d;                                            // stocks: end of the window's last day (R1c)
let ok = toscalar(_snapshot_consistency | project consistent);   // G1 (B1): false while the snapshot predates the current attribution
let withheld = "withheld: see Data consistency";
let a = external_table('fact_facility_daily_snapshot') | where facility_id == _facility_id and snapshot_date == anchor and snapshot_date >= W_start;
let notices = toscalar(external_table('fact_facility_daily_snapshot')
    | where facility_id == _facility_id and snapshot_date >= W_start and snapshot_date < W_end | summarize sum(compliance_violations));
let band = toscalar(external_table('gold_plume_catalog')
    | where detection_date >= W_start and detection_date < W_end and attributed_facility_id == _facility_id
    | summarize arg_max(emission_rate_kg_h, emission_rate_p5_kg_h, emission_rate_p95_kg_h)
    | where isnotnull(emission_rate_kg_h)
    | project b = strcat(fmt(emission_rate_p5_kg_h / 1000.0, 1), " – ", fmt(emission_rate_p95_kg_h / 1000.0, 1)));
a
| project r = pack_array(
     pack("ord", 1, "figure", "Open work orders within SLA", "value", tostring(open_tickets), "unit", "tickets", "g", true),
     pack("ord", 2, "figure", "Open work orders past SLA", "value", tostring(stalled_backlog), "unit", "tickets", "g", true),
     pack("ord", 3, "figure", "Mean time to close (closed in last 30 days)",
          "value", iff(isnull(mean_time_to_repair_hr), "none closed", fmt(mean_time_to_repair_hr, 1)), "unit", "hours", "g", true),
     pack("ord", 4, "figure", "Active CH₄ exceedances", "value", tostring(active_exceedances), "unit", "sensors", "g", false),
     pack("ord", 5, "figure", "Sensors offline", "value", tostring(sensors_offline), "unit", "sensors", "g", false),
     pack("ord", 6, "figure", "Alarms standing", "value", tostring(alarms_active), "unit", "alarms", "g", false),
     pack("ord", 7, "figure", "Overdue preventive maintenance", "value", tostring(overdue_pms), "unit", "assets", "g", true),
     pack("ord", 8, "figure", "LDAR leaks outstanding", "value", tostring(ldar_leaks_outstanding), "unit", "leaks", "g", true),
     pack("ord", 9, "figure", "Open compliance cases, all types", "value", tostring(open_compliance_cases), "unit", "cases", "g", true),
     pack("ord", 10, "figure", "Notices of violation (in window)", "value", tostring(notices), "unit", "notices", "g", true),
     pack("ord", 11, "figure", "Largest plume's P5–P95", "value", iff(isempty(band), "no detection in window", band), "unit", "t/h", "g", false))
| mv-expand r
| project ord = toint(r.ord), figure = tostring(r.figure), value = tostring(r.value), unit = tostring(r.unit), g = tobool(r.g)
| extend value = iff(g and not(ok), withheld, value)
| order by ord asc
| project figure, value, unit
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

11 rows (defaults, GS-0091):

| figure | value | unit |
|---|---|---|
| Open work orders within SLA | 3 | tickets |
| Open work orders past SLA | 0 | tickets |
| Mean time to close (closed in last 30 days) | 49.3 | hours |
| Active CH₄ exceedances | 2 | sensors |
| Sensors offline | 0 | sensors |
| Alarms standing | 0 | alarms |
| Overdue preventive maintenance | 6 | assets |
| LDAR leaks outstanding | 0 | leaks |
| Open compliance cases, all types | 3 | cases |
| Notices of violation (in window) | 0 | notices |
| Largest plume's P5–P95 | 36.4 – 107.9 | t/h |


#### 6.7 · T24 · Alarms raised per day

| field | value |
|---|---|
| visual | Column chart |
| position x, y | 12, 14 |
| size w × h | 12 × 8 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`), Facility (`_facility_id`) |
| conditional formatting | none |
| interactions | none |

**Visual settings.** X: `snapshot_date`. Y: `alarms_raised`. Series: `series` (one value; carries the window total and as-of).

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T24 Alarms raised per day · measure F9 (flow) · as-of: last day of window, capped at H_ent (2026-09-14) · SYNTHETIC ESTATE
// The snapshot writes a row for every facility-day, so a 0 here is a computed zero, not a gap (R2).
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let s = external_table('fact_facility_daily_snapshot')
    | where facility_id == _facility_id and snapshot_date >= W_start and snapshot_date < W_end;
let total = toscalar(s | summarize sum(alarms_raised));
s
| project snapshot_date, alarms_raised,
          series = strcat("alarms raised at ", _facility_id, " (window total ", total, ")")
| order by snapshot_date asc
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

30 rows (defaults, GS-0091); first row: `2026-08-16T00:00:00Z | 5 | alarms raised at GS-0091 (window total 72)`


#### 6.8 · T22 · Plumes at this facility - rate t/h

| field | value |
|---|---|
| visual | Table |
| position x, y | 0, 22 |
| size w × h | 24 × 5 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`), Facility (`_facility_id`) |
| conditional formatting | none. **No colour by rate** (Decision A). |
| interactions | none |

**Visual settings.** All columns, in query order.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T22 Plumes at the facility, with attribution evidence · measures F3, F4; Decisions A and B · as-of: last day of window, capped at H_ent (2026-09-14) · SYNTHETIC ESTATE
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let as_of   = strcat("as of ", format_datetime(W_end - 1d, "yyyy-MM-dd"));
let p = external_table('gold_plume_catalog')
    | where detection_date >= W_start and detection_date < W_end and attributed_facility_id == _facility_id;
let n = toscalar(p | summarize count());
union
 (p | order by emission_rate_kg_h desc, plume_id asc
    | project detection_utc = format_datetime(detection_date, "yyyy-MM-dd HH:mm"),
              plume_id,
              rate_t_h = round(emission_rate_kg_h / 1000.0, 1),       // kg/h → t/h, point estimate
              p5_t_h   = round(emission_rate_p5_kg_h / 1000.0, 1),
              p95_t_h  = round(emission_rate_p95_kg_h / 1000.0, 1),
              ime_t    = round(ime_kg / 1000.0, 1),                   // kg → t
              t_mix_method,
              confidence_grade = confidence,
              attribution = case(facilities_in_range == 1, "only candidate within 50 km",
                                 attribution_probability >= 0.995, strcat("> 0.99 share of ", facilities_in_range, " candidates"),
                                 strcat(tostring(round(attribution_probability, 2)), " share of ", facilities_in_range, " candidates")),
              facilities_in_range,
              distance_km = round(attribution_distance_km, 1),
              angular_offset_deg = round(attribution_angular_offset_deg, 0),
              attribution_method,
              runner_up = iff(isempty(second_facility_id), "none (single candidate)",
                              strcat(second_facility_id, " · ", tostring(round(second_facility_probability, 2)), " share"))),
 (print detection_utc = "", plume_id = strcat("no plume attributed to ", _facility_id, " in window") | where n == 0)
| extend estate = "real plume · synthetic facility", as_of
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

2 rows (defaults, GS-0091):

| detection_utc | plume_id | rate_t_h | p5_t_h | p95_t_h | ime_t | t_mix_method | confidence_grade | attribution | facilities_in_range | distance_km | angular_offset_deg | attribution_method | runner_up | estate | as_of |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-09-11 19:55 | PL-dd9cfda8fa72 | 71.8 | 36.4 | 107.9 | 110.7 | wind_dependent | high | 0.49 share of 15 candidates | 15 | 24.1 | 20 | wind_aware | GS-0105 · 0.41 share | real plume · synthetic facility | as of 2026-09-14 |
| 2026-08-26 19:57 | PL-cecdbc4553e4 | 29.4 | 14 | 44.8 | 47.8 | wind_dependent | high | 0.86 share of 32 candidates | 32 | 14.6 | 41 | wind_aware | GS-0058 · 0.05 share | real plume · synthetic facility | as of 2026-09-14 |


#### 6.9 · T25 · Risk points by day

| field | value |
|---|---|
| visual | Stacked column chart |
| position x, y | 0, 27 |
| size w × h | 24 × 6 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`), Facility (`_facility_id`), base query `_snapshot_consistency` |
| conditional formatting | none. **No total, no `risk_score`**. |
| interactions | none |

**Visual settings.** X: `snapshot_date`. Y: `points`. Series: `series` (five components). Legend on.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T25 Risk points by day at the facility · Decision C context (risk_pts_* stocks, each day read on its own day) · as-of: last day of window, capped at H_ent (2026-09-14) · SYNTHETIC ESTATE
// All five components shown separately; emissions and compliance released 2026-10-15 after the enterprise backfill. No sum, no risk_score.
// GATED on G1: emissions, compliance, maintenance and open work are drawn only when G1 passes; otherwise the condition label says they are withheld.
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let ok = toscalar(_snapshot_consistency | project consistent);   // G1 (B1): false while the snapshot predates the current attribution
let withheld = "withheld: enterprise data predates the current attribution (see Data consistency)";
external_table('fact_facility_daily_snapshot')
| where facility_id == _facility_id and snapshot_date >= W_start and snapshot_date < W_end
| mv-expand component = dynamic(["emissions (real plumes via synthetic attribution, max 25)", "compliance: violations in 30 days + open state cases (synthetic, max 20)",
                                 "condition (synthetic, max 30)", "maintenance (synthetic, max 15)", "open work (synthetic, max 10)"]) to typeof(string)
| extend points = case(component startswith "emissions",   risk_pts_emissions,
                       component startswith "compliance",  risk_pts_compliance,
                       component startswith "condition",   risk_pts_condition,
                       component startswith "maintenance", risk_pts_maintenance,
                       risk_pts_open_work)
| where ok or component startswith "condition"
| project snapshot_date, series = strcat(component,
                                         iff(ok, "", " · emissions, compliance, maintenance and open work "), iff(ok, "", withheld)), points
| order by snapshot_date asc, series asc
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

150 rows (defaults, GS-0091); first row: `2026-08-16T00:00:00Z | compliance: violations in 30 days + open state cases (synthetic, max 20) | 0`


#### 6.10 · T26 · SCADA by measurand

| field | value |
|---|---|
| visual | Table |
| position x, y | 0, 33 |
| size w × h | 24 × 6 |
| data source | greensky_v2_rti |
| parameters used | Window (`_startTime`, `_endTime`), Facility (`_facility_id`) |
| conditional formatting | none |
| interactions | none |

**Visual settings.** All columns.

**Query.** Paste as-is. Do not declare the parameters; the dashboard injects them.

```kql
// T26 SCADA by measurand at the facility · measures F13 (re-weighted), F14, F15, R2d coverage · as-of: last bucket_ts in window, capped at H_ent (2026-09-14 23:00) · SYNTHETIC ESTATE
// Measurand = tag prefix + uom (R5d). Values never cross measurands. Ratios are ratios of sums (R2d).
// 1e6 is 02c's fixed-point scale on value_sum_e6 (spec F13). Mean not shown for uom "state" (a state code, not a quantity).
let W_start = startofday(_startTime);
let W_end   = min_of(startofday(_endTime), datetime(2026-09-15));   // H_ent exclusive end (R1b)
let hours_in_window = datetime_diff("hour", W_end, W_start);
let fsk = toscalar(external_table('dim_facility') | where facility_id == _facility_id | project facility_sk);
let h = external_table('scada_telemetry_hourly')
    | where facility_sk == fsk and bucket_ts >= W_start and bucket_ts < W_end
    | extend measurand = strcat(extract(@"\.([A-Z]+)-[0-9]+$", 1, tag_id), " · ", uom);
let n = toscalar(h | summarize count());
union
 (h | summarize tags = count_distinct(tag_id), tag_hours = count(),
                 samples = sum(sample_count), expected = sum(expected_count), good = sum(good_count), running = sum(n_running),
                 sum_e6 = sum(value_sum_e6), vmin = min(value_min), vmax = max(value_max), last_hour = max(bucket_ts)
              by measurand, uom
    | project measurand, tags,
              coverage_pct      = round(100.0 * tag_hours / (tags * hours_in_window), 1),   // share → %
              completeness_pct  = round(100.0 * samples / expected, 1),
              good_share_pct    = round(100.0 * good / samples, 1),
              running_share_pct = round(100.0 * running / samples, 1),
              mean_of_good_readings = iff(uom == "state", real(null), round(todouble(sum_e6) / good / 1e6, 3)),
              min_reading = vmin, max_reading = vmax, unit = uom,
              last_hour_utc = format_datetime(last_hour, "yyyy-MM-dd HH:mm")),
 (print measurand = strcat("no SCADA data for ", _facility_id, " in window") | where n == 0)
| extend facility_id = _facility_id, estate = "synthetic"
| order by measurand asc
```

**Expected output** (the result of this exact query on 2026-10-15, after the release):

10 rows (defaults, GS-0091):

| measurand | tags | coverage_pct | completeness_pct | good_share_pct | running_share_pct | mean_of_good_readings | min_reading | max_reading | unit | last_hour_utc | facility_id | estate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| AT · ratio | 7 | 98.8 | 99.7 | 92.2 | 91.6 | 15.755 | 0 | 18.91 | ratio | 2026-09-14 23:00 | GS-0091 | synthetic |
| BT · state | 1 | 100 | 99.7 | 95.6 | 93.8 |  | 1 | 1 | state | 2026-09-14 23:00 | GS-0091 | synthetic |
| FT · bpd | 3 | 99.8 | 99.7 | 95.3 | 96.6 | 389.375 | 0 | 827 | bpd | 2026-09-14 23:00 | GS-0091 | synthetic |
| FT · mscfd | 11 | 98.1 | 99.5 | 93.7 | 93.5 | 2500.95 | 0 | 4496 | mscfd | 2026-09-14 23:00 | GS-0091 | synthetic |
| LT · percent | 2 | 100 | 99.8 | 98.3 | 100 | 50.495 | 33.5 | 70.6 | percent | 2026-09-14 23:00 | GS-0091 | synthetic |
| PDT · inH2O | 2 | 94.7 | 99.5 | 98.3 | 100 | 101.689 | 33.5 | 174.4 | inH2O | 2026-09-14 23:00 | GS-0091 | synthetic |
| PT · psig | 23 | 99.7 | 99.7 | 92.5 | 91.9 | 381.751 | 2.2 | 1253 | psig | 2026-09-14 23:00 | GS-0091 | synthetic |
| ST · rpm | 6 | 99.6 | 99.7 | 91.8 | 90.9 | 1040.21 | 0 | 1226 | rpm | 2026-09-14 23:00 | GS-0091 | synthetic |
| TT · degF | 16 | 98.8 | 99.7 | 92.4 | 92.1 | 202.063 | 54.5 | 1859 | degF | 2026-09-14 23:00 | GS-0091 | synthetic |
| VT · in/s | 7 | 99.5 | 99.8 | 91.5 | 90.8 | 0.147 | 0 | 0.291 | in/s | 2026-09-14 23:00 | GS-0091 | synthetic |


---

## 7. Page 3: README

Create the page **README**. Add one Text (markdown) tile at x 0, y 0, w 24, h 27, with no title and no parameters. Paste the content below exactly; it is the text between the two ``` lines.

```markdown
# Green Sky — RTI board V2: how to read it

## What is real and what is not
- REAL: the satellite side of every plume — when it was seen, where its source pixel is, the methane in it
  (IME), its emission rate and that rate's P5–P95 band, the ERA5 wind used.
- SYNTHETIC: everything with a facility in it — the 150 facilities (topology_seed 20260915), which facility a
  plume is attributed to, work orders, maintenance, LDAR, sensors, alarms, SCADA. The banner at the top of each
  page says so, and the estate column in the tables marks which rows are real and which are synthetic.
- Facility names are decorative: the town in a name is drawn independently of the coordinates. Read the
  facility_id, never the name, and never read a name as a place.

## Dates
- Satellite detections run to 2026-09-15; the synthetic enterprise and SCADA data run to 2026-09-14.
  Every tile on this board stops at 2026-09-14 so that the two never disagree. One plume (2026-09-15) is
  therefore not shown; the freshness strip counts it.
- The window selector defaults to 2026-08-16 → 2026-09-15 (end exclusive). Pick any part of it. Ranges past
  2026-09-15 are capped. The freshness strip at the top of each page prints the date the page is "as of";
  a tile with a different date (the consistency check, facility identity) prints its own.
- "As of end of <day>" figures (open work orders, overdue maintenance, leaks outstanding, standing alarms,
  risk points) are the state at midnight UTC ending that day. They are never added across days.
- There is no "now". The pipelines are not scheduled; nothing on this board is live.

## Reading the tiles
- Titles are short on purpose. They do not repeat "synthetic": the banner at the top of each page and the
  estate column in the tables mark what is real and what is synthetic.
- Detected plumes, Unattributed plumes, Plumes at this facility: one row per plume, sorted by point rate,
  each with that plume's own P5-P95 band. A facility_id in the detected-plumes table opens that facility's
  page; unattributed plumes have no facility to open.
- Observed plume methane, Daily observed methane, Facilities by observed methane: point estimates in t CH4.
  The ranking's last two rows are the unattributed bucket and the catalog total; facilities plus
  unattributed equal the total.
- Emission rate by plume: three points per plume (point estimate, P5, P95), ranked by point rate. The
  horizontal line is the 25 t/h ultra-emitter threshold of Lauvaux et al. 2022, for reference only.
- Risk points by component, Risk points by day: the five components are stacked for reading. They are
  never added up into a score.
- Each page has a headline (a few large numbers to read first) and, below it, a compact table with the
  rest. On the executive page that table holds the splits and the reconciliation (facilities plus
  unattributed equal the catalog total). On the facility page it holds operations and compliance, which
  are zero for most facilities.
- A headline card that says "none in window" means there is nothing to count, not a measured zero.
- Work orders, maintenance, leaks, sensors, alarms and open cases are the state at midnight UTC ending the
  as-of day.
- Financial impact ("est." on the facility page) is a point estimate in USD for attributed plumes only,
  social cost off. The lost-gas and fines rows are derived from the total.

## Methane numbers
- "Observed plume methane" (t CH4) is the methane in the plume at the moment of the overpass (the IME). It is
  a floor on what was released, not an emission total, and not a daily or annual figure.
- Emission rate (t/h) is a point estimate for one overpass, with the 5th–95th percentile of 500 Monte Carlo
  draws for that plume. Totals are sums of point estimates; no total carries a band.
- No severity tiers. The board sorts by rate. The one line on the distribution chart is the 25 t/h
  "ultra-emitter" threshold of Lauvaux et al. 2022, for reference only. The catalog's median rate (~19.7 t/h)
  sits close to that line, and detection density here runs well above independent inventories while known
  striping artefacts are unresolved, so a high rate is a lead to check, not a confirmed super-emitter.
- "confidence" on a plume (high / medium) is a points grade from wind alignment, uncertainty ratio and pixel
  count. It is not a probability.
- TROPOMI passes over the basin once a day, early afternoon local time (19:35–20:35 UTC in this window). A
  plume is a snapshot at that instant; the board never turns a rate into a daily mass.

## Attribution
- Each plume is matched to the synthetic facilities within 50 km, weighted by distance and by how well the
  facility sits upwind in the ERA5 wind. The number shown is the plume's share among those candidates, not a
  probability that the facility is the source.
- "only candidate within 50 km" means exactly one facility was in range; its share is 1 by construction and is
  not shown as a number.
- attribution_method: wind_aware (ERA5 wind used), distance_only (wind below 0.5 m/s, distance alone),
  no_match (no facility within 50 km). Unattributed plumes are shown as their own bucket on every total,
  and in their own detection table; clicking a facility in the attributed table opens its page.

## Facility ranking and risk
- Facilities are ranked on observed plume methane in the window, then on plume count. There is no single
  risk score on this board.
- The risk-points bars beside the ranking are context: emissions (real plumes, synthetic attribution),
  compliance, condition, maintenance and open work, each shown separately and unitless. They are never
  added up on this board.
- "Open compliance cases, all types" counts every open case. The compliance risk points count something
  else: violations in the last 30 days plus open state cases (venting, flaring). A facility can have open
  cases and zero compliance points.
- Financial impact is the lost-gas value of attributed plume methane at $2.75/mcf plus fines on notices of
  violation, as a point estimate with no uncertainty band. Unattributed methane is not priced.

## Data consistency
- The satellite pipeline and the synthetic-enterprise pipeline run separately. The Data consistency tile
  checks that the enterprise figures were built on the current plume attribution. When it says
  INCONSISTENT, every figure that depends on attribution is withheld, and the tile names the run that
  fixes it.

## Not on this board, and why
- Methane intensity, financial bands and composition, emission signatures, compliance severity, work-order
  trails, LDAR repair performance, equipment-level risk: the serving layer does not hold the tables they need.
- Equipment health index: not shown until its name and meaning are settled.

## SCADA
- Grouped by measurand (tag type + unit); values never mix measurands. The mean is weighted by good readings.
  Coverage is hours with data ÷ hours in the window, over tags that reported at least once. Three facilities
  have no SCADA data at all and say so.
```

---

## 8. Verify the built board

With the parameters at their defaults (window 2026-08-16 → 2026-09-15, facility GS-0091), the board must
show these values. Each tile section above has its full expected output. This list is the quick pass.

| page | tile | must show |
|---|---|---|
| both | T12 | `PASS`, green, 42 / 42 / 0 / 0 / 0 |
| both | T01 | 6 rows; the board row reads `2026-09-14` |
| Executive | T30 | six cards: Plumes detected 49 · Observed plume methane at facilities, t CH₄ 1,445.8 · Median detected rate, t/h 19.7 · Financial impact (point estimate), USD 268,504.28 · Open work orders, total 126 · Notices of violation 3 |
| Executive | T31 | 17 rows: attributed 42 · unattributed 7 · facilities 31 · methane 1,445.8 + 275.2 = 1,721.1, reconciliation 0.0 · largest rate 71.8 (36.4 – 107.9) · lost gas 206,438.77 · fines 62,065.51 · 74 / 52 tickets · 299 PMs · 51 leaks · 150 facilities · 22 open cases |
| Executive | T05 | 147 points (49 plumes × point, P5, P95); horizontal line at 25 |
| Executive | T06 | 13 detection days with bars; bars sum to 1,721.1 t |
| Executive | T07 | 157 markers: 31 facilities with plumes, 119 with no detection, 7 unattributed plumes |
| Executive | T09 | 31 ranked facilities (#1 GS-0091 158.5 t) + UNATTRIBUTED 275.2 t + CATALOG TOTAL 1,721.1 t (49 plumes) |
| Executive | T10 | 31 bars × 5 components; #01 GS-0091 |
| Executive | T11a / T11b | 42 rows / 7 rows; single-candidate rows read "only candidate within 50 km", never 1.00 |
| Facility (GS-0091) | T20 | `GS-0091 · Orla Booster Station · Compression Station · Operator 04 · Midland Basin · 32.3814, -101.772 · commissioned 2014-08-21 · synthetic` |
| Facility (GS-0091) | T32 | Plumes attributed 2 · Observed plume methane, t CH₄ 158.5 · Largest rate, t/h 71.8 · Financial impact (est.), USD 22,627.17 |
| Facility (GS-0091) | T33 | 11 rows: 3 / 0 tickets · 49.3 h · 2 exceedances · 0 offline · 0 alarms · 6 PMs · 0 leaks · 3 cases · 0 notices · band 36.4 – 107.9 t/h |
| Facility (GS-0119) | T32 | all four cards read "none in window" (no plume, no cost) |
| Facility (GS-0091) | T22 | 2 plumes: PL-dd9cfda8fa72 (0.49 share of 15), PL-cecdbc4553e4 (0.86 share of 32) |
| Facility (GS-0091) | T26 | 10 measurands |
| Facility (GS-0119) | T26 / T33 | "no SCADA data for GS-0119 in window" / band "no detection in window" |

**Interaction checks:**
1. **Drill-through from T11a.** Right-click a `facility_id` cell, then Drill through to → Facility
   Operations. The page should open on that facility.
2. **No drill-through from T11b.** Right-clicking a T11b row offers no drill-through.
3. **The window cap.** Set the window to 2026-09-01 → 2026-09-08. T30 should show 2 plumes and 117 open work orders
   (end of 2026-09-07), and T01's board row `[2026-09-01, 2026-09-08)`.
4. **Back to the defaults.** Reset the window to the default before saving.

---

## 9. Build-time checks (the docs could not settle these)

| # | what to check in the editor | if it is not so |
|---|---|---|
| 1 | Scatter chart offers **Y Axis → Reference lines**. Documented for Scatter, Column, Bar, Area, Time chart, KPI and Multi Stat; **not** for Line chart. | Keep T05 as Scatter. If Scatter lacks it, use a **Time chart** with `plume_rank` as X, or tell me before falling back to a constant series. |
| 2 | The editor accepts B1 as a **base query** named `_snapshot_consistency`. | Inline B1 as a `let` at the top of T12, T30, T31, T10, T32, T33 and T25 (§3). |
| 3 | The map visual takes a **size** column, and a series / colour column. | Without size: plain markers. Without series: `marker` stays in the tooltip only; tell me, since R3d wants unattributed plumes visibly distinct. |
| 4 | Multi Stat cards show the **full label** and the **exact value** (no abbreviation, no cut-off label). Check every T30 and T32 label against the 5-characters-per-unit budget (all pass today). | Widen the tile or change its card grid (`slot`). Never shorten the label below its unit. |
| 7 | **Multi Stat minimum = 3 per card column × 3 per card row** of its grid (`slot`). The editor rejects smaller tiles ("…smaller than the minimum supported tile size (w, h)"). Here: T30 grid 3×2 needs 9×6 (is 24×6); T32 grid 4×1 needs 12×3 (is 24×4). | Recompute the minimum whenever the grid changes. To fit more cards, widen or heighten the tile, or change the grid; never shrink below the rule. |
| 8 | The header image renders from `image2url.com`. | Swap in the repo copy `archive/green sky-dev/ref/greensky_header.png`, inline as a base64 data URI or hosted by us. Check the dashboard's `imageAllowlist` first. |
| 5 | Series names (they carry the as-of date for charts T05, T06, T10, T24, T25) are visible in the legend. | Turn the legend on. Tile titles cannot carry a dynamic date. |
| 6 | The dashboard's time display is UTC. | Queries are UTC regardless; only the picker's display is affected. |

---

## 10. Do not

- **Do not** add colour by rate, tonnage or risk to any tile, and do not add a severity tier (Decision A).
- **Do not** add a `risk_score` tile, or a total over the risk components (Decision C).
- **Do not** label E9 ("Open compliance cases, all types") as the compliance risk input. They count
  different case sets (spec Part 1a).
- **Do not** use `now()`, `ago()`, `dcount()` or bare `| count` in any query you add. Do not filter on
  `date_sk`, which reads as a string.
- **Do not** remove the consistency gate (B1/T12) or the gating in the seven tiles. It is what withholds
  stale figures if detection is re-run without enterprise.
- **Re-running the pipelines.** If the pipelines are re-run:
  - **Detection** takes `pipeline_date = 2026-09-15`.
  - **Enterprise** takes `run_mode = backfill` with `pipeline_date` **empty**. Never give enterprise the
    detection's date: 2026-09-15 fails at 02a.
- **Do not** edit the V1 dashboard.
