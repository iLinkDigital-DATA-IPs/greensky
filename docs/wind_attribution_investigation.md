# 05 attribution uses no wind: investigation and proposal (2026-10-14)

**Read-only.** No notebook was edited or run. Nothing in Fabric was written, and no policy was altered.

**Reads.**
- Serving table `gold_plume_catalog` via KQL on `greensky_v2_rti`, for the baseline.
- Current Delta snapshots of `silver_plume_ready_pixels` (v5, 34,309 rows), `ref_facilities` (v10, 150 rows)
  and `gold_plume_catalog` (v26, 49 rows), read from OneLake into local Parquet.
- Everything else was computed offline from those copies.

**Scripts** are in the session scratchpad, not the repo:
- `baseline.py`: the baseline CSV.
- `wind_sim.py`, `wind_sim_lib.py`: the offline replica of 05.
- `conv_check.py`: the ERA5 sign check.
- `test_detail.py`: test-plume detail.

**Line references** are to `notebook-content.py` under
`Methane Emissions/Accelerator/Planetary Computer/Varon et al Approach/notebooks/`:
- `04` = `04_core_logic/04_derive_emissions`
- `05` = `05_facility_attribution/05_attribute_facilities .Notebook` (trailing space)
- `03` = `03_joins/03_join_data`

## Summary

| question | answer |
|---|---|
| Baseline | `docs/attribution_baseline_pre_wind_fix.csv`: 49 rows, read from the serving table (KQL, acceleration OFF). 42 attributed, 7 `no_match` with NULLs as empty fields. |
| Does a per-plume u/v exist in 04? | **Yes.** `ime_df` holds `era5_u10` and `era5_v10` as the cluster-pixel mean from 04:1105–1116. They are dropped at the write only because `GOLD_CATALOG_SCHEMA` does not list them (04:1331–1366). |
| Where do they come from? | Per pixel. 03 joins each pixel to the nearest ERA5 grid point at the rounded hour (03:420–456) and writes `era5_u10` / `era5_v10` to `silver_plume_ready_pixels` (03:521–522). |
| **Is the missing column the only defect?** | **No.** 05 compares the facility→plume bearing with the direction the wind blows **from** (05:180, 05:200). A source upwind of its plume has a facility→plume bearing equal to the direction the wind blows **toward**. The comparison is **180° wrong**. Option (a) as written ("05 unchanged") would install the wrong sign. |
| Convention, checked from data | `atan2(u10, v10)` is the direction the wind blows toward, degrees clockwise from north. ERA5 and 03's own Open-Meteo toward-vector agree within 45° on 96.5 % of 33,179 pixels (median 9.4°). |
| Does 04 disagree with 05? | No. Both use `atan2(u, v)` = toward, clockwise from north (04:897, 05:176). 04 folds its alignment to [0°, 90°], so it cannot tell "from" from "toward" and is unaffected. 05 is the only place the direction's sense matters, and it adds 180° in the wrong place. |
| Size of the change | An offline replica of 05 reproduces today's stored attribution exactly (42/42 facility, probability and candidate count; 7/7 no-match). Using the real wind with the **correct** sign changes the winning facility on **32 of 42** plumes. With 05's **as-coded** sign, it changes 20 of 42. The two disagree with each other on 33. |
| Recommendation | Approach (a), **plus** a two-line convention fix in 05, plus removing the silent fallback. Details in Part 3. |
| Sign-error test | **PL-fba050bced02** must go to **GS-0100** (correct sign), not **GS-0129** (flipped), and not stay on **GS-0026** (today, no wind). Details at the end. |

---

## Phase 0. Baseline

Written to `docs/attribution_baseline_pre_wind_fix.csv` (not committed).
- **Source.** The serving external table `greensky_v2_rti.gold_plume_catalog`, `order by plume_id`. Doubles
  are written with `repr()` (full precision); NULLs are empty fields.
- **Contents.** 49 rows: 42 `wind_aware` (attributed) and 7 `no_match` (`facilities_in_range = 0`, every
  attribute empty).
- **Columns.** As requested: `plume_id, source_lat, source_lon, attributed_facility_id,
  attribution_probability, attribution_distance_km, attribution_angular_offset_deg, facilities_in_range,
  second_facility_id, second_facility_probability, attribution_method`.

The lakehouse snapshot of the same table (Delta v26) gives identical attribution: the replica below
reproduces it from `ref_facilities` and a fixed 180°.

---

## Phase 1. What wind 04 has

### 1. Where ERA5 enters, and at what grain

**Per pixel, from silver.** 04 reads `silver_plume_ready_pixels` whole (04:64). Each pixel row carries
`era5_u10`, `era5_v10` and `era5_blh` from 03.

**Aggregated to the plume (cluster) twice, both as the arithmetic mean of the components over the cluster's
pixels.**

First, step 5, wind alignment (04:893–898):
```python
        # Average wind direction from ERA5 u/v components
        mean_u = plume_group["era5_u10"].mean()
        mean_v = plume_group["era5_v10"].mean()
        wind_direction = np.degrees(np.arctan2(mean_u, mean_v)) % 360
        wind_speed = np.sqrt(mean_u**2 + mean_v**2)
```

Second, step 7, emission rate (04:1103–1116):
```python
if len(ime_df) > 0 and len(plumes_pdf) > 0:
    # Merge wind info into IME results
    wind_info = plumes_pdf.groupby("cluster_idx").agg({
        "era5_u10": "mean",
        "era5_v10": "mean",
        "era5_blh": "mean",
        "wind_speed_10m": "mean",
    }).reset_index()
    
    wind_info["era5_wind_speed"] = np.sqrt(
        wind_info["era5_u10"]**2 + wind_info["era5_v10"]**2
    )
    
    ime_df = ime_df.merge(wind_info, on="cluster_idx", how="left")
```

**So a per-plume u/v pair already exists at catalog assembly.** It is `ime_df["era5_u10"]` and
`ime_df["era5_v10"]`, the cluster means. The write keeps only the columns named in `GOLD_CATALOG_SCHEMA`
(04:1361–1366):
```python
    gold_columns = GOLD_CATALOG_SCHEMA.fieldNames()
    
    # Only include columns that exist
    available_columns = [c for c in gold_columns if c in ime_df.columns]
    gold_pdf = ime_df[available_columns].copy()
```
`era5_u10` and `era5_v10` are not in that schema (04:1331–1360), so they are discarded there. No new
aggregation is needed. `wind_df["wind_direction_deg"]` (04:917) is also per plume, from the same means, but
only `wind_alignment_deg` and `wind_confidence` are merged into `ime_df` (04:1120–1123).

**Aggregation options**, listed as asked and not chosen:

| option | what it is | note |
|---|---|---|
| Vector mean of the cluster's pixel u/v | what 04 does today, in both places above | The catalog's `U_eff_ms` is the magnitude of exactly this vector (04:1112–1114, 04:1135). Reusing it keeps direction and speed from one source. |
| Peak pixel's u/v | the value at `source_lat/lon` (04:1050–1052) | A single ERA5 grid point. It is near-identical to the mean when the cluster sits in one grid cell. |
| Enhancement-weighted vector mean | weights each pixel's u/v by its enhancement | A new modelling choice with no precedent in the code. |
| Mean of per-pixel direction angles | — | **Wrong for circular quantities** (350° and 10° average to 180°). Listed only to exclude it. |

### 2. How `U_eff_ms` and `wind_alignment_deg` are computed

- **`U_eff_ms`** = `row["era5_wind_speed"]` (04:1135, stored at 04:1151). That is
  `sqrt(mean_u10² + mean_v10²)` from `wind_info` (04:1112–1114). **It derives from the per-plume u/v pair
  `wind_info["era5_u10"]`, `wind_info["era5_v10"]`.** Only the magnitude survives; the direction is lost at
  the write.
- **`wind_alignment_deg`** = `angle_diff` (04:918), computed in step 5:
  - The plume's orientation is the first principal axis of its pixels.
  - `wind_direction` is computed from `mean_u, mean_v` (04:895–897).
  - `angle_diff` is the angle between the two, folded to [0°, 90°] (04:900–906).

  It derives from the step-5 `mean_u` / `mean_v`, the same numbers as above.

### 3. Does 03 carry u/v into `silver_plume_ready_pixels`?

**Yes, two pairs.**

**ERA5**, as `era5_u10` / `era5_v10` (03:520–523):
```python
    # ERA5 wind (filled or null)
    col("u10").alias("era5_u10"),
    col("v10").alias("era5_v10"),
    col("boundary_layer_height").alias("era5_blh"),
```
`u10` / `v10` come from `bronze_era5_wind`, renamed from the ERA5 file's variables by `02b_ingest_era5`
(02b:243–246). For each pixel, 03 takes the nearest ERA5 grid point at the pixel's hour, rounded to the
nearest hour (03:420–456), and asserts that ≤ 10 % of pixels have null ERA5 (03:468–471).

**Open-Meteo**, as `wind_u` / `wind_v`, derived from the meteorological FROM direction (03:218–229):
```python
# Wind decomposition: speed + direction -> u/v components
# Meteorological convention: direction is where wind comes FROM
# u = east-west component (positive = eastward)
# v = north-south component (positive = northward)

weather = weather.withColumn(
    "wind_u",
    -col("wind_speed_10m") * sin(radians(col("wind_direction_10m")))
).withColumn(
    "wind_v",
    -col("wind_speed_10m") * cos(radians(col("wind_direction_10m")))
)
```
The raw `wind_speed_10m` and `wind_direction_10m` (FROM) are kept too (03:510–513).

---

## Phase 2. The convention

### 4. 05:170–205, verbatim

```python
    # ERA5 u10/v10: u=eastward, v=northward
    # Wind blows FROM upwind direction
    if "U_eff_ms" in plume and plume["U_eff_ms"] > 0:
        # Wind direction the plume came from (upwind)
        # atan2(u, v) gives the direction wind is blowing TO
        # We need where it came FROM (add 180)
        wind_to_deg = np.degrees(np.arctan2(
            plume.get("era5_u10", 0) if "era5_u10" in plume.index else 0,
            plume.get("era5_v10", 0) if "era5_v10" in plume.index else 0
        )) % 360
        upwind_dir = (wind_to_deg + 180) % 360
    else:
        upwind_dir = None

    # Score each facility
    candidates = []
    for i in range(len(fac_lats)):
        dist = haversine_km(fac_lats[i], fac_lons[i], plume_lat, plume_lon)

        if dist > search_radius_km:
            continue

        # Distance weight (Gaussian, sigma = 15 km)
        w_dist = np.exp(-dist**2 / (2.0 * 15.0**2))

        # Wind-direction weight
        if upwind_dir is not None:
            # Bearing from facility to plume
            bearing = compute_bearing(fac_lats[i], fac_lons[i], plume_lat, plume_lon)
            # Is the facility upwind of the plume?
            theta = angular_difference(bearing, upwind_dir)
            w_wind = np.exp(-theta**2 / (2.0 * wind_sigma_deg**2))
        else:
            w_wind = 1.0  # no wind info, equal weight

        score = w_dist * w_wind
```

**What each quantity means, from the code.**

| quantity | meaning | basis |
|---|---|---|
| `atan2(u, v)` | **u first.** For a vector of eastward u and northward v, `atan2(u, v)` is the vector's compass angle, **degrees clockwise from north** (0 = N, 90 = E). | 05:176–179 |
| `wind_to_deg` | ERA5's (u10, v10) is the velocity of the moving air, so this angle is where the air goes: the direction the wind blows **toward**. The comment at 05:174 says so. **Checked from data, below.** | 05:176 |
| `upwind_dir` | `wind_to_deg + 180`: the direction the wind blows **from**, i.e. the meteorological convention. Degrees clockwise from north. | 05:180 |
| `bearing` | `compute_bearing(facility, plume)`: the initial great-circle bearing **from the facility to the plume**, degrees clockwise from north. `x = sin Δλ · cos φ₂`, `y = cos φ₁ sin φ₂ − sin φ₁ cos φ₂ cos Δλ`, `atan2(x, y) mod 360` with point 1 = facility. | 05:136–144, 05:198 |
| `theta` | angle between `bearing` and `upwind_dir` | 05:200 |

**Empirical check of the ERA5 sign** (`conv_check.py`). Over the 33,179 silver pixels where both winds
exceed 1 m/s:
- The angle between ERA5's `atan2(u10, v10)` and Open-Meteo's toward-vector from 03 (`atan2(wind_u, wind_v)`,
  built from a FROM direction with the explicit minus signs above) has a **median of 9.4°**.
- It is under 45° on 96.5 % of pixels and over 135° on 0.1 %.
- Against Open-Meteo's raw FROM direction, the median is 170.6°.

The two independent sources agree that `atan2(u10, v10)` is **toward**. The ERA5 components are therefore
eastward and northward velocity, as ECMWF defines them. I cite that definition from memory; the data check
above is what this report relies on.

### 5. What the code expects of a correct attribution

A plume drifts **downwind** of its source, so the source lies **upwind** of the plume. In bearings:

- bearing(**plume → source**) = the direction the wind comes **from** = `upwind_dir`.
- Equivalently, bearing(**source → plume**) = the direction the wind blows **toward** = `wind_to_deg`.

Worked example: wind from the south-east (from 135°, toward 315°). The source is south-east of the plume. Going
from the source to the plume you head north-west, 315° = `wind_to_deg`.

**05 computes bearing(facility → plume) and compares it with `upwind_dir` (from).** That pairs a
source→plume bearing with a plume→source direction. For the true source, `theta` ≈ 180° and
`w_wind = exp(−180²/1800)` ≈ 1.6 × 10⁻⁸. The facility that is upwind is scored as least likely, and a
facility **downwind** of the plume is scored as most likely. The comment at 05:199 ("Is the facility upwind of
the plume?") states the intent correctly; line 200 implements the opposite.

The correct line is either of:
- `theta = angular_difference(bearing, wind_to_deg)`, keeping `bearing` = facility → plume; or
- keep `upwind_dir`, and compute `bearing` plume → facility (`compute_bearing(plume_lat, plume_lon, fac_lats[i], fac_lons[i])`).

Today the sign error is masked: u = v = 0 gives `wind_to_deg = 0`, `upwind_dir = 180`, and every plume favours
facilities whose bearing to the plume is 180°, i.e. facilities due **north** of the plume.

### 6. Cross-check against 04

| | 04 (step 5) | 05 |
|---|---|---|
| wind angle | `wind_direction = degrees(arctan2(mean_u, mean_v)) % 360` (04:897): **toward**, clockwise from north. The name says "wind_direction", which meteorologically means *from*; **the value is toward.** | `wind_to_deg`, same formula (05:176–179): toward. Then `+180` gives from (05:180). |
| geometry angle | plume orientation `degrees(arctan2(axis[1], axis[0])) % 360` (04:890), where `axis` = (north_km, east_km) from `coords_km` (04:507–512) and `principal_axis` (04:514–530): clockwise from north | `compute_bearing`: clockwise from north (05:136–144) |
| sense | **Undirected.** The orientation is an axis, and `angle_diff` is folded twice (04:900–906) into [0°, 90°]. 04 cannot distinguish from from toward, and it does not need to. | **Directed.** The sense decides which side of the plume the source is on. |

**Conventions agree on units and origin.** Both use degrees, clockwise from north, `atan2(east, north)`. There
is no radians-from-east mismatch.

**There is a naming hazard, not a numeric one.** 04 calls a toward angle `wind_direction_deg` (04:917), the
reverse of meteorological usage. Open-Meteo's `wind_direction_10m` in the same silver table is a FROM angle.
A future change that moves either into 05 under the name "wind direction" would be one rename away from
another 180° error. This is the strongest argument against approach (b) in Part 3.

The cause of 05's error is therefore **not** a 04/05 convention mismatch. It is 05's own pairing of a
facility→plume bearing with a from-direction.

### 7. A test that catches a sign error

**Method.** An offline replica of 05's loop (`wind_sim.py`, 05's formulas copied verbatim), run three ways:

| mode | what it does | result |
|---|---|---|
| FIXED180 | u = v = 0, i.e. today | Reproduces the stored attribution **exactly**: 42/42 facility, probability and `facilities_in_range`; 7/7 no-match. This validates the replica. |
| AS_CODED | real wind, 05:180/200 unchanged, i.e. option (a) as written | Winner differs from today on 20/42. |
| INTENDED | real wind, `theta = angdiff(bearing, wind_to_deg)` | Winner differs from today on 32/42: 30/32 with ≥ 3 candidates, 2/3 with 2, 0/7 with 1. |

AS_CODED and INTENDED pick different winners on 33/42 plumes.

**Per-plume wind.** The replica uses the vector mean of `era5_u10` / `era5_v10` over the silver pixels of the
same overpass (±15 min) within 15 km of the source pixel. This approximates 04's cluster mean; the cluster's
own pixel list is not stored. The source pixel was found for 49/49 plumes. The speed matches the catalog's
`U_eff_ms` with a median relative difference of 1.7 % (max 32 %).

**The test plume: PL-fba050bced02** (detected 2026-08-31 20:08:46Z, source 31.432585, −102.983925,
11 candidates).

ERA5 at the plume: u10 = −3.070 m/s, v10 = +2.901 m/s, 4.22 m/s (catalog `U_eff_ms` 4.26). The wind blows
**from 133.4° (south-east)** toward 313.4°. The source should lie south-east of the plume.

| outcome | facility | position relative to plume | distance | bearing facility → plume | off `wind_to` 313.4° | probability |
|---|---|---|---|---|---|---|
| **today (no wind)** | GS-0026 Stanton Gathering Hub | due **north** | 9.42 km | 181.8° | 131.6° | 0.750 |
| **correct sign** | **GS-0100** Wink Collection Facility | **south-east** (upwind) | 12.05 km | 304.8° | **8.6°** | **1.000** |
| **flipped 180°** | GS-0129 Pecos Gathering Hub 3 | **west** (downwind side) | 6.95 km | 93.4° | 140.0° | 0.306 |

Three outcomes, three different facilities. The result cannot be ambiguous.

**Two more confirmations**, both with ~5.9 m/s winds from the SSE/SE:

| plume | wind from | today | correct sign | flipped |
|---|---|---|---|---|
| PL-290605ca1745 | 141.6° | GS-0026 (0.780) | **GS-0100** (0.995, 3.6° off) | GS-0026 (0.499) |
| PL-9af924cd9b69 | 166.6° | GS-0058 (0.492) | **GS-0056** (0.742, 8.1° off) | GS-0058 (0.381) |

In both, the flipped sign happens to keep today's winner. That is why PL-fba050bced02, where all three
differ, is the primary test.

---

## Phase 3. Proposal (not implemented)

### Approach (a): 04 writes `era5_u10` and `era5_v10`; 05 reads them

**04:**
- **04:1331–1360.** Add two fields to `GOLD_CATALOG_SCHEMA`: `StructField("era5_u10", DoubleType())` and
  `StructField("era5_v10", DoubleType())`. Placing them after `U_eff_ms` keeps the wind columns together.
  - `gold_columns` (04:1361) and the column order derive from this list, so they move with it.
  - `ime_df` already holds both (04:1116), so `available_columns` (04:1364) picks them up with no other
    change.
- **04:1364.** Replace the silent filter with an assertion that every schema column is in `ime_df`. Today a
  schema column missing from `ime_df` is dropped without a word. That is the mirror image of the 05 bug: the
  write would quietly omit `era5_u10`, and 05 would quietly fall back to 0 again.
- **The attribution placeholders** (04:1392–1413) are appended after the base schema and are unaffected.
- **04's validation** (04:1717–1725) checks only `plume_id` / `scene_id` types and counts; unaffected.

**05:**
- **New columns flow through 05 automatically.** 05 captures the catalog's schema as read
  (`CATALOG_SCHEMA = _catalog.schema`, 05:54). It drops only the attribution columns (prefixes `attributed_`
  and `attribution_`, plus three names, 05:330–332). `OUT_SCHEMA = _base + ATTRIBUTION_FIELDS` (05:379–380),
  where `_base` is every catalog field not in that list. `era5_u10` / `era5_v10` match neither prefix, so they
  pass through to the rewrite with their 04 types. No change is needed for flow-through.
- **What 05 does need**, contrary to "05 unchanged":
  1. **05:180 or 05:200, the sign.** Compare the facility→plume bearing with `wind_to_deg` (drop the `+ 180`,
     or rename and use it). Without this, (a) installs the AS_CODED result: 20 winners change, in the wrong
     direction.
  2. **05:172–179, the fallback.** Remove `if "era5_u10" in plume.index else 0`. Assert that both columns
     exist and are finite for every plume. A missing column must fail the run, not become a north wind.
  3. **05:172, the guard.** `plume["U_eff_ms"] > 0` admits PL-8a46231dbd67 at 0.36 m/s, below
     `min_wind_speed_ms` 0.5 (00_config:86), where 04 itself declared the wind too weak and used the 48 h
     fallback. Proposal: use `CONFIG["min_wind_speed_ms"]` here too, and fall to `w_wind = 1.0`
     (distance only) below it. **This is a modelling choice for you to confirm.**
  4. **New assertion.** After scoring, no attributed plume's `attribution_angular_offset_deg` may equal
     `angdiff(bearing, 180)` on every row, which is the frozen-wind signature found here.

**Cost.**
- **Schema change to `gold_plume_catalog`: 39 → 41 columns.** One-time, under `overwriteSchema` (04:1437,
  05:414). Its serving external table has acceleration OFF, which avoids the frozen-catalog failure. Whether
  the KQL external table picks up the two new columns without being recreated is **not known**; check with
  `getschema` after the rerun.
- **Downstream.** The measure spec's "39 columns" becomes 41. 03c, 03e and 06 select catalog columns by name
  and are unaffected.
- **04b resolves its timestamp column by substring** (see "Other silent fallbacks"). `era5_u10` / `era5_v10`
  contain none of "time", "date" or "ts", so it still lands on `detection_date`.
- **Rerun order.** 04 → 05 → 04b, 06, 03c → 03b → 03d → 03e. That is a full detection rerun plus the
  enterprise layer, because the attributions move.

### Approach (b): 04 derives a direction; 05 reads it

**04:**
- **04:1120–1123.** Merge `wind_direction_deg` from `wind_df` into `ime_df`. It is already per plume
  (04:917).
- **Schema.** Add one field, under a name that states its sense, `wind_to_deg`, not `wind_direction_deg`.
- **04:1364.** The same assertion as in (a).

**05:**
- **05:172–182.** Read `wind_to_deg` instead of recomputing from u/v.
- **05:200.** The same sign fix as (a).
- The same fallback and guard changes as (a).

**Cost.** The same rerun chain. One column instead of two, but **05 changes in the same places**: the
sign error is in the comparison, not in where the angle comes from. It also adds a third stored wind
quantity, a derived angle. Only the angle is stored, so it cannot be re-averaged, and its meaning lives in its
name. Section 6 shows how that has already gone wrong once: 04's `wind_direction_deg` is a toward angle, the
reverse of standard usage.

### Recommendation

**Approach (a), with the 05 changes listed under it.** "05 unchanged" is not an option under either approach.

- **Components carry one meaning.** Signed eastward/northward components have one meaning, which the data
  confirms (section 4). An angle needs a convention attached to it.
- **The names already exist.** They are in silver (03:521–522), in 04's working frame (04:1105–1107) and in
  05's reader (05:177–178).
- **05 keeps both direction and magnitude.** That allows the low-wind guard and any later weighting.
- **The convention lives in one place.** The single line in 05 that turns u/v into a bearing to compare with.
  Pair that line with a comment and an assertion, not a column name.

One structural point either way: **one rerun moves ~32 of 42 attributions.** Freeze the Phase 0 baseline
beside the post-fix table and diff them before any facility-keyed number is quoted again. Under the replica:
- GS-0070 (4 plumes today) and GS-0001 (1 today) would have 0.
- GS-0100 would go from 1 to 3.
- 20 facilities would lose every plume; 17 would gain their first.

The measure spec's Decision C discussion of GS-0001 vs GS-0002 would need to be redone.

The 7 single-candidate plumes keep probability 1.0 under any fix. Their candidate set does not depend on wind.
Decision B's single-candidate rule still applies.

---

## Other silent fallbacks found

The task was to grep the catalog's readers (03a, 03c, 03e, 04b, 05, 06, 07b, 07c) for reads of columns 04
does not write.

- **The only read of a column that does not exist is 05:177–178.** Every other reader of `era5_u10` /
  `era5_v10` (07b:124, 07b:148, 07b:537–538; 07c:911, 07c:1357) reads them from silver, where they exist.
- **The same pattern exists elsewhere, dormant today**: a fallback that would hide a missing input.

| location | pattern | today | risk |
|---|---|---|---|
| **04:1364** | `available_columns = [c for c in gold_columns if c in ime_df.columns]`: a schema column absent from `ime_df` is dropped silently | all listed columns present | the write side of this same bug class; would let (a) silently fail |
| **04b:150–162** | timestamp column chosen by name: `detect_ts`, `datetime`, `scene_datetime`, else **the first column whose name contains "time", "date" or "ts"** | the catalog has none of the three names, so it falls through to the heuristic and lands on `detection_date` by **column order** | adding any earlier column containing those substrings changes 04b's time axis without error |
| **04b:165–170** | `RATE_COL = None` if neither rate name is found | `emission_rate_kg_s` present | downstream use of a None rate column was not traced here |
| **03c:789–795** | if `gold_multi_gas_signatures` is absent, every plume's signature becomes `"Undetermined"`, and every state case is then `"Venting"` (03c:464) | the table exists | table-level, not column-level; one missing table silently relabels every Flaring case |
| **04:1278–1280** | `row.get("wind_confidence", "low")`, `row.get("uncertainty_ratio", inf)`, `row.get("n_pixels", 0)` | all present (04:1119–1123 merges them) | if the 04:1119 guard ever skipped the merge, every plume's `confidence` would drop silently |
| **06:196, 06:440** | `if "attributed_facility_*" in columns … else None / set()` | present | a dropped attribution column would read as "no site attributed" |
| **07b:215** | `if "mean_ch4_enhancement_ppb" in plumes_pdf.columns` | present | diagnostic only |

**Recommendation.** Convert 04:1364 and 05:177–178 to assertions as part of this fix. The 04b, 03c, 04, 06
and 07b items are separate, smaller changes; none is wrong today.

---

## The check to run after the rerun

Run against the serving table after 04 and 05 are rerun with the fix:

```kql
external_table('gold_plume_catalog')
| where plume_id in ('PL-fba050bced02', 'PL-290605ca1745', 'PL-9af924cd9b69')
| project plume_id, attributed_facility_id, attribution_probability, attribution_angular_offset_deg,
          era5_u10, era5_v10
| order by plume_id asc
```

| plume_id | **pass: sign correct** | **fail: flipped 180°** | **fail: wind still missing** |
|---|---|---|---|
| PL-fba050bced02 | **GS-0100**, probability ≈ 1.00, offset ≈ 9° | GS-0129 | GS-0026 |
| PL-290605ca1745 | **GS-0100**, ≈ 0.99, offset ≈ 4° | GS-0026 | GS-0026 |
| PL-9af924cd9b69 | **GS-0056**, ≈ 0.74, offset ≈ 8° | GS-0058 | GS-0058 |

`era5_u10` for PL-fba050bced02 should be ≈ −3.1 and `era5_v10` ≈ +2.9. Exact values depend on the cluster's
pixels; the replica used a 15 km neighbourhood.

**By eye.**
- **PL-fba050bced02 decides it alone.** GS-0100 means correct; GS-0129 means flipped; GS-0026 means the wind
  never arrived.
- **Expected offsets.** A correct fix puts the winning facility's `attribution_angular_offset_deg` near 0 on
  these strong-wind plumes. A flipped one puts the true source near 180°, and it will not win.

Then diff the full table against `docs/attribution_baseline_pre_wind_fix.csv`:
- About **32 of 42** winners should change.
- The 7 `no_match` rows and the 7 single-candidate rows should be unchanged in facility and probability.
