# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5d5c8002-789e-4319-81d1-a60f08a77996",
# META       "default_lakehouse_name": "greensky_v2_lakehouse",
# META       "default_lakehouse_workspace_id": "640876ea-6158-4ffd-8598-5eb210e088a0",
# META       "known_lakehouses": [
# META         {
# META           "id": "5d5c8002-789e-4319-81d1-a60f08a77996"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "cf70e84c-e5f3-9589-4218-88cc1ae7b47d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 01d — Build SCADA Tag Registry
#
# Writes **`dim_scada_tag`**, the registry of process measurements — pressure, flow,
# temperature, level, vibration, valve position, rpm, plus the combustion tags.
#
# **This is a second registry, not a replacement.** `dim_sensor` holds the 600 CH₄ detectors
# and is left exactly as it is, because `gold.sensor_telemetry` depends on its schema. Two
# registries keep that contract intact while giving process measurements somewhere of their
# own to live.
#
# ```
# dim_facility → dim_area → dim_equipment → ┬ dim_sensor      (CH₄ detectors, untouched)
#                                           └ dim_scada_tag   (process measurements, new)
# ```
#
# ### Not every asset is instrumented
# An asset is instrumented when its type is instrumentable **and** it clears the criticality
# bar, capped at `MAX_INSTRUMENTED_ASSETS_PER_FACILITY` per site. Valves and pipeline
# segments are never instrumented. This is realistic — real fields meter compressors,
# separators, tanks and metering runs heavily and barely instrument pipe — and it is the
# primary control on telemetry volume: at 15-minute cadence each 1,000 tags costs roughly
# 2.9M rows per 30 days.
#
# Selection is deterministic: eligible assets rank by
# `(criticality_rank, equipment_type_priority, equipment_id)` and the top N per facility are
# taken with a **stable** sort. An unstable sort would let the instrumented set differ
# between runs on ties.
#
# ### Writes
# `dim_scada_tag` only. `dim_facility`, `dim_area`, `dim_equipment` and `dim_sensor` are read
# and not modified. No telemetry, alarms or operating states — those are the next notebooks.

# MARKDOWN ********************

# `00_config` is already run by `01_topology_config`, which needs `CONFIG` and `BBOX` itself.
# It is run explicitly here as well so this notebook's dependencies are visible at the top of
# the file rather than inherited two levels down.

# CELL ********************

%run 00_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run 01_topology_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fac_pdf = (spark.table("dim_facility").filter("is_current = true").toPandas()
                .sort_values("facility_sk").reset_index(drop=True))
area_pdf = (spark.table("dim_area").filter("is_current = true").toPandas()
                 .sort_values(["facility_sk", "area_id"]).reset_index(drop=True))
eq_pdf = (spark.table("dim_equipment").toPandas()
               .sort_values("equipment_sk").reset_index(drop=True))
sen_pdf = spark.table("dim_sensor").toPandas()

assert len(fac_pdf) == N_FACILITIES, (
    f"dim_facility holds {len(fac_pdf)} current rows, expected {N_FACILITIES} -- rerun 01a"
)
for label, frame in (("dim_facility", fac_pdf), ("dim_area", area_pdf),
                     ("dim_equipment", eq_pdf), ("dim_sensor", sen_pdf)):
    assert (frame["topology_seed"] == TOPOLOGY_SEED).all(), (
        f"{label} was built with a different TOPOLOGY_SEED; rerun 01a-01c so the whole "
        "estate comes from one seed"
    )

assert "area_sk" in eq_pdf.columns and "area_id" in eq_pdf.columns, (
    "dim_equipment carries no area columns -- run 01c_build_area_topology first"
)

print(f"facilities {len(fac_pdf)}   areas {len(area_pdf)}   assets {len(eq_pdf):,}   "
      f"CH4 detectors {len(sen_pdf)}  (seed {TOPOLOGY_SEED})")
print(f"dim_sensor is read for cross-reference only and is not modified by this notebook.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Select which assets are instrumented
#
# Eligibility is type plus criticality; the per-facility cap then decides which of the
# eligible assets actually get instruments. The sort is `kind="mergesort"` — pandas' stable
# sort — so ties break identically on every run.

# CELL ********************

elig = eq_pdf[
    eq_pdf["equipment_type"].isin(INSTRUMENTABLE_EQUIPMENT)
    & eq_pdf["criticality"].isin(INSTRUMENTABLE_CRITICALITY)
].copy()

elig["crit_rank"] = elig["criticality"].map(CRITICALITY_RANK)
elig["type_rank"] = elig["equipment_type"].map(INSTRUMENT_PRIORITY)

# STABLE sort. equipment_id is in the key so the order is fully determined even before
# stability matters; mergesort guarantees that a future key change cannot silently make the
# selection depend on input order.
elig = elig.sort_values(
    ["facility_sk", "crit_rank", "type_rank", "equipment_id"],
    kind="mergesort",
).reset_index(drop=True)

instrumented = (elig.groupby("facility_sk", sort=False, group_keys=False)
                    .head(MAX_INSTRUMENTED_ASSETS_PER_FACILITY)
                    .reset_index(drop=True))

n_elig, n_instr = len(elig), len(instrumented)
print(f"assets                      {len(eq_pdf):>6,}")
print(f"  of instrumentable type    {int(eq_pdf['equipment_type'].isin(INSTRUMENTABLE_EQUIPMENT).sum()):>6,}")
print(f"  eligible (type + {'/'.join(sorted(INSTRUMENTABLE_CRITICALITY))})  {n_elig:>6,}")
print(f"  instrumented (cap {MAX_INSTRUMENTED_ASSETS_PER_FACILITY})      {n_instr:>6,}"
      f"   {n_instr/len(eq_pdf):.1%} of the estate")
print(f"  dropped by the cap        {n_elig - n_instr:>6,}")
print()
print("instrumented assets by equipment type:")
for et, n in instrumented["equipment_type"].value_counts().items():
    pool = int((eq_pdf["equipment_type"] == et).sum())
    print(f"  {et:<20}{n:>6}  of {pool:>5} in the estate  ({n/pool:>5.1%})")

for et in sorted(UNINSTRUMENTED_EQUIPMENT):
    assert et not in set(instrumented["equipment_type"]), \
        f"{et} is in UNINSTRUMENTED_EQUIPMENT but was selected for instrumentation"
print()
print(f"never instrumented: {', '.join(sorted(UNINSTRUMENTED_EQUIPMENT))} -- 0 selected")

# Reported, not silently corrected: the policy is the user's to set.
if not (600 <= n_instr <= 900):
    print()
    print(f"NOTE  {n_instr:,} instrumented assets is outside the 600-900 target band.")
    print(f"      The knobs are MAX_INSTRUMENTED_ASSETS_PER_FACILITY "
          f"(currently {MAX_INSTRUMENTED_ASSETS_PER_FACILITY}) and INSTRUMENTABLE_CRITICALITY "
          f"(currently {sorted(INSTRUMENTABLE_CRITICALITY)}).")
    print(f"      {n_elig:,} assets are eligible before the cap binds.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Build the tags
#
# `tag_id` is `GS-0001.A1.PT-101` — facility, area ordinal, ISA instrument code, loop number.
# The loop number is a single counter per **area**, so it is unique within its area
# regardless of instrument code, and the whole `tag_id` is unique estate-wide because the
# facility and area prefix already are.

# CELL ********************

area_by_id = area_pdf.set_index("area_id").to_dict("index")

# Loop numbers run per area, starting at 101, in the deterministic order assets were
# selected -- so a tag's number depends only on the selection order, not on dict iteration.
loop_counter = {}
tag_rows = []

for a in instrumented.itertuples():
    area = area_by_id[a.area_id]
    # area_id is "<facility_id>-A<n>"; the tag_id uses the "A<n>" ordinal.
    area_ordinal = a.area_id.rsplit("-", 1)[-1]

    for tmpl in tag_template_dicts(a.equipment_type):
        loop = loop_counter.get(a.area_id, 100) + 1
        loop_counter[a.area_id] = loop

        tag_id = f"{a.facility_id}.{area_ordinal}.{tmpl['isa']}-{loop}"

        # install_date at or after the asset's, and never after the as-of date.
        trng = get_rng("tag_install", tag_id)
        asset_install = pd.Timestamp(a.install_date)
        max_offset = max(0, (pd.Timestamp(TOPOLOGY_AS_OF) - asset_install).days)
        offset = int(trng.integers(0, min(365, max_offset) + 1)) if max_offset else 0
        install_ts = asset_install + pd.Timedelta(days=offset)

        status = str(trng.choice(["Active", "Faulty", "Decommissioned"],
                                 p=[0.97, 0.02, 0.01]))

        tag_rows.append({
            "tag_sk":           stable_key("tag", tag_id),
            "tag_id":           tag_id,
            "tag_name":         tmpl["tag_name"],
            "equipment_sk":     int(a.equipment_sk),
            "equipment_id":     a.equipment_id,
            "area_sk":          int(a.area_sk),
            "area_id":          a.area_id,
            "facility_sk":      int(a.facility_sk),
            "facility_id":      a.facility_id,
            "measurement_type": tmpl["measurement_type"],
            "uom":              tmpl["uom"],
            "normal_min":       float(tmpl["normal_min"]),
            "normal_max":       float(tmpl["normal_max"]),
            "alarm_lo":         None if tmpl["alarm_lo"] is None else float(tmpl["alarm_lo"]),
            "alarm_lolo":       None if tmpl["alarm_lolo"] is None else float(tmpl["alarm_lolo"]),
            "alarm_hi":         None if tmpl["alarm_hi"] is None else float(tmpl["alarm_hi"]),
            "alarm_hihi":       None if tmpl["alarm_hihi"] is None else float(tmpl["alarm_hihi"]),
            "resolution":       float(tmpl["resolution"]),
            "noise_sigma":      float(tmpl["noise_sigma"]),
            "drift_per_year":   float(tmpl["drift_per_year"]),
            "install_date":     install_ts,
            "status":           status,
            "is_synthetic":     True,
            # working columns for tiering, dropped before the write
            "_crit_rank":       CRITICALITY_RANK[a.criticality],
            "_type_rank":       INSTRUMENT_PRIORITY[a.equipment_type],
        })

tag_pdf = pd.DataFrame(tag_rows)
print(f"{len(tag_pdf):,} tags across {tag_pdf['equipment_id'].nunique():,} instrumented assets")
print(f"tags per instrumented asset: mean {len(tag_pdf)/len(instrumented):.1f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Tier by asset criticality
#
# The hot share goes to the tags on the **most critical assets**, not to a random quarter of
# the estate — that is how a historian is actually configured, and it means the
# high-frequency data sits where an incident is most likely.

# CELL ********************

tag_pdf = tag_pdf.sort_values(
    ["_crit_rank", "_type_rank", "equipment_id", "tag_id"], kind="mergesort"
).reset_index(drop=True)

n_hot = int(np.floor(len(tag_pdf) * HOT_TAG_SHARE))
tag_pdf["tier"] = np.where(np.arange(len(tag_pdf)) < n_hot, "hot", "standard")
tag_pdf["sampling_interval_seconds"] = np.where(
    tag_pdf["tier"] == "hot", HOT_INTERVAL_SECONDS, STANDARD_INTERVAL_SECONDS
).astype("int32")

tag_pdf["effective_from"] = pd.Timestamp(TOPOLOGY_AS_OF)
tag_pdf["effective_to"]   = pd.Timestamp("2999-12-31")
tag_pdf["is_current"]     = True
tag_pdf["topology_seed"]  = TOPOLOGY_SEED

print(f"tier split: hot {int((tag_pdf['tier'] == 'hot').sum()):,} "
      f"({(tag_pdf['tier'] == 'hot').mean():.1%} of tags, target {HOT_TAG_SHARE:.0%})"
      f"   standard {int((tag_pdf['tier'] == 'standard').sum()):,}")
print()
print("criticality of the assets carrying hot tags:")
hot_assets = tag_pdf[tag_pdf["tier"] == "hot"]["equipment_id"].unique()
crit_of = instrumented.set_index("equipment_id")["criticality"].to_dict()
for c in ("Critical", "High", "Medium", "Low"):
    n = sum(1 for e in hot_assets if crit_of.get(e) == c)
    if n:
        print(f"  {c:<10}{n:>5} assets")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Projected telemetry volume
#
# Printed **before** the write, so an unreasonable configuration is visible before any
# telemetry exists to be regretted.

# CELL ********************

SEC_PER_DAY = 86_400
RAW_WINDOW_DAYS = 30
RAW_ROW_CAP_30D = 30_000_000

hot_n = int((tag_pdf["tier"] == "hot").sum())
std_n = int((tag_pdf["tier"] == "standard").sum())
hot_per_day = SEC_PER_DAY / HOT_INTERVAL_SECONDS
std_per_day = SEC_PER_DAY / STANDARD_INTERVAL_SECONDS

hot_rows_day = hot_n * hot_per_day
std_rows_day = std_n * std_per_day
rows_day = hot_rows_day + std_rows_day
rows_30d = rows_day * RAW_WINDOW_DAYS

history_days = (pd.Timestamp(TOPOLOGY_AS_OF) - tag_pdf["install_date"].min()).days
hourly_rows = len(tag_pdf) * 24 * history_days
daily_rows = len(tag_pdf) * history_days

print("=" * 76)
print("PROJECTED TELEMETRY VOLUME")
print("=" * 76)
print(f"  {'tier':<12}{'tags':>9}{'interval':>11}{'readings/tag/day':>19}{'rows/day':>14}")
print("  " + "-" * 66)
print(f"  {'hot':<12}{hot_n:>9,}{f'{HOT_INTERVAL_SECONDS}s':>11}"
      f"{hot_per_day:>19,.0f}{hot_rows_day:>14,.0f}")
print(f"  {'standard':<12}{std_n:>9,}{f'{STANDARD_INTERVAL_SECONDS}s':>11}"
      f"{std_per_day:>19,.0f}{std_rows_day:>14,.0f}")
print("  " + "-" * 66)
print(f"  {'total':<12}{len(tag_pdf):>9,}{'':>11}{'':>19}{rows_day:>14,.0f}")
print()
print(f"  raw, {RAW_WINDOW_DAYS}-day window     {rows_30d:>18,.0f} rows")
print(f"    of which hot         {hot_rows_day * RAW_WINDOW_DAYS:>18,.0f} rows"
      f"  ({hot_rows_day / rows_day:.0%})")
print(f"    of which standard    {std_rows_day * RAW_WINDOW_DAYS:>18,.0f} rows"
      f"  ({std_rows_day / rows_day:.0%})")
print()
print(f"  full history is {history_days:,} days (earliest tag install to the as-of date)")
print(f"  hourly rollup, full history  {hourly_rows:>18,.0f} rows")
print(f"  daily rollup, full history   {daily_rows:>18,.0f} rows")
print()
gb_30d = rows_30d * ASSUMED_BYTES_PER_TELEMETRY_ROW / 1024 ** 3
gb_hourly = hourly_rows * ASSUMED_BYTES_PER_TELEMETRY_ROW / 1024 ** 3
print(f"  ESTIMATED storage at {ASSUMED_BYTES_PER_TELEMETRY_ROW} bytes/row:")
print(f"    raw {RAW_WINDOW_DAYS}-day window   {gb_30d:>8.2f} GB")
print(f"    hourly rollup        {gb_hourly:>8.2f} GB")
print()
print("  BYTES-PER-ROW IS AN ASSUMPTION, NOT A MEASUREMENT. It is a guess at a narrow")
print("  Delta row (tag_sk, timestamp, value, quality) after Parquet encoding, set in")
print("  01_topology_config as ASSUMED_BYTES_PER_TELEMETRY_ROW. Measure the real figure")
print("  once telemetry exists and correct it there; the row counts above are exact")
print("  arithmetic, the storage figures are not.")
print("=" * 76)

assert rows_30d < RAW_ROW_CAP_30D, (
    f"projected {RAW_WINDOW_DAYS}-day raw telemetry is {rows_30d:,.0f} rows, over the "
    f"{RAW_ROW_CAP_30D:,} cap. Turn one of three knobs in 01_topology_config: "
    f"MAX_INSTRUMENTED_ASSETS_PER_FACILITY (now {MAX_INSTRUMENTED_ASSETS_PER_FACILITY}, "
    f"fewer instrumented assets), HOT_TAG_SHARE (now {HOT_TAG_SHARE:.0%}, fewer tags at the "
    f"fast cadence), or the cadences themselves (now {HOT_INTERVAL_SECONDS}s / "
    f"{STANDARD_INTERVAL_SECONDS}s)."
)
print(f"OK  projected {rows_30d:,.0f} rows over {RAW_WINDOW_DAYS} days is under the "
      f"{RAW_ROW_CAP_30D:,} cap "
      f"({rows_30d / RAW_ROW_CAP_30D:.0%} of it)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validation — every check fails the run, none warns

# CELL ********************

# --- keys -----------------------------------------------------------------------------------
assert tag_pdf["tag_id"].is_unique, "tag_id is not unique"
assert tag_pdf["tag_sk"].is_unique, "tag_sk is not unique -- stable_key collision?"

key_cols = ["tag_sk", "tag_id", "equipment_sk", "equipment_id", "area_sk", "area_id",
            "facility_sk", "facility_id", "measurement_type", "uom", "tier",
            "sampling_interval_seconds", "normal_min", "normal_max", "resolution",
            "noise_sigma", "drift_per_year", "install_date", "status"]
nulls = tag_pdf[key_cols].isna().sum()
assert nulls.sum() == 0, f"nulls in dim_scada_tag key columns:\n{nulls[nulls > 0]}"

assert set(tag_pdf["measurement_type"]) <= MEASUREMENT_TYPES, (
    f"unknown measurement_type(s): {sorted(set(tag_pdf['measurement_type']) - MEASUREMENT_TYPES)}"
)
assert set(tag_pdf["tier"]) <= {"hot", "standard"}, "unknown tier value"
assert set(tag_pdf["status"]) <= {"Active", "Faulty", "Decommissioned"}, "unknown status value"

# --- referential integrity, including the area cross-check ------------------------------------
eq_area = eq_pdf.set_index("equipment_id")[["area_sk", "area_id", "facility_id"]]
unknown_eq = set(tag_pdf["equipment_id"]) - set(eq_area.index)
assert not unknown_eq, f"tags on unknown assets: {sorted(unknown_eq)[:5]}"

unknown_area = set(tag_pdf["area_id"]) - set(area_pdf["area_id"])
assert not unknown_area, f"tags referencing unknown areas: {sorted(unknown_area)[:5]}"

unknown_fac = set(tag_pdf["facility_id"]) - set(fac_pdf["facility_id"])
assert not unknown_fac, f"tags referencing unknown facilities: {sorted(unknown_fac)[:5]}"

# A tag's area must be the area its ASSET belongs to. This is the check that catches a tag
# attached to the right facility but the wrong area.
chk = tag_pdf.join(eq_area, on="equipment_id", rsuffix="_eq")
mis_area = chk[(chk["area_id"] != chk["area_id_eq"]) | (chk["area_sk"] != chk["area_sk_eq"])]
assert mis_area.empty, (
    f"{len(mis_area)} tag(s) whose area does not match their asset's area:\n"
    f"{mis_area[['tag_id', 'equipment_id', 'area_id', 'area_id_eq']].head().to_string(index=False)}"
)
mis_fac = chk[chk["facility_id"] != chk["facility_id_eq"]]
assert mis_fac.empty, f"{len(mis_fac)} tag(s) whose facility does not match their asset's"

# area_sk must agree with dim_area's own mapping, not just with dim_equipment's copy
area_sk_map = area_pdf.set_index("area_id")["area_sk"].to_dict()
bad_sk = tag_pdf[tag_pdf["area_id"].map(area_sk_map) != tag_pdf["area_sk"]]
assert bad_sk.empty, f"{len(bad_sk)} tag(s) whose area_sk disagrees with dim_area"

# --- instrumented set ---------------------------------------------------------------------------
tagged = set(tag_pdf["equipment_id"])
selected = set(instrumented["equipment_id"])
assert tagged == selected, (
    f"instrumented assets without tags: {sorted(selected - tagged)[:5]}; "
    f"tags on assets never selected: {sorted(tagged - selected)[:5]}"
)

uninstr_types = eq_pdf[eq_pdf["equipment_type"].isin(UNINSTRUMENTED_EQUIPMENT)]["equipment_id"]
leaked = tagged & set(uninstr_types)
assert not leaked, f"tags on never-instrumented equipment types: {sorted(leaked)[:5]}"

below_bar = eq_pdf[~eq_pdf["criticality"].isin(INSTRUMENTABLE_CRITICALITY)]["equipment_id"]
leaked_crit = tagged & set(below_bar)
assert not leaked_crit, (
    f"tags on assets below the criticality bar: {sorted(leaked_crit)[:5]}"
)

per_fac = instrumented.groupby("facility_id").size()
over = per_fac[per_fac > MAX_INSTRUMENTED_ASSETS_PER_FACILITY]
assert over.empty, (
    f"{len(over)} facility/facilities exceed the cap of "
    f"{MAX_INSTRUMENTED_ASSETS_PER_FACILITY}:\n{over.head().to_string()}"
)

# --- alarm ordering ------------------------------------------------------------------------------
order = ["alarm_lolo", "alarm_lo", "normal_min", "normal_max", "alarm_hi", "alarm_hihi"]
viol = []
for r in tag_pdf.itertuples():
    vals = [(c, getattr(r, c)) for c in order]
    present = [(c, v) for c, v in vals if v is not None and not pd.isna(v)]
    for (c1, v1), (c2, v2) in zip(present, present[1:]):
        if v1 > v2:
            viol.append((r.tag_id, r.tag_name, c1, v1, c2, v2))
            break
assert not viol, (
    f"{len(viol)} tag(s) with alarm limits out of order (expected "
    f"alarm_lolo <= alarm_lo <= normal_min <= normal_max <= alarm_hi <= alarm_hihi "
    f"over the values that are present): {viol[:5]}"
)

# --- install_date ---------------------------------------------------------------------------------
eq_install = eq_pdf.set_index("equipment_id")["install_date"]
early = tag_pdf[tag_pdf["install_date"].values
                < pd.to_datetime(tag_pdf["equipment_id"].map(eq_install)).values]
assert early.empty, (
    f"{len(early)} tag(s) installed before their asset:\n"
    f"{early[['tag_id', 'equipment_id', 'install_date']].head().to_string(index=False)}"
)
future = tag_pdf[tag_pdf["install_date"] > pd.Timestamp(TOPOLOGY_AS_OF)]
assert future.empty, f"{len(future)} tag(s) installed after the as-of date"

# --- tiering ----------------------------------------------------------------------------------------
bad_interval = tag_pdf[
    ((tag_pdf["tier"] == "hot") & (tag_pdf["sampling_interval_seconds"] != HOT_INTERVAL_SECONDS))
    | ((tag_pdf["tier"] == "standard")
       & (tag_pdf["sampling_interval_seconds"] != STANDARD_INTERVAL_SECONDS))
]
assert bad_interval.empty, f"{len(bad_interval)} tag(s) whose interval disagrees with their tier"

print("OK  tag_sk / tag_id unique, no nulls in key columns")
print("OK  every tag resolves to a valid asset, area and facility")
print("OK  every tag's area matches its asset's area (no cross-area attachment)")
print(f"OK  all {len(selected):,} instrumented assets carry tags; no tag on an "
      "uninstrumented asset")
print(f"OK  no facility exceeds the cap of {MAX_INSTRUMENTED_ASSETS_PER_FACILITY} "
      f"(max observed {int(per_fac.max())})")
print("OK  alarm limits ordered wherever present")
print("OK  every tag installed at or after its asset, and not after the as-of date")
print("OK  sampling interval agrees with tier for every tag")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Distributions

# CELL ********************

tpf = tag_pdf.groupby("facility_id").size()
tpa = tag_pdf.groupby("equipment_id").size()

print("tags per facility:")
print(f"  min {int(tpf.min())}   p25 {int(np.percentile(tpf, 25))}   "
      f"median {int(tpf.median())}   p75 {int(np.percentile(tpf, 75))}   max {int(tpf.max())}"
      f"   mean {tpf.mean():.1f}")
print(f"  facilities with no tag at all: {N_FACILITIES - tpf.shape[0]}")

print()
print("tags per instrumented asset, by equipment type:")
byt = tag_pdf.merge(eq_pdf[["equipment_id", "equipment_type"]], on="equipment_id")
for et in sorted(INSTRUMENTABLE_EQUIPMENT, key=lambda e: INSTRUMENT_PRIORITY[e]):
    sub = byt[byt["equipment_type"] == et]
    if sub.empty:
        continue
    n_assets = sub["equipment_id"].nunique()
    expected = len(TAG_TEMPLATES[et])
    print(f"  {et:<20}{n_assets:>5} assets x {expected} tags = {len(sub):>6} tags")

print()
print("measurement_type distribution:")
for mt, n in tag_pdf["measurement_type"].value_counts().items():
    print(f"  {mt:<18}{n:>7,}  ({n/len(tag_pdf):>5.1%})")

print()
print("tier split:")
for tier, n in tag_pdf["tier"].value_counts().items():
    secs = HOT_INTERVAL_SECONDS if tier == "hot" else STANDARD_INTERVAL_SECONDS
    print(f"  {tier:<10}{n:>7,}  ({n/len(tag_pdf):>5.1%})  at {secs}s")

print()
print("status:")
for s, n in tag_pdf["status"].value_counts().items():
    print(f"  {s:<18}{n:>7,}  ({n/len(tag_pdf):>5.1%})")

print()
print("uom:")
for u, n in tag_pdf["uom"].value_counts().items():
    print(f"  {u:<10}{n:>7,}")

# Cross-reference against the CH4 detector registry -- the two are independent, and an asset
# may carry both a process tag and a methane detector.
both = set(tag_pdf["equipment_id"]) & set(sen_pdf["equipment_id"])
print()
print(f"assets carrying BOTH a CH4 detector and SCADA tags: {len(both)}")
print(f"  of {sen_pdf['equipment_id'].nunique()} detector-bearing and "
      f"{tag_pdf['equipment_id'].nunique()} tag-bearing assets")
print("  the two registries are independent by design; overlap is expected, not required")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### One instrumented compressor, in full
#
# The naming eyeball check.

# CELL ********************

comp = tag_pdf.merge(eq_pdf[["equipment_id", "equipment_type", "criticality"]],
                     on="equipment_id")
comp = comp[comp["equipment_type"] == "Compressor"]

if comp.empty:
    print("no instrumented compressor in this estate")
else:
    eid = comp.sort_values("tag_id", kind="mergesort")["equipment_id"].iloc[0]
    sub = comp[comp["equipment_id"] == eid].sort_values("tag_id", kind="mergesort")
    a0 = sub.iloc[0]
    arow = area_by_id[a0["area_id"]]
    frow = fac_pdf[fac_pdf["facility_id"] == a0["facility_id"]].iloc[0]

    print(f"{frow['facility_name']}  [{frow['facility_type']}]")
    print(f"  area   {arow['area_name']}  ({a0['area_id']}, {arow['criticality']})")
    print(f"  asset  {eid}  Compressor, criticality {a0['criticality']}")
    print(f"  tags   {len(sub)}")
    print()
    print(f"  {'tag_id':<24}{'tag_name':<20}{'meas type':<16}{'uom':<8}{'tier':<10}"
          f"{'normal band':>20}{'alarms lo/hi':>26}")
    print("  " + "-" * 122)
    for r in sub.itertuples():
        band = f"{r.normal_min:g} .. {r.normal_max:g}"
        lo = "-" if pd.isna(r.alarm_lolo) else f"{r.alarm_lolo:g}/{r.alarm_lo:g}" \
            if not pd.isna(r.alarm_lo) else f"{r.alarm_lolo:g}/-"
        hi = "-" if pd.isna(r.alarm_hihi) else f"{r.alarm_hi:g}/{r.alarm_hihi:g}" \
            if not pd.isna(r.alarm_hi) else f"-/{r.alarm_hihi:g}"
        print(f"  {r.tag_id:<24}{r.tag_name:<20}{r.measurement_type:<16}{r.uom:<8}"
              f"{r.tier:<10}{band:>20}{f'{lo}  |  {hi}':>26}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write `dim_scada_tag`

# CELL ********************

TAG_SCHEMA = [
    "tag_sk", "tag_id", "tag_name",
    "equipment_sk", "equipment_id", "area_sk", "area_id", "facility_sk", "facility_id",
    "measurement_type", "uom", "tier", "sampling_interval_seconds",
    "normal_min", "normal_max", "alarm_lo", "alarm_lolo", "alarm_hi", "alarm_hihi",
    "resolution", "noise_sigma", "drift_per_year",
    "install_date", "status", "is_synthetic",
    "effective_from", "effective_to", "is_current", "topology_seed",
]

tag_out = tag_pdf.drop(columns=[c for c in tag_pdf.columns if c.startswith("_")])
assert set(tag_out.columns) == set(TAG_SCHEMA), (
    f"dim_scada_tag schema drift: unexpected {sorted(set(tag_out.columns) - set(TAG_SCHEMA))}, "
    f"missing {sorted(set(TAG_SCHEMA) - set(tag_out.columns))}"
)
tag_out = tag_out[TAG_SCHEMA]

# Nullable alarm columns must land as double, not object, or Spark infers the wrong type.
for c in ("alarm_lo", "alarm_lolo", "alarm_hi", "alarm_hihi"):
    tag_out[c] = pd.to_numeric(tag_out[c], errors="coerce").astype("float64")

dim_scada_tag = spark.createDataFrame(tag_out)
(dim_scada_tag.write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable("dim_scada_tag"))
print(f"dim_scada_tag: {dim_scada_tag.count():,} rows x {len(tag_out.columns)} columns")

display(dim_scada_tag.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Verify the join path
#
# tag → asset → area → facility, end to end.

# CELL ********************

chain = spark.sql("""
    SELECT t.tag_id, t.measurement_type, t.tier,
           e.equipment_id, e.equipment_type, e.criticality,
           a.area_name, a.area_type,
           f.facility_id, f.facility_name, f.facility_type
    FROM dim_scada_tag t
    JOIN dim_equipment e ON t.equipment_id = e.equipment_id
    JOIN dim_area a      ON t.area_id = a.area_id AND a.is_current = true
    JOIN dim_facility f  ON t.facility_id = f.facility_id AND f.is_current = true
""")

n_tags = spark.table("dim_scada_tag").count()
n_chain = chain.count()
assert n_chain == n_tags, (
    f"tag -> asset -> area -> facility join returned {n_chain:,} rows for {n_tags:,} tags "
    "-- the chain is not 1:1"
)
print(f"OK  all {n_chain:,} tags resolve through asset and area to a facility")

display(chain.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary

# CELL ********************

print("=" * 76)
print("SCADA TAG REGISTRY BUILT")
print("=" * 76)
print(f"  seed                  {TOPOLOGY_SEED}")
print(f"  instrumented assets   {len(instrumented):,} of {len(eq_pdf):,} "
      f"({len(instrumented)/len(eq_pdf):.1%})")
print(f"  tags                  {len(tag_pdf):,}")
print(f"  tags per asset        mean {len(tag_pdf)/len(instrumented):.1f}")
print(f"  tier split            {hot_n:,} hot / {std_n:,} standard")
print(f"  projected 30-day raw  {rows_30d:,.0f} rows  (cap {RAW_ROW_CAP_30D:,})")
print()
print("  table written         dim_scada_tag (new)")
print("  not modified          dim_facility, dim_area, dim_equipment, dim_sensor")
print()
print("  dim_sensor remains the CH4 detector registry and keeps its contract, so")
print("  gold.sensor_telemetry is unaffected. This registry holds process measurements only.")
print()
print("  next                  telemetry generation, then alarms and operating states")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
