# tools

Offline checks for the SCADA notebooks. Nothing here runs in Fabric, and nothing here is
imported by a notebook — these exist to catch defects on a laptop, before a notebook is
synced and run against the lakehouse.

```
tools/
  lint_lit.py      static check for out-of-range Spark literals
  check_nb.py      notebook cell structure and Python syntax
  harness/         offline mirror of the generators' deterministic core
```

---

## `lint_lit.py`

```
python tools/lint_lit.py <notebook-content.py> [...]
```

**What it catches.** Integer literals passed to `F.lit()` that fall outside the Java `long`
range, statically, by walking the AST. Spark builds `F.lit(<python int>)` as a `LongType`
literal, so `F.lit(2**63)` raises `NumberFormatException` while the expression is being
*constructed* — before any data moves, and regardless of what the query would have done.
That is how it got into `02d`'s `alarm_key` and `event_sk`: `% 2**63` is the correct
reduction to 63 bits, but the modulus has to be expressed as a decimal
(`F.lit("9223372036854775808").cast("decimal(20,0)")`) to survive the round trip.

**What it does not catch: anything that needs a JVM.** It executes no Spark expression. It
cannot tell you that a Spark expression computes the same value as its Python counterpart,
nor catch a type mismatch, a null-propagation difference, or a Spark function that behaves
differently from the NumPy mirror. Those are covered only by the golden-vector assertions
inside the notebooks, which fire on Fabric and nowhere else. This file closes exactly one
class of defect — the one that was cheap to close without a JVM.

## `check_nb.py`

```
python tools/check_nb.py <notebook-content.py> [...]
```

Verifies the Fabric file format: every `# CELL` followed by a `# METADATA` block, markdown
cells carrying only comments, the header metadata present, and that the code cells parse as
Python in order. Catches a hand-edited notebook that Fabric would refuse or silently
mis-split.

---

## `harness/`

A pure NumPy/pandas mirror of the deterministic core of `02b_gen_scada_telemetry` and
`02d_gen_alarms`. Same limitation as above: **no Spark, so it verifies the model, not the
translation.** The golden vectors in the notebooks are the only thing that checks Spark
agrees with Python.

Run any script directly; each asserts its own claims and exits non-zero on failure.

| script | what it asserts |
|---|---|
| `harness_model.py` | the mirror itself — hash derivation, spectral synthesis, state conditioning, drift, noise, quantisation, outage and freeze slot grids |
| `harness_run.py` | determinism, window independence (backfill == 30 incremental days), spectral unit variance, realised gap/quality rates, and the golden hash vectors |
| `harness_run9.py` | lag-1 autocorrelation on Running-only rows, the conservative case without state steps |
| `harness_joins.py` | per-day emitted row counts against the dimension, and that the suppressed-slot set does not depend on the window |
| `harness_slots.py` | `window_slots` (whole window) equals the sum of `slots_for_day`, including on day, slot and window boundaries |
| `harness_installs.py` | the install-date chain 01a → 01b → 01d, and the partial-window tag population it produces |
| `harness_outage_slots.py` | outage slot counting against explicit enumeration, on the same phase grid the generator uses |
| `harness_sessionise.py` | 02d's three-window-pass sessioniser is equivalent to an explicit debounce/deadband state machine |
| `harness_alarm_incr.py` | 02d's full-retention scan reproduces the backfill, **and** that the rejected lookback design does not |
| `harness_alarm_keys.py` | `alarm_sk` and `event_sk` are unique at the grain their tables actually have, including when two alarm types raise on the same reading |
| `harness_plume_ids.py` | 04's `scene_id` / `plume_id` from 00_config's own helpers: golden vectors hold, scene labels sort chronologically, IDs survive shuffled rows and a shifted window (where the old counters renumber), session time zones honoured, and 04's own `mc_rates_for` gives each plume bounds that do not move when other plumes are dropped or reordered (where one global seed would). Cannot show two Fabric executions agree -- 04's rerun regression does that |
| `harness_ch4.py` | 02e's copy of 02b's helpers is identical to 02b's text; and, on a synthetic registry and state history, the CH4 model's exceedance rate is in band, concentrated by risk and state, zero in Maintenance and clustered in runs, lag-1 autocorrelation > 0.7 on every sensor, offline share 1-3% with a non-zero 8-hour KPI, and a backfill equal to 30 one-day runs |
| `shared_defs.py` | not a check: the list of definitions 02e copies from 02b, and the `ast` extractor `harness_ch4.py` compares them with |
| `harness_rollup.py` | 02c's rollups are deterministic under any row order, a backfill equals 30 one-day incremental runs bitwise, an incomplete final day converges once re-rolled, day-grain `value_avg` is `good_count`-weighted (and a flat mean would fail), stddev matches raw, and missing hours produce no row |
| `harness_alarm_backing.py` | validation section 1 flags an alarm with no telemetry behind it, and that the earlier filter-after-left-join formulation, and the join without `coalesce`, both did not |
| `harness_alarms.py` | how far the alarm limits sit from centre in units of the generated process sd |
| `harness_alarm_rate.py` | alarm rate per facility-month using 02a's real state machine |
| `harness_final_rate.py` | the realised rate and type mix against the actual `TAG_TEMPLATES` in the repo, plus 02b's envelope and autocorrelation checks |

Scripts that others import from guard their own checks behind `if __name__ == "__main__"`,
so importing one to reuse a helper does not run its suite.

`harness_final_rate.py` loads `01_topology_config` from the repo and executes it, so it
reflects the committed configuration rather than a copy.
