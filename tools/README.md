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
| `harness_episodes.py` | 03a's own model cell, executed verbatim. Static checks: 03a reads only through `read_input()`, and no observation table appears in its code outside the refusal list; 00_config and 01_topology_config read no table; and every root cause allowed on an instrumented type has a 02b `EPISODE_SIGNATURE` tag on that type. On a synthetic estate shaped like 01b's, with 02a's real state machine: no episode overlaps Maintenance or Down, the rate calibration against the TROPOMI catalogue is printed, two runs are identical, and a 90-day backfill equals 89 incremental days each seeing state only to its own horizon. Negative control: the same incremental runs without the 30-day lookback do **not** match |
| `shared_defs.py` | not a check: the list of definitions 02e copies from 02b, and the `ast` extractor `harness_ch4.py` compares them with |
| `harness_rollup.py` | 02c's rollups are deterministic under any row order, a backfill equals 30 one-day incremental runs bitwise, an incomplete final day converges once re-rolled, day-grain `value_avg` is `good_count`-weighted (and a flat mean would fail), stddev matches raw, and missing hours produce no row |
| `harness_alarm_backing.py` | validation section 1 flags an alarm with no telemetry behind it, and that the earlier filter-after-left-join formulation, and the join without `coalesce`, both did not |
| `harness_alarms.py` | how far the alarm limits sit from centre in units of the generated process sd |
| `harness_alarm_rate.py` | alarm rate per facility-month using 02a's real state machine |
| `harness_final_rate.py` | the realised rate and type mix against the actual `TAG_TEMPLATES` in the repo, plus 02b's envelope and autocorrelation checks |
| `harness_work_orders.py` | 03b's own model cell, executed verbatim. Static: `fact_emission_episode` is named only in the refusal list, every read goes through `read_input()`, and `SENSOR_OFFLINE_MULTIPLE` and the outage cap agree with 02d / 02b. On a synthetic estate and source stream, each upstream table cut to what it would hold at each run's horizon: two backfills identical, a 30-day backfill equal to 1 backfill day + 29 incremental days through the notebook's own `merge_for_write()` and `replaceWhere` semantics, and a rerun of the last day a no-op. Negative controls: pass 1 on `status != 'Closed'` loses the rerun day's closures, and a plan re-drawn per run does not reproduce the backfill. The stream includes 02a trips, and the trip mapping (P1-P4 by criticality, at the trip instant) is checked. The determinism checks run twice, the second time with a 7-day stalled exit so cancellations cross incremental-run boundaries. Over 120 days: open count in the λ×T band with no upward trend, stall share ~5%, breach share by priority in band, nothing open past `WO_CANCEL_AFTER_DAYS`, and the stalled-and-open population levelling off at stall rate × exit rather than growing. The dashboard's two measures are the observable within-SLA / past-SLA split: disjoint, covering exactly the open tickets, matching their trajectories, the backlog levelling off, and disagreeing with the generator-side `is_stalled` split in both directions. Arrival counts here are synthetic, not the calibration |
| `harness_compliance.py` | 03c's own model cell, executed verbatim. Static: `fact_emission_episode` named only in the refusal list, every read through `read_input()`. On a synthetic plume catalogue, CH4 detector flags and flare alarms, each cut to what it would hold at each run's horizon: two backfills identical, a 30-day backfill equal to 1 backfill day + 29 incremental days, and a rerun of the last day a no-op. Negative controls: a case plan re-drawn per run, and a repetition rule with hindsight, each break backfill = incremental. Over 300 days: open non-stalled cases in the λ×T band with no upward trend, stall shares near target, violations only on state cases and fined inside their regulation's range; nothing open past the 120-day stalled exit, the stalled pool bounded at stall rate × exit and well below the no-exit count, and Cancelled exactly the stalled cases that reached it. The determinism checks also run with a 10-day exit so cancellations cross run boundaries |
| `harness_maintenance.py` | 03d's own model cell, executed verbatim. Static: `fact_emission_episode` named only in the refusal list, every read through `read_input()`, `pm_phase_days` character-for-character 02a's (so PM due dates sit on 02a's Scheduled PM stops) and the team roster 03b's. On a synthetic estate with 02a's real state machine and synthetic closed work orders, each cut to each run's horizon: two backfills identical, and a backfill equal to a 60-day prefix + 30 incremental days across all four tables, each run reading its prior state back from the tables it wrote; a rerun of the last day a no-op. Negative control: a PM completion re-drawn per run breaks backfill = incremental. The overdue rebuild is checked unit-proof (microsecond and nanosecond records give the same series, ending at the snapshot). Over 180 days: established assets' overdue PMs stationary (14-day slope; 91-day net rise under half the entries; not strictly rising every week; overdue share under the ceiling), each backlog check with its own negative control (half the estate stops completing PMs at day 90; a slow noisy climb; no PM ever completed), the first-PM cohort inside its bound, PM completion on target, overdue PMs and LDAR outstanding not trending, repairs >= 70% of detections 30 days earlier, Leak Found rising with condition, every preventive record on a Maintenance interval |
| `drift_run.py` | not a pass/fail check: the **multi-seed null** behind 03d's backlog thresholds. One 03d backfill on `harness_maintenance`'s estate under one seed (`python drift_run.py SEED DAYS [OUT_DIR]`, `SEED 0` = the repo's seed), pickling the daily overdue series with entries into overdue, and LDAR detected / repaired / outstanding. 03d's thresholds were set from 8 seeds (`0 11 23 37 41 53 67 79`) at 540 days: run them as separate processes, **about 7 minutes each at 540 days, all 8 in parallel on a laptop** (about 1.5 minutes at 120). **Matched seeds:** a non-zero seed must move 03d's `TOPOLOGY_SEED` **and** `harness_model.TOPOLOGY_SEED` (the harness copy of 02a) together, and the script does both. Move only one and 03d's PM due dates stop landing on 02a's Scheduled PM stops, so PMs never record as done and the overdue share reads **35-43% against ~12%**. That looks like a drift finding and is not one. The script asserts that the two seeds agree and that the established share averages under 25%, so a desynced run fails instead of producing a pickle |
| `drift_analyse.py` | over `drift_run.py`'s pickles (`python drift_analyse.py DIR`, seconds): whether the established overdue level drifts after the first quarter; the lag-1 autocorrelation of weekly means (0.81-0.94, mean 0.89 over the 8 seeds); and how long runs of weekly rises get by chance, measured three ways: real windows, AR(1) at the measured autocorrelation, and iid. The evidence for retiring the consecutive-rise check: six or more rises in 12% of stationary 13-week windows, against 0.1% if weeks were independent. **Rerun this before tightening 03d's backlog checks** |
| `thresh_analyse.py` | over the same pickles (`python thresh_analyse.py DIR`, seconds): every 13-week window's net rise / entries and monotonicity, for established overdue PMs and LDAR outstanding, and the daily overdue share by seed. The margins in the comment above 03d's `NET_RISE_MAX_SHARE` and `OVERDUE_SHARE_MAX` are this output's |

Scripts that others import from guard their own checks behind `if __name__ == "__main__"`,
so importing one to reuse a helper does not run its suite.

`harness_final_rate.py` loads `01_topology_config` from the repo and executes it, so it
reflects the committed configuration rather than a copy.
