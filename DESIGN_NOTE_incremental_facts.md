# Design Note — Incremental Fact Generation and Steady-State Backlogs

**Status:** proposed. Not yet implemented.
**Applies to:** every enterprise fact generator — work orders, compliance events,
maintenance, inspections, LDAR surveys, alarms, financial impact.
**Written because:** the V1 generators were built for one-shot backfill and produce
unbounded backlogs when run daily.

---

## 1. The problem

V1's `gen_work_orders` decides whether a ticket is open or closed **at creation time**:
roughly 85% get a resolution time sampled immediately, the remaining 15% stay open with a
null closure and never change.

That is a reasonable way to manufacture a plausible snapshot in a single backfill run. It
is wrong for a pipeline that runs daily, because each run adds new open tickets and never
retires the ones already there. Open work orders accumulate without bound. The dashboard's
maintenance-backlog KPI climbs every day, and after a few weeks the demo shows an
operation in collapse — not because anything is modelled as failing, but because the
generator has no concept of work finishing.

The same shape of error applies to anything the dashboard presents as a **backlog**:

| Dashboard element | Fills from | Needs to drain via |
|---|---|---|
| Open Work Orders | compliance events, alarms, inspections | ticket closure |
| LDAR Leak Repairs Performance | leaks detected | leaks repaired |
| Overdue Preventive Maintenance | PM schedule falling due | PM completion |
| Active Exceedances Now | sensor readings above threshold | readings returning to normal |
| Compliance Violations (open) | OLRE detections | case resolution |

A backlog with an inflow and no outflow is not a model of anything. Every one of these
needs both.

---

## 2. Three distinct requirements

These are often conflated. They are separate and all three are needed.

### 2.1 Topology is not a daily job

`01_topology_config`, `01a_build_facility_topology` and `01b_build_asset_topology` are
slowly-changing dimensions. They are regenerated deliberately — when the seed, the scale
or the taxonomy changes — not on a schedule.

They belong in a **setup pipeline**, run on demand. Two reasons beyond tidiness:

- `TOPOLOGY_AS_OF` bounds `commission_date` and `install_date`. If it is ever bumped while
  facts already exist, every asset age shifts underneath them and derived quantities such
  as the equipment condition index move without any fact table changing.
- Regenerating the estate re-keys `equipment_sk` and `facility_sk`. Anything already
  written against the old keys is orphaned.

**Rule:** the daily pipeline reads the topology. It never writes it.

### 2.2 Fact writes must be window-scoped and idempotent

V1 used `mode("overwrite")` for backfill and `mode("append")` for incremental, with no
deduplication. Re-running a day appends the same events a second time.

Every fact table gets a `date_sk` partition and is written with `replaceWhere` scoped to
the run window:

```python
(df.write.format("delta")
   .mode("overwrite")
   .option("replaceWhere", f"date_sk >= {start_sk} AND date_sk <= {end_sk}")
   .partitionBy("date_sk")
   .saveAsTable(table))
```

Re-running any window then produces identical content, and a backfill and an incremental
run over the same day are indistinguishable. This also requires that event generation be a
deterministic function of `(entity, date, seed)` rather than depending on iteration order
or on how the window was sliced.

**Rule:** re-running a day replaces that day. It never adds to it.

### 2.3 Backlogs need state transitions, not creation-time outcomes

This is the substantive change, and it alters the shape of the generators.

Each daily run performs **two passes**:

**Pass 1 — advance existing work.** Read entities still open as of the previous run. For
each, decide whether it transitions today. Write the transition.

**Pass 2 — create new work.** Generate today's arrivals from the day's compliance events,
alarms, LDAR findings and inspections.

V1 has no pass 1. Adding it is what makes the open population converge.

---

## 3. The steady-state model

For any backlog with arrival rate `λ` (items per day) and mean time to resolution `T`
(days), the open population converges to:

```
steady_state_open ≈ λ × T
```

This is Little's Law. It gives both a design target and a validation threshold.

### Worked example — work orders

Current detection rate is roughly 75 plumes over a 30-day window, so ~2.5 plumes per day.
Not every plume produces a compliance event, and not every compliance event produces a
ticket. Suppose:

| Quantity | Value | Source |
|---|---|---|
| Plumes per day | ~2.5 | `gold_plume_catalog` over the run window |
| Share producing a compliance event | ~0.4 | OLRE threshold in `gen_compliance` |
| Tickets per compliance event | ~1.0 | one per violation |
| Plus alarm-sourced, LDAR-sourced, inspection-sourced | ~2 | proposed new sources |
| **Arrival rate λ** | **~3 / day** | |
| Mean time to close T, weighted across P1–P4 | ~5 days | SLA 24 / 72 / 168 / 336 hours |
| **Steady-state open** | **~15** | λ × T |

So the expected open population is on the order of 15, not a number that grows daily. If
the dashboard shows 40 open work orders after a month of daily runs, the drain is broken.

Note this arithmetic is illustrative — the arrival rate should be measured from the actual
generators once they exist, not assumed. What matters is that a target exists at all.

### Per-priority resolution

Resolution time should depend on priority, matching the existing SLA vocabulary:

| Priority | SLA | Mean resolution | Share breaching SLA |
|---|---|---|---|
| P1 Critical | 24 h | ~18 h | ~20% |
| P2 High | 72 h | ~55 h | ~20% |
| P3 Medium | 168 h | ~140 h | ~25% |
| P4 Low | 336 h | ~300 h | ~30% |

The SLA-breach share is what makes `is_breached` meaningful. If nothing breaches, the
metric is decoration; if everything does, it is noise.

### Closure must be deterministic

Pass 1 must not re-roll a random draw each day, or a ticket's fate would change every time
the pipeline runs. Instead, fix the intended resolution duration **at creation time**,
derived deterministically from `(ticket_id, seed)`, and have pass 1 simply close anything
whose `created_ts + resolution_hours` has now elapsed.

This means:
- The close decision is a function of elapsed time, not of chance on the day
- Re-running any day produces the same closures
- A backfill over 30 days and 30 incremental runs produce identical tables

A small share should be left genuinely unresolved — stalled work exists — but that share
must also be fixed at creation, not re-drawn.

---

## 4. What this means per fact table

### `fact_work_order`

- Fix `resolution_hours` and `will_stall` at creation from `(ticket_id, seed)`
- Pass 1: close tickets whose elapsed time has passed; emit rows into `workorder_event`
- Pass 2: create from compliance events, alarms, LDAR leaks found, and inspections with
  `result = 'Leak Found'` — **not** from `fact_emission_episode`, which is hidden ground
  truth and must not leak into an operational table
- Validation: open count within a configured band; assert it is not trending upward across
  the window

### `fact_ldar_survey`

- Leaks detected and leaks repaired already both exist as columns, but repairs are sampled
  at survey time as a fraction of detections
- Change to a carried backlog: leaks found at one survey are repaired across subsequent
  days, so `leaks_detected - leaks_repaired` is a real outstanding count rather than a
  per-survey ratio
- Validation: cumulative repaired approaches cumulative detected with a realistic lag

### `fact_pm_schedule` (proposed, from the design addendum)

- Each asset carries `next_due_ts` from its `inspection_frequency_days`
- Pass 1: PMs falling due become overdue; completed PMs advance `next_due_ts`
- Compliance rate should be a configured parameter — say 80–90% completed on time — so
  overdue count is stable rather than monotonic
- Validation: overdue count stable; mean days overdue within a band

### `fact_compliance_event`

- Events are point-in-time, so no drain is needed for the event itself
- But `status` (Reported → Under Review → Closed) is a state machine and needs the same
  two-pass treatment if the dashboard shows open violations

### `fact_scada_alarm` (proposed)

- Alarms raise and clear within the telemetry itself, so the drain is intrinsic
- `acknowledged_ts` is a separate operator action and needs pass 1 treatment if surfaced

---

## 5. Pipeline shape

```
SETUP PIPELINE  (on demand, never scheduled)
    01_topology_config
    01a_build_facility_topology
    01b_build_asset_topology
    [SCADA topology: dim_area, dim_scada_tag]

DAILY PIPELINE  (scheduled)
    02_ingest_*  →  03_join_data  →  04_derive_emissions  →  04b  →  05  →  06
        ↓
    PASS 1 — advance open state
        close due work orders
        complete due PMs, mark newly overdue
        repair outstanding LDAR leaks
        acknowledge and clear alarms
        ↓
    PASS 2 — create new events from the day's detections
        compliance events from today's plumes
        work orders from compliance, alarms, LDAR, inspections
        financial impact from today's attributed plumes
        ↓
    VALIDATE — steady-state checks, fail if a backlog is trending
        ↓
    build_snapshots
```

---

## 6. Prerequisites

Two things must land before any of this is implementable:

**Stable `plume_id`.** Currently an iteration-order counter, with `04` writing via
`overwrite`. A daily run renumbers every plume, orphaning every compliance event, work
order and financial impact row keyed to the old IDs. Tracked as Phase 0 Prompt 2.

**Partitioned fact tables.** None of the existing fact generators partition by `date_sk`,
so `replaceWhere` has nothing to scope against.

Building the two-pass generators before these land means building against a key that
changes underneath them.

---

## 7. Validation

Each of these should run after the daily pipeline and fail it, not warn:

- Open work orders within `[expected × 0.4, expected × 2.5]` where
  `expected = arrival_rate × mean_resolution_days`
- No backlog metric trending monotonically upward across the last 14 runs
- Cumulative LDAR repaired ≥ 70% of cumulative detected, with lag
- Overdue PM count stable within a band; mean days overdue below a threshold
- Re-running yesterday produces byte-identical fact tables
- No duplicate business keys in any fact table
- Every fact row's `date_sk` within the run window

The trending check is the important one. A single day's count looking reasonable does not
prove the drain works — only the trajectory does.

---

## 8. Open questions

1. **What arrival rate do we actually want?** The detection rate gives ~2.5 plumes/day
   today, but the striping fix will reduce it. Should the target open-work-order count be
   set against the post-fix detection rate, or should non-plume sources (alarms,
   inspections) carry more of the arrivals so the dashboard is not hostage to detection
   volume?
2. **How far back does pass 1 look?** Scanning all open tickets every day is cheap at this
   scale, but needs a bound if the history grows.
3. **Should a backfill run both passes day by day, or generate a converged snapshot
   directly?** Day-by-day is more faithful and guarantees backfill and incremental agree;
   direct is much faster over a 12-month window.
4. **Where do stalled items go?** A permanently-open ticket is realistic but skews the
   steady-state check. Suggest capping the stall share at ~5% and excluding stalled items
   from the trending assertion.
