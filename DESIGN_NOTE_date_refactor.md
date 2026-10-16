# Design Note — Splitting TOPOLOGY_AS_OF

**Status:** decided. Not yet implemented.
**Applies to:** `01_topology_config` and every generator that reads `TOPOLOGY_AS_OF`
(02a-02e, 03a-03e), plus the offline harnesses that mirror them.
**Written because:** the enterprise pipeline cannot advance past 2026-09-15. Every generator
takes its window from one fixed date, so `greensky_enterprise` can backfill but cannot run
incrementally at all (§4).

---

## 1. Decision

`TOPOLOGY_AS_OF` is one constant doing three jobs. It splits into three:

| Constant | Value | Governs |
|---|---|---|
| `ESTATE_AS_OF` | 2026-09-15, pinned with the topology artefact | commission and install dates, age-derived attributes, SCD2 `effective_from` |
| `HISTORY_START` | an **absolute literal**, currently `ESTATE_AS_OF - STATE_HISTORY_DAYS` (2026-06-17), written down once and never derived again | where the state chain and the PM seed start |
| `PIPELINE_DATE` | per run, from the pipeline parameter | window ends, horizon guards, `coalesce(end_ts, AS_OF)` |

`HISTORY_START` is frozen because two seeds are keyed on it:

- 02a's state chain starts at `start_ts = max(HISTORY_START, install)`
  (`02a_build_asset_state`, `start_ts = max(HISTORY_START, a.install_date)`).
- 03d's and 03e's `pm_seed` walks back from `k0 = floor((history_start - pm_anchor) / span)`
  (`03d_gen_maintenance`, `pm_seed`; 03e carries a copy).

If `HISTORY_START` moves, every established asset's state history and PM schedule re-rolls.
Deriving it from a date that advances would do exactly that on every run.

## 2. Why this and not the alternatives

Changelog Day 15 ("STRUCTURAL: the pipeline cannot advance past TOPOLOGY_AS_OF") records four
options: split the clocks, move `TOPOLOGY_AS_OF` without regenerating, regenerate on a cadence
with stable keys, or declare the demo fixed-window. This is the first.

`DESIGN_NOTE_incremental_facts.md` §2.1 argues against moving the date on two grounds: bumping
it re-ages the estate, and regenerating the estate re-keys `equipment_sk` / `facility_sk` and
orphans the facts. Day 15 carries the same pair as the reason "simply moving the date does not
work". The second ground does not apply to this refactor. It is about regenerating the estate,
and nothing here regenerates it. A call-site scan found that entity keys do not depend on the
date. What actually moves when the date moves is the seed inputs: the state chain and
`pm_seed`. Freezing `HISTORY_START` stops that, and pinning `ESTATE_AS_OF` with the topology
stops the re-ageing. **Correct §2.1 as part of the refactor**, so that it separates moving the
date (survivable, given this split) from regenerating the estate (orphans the facts).

## 3. The gate

All 21 harness fingerprints identical to `tools/harness/baseline/` at the current date
(`tools/harness/README.md`, "The gate"). Run it **once, after the migration**, not between
passes. It proves the refactor is a no-op at today's date. It does not say which entries should
move when the date moves (§6).

## 4. What the refactor must also fix: an incremental run stops at 02a

`greensky_enterprise` was run with `run_mode=incremental` and `pipeline_date` empty on
2026-10-12, after a backfill the same day (job `9ff5890c-efe7-4f2c-b8f5-dfa395636803`):

- 02a resolved `[2026-09-15, 2026-09-16)`: the day after its watermark,
  `max(date_sk) = 20260914`.
- Generating that day produced intervals starting on 2026-09-15. 02a's own validation, which
  runs before its write, raised `AssertionError: 204 interval(s) start at or after the as-of
  date`.
- Nothing was written (`fact_asset_state` stayed at 20,261 rows, Delta version 2). The
  pipeline stopped at 02a, so nothing downstream ran.

So the failure is loud, not silent. But it means **no incremental run of the enterprise
pipeline can succeed while the estate's clock is `TOPOLOGY_AS_OF`**. That is why the 12 Oct
backfill ran with `pipeline_date` empty: every window is derived from `TOPOLOGY_AS_OF` and ends
at 2026-09-15. A `pipeline_date` of 2026-09-15 would have passed `end_date` 2026-09-16, one day
past the as-of date.

02a's window cell itself has no as-of bound; the guard is the validation assertion. After the
split, the window end must come from `PIPELINE_DATE`. The as-of guard must then stop meaning
"never past 2026-09-15" and become "never past the run's own horizon", or the first daily run
fails here.

## 5. Authoritative call-site list

`tools/harness/README.md` ("Where the harnesses read the topology date") has the real list of
date reads. A four-file list written from memory was wrong: one entry was dead code, and eight
real reads were missing. Work from the README.

## 6. Started and not finished

The SET A / SET B refinement in `tools/harness/perturbations.py`: classify every harness entry
as moving under Perturbation A (the estate date), under B (the seeding floor,
`STATE_HISTORY_DAYS`), or neither, and accept the refactor against that classification.
`sets.json` was never written. It would be a stronger gate than plain fingerprint identity, but
it was deliberately dropped for cost.
