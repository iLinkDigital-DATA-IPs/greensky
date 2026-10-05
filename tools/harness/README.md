# tools/harness

The per-harness descriptions live in `tools/README.md` (section `harness/`). This file holds
what the date refactor works from: where the harnesses read the topology date, and the gate
built around them.

## Where the harnesses read the topology date

Authoritative list, verified against the tree on 2026-10-01 by search and by Perturbation A
(`perturbations.py`). Prompt 1 works from this list.

### Hard-coding it (a literal `2026-09-15`): 11 reads

| file:line | reads | note |
|---|---|---|
| `harness_model.py:13` | `TOPOLOGY_AS_OF = pd.Timestamp("2026-09-15")` | **DEAD.** No harness reads `M.TOPOLOGY_AS_OF`; changing it moves nothing |
| `harness_alarms.py:10` | `AS_OF = pd.Timestamp("2026-09-15")` | 30-day window `WIN_START, WIN_END` derive from it (line 11) |
| `harness_alarm_rate.py:13` | `AS_OF = pd.Timestamp("2026-09-15")` | window (14), install (113), state start `AS_OF - 120 d` (114), drift ref `AS_OF - 90 d` (130) |
| `harness_alarm_keys.py:21` | `AS_OF = pd.Timestamp("2026-09-15")` | window (22), install and state start (52-53), drift ref (78) |
| `harness_ch4.py:24` | `AS_OF = pd.Timestamp("2026-09-15")` | `asset_states(hi=AS_OF + 1 d)` (80) and the raw window |
| `harness_final_rate.py:40` | `AS_OF = pd.Timestamp("2026-09-15")` | window (41), install and state start, drift ref `AS_OF - 90 d` |
| `harness_installs.py:10` | `AS_OF = pd.Timestamp("2026-09-15")` | the 01a -> 01b -> 01d install-date chain (19, 31, 42, 50) |
| `harness_joins.py:16` | `AS_OF = pd.Timestamp("2026-09-15")` | window |
| `harness_outage_slots.py:11` | `AS_OF = pd.Timestamp("2026-09-15")` | window (12) |
| `harness_slots.py:10` | `AS_OF = pd.Timestamp("2026-09-15")` | window (12) |
| `harness_run.py:6` | `WIN_END = pd.Timestamp("2026-09-15")` | 02b's window end (named `WIN_END`, not `AS_OF`); `harness_run9.py` imports it |

### Reading `01_topology_config` (`load_config()` -> `TOPOLOGY_AS_OF`): 6 reads

The source is `01_topology_config.Notebook/notebook-content.py:343`,
`TOPOLOGY_AS_OF = date(2026, 9, 15)`. `STATE_HISTORY_DAYS` is at `:1273` and
`TELEMETRY_RAW_DAYS` at `:972`.

| file:line | reads |
|---|---|
| `harness_episodes.py:136` | `estate()`: commission and install dates of the synthetic estate |
| `harness_episodes.py:192` | `full_state()`: asset age passed to `simulate` |
| `harness_episodes.py:213` | `check_state_machine_matches_02a()`: window end edge |
| `harness_episodes.py:319` | `AS_OF`; `H0 = AS_OF - STATE_HISTORY_DAYS` at `:320` |
| `harness_maintenance.py:397` | `H0 = TOPOLOGY_AS_OF - STATE_HISTORY_DAYS` (03d's `HISTORY_START`; was the literal `2026-06-17` until 2026-10-01) |
| `harness_rollup.py:31` | `START = TOPOLOGY_AS_OF - TELEMETRY_RAW_DAYS`, with `DAYS = TELEMETRY_RAW_DAYS` at `:30` (02c:180-181; was the literal `2026-08-16` until 2026-10-01) |

`harness_compliance`, `harness_work_orders` and `harness_financial` load the same config but
never read `TOPOLOGY_AS_OF`. No notebook cell that a harness execs (03a-03e model cells,
02a's state machine, 00_config, 01_topology_config) reads the date in code. Their only
mentions are in docstrings and comments.

### Fixed dates that are NOT readings of the topology date (stay hard-coded)

| file:line | value | why it is fixed |
|---|---|---|
| `harness_compliance.py:296` | `START = 2026-08-16` | origin of a synthetic 300-day upstream. 03c takes its horizon from its sources, not the topology date |
| `harness_work_orders.py:429` | `START = 2026-08-16` | origin of a synthetic 120-day upstream, same pattern |
| `harness_work_orders.py:406-415` | `2026-09-01..04`, `2026-09-15`, `2026-09-02` | self-consistent fixture for `compliance_sources` |
| `harness_alarm_backing.py:23` | `R = 2026-09-10 12:00` | an arbitrary reading time for a join-formulation test |
| `harness_financial.py:114`, `harness_plume_ids.py:46, 155` | various | unit fixtures |
| `harness_alarms.py:138`, `harness_run.py:77`, `harness_ch4.py:80` | `2026-06-01` | drift reference / state-history lower bound of a synthetic run, inside the window arithmetic |
| `harness_episodes.py:289` | `w0, w1 = 2026-08-16, 2026-09-16` | print-only "active 08-16..09-15" window in `report()`. It is labelled relative to the as-of date and will go stale if that moves, but nothing asserts on it (see below) |

## The gate

- `capture.py`: fingerprints every DataFrame, Series, Index and ndarray a harness produces
  (see its docstring). Refuses to run if Python, pandas or numpy differ from `baseline/`.
- `baseline/`: the canonical capture. The committed `baseline/**/*.fingerprint.json` files are
  the reference: the refactor is accepted or rejected against them, and they are tracked so
  that a lost reference (e.g. after `git clean -xdf`) can never be mistaken for a regression.
  The `*.stdout.txt` files beside them stay git-ignored; they are regenerable at any time with
  `capture.py` (or `perturbations.py suite`) on an unperturbed tree. Regenerate the JSONs only
  deliberately, and commit the regeneration on its own.
- `perturbations.py`: Perturbation A (the config source at `:343` plus the 11 literals above,
  12 edits, 2026-09-15 -> 2026-09-16; the 6 config reads follow by construction) and
  Perturbation B (`STATE_HISTORY_DAYS` 90 -> 91), with apply, revert, suite, diff and sets.
- `sets.json`: SET B, SET A minus B and NEITHER for every harness entry. **These are the
  refactor's acceptance criteria.**
- `captures/`: perturbed and confirmation captures. Git-ignored.
