"""window_slots (whole window, per tag) must equal sum of slots_for_day (per day, per tier).

02b now asserts SLOTS_TOTAL == LIVE_SLOTS. They come from different code: one loops days and
groups by cadence tier, the other clips one tag's window once. Both are copied verbatim from
the notebook here so the assertion cannot be satisfied only by luck.
"""
import numpy as np
import pandas as pd

AS_OF = pd.Timestamp("2026-09-15")
WINDOW_DAYS = 30
WINDOW_START, WINDOW_END = AS_OF - pd.Timedelta(days=WINDOW_DAYS), AS_OF
_EPOCH_TS = pd.Timestamp("1970-01-01")


def window_slots(install_date, cadence_s, lo=None, hi=None):
    lo = WINDOW_START if lo is None else lo
    hi = WINDOW_END if hi is None else hi
    start = max(pd.Timestamp(install_date), pd.Timestamp(lo))
    hi = pd.Timestamp(hi)
    if start >= hi:
        return 0
    cad = int(cadence_s)
    first = int(np.ceil((start - _EPOCH_TS).total_seconds() / cad))
    last = int((hi - _EPOCH_TS).total_seconds() // cad)
    return max(0, last - first)


def slots_for_day(live, day_ts):
    day_end = day_ts + pd.Timedelta(days=1)
    n = 0
    for cad, grp in live.groupby("sampling_interval_seconds"):
        cad, per_day = int(cad), 86400 // int(cad)
        inst = grp["install_date"]
        n += int((inst <= day_ts).sum()) * per_day
        last = int(day_end.timestamp()) // cad
        for d in inst[(inst > day_ts) & (inst < day_end)]:
            first = int(np.ceil((d - _EPOCH_TS).total_seconds() / cad))
            n += max(0, last - first)
    return n


rng = np.random.default_rng(7)
N = 4000
# install dates deliberately concentrated around the window edges, where the two disagree if
# either gets its boundary wrong
inst = []
for _ in range(N):
    r = rng.random()
    if r < 0.45:
        inst.append(WINDOW_START - pd.Timedelta(days=float(rng.uniform(0, 900))))
    elif r < 0.9:
        inst.append(WINDOW_START + pd.Timedelta(seconds=float(rng.uniform(0, 30 * 86400))))
    else:                       # exactly on a day boundary / window edge / slot boundary
        inst.append(rng.choice([WINDOW_START, WINDOW_END,
                                WINDOW_START + pd.Timedelta(days=int(rng.integers(0, 30))),
                                WINDOW_START + pd.Timedelta(seconds=900 * int(rng.integers(0, 2880)))]))
live = pd.DataFrame({
    "install_date": pd.to_datetime(pd.Series(inst)),
    "sampling_interval_seconds": rng.choice([300, 900], N, p=[0.25, 0.75]),
})
live = live[live["install_date"] < WINDOW_END].reset_index(drop=True)

per_tag = int(sum(window_slots(d, c) for d, c in
                  zip(live["install_date"], live["sampling_interval_seconds"])))
per_day = int(sum(slots_for_day(live, WINDOW_START + pd.Timedelta(days=i))
                  for i in range(WINDOW_DAYS)))

print(f"  tags                      {len(live):,}")
print(f"  installed before window   {int((live['install_date'] <= WINDOW_START).sum()):,}")
print(f"  installed inside window   {int((live['install_date'] > WINDOW_START).sum()):,}")
print(f"  LIVE_SLOTS (window_slots) {per_tag:,}")
print(f"  SLOTS_TOTAL (slots_for_day){per_day:,}")
print(f"  difference                {per_day - per_tag:+,}")
assert per_day == per_tag, "the two slot counts disagree -- 02b's new assertion would fire"
print("\nOK  the whole-window and per-day slot counts agree exactly, including on tags")
print("    installed on a day boundary, on a slot boundary and on the window edges")
