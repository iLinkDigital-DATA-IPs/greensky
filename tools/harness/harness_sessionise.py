"""Is 02d's window-function sessioniser equivalent to an explicit debounce state machine?

02d replaces an iterative alarm state machine with three window passes: run numbering, a
row_number filter at the debounce count, and an alternation collapse. That is the part most
likely to be subtly wrong, so it is checked against a brute-force machine on random action
sequences, including the cases that break naive implementations.
"""
import numpy as np

DEBOUNCE = 3


def brute(actions, debounce):
    """The obvious sequential machine: raise after N consecutive +1, clear after N of -1."""
    out, in_alarm, run_val, run_len, raised = [], False, 0, 0, None
    for i, a in enumerate(actions):
        if a == 0:
            continue                       # neutral: extends whatever state is current
        if a == run_val:
            run_len += 1
        else:
            run_val, run_len = a, 1
        if a == 1 and run_len == debounce and not in_alarm:
            in_alarm, raised = True, i
        elif a == -1 and run_len == debounce and in_alarm:
            out.append((raised, i)); in_alarm = False
    if in_alarm:
        out.append((raised, None))
    return out


def sessionise(actions, debounce):
    """02d's algorithm, in numpy: drop neutrals, number runs, take the Nth, collapse."""
    idx = np.flatnonzero(np.asarray(actions) != 0)
    if len(idx) == 0:
        return []
    a = np.asarray(actions)[idx]
    # run_id = cumulative count of action changes
    new_run = np.concatenate(([True], a[1:] != a[:-1]))
    run_id = np.cumsum(new_run)
    # row_number within run
    rn = np.ones(len(a), dtype=int)
    for i in range(1, len(a)):
        rn[i] = 1 if new_run[i] else rn[i - 1] + 1
    cand = np.flatnonzero(rn == debounce)
    if len(cand) == 0:
        return []
    ck, ci = a[cand], idx[cand]
    # alternation collapse: keep a candidate only if it differs in kind from the previous
    keep = np.concatenate(([True], ck[1:] != ck[:-1]))
    ck, ci = ck[keep], ci[keep]
    out = []
    for j in range(len(ck)):
        if ck[j] != 1:
            continue
        nxt = ci[j + 1] if (j + 1 < len(ck) and ck[j + 1] == -1) else None
        out.append((int(ci[j]), None if nxt is None else int(nxt)))
    return out


if __name__ == "__main__":
    rng = np.random.default_rng(20260915)
    cases, fails = 0, 0

    # --- random sequences, several action mixes -------------------------------------------------
    for trial in range(4000):
        n = int(rng.integers(1, 120))
        p = rng.choice([[.2, .2, .6], [.45, .45, .1], [.05, .9, .05], [.34, .33, .33],
                        [.8, .15, .05], [.1, .1, .8]])
        acts = rng.choice([1, -1, 0], size=n, p=p)
        deb = int(rng.integers(1, 5))
        b, s = brute(acts, deb), sessionise(acts, deb)
        cases += 1
        if b != s:
            fails += 1
            if fails <= 3:
                print(f"  MISMATCH deb={deb} acts={list(acts)}\n    brute={b}\n    sess ={s}")

    # --- the shapes that break naive implementations ---------------------------------------------
    named = {
        "short clear run leaves alarm open, then re-breach":
            [1, 1, 1, -1, -1, 1, 1, 1, -1, -1, -1],
        "breach run shorter than debounce never raises":
            [1, 1, -1, -1, -1, 1, 1],
        "neutrals interleaved do not break a run":
            [1, 0, 1, 0, 1, 0, -1, 0, -1, -1],
        "starts mid-clear":
            [-1, -1, -1, -1, 1, 1, 1],
        "alarm still open at the end":
            [-1, -1, -1, 1, 1, 1, 1],
        "immediate re-raise after clear":
            [1, 1, 1, -1, -1, -1, 1, 1, 1],
        "all neutral":
            [0, 0, 0, 0],
        "exactly debounce length":
            [1, 1, 1],
        "one sample over and back repeatedly (chatter)":
            [1, -1] * 20,
    }
    for name, acts in named.items():
        b, s = brute(acts, DEBOUNCE), sessionise(acts, DEBOUNCE)
        cases += 1
        ok = b == s
        fails += (not ok)
        print(f"  {'OK  ' if ok else 'FAIL'} {name:<52} {s}")

    print()
    print(f"  {cases:,} cases, {fails} mismatches")
    assert fails == 0, "the window-function sessioniser is not equivalent to the state machine"
    print("OK  02d's sessioniser is equivalent to an explicit debounce/deadband state machine")
