"""Negative controls for the harness gate: the perturbation definitions, a runner, and the
set computation behind sets.json.

    python perturbations.py show                 # list every edit of every perturbation
    python perturbations.py apply A|B            # apply one perturbation in place
    python perturbations.py revert               # revert the applied perturbation via git
    python perturbations.py suite OUT_DIR        # capture all harnesses into OUT_DIR
    python perturbations.py diff OUT_DIR         # OUT_DIR against baseline/, per harness
    python perturbations.py sets A_DIR B_DIR     # write sets.json from two perturbed captures

A perturbation is a list of exact single-line edits, each pinned to file and line. `apply`
refuses unless every target file has no diff against HEAD and every line reads exactly as
expected, and it edits bytes in place so line endings are untouched. `revert` restores each
file from git (git restore, then the raw blob if that still differs in line endings), checks
it byte-for-byte against the hash taken before `apply`, and checks `git diff` on the targets
is empty. State lives in captures/ (git-ignored).

PERTURBATION A moves the estate date one day, 2026-09-15 -> 2026-09-16, at every place a
harness reads it: the config source and all eleven harness literals (see README, "Where the
harnesses read the topology date"). HISTORY_START and the derived windows (harness_rollup,
harness_maintenance H0) follow automatically. PERTURBATION B moves only the seeding floor:
STATE_HISTORY_DAYS 90 -> 91, so HISTORY_START moves back one day and the estate date stays.

SET B = entries that move under B. SET A minus B = entries that move under A but not B.
NEITHER = entries invariant to both. These are the refactor's acceptance criteria.
"""
import hashlib
import json
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))                     # greensky/
BASELINE = os.path.join(HERE, "baseline")
CAPTURES = os.path.join(HERE, "captures")
STATE = os.path.join(CAPTURES, ".perturbation_state.json")
CONFIG = ("Methane Emissions/Accelerator/Planetary Computer/Varon et al Approach/notebooks/"
          "01_topology/01_topology_config.Notebook/notebook-content.py")
H = "tools/harness/"

_D15, _D16 = 'pd.Timestamp("2026-09-15")', 'pd.Timestamp("2026-09-16")'
PERTURBATIONS = {
    "A": [  # (repo-relative path, 1-based line, expected line, replacement line)
        (CONFIG, 343, "TOPOLOGY_AS_OF = date(2026, 9, 15)", "TOPOLOGY_AS_OF = date(2026, 9, 16)"),
        (H + "harness_model.py", 13, f"TOPOLOGY_AS_OF = {_D15}", f"TOPOLOGY_AS_OF = {_D16}"),
        (H + "harness_alarms.py", 10, f"AS_OF = {_D15}", f"AS_OF = {_D16}"),
        (H + "harness_alarm_rate.py", 13, f"AS_OF = {_D15}", f"AS_OF = {_D16}"),
        (H + "harness_alarm_keys.py", 21, f"AS_OF = {_D15}", f"AS_OF = {_D16}"),
        (H + "harness_ch4.py", 24, f"AS_OF = {_D15}", f"AS_OF = {_D16}"),
        (H + "harness_final_rate.py", 40, f"AS_OF = {_D15}", f"AS_OF = {_D16}"),
        (H + "harness_installs.py", 10, f"AS_OF = {_D15}", f"AS_OF = {_D16}"),
        (H + "harness_joins.py", 16, f"AS_OF = {_D15}", f"AS_OF = {_D16}"),
        (H + "harness_outage_slots.py", 11, f"AS_OF = {_D15}", f"AS_OF = {_D16}"),
        (H + "harness_slots.py", 10, f"AS_OF = {_D15}", f"AS_OF = {_D16}"),
        (H + "harness_run.py", 6, f"WIN_END = {_D15}", f"WIN_END = {_D16}"),
    ],
    "B": [
        (CONFIG, 1273, "STATE_HISTORY_DAYS = 90", "STATE_HISTORY_DAYS = 91"),
    ],
}

HARNESSES = [  # longest first, so the pool finishes sooner
    "maintenance", "episodes", "joins", "compliance", "alarm_keys", "work_orders", "final_rate",
    "ch4", "alarm_rate", "rollup", "alarm_incr", "run", "run9", "outage_slots", "plume_ids",
    "financial", "alarms", "sessionise", "slots", "alarm_backing", "installs"]


def git(*args, check=True):
    r = subprocess.run(["git", "-C", REPO, *args], capture_output=True)
    if check and r.returncode not in (0, 1):
        sys.exit(f"git {' '.join(args)} failed: {r.stderr.decode(errors='replace')}")
    return r


def sha(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def show():
    for name, edits in PERTURBATIONS.items():
        print(f"PERTURBATION {name}: {len(edits)} edit(s)")
        for p, ln, old, new in edits:
            print(f"  {p}:{ln}\n      - {old}\n      + {new}")


def apply(name):
    if os.path.exists(STATE):
        sys.exit(f"a perturbation is already applied ({STATE}); revert it first")
    edits = PERTURBATIONS[name]
    paths = sorted({p for p, *_ in edits})
    for p in paths:
        if git("diff", "--quiet", "--", p, check=False).returncode != 0:
            sys.exit(f"refusing: {p} has uncommitted changes, and revert restores it from HEAD")
    before = {p: sha(os.path.join(REPO, p)) for p in paths}
    for p in paths:
        full = os.path.join(REPO, p)
        lines = open(full, "rb").read().split(b"\n")
        for q, ln, old, new in edits:
            if q != p:
                continue
            cur = lines[ln - 1]
            cr = cur.endswith(b"\r")
            if cur.rstrip(b"\r").decode() != old:
                sys.exit(f"refusing: {p}:{ln} reads {cur.rstrip(b'\r').decode()!r}, expected {old!r}")
            lines[ln - 1] = new.encode() + (b"\r" if cr else b"")
        open(full, "wb").write(b"\n".join(lines))
    os.makedirs(CAPTURES, exist_ok=True)
    json.dump({"perturbation": name, "before_sha256": before}, open(STATE, "w"), indent=1)
    print(f"applied {name}:")
    print(git("diff", "--stat", "--", *paths).stdout.decode())


def revert():
    if not os.path.exists(STATE):
        sys.exit("nothing applied")
    st = json.load(open(STATE))
    bad = []
    for p, want in st["before_sha256"].items():
        full = os.path.join(REPO, p)
        git("restore", "--source=HEAD", "--", p)
        if sha(full) != want:                      # autocrlf wrote CRLF; the original was LF
            with open(full, "wb") as f:
                f.write(git("cat-file", "blob", f"HEAD:{p}").stdout)
        if sha(full) != want:
            bad.append(p)
    paths = list(st["before_sha256"])
    empty = git("diff", "--quiet", "--", *paths, check=False).returncode == 0
    print(f"reverted {st['perturbation']}: git diff on {len(paths)} target(s) "
          f"{'EMPTY' if empty else 'NOT EMPTY'}; byte-exact {len(paths) - len(bad)}/{len(paths)}")
    for p in bad:
        print(f"  NOT byte-exact (content restored from git, line endings differ): {p}")
    if not empty:
        sys.exit(1)
    os.remove(STATE)


def suite(out, parallel=8):
    os.makedirs(out, exist_ok=True)
    todo, running, t0 = list(HARNESSES), {}, {}
    while todo or running:
        while todo and len(running) < parallel:
            h = "harness_" + todo.pop(0)
            p = subprocess.Popen(
                [sys.executable, os.path.join(HERE, "capture.py"), h + ".py", out], cwd=HERE,
                stdout=open(os.path.join(out, h + ".stdout.txt"), "wb"),
                stderr=open(os.path.join(out, h + ".stderr.txt"), "wb"))
            running[h], t0[h] = p, time.time()
            print(f"start {h} pid={p.pid}", flush=True)
        for h, p in list(running.items()):
            if p.poll() is not None:
                print(f"{h} rc={p.returncode} secs={time.time() - t0[h]:.0f}", flush=True)
                del running[h]
        time.sleep(1)


def load(d, n):
    p = os.path.join(d, n + ".fingerprint.json")
    return json.load(open(p, encoding="utf-8")) if os.path.exists(p) else None


def changed(base, other):
    fa = {f["frame_name"]: f["sha256"] for f in base["frames"]}
    fb = {f["frame_name"]: f["sha256"] for f in other["frames"]}
    return {k for k in fa if fb.get(k) != fa[k]}


def diff(out):
    for h in HARNESSES:
        n = "harness_" + h
        a, b = load(BASELINE, n), load(out, n)
        if b is None:
            print(f"{n:22} MISSING"); continue
        ch = changed(a, b)
        new = {f["frame_name"] for f in b["frames"]} - {f["frame_name"] for f in a["frames"]}
        so = open(os.path.join(BASELINE, n + ".stdout.txt"), "rb").read() == \
            open(os.path.join(out, n + ".stdout.txt"), "rb").read()
        print(f"{n:22} {b['status']:6} differ {len(ch):4}/{len(a['frames']):4} "
              f"stdout_same={so}" + (f" new_entries={len(new)}" if new else ""))


def sets(a_dir, b_dir):
    out = {"definition": "SET_B: moves under B. SET_A_MINUS_B: moves under A, not under B. "
                         "NEITHER: invariant to both. Against baseline/ at the time of writing.",
           "perturbations": {k: [{"file": p, "line": ln, "from": o, "to": n} for p, ln, o, n in v]
                             for k, v in PERTURBATIONS.items()},
           "harnesses": {}}
    for h in sorted(HARNESSES):
        n = "harness_" + h
        base, A, B = load(BASELINE, n), load(a_dir, n), load(b_dir, n)
        names = [f["frame_name"] for f in base["frames"]]
        ca, cb = changed(base, A), changed(base, B)
        out["harnesses"][n] = {
            "status_under": {"A": A["status"], "B": B["status"]},
            "SET_B": [f for f in names if f in cb],
            "SET_A_MINUS_B": [f for f in names if f in ca and f not in cb],
            "NEITHER": [f for f in names if f not in ca and f not in cb],
            "B_but_not_A": [f for f in names if f in cb and f not in ca]}
    with open(os.path.join(HERE, "sets.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(out, f, indent=1)
        f.write("\n")
    for n, r in out["harnesses"].items():
        print(f"{n:22} B={len(r['SET_B']):4} A-B={len(r['SET_A_MINUS_B']):4} "
              f"NEITHER={len(r['NEITHER']):4} A:{r['status_under']['A']} B:{r['status_under']['B']}")


if __name__ == "__main__":
    cmd, *rest = sys.argv[1:] or ["show"]
    {"show": show, "apply": apply, "revert": revert, "suite": suite, "diff": diff,
     "sets": sets}[cmd](*rest)
