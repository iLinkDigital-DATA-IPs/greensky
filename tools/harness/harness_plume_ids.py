"""04's content-derived scene_id and plume_id: order-free within an execution.

Runs 00_config's own helpers (scene_label, plume_key) -- not a copy -- over synthetic scenes
and clusters, and asserts:
  - the golden vectors in 00_config hold
  - scene_id labels sort chronologically, so 04's groupby("scene_id") visits scenes in the
    same order the old integer cumsum did (clustering order, and the plume set, unchanged)
  - shuffling input rows changes no scene_id and no plume_id
  - shifting the window -- dropping the earliest scene -- leaves every remaining plume_id
    unchanged, where the old counters renumber (asserted too, so the test has teeth)
  - session time zones are honoured: a naive CDT timestamp labels as its UTC instant

What this cannot show is that two separate executions of 04 on Fabric agree: that depends on
real silver data and Spark, and is checked only by the rerun regression at the end of 04.
"""
from pathlib import Path
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
CFG = (REPO / "Methane Emissions/Accelerator/Planetary Computer/Varon et al Approach/notebooks"
       / "00_prereqs/00_config.Notebook/notebook-content.py")


def load():
    src = CFG.read_text(encoding="utf-8")
    code = "\n".join(("pass  # " + ln) if ln.startswith("%") else ln for ln in src.splitlines())
    g = {}
    import contextlib, io
    with contextlib.redirect_stdout(io.StringIO()):
        exec(compile(code, "00_config", "exec"), g)       # runs its golden-vector asserts
    return g


G = load()
scene_label, plume_key = G["scene_label"], G["plume_key"]
GAP_S = G["CONFIG"]["scene_gap_minutes"] * 60


def synth(seed=7, n_scenes=12):
    """stac_ids with start/end times (some scenes span two granules), and cluster pixels."""
    rng = np.random.default_rng(seed)
    t = pd.Timestamp("2026-06-10 18:00:00")
    stacs, pixels, sid = [], [], 0
    for s in range(n_scenes):
        t += pd.Timedelta(hours=int(rng.integers(20, 30)), seconds=int(rng.integers(0, 3600)))
        for g in range(int(rng.integers(1, 3))):          # 1-2 granules within the gap
            sid += 1
            a = t + pd.Timedelta(seconds=60 * g)
            stacs.append((f"S5P_{sid:04d}", a, a + pd.Timedelta(seconds=40)))
        for c in range(int(rng.integers(1, 4))):          # clusters in this scene
            for k in range(int(rng.integers(3, 8))):
                pixels.append((s, c, stacs[-1][0], round(31 + rng.random() * 2, 6),
                               round(-104 + rng.random() * 3, 6), float(rng.normal(40, 15))))
    st = pd.DataFrame(stacs, columns=["stac_id", "scene_start", "scene_end"])
    px = pd.DataFrame(pixels, columns=["scene", "cluster", "stac_id", "latitude", "longitude",
                                       "enh"])
    return st, px


def label_scenes(st, tz="UTC"):
    """04's step 1: gap grouping unchanged, then the new label."""
    st = st.sort_values("scene_start").reset_index(drop=True)
    gap = (st["scene_start"] - st["scene_end"].shift(1)).dt.total_seconds()
    st["scene_seq"] = (gap.isna() | (gap > GAP_S)).cumsum()
    st["scene_id"] = scene_label(st.groupby("scene_seq")["scene_start"].transform("min"), tz)
    return st


def plume_ids(st, px):
    """plume_id per cluster from its peak pixel, as step 6 does; and the old counter."""
    m = px.merge(label_scenes(st)[["stac_id", "scene_id", "scene_seq"]], on="stac_id")
    new, old, counter = {}, {}, 0
    for (_, cl), g in m.groupby(["scene_seq", "cluster"]):
        counter += 1
        i = int(g["enh"].values.argmax())
        pid = plume_key(g["scene_id"].iloc[0], g["latitude"].iloc[i], g["longitude"].iloc[i])
        new[(g["stac_id"].iloc[0], cl)] = pid
        old[(g["stac_id"].iloc[0], cl)] = counter
    return new, old


if __name__ == "__main__":
    print("=" * 84)
    print("04 CONTENT-DERIVED IDENTIFIERS")
    print("=" * 84)
    print("OK  00_config's golden vectors hold (plume_key string, digest prefix, scene_label)")

    st, px = synth()
    lab = label_scenes(st)
    per = lab.groupby("scene_seq")["scene_id"].first()
    assert list(per) == sorted(per), "scene_id labels do not sort chronologically"
    assert per.is_unique
    print(f"OK  {len(per)} scenes: labels unique and sort chronologically, so groupby order "
          "matches the old integer scene_id")

    base_new, base_old = plume_ids(st, px)
    assert len(set(base_new.values())) == len(base_new)
    shuf_new, _ = plume_ids(st.sample(frac=1, random_state=3),
                            px.sample(frac=1, random_state=4))
    assert shuf_new == base_new, "shuffling input rows changed a plume_id"
    print(f"OK  {len(base_new)} plume_ids unique, and unchanged when every input row is shuffled")

    first = lab.loc[lab["scene_seq"] == 1, "stac_id"]
    st2, px2 = st[~st["stac_id"].isin(first)], px[~px["stac_id"].isin(first)]
    new2, old2 = plume_ids(st2, px2)
    kept = set(new2)
    assert all(new2[k] == base_new[k] for k in kept), "a window shift changed a plume_id"
    moved = sum(old2[k] != base_old[k] for k in kept)
    assert moved > 0, "the old counter did not renumber -- this test would not catch the defect"
    print(f"OK  window shift (earliest scene dropped): all {len(kept)} remaining plume_ids "
          f"unchanged; the old counter renumbered {moved} of them")

    cdt = scene_label(pd.Series([pd.Timestamp("2026-06-10 13:12:03")]), "America/Chicago").iloc[0]
    assert cdt == "SCN-20260610T181203", cdt
    print("OK  a naive session-local timestamp (America/Chicago) labels as its UTC instant")
    print("\n    Not provable here: that two executions of 04 on Fabric agree. The rerun")
    print("    regression at the end of 04 checks that, and only on Fabric.")
