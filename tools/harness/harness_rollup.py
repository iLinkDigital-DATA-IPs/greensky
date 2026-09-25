"""Are 02c's rollups deterministic, window-independent, weighted, and honest about gaps?

A pandas mirror of 02c's aggregation -- same exact-integer sums, same derived columns, same
null rules -- run over a synthetic raw table built to contain every case the notebook claims
to handle: a whole hour with no readings, a whole day with none, partial hours, an all-Bad
hour, an hour with a single Good reading, a frozen tag-day, a tag installed mid-hour, and a
final day that is incomplete when first rolled.

What it asserts:
  - determinism, including under a shuffled row order (Spark's merge order is not fixed)
  - a 30-day backfill equals 30 one-day incremental runs, bitwise, at both grains
  - an incomplete final day, re-rolled once complete, converges to the backfill row
  - the day-grain value_avg is the good_count-weighted mean, and a flat mean would fail
  - the day-grain stddev equals the stddev computed directly from raw
  - missing hours produce no row; no-Good buckets have null values and a real sample_count

Same limitation as the rest of harness/: it verifies the model, not Spark's execution of it.
"""
import math
import numpy as np
import pandas as pd
from harness_final_rate import load_config

STATES = load_config()["STATES"]
SCALE = 10 ** 6
TOL = 1.0 / SCALE
START = pd.Timestamp("2026-08-16")
DAYS = 30
END = START + pd.Timedelta(days=DAYS)
H = pd.Timedelta(hours=1)


# --- synthetic raw ------------------------------------------------------------------------------
def make_raw(seed=20260915):
    rng = np.random.default_rng(seed)
    rows = []
    #        tag_sk  cad  res    base    sd
    tags = [(1, 300, 0.1, 1000.0, 12.0), (2, 300, 0.01, 85.0, 2.0), (3, 900, 0.001, 4.5, 0.2),
            (4, 900, 0.5, 250.0, 6.0), (5, 900, 1.0, 3200.0, 40.0), (6, 300, 0.1, 60.0, 1.5),
            (7, 900, 0.1, 1234.3, 5.0), (8, 300, 0.01, 30.0, 0.8), (9, 900, 0.1, 500.0, 9.0),
            (10, 900, 0.1, 75.0, 2.0)]
    for tag, cad, res, base, sd in tags:
        start = START + pd.Timedelta(days=11, hours=10, minutes=40) if tag == 8 else START
        k0 = math.ceil((start - pd.Timestamp(0)).total_seconds() / cad)
        k1 = int((END - pd.Timestamp(0)).total_seconds() // cad)
        ts = pd.to_datetime(np.arange(k0, k1) * cad, unit="s")
        n = len(ts)
        v = base + sd * rng.standard_normal(n)
        state = np.where(rng.random(n) < 0.03, "Standby", "Running").astype(object)
        # a maintenance block: Substituted quality, as 02b does
        m = (ts >= START + pd.Timedelta(days=4, hours=6)) & (ts < START + pd.Timedelta(days=4, hours=14))
        state[m] = "Maintenance"
        state[(ts >= START + pd.Timedelta(days=9)) & (ts < START + pd.Timedelta(days=9, hours=3))] = "Down"
        u = rng.random(n)
        q = np.where(u < 0.005, "Bad", np.where(u < 0.016, "Uncertain", "Good")).astype(object)
        q[state == "Maintenance"] = "Substituted"
        v = np.where(q == "Bad", base + 50 * sd, v)                   # rail value
        if tag == 7:                                                 # frozen for a whole day
            f = (ts >= START + pd.Timedelta(days=5)) & (ts < START + pd.Timedelta(days=6))
            v[f], q[f] = base, "Good"                                # not a dyadic value
        if tag == 10:                                                # an all-Bad hour
            q[(ts >= START + pd.Timedelta(days=2, hours=7)) & (ts < START + pd.Timedelta(days=2, hours=8))] = "Bad"
        if tag == 3:                                                 # one Good reading in an hour
            h = (ts >= START + pd.Timedelta(days=3, hours=5)) & (ts < START + pd.Timedelta(days=3, hours=6))
            q[h] = "Uncertain"; q[np.flatnonzero(h)[0]] = "Good"
        v = np.round(v / res) * res                                  # 02b's quantisation
        keep = rng.random(n) >= 0.01                                 # scattered dropouts
        if tag == 9:                                                 # outages
            keep &= ~((ts >= START + pd.Timedelta(days=6, hours=3)) & (ts < START + pd.Timedelta(days=6, hours=4)))
            keep &= ~((ts >= START + pd.Timedelta(days=7, hours=10, minutes=20)) & (ts < START + pd.Timedelta(days=7, hours=12, minutes=40)))
            keep &= ~((ts >= START + pd.Timedelta(days=15)) & (ts < START + pd.Timedelta(days=16)))
        for i in np.flatnonzero(keep):
            rows.append((tag, f"GS-T{tag:02d}", ts[i], float(v[i]), q[i], state[i]))
    raw = pd.DataFrame(rows, columns=["tag_sk", "tag_id", "reading_ts", "value_num",
                                      "quality_code", "operating_state"])
    raw["date_sk"] = raw["reading_ts"].dt.strftime("%Y%m%d").astype(int)
    cad = {t: c for t, c, *_ in tags}
    return raw, cad


# --- 02c, mirrored ---------------------------------------------------------------------------------
def round_half_up(x):                   # Spark's F.round, not numpy's banker's rounding
    return np.sign(x) * np.floor(np.abs(x) + 0.5)


def exact_sums(df, keys, cols):
    """SQL sum() over exact Python ints, per group, as object columns.

    Done by hand: pandas' groupby infers a dtype for the result, and a column of ints wider
    than int64 with nulls in it comes back as float64 -- exactly the rounding the notebook's
    integer sums exist to avoid.
    """
    acc = {c: {} for c in cols}
    for row in zip(*(df[k] for k in keys), *(df[c] for c in cols)):
        k = row[:len(keys)]
        for c, v in zip(cols, row[len(keys):]):
            if v is not None:
                acc[c][k] = acc[c].get(k, 0) + v
    return acc


def attach(g, keys, acc):
    idx = list(zip(*(g[k] for k in keys)))
    for c, d in acc.items():
        g[c] = pd.Series([d.get(k) for k in idx], index=g.index, dtype=object)
    return g


def finish(g):
    out = g.copy()
    avg, sd = [], []
    for n, s1, s2 in zip(out["good_count"], out["value_sum_e6"], out["value_sumsq_e12"]):
        avg.append(float(s1) / n / float(SCALE) if n > 0 else None)
        sd.append(math.sqrt(float(n * s2 - s1 * s1) / (float(n) * (n - 1))) / float(SCALE)
                  if n > 1 else None)
    out["value_avg"], out["value_stddev"] = avg, sd
    out["pct_good"] = out["good_count"] / out["sample_count"]
    out["pct_time_running"] = out["n_running"] / out["sample_count"]
    sc = [f"n_{s.lower()}" for s in STATES]
    out["dominant_state"] = [max(((r[c], -i, s) for i, (s, c) in enumerate(zip(STATES, sc))))[2]
                             for _, r in out[sc].iterrows()]
    out["date_sk"] = out["bucket_ts"].dt.strftime("%Y%m%d").astype(int)
    return out.sort_values(["tag_sk", "bucket_ts"]).reset_index(drop=True)


def rollup_hourly(raw, cad):
    r = raw.copy()
    r["bucket_ts"] = r["reading_ts"].dt.floor("h")
    good = r["quality_code"] == "Good"
    xi = round_half_up(r["value_num"] * float(SCALE))
    # object dtype, or pandas turns ints-with-None into float64 and the sums stop being exact
    r["xi"] = pd.Series([int(x) if g else None for x, g in zip(xi, good)],
                        index=r.index, dtype=object)
    r["xi2"] = pd.Series([x * x if x is not None else None for x in r["xi"]],
                         index=r.index, dtype=object)
    r["gv"] = r["value_num"].where(good)
    for q in ("Good", "Bad", "Uncertain", "Substituted"):
        r[q] = (r["quality_code"] == q).astype(int)
    for s in STATES:
        r[f"n_{s.lower()}"] = (r["operating_state"] == s).astype(int)
    agg = dict(sample_count=("tag_id", "size"), good_count=("Good", "sum"),
               bad_count=("Bad", "sum"), uncertain_count=("Uncertain", "sum"),
               substituted_count=("Substituted", "sum"),
               value_min=("gv", "min"), value_max=("gv", "max"),
               **{f"n_{s.lower()}": (f"n_{s.lower()}", "sum") for s in STATES})
    keys = ["tag_sk", "tag_id", "bucket_ts"]
    g = r.groupby(keys, as_index=False).agg(**agg)
    g = attach(g, keys, exact_sums(r.rename(columns={"xi": "value_sum_e6",
                                                     "xi2": "value_sumsq_e12"}),
                                   keys, ["value_sum_e6", "value_sumsq_e12"]))
    g["expected_count"] = [3600 // cad[t] for t in g["tag_sk"]]
    return finish(g)


def rollup_daily(hourly):
    h = hourly.copy()
    h["bucket_ts"] = h["bucket_ts"].dt.floor("D")
    counts = ["sample_count", "good_count", "bad_count", "uncertain_count", "substituted_count"]
    agg = {c: (c, "sum") for c in counts + [f"n_{s.lower()}" for s in STATES]}
    agg.update(hours_present=("sample_count", "size"),
               expected_count=("expected_count", lambda s: int(s.max()) * 24),
               value_min=("value_min", "min"), value_max=("value_max", "max"))
    keys = ["tag_sk", "tag_id", "bucket_ts"]
    g = h.groupby(keys, as_index=False).agg(**agg)
    return finish(attach(g, keys, exact_sums(h, keys, ["value_sum_e6", "value_sumsq_e12"])))


def roll(raw, cad):
    hr = rollup_hourly(raw, cad)
    return hr, rollup_daily(hr)


def same(a, b):
    try:
        pd.testing.assert_frame_equal(a.reset_index(drop=True), b.reset_index(drop=True),
                                      check_exact=True)
        return True
    except AssertionError:
        return False


def incremental(store, raw_now, cad, days):
    """Replace whole-day partitions [days] from raw -- replaceWhere, mirrored."""
    part = raw_now[raw_now["date_sk"].isin(days)]
    hr, dy = roll(part, cad)
    hs, ds = store
    hs = pd.concat([hs[~hs["date_sk"].isin(days)], hr]).sort_values(["tag_sk", "bucket_ts"])
    ds = pd.concat([ds[~ds["date_sk"].isin(days)], dy]).sort_values(["tag_sk", "bucket_ts"])
    return hs.reset_index(drop=True), ds.reset_index(drop=True)


if __name__ == "__main__":
    raw, cad = make_raw()
    day_sks = sorted(raw["date_sk"].unique())
    assert len(day_sks) == DAYS
    good_raw = raw[raw["quality_code"] == "Good"]
    print("=" * 84)
    print("02c ROLLUP")
    print("=" * 84)
    print(f"  raw rows {len(raw):,} across {raw['tag_sk'].nunique()} tags, {DAYS} days")

    hB, dB = roll(raw, cad)
    print(f"  hourly {len(hB):,}   daily {len(dB):,}")

    # --- determinism, including under a different row order ---------------------------------
    hB2, dB2 = roll(raw, cad)
    shuf = raw.sample(frac=1.0, random_state=1)
    hS, dS = roll(shuf, cad)
    assert same(hB, hB2) and same(dB, dB2), "two runs differ"
    assert same(hB, hS) and same(dB, dS), "the rollup depends on row order"
    # the reason the sums are integers: a double sum of the same values, in two orders
    _d = 0
    for (_, g) in good_raw.groupby(["tag_sk", "date_sk"]):
        v = g["value_num"].tolist()
        a = 0.0
        for x in v:
            a += x
        b = 0.0
        for x in v[::-1]:
            b += x
        _d += a != b
    print(f"\nOK  two runs identical, and identical under a shuffled row order")
    print(f"    (a double sum in two orders differs in the last bit on {_d} of "
          f"{good_raw.groupby(['tag_sk', 'date_sk']).ngroups} tag-days -- the integer sums do not)")

    # --- backfill == 30 incremental days ----------------------------------------------------
    store = (hB.iloc[0:0], dB.iloc[0:0])
    for d in day_sks:
        store = incremental(store, raw, cad, [d])
    assert same(store[0], hB), "30 incremental days != backfill (hourly)"
    assert same(store[1], dB), "30 incremental days != backfill (daily)"
    print("OK  30 one-day incremental runs equal the 30-day backfill, bitwise, at both grains")

    # --- the final day incomplete when first rolled, then re-rolled -------------------------
    last = day_sks[-1]
    cut = END - pd.Timedelta(minutes=40)
    partial = raw[raw["reading_ts"] < cut]
    store = incremental((hB.iloc[0:0], dB.iloc[0:0]), partial, cad, day_sks)
    short = store[0][(store[0]["bucket_ts"] == END - H)]
    assert (short["sample_count"] < short["expected_count"]).all() and not same(store[0], hB), (
        "truncating the final hour did not change the rollup -- the boundary case is not "
        "being exercised")
    store = incremental(store, raw, cad, [last])         # watermark = last day, re-rolled
    assert same(store[0], hB) and same(store[1], dB), "re-rolled final day != backfill"
    print("OK  a final day rolled while incomplete converges to the backfill once re-rolled")

    # --- counts ---------------------------------------------------------------------------------
    assert hB["sample_count"].sum() == len(raw) == dB["sample_count"].sum()
    assert not hB.duplicated(["tag_sk", "bucket_ts"]).any()
    assert not dB.duplicated(["tag_sk", "bucket_ts"]).any()
    assert (hB["sample_count"] <= hB["expected_count"]).all()
    assert (dB["sample_count"] <= dB["expected_count"]).all()
    print("OK  sample_count sums to raw at both grains; grain unique; sample <= expected")

    # --- missing buckets produce no row --------------------------------------------------------
    t9 = hB[hB["tag_sk"] == 9]
    assert (START + pd.Timedelta(days=6, hours=3)) not in set(t9["bucket_ts"])
    assert int((START + pd.Timedelta(days=15)).strftime("%Y%m%d")) not in \
        set(dB[dB["tag_sk"] == 9]["date_sk"])
    part = t9[t9["bucket_ts"] == START + pd.Timedelta(days=7, hours=10)]
    assert len(part) == 1 and part["sample_count"].iloc[0] < part["expected_count"].iloc[0]
    t8 = hB[hB["tag_sk"] == 8].iloc[0]
    assert t8["bucket_ts"] == START + pd.Timedelta(days=11, hours=10) and t8["sample_count"] <= 4
    print("OK  a silent hour and a silent day have no row; partial hours are reported short")

    # --- nulls are the intended ones ------------------------------------------------------------
    for df in (hB, dB):
        ng = df["good_count"] == 0
        for c in ("value_avg", "value_min", "value_max"):
            assert (df[c].isna() == ng).all(), f"{c} null other than for good_count = 0"
        assert (df["value_stddev"].isna() == (df["good_count"] < 2)).all()
    bad_h = hB[(hB["tag_sk"] == 10) & (hB["bucket_ts"] == START + pd.Timedelta(days=2, hours=7))]
    assert len(bad_h) == 1 and bad_h["good_count"].iloc[0] == 0 \
        and bad_h["sample_count"].iloc[0] > 0 and bad_h["value_avg"].isna().all()
    one = hB[(hB["tag_sk"] == 3) & (hB["bucket_ts"] == START + pd.Timedelta(days=3, hours=5))]
    assert one["good_count"].iloc[0] == 1 and one["value_stddev"].isna().all() \
        and one["value_avg"].notna().all()
    assert (hB["value_max"].dropna().values
            <= good_raw.groupby("tag_sk")["value_num"].max().reindex(
                hB.dropna(subset=["value_max"])["tag_sk"]).values).all(), \
        "a non-Good reading reached value_max"
    print(f"OK  no-Good buckets: {int((hB['good_count'] == 0).sum())} hourly, row present, "
          "values null; single-Good hour: avg set, stddev null")

    # --- min <= avg <= max -----------------------------------------------------------------------
    for df in (hB, dB):
        v = df.dropna(subset=["value_avg"])
        assert ((v["value_avg"] >= v["value_min"] - TOL)
                & (v["value_avg"] <= v["value_max"] + TOL)).all()

    # --- day grain against raw: weighting and stddev --------------------------------------------
    gr = good_raw.assign(bucket_ts=good_raw["reading_ts"].dt.floor("D"),
                         h=good_raw["reading_ts"].dt.floor("h"))
    direct = gr.groupby(["tag_sk", "bucket_ts"])["value_num"].agg(
        raw_avg="mean", raw_sd=lambda s: s.std(ddof=1)).reset_index()
    flat = (gr.groupby(["tag_sk", "bucket_ts", "h"])["value_num"].mean()
            .groupby(["tag_sk", "bucket_ts"]).mean().rename("flat_avg").reset_index())
    cmp = dB.merge(direct, on=["tag_sk", "bucket_ts"]).merge(flat, on=["tag_sk", "bucket_ts"])
    err_avg = (cmp["value_avg"] - cmp["raw_avg"]).abs()
    err_sd = (cmp["value_stddev"] - cmp["raw_sd"]).abs()
    teeth = cmp[(cmp["flat_avg"] - cmp["raw_avg"]).abs() > 100 * TOL]
    assert (err_avg <= TOL).all(), f"daily value_avg off raw by up to {err_avg.max():.3g}"
    assert (err_sd.dropna() <= TOL + 1e-9 * cmp["raw_sd"].dropna()).all(), \
        f"daily stddev off raw by up to {err_sd.max():.3g}"
    assert len(teeth) >= 20, "too few tag-days where a flat mean differs -- no teeth"
    assert ((teeth["flat_avg"] - teeth["raw_avg"]).abs() > TOL).all()
    print(f"OK  daily value_avg = raw weighted mean on all {len(cmp):,} tag-days "
          f"(max err {err_avg.max():.2g}); stddev = raw stddev (max err {err_sd.max():.2g})")
    print(f"    a flat mean of hourly averages would fail on {len(teeth):,} tag-days, "
          f"by up to {(teeth['flat_avg'] - teeth['raw_avg']).abs().max():.3g}")

    # --- a frozen day ---------------------------------------------------------------------------
    fz = dB[(dB["tag_sk"] == 7) & (dB["bucket_ts"] == START + pd.Timedelta(days=5))].iloc[0]
    x = gr[(gr["tag_sk"] == 7) & (gr["bucket_ts"] == START + pd.Timedelta(days=5))]["value_num"]
    s1, s2, n = float(x.sum()), float((x * x).sum()), len(x)
    naive = (s2 - s1 * s1 / n) / (n - 1)
    assert fz["value_stddev"] == 0.0, f"frozen day stddev {fz['value_stddev']!r}, not 0"
    print(f"OK  frozen tag-day: stddev exactly 0.0 (a double sum(x^2) formula gives variance "
          f"{naive:.3g})")
