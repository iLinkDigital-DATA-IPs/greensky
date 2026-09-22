"""Section 9: lag-1 autocorrelation measured on Running rows only.

State steps (Running -> Down -> Running) inflate lag-1 autocorrelation, so the honest
worst case for the >0.7 validation is a contiguous Running run, where only the spectral
process, the drift and the white noise are in play.
"""
import numpy as np
import pandas as pd
import harness_model as M
from harness_run import TAGS, WIN_START, WIN_END, build_state_trace, generate

print("\n9. lag-1 autocorr on RUNNING rows only -- the conservative case")
for cad in (300, 900):
    tr = build_state_trace(cad, WIN_START - pd.Timedelta(days=60), WIN_END)
    d2, i2, s2, _, _ = generate(cad, WIN_START, WIN_END, tr)
    m = s2 == "Running"
    pos = np.flatnonzero(m)
    runs = np.split(pos, np.where(np.diff(pos) != 1)[0] + 1)
    runs = [r for r in runs if len(r) > 50]
    print(f"\n   cadence {cad}s  {len(runs)} contiguous Running runs, "
          f"{sum(len(r) for r in runs):,} rows")
    print(f"   {'tag':<20}{'lag-1':>9}{'noise var share':>18}")
    worst = 1.0
    for tag in TAGS:
        v = d2[tag["tag_name"]].values
        num = den = 0.0
        for r in runs:
            x = v[r] - v[r].mean()
            num += float((x[:-1] * x[1:]).sum())
            den += float((x * x).sum())
        r1 = num / den if den else float("nan")
        sp = M.PROCESS_SD_FRACTION * 0.5 * (tag["normal_max"] - tag["normal_min"])
        share = tag["noise_sigma"] ** 2 / (sp ** 2 + tag["noise_sigma"] ** 2) if sp else float("nan")
        flag = "" if (np.isnan(r1) or r1 > 0.7) else "   <-- UNDER 0.7"
        worst = min(worst, r1) if not np.isnan(r1) else worst
        print(f"   {tag['tag_name']:<20}{r1:>9.3f}{share:>18.3f}{flag}")
    print(f"   worst: {worst:.3f}  (threshold 0.70)")
