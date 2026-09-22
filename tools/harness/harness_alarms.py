"""How often does 02b's telemetry actually breach dim_scada_tag's alarm limits?

02d derives alarms from the telemetry, so its achievable rate is fixed by the telemetry, not
by anything 02d can choose. This measures it before 02d is written.
"""
import numpy as np
import pandas as pd
import harness_model as M

AS_OF = pd.Timestamp("2026-09-15")
WIN_START, WIN_END = AS_OF - pd.Timedelta(days=30), AS_OF
CAD = 900
DEBOUNCE = 3

F = ("tag_name", "measurement_type", "uom", "normal_min", "normal_max",
     "alarm_lo", "alarm_lolo", "alarm_hi", "alarm_hihi", "resolution",
     "noise_sigma", "drift_per_year")

TEMPLATES = {
"Compressor": [
 ("suction_pressure","pressure","psig",40.,120.,30.,20.,150.,175.,.1,.8,1.5),
 ("discharge_pressure","pressure","psig",800.,1200.,700.,600.,1300.,1450.,1.,5.,12.),
 ("suction_temp","temperature","degF",60.,110.,40.,20.,130.,150.,.1,.5,1.),
 ("discharge_temp","temperature","degF",180.,280.,140.,120.,310.,350.,.1,1.5,2.),
 ("rpm","rpm","rpm",900.,1200.,800.,700.,1260.,1320.,1.,4.,3.),
 ("vibration","vibration","in/s",.05,.25,None,None,.40,.60,.001,.010,.020),
 ("flow","flow","mscfd",1500.,4500.,800.,400.,5200.,6000.,1.,35.,60.),
 ("seal_gas_pressure","pressure","psig",45.,90.,35.,25.,110.,130.,.1,.6,1.2),
 ("air_fuel_ratio","air_fuel_ratio","ratio",14.,17.5,13.,12.,19.,21.,.01,.08,.15)],
"Separator": [
 ("inlet_pressure","pressure","psig",60.,260.,45.,30.,300.,350.,.1,1.2,2.5),
 ("level","level","percent",30.,70.,20.,10.,80.,90.,.1,.6,1.),
 ("temperature","temperature","degF",70.,130.,45.,32.,150.,170.,.1,.5,1.),
 ("gas_flow","flow","mscfd",300.,2500.,150.,50.,3000.,3500.,1.,20.,45.),
 ("liquid_flow","flow","bpd",50.,600.,20.,5.,750.,900.,1.,6.,12.)],
"Storage Tank": [
 ("level","level","percent",20.,80.,12.,5.,88.,95.,.1,.4,.8),
 ("vapour_pressure","pressure","psig",.5,6.,.2,0.,8.,12.,.01,.10,.20),
 ("temperature","temperature","degF",55.,115.,35.,20.,135.,150.,.1,.5,1.),
 ("thief_hatch_position","valve_position","percent",0.,2.,None,None,5.,20.,.1,.05,.10)],
"Flare": [
 ("pilot_flame","pilot_flame","state",1.,1.,None,0.,None,None,1.,0.,0.),
 ("flow","flow","mscfd",0.,150.,None,None,400.,900.,.1,3.,5.),
 ("stack_temperature","temperature","degF",900.,1800.,600.,400.,2000.,2200.,1.,12.,20.),
 ("air_fuel_ratio","air_fuel_ratio","ratio",15.,19.,13.5,12.,22.,25.,.01,.10,.20)],
"Pump": [
 ("discharge_pressure","pressure","psig",120.,600.,90.,60.,700.,820.,.5,2.5,6.),
 ("flow","flow","bpd",100.,900.,50.,20.,1100.,1300.,1.,8.,15.),
 ("vibration","vibration","in/s",.04,.22,None,None,.35,.55,.001,.008,.015)],
"Metering Station": [
 ("flow","flow","mscfd",500.,4000.,250.,100.,4800.,5500.,1.,25.,50.),
 ("pressure","pressure","psig",250.,900.,180.,120.,1000.,1150.,.5,3.,8.),
 ("temperature","temperature","degF",50.,110.,30.,15.,130.,145.,.1,.5,1.),
 ("differential_pressure","pressure","inH2O",20.,180.,10.,4.,200.,240.,.1,1.2,2.5)],
}

# --- how many sigma out do the limits sit? ------------------------------------------------
if __name__ == "__main__":
    print("=" * 94)
    print("ALARM LIMITS IN UNITS OF THE PROCESS SD (sigma = PROCESS_SD_FRACTION x half_band)")
    print("=" * 94)
    print(f"  {'equipment':<17}{'tag':<22}{'sigma':>9}{'Hi':>9}{'HiHi':>9}{'Lo':>9}{'LoLo':>9}")
    print("  " + "-" * 90)
    rows = []
    for et, tmpls in TEMPLATES.items():
        for t in tmpls:
            d = dict(zip(F, t))
            C = .5 * (d["normal_min"] + d["normal_max"])
            H = .5 * (d["normal_max"] - d["normal_min"])
            s = M.PROCESS_SD_FRACTION * H
            def sig(v, up):
                if v is None or s == 0:
                    return None
                return (v - C) / s if up else (C - v) / s
            z = [sig(d["alarm_hi"], 1), sig(d["alarm_hihi"], 1),
                 sig(d["alarm_lo"], 0), sig(d["alarm_lolo"], 0)]
            rows.append((et, d["tag_name"], s, z))
            f = lambda x: f"{x:>9.1f}" if x is not None else f"{'-':>9}"
            print(f"  {et:<17}{d['tag_name']:<22}{s:>9.3f}" + "".join(f(x) for x in z))

    allz = [x for _, _, _, z in rows for x in z if x is not None]
    print("  " + "-" * 90)
    print(f"  limits sit {min(allz):.1f} to {max(allz):.1f} sigma from centre, median "
          f"{np.median(allz):.1f}")
    print(f"  a 3-sigma limit fires ~1 reading in 740; a {np.median(allz):.0f}-sigma limit fires "
          f"~1 in {1/max(2*(1-0.5*(1+__import__("math").erf(np.median(allz)/np.sqrt(2)))), 1e-300):.3g}")

    # --- empirical: generate a compressor and count breaches ------------------------------------
    print()
    print("=" * 94)
    print("EMPIRICAL BREACH COUNT -- one compressor, 30 days at 900s, full 02b value model")
    print("=" * 94)

    EQ, FAC = "GS-0007-E003", "GS-0007"
    tags = []
    for i, t in enumerate(TEMPLATES["Compressor"]):
        d = dict(zip(F, t)); d["tag_id"] = f"{FAC}.A2.XT-{101+i}"; tags.append(d)


    def trace(start, end):
        rows, ts, state = [], start, "Running"
        rng = M.get_rng("state_trace", EQ)
        chain = {"Shutdown": ("Maintenance", .6), "Maintenance": ("Startup", 30.),
                 "Startup": ("Running", .6), "Down": ("Maintenance", 8.),
                 "Standby": ("Startup", 40.)}
        while ts < end:
            if state == "Running":
                dur = float(rng.exponential(5. * 24))
                nxt = str(rng.choice(["Shutdown", "Down", "Standby"], p=[.5, .35, .15]))
            else:
                nxt, dur = chain[state]; dur = float(dur) * float(rng.uniform(.6, 1.4))
            rows.append((state, ts, ts + pd.Timedelta(hours=dur)))
            ts += pd.Timedelta(hours=dur); state = nxt
        return pd.DataFrame(rows, columns=["state", "start_ts", "end_ts"])


    tr = trace(WIN_START - pd.Timedelta(days=60), WIN_END)
    k0 = int(WIN_START.timestamp()) // CAD
    k1 = int(WIN_END.timestamp()) // CAD
    idx = np.arange(k0, k1)
    ts = pd.DatetimeIndex(pd.Timestamp("1970-01-01") + pd.to_timedelta(idx * CAD, unit="s"))
    i = np.clip(np.searchsorted(tr["start_ts"].values, ts.values, side="right") - 1, 0, len(tr) - 1)
    st = tr["state"].values[i]
    s0, s1 = tr["start_ts"].values[i], tr["end_ts"].values[i]
    span = (s1 - s0) / np.timedelta64(1, "s")
    phi = (ts.values - s0) / np.timedelta64(1, "s") / np.maximum(span, 1)

    la, lp = M.series_params("asset", EQ); fa, fp = M.series_params("facility", FAC)
    tsec = idx.astype(float) * CAD
    load, fac = M.spectral(la, lp, tsec), M.spectral(fa, fp, tsec)

    SUPPRESS = {"Down", "Maintenance", "Startup", "Shutdown"}
    print(f"  {'tag':<22}{'Hi':>7}{'HiHi':>7}{'Lo':>7}{'LoLo':>7}   "
          f"{'debounced alarms (Running/Standby only)':>40}")
    print("  " + "-" * 92)
    tot_alarms = 0
    for tg in tags:
        v, _ = M.value_model(tg, ts, st, phi, load, fac, idx, pd.Timestamp("2026-06-01"))
        elig = ~np.isin(st, list(SUPPRESS))
        if tg["measurement_type"] in ("flow", "rpm"):
            elig &= (st != "Standby")
        counts, alarms = [], 0
        for lim, up in ((tg["alarm_hi"], 1), (tg["alarm_hihi"], 1),
                        (tg["alarm_lo"], 0), (tg["alarm_lolo"], 0)):
            if lim is None:
                counts.append(0); continue
            br = ((v > lim) if up else (v < lim)) & elig
            counts.append(int(br.sum()))
            # debounce: count runs of >= DEBOUNCE consecutive breaches
            d = np.diff(np.concatenate(([0], br.view(np.int8), [0])))
            runs = np.flatnonzero(d == -1) - np.flatnonzero(d == 1)
            alarms += int((runs >= DEBOUNCE).sum())
        tot_alarms += alarms
        print(f"  {tg['tag_name']:<22}" + "".join(f"{c:>7,}" for c in counts)
              + f"{alarms:>40,}")

    print("  " + "-" * 92)
    print(f"  one compressor, 30 days: {tot_alarms} debounced alarms")
    print()
    inst_assets, facilities = 662, 150
    per_fac_month = tot_alarms * inst_assets / facilities
    print(f"  state mix: " + "  ".join(f"{k} {v/len(st):.1%}"
                                       for k, v in pd.Series(st).value_counts().items()))
    print(f"  naive estate extrapolation: {tot_alarms} x {inst_assets} assets / {facilities} "
          f"facilities = {per_fac_month:.1f} alarms per facility-month")
    print(f"  target band                : 10 - 60 per facility-month")
    print()
    if per_fac_month < 10:
        print("  VERDICT  the telemetry cannot supply the target alarm rate. The limits sit far")
        print("           outside the process distribution, so baseline readings never reach them.")
    else:
        print("  VERDICT  achievable from baseline telemetry.")
