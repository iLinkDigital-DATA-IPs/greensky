"""Offline mirror of 02b_gen_scada_telemetry's deterministic core.

Pure numpy/pandas, no Spark. Mirrors the hash derivation, spectral synthesis, state
conditioning, drift, noise, quantisation and the outage/freeze slot model, so the
statistical properties and the window-independence property can be checked before the
notebook ever runs on Fabric.
"""
import hashlib
import numpy as np
import pandas as pd

TOPOLOGY_SEED = 20260915
TOPOLOGY_AS_OF = pd.Timestamp("2026-09-15")


def _seed(*parts) -> int:
    h = hashlib.sha256("|".join(map(str, (TOPOLOGY_SEED, *parts))).encode()).hexdigest()
    return int(h[:16], 16) % (2 ** 32)


def get_rng(*parts) -> np.random.Generator:
    return np.random.default_rng(_seed(*parts))


# ---------------------------------------------------------------- per-row hash uniforms
def row_uniforms(tag_id, idx, n=6):
    """Mirror of sha2(concat_ws('|', SEED, tag_id, idx), 256) sliced into 32-bit chunks."""
    out = np.empty((len(idx), n))
    for i, k in enumerate(idx):
        h = hashlib.sha256(f"{TOPOLOGY_SEED}|{tag_id}|{int(k)}".encode()).hexdigest()
        for j in range(n):
            out[i, j] = (int(h[8 * j:8 * j + 8], 16) + 0.5) / 4294967296.0
    return out


# ---------------------------------------------------------------- spectral synthesis
TELEMETRY_HARMONICS = [
    (1.63, 0.35), (4.37, 0.60), (11.90, 0.85),
    (23.93, 1.00), (61.70, 0.80), (157.00, 0.60),
]
TELEMETRY_AMP_JITTER = 0.40


def series_params(kind, series_id):
    """Amplitude/phase per harmonic, hash-derived from (SEED, series_id, harmonic_index)."""
    amps, phases = [], []
    for h in range(len(TELEMETRY_HARMONICS)):
        weight = TELEMETRY_HARMONICS[h][1]
        rng = get_rng("tel_spectral", kind, series_id, h)
        u, v = rng.random(), rng.random()
        amps.append(weight * (1.0 - TELEMETRY_AMP_JITTER + 2.0 * TELEMETRY_AMP_JITTER * u))
        phases.append(2.0 * np.pi * v)
    amps = np.array(amps)
    norm = np.sqrt((amps ** 2).sum() / 2.0)      # unit variance
    return amps / norm, np.array(phases)


def spectral(amps, phases, t_sec):
    """Unit-variance smooth series; a pure function of (series_id, timestamp)."""
    out = np.zeros(len(t_sec))
    for a, p, (period_h, _) in zip(amps, phases, TELEMETRY_HARMONICS):
        w = 2.0 * np.pi / (period_h * 3600.0)
        out = out + a * np.sin(w * t_sec + p)
    return out


# ---------------------------------------------------------------- model constants
PROCESS_SD_FRACTION = 0.25
LATENT_W_IDIO, LATENT_W_LOAD, LATENT_W_FAC = 0.70, 1.00, 0.55
FACILITY_LOAD_COUPLING = 0.45

AMBIENT_MEAN_F, AMBIENT_SEASONAL_F = 65.0, 22.0
AMBIENT_DIURNAL_F, AMBIENT_LATENT_F = 14.0, 6.0
AMBIENT_PEAK_DOY, AMBIENT_PEAK_HOUR = 200.0, 16.0

PROCESS_SEASONAL_PEAK_DOY, PROCESS_DIURNAL_PEAK_HOUR = 200.0, 15.0
SEASONAL_AMPLITUDE = {"temperature": 0.040, "pressure": 0.025, "flow": 0.045, "level": 0.015,
                      "vibration": 0.010, "valve_position": 0.008, "rpm": 0.020,
                      "air_fuel_ratio": 0.008, "pilot_flame": 0.0}
DIURNAL_AMPLITUDE = {"temperature": 0.028, "pressure": 0.020, "flow": 0.030, "level": 0.012,
                     "vibration": 0.012, "valve_position": 0.010, "rpm": 0.018,
                     "air_fuel_ratio": 0.008, "pilot_flame": 0.0}

LOAD_BETA = {
    "suction_pressure": -0.85, "discharge_pressure": 0.80, "suction_temp": 0.25,
    "discharge_temp": 0.90, "rpm": 0.95, "vibration": 0.55, "flow": 0.90,
    "seal_gas_pressure": 0.30, "air_fuel_ratio": -0.35, "inlet_pressure": 0.45,
    "level": -0.45, "temperature": 0.50, "gas_flow": 0.90, "liquid_flow": 0.85,
    "vapour_pressure": 0.30, "thief_hatch_position": 0.0, "pressure": 0.55,
    "differential_pressure": 0.90, "pilot_flame": 0.0, "stack_temperature": 0.85,
}

PRESSURE_ROLE = {
    "suction_pressure": "suction", "discharge_pressure": "discharge",
    "seal_gas_pressure": "seal", "vapour_pressure": "vapour",
    "inlet_pressure": "line", "pressure": "line",
    "differential_pressure": "differential",
}
PRESSURE_REST_FRACTION = {"suction": 1.25, "discharge": 0.45, "seal": 0.30,
                          "vapour": 1.00, "line": 1.05, "differential": 0.00}
PRESSURE_STATIC_FRACTION = {"suction": 1.35, "discharge": 0.25, "seal": 0.05,
                            "vapour": 1.00, "line": 1.08, "differential": 0.00}

_ALL_ONE = {m: 1.00 for m in SEASONAL_AMPLITUDE}
STATE_ANCHOR_FRAC = {
    "Running": dict(_ALL_ONE),
    "Standby": {"flow": 0.02, "rpm": 0.00, "pressure": "REST", "temperature": 1.00,
                "level": 1.00, "vibration": 0.04, "valve_position": 1.00,
                "pilot_flame": 1.00, "air_fuel_ratio": 1.00},
    "Down":    {"flow": 0.00, "rpm": 0.00, "pressure": "STATIC", "temperature": 1.00,
                "level": 1.00, "vibration": 0.00, "valve_position": 1.00,
                "pilot_flame": 0.00, "air_fuel_ratio": 0.00},
}
STATE_ANCHOR_FRAC["Maintenance"] = dict(STATE_ANCHOR_FRAC["Down"])
STATE_RESID_SCALE = {
    "Running": dict(_ALL_ONE),
    "Standby": {"flow": 0.05, "rpm": 0.02, "pressure": 0.25, "temperature": 0.25,
                "level": 0.40, "vibration": 0.05, "valve_position": 0.30,
                "pilot_flame": 0.00, "air_fuel_ratio": 0.20},
    "Down":    {"flow": 0.00, "rpm": 0.00, "pressure": 0.10, "temperature": 0.05,
                "level": 0.15, "vibration": 0.02, "valve_position": 0.10,
                "pilot_flame": 0.00, "air_fuel_ratio": 0.00},
}
STATE_RESID_SCALE["Maintenance"] = dict(STATE_RESID_SCALE["Down"])
STATE_AMBIENT_BLEND = {"Running": 0.00, "Standby": 0.65, "Down": 1.00, "Maintenance": 1.00}

STATE_NOISE_FLOOR = 0.15
DRIFT_MAX_YEARS = 1.5
ENVELOPE_PAD = 0.10
ENVELOPE_FALLBACK = 0.75
NON_NEGATIVE_TYPES = {"flow", "rpm", "level", "vibration", "valve_position", "pilot_flame",
                      "air_fuel_ratio", "temperature"}
PRESSURE_FLOOR = -14.7

DROPOUT_RATE, BAD_RATE, UNCERTAIN_RATE = 0.002, 0.005, 0.010
OUTAGE_MEAN_DAYS, OUTAGE_MEDIAN_H = 45.0, 6.0
OUTAGE_SIGMA, OUTAGE_MAX_H = 0.894, 120.0
FAULTY_OUTAGE_MULT = 8.0
FROZEN_TAG_SHARE, FROZEN_MIN_H, FROZEN_MAX_H = 0.003, 6.0, 48.0
TELEMETRY_EPOCH = pd.Timestamp("2026-01-01")


def anchor_fraction(state, mt, tag_name):
    spec = STATE_ANCHOR_FRAC[state][mt]
    if spec == "REST":
        return PRESSURE_REST_FRACTION[PRESSURE_ROLE[tag_name]]
    if spec == "STATIC":
        return PRESSURE_STATIC_FRACTION[PRESSURE_ROLE[tag_name]]
    return float(spec)


def smoothstep(x):
    x = np.clip(np.asarray(x, dtype=float), 0.0, 1.0)
    return x * x * (3.0 - 2.0 * x)


def value_model(tag, ts, state_names, phi, load, fac, idx, drift_ref_ts):
    """The full per-row value model, mirroring the notebook's Spark expression."""
    mt, tn = tag["measurement_type"], tag["tag_name"]
    nmin, nmax = tag["normal_min"], tag["normal_max"]
    centre, half, band = 0.5 * (nmin + nmax), 0.5 * (nmax - nmin), nmax - nmin
    sigma_proc = PROCESS_SD_FRACTION * half

    t_sec = (ts - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1)
    t_sec = np.asarray(t_sec, dtype=float)
    doy = ts.dayofyear.values + ts.hour.values / 24.0
    hod = ts.hour.values + ts.minute.values / 60.0

    seas = SEASONAL_AMPLITUDE[mt] * np.cos(2 * np.pi * (doy - PROCESS_SEASONAL_PEAK_DOY) / 365.25)
    diur = DIURNAL_AMPLITUDE[mt] * np.cos(2 * np.pi * (hod - PROCESS_DIURNAL_PEAK_HOUR) / 24.0)
    ambient = (AMBIENT_MEAN_F
               + AMBIENT_SEASONAL_F * np.cos(2 * np.pi * (doy - AMBIENT_PEAK_DOY) / 365.25)
               + AMBIENT_DIURNAL_F * np.cos(2 * np.pi * (hod - AMBIENT_PEAK_HOUR) / 24.0)
               + AMBIENT_LATENT_F * fac)

    beta = LOAD_BETA[tn]
    gamma = FACILITY_LOAD_COUPLING * beta
    idio_a, idio_p = series_params("tag", tag["tag_id"])
    idio = spectral(idio_a, idio_p, t_sec)
    norm = np.sqrt(LATENT_W_IDIO ** 2 + (LATENT_W_LOAD * beta) ** 2 + (LATENT_W_FAC * gamma) ** 2)
    mix = (LATENT_W_IDIO * idio + LATENT_W_LOAD * beta * load + LATENT_W_FAC * gamma * fac) / norm

    state_names = np.asarray(state_names)

    def profile(names):
        af = np.array([anchor_fraction(s, mt, tn) for s in names])
        rs = np.array([STATE_RESID_SCALE[s][mt] for s in names])
        wamb = np.array([STATE_AMBIENT_BLEND[s] for s in names])
        base = af * centre * (1.0 + seas + diur)
        if mt == "temperature":
            base = (1.0 - wamb) * base + wamb * ambient
        return base, rs

    is_ramp = np.isin(state_names, ["Startup", "Shutdown"])
    eff = np.where(is_ramp, "Down", state_names)
    base_a, resid_a = profile(eff)
    if is_ramp.any():
        base_r, resid_r = profile(np.full(len(eff), "Running"))
        s = smoothstep(phi)
        w = np.where(state_names == "Startup", s, 1.0 - s)
        base_a = np.where(is_ramp, (1 - w) * base_a + w * base_r, base_a)
        resid_a = np.where(is_ramp, (1 - w) * resid_a + w * resid_r, resid_a)

    u = row_uniforms(tag["tag_id"], idx)
    z = np.sqrt(-2.0 * np.log(u[:, 0])) * np.cos(2.0 * np.pi * u[:, 1])

    dyears = np.clip((ts - drift_ref_ts) / pd.Timedelta(days=365.25), 0.0, DRIFT_MAX_YEARS)
    dsign = 1.0 if get_rng("tel_drift_sign", tag["tag_id"]).random() < 0.5 else -1.0

    noise_scale = np.maximum(STATE_NOISE_FLOOR, resid_a)
    v = (base_a
         + resid_a * sigma_proc * mix
         + resid_a * dsign * tag["drift_per_year"] * np.asarray(dyears, dtype=float)
         + noise_scale * tag["noise_sigma"] * z)

    alolo = tag["alarm_lolo"] if tag["alarm_lolo"] is not None else nmin - ENVELOPE_FALLBACK * band
    ahihi = tag["alarm_hihi"] if tag["alarm_hihi"] is not None else nmax + ENVELOPE_FALLBACK * band
    lo = np.minimum(base_a, alolo) - ENVELOPE_PAD * band
    hi = np.maximum(base_a, ahihi) + ENVELOPE_PAD * band
    if mt in NON_NEGATIVE_TYPES:
        lo = np.maximum(lo, 0.0)
    elif mt == "pressure":
        lo = np.maximum(lo, PRESSURE_FLOOR)
    v = np.clip(v, lo, hi)
    if tag["resolution"] > 0:
        v = np.round(v / tag["resolution"]) * tag["resolution"]
    return np.round(v, 6), u


# ---------------------------------------------------------------- outages and freezes
def outage_intervals(tag_id, status, win_start, win_end):
    """Poisson arrivals on a fixed 45-day slot grid anchored at TELEMETRY_EPOCH.

    A pure function of (SEED, tag_id, slot) -- no dependence on the run window, so an
    incremental day sees exactly the outages a backfill would have given it.
    """
    slot_s = OUTAGE_MEAN_DAYS * 86400.0
    k0 = int(np.floor((win_start - TELEMETRY_EPOCH).total_seconds() / slot_s)) - 1
    k1 = int(np.floor((win_end - TELEMETRY_EPOCH).total_seconds() / slot_s))
    lam = FAULTY_OUTAGE_MULT if status == "Faulty" else 1.0
    out = []
    for k in range(k0, k1 + 1):
        rng = get_rng("tel_outage", tag_id, k)
        n = int(rng.poisson(lam))
        if n == 0:
            continue
        offs = rng.random(n) * slot_s
        durs = np.minimum(OUTAGE_MEDIAN_H * np.exp(OUTAGE_SIGMA * rng.standard_normal(n)),
                          OUTAGE_MAX_H)
        for o, d in zip(offs, durs):
            s = TELEMETRY_EPOCH + pd.Timedelta(seconds=k * slot_s + o)
            e = s + pd.Timedelta(hours=float(d))
            if e > win_start and s < win_end:      # only outages that touch the window,
                out.append((s, e))                 # matching 02b's own filter
    return out


def freeze_interval(tag_id, win_start, win_end):
    """At most one freeze per tag per 30-day slot, on the same fixed-grid principle."""
    slot_s = 30 * 86400.0
    k0 = int(np.floor((win_start - TELEMETRY_EPOCH).total_seconds() / slot_s)) - 1
    k1 = int(np.floor((win_end - TELEMETRY_EPOCH).total_seconds() / slot_s))
    out = []
    for k in range(k0, k1 + 1):
        rng = get_rng("tel_freeze", tag_id, k)
        if rng.random() >= FROZEN_TAG_SHARE:
            continue
        off = rng.random() * slot_s
        dur = FROZEN_MIN_H + rng.random() * (FROZEN_MAX_H - FROZEN_MIN_H)
        s = TELEMETRY_EPOCH + pd.Timedelta(seconds=k * slot_s + off)
        e = s + pd.Timedelta(hours=float(dur))
        if e > win_start and s < win_end:      # only freezes that touch the window
            out.append((s, e))
    return out
