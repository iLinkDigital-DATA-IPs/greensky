# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "95f7f55e-9d06-4699-9483-754709a1d397",
# META       "default_lakehouse_name": "Planetary_computer_LH",
# META       "default_lakehouse_workspace_id": "060ba34b-f1a3-4509-a6e2-36d1e736a8eb",
# META       "known_lakehouses": [
# META         {
# META           "id": "95f7f55e-9d06-4699-9483-754709a1d397"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "cf70e84c-e5f3-9589-4218-88cc1ae7b47d",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from __future__ import annotations

import warnings
from typing import Optional

import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

warnings.filterwarnings("ignore")


# ─────────────────────────────────────────────────────────────────────────────
# 0.  Geometry helpers
# ─────────────────────────────────────────────────────────────────────────────

def _haversine_km(
    lat1: np.ndarray, lon1: np.ndarray,
    lat2: np.ndarray, lon2: np.ndarray,
) -> np.ndarray:
    """Vectorised haversine distance in km."""
    R = 6_371.0
    dlat = np.radians(lat2 - lat1)
    dlon = np.radians(lon2 - lon1)
    a = (np.sin(dlat / 2) ** 2
         + np.cos(np.radians(lat1)) * np.cos(np.radians(lat2))
         * np.sin(dlon / 2) ** 2)
    return 2 * R * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def _to_unit_sphere(lat_deg: np.ndarray, lon_deg: np.ndarray) -> np.ndarray:
    """Convert lat/lon arrays to unit-sphere Cartesian for cKDTree."""
    lat = np.radians(lat_deg)
    lon = np.radians(lon_deg)
    return np.column_stack([
        np.cos(lat) * np.cos(lon),
        np.cos(lat) * np.sin(lon),
        np.sin(lat),
    ])


def _chord_from_km(km: float, R: float = 6_371.0) -> float:
    """Convert a great-circle distance in km to unit-sphere chord length."""
    return 2.0 * np.sin(km / (2.0 * R))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# 1. Timestamp resolution (FIXED)
# ─────────────────────────────────────────────────────────────────────────────

import pandas as pd
import numpy as np
import datetime

def resolve_my_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a UTC-aware '_t' column for your TROPOMI plumes.

    Priority:
      1. acquisition_time (full timestamp)
      2. scene_id (YYYYMMDD_HHmmss)
      3. scene_date → noon UTC fallback

    Adds:
      _t
      _t_is_date_only
      _time_uncertainty_h
    """
    df = df.copy()
    n = len(df)

    ts_list = [pd.NaT] * n
    source = ["unknown"] * n

    # ── 1. acquisition_time ───────────────────────────────────────────────
    if "acquisition_time" in df.columns:
        parsed = pd.to_datetime(df["acquisition_time"], utc=True, errors="coerce")
        for i, v in enumerate(parsed):
            if pd.notna(v):
                ts_list[i] = v
                source[i] = "full"

    # ── 2. scene_id parsing ───────────────────────────────────────────────
    if "scene_id" in df.columns:
        extracted = df["scene_id"].str.extract(r"scene_(\d{8})_(\d{6})")
        for i in range(n):
            if pd.notna(ts_list[i]):
                continue
            if extracted.shape[1] == 2:
                d_part = extracted.iloc[i, 0]
                t_part = extracted.iloc[i, 1]
                if pd.notna(d_part) and pd.notna(t_part):
                    try:
                        raw = datetime.datetime.strptime(d_part + t_part, "%Y%m%d%H%M%S")
                        ts_list[i] = pd.Timestamp(raw, tz="UTC")
                        source[i] = "scene"
                    except Exception:
                        pass

    # ── 3. scene_date fallback (NOON UTC) ─────────────────────────────────
    if "scene_date" in df.columns:
        scene_dates = pd.to_datetime(df["scene_date"], errors="coerce")
        for i in range(n):
            if pd.notna(ts_list[i]):
                continue
            if pd.notna(scene_dates.iloc[i]):
                ts_list[i] = (pd.Timestamp(scene_dates.iloc[i]) + pd.Timedelta(hours=12)).tz_localize("UTC")
                source[i] = "date"

    df["_t"] = pd.array(ts_list, dtype="datetime64[ns, UTC]")

    # correct date-only flag
    df["_t_is_date_only"] = [s == "date" for s in source]

    # uncertainty (your plumes: ±12h if date-only)
    df["_time_uncertainty_h"] = np.where(df["_t_is_date_only"], 12.0, 3.0)

    n_nat = df["_t"].isna().sum()
    n_date_only = df["_t_is_date_only"].sum()
    print(f"  My timestamps: {len(df)} total | {n_nat} NaT | "
          f"{n_date_only} date-only (±12h uncertainty)")

    return df


def resolve_cm_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a UTC-aware '_t' column for Carbon Mapper records.

    Priority:
      1. cm_scene_timestamp (full timestamp)
      2. cm_date → noon UTC fallback

    Adds:
      _t
      _t_is_date_only
      _time_uncertainty_h
    """
    df = df.copy()
    n = len(df)

    ts_list = [pd.NaT] * n

    # ── 1. full timestamp ────────────────────────────────────────────────
    if "cm_scene_timestamp" in df.columns:
        parsed = pd.to_datetime(df["cm_scene_timestamp"], utc=True, errors="coerce")
        for i, v in enumerate(parsed):
            if pd.notna(v):
                ts_list[i] = v

    # ── 2. fallback: cm_date (NOON UTC) ──────────────────────────────────
    n_fallback = 0
    if "cm_date" in df.columns:
        cm_dates = pd.to_datetime(df["cm_date"], errors="coerce")
        for i in range(n):
            if pd.notna(ts_list[i]):
                continue
            if pd.notna(cm_dates.iloc[i]):
                ts_list[i] = (pd.Timestamp(cm_dates.iloc[i]) + pd.Timedelta(hours=12)).tz_localize("UTC")
                n_fallback += 1

    df["_t"] = pd.array(ts_list, dtype="datetime64[ns, UTC]")

    # correct date-only flag
    if "cm_scene_timestamp" in df.columns:
        df["_t_is_date_only"] = pd.to_datetime(
            df["cm_scene_timestamp"], errors="coerce"
        ).isna()
    else:
        df["_t_is_date_only"] = True

    # uncertainty (CM is noisier → ±24h)
    df["_time_uncertainty_h"] = np.where(df["_t_is_date_only"], 24.0, 3.0)

    n_nat = df["_t"].isna().sum()
    print(f"  CM timestamps:  {len(df)} total | {n_nat} NaT | "
          f"{n_fallback} fell back to date-only")

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import matplotlib.pyplot as plt


# ─────────────────────────────────────────────────────────────────────────────
# MatchDiagnostics
# ─────────────────────────────────────────────────────────────────────────────

class MatchDiagnostics:
    """Collects per-pair and aggregate diagnostics during the matching pipeline."""

    def __init__(self):
        # ── Per-pair records ──────────────────────────────────────────────
        self.records: list[dict] = []

        # ── Aggregate counters ────────────────────────────────────────────
        self.n_mine: int = 0
        self.n_cm: int = 0
        self.n_pass: int = 0
        self.n_fail_spatial_only: int = 0
        self.n_fail_temporal_only: int = 0
        self.n_fail_both: int = 0
        self.n_no_candidate: int = 0
        self.n_no_cm_candidate: int = 0

    # ── Per-pair API ──────────────────────────────────────────────────────
    def add(self, **kwargs):
        self.records.append(kwargs)

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.records)

    # ── Core summary (internal helper) ───────────────────────────────────
    def _print_summary(self):
        print(f"  Mine={self.n_mine}  CM={self.n_cm}")
        print(f"  Pass               : {self.n_pass}")
        print(f"  Fail spatial only  : {self.n_fail_spatial_only}")
        print(f"  Fail temporal only : {self.n_fail_temporal_only}")
        print(f"  Fail both          : {self.n_fail_both}")
        no_cand = self.n_no_cm_candidate or self.n_no_candidate
        print(f"  No candidate       : {no_cand}")
        df = self.to_dataframe()
        if df.empty:
            return
        for col in ["dist_km", "dt_h", "score"]:
            if col in df.columns:
                print(f"  {col}: min={df[col].min():.2f}  "
                      f"median={df[col].median():.2f}  "
                      f"max={df[col].max():.2f}")

    # ── Public aliases — all names match_plumes might call ────────────────
    def summary(self):
        self._print_summary()

    def print_report(self):          # ← called by match_plumes at line 232
        self._print_summary()

    def report(self):
        self._print_summary()

    # ── Core plot (internal helper) ───────────────────────────────────────
    def _plot(
        self,
        dist_threshold_km: float = 50.0,
        dt_threshold_h: float = 120.0,
        save_path: Optional[str] = None,
    ):
        df = self.to_dataframe()
        if df.empty:
            print("  No diagnostics to plot.")
            return

        fig, axes = plt.subplots(1, 3, figsize=(15, 4))

        for ax, col, label, threshold in zip(
            axes,
            ["dist_km", "dt_h", "score"],
            ["Distance (km)", "Δt (hours)", "Match score"],
            [dist_threshold_km, dt_threshold_h, None],
        ):
            if col not in df.columns:
                ax.set_visible(False)
                continue
            ax.hist(df[col].dropna(), bins=30, edgecolor="white")
            ax.set_xlabel(label)
            ax.set_ylabel("Count")
            ax.set_title(f"Distribution of {label}")
            if threshold is not None:
                ax.axvline(threshold, color="red", linestyle="--",
                           label=f"threshold={threshold}")
                ax.legend()

        plt.tight_layout()
        if save_path:
            plt.savefig(save_path, dpi=120)
            print(f"  Diagnostics plot saved → {save_path}")
        plt.show()

    # ── Public aliases — all names match_plumes might call ────────────────
    def plot(self, dist_threshold_km: float = 50.0,
             dt_threshold_h: float = 120.0,
             save_path: Optional[str] = None):
        self._plot(dist_threshold_km, dt_threshold_h, save_path)

    def plot_histograms(self, dist_threshold_km: float = 50.0,
                        dt_threshold_h: float = 120.0,
                        save_path: Optional[str] = None):   # ← called at line 234
        self._plot(dist_threshold_km, dt_threshold_h, save_path)

    def plot_diagnostics(self, dist_threshold_km: float = 50.0,
                         dt_threshold_h: float = 120.0,
                         save_path: Optional[str] = None):
        self._plot(dist_threshold_km, dt_threshold_h, save_path)


# ─────────────────────────────────────────────────────────────────────────────
# compute_diagnostics
# ─────────────────────────────────────────────────────────────────────────────

def compute_diagnostics(
    mine_valid: pd.DataFrame,
    cm_valid: pd.DataFrame,
    dist_threshold_km: float = 50.0,
    dt_threshold_h: float = 120.0,
) -> "MatchDiagnostics":
    diag = MatchDiagnostics()

    if cm_valid.empty:
        diag.n_no_cm_candidate = len(mine_valid)
        diag.n_no_candidate    = len(mine_valid)
        return diag

    cm_lat = cm_valid["cm_lat"].values
    cm_lon = cm_valid["cm_lon"].values
    cm_xyz = _to_unit_sphere(cm_lat, cm_lon)
    chord  = _chord_from_km(dist_threshold_km)
    tree   = cKDTree(cm_xyz)

    my_lat = mine_valid["lat"].values if "lat" in mine_valid.columns else mine_valid["my_lat"].values
    my_lon = mine_valid["lon"].values if "lon" in mine_valid.columns else mine_valid["my_lon"].values
    my_xyz = _to_unit_sphere(my_lat, my_lon)

    # Spatial candidates within dist_threshold_km
    spatial_candidates = tree.query_ball_point(my_xyz, chord)

    # Nearest-neighbour distance & dt for every mine plume
    nn_dist_km = np.full(len(mine_valid), np.nan)
    nn_dt_h    = np.full(len(mine_valid), np.nan)

    for i in range(len(mine_valid)):
        _, j = tree.query(my_xyz[i], k=1)
        nn_dist_km[i] = _haversine_km(my_lat[i], my_lon[i], cm_lat[j], cm_lon[j])
        t_mine = mine_valid["_t"].iloc[i]
        t_cm   = cm_valid["_t"].iloc[j] if "_t" in cm_valid.columns else pd.NaT
        if pd.notna(t_mine) and pd.notna(t_cm):
            nn_dt_h[i] = abs((t_mine - t_cm).total_seconds()) / 3600.0

    for i in range(len(mine_valid)):
        cands = spatial_candidates[i]
        pass_temporal = np.isnan(nn_dt_h[i]) or nn_dt_h[i] <= dt_threshold_h

        if len(cands) == 0:
            diag.n_no_cm_candidate += 1
            diag.n_no_candidate    += 1
            if pass_temporal:
                diag.n_fail_spatial_only += 1
            else:
                diag.n_fail_both += 1
        else:
            if pass_temporal:
                diag.n_pass += 1
            else:
                diag.n_fail_temporal_only += 1

        diag.add(dist_km=nn_dist_km[i], dt_h=nn_dt_h[i], score=np.nan)

    return diag


# ─────────────────────────────────────────────────────────────────────────────
# cluster_cm_sources
# ─────────────────────────────────────────────────────────────────────────────

def cluster_cm_sources(
    df_cm: pd.DataFrame,
    cluster_radius_km: float = 5.0,
    cluster_window_days: float = 30.0,
    strategy: str = "median",
) -> pd.DataFrame:
    if df_cm.empty:
        return df_cm

    df    = df_cm.copy().reset_index(drop=True)
    lat   = df["cm_lat"].values
    lon   = df["cm_lon"].values
    xyz   = _to_unit_sphere(lat, lon)
    chord = _chord_from_km(cluster_radius_km)
    tree  = cKDTree(xyz)

    visited     = np.zeros(len(df), dtype=bool)
    cluster_ids = np.full(len(df), -1, dtype=int)
    cid = 0

    for i in range(len(df)):
        if visited[i]:
            continue
        neighbours = np.array(tree.query_ball_point(xyz[i], chord))
        if "_t" in df.columns and df["_t"].notna().any():
            t_i      = df["_t"].iloc[i]
            window   = pd.Timedelta(days=cluster_window_days)
            filtered = [
                j for j in neighbours
                if pd.notna(df["_t"].iloc[j]) and abs(df["_t"].iloc[j] - t_i) <= window
            ]
            if filtered:
                neighbours = np.array(filtered)
        cluster_ids[neighbours] = cid
        visited[neighbours]     = True
        cid += 1

    df["_cluster_id"] = cluster_ids
    rows = []
    for c, grp in df.groupby("_cluster_id"):
        if strategy == "latest":
            rep = grp.sort_values("_t").iloc[-1]
        elif strategy == "first":
            rep = grp.sort_values("_t").iloc[0]
        else:  # median
            rep = grp.iloc[0].copy()
            rep["cm_lat"] = grp["cm_lat"].median()
            rep["cm_lon"] = grp["cm_lon"].median()
            if "_t" in grp.columns:
                valid_t = grp["_t"].dropna()
                if not valid_t.empty:
                    rep["_t"] = valid_t.sort_values().iloc[len(valid_t) // 2]
        rows.append(rep)

    result = pd.DataFrame(rows).drop(columns=["_cluster_id"], errors="ignore")
    print(f"  CM clustering: {len(df)} → {len(result)} records "
          f"(radius={cluster_radius_km} km, window={cluster_window_days} d, strategy='{strategy}')")
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# 3. Scoring functions (PATCHED)
# ─────────────────────────────────────────────────────────────────────────────

def spatial_score(dist_km: float, d_scale_km: float = 30.0) -> float:
    """Exponential spatial decay."""
    return float(np.exp(-dist_km / d_scale_km))


def temporal_score(
    dt_h: float,
    t_scale_h: float = 720.0,      # 30 days (aligned with your data)
    date_only_penalty: float = 0.7,
    dt_cap_h: float = 720.0,       # cap at 30 days
) -> float:
    """
    Exponential temporal decay with cap + uncertainty handling.

    Fixes:
    - caps extreme dt (prevents score collapse)
    - weakens penalty for date-only timestamps
    """
    if np.isnan(dt_h):
        return date_only_penalty

    # Cap extreme deltas
    dt_h = min(dt_h, dt_cap_h)

    score = np.exp(-dt_h / t_scale_h)

    # Apply penalty only if date-only
    return float(score)


def combined_score(
    dist_km: float,
    dt_h: float,
    d_scale_km: float = 30.0,
    t_scale_h: float = 720.0,   # FIXED (was 120 → too harsh)
    w_spatial: float = 0.6,
    w_temporal: float = 0.4,
    date_only: bool = False,
) -> float:
    """
    Weighted combination of spatial and temporal scores.

    Key design:
    - Spatial slightly dominates (more reliable signal)
    - Temporal is informative but not destructive
    """

    ss = spatial_score(dist_km, d_scale_km)

    ts = temporal_score(
        dt_h,
        t_scale_h=t_scale_h,
        date_only_penalty=0.7
    )

    # Apply date-only penalty AFTER computing temporal score
    if date_only:
        ts *= 0.7

    # Prevent temporal score from collapsing overall match
    ts = max(ts, 0.3)

    return float(w_spatial * ss + w_temporal * ts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# 4. Per-plume diagnostics (PATCHED)
# ─────────────────────────────────────────────────────────────────────────────

def compute_diagnostics(
    mine_valid: pd.DataFrame,
    cm_valid: pd.DataFrame,
    dist_threshold_km: float = 50.0,
    dt_threshold_h: float = 120.0,
) -> MatchDiagnostics:

    diag = MatchDiagnostics()
    diag.dist_threshold_km = dist_threshold_km
    diag.dt_threshold_h    = dt_threshold_h

    # Ensure clean indexing
    mine_valid = mine_valid.reset_index(drop=True)
    cm_valid   = cm_valid.reset_index(drop=True)

    diag.n_mine = len(mine_valid)
    diag.n_cm   = len(cm_valid)

    if len(mine_valid) == 0 or len(cm_valid) == 0:
        return diag

    # ── KD-tree nearest neighbour ────────────────────────────────────────
    cm_xyz   = _to_unit_sphere(cm_valid["cm_lat"].values, cm_valid["cm_lon"].values)
    mine_xyz = _to_unit_sphere(mine_valid["centroid_lat"].values, mine_valid["centroid_lon"].values)

    tree = cKDTree(cm_xyz)

    chord_dists, nn_idxs = tree.query(mine_xyz, k=1)

    nn_dist_km = 2 * 6371.0 * np.arcsin(np.clip(chord_dists / 2, 0, 1))

    nn_dt_h = np.full(len(mine_valid), np.nan)

    for i, cm_idx in enumerate(nn_idxs):
        my_t = mine_valid.loc[i, "_t"] if "_t" in mine_valid.columns else pd.NaT
        cm_t = cm_valid.loc[int(cm_idx), "_t"] if "_t" in cm_valid.columns else pd.NaT

        if pd.notna(my_t) and pd.notna(cm_t):
            dt_h = abs((my_t - cm_t).total_seconds()) / 3600.0

            # subtract uncertainty if available
            unc_m = mine_valid.loc[i].get("_time_uncertainty_h", 0)
            unc_c = cm_valid.loc[int(cm_idx)].get("_time_uncertainty_h", 0)

            dt_h = max(0, dt_h - (unc_m + unc_c))

            nn_dt_h[i] = dt_h

    diag.nearest_dist_km = nn_dist_km
    diag.nearest_dt_h    = nn_dt_h

    # ── Failure mode classification ──────────────────────────────────────
    chord_thresh = _chord_from_km(dist_threshold_km)
    spatial_candidates = tree.query_ball_point(mine_xyz, r=chord_thresh)

    for i in range(len(mine_valid)):
        cands = spatial_candidates[i]

        if len(cands) == 0:
            diag.n_no_cm_candidate += 1

            pass_temporal = np.isnan(nn_dt_h[i]) or nn_dt_h[i] <= dt_threshold_h

            if pass_temporal:
                diag.n_fail_spatial_only += 1
            else:
                diag.n_fail_both += 1

            continue

        # Evaluate temporal over all spatial candidates (FIXED)
        valid_dt = []

        for j in cands:
            my_t = mine_valid.loc[i, "_t"]
            cm_t = cm_valid.loc[int(j), "_t"]

            if pd.notna(my_t) and pd.notna(cm_t):
                dt_h = abs((my_t - cm_t).total_seconds()) / 3600.0

                unc_m = mine_valid.loc[i].get("_time_uncertainty_h", 0)
                unc_c = cm_valid.loc[int(j)].get("_time_uncertainty_h", 0)

                dt_h = max(0, dt_h - (unc_m + unc_c))

                valid_dt.append(dt_h)

        best_dt = min(valid_dt) if len(valid_dt) > 0 else np.nan

        pass_temporal = np.isnan(best_dt) or best_dt <= dt_threshold_h

        if pass_temporal:
            diag.n_pass += 1
        else:
            diag.n_fail_temporal_only += 1

    return diag

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# 5.  Main matching function (FULLY PATCHED)
# ─────────────────────────────────────────────────────────────────────────────

def match_plumes(
    df_mine: pd.DataFrame,
    df_cm: pd.DataFrame,

    # Spatial pre-filter (increased to avoid dropping valid candidates)
    spatial_km: float = 200.0,

    # Scoring decay scales (absolute exponential, NOT Gaussian)
    d_scale_km: float = 30.0,
    t_scale_h: float = 720.0,   # FIXED: was 120 → now 30 days

    # Score weights
    w_spatial: float = 0.6,
    w_temporal: float = 0.4,

    # Match thresholds
    score_threshold: float = 0.1,

    # Temporal hard cap (optional)
    max_temporal_h: Optional[float] = None,

    # Matching behavior
    allow_many_to_one: bool = True,

    # CM clustering
    cluster_cm: bool = True,
    cluster_radius_km: float = 5.0,
    cluster_window_days: float = 30.0,
    cluster_strategy: str = "median",   # FIXED

    # Diagnostics
    run_diagnostics: bool = True,
    diag_dist_threshold_km: float = 50.0,
    diag_dt_threshold_h: float = 120.0,
    save_diag_plot: Optional[str] = "match_diagnostics.png",
    debug: bool = True,
) -> tuple[pd.DataFrame, MatchDiagnostics]:

    # ── Input validation ──────────────────────────────────────────────────
    required_mine = {"centroid_lat", "centroid_lon", "emission_kg_hr"}
    required_cm   = {"cm_lat", "cm_lon", "cm_emission_kg_hr"}

    for col in required_mine:
        if col not in df_mine.columns:
            raise ValueError(f"df_mine missing column: {col!r}")
    for col in required_cm:
        if col not in df_cm.columns:
            raise ValueError(f"df_cm missing column: {col!r}")

    mine_valid = df_mine.dropna(subset=list(required_mine)).copy().reset_index(drop=True)
    cm_valid   = df_cm.dropna(subset=list(required_cm)).copy().reset_index(drop=True)

    if len(mine_valid) == 0 or len(cm_valid) == 0:
        print("WARNING: empty input after null-drop")
        return pd.DataFrame(), MatchDiagnostics()

    # ── Resolve timestamps ────────────────────────────────────────────────
    print("\n── Timestamp resolution ──")
    mine_valid = resolve_my_timestamps(mine_valid)
    cm_valid   = resolve_cm_timestamps(cm_valid)

    # ── Temporal overlap diagnostic ───────────────────────────────────────
    if debug:
        _print_temporal_overlap(mine_valid, cm_valid)

    # ── Cluster CM sources ────────────────────────────────────────────────
    if cluster_cm:
        print("\n── CM source clustering ──")
        cm_valid = cluster_cm_sources(
            cm_valid,
            cluster_radius_km=cluster_radius_km,
            cluster_window_days=cluster_window_days,
            strategy=cluster_strategy,
        ).reset_index(drop=True)

    # ── Diagnostics ───────────────────────────────────────────────────────
    diag = MatchDiagnostics()
    if run_diagnostics:
        print("\n── Pre-match diagnostics ──")
        diag = compute_diagnostics(
            mine_valid, cm_valid,
            dist_threshold_km=diag_dist_threshold_km,
            dt_threshold_h=diag_dt_threshold_h,
        )
        diag.n_mine = len(mine_valid)
        diag.n_cm   = len(cm_valid)

    # ── Spatial pre-filter ────────────────────────────────────────────────
    cm_xyz   = _to_unit_sphere(cm_valid["cm_lat"].values, cm_valid["cm_lon"].values)
    mine_xyz = _to_unit_sphere(mine_valid["centroid_lat"].values, mine_valid["centroid_lon"].values)

    tree = cKDTree(cm_xyz)
    chord_thresh = _chord_from_km(spatial_km)
    candidate_lists = tree.query_ball_point(mine_xyz, r=chord_thresh)

    n_spatial = sum(len(c) for c in candidate_lists)
    n_with_candidates = sum(1 for c in candidate_lists if len(c) > 0)

    print(f"\nSpatial pre-filter ({spatial_km} km):")
    print(f"  {n_with_candidates}/{len(mine_valid)} my plumes have ≥1 CM candidate")
    print(f"  {n_spatial} total candidate pairs")

    # ── Score candidates ──────────────────────────────────────────────────
    pending = []

    for my_idx, cm_candidates in enumerate(candidate_lists):
        if not cm_candidates:
            continue

        m = mine_valid.loc[my_idx]
        my_lat, my_lon = m["centroid_lat"], m["centroid_lon"]
        my_t = m.get("_t", pd.NaT)
        my_unc = m.get("_time_uncertainty_h", 0)
        my_date_only = m.get("_t_is_date_only", False)

        for cm_idx in cm_candidates:
            cm = cm_valid.loc[cm_idx]

            # Distance
            dist_km = float(_haversine_km(
                np.array([my_lat]), np.array([my_lon]),
                np.array([cm["cm_lat"]]), np.array([cm["cm_lon"]]),
            )[0])

            # Time delta (WITH uncertainty correction)
            cm_t = cm.get("_t", pd.NaT)
            cm_unc = cm.get("_time_uncertainty_h", 0)

            dt_h = np.nan
            if pd.notna(my_t) and pd.notna(cm_t):
                dt_h = abs((my_t - cm_t).total_seconds()) / 3600.0
                dt_h = max(0, dt_h - (my_unc + cm_unc))
                dt_h = min(dt_h, 720.0)  # cap extreme values

            # Optional hard cap
            if max_temporal_h is not None and not np.isnan(dt_h):
                if dt_h > max_temporal_h:
                    continue

            either_date_only = my_date_only or cm.get("_t_is_date_only", False)

            sc = combined_score(
                dist_km=dist_km,
                dt_h=dt_h,
                d_scale_km=d_scale_km,
                t_scale_h=t_scale_h,
                w_spatial=w_spatial,
                w_temporal=w_temporal,
                date_only=either_date_only,
            )

            if sc >= score_threshold:
                pending.append((sc, dist_km, dt_h, my_idx, cm_idx))

    pending.sort(key=lambda x: -x[0])

    print(f"\nScored candidates (score ≥ {score_threshold}): {len(pending)}")

    if debug and pending:
        avg_dt = np.nanmean([p[2] for p in pending])
        print(f"  Avg Δt (scored): {avg_dt:.1f} h")

    if not pending:
        print("\nNo candidates above score threshold.")
        diag.n_matched = 0
        diag.print_report()
        return pd.DataFrame(), diag

    # ── Greedy matching ───────────────────────────────────────────────────
    matched_my = set()
    matched_cm = set()
    match_pairs = []

    for sc, dist_km, dt_h, my_idx, cm_idx in pending:
        if my_idx in matched_my:
            continue
        if cm_idx in matched_cm and not allow_many_to_one:
            continue

        matched_my.add(my_idx)
        if not allow_many_to_one:
            matched_cm.add(cm_idx)

        match_pairs.append((my_idx, cm_idx, dist_km, dt_h, sc))

    # ── Build output ──────────────────────────────────────────────────────
    rows = []
    for my_idx, cm_idx, dist_km, dt_h, sc in match_pairs:
        m  = mine_valid.loc[my_idx]
        cm = cm_valid.loc[cm_idx]

        my_em = float(m["emission_kg_hr"])
        cm_em = float(cm["cm_emission_kg_hr"])

        rows.append({
            "my_plume_key": m.get("my_plume_key", f"mine_{my_idx}"),
            "cm_id": cm.get("cm_id", f"cm_{cm_idx}"),
            "cm_cluster_id": cm.get("cm_cluster_id"),
            "cm_cluster_n": cm.get("cm_cluster_n", 1),
            "distance_km": round(dist_km, 3),
            "time_delta_h": round(dt_h, 2) if not np.isnan(dt_h) else None,
            "match_score": round(sc, 4),
            "my_emission_kg_hr": round(my_em, 2),
            "cm_emission_kg_hr": round(cm_em, 2),
            "emission_ratio_my_cm": round(my_em / cm_em, 4) if cm_em > 0 else None,
        })

    matched = pd.DataFrame(rows).sort_values("match_score", ascending=False).reset_index(drop=True)
    diag.n_matched = len(matched)

    # ── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'─'*50}")
    print(f"  Matching results:")
    print(f"    My plumes:        {len(mine_valid)}")
    print(f"    CM sources:       {len(cm_valid)}")
    print(f"    Matched pairs:    {len(matched)}")
    print(f"    Match rate:       {100*len(matched)/len(mine_valid):.1f}%")

    if len(matched) > 0:
        print(f"    Median distance:  {matched['distance_km'].median():.2f} km")
        print(f"    Median score:     {matched['match_score'].median():.3f}")
        if matched["time_delta_h"].notna().any():
            print(f"    Median Δt:        {matched['time_delta_h'].median():.1f} h")

    print(f"{'─'*50}\n")

    if run_diagnostics:
        diag.print_report()
        if save_diag_plot:
            diag.plot_histograms(save_path=save_diag_plot)

    return matched, diag

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# 6. Helper functions (PATCHED)
# ─────────────────────────────────────────────────────────────────────────────

def _print_temporal_overlap(mine: pd.DataFrame, cm: pd.DataFrame) -> None:
    print("\n── Temporal overlap diagnostic ──")

    mine_valid = mine.dropna(subset=["_t"])
    cm_valid   = cm.dropna(subset=["_t"])

    if len(mine_valid) == 0:
        print("  ⚠ All my timestamps are NaT — check scene_id / scene_date parsing")
        return
    if len(cm_valid) == 0:
        print("  ⚠ All CM timestamps are NaT — check cm_scene_timestamp parsing")
        return

    mine_t = mine_valid["_t"]
    cm_t   = cm_valid["_t"]

    print(f"  My date range:  {mine_t.min().date()} → {mine_t.max().date()}")
    print(f"  CM date range:  {cm_t.min().date()} → {cm_t.max().date()}")

    # Surface uncertainty (NEW)
    if "_t_is_date_only" in mine.columns:
        pct = 100 * mine["_t_is_date_only"].mean()
        print(f"  My date-only timestamps: {pct:.1f}%")

    if "_t_is_date_only" in cm.columns:
        pct = 100 * cm["_t_is_date_only"].mean()
        print(f"  CM date-only timestamps: {pct:.1f}%")

    overlap_s = max(mine_t.min(), cm_t.min())
    overlap_e = min(mine_t.max(), cm_t.max())

    if overlap_s < overlap_e:
        n_mine = mine_valid["_t"].between(overlap_s, overlap_e).sum()
        n_cm   = cm_valid["_t"].between(overlap_s, overlap_e).sum()

        print(f"  Overlap window: {overlap_s.date()} → {overlap_e.date()}")
        print(f"  My plumes in overlap:  {n_mine}/{len(mine_valid)}")
        print(f"  CM plumes in overlap:  {n_cm}/{len(cm_valid)}")
    else:
        print("  ★ NO TEMPORAL OVERLAP — date ranges do not intersect!")
        print("    This is the primary cause of low match rate.")
        print("    Fix: expand CM fetch window (cm_date_start/cm_date_end)")


def _print_tuning_advice(
    mine, cm,
    spatial_km, d_scale_km, t_scale_h, score_threshold,
) -> None:
    """Print concrete parameter suggestions when no matches are found."""

    print("\n── Tuning advice ──")
    print(f"  Current: spatial_km={spatial_km}, d_scale_km={d_scale_km}, "
          f"t_scale_h={t_scale_h}, score_threshold={score_threshold}")

    print("  Suggestions:")
    print("    1. Run with run_diagnostics=True to see failure-mode breakdown")
    print("    2. Increase spatial_km to 200–300 (avoid dropping valid candidates)")
    print("    3. Use t_scale_h ≈ 720 (30 days; CM revisits are sparse)")
    print("    4. Lower score_threshold to 0.05 if matches are too sparse")
    print("    5. Check temporal overlap — misaligned date ranges kill matches")


def recommend_parameters(
    nearest_dist_km_p50: float,
    nearest_dt_h_p50: float,
) -> dict:
    """
    Suggest stable parameters based on diagnostics.
    """

    # Spatial: moderate scaling
    d_scale = np.clip(nearest_dist_km_p50 * 1.5, 10.0, 100.0)

    # Temporal: capped to prevent explosion (KEY FIX)
    t_scale = np.clip(nearest_dt_h_p50 * 0.5, 72.0, 720.0)

    # Pre-filter radius
    spatial_km = np.clip(nearest_dist_km_p50 * 4, 100.0, 300.0)

    print("Recommended parameters (based on your data):")
    print(f"  d_scale_km      = {d_scale:.0f}")
    print(f"  t_scale_h       = {t_scale:.0f}")
    print(f"  spatial_km      = {spatial_km:.0f}")
    print(f"  score_threshold = 0.10")

    return {
        "d_scale_km": float(d_scale),
        "t_scale_h": float(t_scale),
        "spatial_km": float(spatial_km),
        "score_threshold": 0.10,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────────────────────────
# 7.   Run Pipeline block (FULLY PATCHED)
# ─────────────────────────────────────────────────────────────────────────────

def run_matching_pipeline(
    df_mine: pd.DataFrame,
    df_cm: pd.DataFrame,
    cm_date_start: str,
    cm_date_end: str,
) -> tuple[pd.DataFrame, MatchDiagnostics]:
    """
    Drop-in replacement for your Section 6 matching call.

    Usage in notebook:
        matched, diag = run_matching_pipeline(df_mine, df_cm,
                                              cm_date_start, cm_date_end)

    Returns
    -------
    matched : pd.DataFrame
    diag    : MatchDiagnostics
    """

    print("\n════════════════════════════════════════════════════════════")
    print("  RUNNING PLUME MATCHING PIPELINE")
    print("════════════════════════════════════════════════════════════")

    print(f"  CM date window (input): {cm_date_start} → {cm_date_end}")

    matched, diag = match_plumes(
        df_mine=df_mine,
        df_cm=df_cm,

        # ── Spatial ──────────────────────────────────────────────────────
        spatial_km=200.0,

        # ── Scoring (FIXED) ──────────────────────────────────────────────
        d_scale_km=30.0,
        t_scale_h=720.0,          # FIXED (was 120)

        # Spatial should dominate
        w_spatial=0.6,
        w_temporal=0.4,

        # ── Thresholds ───────────────────────────────────────────────────
        score_threshold=0.10,
        max_temporal_h=None,

        # ── Matching behavior ────────────────────────────────────────────
        allow_many_to_one=True,

        # ── CM clustering (FIXED) ────────────────────────────────────────
        cluster_cm=True,
        cluster_radius_km=5.0,
        cluster_window_days=30.0,
        cluster_strategy="median",   # FIXED (was "latest")

        # ── Diagnostics ──────────────────────────────────────────────────
        run_diagnostics=True,
        diag_dist_threshold_km=50.0,
        diag_dt_threshold_h=120.0,
        save_diag_plot="match_diagnostics.png",
        debug=True,
    )

    print("\n════════════════════════════════════════════════════════════")
    print("  PIPELINE COMPLETE")
    print("════════════════════════════════════════════════════════════")

    return matched, diag

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Load data from Lakehouse (FULLY PATCHED) ────────────────────────────────

import pandas as pd
import numpy as np

# ── Helper: safe column selection ───────────────────────────────────────────
def safe_select(df_spark, cols):
    existing = [c for c in cols if c in df_spark.columns]
    missing  = [c for c in cols if c not in df_spark.columns]
    if missing:
        print(f"⚠ Missing columns (ignored): {missing}")
    return df_spark.select(*existing)

# ── Load base tables ────────────────────────────────────────────────────────
df_mine_spark_raw = spark.read.table('Planetary_computer_LH.silver.ch4_plume_catalog')
df_cm_spark_raw   = spark.read.table('Planetary_computer_LH.silver.carbon_mapper_cache')

# ── Select only available columns ───────────────────────────────────────────
df_mine_spark = safe_select(
    df_mine_spark_raw,
    [
        "scene_id",
        "plume_id",
        "centroid_lat",
        "centroid_lon",
        "emission_rate_kg_s",
        "scene_date",   # may not exist
        "area_km2",
        "max_delta_ch4_ppb",
        "ime_kg",
        "wind_speed_ms",
        "wind_aligned",
    ],
)

df_cm_spark = safe_select(
    df_cm_spark_raw,
    [
        "cm_id",
        "cm_lat",
        "cm_lon",
        "cm_emission_kg_hr",
        "cm_emission_uncertainty_kg_hr",
        "cm_date",
        "cm_scene_timestamp",
        "cm_instrument",
    ],
)

# ── Convert to pandas ───────────────────────────────────────────────────────
df_mine = df_mine_spark.toPandas()
df_cm   = df_cm_spark.toPandas()

# ── Type enforcement (CRITICAL) ─────────────────────────────────────────────
for col in ["centroid_lat", "centroid_lon"]:
    if col in df_mine.columns:
        df_mine[col] = pd.to_numeric(df_mine[col], errors="coerce")

for col in ["cm_lat", "cm_lon"]:
    if col in df_cm.columns:
        df_cm[col] = pd.to_numeric(df_cm[col], errors="coerce")

# ── Emission conversion (kg/s → kg/hr) ──────────────────────────────────────
df_mine["emission_rate_kg_s"] = pd.to_numeric(df_mine["emission_rate_kg_s"], errors="coerce")
df_mine["emission_kg_hr"] = df_mine["emission_rate_kg_s"] * 3600.0

# ── Scene date reconstruction (FIXED ROOT CAUSE) ────────────────────────────
if "scene_date" not in df_mine.columns:
    df_mine["scene_date"] = pd.NaT

# Always attempt reconstruction from scene_id (more reliable)
scene_from_id = pd.to_datetime(
    df_mine["scene_id"].astype(str).str.extract(r"scene_(\d{8})")[0],
    format="%Y%m%d",
    errors="coerce",
)

df_mine["scene_date"] = df_mine["scene_date"].fillna(scene_from_id)
df_mine["scene_date"] = pd.to_datetime(df_mine["scene_date"], errors="coerce")

# ── Unique plume key ────────────────────────────────────────────────────────
if "my_plume_key" not in df_mine.columns:
    df_mine["my_plume_key"] = (
        df_mine["scene_id"].astype(str) + "_p" + df_mine["plume_id"].astype(str)
    )

# ── Drop invalid rows early ─────────────────────────────────────────────────
df_mine = df_mine.dropna(subset=["centroid_lat", "centroid_lon", "emission_kg_hr"])
df_cm   = df_cm.dropna(subset=["cm_lat", "cm_lon", "cm_emission_kg_hr"])

# ── Deduplicate CM ─────────────────────────────────────────────────────────
if "cm_id" in df_cm.columns:
    df_cm = df_cm.drop_duplicates(subset=["cm_id"])

# ── Basic sanity stats ──────────────────────────────────────────────────────
print(f"My plumes:    {len(df_mine)}")
print(f"CM plumes:    {len(df_cm)}")

if len(df_mine) > 0:
    print(f"My emission range: {df_mine['emission_kg_hr'].min():.0f} – {df_mine['emission_kg_hr'].max():.0f} kg/hr")

if len(df_cm) > 0:
    print(f"CM emission range: {df_cm['cm_emission_kg_hr'].min():.0f} – {df_cm['cm_emission_kg_hr'].max():.0f} kg/hr")

# ── Sanity warnings ─────────────────────────────────────────────────────────
if df_mine["centroid_lat"].isna().any():
    print("⚠ Warning: Some TROPOMI plumes have invalid lat/lon")

if df_cm["cm_lat"].isna().any():
    print("⚠ Warning: Some CM plumes have invalid lat/lon")

if len(df_mine) > 0 and df_mine["emission_kg_hr"].max() > 1e7:
    print("⚠ Warning: Extremely large emissions detected — check unit conversion")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Run matching ──────────────────────────────────────────────────────

matched, diag = run_matching_pipeline(
    df_mine=df_mine,
    df_cm=df_cm,
    cm_date_start='2025-01-01',  # reference only
    cm_date_end=(
        df_mine['scene_date'].max().strftime('%Y-%m-%d')
        if 'scene_date' in df_mine.columns and df_mine['scene_date'].notna().any()
        else None
    ),
)

# ── Safe summary ──────────────────────────────────────────────────────
n_total = len(df_mine)
n_valid = diag.n_mine if hasattr(diag, "n_mine") and diag.n_mine > 0 else n_total
n_matched = len(matched)

print(f"\nMatched pairs: {n_matched}")
print(f"Match rate:    {100 * n_matched / max(n_valid, 1):.1f}% "
      f"(based on {n_valid} valid plumes)")

# Optional quick sanity check
if n_matched == 0:
    print("⚠ No matches found — check diagnostics above for root cause.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%matplotlib inline
import matplotlib.pyplot as plt
import numpy as np

if "matched" not in dir() or len(matched) == 0:
    print("No matched data available.")
else:
    df = matched.copy()
    df = df.dropna(subset=["cm_emission_kg_hr", "my_emission_kg_hr"])
    df = df[(df["cm_emission_kg_hr"] > 0) & (df["my_emission_kg_hr"] > 0)]

    if len(df) == 0:
        print("No rows with valid emission values in both columns.")
    else:
        fig, ax = plt.subplots(figsize=(6, 6))

        ax.scatter(
            df["cm_emission_kg_hr"],
            np.ones_like(df["cm_emission_kg_hr"]),
            alpha=0.4, color="tab:orange",
            label="Carbon Mapper (x-axis projection)"
        )
        ax.scatter(
            np.ones_like(df["my_emission_kg_hr"]),
            df["my_emission_kg_hr"],
            alpha=0.4, color="tab:green",
            label="TROPOMI (y-axis projection)"
        )
        ax.scatter(
            df["cm_emission_kg_hr"],
            df["my_emission_kg_hr"],
            alpha=0.5, color="tab:blue",
            label=f"Matched pairs (n={len(df)})"
        )

        min_val = max(0.1, min(df["cm_emission_kg_hr"].min(),
                               df["my_emission_kg_hr"].min()))
        max_val = max(df["cm_emission_kg_hr"].max(),
                      df["my_emission_kg_hr"].max()) * 1.2
        ax.plot([min_val, max_val], [min_val, max_val],
                linestyle="--", color="black", label="1:1")

        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("CM emission (kg/hr)")
        ax.set_ylabel("TROPOMI emission (kg/hr)")
        ax.set_title("Emission Comparison with Source Separation")
        ax.legend()
        ax.grid(True, which="both", alpha=0.3)
        plt.tight_layout()

        plt.savefig("emission_comparison.png", dpi=150, bbox_inches="tight")
        display(fig)          # ← Synapse requires display() not plt.show()
        plt.close(fig)
        print(f"Plot saved → emission_comparison.png  |  n={len(df)} pairs plotted")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%matplotlib inline
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

if "matched" not in dir() or len(matched) == 0:
    print("No matched data available.")
else:
    df = matched.copy()
    df = df[
        (df["my_emission_kg_hr"] > 0) &
        (df["cm_emission_kg_hr"] > 0)
    ].copy()

    if len(df) == 0:
        print("No valid emission pairs after filtering.")
    else:
        df["ratio_my_cm"] = df["my_emission_kg_hr"] / df["cm_emission_kg_hr"]
        df["log_ratio"]   = np.log10(df["ratio_my_cm"])

        print("\n════════════════════════════════════════════")
        print("  EMISSION COMPARISON SUMMARY")
        print("════════════════════════════════════════════")
        print(f"Pairs analyzed: {len(df)}")
        print("\nRatio (TROPOMI / CM):")
        print(f"  Median: {df['ratio_my_cm'].median():.2f}x")
        print(f"  p25–p75: {df['ratio_my_cm'].quantile(0.25):.2f} – {df['ratio_my_cm'].quantile(0.75):.2f}")
        print(f"  p10–p90: {df['ratio_my_cm'].quantile(0.10):.2f} – {df['ratio_my_cm'].quantile(0.90):.2f}")
        print("\nLog10 ratio:")
        print(f"  Median: {df['log_ratio'].median():.2f}")
        print(f"  Std:    {df['log_ratio'].std():.2f}")

        within_2x  = ((df["ratio_my_cm"] >= 0.5) & (df["ratio_my_cm"] <= 2.0)).mean()
        within_5x  = ((df["ratio_my_cm"] >= 0.2) & (df["ratio_my_cm"] <= 5.0)).mean()
        within_10x = ((df["ratio_my_cm"] >= 0.1) & (df["ratio_my_cm"] <= 10.0)).mean()
        print("\nAgreement levels:")
        print(f"  Within 2×:  {100*within_2x:.1f}%")
        print(f"  Within 5×:  {100*within_5x:.1f}%")
        print(f"  Within 10×: {100*within_10x:.1f}%")

        # ── Scatter plot (log-log) ────────────────────────────────────────
        fig1, ax1 = plt.subplots(figsize=(6, 6))

        ax1.scatter(df["cm_emission_kg_hr"], df["my_emission_kg_hr"], alpha=0.4)

        min_val = max(0.1, min(df["cm_emission_kg_hr"].min(),
                               df["my_emission_kg_hr"].min()))
        max_val = max(df["cm_emission_kg_hr"].max(),
                      df["my_emission_kg_hr"].max()) * 1.2

        ax1.plot([min_val, max_val], [min_val, max_val],
                 linestyle="--", color="black", label="1:1")
        ax1.plot([min_val, max_val], [10*min_val, 10*max_val],
                 linestyle=":", color="grey", label="10×")
        ax1.plot([min_val, max_val], [0.1*min_val, 0.1*max_val],
                 linestyle=":", color="grey", label="0.1×")

        ax1.set_xscale("log")
        ax1.set_yscale("log")
        ax1.set_xlabel("CM emission (kg/hr)")
        ax1.set_ylabel("TROPOMI emission (kg/hr)")
        ax1.set_title("Emission Comparison (log-log)")
        ax1.legend()
        ax1.grid(True, which="both", alpha=0.3)
        plt.tight_layout()

        plt.savefig("emission_loglog.png", dpi=150, bbox_inches="tight")
        display(fig1)         # ← display() not plt.show()
        plt.close(fig1)

        # ── Histogram of log ratios ───────────────────────────────────────
        fig2, ax2 = plt.subplots(figsize=(6, 4))

        ax2.hist(df["log_ratio"], bins=40, alpha=0.7)
        ax2.axvline(0, linestyle="--", color="black", label="perfect agreement")
        ax2.set_xlabel("log10(TROPOMI / CM)")
        ax2.set_ylabel("Count")
        ax2.set_title("Distribution of Emission Ratios")
        ax2.legend()
        ax2.grid(alpha=0.3)
        plt.tight_layout()

        plt.savefig("emission_ratio_hist.png", dpi=150, bbox_inches="tight")
        display(fig2)         # ← display() not plt.show()
        plt.close(fig2)

        # ── Strict matches ────────────────────────────────────────────────
        dist_col = next((c for c in ["distance_km", "_match_dist_km"] if c in df.columns), None)
        dt_col   = next((c for c in ["time_delta_h", "_match_dt_h"]   if c in df.columns), None)

        if dist_col and dt_col:
            df_strict = df[
                (df[dist_col] < 50) &
                (df[dt_col].fillna(9999) < 240)
            ]
            if len(df_strict) > 10:
                print("\n── STRICT MATCHES (dist<50 km, Δt<240 h) ──")
                print(f"Pairs: {len(df_strict)}")
                print(f"Median ratio: {df_strict['ratio_my_cm'].median():.2f}x")
            else:
                print("\n(Too few strict matches for robust stats)")
        else:
            print(f"\n(Strict filter skipped — columns not found: dist={dist_col}, dt={dt_col})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Bias vs drivers (ROBUST PATCH) ───────────────────────────────────────

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

df = matched.copy()
df = df.replace([np.inf, -np.inf], np.nan)

# ── Core metrics ─────────────────────────────────────────────────────────
df["ratio"] = df["my_emission_kg_hr"] / df["cm_emission_kg_hr"]
df["log_ratio"] = np.log10(df["ratio"])

# ── Column auto-detection ────────────────────────────────────────────────
col_map = {}

def find_col(possible_names):
    for name in possible_names:
        if name in df.columns:
            return name
    return None

col_map["wind"] = find_col(["my_wind_speed_ms", "wind_speed_ms"])
col_map["area"] = find_col(["my_area_km2", "area_km2"])
col_map["ime"]  = find_col(["my_ime_kg", "ime_kg"])

print("\nDetected columns:")
print(col_map)

# ── Drop invalid rows (only for available cols) ──────────────────────────
required = ["ratio", "log_ratio"]
for k, v in col_map.items():
    if v is not None:
        required.append(v)

df = df.dropna(subset=required)

print(f"\nUsable rows: {len(df)}")

# ─────────────────────────────────────────────────────────────────────────
def summarize_by_bins(df, col, bins, label):
    df["bin"] = pd.cut(df[col], bins=bins)

    summary = df.groupby("bin").agg(
        count=("ratio", "count"),
        median_ratio=("ratio", "median"),
        p25=("ratio", lambda x: np.percentile(x, 25)),
        p75=("ratio", lambda x: np.percentile(x, 75)),
    ).reset_index()

    print(f"\n── Bias vs {label} ──")
    print(summary)

    plt.figure(figsize=(7,4))
    plt.plot(summary["bin"].astype(str), summary["median_ratio"], marker="o")
    plt.xticks(rotation=45)
    plt.ylabel("Median ratio (TROPOMI / CM)")
    plt.title(f"Bias vs {label}")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# ─────────────────────────────────────────────────────────────────────────
# WIND
# ─────────────────────────────────────────────────────────────────────────
if col_map["wind"] is not None:
    wind_bins = [0, 2, 4, 6, 8, 12, 20]
    summarize_by_bins(df, col_map["wind"], wind_bins, "Wind Speed (m/s)")
else:
    print("\n⚠ Wind column not found — skipping wind analysis")

# ─────────────────────────────────────────────────────────────────────────
# AREA
# ─────────────────────────────────────────────────────────────────────────
if col_map["area"] is not None:
    area_bins = np.quantile(df[col_map["area"]], [0, 0.2, 0.4, 0.6, 0.8, 1.0])
    summarize_by_bins(df, col_map["area"], area_bins, "Plume Area (km²)")
else:
    print("\n⚠ Area column not found — skipping area analysis")

# ─────────────────────────────────────────────────────────────────────────
# IME
# ─────────────────────────────────────────────────────────────────────────
if col_map["ime"] is not None:
    ime_bins = np.quantile(df[col_map["ime"]], [0, 0.2, 0.4, 0.6, 0.8, 1.0])
    summarize_by_bins(df, col_map["ime"], ime_bins, "IME (kg)")
else:
    print("\n⚠ IME column not found — skipping IME analysis")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
