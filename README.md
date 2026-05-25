# Methane Emissions Detection — Varon et al. Approach
**Greensky | iLink Digital — Data & AI Practice**

> **Version:** 0.1.0 (Scaffold)
> **Maintainer:** Data & AI Engineering Team
> **Classification:** Internal / Partner Confidential
> **Branch:** `dev`

---

## What This Is

A satellite-based methane (CH₄) plume detection and quantification pipeline implementing the **Varon et al.** retrieval approach, built on Microsoft's **Planetary Computer** STAC catalog. The pipeline ingests Sentinel-2 imagery, detects methane plume signatures, estimates emission rates, and surfaces results through a real-time RTI dashboard focused on the **Permian Basin**.

This accelerator is part of the broader Greensky Methane Emissions workstream and is designed to run as a scheduled daily pipeline within Microsoft Fabric.

---

## Repository Structure

```
Methane Emissions/Accelerator/Planetary Computer/Varon et al Approach/
├── Ch4 Plume Monitoring - Permian Basin/   # RTI dashboard for CH₄ plume visualisation
├── emissions_pipeline.DataPipeline/        # Fabric Data Pipeline (scheduled daily @ 9 AM IST)
├── lakehouses/Planetary_computer_LH.L.../  # Lakehouse configuration for Planetary Computer
├── notebooks/                              # Jupyter notebooks used in the pipeline
│   └── methane/                            # Core methane detection & quantification notebooks
└── planetary_computer.Environment/        # Custom Python environment & dependencies
```

---

## Key Components

### `notebooks/methane/`
Jupyter notebooks that implement each stage of the Varon et al. retrieval pipeline:
- Planetary Computer STAC queries for scene discovery
- Methane enhancement computation
- Plume detection and masking
- Emission rate quantification (Q, kg/hr)
- Output writing to Lakehouse

### `emissions_pipeline.DataPipeline`
A Microsoft Fabric Data Pipeline that orchestrates the notebooks end-to-end. Scheduled to trigger once daily at **9:00 AM IST**.

### `Ch4 Plume Monitoring - Permian Basin`
An RTI (Real-Time Intelligence) dashboard providing:
- Spatial visualisation of detected CH₄ plumes over the Permian Basin
- Time-series emission rate trends
- Alert thresholds for anomalous emission events

### `planetary_computer.Environment`
A custom Fabric environment containing all Python dependencies required for Planetary Computer API access, geospatial processing, and methane retrieval calculations. Attach this environment to notebooks and pipeline activities before running.

### `lakehouses/Planetary_computer_LH`
Lakehouse used for storing intermediate and final outputs (scene metadata, enhancement rasters, plume polygons, emission estimates).

---

## Prerequisites

- Access to **Microsoft Fabric** workspace (with Lakehouse, Data Pipeline, and Environment capabilities)
- **Planetary Computer** account / SAS token (or anonymous access for public datasets)
- The `planetary_computer.Environment` attached to all notebooks and pipeline activities
- Appropriate permissions to the `greensky` repo (`dashboard` branch)

---

## Getting Started

### 1. Clone / sync the repository
```bash
git clone https://github.com/iLinkDigital-DATA-IPs/greensky.git
cd "greensky/Methane Emissions/Accelerator/Planetary Computer/Varon et al Approach"
```

### 2. Attach the custom environment
In Microsoft Fabric, attach `planetary_computer.Environment` to:
- All notebooks under `notebooks/methane/`
- The `emissions_pipeline.DataPipeline` activities

### 3. Configure the Lakehouse
Ensure `Planetary_computer_LH` is mounted and the correct paths are set in the notebook parameters.

### 4. Run manually (ad-hoc)
Open any notebook in `notebooks/methane/` and run cells sequentially, or trigger the full pipeline via `emissions_pipeline.DataPipeline`.

### 5. Scheduled run
The pipeline is pre-configured to run daily at **9:00 AM IST**. Verify the schedule is active in the Fabric Data Pipeline settings.

---

## Methodology Reference

This pipeline implements the retrieval approach described in:

> **Varon et al. (2018)** — *Quantifying methane point sources from fine-scale satellite observations of atmospheric methane plumes.* Atmospheric Measurement Techniques.

Key steps from the paper implemented here:
1. Multi-band ratio technique using SWIR channels to derive methane enhancement (Δω)
2. Integrated Mass Enhancement (IME) method for emission rate (Q) estimation
3. Wind-field integration using ERA5 reanalysis data

---


## Contributing

1. Branch off `dev` for feature work
2. Raise a PR back to `dev`; `dashboard` branch is for deployed/production state
3. Tag the Data & AI Engineering Team for review

---

## Contact

**iLink Digital — Data & AI Practice**
Internal repository. For access or questions, contact the Data & AI Engineering Team.
