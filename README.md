# Multi-Disease Dashboard Data Pipeline

## Overview

This dashboard now supports three diseases:
- COVID-19 (`covid`)
- RSV (`rsv`)  
- Influenza (`flu`)

## Data Sources

### Forecasts
- **Source**: Google Research [epi_forecasts](https://github.com/google-research/google-research/tree/master/epi_forecasts)
- **Update**: Fresh forecasts fetched on each pipeline run
- **Organization**: `data/forecasts/[disease]/[model]/[date]-[model].csv`
- **Format**: Hubverse-compatible CSV with columns:
  - reference_date, target, horizon, target_end_date
  - location, output_type, output_type_id, value

### Target/Observed Data
- **Source**: CDC official forecast hubs
  - COVID: [covid19-forecast-hub](https://github.com/CDCgov/covid19-forecast-hub)
  - RSV: [rsv-forecast-hub](https://github.com/CDCgov/rsv-forecast-hub)
  - Flu: [FluSight-forecast-hub](https://github.com/cdcepi/FluSight-forecast-hub)
- **Update**: Fetched from latest hub data each run

## Configuration Files

### Evaluation Configs (Hubverse PredEvalsData)
- `predevals-config-covid.yml`: COVID-19 evaluation targets and eval sets
- `predevals-config-rsv.yml`: RSV evaluation targets and eval sets
- `predevals-config-flu.yml`: Flu evaluation targets and eval sets

### Visualization Configs (Hubverse PredTimeChart)
- `predtimechart-config-covid.yml`: COVID forecast visualization settings
- `predtimechart-config-rsv.yml`: RSV forecast visualization settings
- `predtimechart-config-flu.yml`: Flu forecast visualization settings

Each config specifies:
- Disease-specific targets (e.g., "wk inc covid hosp", "wk inc rsv hosp", "wk inc flu hosp")
- Evaluation sets (geographic and temporal filters)
- Location code mappings (state FIPS codes)

## Data Flow Diagram

```
Google Research Repo          CDC Hubs
      |                          |
      v                          v
fetch_and_prepare_data.py
      |
      +---> data/forecasts/[disease]/[model]/*.csv
      |
      +---> data/targets/*.parquet
            |
            +---> validate_data.py (validation)
                  |
                  +---> GitHub Actions: rebuild-data.yaml
                        |
                        +---> Hubverse (R): Evaluate forecasts
                        |
                        +---> _site/ (generate website)
                              |
                              +---> GitHub Pages (deploy)
```
