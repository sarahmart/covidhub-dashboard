"""
Validate that forecast and target data are in the correct format for Hubverse.
"""

import sys
from pathlib import Path
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def validate_forecast_format(df: pd.DataFrame, disease: str) -> bool:
    """Validate forecast dataframe has required columns."""
    required_cols = [
        "reference_date",
        "target",
        "horizon",
        "target_end_date",
        "location",
        "output_type",
        "output_type_id",
        "value",
    ]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        logger.error(f"  {disease}: Missing forecast columns: {missing}")
        return False
    logger.info(f"  {disease}: ✓ Forecast format valid")
    return True


def validate_target_format(df: pd.DataFrame, disease: str) -> bool:
    """Validate target dataframe has required columns."""
    required_cols = ["date", "location", "observation", "target"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        logger.error(f"  {disease}: Missing target columns: {missing}")
        return False
    logger.info(f"  {disease}: ✓ Target format valid")
    return True


def validate_data():
    """Validate all forecast and target data."""
    all_valid = True
    diseases = ["covid", "rsv", "flu"]

    logger.info("Validating forecast files...")
    for disease in diseases:
        forecast_dir = Path("data/forecasts") / disease
        if not forecast_dir.exists():
            logger.error(f"  {disease}: Directory not found: {forecast_dir}")
            all_valid = False
            continue

        csv_files = list(forecast_dir.glob("**/*.csv"))
        if not csv_files:
            logger.error(f"  {disease}: No forecast CSV files found")
            all_valid = False
            continue

        # Check first CSV file
        df = pd.read_csv(csv_files[0])
        if not validate_forecast_format(df, disease):
            all_valid = False

    logger.info("\nValidating target data files...")
    for disease in diseases:
        target_file = Path("data/targets") / f"{disease}-targets.parquet"
        if not target_file.exists():
            logger.error(f"  {disease}: Target file not found: {target_file}")
            all_valid = False
            continue

        df = pd.read_parquet(target_file)
        if not validate_target_format(df, disease):
            all_valid = False

    if all_valid:
        logger.info("\n✓ All data validation checks passed!")
        return 0
    else:
        logger.error("\n✗ Data validation failed!")
        return 1


if __name__ == "__main__":
    sys.exit(validate_data())
