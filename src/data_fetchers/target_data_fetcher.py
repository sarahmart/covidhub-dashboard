"""
Fetch target/observed data from CDC forecast hubs.

This module downloads observed values (ground truth) for COVID-19, RSV, and Flu
from the respective CDC hub repositories in standard hubverse format.
"""

import os
import subprocess
from pathlib import Path
from typing import Optional
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Hub repositories and their target data locations
HUB_TARGETS = {
    "covid": {
        "repo_url": "https://github.com/CDCgov/covid19-forecast-hub.git",
        "target_path": "target-data",
        "data_file": "time-series.parquet",
    },
    "rsv": {
        "repo_url": "https://github.com/CDCgov/rsv-forecast-hub.git",
        "target_path": "target-data",
        "data_file": "time-series.parquet",
    },
    "flu": {
        "repo_url": "https://github.com/cdcepi/FluSight-forecast-hub.git",
        "target_path": "target-data",
        "data_file": "time-series.csv", # or target-hospital-admissions.csv ? 
    },
}


class TargetDataFetcher:
    """Fetch observed/target data from CDC forecast hub repositories."""

    def __init__(self, cache_dir: str = "./data/cache/targets"):
        """
        Initialize the target data fetcher.

        Parameters
        ----------
        cache_dir : str
            Directory to cache cloned hub repositories.
        """
        self.cache_dir = Path(cache_dir)
        self.repos = {
            disease: self.cache_dir / f"{disease}-hub"
            for disease in HUB_TARGETS.keys()
        }

    def fetch_all(self) -> dict:
        """
        Fetch target data for all diseases.

        Returns
        -------
        dict
            Dictionary mapping disease names to their target data DataFrames.
        """
        results = {}
        for disease in HUB_TARGETS.keys():
            try:
                df = self.fetch_disease(disease)
                results[disease] = df
                logger.info(f"Fetched target data for {disease}: {len(df)} records")
            except Exception as e:
                logger.warning(f"Failed to fetch {disease} target data: {e}")
        return results

    def fetch_disease(self, disease: str) -> pd.DataFrame:
        """
        Fetch target data for a specific disease.

        Parameters
        ----------
        disease : str
            Disease to fetch ("covid", "rsv", "flu").

        Returns
        -------
        pd.DataFrame
            Target data with columns: date, location, observation, as_of, target
        """
        if disease not in HUB_TARGETS:
            raise ValueError(
                f"Unknown disease: {disease}. Must be one of {list(HUB_TARGETS.keys())}"
            )

        hub_config = HUB_TARGETS[disease]
        repo_dir = self.repos[disease]

        # Ensure the repository is available
        self._ensure_repo(disease, hub_config)

        # Load the data file (csv [old, only for flu and they will likely soon move away] or parquet)
        data_path = repo_dir / hub_config["target_path"] / hub_config["data_file"]

        if not data_path.exists():
            raise FileNotFoundError(f"Target data file not found: {data_path}")

        logger.info(f"Loading target data from {data_path}")
        
        # Read file based on extension
        if data_path.suffix.lower() == ".csv":
            df = pd.read_csv(data_path)
        elif data_path.suffix.lower() == ".parquet":
            df = pd.read_parquet(data_path)
        else:
            raise ValueError(f"Unsupported file format: {data_path.suffix}")

        # Standardize column names and types
        df = self._standardize_data(df, disease)

        return df

    def _ensure_repo(self, disease: str, hub_config: dict):
        """Clone or update the hub repository."""
        repo_dir = self.repos[disease]
        repo_url = hub_config["repo_url"]

        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if repo_dir.exists():
            logger.info(f"Updating {disease} hub repository")
            subprocess.run(
                ["git", "-C", str(repo_dir), "pull"],
                check=True,
                capture_output=True,
            )
        else:
            logger.info(f"Cloning {disease} hub repository")
            subprocess.run(
                ["git", "clone", "--depth", "1", repo_url, str(repo_dir)],
                check=True,
                capture_output=True,
            )

    def _standardize_data(self, df: pd.DataFrame, disease: str) -> pd.DataFrame:
        """
        Standardize target data format across hubs.

        Parameters
        ----------
        df : pd.DataFrame
            Raw data from hub.
        disease : str
            Disease identifier for context.

        Returns
        -------
        pd.DataFrame
            Standardized dataframe with required columns.
        """
        # Map column names by disease (handle different hub formats)
        column_mapping = {
            "covid": {
                "date": "date",
                "location": "location",
                "observation": "observation",
                "target": "target",
            },
            "rsv": {
                "date": "date",
                "location": "location",
                "observation": "observation",
                "target": "target",
            },
            "flu": {
                "date": "target_end_date",  # Flu uses target_end_date (week ending date)
                "location": "location",
                "observation": "observation",  # Flu also uses observation
                "target": "target",
            },
        }
        
        mapping = column_mapping.get(disease)
        if not mapping:
            raise ValueError(f"Unknown disease: {disease}")
        
        # Rename columns to standard names
        rename_dict = {}
        for standard_col, actual_col in mapping.items():
            if actual_col in df.columns:
                rename_dict[actual_col] = standard_col
        
        df = df.rename(columns=rename_dict)
        
        # Check required columns exist
        required_cols = ["date", "location", "observation", "target"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(
                f"Missing required columns {missing} in {disease} target data. "
                f"Available columns: {df.columns.tolist()}"
            )

        # Convert date to datetime
        if df["date"].dtype != "datetime64[ns]":
            df["date"] = pd.to_datetime(df["date"])

        # Add as_of column if not present (assume current date)
        if "as_of" not in df.columns:
            df["as_of"] = pd.Timestamp.now()

        # Select and reorder columns
        cols_to_keep = ["date", "location", "observation", "as_of", "target"]
        df = df[cols_to_keep].copy()

        # Remove any null observations
        df = df.dropna(subset=["observation"])

        return df

    def get_latest_value(
        self,
        disease: str,
        location: str,
        target: Optional[str] = None,
        before_date: Optional[pd.Timestamp] = None,
    ) -> Optional[float]:
        """
        Get the latest observed value for a location and timepoint.

        Parameters
        ----------
        disease : str
            Disease identifier.
        location : str
            Location code.
        target : str, optional
            Target type (e.g., "wk inc covid hosp").
        before_date : pd.Timestamp, optional
            Only get observations before this date.

        Returns
        -------
        float or None
            Observed value, or None if not found.
        """
        df = self.fetch_disease(disease)

        # Filter by location
        df = df[df["location"] == location]

        # Filter by target if specified
        if target is not None:
            df = df[df["target"] == target]

        # Filter by date if specified
        if before_date is not None:
            df = df[df["date"] <= before_date]

        if df.empty:
            return None

        # Return the most recent observation
        return df.sort_values("date").iloc[-1]["observation"]


if __name__ == "__main__":
    # Example usage
    fetcher = TargetDataFetcher()

    # Fetch all diseases
    all_data = fetcher.fetch_all()
    for disease, df in all_data.items():
        print(f"\n{disease.upper()}:")
        print(f"  Rows: {len(df)}")
        print(f"  Targets: {df['target'].unique().tolist()}")
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
