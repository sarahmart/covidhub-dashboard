"""
Fetch forecast data from Google Research epi_forecasts repository.

This module handles downloading and organizing forecast CSV files from
https://github.com/google-research/google-research/tree/master/epi_forecasts
for all three forecasting hubs (COVID-19, RSV, Flu).
"""


import os
import subprocess
from pathlib import Path
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GOOGLE_REPO_URL = "https://github.com/google-research/google-research.git"
FORECASTS_SUBDIR = "epi_forecasts"

# Hub identifiers in Google Research repo
HUB_NAMES = {
    "covid": "covid_hub",
    "rsv": "rsv_hub",
    "flu": "flu_hub",
}


class GoogleResearchFetcher:
    """Fetch forecasts from Google Research epi_forecasts repository."""

    def __init__(self, cache_dir: str = "./data/cache/google_research"):
        """
        Initialize the fetcher.

        Parameters
        ----------
        cache_dir : str
            Directory to cache the cloned repository.
        """
        self.cache_dir = Path(cache_dir)
        self.repo_dir = self.cache_dir / "google-research"
        self.epi_forecasts_dir = self.repo_dir / FORECASTS_SUBDIR

    def fetch_all(self) -> dict:
        """
        Fetch forecasts for all three diseases.

        Returns
        -------
        dict
            Dictionary mapping disease names to their forecast hub data.
        """
        # Clone or update the repository
        self._ensure_repo()

        results = {}
        for disease, hub_name in HUB_NAMES.items():
            hub_dir = self.epi_forecasts_dir / hub_name
            if hub_dir.exists():
                model_output_dir = hub_dir / "model_output"
                results[disease] = {
                    "path": hub_dir,
                    "models": self._get_models(model_output_dir) if model_output_dir.exists() else [],
                }
                logger.info(f"Found {hub_name} at {hub_dir}")
            else:
                logger.warning(f"Hub directory not found: {hub_dir}")

        return results

    def fetch_disease(self, disease: str) -> dict:
        """
        Fetch forecasts for a specific disease.

        Parameters
        ----------
        disease : str
            Disease to fetch ("covid", "rsv", "flu").

        Returns
        -------
        dict
            Dictionary with path and models for the disease.
        """
        self._ensure_repo()

        hub_name = HUB_NAMES.get(disease)
        if not hub_name:
            raise ValueError(f"Unknown disease: {disease}. Must be one of {list(HUB_NAMES.keys())}")

        hub_dir = self.epi_forecasts_dir / hub_name
        if not hub_dir.exists():
            raise FileNotFoundError(f"Hub directory not found: {hub_dir}")

        model_output_dir = hub_dir / "model_output"
        models = self._get_models(model_output_dir) if model_output_dir.exists() else []

        logger.info(f"Fetched {disease} from {hub_dir}, found {len(models)} models")
        return {
            "path": hub_dir,
            "models": models,
        }

    def _ensure_repo(self):
        """Clone or update the Google Research repository."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if self.repo_dir.exists():
            logger.info(f"Updating repository at {self.repo_dir}")
            subprocess.run(
                ["git", "-C", str(self.repo_dir), "pull"],
                check=True,
                capture_output=True,
            )
        else:
            logger.info(f"Cloning repository to {self.repo_dir}")
            # Do a shallow clone without filters
            subprocess.run(
                ["git", "clone", "--depth", "1", GOOGLE_REPO_URL, str(self.repo_dir)],
                check=True,
                capture_output=True,
            )

    def _get_models(self, model_output_dir: Path) -> list:
        """
        Get list of model subdirectories in model_output/.

        Parameters
        ----------
        model_output_dir : Path
            Path to the model_output directory.

        Returns
        -------
        list
            List of model identifiers (directory names).
        """
        if not model_output_dir.exists():
            return []

        models = [d.name for d in model_output_dir.iterdir() if d.is_dir()]
        return sorted(models)

    def get_forecast_files(self, disease: str, model: Optional[str] = None) -> list:
        """
        Get list of forecast CSV files for a disease and optional model.

        Parameters
        ----------
        disease : str
            Disease to get forecasts for ("covid", "rsv", "flu").
        model : str, optional
            Specific model directory (e.g., "Google_SAI-Adapted_12").
            If None, returns all forecast files across all models.

        Returns
        -------
        list
            List of Path objects for forecast CSV files.
        """
        disease_data = self.fetch_disease(disease)
        hub_dir = disease_data["path"]
        models = disease_data["models"]

        if not models:
            logger.warning(f"No models found for {disease}")
            return []

        forecast_files = []

        if model is not None:
            # Get files for a specific model
            if model not in models:
                raise ValueError(f"Model {model} not found. Available: {models}")
            model_dir = hub_dir / "model_output" / model
            files = sorted(model_dir.glob("*.csv"))
            forecast_files.extend(files)
        else:
            # Get files for all models
            for m in models:
                model_dir = hub_dir / "model_output" / m
                files = sorted(model_dir.glob("*.csv"))
                forecast_files.extend(files)

        logger.info(f"Found {len(forecast_files)} forecast files for {disease}")
        return forecast_files


if __name__ == "__main__":
    # Example usage
    fetcher = GoogleResearchFetcher()

    # Fetch all diseases
    all_data = fetcher.fetch_all()
    print("All diseases:", all_data.keys())

    # Fetch specific disease
    covid_data = fetcher.fetch_disease("covid")
    print(f"COVID models: {covid_data['models'][:3]}...")

    # Get forecast files
    covid_files = fetcher.get_forecast_files("covid")
    print(f"COVID forecast files: {len(covid_files)}")
    for f in covid_files[:5]:
        print(f"  - {f.name}")

    # Get forecast files for specific model
    if covid_data["models"]:
        model_files = fetcher.get_forecast_files("covid", model=covid_data["models"][0])
        print(f"Files for model {covid_data['models'][0]}: {len(model_files)}")

