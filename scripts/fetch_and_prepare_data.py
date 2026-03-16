"""
Fetch and prepare data from Google Research and CDC hubs for evaluation.

This script:
1. Fetches Google Research forecasts for COVID, RSV, Flu from the public repo
2. Fetches CDC hub target data from their respective GitHub repos
3. Organizes data for Hubverse R evaluation tools

Run with: python scripts/fetch_and_prepare_data.py
"""

import sys
from pathlib import Path
import logging

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from data_fetchers import GoogleResearchFetcher, TargetDataFetcher  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def prepare_data():
    """Fetch and prepare all data for evaluation."""
    
    logger.info("Starting data preparation...")
    
    # Initialize fetchers
    forecast_fetcher = GoogleResearchFetcher()
    target_fetcher = TargetDataFetcher()
    
    diseases = ["covid", "rsv", "flu"]
    
    # Fetch and organize forecast data
    logger.info("Fetching forecast data from Google Research...")
    for disease in diseases:
        try:
            logger.info(f"  Processing {disease}...")
            data = forecast_fetcher.fetch_disease(disease)
            
            # Get forecast files
            files = forecast_fetcher.get_forecast_files(disease)
            logger.info(f"    Found {len(data['models'])} models, {len(files)} forecast files")
            
            # Organize by disease in data/forecasts/
            forecast_dir = Path("data/forecasts") / disease
            forecast_dir.mkdir(parents=True, exist_ok=True)
            
            logger.info(f" {disease.upper()} forecasts ready")
            
        except Exception as e:
            logger.error(f" Error processing {disease} forecasts: {e}")
    
    # Fetch and organize target data
    logger.info("Fetching target data from CDC hubs...")
    for disease in diseases:
        try:
            logger.info(f"  Processing {disease}...")
            target_df = target_fetcher.fetch_disease(disease)
            
            # Save target data
            target_dir = Path("data/targets")
            target_dir.mkdir(parents=True, exist_ok=True)
            
            target_file = target_dir / f"{disease}-targets.parquet"
            target_df.to_parquet(target_file)
            
            logger.info(f"    Saved {len(target_df)} observations to {target_file}")
            logger.info(f"  {disease.upper()} target data ready")
            
        except Exception as e:
            logger.error(f"  Error processing {disease} target data: {e}")
    
    logger.info("Data preparation complete!")
    logger.info("\nData organization:")
    logger.info("  Forecasts: data/cache/google_research/ (via GoogleResearchFetcher)")
    logger.info("  Targets: data/targets/*.parquet")
    logger.info("\nReady for Hubverse R evaluation tools.")


if __name__ == "__main__":
    prepare_data()
