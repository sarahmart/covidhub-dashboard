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
import shutil

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from data_fetchers import GoogleResearchFetcher, TargetDataFetcher  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def copy_forecast_files(disease: str, source_base: Path, dest_base: Path) -> int:
    """Copy forecast files from source to destination, organized by disease and model.
    
    Args:
        disease: Disease name (covid, rsv, flu)
        source_base: Source base path (data/cache/google_research/google-research/epi_forecasts/)
        dest_base: Destination base path (data/forecasts/)
    
    Returns:
        Number of files copied
    """
    source_dir = source_base / f"{disease}_hub/model_output"
    dest_dir = dest_base / disease
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    files_copied = 0
    
    if source_dir.exists():
        # Copy all CSV files from each model directory
        for model_dir in source_dir.iterdir():
            if model_dir.is_dir():
                model_name = model_dir.name
                model_dest = dest_dir / model_name
                model_dest.mkdir(parents=True, exist_ok=True)
                
                for csv_file in model_dir.glob("*.csv"):
                    dest_file = model_dest / csv_file.name
                    shutil.copy2(csv_file, dest_file)
                    files_copied += 1
    
    return files_copied


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
            
            # Get forecast files and copy them
            source_base = Path("data/cache/google_research/google-research/epi_forecasts")
            dest_base = Path("data/forecasts")
            
            num_files = copy_forecast_files(disease, source_base, dest_base)
            
            logger.info(f"    Found {len(data['models'])} models, copied {num_files} forecast files")
            logger.info(f"  {disease.upper()} forecasts ready")
            
        except Exception as e:
            logger.error(f"  Error processing {disease} forecasts: {e}")
            import traceback
            traceback.print_exc()
    
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
            import traceback
            traceback.print_exc()
    
    logger.info("Data preparation complete!")
    logger.info("\nData organization:")
    logger.info("  Forecasts: data/forecasts/[disease]/[model]/ (organized by disease and model)")
    logger.info("  Targets: data/targets/*.parquet")
    logger.info("\nReady for Hubverse R evaluation tools.")


if __name__ == "__main__":
    prepare_data()
