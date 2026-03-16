"""Data fetchers for forecast data and target observations from various sources."""

from .google_research_fetcher import GoogleResearchFetcher
from .target_data_fetcher import TargetDataFetcher

__all__ = ["GoogleResearchFetcher", "TargetDataFetcher"]
