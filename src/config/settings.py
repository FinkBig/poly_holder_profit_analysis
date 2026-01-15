"""Configuration settings for Top Holders PNL Imbalance Scanner."""

# API Endpoints
GAMMA_API_URL = "https://gamma-api.polymarket.com"
DATA_API_URL = "https://data-api.polymarket.com"

# Scanner thresholds
IMBALANCE_THRESHOLD = 0.60  # 60% profitable skew triggers flag
TOP_HOLDER_COUNT = 50  # Top 50 holders to analyze (API may cap lower)
TOP_HOLDER_PERCENT = 0.50  # Top 50% by share count

# Rate limiting
REQUEST_DELAY_SECONDS = 0.1
BATCH_SIZE = 5  # Parallel holder requests per batch
BATCH_DELAY_SECONDS = 0.5
LEADERBOARD_BATCH_SIZE = 50  # Max per leaderboard request

# Leaderboard time periods
LEADERBOARD_PERIODS = ["ALL", "MONTH"]  # Overall and 30-day

# Database
DEFAULT_DB_PATH = "data/scanner.db"

# Market filters
MIN_LIQUIDITY = 1000  # Skip low-liquidity markets ($)
MIN_HOLDER_COUNT = 5  # Need at least N holders per side to analyze
