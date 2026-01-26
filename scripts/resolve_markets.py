#!/usr/bin/env python3
"""
Script to check resolved markets and update backtest snapshots.

Fetches market resolution status from Gamma API and updates
the backtest_snapshots table with actual outcomes.

Run periodically (e.g., daily) to update accuracy metrics.
"""

import asyncio
import aiohttp
import logging
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.repository import ScannerRepository
from src.config.settings import DEFAULT_DB_PATH, GAMMA_API_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def fetch_market_status(session: aiohttp.ClientSession, market_id: str) -> dict:
    """Fetch market status from Gamma API."""
    try:
        async with session.get(f"{GAMMA_API_URL}/markets/{market_id}") as response:
            if response.status == 200:
                return await response.json()
            else:
                logger.warning(f"Failed to fetch market {market_id}: {response.status}")
                return {}
    except Exception as e:
        logger.error(f"Error fetching market {market_id}: {e}")
        return {}


def parse_resolution(market_data: dict) -> tuple:
    """
    Parse market data to determine resolution status.

    Returns: (resolved_outcome, resolved_at) or (None, None) if not resolved
    """
    # Check various resolution indicators
    closed = market_data.get("closed", False)
    resolved = market_data.get("resolved", False)

    if not (closed or resolved):
        return None, None

    # Get winning outcome
    winning_outcome = market_data.get("winningOutcome")

    if winning_outcome is None:
        # Try to determine from outcome prices
        outcome_prices = market_data.get("outcomePrices", [])
        if isinstance(outcome_prices, str):
            import json
            outcome_prices = json.loads(outcome_prices)

        if len(outcome_prices) >= 2:
            yes_price = float(outcome_prices[0])
            no_price = float(outcome_prices[1])

            # If resolved, one price should be 1.0 (or very close)
            if yes_price > 0.95:
                winning_outcome = "Yes"
            elif no_price > 0.95:
                winning_outcome = "No"

    if winning_outcome is None:
        return None, None

    # Normalize outcome to YES/NO
    resolved_outcome = "YES" if winning_outcome.lower() in ["yes", "true", "1"] else "NO"

    # Get resolution timestamp
    resolved_at = market_data.get("resolvedAt") or market_data.get("closedAt")
    if resolved_at:
        try:
            if isinstance(resolved_at, str):
                resolved_at = resolved_at.replace("Z", "+00:00")
                dt = datetime.fromisoformat(resolved_at)
                resolved_at = int(dt.timestamp())
            else:
                resolved_at = int(resolved_at)
        except Exception:
            resolved_at = int(datetime.now().timestamp())
    else:
        resolved_at = int(datetime.now().timestamp())

    return resolved_outcome, resolved_at


async def main():
    """Main function to check and update market resolutions."""
    # Initialize repository
    project_root = Path(__file__).parent.parent
    db_path = project_root / DEFAULT_DB_PATH
    repo = ScannerRepository(str(db_path))

    # Get unresolved flagged markets
    unresolved = repo.get_unresolved_flagged_markets()

    if not unresolved:
        logger.info("No unresolved markets to check")
        return

    logger.info(f"Checking {len(unresolved)} unresolved markets...")

    resolved_count = 0

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        for market in unresolved:
            market_id = market["market_id"]

            # Fetch market status
            market_data = await fetch_market_status(session, market_id)

            if not market_data:
                continue

            # Parse resolution
            resolved_outcome, resolved_at = parse_resolution(market_data)

            if resolved_outcome:
                # Update backtest snapshot
                repo.update_backtest_resolution(
                    market_id=market_id,
                    resolved_outcome=resolved_outcome,
                    resolved_at=resolved_at,
                )
                resolved_count += 1
                logger.info(f"Resolved: {market['question'][:50]}... -> {resolved_outcome}")

            # Rate limiting
            await asyncio.sleep(0.1)

    logger.info(f"Updated {resolved_count} market resolutions")

    # Print summary stats
    stats = repo.get_backtest_stats()
    logger.info(f"\n=== Backtest Statistics ===")
    logger.info(f"Total flagged: {stats['total_flagged']}")
    logger.info(f"Resolved: {stats['resolved']} ({stats['pending']} pending)")
    logger.info(f"Correct: {stats['correct']} / {stats['resolved']}")
    logger.info(f"Accuracy: {stats['accuracy']:.1%}")
    logger.info(f"Theoretical PNL: ${stats['total_pnl']:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
