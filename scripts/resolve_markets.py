#!/usr/bin/env python3
"""
Script to check resolved markets and update backtest snapshots.

Uses bulk fetching from Gamma API (similar to poly_data approach) for efficiency.
Fetches closed markets in batches instead of individual API calls.

Run periodically (e.g., daily) to update accuracy metrics.
"""

import asyncio
import aiohttp
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.repository import ScannerRepository
from src.config.settings import DEFAULT_DB_PATH, GAMMA_API_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Batch size for bulk fetching (poly_data uses 500)
BATCH_SIZE = 500
REQUEST_DELAY = 0.2


async def fetch_closed_markets_batch(
    session: aiohttp.ClientSession,
    offset: int = 0,
    limit: int = BATCH_SIZE
) -> List[Dict]:
    """Fetch a batch of closed markets from Gamma API, ordered by newest first."""
    params = {
        "closed": "true",
        "limit": limit,
        "offset": offset,
        "order": "id",
        "ascending": "false",  # Newest first
    }

    try:
        async with session.get(f"{GAMMA_API_URL}/markets", params=params) as response:
            if response.status == 200:
                return await response.json()
            elif response.status == 429:
                # Rate limited, wait and retry
                logger.warning("Rate limited, waiting 5 seconds...")
                await asyncio.sleep(5)
                return await fetch_closed_markets_batch(session, offset, limit)
            else:
                logger.warning(f"Markets API returned {response.status}")
                return []
    except Exception as e:
        logger.error(f"Error fetching closed markets: {e}")
        return []


async def fetch_recent_closed_markets(
    session: aiohttp.ClientSession,
    target_ids: set,
    max_pages: int = 20
) -> Dict[str, Dict]:
    """
    Fetch recent closed markets until we find all target IDs or hit max pages.

    Args:
        session: aiohttp session
        target_ids: Set of market IDs we're looking for
        max_pages: Maximum pages to fetch (default 20 = 10,000 markets)

    Returns: Dict mapping market_id -> market data
    """
    closed_markets = {}
    offset = 0
    found_count = 0

    # Find min ID we're looking for to know when to stop
    min_target_id = min(int(mid) for mid in target_ids if mid.isdigit()) if target_ids else 0

    logger.info(f"Fetching recent closed markets (looking for {len(target_ids)} markets, min_id={min_target_id})...")

    for page in range(max_pages):
        batch = await fetch_closed_markets_batch(session, offset=offset)

        if not batch:
            break

        batch_min_id = float('inf')
        for market in batch:
            market_id = str(market.get("id", ""))
            if market_id:
                closed_markets[market_id] = market
                if market_id in target_ids:
                    found_count += 1
                try:
                    batch_min_id = min(batch_min_id, int(market_id))
                except ValueError:
                    pass

        logger.info(f"Page {page+1}: fetched {len(batch)} markets, found {found_count}/{len(target_ids)} targets")

        # Stop early if we found all targets
        if found_count >= len(target_ids):
            logger.info("Found all target markets!")
            break

        # Stop if we've gone past all target IDs (oldest batch ID < min target ID)
        if batch_min_id < min_target_id:
            logger.info(f"Reached markets older than our targets (batch_min={batch_min_id} < min_target={min_target_id})")
            break

        if len(batch) < BATCH_SIZE:
            # Last page
            break

        offset += BATCH_SIZE
        await asyncio.sleep(REQUEST_DELAY)

    logger.info(f"Total: fetched {len(closed_markets)} markets, found {found_count} targets")
    return closed_markets


async def fetch_market_by_id(session: aiohttp.ClientSession, market_id: str) -> Optional[Dict]:
    """Fallback: fetch individual market if not found in bulk."""
    try:
        async with session.get(f"{GAMMA_API_URL}/markets/{market_id}") as response:
            if response.status == 200:
                return await response.json()
            return None
    except Exception as e:
        logger.error(f"Error fetching market {market_id}: {e}")
        return None


def parse_resolution(market_data: Dict) -> Tuple[Optional[str], Optional[int]]:
    """
    Parse market data to determine resolution status.

    Uses multiple indicators (similar to poly_data):
    - closedTime: timestamp when market closed
    - closed: boolean flag
    - resolved: boolean flag
    - winningOutcome: the winning side
    - outcomePrices: final prices (winner = 1.0)

    Returns: (resolved_outcome, resolved_at) or (None, None) if not resolved
    """
    # Check if market is closed/resolved
    closed = market_data.get("closed", False)
    resolved = market_data.get("resolved", False)
    closed_time = market_data.get("closedTime")

    # If not closed at all, skip
    if not (closed or resolved or closed_time):
        return None, None

    # Get winning outcome - try multiple fields
    winning_outcome = market_data.get("winningOutcome")

    if winning_outcome is None:
        # Try to determine from outcome prices
        outcome_prices = market_data.get("outcomePrices", [])
        if isinstance(outcome_prices, str):
            try:
                outcome_prices = json.loads(outcome_prices)
            except json.JSONDecodeError:
                outcome_prices = []

        if len(outcome_prices) >= 2:
            try:
                yes_price = float(outcome_prices[0])
                no_price = float(outcome_prices[1])

                # If resolved, one price should be 1.0 (or very close)
                if yes_price > 0.95:
                    winning_outcome = "Yes"
                elif no_price > 0.95:
                    winning_outcome = "No"
            except (ValueError, TypeError):
                pass

    if winning_outcome is None:
        # Market is closed but outcome not determined yet
        return None, None

    # Normalize outcome to YES/NO
    if isinstance(winning_outcome, str):
        resolved_outcome = "YES" if winning_outcome.lower() in ["yes", "true", "1"] else "NO"
    else:
        resolved_outcome = "YES" if winning_outcome else "NO"

    # Get resolution timestamp - prefer closedTime (poly_data approach)
    resolved_at = closed_time or market_data.get("resolvedAt") or market_data.get("closedAt")

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
    """Main function to check and update market resolutions using bulk fetching."""
    # Initialize repository
    project_root = Path(__file__).parent.parent
    db_path = project_root / DEFAULT_DB_PATH
    repo = ScannerRepository(str(db_path))

    # Get unresolved flagged markets from our database
    unresolved = repo.get_unresolved_flagged_markets()

    if not unresolved:
        logger.info("No unresolved markets to check")
        return

    logger.info(f"Have {len(unresolved)} unresolved markets to check")

    # Build set of market IDs we need to check
    unresolved_ids = {m["market_id"] for m in unresolved}
    unresolved_map = {m["market_id"]: m for m in unresolved}

    resolved_count = 0

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        # Step 1: Bulk fetch recent closed markets (newest first, stop when we find all targets)
        closed_markets = await fetch_recent_closed_markets(session, unresolved_ids)

        # Step 2: Check our unresolved markets against closed markets
        found_in_bulk = 0
        not_found = []

        for market_id in unresolved_ids:
            if market_id in closed_markets:
                found_in_bulk += 1
                market_data = closed_markets[market_id]

                resolved_outcome, resolved_at = parse_resolution(market_data)

                if resolved_outcome:
                    repo.update_backtest_resolution(
                        market_id=market_id,
                        resolved_outcome=resolved_outcome,
                        resolved_at=resolved_at,
                    )
                    resolved_count += 1
                    question = unresolved_map[market_id].get("question", "")[:50]
                    logger.info(f"Resolved: {question}... -> {resolved_outcome}")
            else:
                not_found.append(market_id)

        logger.info(f"Found {found_in_bulk} markets in bulk fetch, {len(not_found)} not found")

        # Step 3: Fallback - fetch individual markets not found in bulk
        # (These might be recently closed or have different status)
        if not_found:
            logger.info(f"Checking {len(not_found)} markets individually...")

            for market_id in not_found:
                market_data = await fetch_market_by_id(session, market_id)

                if market_data:
                    resolved_outcome, resolved_at = parse_resolution(market_data)

                    if resolved_outcome:
                        repo.update_backtest_resolution(
                            market_id=market_id,
                            resolved_outcome=resolved_outcome,
                            resolved_at=resolved_at,
                        )
                        resolved_count += 1
                        question = unresolved_map[market_id].get("question", "")[:50]
                        logger.info(f"Resolved (fallback): {question}... -> {resolved_outcome}")

                await asyncio.sleep(0.1)

    logger.info(f"\nUpdated {resolved_count} market resolutions")

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
