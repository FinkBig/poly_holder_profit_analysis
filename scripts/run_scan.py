#!/usr/bin/env python
"""
Run a full PNL imbalance scan on all active Polymarket markets.

Usage:
    python scripts/run_scan.py [--max-markets 100] [--threshold 0.60]
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.fetchers.market_fetcher import ActiveMarketFetcher
from src.fetchers.holder_fetcher import HolderFetcher
from src.fetchers.leaderboard_fetcher import LeaderboardFetcher
from src.analysis.imbalance_calculator import ImbalanceCalculator
from src.db.repository import ScannerRepository
from src.config.settings import (
    DEFAULT_DB_PATH,
    IMBALANCE_THRESHOLD,
    MIN_LIQUIDITY,
    MIN_HOLDER_COUNT,
    BATCH_SIZE,
    BATCH_DELAY_SECONDS
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def run_scan(
    max_markets: int = None,
    threshold: float = IMBALANCE_THRESHOLD,
    min_liquidity: float = MIN_LIQUIDITY,
    leaderboard_size: int = 2000,
):
    """Execute a full scan."""
    project_root = Path(__file__).parent.parent
    db_path = project_root / DEFAULT_DB_PATH

    repo = ScannerRepository(str(db_path))
    session_id = repo.create_session()
    logger.info(f"Started scan session {session_id}")

    scanned_count = 0
    flagged_count = 0

    try:
        # Step 1: Fetch all active markets
        logger.info("Step 1: Fetching active markets...")
        async with ActiveMarketFetcher() as market_fetcher:
            markets = await market_fetcher.fetch_all_active_markets(
                max_markets=max_markets
            )

        # Filter by liquidity
        markets = [m for m in markets if m.liquidity >= min_liquidity]
        total_markets_to_scan = len(markets)
        logger.info(
            f"Found {total_markets_to_scan} markets with liquidity >= ${min_liquidity:,.0f}"
        )

        # Update total markets count in session immediately
        repo._get_conn().execute(
            "UPDATE scan_sessions SET total_markets = ? WHERE id = ?",
            (total_markets_to_scan, session_id)
        ).connection.commit()

        if not markets:
            logger.warning("No markets to scan!")
            repo.complete_session(session_id, 0, 0)
            return

        # Step 2: Build leaderboard cache & Process Markets
        logger.info("Step 2: Building leaderboard cache...")
        
        async with LeaderboardFetcher() as lb_fetcher, HolderFetcher() as holder_fetcher:
            # Build cache first
            await lb_fetcher.build_leaderboard_cache(
                time_periods=["ALL", "MONTH"],
                max_entries=leaderboard_size,
            )
            cache_stats = lb_fetcher.get_cache_stats()
            logger.info(f"Leaderboard cache: {cache_stats}")

            logger.info("Step 3: Processing markets (Streaming)...")
            calculator = ImbalanceCalculator(threshold=threshold)

            # Process in batches
            for i in range(0, total_markets_to_scan, BATCH_SIZE):
                batch = markets[i : i + BATCH_SIZE]
                
                # Fetch holders for batch concurrently
                tasks = [
                    holder_fetcher.fetch_market_holders(
                        m.condition_id, 
                        m.token_id_yes, 
                        m.token_id_no
                    )
                    for m in batch
                ]
                
                # Execute batch fetch
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                # Process results
                for market, result in zip(batch, batch_results):
                    if isinstance(result, Exception):
                        logger.error(f"Error processing {market.market_id}: {result}")
                        continue
                        
                    yes_holders, no_holders = result

                    # Skip markets with insufficient holders
                    if (
                        len(yes_holders) < MIN_HOLDER_COUNT
                        or len(no_holders) < MIN_HOLDER_COUNT
                    ):
                        continue

                    # Enrich with PNL
                    await lb_fetcher.enrich_holders_with_pnl(yes_holders)
                    await lb_fetcher.enrich_holders_with_pnl(no_holders)

                    # Calculate imbalance
                    scan_result = calculator.create_scan_result(market, yes_holders, no_holders)

                    # Store
                    repo.upsert_market(market)
                    repo.insert_scan_result(session_id, scan_result)
                    scanned_count += 1

                    if scan_result.is_flagged:
                        flagged_count += 1
                        logger.info(
                            f"FLAGGED: {market.question[:60]}... "
                            f"({scan_result.flagged_side} side: {scan_result.profitable_skew_yes if scan_result.flagged_side == 'YES' else scan_result.profitable_skew_no:.1%} profitable)"
                        )

                # Update session progress every batch
                repo.update_session_progress(session_id, scanned_count, flagged_count)
                
                # Log progress periodically
                if (i + BATCH_SIZE) % 50 == 0:
                     logger.info(f"Progress: {scanned_count}/{total_markets_to_scan} scanned ({flagged_count} flagged)")

                # Rate limiting
                if i + BATCH_SIZE < total_markets_to_scan:
                    await asyncio.sleep(BATCH_DELAY_SECONDS)

        repo.complete_session(session_id, scanned_count, flagged_count)
        logger.info(
            f"Scan complete! Session {session_id}: "
            f"{flagged_count}/{scanned_count} markets flagged"
        )

        # Print summary
        print("\n" + "=" * 60)
        print("SCAN SUMMARY")
        print("=" * 60)
        print(f"Session ID: {session_id}")
        print(f"Total markets scanned: {scanned_count}")
        print(f"Flagged markets (>{threshold:.0%} profitable skew): {flagged_count}")
        print(f"\nTo view results, run: streamlit run dashboard.py")
        print("=" * 60)

    except Exception as e:
        logger.error(f"Scan failed: {e}")
        repo.complete_session(session_id, scanned_count, flagged_count, status="failed")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Scan Polymarket for PNL imbalances among top holders"
    )
    parser.add_argument(
        "--max-markets",
        type=int,
        default=None,
        help="Maximum markets to scan (None = all)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=IMBALANCE_THRESHOLD,
        help=f"Imbalance threshold (default: {IMBALANCE_THRESHOLD})",
    )
    parser.add_argument(
        "--min-liquidity",
        type=float,
        default=MIN_LIQUIDITY,
        help=f"Minimum market liquidity $ (default: {MIN_LIQUIDITY})",
    )
    parser.add_argument(
        "--leaderboard-size",
        type=int,
        default=2000,
        help="Number of top traders to cache from leaderboard (default: 2000)",
    )

    args = parser.parse_args()
    asyncio.run(
        run_scan(
            args.max_markets,
            args.threshold,
            args.min_liquidity,
            args.leaderboard_size,
        )
    )


if __name__ == "__main__":
    main()
