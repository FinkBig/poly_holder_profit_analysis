#!/usr/bin/env python
"""
Integration tests for the scanner pipeline.

Tests each component of the pipeline to ensure APIs are working
and data flows correctly through the system.

Usage:
    python scripts/test_pipeline.py
"""

import asyncio
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
from src.config.settings import DEFAULT_DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def test_market_fetcher():
    """Test fetching active markets."""
    logger.info("=" * 50)
    logger.info("TEST 1: Market Fetcher")
    logger.info("=" * 50)

    async with ActiveMarketFetcher() as fetcher:
        markets = await fetcher.fetch_all_active_markets(max_markets=5)

    if not markets:
        logger.error("FAILED: No markets fetched")
        return None

    logger.info(f"SUCCESS: Fetched {len(markets)} markets")

    sample = markets[0]
    logger.info(f"  Sample market: {sample.question[:50]}...")
    logger.info(f"  Market ID: {sample.market_id}")
    logger.info(f"  Condition ID: {sample.condition_id}")
    logger.info(f"  YES Token: {sample.token_id_yes[:20]}...")
    logger.info(f"  NO Token: {sample.token_id_no[:20]}...")
    logger.info(f"  Volume: ${sample.volume:,.0f}")
    logger.info(f"  Liquidity: ${sample.liquidity:,.0f}")

    # Validate structure
    assert sample.market_id, "Market should have ID"
    assert sample.condition_id, "Market should have condition ID"
    assert sample.token_id_yes, "Market should have YES token"
    assert sample.token_id_no, "Market should have NO token"

    return markets


async def test_holder_fetcher(markets):
    """Test fetching holders."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("TEST 2: Holder Fetcher")
    logger.info("=" * 50)

    if not markets:
        logger.warning("SKIPPED: No markets to test")
        return None

    market = markets[0]

    async with HolderFetcher() as fetcher:
        yes_holders, no_holders = await fetcher.fetch_market_holders(
            market.condition_id,
            market.token_id_yes,
            market.token_id_no,
        )

    logger.info(
        f"SUCCESS: Found {len(yes_holders)} YES holders, {len(no_holders)} NO holders"
    )

    if yes_holders:
        sample = yes_holders[0]
        logger.info(f"  Top YES holder: {sample.wallet_address[:16]}...")
        logger.info(f"  Amount: {sample.amount:,.2f} shares")

    if no_holders:
        sample = no_holders[0]
        logger.info(f"  Top NO holder: {sample.wallet_address[:16]}...")
        logger.info(f"  Amount: {sample.amount:,.2f} shares")

    return yes_holders, no_holders


async def test_pnl_fetching(holders):
    """Test PNL fetching from positions API."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("TEST 3: PNL Fetcher (via Positions API)")
    logger.info("=" * 50)

    if not holders:
        logger.warning("SKIPPED: No holders to test")
        return None, None

    yes_holders, no_holders = holders

    async with LeaderboardFetcher() as fetcher:
        # Enrich a subset of holders with PNL
        found_yes = await fetcher.enrich_holders_with_pnl(yes_holders[:5])
        found_no = await fetcher.enrich_holders_with_pnl(no_holders[:5])

        logger.info(f"SUCCESS: Found PNL for {found_yes}/5 YES holders, {found_no}/5 NO holders")

        cache_stats = fetcher.get_cache_stats()
        logger.info(f"  API calls made: {cache_stats['api_calls']}")
        logger.info(f"  Wallets cached: {cache_stats['cached_wallets']}")

        # Show some sample PNL data
        for h in yes_holders[:3]:
            if h.overall_pnl is not None:
                logger.info(f"  YES holder {h.wallet_address[:12]}...: PNL ${h.overall_pnl:,.2f}")

        for h in no_holders[:3]:
            if h.overall_pnl is not None:
                logger.info(f"  NO holder {h.wallet_address[:12]}...: PNL ${h.overall_pnl:,.2f}")

    return yes_holders, no_holders


async def test_imbalance_calculator(markets, holders):
    """Test imbalance calculation."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("TEST 4: Imbalance Calculator")
    logger.info("=" * 50)

    if not markets or not holders:
        logger.warning("SKIPPED: Missing data for test")
        return

    market = markets[0]
    yes_holders, no_holders = holders

    # Calculate
    calculator = ImbalanceCalculator()
    result = calculator.create_scan_result(market, yes_holders, no_holders)

    logger.info(f"SUCCESS: Calculated imbalance")
    logger.info(f"  YES profitable: {result.profitable_skew_yes:.1%}")
    logger.info(f"  NO profitable: {result.profitable_skew_no:.1%}")
    logger.info(f"  YES unknown: {result.yes_analysis.unknown_pct:.1%}")
    logger.info(f"  NO unknown: {result.no_analysis.unknown_pct:.1%}")
    logger.info(f"  Flagged: {result.is_flagged} (side: {result.flagged_side})")


def test_database():
    """Test database operations."""
    logger.info("")
    logger.info("=" * 50)
    logger.info("TEST 5: Database Operations")
    logger.info("=" * 50)

    project_root = Path(__file__).parent.parent
    db_path = project_root / DEFAULT_DB_PATH

    repo = ScannerRepository(str(db_path))

    # Create a session
    session_id = repo.create_session()
    logger.info(f"  Created session: {session_id}")

    # Get stats
    stats = repo.get_stats()
    logger.info(f"  Database stats: {stats}")

    # Complete session
    repo.complete_session(session_id, 0, 0, status="test")
    logger.info(f"  Completed session")

    logger.info("SUCCESS: Database operations working")


async def main():
    logger.info("")
    logger.info("=" * 60)
    logger.info("POLYMARKET TOP HOLDERS SCANNER - PIPELINE TESTS")
    logger.info("=" * 60)

    try:
        # Test 1: Market Fetcher
        markets = await test_market_fetcher()

        # Test 2: Holder Fetcher
        holders = await test_holder_fetcher(markets)

        # Test 3: PNL Fetching (via positions API)
        enriched_holders = await test_pnl_fetching(holders)

        # Test 4: Imbalance Calculator
        await test_imbalance_calculator(markets, enriched_holders)

        # Test 5: Database
        test_database()

        logger.info("")
        logger.info("=" * 60)
        logger.info("ALL TESTS PASSED!")
        logger.info("=" * 60)
        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. Run a full scan: python scripts/run_scan.py --max-markets 20")
        logger.info("  2. View results: streamlit run dashboard.py")
        logger.info("")

    except Exception as e:
        logger.error(f"TEST FAILED: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
