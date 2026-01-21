"""Lightweight fetcher for current market prices only."""

import aiohttp
import asyncio
import logging
from typing import List, Dict, Tuple, Optional

from ..config.settings import REQUEST_DELAY_SECONDS

logger = logging.getLogger(__name__)

# CLOB API is more reliable for real-time price data
CLOB_API_URL = "https://clob.polymarket.com"


class PriceFetcher:
    """Fetch current prices for specific markets using CLOB API."""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False)
        )
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def fetch_market_by_condition(
        self, condition_id: str
    ) -> Optional[Dict]:
        """Fetch market data by condition ID from CLOB API."""
        try:
            async with self.session.get(
                f"{CLOB_API_URL}/markets/{condition_id}"
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(
                        f"CLOB API returned {response.status} for {condition_id}"
                    )
                return None
        except Exception as e:
            logger.error(f"Error fetching market {condition_id}: {e}")
            return None

    def parse_prices(self, market_data: Dict) -> Tuple[float, float]:
        """Parse YES/NO prices from CLOB market data."""
        tokens = market_data.get("tokens", [])

        yes_price = 0.5
        no_price = 0.5

        for token in tokens:
            outcome = token.get("outcome", "").lower()
            price = token.get("price", 0.5)

            if outcome == "yes":
                yes_price = float(price)
            elif outcome == "no":
                no_price = float(price)

        return yes_price, no_price

    async def fetch_market_prices(
        self, condition_ids: List[str]
    ) -> Dict[str, Tuple[float, float]]:
        """
        Fetch current YES/NO prices for given markets.

        Returns: {condition_id: (yes_price, no_price)}
        """
        results = {}

        for condition_id in condition_ids:
            market_data = await self.fetch_market_by_condition(condition_id)
            if market_data:
                yes_price, no_price = self.parse_prices(market_data)
                results[condition_id] = (yes_price, no_price)

            # Rate limiting
            await asyncio.sleep(REQUEST_DELAY_SECONDS)

        return results

    async def fetch_single_market_price(
        self, condition_id: str
    ) -> Optional[Tuple[float, float]]:
        """Fetch prices for a single market."""
        market_data = await self.fetch_market_by_condition(condition_id)
        if market_data:
            return self.parse_prices(market_data)
        return None


async def fetch_prices_for_trades(condition_ids: List[str]) -> Dict[str, Tuple[float, float]]:
    """Convenience function to fetch prices for multiple condition IDs."""
    async with PriceFetcher() as fetcher:
        return await fetcher.fetch_market_prices(condition_ids)
