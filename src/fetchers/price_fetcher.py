"""Lightweight fetcher for current market prices only."""

import aiohttp
import asyncio
import json
import logging
from typing import List, Dict, Tuple, Optional

from ..config.settings import GAMMA_API_URL, REQUEST_DELAY_SECONDS

logger = logging.getLogger(__name__)


class PriceFetcher:
    """Fetch current prices for specific markets."""

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
        """Fetch market data by condition ID."""
        params = {"condition_id": condition_id}

        try:
            async with self.session.get(
                f"{GAMMA_API_URL}/markets", params=params
            ) as response:
                if response.status == 200:
                    markets = await response.json()
                    if markets and len(markets) > 0:
                        return markets[0]
                return None
        except Exception as e:
            logger.error(f"Error fetching market {condition_id}: {e}")
            return None

    def parse_prices(self, market_data: Dict) -> Tuple[float, float]:
        """Parse YES/NO prices from market data."""
        outcome_prices = market_data.get("outcomePrices", [])
        if isinstance(outcome_prices, str):
            outcome_prices = json.loads(outcome_prices)

        yes_price = float(outcome_prices[0]) if len(outcome_prices) > 0 else 0.5
        no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else 0.5

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
