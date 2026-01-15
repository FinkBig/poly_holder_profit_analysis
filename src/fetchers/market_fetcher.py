"""Fetcher for all active Polymarket markets from Gamma API."""

import aiohttp
import asyncio
import json
import logging
from typing import List, Dict, Any, Optional

from datetime import datetime
from ..models.market import ActiveMarket
from ..config.settings import GAMMA_API_URL, REQUEST_DELAY_SECONDS

logger = logging.getLogger(__name__)


class ActiveMarketFetcher:
    """Fetches all active markets from Polymarket Gamma API."""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False))
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def fetch_markets_page(
        self, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Fetch a single page of active markets."""
        params = {
            "active": "true",
            "closed": "false",
            "archived": "false",
            "limit": limit,
            "offset": offset,
        }

        try:
            async with self.session.get(
                f"{GAMMA_API_URL}/markets", params=params
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"Markets API returned {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error fetching markets: {e}")
            return []

    def parse_market(self, raw: Dict[str, Any]) -> Optional[ActiveMarket]:
        """Parse raw API response into ActiveMarket."""
        try:
            # Extract token IDs from outcomes
            outcomes = raw.get("outcomes", [])
            clob_token_ids = raw.get("clobTokenIds", [])

            # Handle JSON strings
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(clob_token_ids, str):
                clob_token_ids = json.loads(clob_token_ids)

            # Skip markets without proper binary outcomes
            if len(outcomes) < 2 or len(clob_token_ids) < 2:
                return None

            # Map outcomes to YES/NO tokens (first is YES, second is NO)
            token_yes = clob_token_ids[0] if len(clob_token_ids) > 0 else None
            token_no = clob_token_ids[1] if len(clob_token_ids) > 1 else None

            if not token_yes or not token_no:
                return None

            # Get prices
            outcome_prices = raw.get("outcomePrices", [])
            if isinstance(outcome_prices, str):
                outcome_prices = json.loads(outcome_prices)

            yes_price = float(outcome_prices[0]) if len(outcome_prices) > 0 else 0.5
            no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else 0.5

            # Get condition ID (required for holders API)
            condition_id = raw.get("conditionId", "")
            if not condition_id:
                return None

            # Parse end date
            end_date_str = raw.get("endDate")
            end_date = None
            if end_date_str:
                # Handle Z suffix
                end_date_str = end_date_str.replace("Z", "+00:00")
                try:
                    end_date = datetime.fromisoformat(end_date_str)
                except ValueError:
                    pass
            
            # Filter expired markets
            if end_date and end_date < datetime.now(end_date.tzinfo):
                return None
            
            # Determine slug (prefer event slug if available)
            slug = raw.get("slug", "")
            events = raw.get("events", [])
            if events and isinstance(events, list) and len(events) > 0:
                event_slug = events[0].get("slug")
                if event_slug:
                    slug = event_slug

            return ActiveMarket(
                market_id=raw.get("id", ""),
                condition_id=condition_id,
                question=raw.get("question", ""),
                slug=slug,
                token_id_yes=token_yes,
                token_id_no=token_no,
                volume=float(raw.get("volumeNum", 0) or 0),
                liquidity=float(raw.get("liquidityNum", 0) or 0),
                yes_price=yes_price,
                no_price=no_price,
                end_date=end_date,
            )
        except Exception as e:
            logger.warning(f"Failed to parse market: {e}")
            return None

    async def fetch_all_active_markets(
        self,
        page_size: int = 100,
        max_markets: Optional[int] = None,
    ) -> List[ActiveMarket]:
        """
        Fetch ALL active markets with pagination.

        Args:
            page_size: Results per API call
            max_markets: Optional limit on total markets

        Returns:
            List of ActiveMarket objects
        """
        all_markets = []
        offset = 0

        while True:
            raw_markets = await self.fetch_markets_page(limit=page_size, offset=offset)

            if not raw_markets:
                break

            for raw in raw_markets:
                market = self.parse_market(raw)
                if market:
                    all_markets.append(market)

            logger.info(f"Fetched {len(all_markets)} markets so far (offset={offset})")

            if len(raw_markets) < page_size:
                # Last page
                break

            if max_markets and len(all_markets) >= max_markets:
                all_markets = all_markets[:max_markets]
                break

            offset += page_size
            await asyncio.sleep(REQUEST_DELAY_SECONDS)

        logger.info(f"Total active markets fetched: {len(all_markets)}")
        return all_markets
