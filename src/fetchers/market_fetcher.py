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

    def __init__(self, max_days_to_expiry: Optional[int] = None):
        self.session: Optional[aiohttp.ClientSession] = None
        self.max_days_to_expiry = max_days_to_expiry

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

            # Filter for near-term markets only (if max_days_to_expiry is set)
            if end_date and hasattr(self, 'max_days_to_expiry') and self.max_days_to_expiry:
                time_to_expiry = end_date - datetime.now(end_date.tzinfo)
                if time_to_expiry.total_seconds() > self.max_days_to_expiry * 24 * 3600:
                    return None  # Skip far-out markets

            # Determine slug and category (prefer event data if available)
            slug = raw.get("slug", "")
            category = raw.get("category")  # Top-level category
            events = raw.get("events", [])
            if events and isinstance(events, list) and len(events) > 0:
                event_slug = events[0].get("slug")
                if event_slug:
                    slug = event_slug
                # Prefer event-level category if available
                event_category = events[0].get("category")
                if event_category:
                    category = event_category

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
                category=category,
            )
        except Exception as e:
            logger.warning(f"Failed to parse market: {e}")
            return None

    # Broad category tags in priority order
    BROAD_CATEGORIES = {
        'politics', 'crypto', 'sports', 'finance', 'tech', 'science',
        'entertainment', 'world', 'economy', 'business', 'culture',
        'u.s. politics', 'pop culture', 'climate', 'ai',
    }

    def _pick_category_from_tags(self, tags: List[Dict]) -> Optional[str]:
        """Pick the most relevant broad category from event tags."""
        if not tags:
            return None
        tag_labels = [t.get("label", "") for t in tags if t.get("label")]
        # Check broad categories first
        for label in tag_labels:
            if label.lower() in self.BROAD_CATEGORIES:
                return label
        # Fallback: first tag
        return tag_labels[0] if tag_labels else None

    async def fetch_event_category(self, slug: str) -> Optional[str]:
        """Fetch category for a market by looking up its event tags."""
        if not slug:
            return None
        try:
            async with self.session.get(
                f"{GAMMA_API_URL}/events", params={"slug": slug}
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and isinstance(data, list) and len(data) > 0:
                        tags = data[0].get("tags", [])
                        return self._pick_category_from_tags(tags)
        except Exception as e:
            logger.debug(f"Failed to fetch event category for {slug}: {e}")
        return None

    async def backfill_categories(
        self, markets: List[ActiveMarket]
    ) -> Dict[str, str]:
        """Fetch categories for a list of markets via their event slugs.

        Returns dict mapping market_id -> category.
        """
        # Deduplicate by slug
        slug_to_market_ids: Dict[str, List[str]] = {}
        for m in markets:
            if m.slug:
                slug_to_market_ids.setdefault(m.slug, []).append(m.market_id)

        result = {}
        for slug, market_ids in slug_to_market_ids.items():
            category = await self.fetch_event_category(slug)
            if category:
                for mid in market_ids:
                    result[mid] = category
            await asyncio.sleep(REQUEST_DELAY_SECONDS)

        return result

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
