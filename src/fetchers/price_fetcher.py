"""Lightweight fetcher for current market prices and holder stats."""

import aiohttp
import asyncio
import logging
from typing import List, Dict, Tuple, Optional, Any

from ..config.settings import REQUEST_DELAY_SECONDS, DATA_API_URL, TOP_HOLDER_COUNT

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


class HolderStatsFetcher:
    """Fetch live holder statistics for portfolio markets."""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self._pnl_cache: Dict[str, float] = {}

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=False)
        )
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    async def fetch_holders(self, condition_id: str) -> List[Dict[str, Any]]:
        """Fetch holders for a market condition."""
        try:
            async with self.session.get(
                f"{DATA_API_URL}/holders",
                params={"market": condition_id}
            ) as response:
                if response.status == 200:
                    return await response.json()
                return []
        except Exception as e:
            logger.error(f"Error fetching holders for {condition_id}: {e}")
            return []

    async def fetch_wallet_pnl(self, wallet: str) -> Optional[float]:
        """Fetch total cash PNL for a wallet (includes unrealized gains/losses)."""
        wallet_lower = wallet.lower()
        if wallet_lower in self._pnl_cache:
            return self._pnl_cache[wallet_lower]

        try:
            async with self.session.get(
                f"{DATA_API_URL}/positions",
                params={"user": wallet}
            ) as response:
                if response.status == 200:
                    positions = await response.json()
                    # Use cashPnl (total including unrealized) instead of realizedPnl
                    # ~80% of active holders have realizedPnl=0
                    total_pnl = sum(float(p.get("cashPnl", 0) or 0) for p in positions)
                    self._pnl_cache[wallet_lower] = total_pnl
                    return total_pnl
                return None
        except Exception:
            return None

    def parse_holders_by_side(
        self,
        raw_response: List[Dict],
        token_id_yes: str,
        token_id_no: str,
        top_n: int = TOP_HOLDER_COUNT
    ) -> Tuple[List[Dict], List[Dict]]:
        """Parse holder response into YES and NO lists."""
        yes_holders = []
        no_holders = []

        excluded_wallets = {
            "0x8bd6c3d7a57d650a1870dd338234f90051fe9918",
            "0x0000000000000000000000000000000000000000",
        }

        for token_data in raw_response:
            token_id = token_data.get("token", "")
            holders = token_data.get("holders", [])

            if token_id == token_id_yes:
                target = yes_holders
            elif token_id == token_id_no:
                target = no_holders
            else:
                continue

            for h in holders:
                wallet = h.get("proxyWallet", "") or h.get("address", "")
                if not wallet or wallet.lower() in excluded_wallets:
                    continue

                amount = float(h.get("amount", 0) or 0)
                if amount > 0:
                    target.append({"wallet": wallet, "amount": amount})

        # Sort by amount and limit
        yes_holders.sort(key=lambda x: x["amount"], reverse=True)
        no_holders.sort(key=lambda x: x["amount"], reverse=True)

        return yes_holders[:top_n], no_holders[:top_n]

    async def calculate_side_stats(self, holders: List[Dict]) -> Dict:
        """Calculate profitable/losing stats for a side."""
        if not holders:
            return {
                "total": 0,
                "profitable": 0,
                "losing": 0,
                "unknown": 0,
                "profitable_pct": 0.0,
                "avg_pnl": 0.0,
            }

        profitable = 0
        losing = 0
        unknown = 0
        pnls = []

        # Fetch PNL for each holder (in batches)
        for i in range(0, len(holders), 5):
            batch = holders[i:i+5]
            tasks = [self.fetch_wallet_pnl(h["wallet"]) for h in batch]
            results = await asyncio.gather(*tasks)

            for pnl in results:
                if pnl is not None:
                    pnls.append(pnl)
                    if pnl > 0:
                        profitable += 1
                    else:
                        losing += 1
                else:
                    unknown += 1

            await asyncio.sleep(REQUEST_DELAY_SECONDS)

        known = profitable + losing
        profitable_pct = profitable / known if known > 0 else 0.0
        avg_pnl = sum(pnls) / len(pnls) if pnls else 0.0

        return {
            "total": len(holders),
            "profitable": profitable,
            "losing": losing,
            "unknown": unknown,
            "profitable_pct": profitable_pct,
            "avg_pnl": avg_pnl,
        }

    async def fetch_market_holder_stats(
        self,
        condition_id: str,
        token_id_yes: str,
        token_id_no: str,
    ) -> Dict:
        """Fetch full holder stats for a market."""
        raw = await self.fetch_holders(condition_id)
        yes_holders, no_holders = self.parse_holders_by_side(
            raw, token_id_yes, token_id_no
        )

        yes_stats = await self.calculate_side_stats(yes_holders)
        no_stats = await self.calculate_side_stats(no_holders)

        # Determine which side is flagged
        flagged_side = None
        edge_pct = abs(yes_stats["profitable_pct"] - no_stats["profitable_pct"]) * 100

        if (yes_stats["profitable_pct"] >= 0.6 and
            yes_stats["profitable_pct"] > no_stats["profitable_pct"] and
            yes_stats["avg_pnl"] > 0):
            flagged_side = "YES"
        elif (no_stats["profitable_pct"] >= 0.6 and
              no_stats["profitable_pct"] > yes_stats["profitable_pct"] and
              no_stats["avg_pnl"] > 0):
            flagged_side = "NO"

        return {
            "yes": yes_stats,
            "no": no_stats,
            "flagged_side": flagged_side,
            "edge_pct": edge_pct,
        }


async def fetch_holder_stats_for_trades(
    trades_with_tokens: List[Dict]
) -> Dict[str, Dict]:
    """
    Fetch holder stats for multiple trades.

    Args:
        trades_with_tokens: List of dicts with condition_id, token_id_yes, token_id_no

    Returns:
        {condition_id: {yes: stats, no: stats, flagged_side, edge_pct}}
    """
    async with HolderStatsFetcher() as fetcher:
        results = {}
        for trade in trades_with_tokens:
            condition_id = trade["condition_id"]
            stats = await fetcher.fetch_market_holder_stats(
                condition_id,
                trade["token_id_yes"],
                trade["token_id_no"],
            )
            results[condition_id] = stats
            await asyncio.sleep(REQUEST_DELAY_SECONDS)
        return results
