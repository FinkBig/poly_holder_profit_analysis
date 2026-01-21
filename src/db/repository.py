"""Database repository for scanner data."""

import sqlite3
from typing import List, Dict, Optional
from pathlib import Path
from datetime import datetime

from .schema import get_connection, init_database, init_portfolio_database
from ..models.market import ActiveMarket
from ..models.scan_result import ImbalanceScanResult


class ScannerRepository:
    """Repository for scanner data operations."""

    def __init__(self, db_path: str, portfolio_db_path: Optional[str] = None):
        self.db_path = db_path
        # Portfolio uses separate database to preserve trades across git operations
        self.portfolio_db_path = portfolio_db_path or db_path.replace("scanner.db", "portfolio.db")

        if not Path(db_path).exists():
            init_database(db_path)

        # Initialize portfolio database if it doesn't exist
        if not Path(self.portfolio_db_path).exists():
            init_portfolio_database(self.portfolio_db_path)

    def _get_conn(self) -> sqlite3.Connection:
        return get_connection(self.db_path)

    def _get_portfolio_conn(self) -> sqlite3.Connection:
        return get_connection(self.portfolio_db_path)

    # ==================== Sessions ====================

    def create_session(self) -> int:
        """Create a new scan session. Returns session ID."""
        conn = self._get_conn()
        try:
            cursor = conn.execute(
                "INSERT INTO scan_sessions (started_at) VALUES (?)",
                (int(datetime.now().timestamp()),),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def update_session_progress(self, session_id: int, scanned: int, flagged: int) -> None:
        """Update session progress."""
        conn = self._get_conn()
        try:
            conn.execute(
                """
                UPDATE scan_sessions
                SET scanned_count = ?, flagged_markets = ?
                WHERE id = ?
                """,
                (scanned, flagged, session_id),
            )
            conn.commit()
        finally:
            conn.close()

    def complete_session(
        self, session_id: int, total: int, flagged: int, status: str = "completed"
    ) -> None:
        """Mark session as complete."""
        conn = self._get_conn()
        try:
            conn.execute(
                """
                UPDATE scan_sessions
                SET completed_at = ?, total_markets = ?, flagged_markets = ?, status = ?
                WHERE id = ?
                """,
                (int(datetime.now().timestamp()), total, flagged, status, session_id),
            )
            conn.commit()
        finally:
            conn.close()

    def get_recent_sessions(self, limit: int = 10) -> List[Dict]:
        """Get recent scan sessions."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT * FROM scan_sessions
                ORDER BY started_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    # ==================== Markets ====================

    def upsert_market(self, market: ActiveMarket) -> None:
        """Insert or update market metadata."""
        conn = self._get_conn()
        try:
            conn.execute(
                """
                INSERT INTO markets
                (market_id, condition_id, question, slug, token_id_yes, token_id_no, end_date, first_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                    question = excluded.question,
                    slug = excluded.slug,
                    end_date = excluded.end_date,
                    last_scanned_at = ?
                """,
                (
                    market.market_id,
                    market.condition_id,
                    market.question,
                    market.slug,
                    market.token_id_yes,
                    market.token_id_no,
                    market.end_date.isoformat() if market.end_date else None,
                    market.fetched_at,
                    int(datetime.now().timestamp()),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    # ==================== Scan Results ====================

    def insert_scan_result(self, session_id: int, result: ImbalanceScanResult) -> int:
        """Insert a scan result. Returns result ID."""
        conn = self._get_conn()
        try:
            cursor = conn.execute(
                """
                INSERT INTO scan_results (
                    session_id, market_id,
                    profitable_skew_yes, profitable_skew_no,
                    is_flagged, flagged_side, imbalance_score,
                    yes_total_holders, yes_top_n_count, yes_top_50_pct_count,
                    yes_profitable_count, yes_losing_count, yes_unknown_count,
                    yes_profitable_pct, yes_unknown_pct,
                    yes_avg_overall_pnl, yes_avg_30d_pnl, yes_position_size,
                    no_total_holders, no_top_n_count, no_top_50_pct_count,
                    no_profitable_count, no_losing_count, no_unknown_count,
                    no_profitable_pct, no_unknown_pct,
                    no_avg_overall_pnl, no_avg_30d_pnl, no_position_size,
                    current_yes_price, current_no_price, volume, liquidity,
                    scanned_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    result.market_id,
                    result.profitable_skew_yes,
                    result.profitable_skew_no,
                    1 if result.is_flagged else 0,
                    result.flagged_side,
                    result.imbalance_score,
                    result.yes_analysis.total_holders,
                    result.yes_analysis.top_n_count,
                    result.yes_analysis.top_50_pct_count,
                    result.yes_analysis.profitable_count,
                    result.yes_analysis.losing_count,
                    result.yes_analysis.unknown_count,
                    result.yes_analysis.profitable_pct,
                    result.yes_analysis.unknown_pct,
                    result.yes_analysis.avg_overall_pnl,
                    result.yes_analysis.avg_30d_pnl,
                    result.yes_analysis.total_position_size,
                    result.no_analysis.total_holders,
                    result.no_analysis.top_n_count,
                    result.no_analysis.top_50_pct_count,
                    result.no_analysis.profitable_count,
                    result.no_analysis.losing_count,
                    result.no_analysis.unknown_count,
                    result.no_analysis.profitable_pct,
                    result.no_analysis.unknown_pct,
                    result.no_analysis.avg_overall_pnl,
                    result.no_analysis.avg_30d_pnl,
                    result.no_analysis.total_position_size,
                    result.current_yes_price,
                    result.current_no_price,
                    result.volume,
                    result.liquidity,
                    result.scanned_at,
                ),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def get_flagged_results(
        self, session_id: Optional[int] = None, limit: int = 100
    ) -> List[Dict]:
        """Get flagged scan results."""
        conn = self._get_conn()
        try:
            query = """
                SELECT r.*, m.question, m.slug, m.end_date, m.condition_id
                FROM scan_results r
                JOIN markets m ON r.market_id = m.market_id
                WHERE r.is_flagged = 1
            """
            params = []

            if session_id:
                query += " AND r.session_id = ?"
                params.append(session_id)

            query += " ORDER BY r.imbalance_score DESC LIMIT ?"
            params.append(limit)

            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_all_results(
        self, session_id: Optional[int] = None, limit: int = 500
    ) -> List[Dict]:
        """Get all scan results (flagged and non-flagged)."""
        conn = self._get_conn()
        try:
            query = """
                SELECT r.*, m.question, m.slug, m.end_date, m.condition_id
                FROM scan_results r
                JOIN markets m ON r.market_id = m.market_id
            """
            params = []

            if session_id:
                query += " WHERE r.session_id = ?"
                params.append(session_id)

            query += " ORDER BY r.imbalance_score DESC LIMIT ?"
            params.append(limit)

            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_market_history(self, market_id: str, limit: int = 20) -> List[Dict]:
        """Get scan history for a specific market."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT * FROM scan_results
                WHERE market_id = ?
                ORDER BY scanned_at DESC
                LIMIT ?
                """,
                (market_id, limit),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_stats(self) -> Dict:
        """Get database statistics."""
        conn = self._get_conn()
        try:
            total_sessions = conn.execute(
                "SELECT COUNT(*) FROM scan_sessions"
            ).fetchone()[0]
            total_results = conn.execute(
                "SELECT COUNT(*) FROM scan_results"
            ).fetchone()[0]
            total_flagged = conn.execute(
                "SELECT COUNT(*) FROM scan_results WHERE is_flagged = 1"
            ).fetchone()[0]
            unique_markets = conn.execute(
                "SELECT COUNT(DISTINCT market_id) FROM scan_results"
            ).fetchone()[0]

            return {
                "total_sessions": total_sessions,
                "total_scan_results": total_results,
                "total_flagged": total_flagged,
                "unique_markets_scanned": unique_markets,
            }
        finally:
            conn.close()

    def get_latest_session_id(self) -> Optional[int]:
        """Get the most recent session ID."""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT id FROM scan_sessions ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def search_markets(self, query: str, limit: int = 50) -> List[Dict]:
        """Search markets by question text. Returns latest scan result for each market."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT r.*, m.question, m.slug, m.end_date, m.condition_id
                FROM scan_results r
                JOIN markets m ON r.market_id = m.market_id
                WHERE m.question LIKE ?
                AND r.id IN (
                    SELECT MAX(id) FROM scan_results GROUP BY market_id
                )
                ORDER BY r.scanned_at DESC
                LIMIT ?
                """,
                (f"%{query}%", limit),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    # ==================== Trades (Portfolio) ====================

    def add_trade(
        self,
        market_id: str,
        condition_id: str,
        question: str,
        slug: str,
        side: str,
        entry_price: float,
        flagged_side: Optional[str] = None,
        edge_pct: Optional[float] = None,
        score: Optional[float] = None,
        scan_result_id: Optional[int] = None,
        notes: Optional[str] = None,
    ) -> int:
        """Log a new trade to portfolio. Returns trade ID."""
        conn = self._get_portfolio_conn()
        try:
            now = int(datetime.now().timestamp())
            cursor = conn.execute(
                """
                INSERT INTO trades (
                    market_id, condition_id, question, slug,
                    side, entry_price, entry_date, scan_result_id,
                    flagged_side, edge_pct, opportunity_score,
                    outcome, notes, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    market_id,
                    condition_id,
                    question,
                    slug,
                    side,
                    entry_price,
                    now,
                    scan_result_id,
                    flagged_side,
                    edge_pct,
                    score,
                    notes,
                    now,
                ),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def get_open_trades(self) -> List[Dict]:
        """Get all trades with outcome='pending'."""
        conn = self._get_portfolio_conn()
        try:
            rows = conn.execute(
                """
                SELECT * FROM trades
                WHERE outcome = 'pending'
                ORDER BY entry_date DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_all_trades(self, limit: int = 100) -> List[Dict]:
        """Get all trades ordered by entry_date DESC."""
        conn = self._get_portfolio_conn()
        try:
            rows = conn.execute(
                """
                SELECT * FROM trades
                ORDER BY entry_date DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def update_trade_outcome(
        self, trade_id: int, outcome: str, exit_price: Optional[float] = None
    ) -> None:
        """Mark trade as win/loss with exit price."""
        conn = self._get_portfolio_conn()
        try:
            now = int(datetime.now().timestamp())
            conn.execute(
                """
                UPDATE trades
                SET outcome = ?, exit_price = ?, exit_date = ?
                WHERE id = ?
                """,
                (outcome, exit_price, now, trade_id),
            )
            conn.commit()
        finally:
            conn.close()

    def delete_trade(self, trade_id: int) -> None:
        """Remove a trade from portfolio."""
        conn = self._get_portfolio_conn()
        try:
            conn.execute("DELETE FROM trades WHERE id = ?", (trade_id,))
            conn.commit()
        finally:
            conn.close()

    def update_trade_entry_price(self, trade_id: int, entry_price: float) -> None:
        """Update the entry price for a trade."""
        conn = self._get_portfolio_conn()
        try:
            conn.execute(
                "UPDATE trades SET entry_price = ? WHERE id = ?",
                (entry_price, trade_id),
            )
            conn.commit()
        finally:
            conn.close()

    def get_portfolio_stats(self) -> Dict:
        """Return portfolio statistics."""
        conn = self._get_portfolio_conn()
        try:
            total = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            open_count = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE outcome = 'pending'"
            ).fetchone()[0]
            wins = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE outcome = 'win'"
            ).fetchone()[0]
            losses = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE outcome = 'loss'"
            ).fetchone()[0]

            closed = wins + losses
            win_rate = wins / closed if closed > 0 else 0

            # Calculate PNL (for closed trades)
            # Win: exit_price - entry_price for YES, entry_price - exit_price for NO
            # Loss: negative of above
            pnl_rows = conn.execute(
                """
                SELECT side, entry_price, exit_price, outcome
                FROM trades
                WHERE outcome IN ('win', 'loss') AND exit_price IS NOT NULL
                """
            ).fetchall()

            total_pnl = 0
            for row in pnl_rows:
                side, entry, exit_p, outcome = row
                if side == "YES":
                    pnl = exit_p - entry
                else:  # NO
                    pnl = (1 - exit_p) - (1 - entry)  # NO payoff is 1 - price
                total_pnl += pnl

            # Avg edge at entry
            avg_edge_row = conn.execute(
                "SELECT AVG(edge_pct) FROM trades WHERE edge_pct IS NOT NULL"
            ).fetchone()
            avg_edge = avg_edge_row[0] if avg_edge_row[0] else 0

            return {
                "total": total,
                "open": open_count,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "total_pnl": total_pnl,
                "avg_edge": avg_edge,
            }
        finally:
            conn.close()

    def get_win_rate_by_edge(self) -> List[Dict]:
        """Get win rate bucketed by edge level at entry."""
        conn = self._get_portfolio_conn()
        try:
            # Bucket edges: 60-65, 65-70, 70-75, 75-80, 80+
            buckets = [
                (60, 65),
                (65, 70),
                (70, 75),
                (75, 80),
                (80, 100),
            ]
            results = []
            for low, high in buckets:
                row = conn.execute(
                    """
                    SELECT
                        COUNT(*) as total,
                        SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) as wins
                    FROM trades
                    WHERE edge_pct >= ? AND edge_pct < ?
                    AND outcome IN ('win', 'loss')
                    """,
                    (low, high),
                ).fetchone()
                total, wins = row
                win_rate = wins / total if total > 0 else 0
                results.append({
                    "edge_range": f"{low}-{high}%",
                    "total": total,
                    "wins": wins,
                    "win_rate": win_rate,
                })
            return results
        finally:
            conn.close()

    def get_prediction_accuracy(self) -> Dict:
        """Get scanner prediction accuracy (when user followed scanner recommendation)."""
        conn = self._get_portfolio_conn()
        try:
            # When scanner flagged YES and user bought YES
            yes_correct_row = conn.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) as wins
                FROM trades
                WHERE flagged_side = 'YES' AND side = 'YES'
                AND outcome IN ('win', 'loss')
                """
            ).fetchone()

            # When scanner flagged NO and user bought NO
            no_correct_row = conn.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) as wins
                FROM trades
                WHERE flagged_side = 'NO' AND side = 'NO'
                AND outcome IN ('win', 'loss')
                """
            ).fetchone()

            yes_total, yes_wins = yes_correct_row
            no_total, no_wins = no_correct_row

            return {
                "yes_total": yes_total,
                "yes_wins": yes_wins,
                "yes_correct": yes_wins / yes_total if yes_total > 0 else 0,
                "no_total": no_total,
                "no_wins": no_wins,
                "no_correct": no_wins / no_total if no_total > 0 else 0,
            }
        finally:
            conn.close()

    def get_trades_by_market(self, market_id: str) -> List[Dict]:
        """Get trade history for a specific market."""
        conn = self._get_portfolio_conn()
        try:
            rows = conn.execute(
                """
                SELECT * FROM trades
                WHERE market_id = ?
                ORDER BY entry_date DESC
                """,
                (market_id,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()
