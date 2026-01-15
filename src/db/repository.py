"""Database repository for scanner data."""

import sqlite3
from typing import List, Dict, Optional
from pathlib import Path
from datetime import datetime

from .schema import get_connection, init_database
from ..models.market import ActiveMarket
from ..models.scan_result import ImbalanceScanResult


class ScannerRepository:
    """Repository for scanner data operations."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        if not Path(db_path).exists():
            init_database(db_path)

    def _get_conn(self) -> sqlite3.Connection:
        return get_connection(self.db_path)

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
                SELECT r.*, m.question, m.slug, m.end_date
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
                SELECT r.*, m.question, m.slug, m.end_date
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
