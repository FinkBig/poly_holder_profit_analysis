"""Database repository for scanner data."""

import sqlite3
from typing import List, Dict, Optional
from pathlib import Path
from datetime import datetime

from .schema import get_connection, init_database, run_migrations
from ..models.market import ActiveMarket
from ..models.scan_result import ImbalanceScanResult


class ScannerRepository:
    """Repository for scanner data operations."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        if not Path(db_path).exists():
            init_database(db_path)
        # Run migrations for schema updates
        run_migrations(db_path)
        # Ensure trades table exists (for existing databases)
        self._ensure_trades_table()

    def _get_conn(self) -> sqlite3.Connection:
        return get_connection(self.db_path)

    def _ensure_trades_table(self) -> None:
        """Ensure trades table exists in database."""
        conn = self._get_conn()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT NOT NULL,
                    condition_id TEXT NOT NULL,
                    question TEXT NOT NULL,
                    slug TEXT,
                    side TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    entry_date INTEGER NOT NULL,
                    scan_result_id INTEGER,
                    flagged_side TEXT,
                    edge_pct REAL,
                    opportunity_score REAL,
                    outcome TEXT DEFAULT 'pending',
                    exit_price REAL,
                    exit_date INTEGER,
                    notes TEXT,
                    created_at INTEGER NOT NULL,
                    FOREIGN KEY (market_id) REFERENCES markets(market_id)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_outcome ON trades(outcome)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(entry_date DESC)")
            conn.commit()
        finally:
            conn.close()

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
                (market_id, condition_id, question, slug, token_id_yes, token_id_no, end_date, category, first_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(market_id) DO UPDATE SET
                    question = excluded.question,
                    slug = excluded.slug,
                    end_date = excluded.end_date,
                    category = excluded.category,
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
                    market.category,
                    market.fetched_at,
                    int(datetime.now().timestamp()),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def update_market_category(self, market_id: str, category: str) -> None:
        """Update the category for a market."""
        conn = self._get_conn()
        try:
            conn.execute(
                "UPDATE markets SET category = ? WHERE market_id = ? AND (category IS NULL OR category = '')",
                (category, market_id),
            )
            conn.commit()
        finally:
            conn.close()

    def get_unique_categories(self) -> List[str]:
        """Get all unique categories from markets table."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT DISTINCT category FROM markets
                WHERE category IS NOT NULL AND category != ''
                ORDER BY category
                """
            ).fetchall()
            return [row[0] for row in rows]
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
                    yes_avg_overall_pnl, yes_avg_realized_pnl, yes_avg_30d_pnl, yes_position_size, yes_data_quality_score,
                    no_total_holders, no_top_n_count, no_top_50_pct_count,
                    no_profitable_count, no_losing_count, no_unknown_count,
                    no_profitable_pct, no_unknown_pct,
                    no_avg_overall_pnl, no_avg_realized_pnl, no_avg_30d_pnl, no_position_size, no_data_quality_score,
                    current_yes_price, current_no_price, volume, liquidity,
                    scanned_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    result.yes_analysis.avg_realized_pnl,
                    result.yes_analysis.avg_30d_pnl,
                    result.yes_analysis.total_position_size,
                    result.yes_analysis.data_quality_score,
                    result.no_analysis.total_holders,
                    result.no_analysis.top_n_count,
                    result.no_analysis.top_50_pct_count,
                    result.no_analysis.profitable_count,
                    result.no_analysis.losing_count,
                    result.no_analysis.unknown_count,
                    result.no_analysis.profitable_pct,
                    result.no_analysis.unknown_pct,
                    result.no_analysis.avg_overall_pnl,
                    result.no_analysis.avg_realized_pnl,
                    result.no_analysis.avg_30d_pnl,
                    result.no_analysis.total_position_size,
                    result.no_analysis.data_quality_score,
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
        self, session_id: Optional[int] = None, limit: int = 100, category: Optional[str] = None
    ) -> List[Dict]:
        """Get flagged scan results."""
        conn = self._get_conn()
        try:
            query = """
                SELECT r.*, m.question, m.slug, m.end_date, m.condition_id, m.category
                FROM scan_results r
                JOIN markets m ON r.market_id = m.market_id
                WHERE r.is_flagged = 1
            """
            params = []

            if session_id:
                query += " AND r.session_id = ?"
                params.append(session_id)

            if category:
                query += " AND m.category = ?"
                params.append(category)

            query += " ORDER BY r.imbalance_score DESC LIMIT ?"
            params.append(limit)

            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_all_results(
        self, session_id: Optional[int] = None, limit: int = 500, category: Optional[str] = None
    ) -> List[Dict]:
        """Get all scan results (flagged and non-flagged)."""
        conn = self._get_conn()
        try:
            query = """
                SELECT r.*, m.question, m.slug, m.end_date, m.condition_id, m.category
                FROM scan_results r
                JOIN markets m ON r.market_id = m.market_id
            """
            params = []
            conditions = []

            if session_id:
                conditions.append("r.session_id = ?")
                params.append(session_id)

            if category:
                conditions.append("m.category = ?")
                params.append(category)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

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

    # ==================== Watched Markets (Live Monitoring) ====================

    def add_watched_market(
        self,
        market_id: str,
        condition_id: str,
        token_id_yes: Optional[str] = None,
        token_id_no: Optional[str] = None,
    ) -> None:
        """Add a market to the watch list."""
        conn = self._get_conn()
        try:
            now = int(datetime.now().timestamp())
            conn.execute(
                """
                INSERT OR REPLACE INTO watched_markets
                (market_id, condition_id, token_id_yes, token_id_no, added_at, last_refreshed_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (market_id, condition_id, token_id_yes, token_id_no, now, now),
            )
            conn.commit()
        finally:
            conn.close()

    def remove_watched_market(self, market_id: str) -> None:
        """Remove a market from the watch list."""
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM watched_markets WHERE market_id = ?", (market_id,))
            conn.commit()
        finally:
            conn.close()

    def get_watched_markets(self) -> List[Dict]:
        """Get all watched markets with their market details."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT w.*, m.question, m.slug, m.end_date, m.category
                FROM watched_markets w
                LEFT JOIN markets m ON w.market_id = m.market_id
                ORDER BY w.added_at DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def is_market_watched(self, market_id: str) -> bool:
        """Check if a market is in the watch list."""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT 1 FROM watched_markets WHERE market_id = ?", (market_id,)
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    def update_watched_market_refresh(self, market_id: str) -> None:
        """Update the last_refreshed_at timestamp for a watched market."""
        conn = self._get_conn()
        try:
            now = int(datetime.now().timestamp())
            conn.execute(
                "UPDATE watched_markets SET last_refreshed_at = ? WHERE market_id = ?",
                (now, market_id),
            )
            conn.commit()
        finally:
            conn.close()

    def get_top_flagged_for_monitoring(self, session_id: int, limit: int = 10) -> List[Dict]:
        """Get top N flagged markets for live monitoring auto-refresh."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT r.*, m.question, m.slug, m.end_date, m.condition_id, m.token_id_yes, m.token_id_no, m.category
                FROM scan_results r
                JOIN markets m ON r.market_id = m.market_id
                WHERE r.session_id = ? AND r.is_flagged = 1
                ORDER BY r.imbalance_score DESC
                LIMIT ?
                """,
                (session_id, limit),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    # ==================== Alert System ====================

    def create_alert_config(
        self,
        market_id: str,
        alert_type: str,
        threshold_value: Optional[float] = None,
    ) -> int:
        """Create an alert configuration. Returns config ID."""
        conn = self._get_conn()
        try:
            now = int(datetime.now().timestamp())
            cursor = conn.execute(
                """
                INSERT INTO alert_configs (market_id, alert_type, threshold_value, enabled, created_at)
                VALUES (?, ?, ?, 1, ?)
                """,
                (market_id, alert_type, threshold_value, now),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def get_alert_configs_for_market(self, market_id: str) -> List[Dict]:
        """Get all alert configs for a specific market."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT * FROM alert_configs
                WHERE market_id = ?
                ORDER BY created_at DESC
                """,
                (market_id,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_all_enabled_alert_configs(self) -> List[Dict]:
        """Get all enabled alert configurations with market details."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT ac.*, m.question, m.slug
                FROM alert_configs ac
                JOIN markets m ON ac.market_id = m.market_id
                WHERE ac.enabled = 1
                ORDER BY ac.created_at DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def update_alert_config(self, config_id: int, enabled: bool) -> None:
        """Enable or disable an alert config."""
        conn = self._get_conn()
        try:
            conn.execute(
                "UPDATE alert_configs SET enabled = ? WHERE id = ?",
                (1 if enabled else 0, config_id),
            )
            conn.commit()
        finally:
            conn.close()

    def delete_alert_config(self, config_id: int) -> None:
        """Delete an alert configuration."""
        conn = self._get_conn()
        try:
            conn.execute("DELETE FROM alert_configs WHERE id = ?", (config_id,))
            conn.commit()
        finally:
            conn.close()

    def create_alert(
        self,
        market_id: str,
        alert_type: str,
        old_value: float,
        new_value: float,
        message: str,
    ) -> int:
        """Create a triggered alert. Returns alert ID."""
        conn = self._get_conn()
        try:
            now = int(datetime.now().timestamp())
            cursor = conn.execute(
                """
                INSERT INTO alerts (market_id, alert_type, old_value, new_value, triggered_at, acknowledged, message)
                VALUES (?, ?, ?, ?, ?, 0, ?)
                """,
                (market_id, alert_type, old_value, new_value, now, message),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def get_unacknowledged_alerts(self, limit: int = 50) -> List[Dict]:
        """Get all unacknowledged alerts with market details."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT a.*, m.question, m.slug
                FROM alerts a
                JOIN markets m ON a.market_id = m.market_id
                WHERE a.acknowledged = 0
                ORDER BY a.triggered_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def acknowledge_alert(self, alert_id: int) -> None:
        """Mark an alert as acknowledged."""
        conn = self._get_conn()
        try:
            conn.execute(
                "UPDATE alerts SET acknowledged = 1 WHERE id = ?", (alert_id,)
            )
            conn.commit()
        finally:
            conn.close()

    def acknowledge_all_alerts(self) -> None:
        """Mark all alerts as acknowledged."""
        conn = self._get_conn()
        try:
            conn.execute("UPDATE alerts SET acknowledged = 1")
            conn.commit()
        finally:
            conn.close()

    def get_recent_alerts(self, limit: int = 100) -> List[Dict]:
        """Get recent alerts (both acknowledged and unacknowledged)."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT a.*, m.question, m.slug
                FROM alerts a
                JOIN markets m ON a.market_id = m.market_id
                ORDER BY a.triggered_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_alert_count(self, unacknowledged_only: bool = True) -> int:
        """Get count of alerts."""
        conn = self._get_conn()
        try:
            if unacknowledged_only:
                row = conn.execute(
                    "SELECT COUNT(*) FROM alerts WHERE acknowledged = 0"
                ).fetchone()
            else:
                row = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    # ==================== Backtesting ====================

    def create_backtest_snapshot(
        self,
        market_id: str,
        scan_result_id: Optional[int],
        flagged_side: str,
        edge_pct: float,
        price_at_flag: float,
        flagged_at: int,
    ) -> int:
        """Create a backtest snapshot for a flagged market. Returns snapshot ID."""
        conn = self._get_conn()
        try:
            # Check if snapshot already exists for this market
            existing = conn.execute(
                "SELECT id FROM backtest_snapshots WHERE market_id = ?",
                (market_id,)
            ).fetchone()
            if existing:
                return existing[0]

            cursor = conn.execute(
                """
                INSERT INTO backtest_snapshots
                (market_id, scan_result_id, flagged_side, edge_pct, price_at_flag, flagged_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (market_id, scan_result_id, flagged_side, edge_pct, price_at_flag, flagged_at),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def update_backtest_resolution(
        self,
        market_id: str,
        resolved_outcome: str,
        resolved_at: int,
    ) -> None:
        """Update backtest snapshot with resolution outcome."""
        conn = self._get_conn()
        try:
            # Get snapshot data
            snapshot = conn.execute(
                "SELECT flagged_side, price_at_flag FROM backtest_snapshots WHERE market_id = ?",
                (market_id,)
            ).fetchone()

            if not snapshot:
                return

            flagged_side, price_at_flag = snapshot
            predicted_correct = 1 if flagged_side == resolved_outcome else 0

            # Calculate theoretical PNL (risking $1 per trade)
            # price_at_flag is already the flagged side's price
            if price_at_flag is not None and price_at_flag > 0:
                # Risk $1 per trade: shares = $1 / price_at_flag
                # Win: shares * $1 payout - $1 cost = (1/price - 1)
                # Loss: -$1 (lose our stake)
                if predicted_correct:
                    theoretical_pnl = (1 / price_at_flag) - 1  # Win
                else:
                    theoretical_pnl = -1  # Loss: always lose $1
            else:
                theoretical_pnl = None

            conn.execute(
                """
                UPDATE backtest_snapshots
                SET resolved_outcome = ?, resolved_at = ?, predicted_correct = ?, theoretical_pnl = ?
                WHERE market_id = ?
                """,
                (resolved_outcome, resolved_at, predicted_correct, theoretical_pnl, market_id),
            )

            # Also update markets table
            conn.execute(
                "UPDATE markets SET resolved_outcome = ?, resolved_at = ? WHERE market_id = ?",
                (resolved_outcome, resolved_at, market_id),
            )
            conn.commit()
        finally:
            conn.close()

    def get_backtest_stats(self) -> Dict:
        """Get overall backtest accuracy statistics."""
        conn = self._get_conn()
        try:
            # Price filter: only count trades with entry price in tradeable range
            price_filter = "price_at_flag BETWEEN 0.05 AND 0.95"

            # Total snapshots
            total = conn.execute(f"SELECT COUNT(*) FROM backtest_snapshots WHERE {price_filter}").fetchone()[0]

            # Resolved snapshots
            resolved = conn.execute(
                f"SELECT COUNT(*) FROM backtest_snapshots WHERE resolved_outcome IS NOT NULL AND {price_filter}"
            ).fetchone()[0]

            # Pending snapshots
            pending = conn.execute(
                f"SELECT COUNT(*) FROM backtest_snapshots WHERE resolved_outcome IS NULL AND {price_filter}"
            ).fetchone()[0]

            # Correct predictions
            correct = conn.execute(
                f"SELECT COUNT(*) FROM backtest_snapshots WHERE predicted_correct = 1 AND {price_filter}"
            ).fetchone()[0]

            # Total theoretical PNL
            pnl_row = conn.execute(
                f"SELECT SUM(theoretical_pnl) FROM backtest_snapshots WHERE theoretical_pnl IS NOT NULL AND {price_filter}"
            ).fetchone()
            total_pnl = pnl_row[0] if pnl_row[0] else 0

            accuracy = correct / resolved if resolved > 0 else 0

            return {
                "total_flagged": total,
                "resolved": resolved,
                "pending": pending,
                "correct": correct,
                "incorrect": resolved - correct,
                "accuracy": accuracy,
                "total_pnl": total_pnl,
            }
        finally:
            conn.close()

    def get_backtest_by_edge_level(self) -> List[Dict]:
        """Get accuracy breakdown by edge level at flag time."""
        conn = self._get_conn()
        try:
            price_filter = "price_at_flag BETWEEN 0.05 AND 0.95"
            buckets = [
                (60, 65),
                (65, 70),
                (70, 75),
                (75, 80),
                (80, 101),  # Include 100% edge
            ]
            results = []
            for low, high in buckets:
                row = conn.execute(
                    f"""
                    SELECT
                        COUNT(*) as total,
                        SUM(CASE WHEN predicted_correct = 1 THEN 1 ELSE 0 END) as correct,
                        SUM(theoretical_pnl) as pnl
                    FROM backtest_snapshots
                    WHERE edge_pct >= ? AND edge_pct < ?
                    AND resolved_outcome IS NOT NULL
                    AND {price_filter}
                    """,
                    (low, high),
                ).fetchone()
                total, correct, pnl = row
                accuracy = correct / total if total > 0 else 0
                results.append({
                    "edge_range": f"{low}-{high}%",
                    "total": total or 0,
                    "correct": correct or 0,
                    "accuracy": accuracy,
                    "pnl": pnl or 0,
                })
            return results
        finally:
            conn.close()

    def get_backtest_by_category(self) -> List[Dict]:
        """Get accuracy breakdown by market category."""
        conn = self._get_conn()
        try:
            price_filter = "b.price_at_flag BETWEEN 0.05 AND 0.95"
            rows = conn.execute(
                f"""
                SELECT
                    m.category,
                    COUNT(*) as total,
                    SUM(CASE WHEN b.predicted_correct = 1 THEN 1 ELSE 0 END) as correct,
                    SUM(b.theoretical_pnl) as pnl
                FROM backtest_snapshots b
                JOIN markets m ON b.market_id = m.market_id
                WHERE b.resolved_outcome IS NOT NULL
                AND m.category IS NOT NULL AND m.category != ''
                AND {price_filter}
                GROUP BY m.category
                ORDER BY total DESC
                """
            ).fetchall()

            results = []
            for row in rows:
                cat, total, correct, pnl = row
                accuracy = correct / total if total > 0 else 0
                results.append({
                    "category": cat,
                    "total": total,
                    "correct": correct,
                    "accuracy": accuracy,
                    "pnl": pnl or 0,
                })
            return results
        finally:
            conn.close()

    def get_backtest_snapshots(self, limit: int = 100, resolved_only: bool = False) -> List[Dict]:
        """Get backtest snapshots with market details."""
        conn = self._get_conn()
        try:
            query = """
                SELECT b.*, m.question, m.slug, m.category
                FROM backtest_snapshots b
                JOIN markets m ON b.market_id = m.market_id
            """
            if resolved_only:
                query += " WHERE b.resolved_outcome IS NOT NULL"
            query += " ORDER BY b.flagged_at DESC LIMIT ?"

            rows = conn.execute(query, (limit,)).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_unresolved_flagged_markets(self) -> List[Dict]:
        """Get flagged markets that haven't been resolved yet (for resolution checking)."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT b.market_id, m.condition_id, m.question
                FROM backtest_snapshots b
                JOIN markets m ON b.market_id = m.market_id
                WHERE b.resolved_outcome IS NULL
                ORDER BY b.flagged_at ASC
                """
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def get_unresolved_backtest_markets(self) -> List[Dict]:
        """Get unresolved backtest markets with token IDs for price refresh."""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                """
                SELECT b.id, b.market_id, b.flagged_side, b.edge_pct, b.price_at_flag,
                       m.condition_id, m.token_id_yes, m.token_id_no, m.question, m.slug
                FROM backtest_snapshots b
                JOIN markets m ON b.market_id = m.market_id
                WHERE b.resolved_outcome IS NULL
                ORDER BY b.flagged_at DESC
                """
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()
