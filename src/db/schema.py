"""SQLite database schema for scanner results."""

import sqlite3
from pathlib import Path


SCHEMA_SQL = """
-- Scan sessions table: Track each scan run
CREATE TABLE IF NOT EXISTS scan_sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at INTEGER NOT NULL,
    completed_at INTEGER,
    total_markets INTEGER DEFAULT 0,
    scanned_count INTEGER DEFAULT 0,
    flagged_markets INTEGER DEFAULT 0,
    status TEXT DEFAULT 'running'  -- running, completed, failed
);

CREATE INDEX IF NOT EXISTS idx_sessions_started ON scan_sessions(started_at);


-- Markets table: Store market metadata
CREATE TABLE IF NOT EXISTS markets (
    market_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    question TEXT NOT NULL,
    slug TEXT,
    token_id_yes TEXT,
    token_id_no TEXT,
    end_date TEXT,
    category TEXT,
    resolved_outcome TEXT,  -- 'YES', 'NO', or NULL if unresolved
    resolved_at INTEGER,
    first_seen_at INTEGER NOT NULL,
    last_scanned_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_markets_condition ON markets(condition_id);
CREATE INDEX IF NOT EXISTS idx_markets_category ON markets(category);


-- Scan results table: Store imbalance analysis for each scan
CREATE TABLE IF NOT EXISTS scan_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    market_id TEXT NOT NULL,

    -- Imbalance metrics
    profitable_skew_yes REAL NOT NULL,
    profitable_skew_no REAL NOT NULL,
    is_flagged INTEGER NOT NULL DEFAULT 0,
    flagged_side TEXT,
    imbalance_score REAL DEFAULT 0,

    -- YES side analysis
    yes_total_holders INTEGER,
    yes_top_n_count INTEGER,
    yes_top_50_pct_count INTEGER,
    yes_profitable_count INTEGER,
    yes_losing_count INTEGER,
    yes_unknown_count INTEGER,
    yes_profitable_pct REAL,
    yes_unknown_pct REAL,
    yes_avg_overall_pnl REAL,
    yes_avg_realized_pnl REAL,
    yes_avg_30d_pnl REAL,
    yes_position_size REAL,
    yes_data_quality_score REAL,

    -- NO side analysis
    no_total_holders INTEGER,
    no_top_n_count INTEGER,
    no_top_50_pct_count INTEGER,
    no_profitable_count INTEGER,
    no_losing_count INTEGER,
    no_unknown_count INTEGER,
    no_profitable_pct REAL,
    no_unknown_pct REAL,
    no_avg_overall_pnl REAL,
    no_avg_realized_pnl REAL,
    no_avg_30d_pnl REAL,
    no_position_size REAL,
    no_data_quality_score REAL,

    -- Market state at scan time
    current_yes_price REAL,
    current_no_price REAL,
    volume REAL,
    liquidity REAL,

    scanned_at INTEGER NOT NULL,

    FOREIGN KEY (session_id) REFERENCES scan_sessions(id),
    FOREIGN KEY (market_id) REFERENCES markets(market_id)
);

CREATE INDEX IF NOT EXISTS idx_results_session ON scan_results(session_id);
CREATE INDEX IF NOT EXISTS idx_results_market ON scan_results(market_id);
CREATE INDEX IF NOT EXISTS idx_results_flagged ON scan_results(is_flagged);
CREATE INDEX IF NOT EXISTS idx_results_scanned ON scan_results(scanned_at);


-- Holder snapshots table: Store detailed holder data (optional, for deep analysis)
CREATE TABLE IF NOT EXISTS holder_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_result_id INTEGER NOT NULL,
    wallet_address TEXT NOT NULL,
    side TEXT NOT NULL,  -- YES or NO
    amount REAL NOT NULL,
    overall_pnl REAL,
    pnl_30d REAL,
    is_on_leaderboard INTEGER NOT NULL DEFAULT 0,

    FOREIGN KEY (scan_result_id) REFERENCES scan_results(id)
);

CREATE INDEX IF NOT EXISTS idx_holders_result ON holder_snapshots(scan_result_id);
CREATE INDEX IF NOT EXISTS idx_holders_wallet ON holder_snapshots(wallet_address);


-- Trades table: Portfolio tracking (shared with all users)
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    condition_id TEXT NOT NULL,
    question TEXT NOT NULL,
    slug TEXT,

    -- Trade entry
    side TEXT NOT NULL,              -- 'YES' or 'NO' (what user is betting on)
    entry_price REAL NOT NULL,       -- Price when trade logged
    entry_date INTEGER NOT NULL,     -- Timestamp
    scan_result_id INTEGER,          -- Link to scan that flagged it

    -- Scanner prediction context
    flagged_side TEXT,               -- What scanner recommended
    edge_pct REAL,                   -- Edge % at entry
    opportunity_score REAL,          -- Score at entry

    -- Outcome tracking
    outcome TEXT DEFAULT 'pending',  -- 'win', 'loss', 'pending', 'expired'
    exit_price REAL,                 -- Final price (0 or 1 if resolved)
    exit_date INTEGER,               -- When closed/resolved

    -- Analytics
    notes TEXT,
    created_at INTEGER NOT NULL,

    FOREIGN KEY (market_id) REFERENCES markets(market_id)
);

CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_id);
CREATE INDEX IF NOT EXISTS idx_trades_outcome ON trades(outcome);
CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(entry_date DESC);


-- Watched markets table for live monitoring
CREATE TABLE IF NOT EXISTS watched_markets (
    market_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    token_id_yes TEXT,
    token_id_no TEXT,
    added_at INTEGER NOT NULL,
    last_refreshed_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_watched_added ON watched_markets(added_at DESC);


-- Alert configurations
CREATE TABLE IF NOT EXISTS alert_configs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    alert_type TEXT NOT NULL,  -- 'tp', 'sl', 'threshold_cross', 'significant_change'
    threshold_value REAL,
    enabled INTEGER DEFAULT 1,
    created_at INTEGER NOT NULL,
    FOREIGN KEY (market_id) REFERENCES markets(market_id)
);

CREATE INDEX IF NOT EXISTS idx_alert_configs_market ON alert_configs(market_id);
CREATE INDEX IF NOT EXISTS idx_alert_configs_enabled ON alert_configs(enabled);


-- Triggered alerts
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    alert_type TEXT NOT NULL,
    old_value REAL,
    new_value REAL,
    triggered_at INTEGER NOT NULL,
    acknowledged INTEGER DEFAULT 0,
    message TEXT,
    FOREIGN KEY (market_id) REFERENCES markets(market_id)
);

CREATE INDEX IF NOT EXISTS idx_alerts_market ON alerts(market_id);
CREATE INDEX IF NOT EXISTS idx_alerts_acknowledged ON alerts(acknowledged);
CREATE INDEX IF NOT EXISTS idx_alerts_triggered ON alerts(triggered_at DESC);


-- Backtest snapshots: Track flagged markets for accuracy analysis
CREATE TABLE IF NOT EXISTS backtest_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id TEXT NOT NULL,
    scan_result_id INTEGER,
    flagged_side TEXT NOT NULL,
    edge_pct REAL NOT NULL,
    price_at_flag REAL,
    flagged_at INTEGER NOT NULL,
    resolved_outcome TEXT,  -- 'YES', 'NO', or NULL
    resolved_at INTEGER,
    predicted_correct INTEGER,  -- 1 if flagged_side == resolved_outcome
    theoretical_pnl REAL,
    FOREIGN KEY (market_id) REFERENCES markets(market_id),
    FOREIGN KEY (scan_result_id) REFERENCES scan_results(id)
);

CREATE INDEX IF NOT EXISTS idx_backtest_market ON backtest_snapshots(market_id);
CREATE INDEX IF NOT EXISTS idx_backtest_flagged_at ON backtest_snapshots(flagged_at DESC);
CREATE INDEX IF NOT EXISTS idx_backtest_resolved ON backtest_snapshots(resolved_outcome);
"""


def init_database(db_path: str) -> None:
    """Initialize the database with schema."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()




def get_connection(db_path: str) -> sqlite3.Connection:
    """Get a database connection with row factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def run_migrations(db_path: str) -> None:
    """Run database migrations for schema updates."""
    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()

        # Check if category column exists in markets table
        cursor.execute("PRAGMA table_info(markets)")
        market_columns = [col[1] for col in cursor.fetchall()]

        if "category" not in market_columns:
            cursor.execute("ALTER TABLE markets ADD COLUMN category TEXT")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_markets_category ON markets(category)")
            conn.commit()

        # Check scan_results table for PNL veracity columns
        cursor.execute("PRAGMA table_info(scan_results)")
        result_columns = [col[1] for col in cursor.fetchall()]

        # Add yes_avg_realized_pnl if not exists
        if "yes_avg_realized_pnl" not in result_columns:
            cursor.execute("ALTER TABLE scan_results ADD COLUMN yes_avg_realized_pnl REAL")
            cursor.execute("ALTER TABLE scan_results ADD COLUMN no_avg_realized_pnl REAL")
            cursor.execute("ALTER TABLE scan_results ADD COLUMN yes_data_quality_score REAL")
            cursor.execute("ALTER TABLE scan_results ADD COLUMN no_data_quality_score REAL")
            conn.commit()

        # Create watched_markets table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watched_markets (
                market_id TEXT PRIMARY KEY,
                condition_id TEXT NOT NULL,
                token_id_yes TEXT,
                token_id_no TEXT,
                added_at INTEGER NOT NULL,
                last_refreshed_at INTEGER
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_watched_added ON watched_markets(added_at DESC)")
        conn.commit()

        # Create alert tables if not exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alert_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                threshold_value REAL,
                enabled INTEGER DEFAULT 1,
                created_at INTEGER NOT NULL,
                FOREIGN KEY (market_id) REFERENCES markets(market_id)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alert_configs_market ON alert_configs(market_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alert_configs_enabled ON alert_configs(enabled)")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                old_value REAL,
                new_value REAL,
                triggered_at INTEGER NOT NULL,
                acknowledged INTEGER DEFAULT 0,
                message TEXT,
                FOREIGN KEY (market_id) REFERENCES markets(market_id)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_market ON alerts(market_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_acknowledged ON alerts(acknowledged)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_alerts_triggered ON alerts(triggered_at DESC)")
        conn.commit()

        # Add resolved_outcome column to markets if not exists
        cursor.execute("PRAGMA table_info(markets)")
        market_cols = [col[1] for col in cursor.fetchall()]
        if "resolved_outcome" not in market_cols:
            cursor.execute("ALTER TABLE markets ADD COLUMN resolved_outcome TEXT")
            cursor.execute("ALTER TABLE markets ADD COLUMN resolved_at INTEGER")
            conn.commit()

        # Create backtest_snapshots table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS backtest_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_id TEXT NOT NULL,
                scan_result_id INTEGER,
                flagged_side TEXT NOT NULL,
                edge_pct REAL NOT NULL,
                price_at_flag REAL,
                flagged_at INTEGER NOT NULL,
                resolved_outcome TEXT,
                resolved_at INTEGER,
                predicted_correct INTEGER,
                theoretical_pnl REAL,
                FOREIGN KEY (market_id) REFERENCES markets(market_id),
                FOREIGN KEY (scan_result_id) REFERENCES scan_results(id)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_backtest_market ON backtest_snapshots(market_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_backtest_flagged_at ON backtest_snapshots(flagged_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_backtest_resolved ON backtest_snapshots(resolved_outcome)")
        conn.commit()

    finally:
        conn.close()
