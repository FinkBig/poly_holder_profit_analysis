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
    first_seen_at INTEGER NOT NULL,
    last_scanned_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_markets_condition ON markets(condition_id);


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
    yes_avg_30d_pnl REAL,
    yes_position_size REAL,

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
    no_avg_30d_pnl REAL,
    no_position_size REAL,

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


PORTFOLIO_SCHEMA_SQL = """
-- Trades table: Portfolio tracking (stored separately to preserve across git operations)
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
    created_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_id);
CREATE INDEX IF NOT EXISTS idx_trades_outcome ON trades(outcome);
CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(entry_date DESC);
"""


def init_portfolio_database(db_path: str) -> None:
    """Initialize the portfolio database with trades schema."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(PORTFOLIO_SCHEMA_SQL)
        conn.commit()
    finally:
        conn.close()


def get_connection(db_path: str) -> sqlite3.Connection:
    """Get a database connection with row factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn
