"""Streamlit dashboard for PNL Imbalance Scanner."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
from pathlib import Path
import math
import asyncio

sys.path.insert(0, str(Path(__file__).parent))

from src.db.repository import ScannerRepository
from src.config.settings import DEFAULT_DB_PATH, IMBALANCE_THRESHOLD
from src.fetchers.price_fetcher import fetch_prices_for_trades, fetch_holder_stats_for_trades

# Page Config
st.set_page_config(
    page_title="Polymarket Scanner",
    layout="wide",
    initial_sidebar_state="collapsed", # Cleaner terminal look
    page_icon="📈"
)

# --- CSS STYLING ---
st.markdown("""
<style>
    /* Global Font & Background */
    .stApp {
        font-family: 'IBM Plex Mono', 'Courier New', monospace;
    }
    
    /* Remove padding around main container for full width feel */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        padding-left: 2rem;
        padding-right: 2rem;
    }

    /* Metric Cards */
    div[data-testid="stMetric"] {
        background-color: #262730;
        border: 1px solid #363945;
        padding: 10px;
        border-radius: 5px;
    }
    div[data-testid="stMetricLabel"] {
        font-size: 0.8rem;
        color: #9CA3AF;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.2rem;
        font-weight: 600;
        color: #F3F4F6;
    }

    /* Table Styling */
    div[data-testid="stDataFrame"] {
        border: 1px solid #363945;
        border-radius: 5px;
    }
    
    /* Buttons */
    .stButton button {
        border-radius: 4px;
        font-weight: bold;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-size: 0.8rem;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 20px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    
    /* Custom Headers */
    .terminal-header {
        border-bottom: 2px solid #00C076;
        padding-bottom: 10px;
        margin-bottom: 20px;
        color: #00C076;
        font-weight: bold;
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }
    
    /* Side panel bg */
    section[data-testid="stSidebar"] {
        background-color: #11141a;
        border-right: 1px solid #363945;
    }

    /* === STATUS BADGES === */
    .badge-yes {
        background: rgba(0,192,118,0.15);
        color: #00C076;
        padding: 4px 12px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.85rem;
        display: inline-block;
    }
    .badge-no {
        background: rgba(255,79,79,0.15);
        color: #FF4F4F;
        padding: 4px 12px;
        border-radius: 12px;
        font-weight: 600;
        font-size: 0.85rem;
        display: inline-block;
    }
    .badge-win {
        background: rgba(0,192,118,0.25);
        color: #00C076;
        border: 1px solid #00C076;
        padding: 4px 10px;
        border-radius: 12px;
        font-weight: bold;
        font-size: 0.8rem;
        display: inline-block;
    }
    .badge-loss {
        background: rgba(255,79,79,0.25);
        color: #FF4F4F;
        border: 1px solid #FF4F4F;
        padding: 4px 10px;
        border-radius: 12px;
        font-weight: bold;
        font-size: 0.8rem;
        display: inline-block;
    }

    /* === TABLE HEADERS === */
    .table-header {
        background: linear-gradient(180deg, #1a1c24 0%, #262730 100%);
        border-bottom: 2px solid #3B82F6;
        padding: 10px 8px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        font-size: 0.7rem;
        color: #9CA3AF;
        margin-bottom: 8px;
    }

    /* === ROW HOVER EFFECTS === */
    .stButton button[kind="secondary"]:hover {
        background: rgba(59,130,246,0.15) !important;
        border-left: 3px solid #3B82F6;
        transition: all 0.15s ease;
    }
    .stButton button[kind="primary"]:hover {
        box-shadow: 0 0 12px rgba(59,130,246,0.4);
    }

    /* === CUSTOM SCROLLBAR === */
    div[data-testid="stVerticalBlock"] > div::-webkit-scrollbar {
        width: 6px;
    }
    div[data-testid="stVerticalBlock"] > div::-webkit-scrollbar-track {
        background: #1a1c24;
    }
    div[data-testid="stVerticalBlock"] > div::-webkit-scrollbar-thumb {
        background: #363945;
        border-radius: 3px;
    }
    div[data-testid="stVerticalBlock"] > div::-webkit-scrollbar-thumb:hover {
        background: #4a4d5a;
    }

    /* === SIDEBAR POLISH === */
    section[data-testid="stSidebar"] .streamlit-expanderHeader {
        background: rgba(38,39,48,0.5);
        border-radius: 8px;
        padding: 8px 12px;
    }
    section[data-testid="stSidebar"] hr {
        border-color: #363945;
        margin: 16px 0;
    }

    /* === ACTION BUTTONS === */
    .action-btn {
        width: 28px;
        height: 28px;
        padding: 0;
        border-radius: 50%;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 14px;
        border: 1px solid transparent;
        background: transparent;
        cursor: pointer;
        transition: all 0.15s ease;
    }
    .action-btn:hover {
        transform: scale(1.1);
    }
    .action-btn-win:hover {
        background: rgba(0,192,118,0.2);
        border-color: #00C076;
    }
    .action-btn-loss:hover {
        background: rgba(255,79,79,0.2);
        border-color: #FF4F4F;
    }
    .action-btn-delete:hover {
        background: rgba(156,163,175,0.2);
        border-color: #9CA3AF;
    }
</style>
""", unsafe_allow_html=True)


def get_repository():
    """Get database repository."""
    project_root = Path(__file__).parent
    db_path = project_root / DEFAULT_DB_PATH
    return ScannerRepository(str(db_path))

def calculate_hours_remaining(end_date_str):
    """Calculate hours remaining until expiration."""
    if not end_date_str:
        return 999999
    try:
        # Parse the ISO format date string
        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))

        # Get current time in same timezone as end_date, or use naive comparison
        if end_date.tzinfo is not None:
            from datetime import timezone
            now = datetime.now(timezone.utc)
        else:
            now = datetime.now()

        if now > end_date:
            return 0
        diff = end_date - now
        return diff.total_seconds() / 3600
    except Exception:
        return 999999

def format_time_remaining(hours):
    """Format hours into readable string."""
    if hours >= 999999:
        return "UNK"
    if hours <= 0:
        return "EXP"
    days = int(hours // 24)
    rem_hours = int(hours % 24)
    if days > 0:
        return f"{days}d {rem_hours}h"
    return f"{rem_hours}h"


def calculate_opportunity_score(row, weights=None):
    """
    Opportunity score based on probability of winning.

    Score Breakdown (customizable weights):
    - Edge strength: Primary signal (default weight: 50)
    - Sample size: min of both sides for confidence (default weight: 30)
    - PNL conviction: experienced traders backing one side (default weight: 10)
    - Data quality: low unknown % = confident (default weight: 10)

    Note: Time/liquidity/volume are filtering preferences, not win probability factors.
    """
    if weights is None:
        weights = {"edge": 50, "sample": 30, "pnl": 10, "quality": 10}

    # Edge strength (0-1 normalized, then scaled by weight)
    yes_prof_pct = row.get("yes_profitable_pct", 0)
    no_prof_pct = row.get("no_profitable_pct", 0)
    imbalance_pct = abs(yes_prof_pct - no_prof_pct)
    edge_normalized = imbalance_pct  # 0 to 1 (0% to 100% edge)
    edge_score = edge_normalized * weights["edge"]

    # Sample size (0-1 normalized) - use MIN of both sides for confidence
    yes_holders = row.get("yes_top_n_count", 5) or 5
    no_holders = row.get("no_top_n_count", 5) or 5
    min_holders = min(yes_holders, no_holders)
    sample_normalized = min(max((min_holders - 5) / 45, 0), 1)  # 5->0, 50->1
    sample_score = sample_normalized * weights["sample"]

    # PNL conviction (0-1 normalized) - experienced traders' track record
    yes_avg_pnl = row.get("yes_avg_overall_pnl", 0) or 0
    no_avg_pnl = row.get("no_avg_overall_pnl", 0) or 0
    pnl_diff = abs(yes_avg_pnl - no_avg_pnl)
    pnl_normalized = min(pnl_diff / 50000, 1)  # $50k diff = 1.0
    pnl_score = pnl_normalized * weights["pnl"]

    # Data quality (0-1 normalized) - penalize high unknown %
    flagged_side = row.get("flagged_side")
    if flagged_side == "YES":
        unknown_pct = row.get("yes_unknown_pct", 1) or 0
    else:
        unknown_pct = row.get("no_unknown_pct", 1) or 0
    quality_normalized = 1 - unknown_pct  # 0% unknown = 1.0
    quality_score = quality_normalized * weights["quality"]

    return edge_score + sample_score + pnl_score + quality_score


def get_trade_action(row):
    """Determine trade recommendation based on flagged side."""
    flagged_side = row.get("flagged_side", "")
    if flagged_side == "YES":
        return "BUY YES", "#00C076"  # Green
    elif flagged_side == "NO":
        return "BUY NO", "#FF4F4F"  # Red
    return "—", "#666666"


def render_edge_bar(edge_pct, flagged_side="YES"):
    """Render a visual edge bar with percentage."""
    color = "#00C076" if flagged_side == "YES" else "#FF4F4F"
    width = min(edge_pct, 100)
    html = f'''
    <div style="display:flex;align-items:center;gap:8px;">
        <div style="flex:1;height:6px;background:#262730;border-radius:3px;overflow:hidden;min-width:60px;">
            <div style="width:{width}%;height:100%;background:{color};border-radius:3px;"></div>
        </div>
        <span style="color:{color};font-weight:bold;font-size:0.85rem;">{edge_pct:.0f}%</span>
    </div>
    '''
    st.markdown(html, unsafe_allow_html=True)


def render_side_badge(side):
    """Render a side badge (YES/NO)."""
    badge_class = "badge-yes" if side == "YES" else "badge-no"
    return f"<span class='{badge_class}'>{side}</span>"


def render_outcome_badge(outcome):
    """Render an outcome badge (WIN/LOSS)."""
    if outcome == "win":
        return "<span class='badge-win'>WIN</span>"
    elif outcome == "loss":
        return "<span class='badge-loss'>LOSS</span>"
    return ""


def render_copy_button(text, key):
    """Render a copy-to-clipboard button using JavaScript."""
    escaped = text.replace("\\", "\\\\").replace("`", "\\`").replace("$", "\\$").replace('"', '\\"')
    html = f'''
    <button onclick="navigator.clipboard.writeText(`{escaped}`).then(() => {{
        this.innerHTML = '✓';
        setTimeout(() => this.innerHTML = '📋', 1500);
    }})" style="
        background: #262730;
        border: 1px solid #363945;
        border-radius: 4px;
        padding: 4px 8px;
        cursor: pointer;
        color: #9CA3AF;
        font-size: 14px;
    ">📋</button>
    '''
    st.components.v1.html(html, height=35)


def render_opportunities_tab(repo):
    """Render the Top 20 Opportunities view with actionable trading info."""
    st.markdown("<div class='terminal-header'>TOP TRADING OPPORTUNITIES</div>", unsafe_allow_html=True)

    # Get filter values from session state
    max_days = st.session_state.get("filter_max_days", 14)
    min_edge = st.session_state.get("filter_min_edge", 60)
    min_liquidity = st.session_state.get("filter_min_liquidity", 1000)
    min_price = st.session_state.get("filter_min_price", 0.15)
    max_price = st.session_state.get("filter_max_price", 0.85)

    # Get score weights from session state
    weights = {
        "edge": st.session_state.get("weight_edge", 50),
        "sample": st.session_state.get("weight_sample", 30),
        "pnl": st.session_state.get("weight_pnl", 10),
        "quality": st.session_state.get("weight_quality", 10),
    }

    # Fetch latest session data
    latest_session = repo.get_latest_session_id()
    if not latest_session:
        st.info("No scan data available. Run a scan first.")
        return

    results = repo.get_flagged_results(session_id=latest_session, limit=500)
    if not results:
        st.info("No flagged opportunities found in latest scan.")
        return

    # Process and score opportunities
    opportunities = []
    for r in results:
        hours_rem = calculate_hours_remaining(r.get("end_date"))

        # Apply filters
        if hours_rem > max_days * 24:
            continue
        if hours_rem <= 0:
            continue

        yes_prof_pct = r.get("yes_profitable_pct", 0)
        no_prof_pct = r.get("no_profitable_pct", 0)
        imbalance_pct = abs(yes_prof_pct - no_prof_pct) * 100
        if imbalance_pct < min_edge:
            continue

        liquidity = r.get("liquidity", 0)
        if liquidity < min_liquidity:
            continue

        # Price filter - check if YES or NO price is within range
        yes_price = r.get("current_yes_price", 0)
        no_price = r.get("current_no_price", 0)
        if not ((min_price <= yes_price <= max_price) or (min_price <= no_price <= max_price)):
            continue

        # Calculate opportunity score
        r["hours_remaining"] = hours_rem
        score = calculate_opportunity_score(r, weights)

        yes_avg_pnl = r.get("yes_avg_overall_pnl", 0) or 0
        no_avg_pnl = r.get("no_avg_overall_pnl", 0) or 0
        pnl_diff = abs(yes_avg_pnl - no_avg_pnl)

        action, color = get_trade_action(r)
        slug = r.get("slug", "")
        url = f"https://polymarket.com/event/{slug}" if slug else ""

        # Get holder counts for both sides
        yes_holders = r.get("yes_top_n_count", 0) or 0
        no_holders = r.get("no_top_n_count", 0) or 0

        opportunities.append({
            "rank": 0,
            "question": r.get("question", ""),
            "action": action,
            "action_color": color,
            "flagged_side": r.get("flagged_side", "YES"),
            "score": score,
            "edge": imbalance_pct,
            "pnl_diff": pnl_diff,
            "hours_remaining": hours_rem,
            "yes_holders": yes_holders,
            "no_holders": no_holders,
            "yes_price": yes_price,
            "no_price": no_price,
            "url": url,
            "market_id": r.get("market_id"),
            "raw_data": r
        })

    # Sort by score and take top 20
    opportunities = sorted(opportunities, key=lambda x: x["score"], reverse=True)[:20]

    if not opportunities:
        st.warning(f"No opportunities match current filters (Edge ≥{min_edge}%, Expires ≤{max_days}d, Liquidity ≥${min_liquidity:,})")
        return

    # Assign ranks
    for i, opp in enumerate(opportunities):
        opp["rank"] = i + 1

    # CSV Export button
    csv_data = "Rank\tMarket\tAction\tScore\tEdge %\tPNL Diff\tYES N\tNO N\tYES Price\tNO Price\tExpires\tURL\n"
    for opp in opportunities:
        csv_data += f"{opp['rank']}\t{opp['question']}\t{opp['action']}\t{opp['score']:.0f}\t{opp['edge']:.0f}%\t${opp['pnl_diff']:,.0f}\t{opp['yes_holders']}\t{opp['no_holders']}\t{opp['yes_price']:.2f}\t{opp['no_price']:.2f}\t{format_time_remaining(opp['hours_remaining'])}\t{opp['url']}\n"

    col_download, col_info = st.columns([1, 3])
    with col_download:
        st.download_button(
            "📥 Download CSV",
            csv_data,
            "top_opportunities.csv",
            "text/csv",
            use_container_width=True
        )
    with col_info:
        st.caption(f"Showing top {len(opportunities)} opportunities • Edge ≥{min_edge}% • Expires ≤{max_days}d")

    st.markdown("---")

    # Split view: List on left, Analysis on right
    col_list, col_detail = st.columns([1.5, 1])

    with col_list:
        # Header row with styled headers
        st.markdown("""
        <div class='table-header'>
            <div style='display:flex;gap:8px;'>
                <span style='width:30px;'>#</span>
                <span style='flex:2.5;'>Market</span>
                <span style='width:70px;'>Action</span>
                <span style='width:50px;'>Score</span>
                <span style='width:80px;'>Edge</span>
                <span style='width:50px;'>N</span>
                <span style='width:50px;'>Exp</span>
                <span style='width:40px;'>📋</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        h_rank, h_market, h_action, h_score, h_edge, h_n, h_exp, h_copy = st.columns([0.3, 2.5, 0.7, 0.5, 0.8, 0.5, 0.5, 0.4])

        # Data rows
        with st.container(height=550):
            for opp in opportunities:
                c_rank, c_market, c_action, c_score, c_edge, c_n, c_exp, c_copy = st.columns([0.3, 2.5, 0.7, 0.5, 0.8, 0.5, 0.5, 0.4])

                # Check if this market is selected
                is_selected = st.session_state.get("opp_selected_market_id") == opp["market_id"]

                with c_rank:
                    st.markdown(f"**{opp['rank']}**")

                with c_market:
                    market_name = opp['question'][:42] + "..." if len(opp['question']) > 42 else opp['question']
                    if st.button(
                        market_name,
                        key=f"opp_market_{opp['rank']}",
                        use_container_width=True,
                        type="primary" if is_selected else "secondary"
                    ):
                        st.session_state["opp_selected_market_id"] = opp["market_id"]
                        st.session_state["opp_selected_raw_data"] = opp["raw_data"]
                        st.rerun()

                with c_action:
                    # Use badge styling for action
                    badge_class = "badge-yes" if opp['flagged_side'] == "YES" else "badge-no"
                    st.markdown(f"<span class='{badge_class}'>{opp['action']}</span>", unsafe_allow_html=True)

                with c_score:
                    st.markdown(f"{opp['score']:.0f}")

                with c_edge:
                    # Use edge bar visualization
                    render_edge_bar(opp['edge'], opp['flagged_side'])

                with c_n:
                    st.markdown(f"{opp['yes_holders']}/{opp['no_holders']}")

                with c_exp:
                    st.markdown(format_time_remaining(opp['hours_remaining']))

                with c_copy:
                    # Tab-separated row for Excel pasting
                    copy_text = f"{opp['question']}\t{opp['action']}\t{opp['score']:.0f}\t{opp['edge']:.0f}%\t${opp['pnl_diff']:,.0f}\tY:{opp['yes_holders']}\tN:{opp['no_holders']}\tYES:{opp['yes_price']:.2f}\tNO:{opp['no_price']:.2f}\t{format_time_remaining(opp['hours_remaining'])}\t{opp['url']}"
                    render_copy_button(copy_text, f"copy_{opp['rank']}")

    # Analysis Panel (Right Side)
    with col_detail:
        selected_data = None

        # Get selected market data
        if "opp_selected_raw_data" in st.session_state:
            selected_data = st.session_state["opp_selected_raw_data"]
        elif opportunities:
            # Default to first opportunity
            selected_data = opportunities[0]["raw_data"]
            st.session_state["opp_selected_market_id"] = opportunities[0]["market_id"]
            st.session_state["opp_selected_raw_data"] = selected_data

        if selected_data:
            render_market_detail_view(selected_data, repo=repo, key_prefix="opp")
        else:
            st.info("Select a market to view analysis.")

def render_portfolio_tab(repo):
    """Render the Portfolio tab with trade tracking and analytics."""
    st.markdown("<div class='terminal-header'>PORTFOLIO TRACKER</div>", unsafe_allow_html=True)

    # Portfolio Stats Row
    stats = repo.get_portfolio_stats()
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Open Trades", stats["open"])
    col2.metric("Total Trades", stats["total"])
    col3.metric("Win Rate", f"{stats['win_rate']:.1%}" if stats["wins"] + stats["losses"] > 0 else "N/A")
    col4.metric("W/L", f"{stats['wins']}/{stats['losses']}")
    col5.metric("Total PNL", f"${stats['total_pnl']:.2f}")

    st.markdown("---")

    # Single Refresh Button
    btn_col1, btn_col2 = st.columns([1, 3])
    with btn_col1:
        if st.button("🔄 Refresh Data", use_container_width=True):
            trades_with_tokens = repo.get_trades_with_token_ids()
            if trades_with_tokens:
                condition_ids = [t["condition_id"] for t in trades_with_tokens]
                valid_trades = [t for t in trades_with_tokens if t.get("token_id_yes") and t.get("token_id_no")]

                with st.spinner("Fetching prices and holder stats..."):
                    try:
                        # Fetch prices
                        prices = asyncio.run(fetch_prices_for_trades(condition_ids))
                        st.session_state["portfolio_prices"] = prices

                        # Fetch holder stats
                        if valid_trades:
                            holder_stats = asyncio.run(fetch_holder_stats_for_trades(valid_trades))
                            st.session_state["portfolio_holder_stats"] = holder_stats

                        st.success(f"Updated {len(prices)} markets")
                    except Exception as e:
                        st.error(f"Failed to refresh: {e}")
            else:
                st.info("No open trades to refresh")

    # Get all trades
    all_trades = repo.get_all_trades(limit=100)

    if not all_trades:
        st.info("No trades in portfolio. Add trades from the Top Opportunities tab.")
        return

    # Get cached data
    cached_prices = st.session_state.get("portfolio_prices", {})
    cached_holder_stats = st.session_state.get("portfolio_holder_stats", {})

    # Filter trades
    open_trades = [t for t in all_trades if t["outcome"] == "pending"]
    closed_trades = [t for t in all_trades if t["outcome"] in ("win", "loss")]

    # Split layout: Trade list on left, Analysis on right
    col_list, col_detail = st.columns([1.5, 1])

    with col_list:
        st.markdown("### Open Trades")

        if open_trades:
            # Styled header row
            st.markdown("""
            <div class='table-header'>
                <div style='display:flex;gap:8px;'>
                    <span style='flex:2;'>Market</span>
                    <span style='width:50px;'>Side</span>
                    <span style='width:50px;'>Entry</span>
                    <span style='width:50px;'>Now</span>
                    <span style='width:80px;'>Edge</span>
                    <span style='width:80px;'>Actions</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            h_market, h_side, h_entry, h_now, h_edge, h_actions = st.columns([2.0, 0.4, 0.5, 0.5, 0.7, 0.8])

            with st.container(height=350):
                for trade in open_trades:
                    c_market, c_side, c_entry, c_now, c_edge, c_actions = st.columns([2.0, 0.4, 0.5, 0.5, 0.7, 0.8])

                    # Clickable market name
                    market_name = trade["question"][:30] + "..." if len(trade["question"]) > 30 else trade["question"]
                    is_selected = st.session_state.get("portfolio_selected_trade_id") == trade["id"]

                    with c_market:
                        if st.button(
                            market_name,
                            key=f"ptrade_{trade['id']}",
                            use_container_width=True,
                            type="primary" if is_selected else "secondary"
                        ):
                            st.session_state["portfolio_selected_trade_id"] = trade["id"]
                            st.session_state["portfolio_selected_trade"] = trade
                            st.rerun()

                    # Side - use badge
                    c_side.markdown(render_side_badge(trade["side"]), unsafe_allow_html=True)

                    # Entry price
                    c_entry.markdown(f"${trade['entry_price']:.2f}")

                    # Current price
                    if trade["condition_id"] in cached_prices:
                        yes_price, no_price = cached_prices[trade["condition_id"]]
                        current_price = yes_price if trade["side"] == "YES" else no_price
                        change = current_price - trade['entry_price']
                        change_color = "#00C076" if change >= 0 else "#FF4F4F"
                        c_now.markdown(f"<span style='color:{change_color};'>${current_price:.2f}</span>", unsafe_allow_html=True)
                    else:
                        c_now.markdown("—")

                    # Live edge - use edge bar
                    with c_edge:
                        if trade["condition_id"] in cached_holder_stats:
                            live_stats = cached_holder_stats[trade["condition_id"]]
                            live_edge = live_stats.get("edge_pct", 0)
                            live_flagged = live_stats.get("flagged_side")
                            render_edge_bar(live_edge, live_flagged or trade["side"])
                        else:
                            edge = trade.get("edge_pct")
                            if edge:
                                render_edge_bar(edge, trade["side"])
                            else:
                                st.markdown("—")

                    # Action buttons
                    with c_actions:
                        b1, b2, b3 = st.columns(3)
                        with b1:
                            if st.button("✓", key=f"win_{trade['id']}", help="Win"):
                                exit_price = 1.0 if trade["side"] == "YES" else 0.0
                                repo.update_trade_outcome(trade["id"], "win", exit_price)
                                st.rerun()
                        with b2:
                            if st.button("✗", key=f"loss_{trade['id']}", help="Loss"):
                                exit_price = 0.0 if trade["side"] == "YES" else 1.0
                                repo.update_trade_outcome(trade["id"], "loss", exit_price)
                                st.rerun()
                        with b3:
                            if st.button("🗑", key=f"del_{trade['id']}", help="Delete"):
                                repo.delete_trade(trade["id"])
                                st.rerun()
        else:
            st.info("No open trades")

        # Closed Trades
        st.markdown("### Closed Trades")
        if closed_trades:
            # Styled header row
            st.markdown("""
            <div class='table-header'>
                <div style='display:flex;gap:8px;'>
                    <span style='flex:2.5;'>Market</span>
                    <span style='width:50px;'>Side</span>
                    <span style='width:50px;'>Entry</span>
                    <span style='width:50px;'>Exit</span>
                    <span style='width:60px;'>Result</span>
                </div>
            </div>
            """, unsafe_allow_html=True)
            with st.container(height=200):
                for trade in closed_trades[:15]:
                    c_market, c_side, c_entry, c_exit, c_result = st.columns([2.5, 0.4, 0.5, 0.5, 0.6])

                    market_name = trade["question"][:35] + "..." if len(trade["question"]) > 35 else trade["question"]
                    is_selected = st.session_state.get("portfolio_selected_trade_id") == trade["id"]

                    with c_market:
                        if st.button(
                            market_name,
                            key=f"ctrade_{trade['id']}",
                            use_container_width=True,
                            type="primary" if is_selected else "secondary"
                        ):
                            st.session_state["portfolio_selected_trade_id"] = trade["id"]
                            st.session_state["portfolio_selected_trade"] = trade
                            st.rerun()

                    # Use badges for side and result
                    c_side.markdown(render_side_badge(trade["side"]), unsafe_allow_html=True)
                    c_entry.markdown(f"${trade['entry_price']:.2f}")

                    exit_price = trade.get("exit_price")
                    c_exit.markdown(f"${exit_price:.2f}" if exit_price is not None else "—")

                    c_result.markdown(render_outcome_badge(trade["outcome"]), unsafe_allow_html=True)
        else:
            st.info("No closed trades yet")

    # Analysis Panel (Right Side)
    with col_detail:
        selected_trade = st.session_state.get("portfolio_selected_trade")

        if selected_trade:
            # Try to get scan data for this market
            market_id = selected_trade.get("market_id")
            scan_data = None
            if market_id:
                history = repo.get_market_history(market_id, limit=1)
                if history:
                    scan_data = history[0]
                    # Add question and slug from trade
                    scan_data["question"] = selected_trade.get("question", "")
                    scan_data["slug"] = selected_trade.get("slug", "")

            if scan_data:
                render_market_detail_view(scan_data, repo=repo, key_prefix="port")
            else:
                # Show basic trade info if no scan data
                st.markdown(f"### {selected_trade.get('question', 'Trade Details')}")
                st.markdown(f"**Side:** {selected_trade.get('side')}")
                st.markdown(f"**Entry Price:** ${selected_trade.get('entry_price', 0):.2f}")
                st.markdown(f"**Entry Edge:** {selected_trade.get('edge_pct', 0):.0f}%")

                # Show live stats if available
                condition_id = selected_trade.get("condition_id")
                if condition_id in cached_holder_stats:
                    st.markdown("---")
                    st.markdown("### Live Holder Stats")
                    live = cached_holder_stats[condition_id]
                    yes_stats = live.get("yes", {})
                    no_stats = live.get("no", {})

                    col_yes, col_no = st.columns(2)
                    with col_yes:
                        st.markdown("**YES Side**")
                        st.markdown(f"Profitable: {yes_stats.get('profitable_pct', 0):.0%}")
                        st.markdown(f"Avg PNL: ${yes_stats.get('avg_pnl', 0):,.0f}")
                    with col_no:
                        st.markdown("**NO Side**")
                        st.markdown(f"Profitable: {no_stats.get('profitable_pct', 0):.0%}")
                        st.markdown(f"Avg PNL: ${no_stats.get('avg_pnl', 0):,.0f}")

                # Edit entry price
                st.markdown("---")
                st.markdown("### Edit Trade")
                new_entry = st.number_input(
                    "Entry Price",
                    min_value=0.01,
                    max_value=0.99,
                    value=float(selected_trade.get('entry_price', 0.5)),
                    step=0.01,
                    format="%.2f",
                    key=f"edit_entry_{selected_trade['id']}"
                )
                if abs(new_entry - selected_trade.get('entry_price', 0)) > 0.001:
                    if st.button("Save Entry Price"):
                        repo.update_trade_entry_price(selected_trade['id'], new_entry)
                        st.success("Entry price updated!")
                        st.rerun()
        else:
            st.info("Select a trade to view analysis")

    # Win Rate Analytics Section
    st.markdown("---")
    with st.expander("📊 Win Rate Analytics", expanded=True):
        if stats["wins"] + stats["losses"] == 0:
            st.info("Complete some trades to see analytics")
        else:
            # Summary metrics
            an_col1, an_col2, an_col3, an_col4 = st.columns(4)
            an_col1.metric("Total Closed", stats["wins"] + stats["losses"])
            an_col2.metric("Win Rate", f"{stats['win_rate']:.1%}")
            an_col3.metric("Avg Edge at Entry", f"{stats['avg_edge']:.1f}%" if stats["avg_edge"] else "N/A")
            an_col4.metric("Total PNL", f"${stats['total_pnl']:.2f}")

            # Win rate by edge level
            st.markdown("#### Win Rate by Edge Level")
            edge_data = repo.get_win_rate_by_edge()
            edge_with_data = [e for e in edge_data if e["total"] > 0]

            if edge_with_data:
                fig_edge = go.Figure()
                fig_edge.add_trace(go.Bar(
                    x=[e["edge_range"] for e in edge_with_data],
                    y=[e["win_rate"] for e in edge_with_data],
                    marker_color="#3B82F6",
                    text=[f"{e['win_rate']:.0%} ({e['wins']}/{e['total']})" for e in edge_with_data],
                    textposition='auto',
                ))
                fig_edge.update_layout(
                    template="plotly_dark",
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    height=250,
                    margin=dict(l=20, r=20, t=20, b=20),
                    yaxis_tickformat=".0%",
                    yaxis_title="Win Rate",
                    xaxis_title="Edge at Entry",
                    yaxis=dict(showgrid=True, gridcolor='rgba(54,57,69,0.4)', gridwidth=1, zeroline=False),
                    xaxis=dict(showgrid=False, zeroline=False),
                    hoverlabel=dict(bgcolor="#1a1c24", bordercolor="#363945", font=dict(color="#F3F4F6", family="IBM Plex Mono", size=12))
                )
                st.plotly_chart(fig_edge, use_container_width=True)
            else:
                st.caption("Not enough data for edge analysis")

            # Scanner prediction accuracy
            st.markdown("#### Scanner Prediction Accuracy")
            accuracy = repo.get_prediction_accuracy()

            acc_col1, acc_col2 = st.columns(2)
            with acc_col1:
                yes_pct = accuracy["yes_correct"]
                yes_total = accuracy["yes_total"]
                st.metric(
                    "When Scanner Flags YES",
                    f"{yes_pct:.1%}" if yes_total > 0 else "N/A",
                    help=f"Based on {yes_total} trades where you followed the YES recommendation"
                )
            with acc_col2:
                no_pct = accuracy["no_correct"]
                no_total = accuracy["no_total"]
                st.metric(
                    "When Scanner Flags NO",
                    f"{no_pct:.1%}" if no_total > 0 else "N/A",
                    help=f"Based on {no_total} trades where you followed the NO recommendation"
                )


def render_sidebar():
    """Render sidebar with controls and stats."""
    st.sidebar.markdown("### 📡 SCANNER CONTROL")

    if st.sidebar.button("RUN NEW SCAN", use_container_width=True, type="primary"):
        st.sidebar.info("⚠️ Scans must be run locally via terminal.")

    st.sidebar.caption("📍 **Hosted app**: View-only (scans disabled)")
    st.sidebar.code("python scripts/run_scan.py", language="bash")
    st.sidebar.markdown("---")

    # Filter Controls
    with st.sidebar.expander("🎯 FILTERS", expanded=False):
        max_days = st.slider("Max days to expiry", 1, 90, 14, key="filter_max_days")
        min_edge = st.slider("Min edge %", 50, 90, 60, key="filter_min_edge")
        min_liquidity = st.number_input("Min liquidity ($)", 100, 100000, 1000, step=500, key="filter_min_liquidity")
        st.markdown("**Price Range**")
        price_col1, price_col2 = st.columns(2)
        with price_col1:
            min_price = st.number_input("Min", 0.01, 0.99, 0.15, step=0.05, key="filter_min_price", format="%.2f")
        with price_col2:
            max_price = st.number_input("Max", 0.01, 0.99, 0.85, step=0.05, key="filter_max_price", format="%.2f")
        st.caption("Filters apply to Top Opportunities tab")

    # Score Weights
    with st.sidebar.expander("⚖️ SCORE WEIGHTS", expanded=False):
        st.caption("Adjust how each factor contributes to the opportunity score")
        weight_edge = st.slider("Edge strength", 0, 100, 50, key="weight_edge", help="Higher edge = higher win probability")
        weight_sample = st.slider("Sample size (min N)", 0, 100, 30, key="weight_sample", help="More holders on both sides = more confidence")
        weight_pnl = st.slider("PNL conviction", 0, 100, 10, key="weight_pnl", help="Experienced traders backing one side")
        weight_quality = st.slider("Data quality", 0, 100, 10, key="weight_quality", help="Lower unknown % = more confident")
        st.caption("Score = Edge×W₁ + Sample×W₂ + PNL×W₃ + Quality×W₄")

    st.sidebar.markdown("---")

    # Stats
    try:
        repo = get_repository()
        stats = repo.get_stats()

        st.sidebar.markdown("### 📊 DATABASE STATS")
        c1, c2 = st.sidebar.columns(2)
        c1.metric("Sessions", stats["total_sessions"])
        c2.metric("Markets", stats["unique_markets_scanned"])
        
        c3, c4 = st.sidebar.columns(2)
        c3.metric("Flagged", stats["total_flagged"])

        # Last scan time
        sessions = repo.get_recent_sessions(limit=1)
        if sessions:
            latest = sessions[0]
            completed_at = latest.get("completed_at")
            if completed_at:
                last_scan_time = datetime.fromtimestamp(completed_at)
                time_ago = datetime.now() - last_scan_time
                if time_ago.days > 0:
                    time_str = f"{time_ago.days}d ago"
                elif time_ago.seconds // 3600 > 0:
                    time_str = f"{time_ago.seconds // 3600}h ago"
                else:
                    minutes = time_ago.seconds // 60
                    time_str = f"{minutes}m ago" if minutes > 0 else "just now"
                c4.metric("Last Scan", time_str)

        # Progress
        latest_session_id = repo.get_latest_session_id()
        if latest_session_id:
            sessions = repo.get_recent_sessions(limit=1)
            if sessions:
                s = sessions[0]
                if s.get("status") == "running" and s.get("total_markets", 0) > 0:
                    # New field scanned_count is preferred if available
                    scanned_val = s.get("scanned_count")
                    if scanned_val is not None:
                         completed_count = scanned_val
                    else:
                        # Fallback for old sessions
                        completed_count = repo._get_conn().execute(
                            "SELECT COUNT(*) FROM scan_results WHERE session_id = ?", 
                            (latest_session_id,)
                        ).fetchone()[0]
                    
                    total = s.get("total_markets")
                    pct = completed_count / total if total else 0
                    st.sidebar.progress(pct, text=f"{completed_count}/{total} Markets")
                    
    except Exception as e:
        st.sidebar.error(f"DB Error: {e}")


def render_market_detail_view(data, repo=None, key_prefix=""):
    """Render detailed interactive analysis for a selected market."""
    # Generate unique key based on market_id and prefix
    market_key = f"{key_prefix}_{data.get('market_id', 'unknown')}"

    # Market Header
    st.markdown(f"<div class='terminal-header'>ANALYSIS :: {data.get('question')}</div>", unsafe_allow_html=True)
    
    # Top Metrics Row
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("VOLUME", f"${data.get('volume', 0):,.0f}")
    m2.metric("LIQUIDITY", f"${data.get('liquidity', 0):,.0f}")
    
    hours = calculate_hours_remaining(data.get('end_date'))
    m3.metric("EXPIRES", format_time_remaining(hours))
    
    if data.get('slug'):
        m4.link_button("↗ OPEN MARKET", f"https://polymarket.com/event/{data.get('slug')}", use_container_width=True)

    st.divider()

    # Layout: Charts on Left (2/3), Detailed Stats on Right (1/3)
    c_left, c_right = st.columns([2, 1])

    with c_left:
        # Comparison Chart
        sides = ["YES", "NO"]
        prof_pct = [data.get("yes_profitable_pct", 0), data.get("no_profitable_pct", 0)]
        avg_pnl = [data.get("yes_avg_overall_pnl", 0), data.get("no_avg_overall_pnl", 0)]
        
        # 1. Profitable Traders
        fig_prof = go.Figure()
        fig_prof.add_trace(go.Bar(
            name="Profitable %",
            x=sides,
            y=prof_pct,
            marker_color=["#00C076", "#FF4F4F"], 
            text=[f"{p:.1%}" for p in prof_pct],
            textposition='auto',
        ))
        fig_prof.add_hline(
            y=IMBALANCE_THRESHOLD,
            line_dash="dot",
            line_color="orange",
            annotation_text=f"Thr {IMBALANCE_THRESHOLD:.0%}",
        )
        fig_prof.update_layout(
            title="Whale Profitability (Top Holders)",
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
            yaxis_tickformat=".0%",
            yaxis=dict(showgrid=True, gridcolor='rgba(54,57,69,0.4)', gridwidth=1, zeroline=False),
            xaxis=dict(showgrid=False, zeroline=False),
            hoverlabel=dict(bgcolor="#1a1c24", bordercolor="#363945", font=dict(color="#F3F4F6", family="IBM Plex Mono", size=12))
        )
        st.plotly_chart(fig_prof, use_container_width=True, key=f"fig_prof_{market_key}")
        
        # 2. Avg Realized PNL
        fig_pnl = go.Figure()
        fig_pnl.add_trace(go.Bar(
            name="Avg Realized PNL",
            x=sides,
            y=avg_pnl,
            marker_color="#3B82F6",
            text=[f"${p:,.0f}" for p in avg_pnl],
            textposition='auto',
        ))
        fig_pnl.update_layout(
            title="Avg Whale Realized PNL ($)",
            template="plotly_dark",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
            yaxis=dict(showgrid=True, gridcolor='rgba(54,57,69,0.4)', gridwidth=1, zeroline=False),
            xaxis=dict(showgrid=False, zeroline=False),
            hoverlabel=dict(bgcolor="#1a1c24", bordercolor="#363945", font=dict(color="#F3F4F6", family="IBM Plex Mono", size=12))
        )
        st.plotly_chart(fig_pnl, use_container_width=True, key=f"fig_pnl_{market_key}")

    with c_right:
        yes_price = data.get('current_yes_price', 0)
        no_price = data.get('current_no_price', 0)
        yes_prof_pct = data.get('yes_profitable_pct', 0)
        no_prof_pct = data.get('no_profitable_pct', 0)

        st.markdown("#### YES SIDE")
        st.markdown(f"**Price:** `{yes_price:.1%}`")
        st.markdown(f"**Holders Analyzed:** `{data.get('yes_top_n_count', 0)}`")
        st.markdown(f"**Profitable:** `{data.get('yes_profitable_count', 0)}` ({yes_prof_pct:.0%})")
        st.markdown(f"**Avg Realized PNL:** `${data.get('yes_avg_overall_pnl', 0):,.0f}`")

        st.divider()

        st.markdown("#### NO SIDE")
        st.markdown(f"**Price:** `{no_price:.1%}`")
        st.markdown(f"**Holders Analyzed:** `{data.get('no_top_n_count', 0)}`")
        st.markdown(f"**Profitable:** `{data.get('no_profitable_count', 0)}` ({no_prof_pct:.0%})")
        st.markdown(f"**Avg Realized PNL:** `${data.get('no_avg_overall_pnl', 0):,.0f}`")

        # Add to Portfolio Section
        st.divider()
        st.markdown("#### ADD TO PORTFOLIO")

        # Determine default side based on flagged_side
        flagged_side = data.get("flagged_side", "YES")
        default_idx = 0 if flagged_side == "YES" else 1

        trade_side = st.radio(
            "Side",
            ["YES", "NO"],
            index=default_idx,
            horizontal=True,
            key=f"trade_side_{key_prefix}_{data.get('market_id')}"
        )

        # Entry price based on selected side
        entry_price = yes_price if trade_side == "YES" else no_price
        st.markdown(f"**Entry Price:** `${entry_price:.2f}`")

        # Calculate edge
        edge_pct = abs(yes_prof_pct - no_prof_pct) * 100

        if st.button("📥 Add Trade", key=f"add_trade_{key_prefix}_{data.get('market_id')}", use_container_width=True):
            if repo:
                repo.add_trade(
                    market_id=data.get("market_id"),
                    condition_id=data.get("condition_id", ""),
                    question=data.get("question", ""),
                    slug=data.get("slug", ""),
                    side=trade_side,
                    entry_price=entry_price,
                    flagged_side=flagged_side,
                    edge_pct=edge_pct,
                    score=data.get("imbalance_score"),
                    scan_result_id=data.get("id"),
                )
                st.success(f"Added {trade_side} trade to portfolio!")
                st.rerun()

    # Historical Trend Charts
    if repo:
        market_id = data.get("market_id")
        if market_id:
            history = repo.get_market_history(market_id)
            if len(history) >= 2:
                # Sort by scanned_at ascending for chronological order
                history = sorted(history, key=lambda x: x.get("scanned_at", 0))

                timestamps = [datetime.fromtimestamp(h.get("scanned_at", 0)) for h in history]
                yes_prof = [h.get("yes_profitable_pct", 0) for h in history]
                no_prof = [h.get("no_profitable_pct", 0) for h in history]
                yes_prices = [h.get("current_yes_price", 0) for h in history]
                no_prices = [h.get("current_no_price", 0) for h in history]

                # Profitability Trend Chart
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=timestamps,
                    y=yes_prof,
                    mode='lines+markers',
                    name='YES Profitable %',
                    line=dict(color='#00C076', width=2),
                    marker=dict(size=6)
                ))
                fig_trend.add_trace(go.Scatter(
                    x=timestamps,
                    y=no_prof,
                    mode='lines+markers',
                    name='NO Profitable %',
                    line=dict(color='#FF4F4F', width=2),
                    marker=dict(size=6)
                ))
                fig_trend.add_hline(
                    y=IMBALANCE_THRESHOLD,
                    line_dash="dot",
                    line_color="orange",
                    annotation_text=f"Threshold {IMBALANCE_THRESHOLD:.0%}",
                )
                fig_trend.update_layout(
                    title="Profitability Trend Over Time",
                    template="plotly_dark",
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    height=250,
                    margin=dict(l=20, r=20, t=40, b=20),
                    yaxis_tickformat=".0%",
                    xaxis_title="",
                    yaxis_title="Profitable %",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    xaxis=dict(showgrid=True, gridcolor='rgba(54,57,69,0.4)', gridwidth=1, zeroline=False),
                    yaxis=dict(showgrid=True, gridcolor='rgba(54,57,69,0.4)', gridwidth=1, zeroline=False),
                    hoverlabel=dict(bgcolor="#1a1c24", bordercolor="#363945", font=dict(color="#F3F4F6", family="IBM Plex Mono", size=12))
                )
                st.plotly_chart(fig_trend, use_container_width=True, key=f"fig_trend_{market_key}")

                # Price History Chart
                fig_price = go.Figure()
                fig_price.add_trace(go.Scatter(
                    x=timestamps,
                    y=yes_prices,
                    mode='lines+markers',
                    name='YES Price',
                    line=dict(color='#00C076', width=2),
                    marker=dict(size=6)
                ))
                fig_price.add_trace(go.Scatter(
                    x=timestamps,
                    y=no_prices,
                    mode='lines+markers',
                    name='NO Price',
                    line=dict(color='#FF4F4F', width=2),
                    marker=dict(size=6)
                ))
                fig_price.update_layout(
                    title="Price History Over Time",
                    template="plotly_dark",
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    height=250,
                    margin=dict(l=20, r=20, t=40, b=20),
                    yaxis_tickformat="$.2f",
                    yaxis_range=[0, 1],
                    xaxis_title="",
                    yaxis_title="Share Price",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    xaxis=dict(showgrid=True, gridcolor='rgba(54,57,69,0.4)', gridwidth=1, zeroline=False),
                    yaxis=dict(showgrid=True, gridcolor='rgba(54,57,69,0.4)', gridwidth=1, zeroline=False),
                    hoverlabel=dict(bgcolor="#1a1c24", bordercolor="#363945", font=dict(color="#F3F4F6", family="IBM Plex Mono", size=12))
                )
                st.plotly_chart(fig_price, use_container_width=True, key=f"fig_price_{market_key}")
            else:
                st.caption("📈 Trend data available after multiple scans")


def render_dashboard(repo):
    """Main dashboard layout with split view."""

    # 1. Global Search (searches ALL markets, not just filtered ones)
    search_col1, search_col2 = st.columns([4, 1])
    with search_col1:
        global_search = st.text_input(
            "🔍 Search ALL Markets",
            placeholder="Search any market by name (e.g., 'Bitcoin', 'Trump', 'Tesla')...",
            label_visibility="collapsed",
            key="global_search"
        )
    with search_col2:
        show_all = st.checkbox("Include filtered", value=False, help="Show markets outside price range (0.15-0.85) and expired")

    # If searching, show search results instead of filtered list
    if global_search and len(global_search) >= 2:
        search_results = repo.search_markets(global_search, limit=100)
        if search_results:
            st.caption(f"Found {len(search_results)} markets matching '{global_search}'")

            # Process search results
            processed_data = []
            for r in search_results:
                yes_price = r.get("current_yes_price", 0)
                no_price = r.get("current_no_price", 0)
                hours_rem = calculate_hours_remaining(r.get("end_date"))

                yes_prof_pct = r.get("yes_profitable_pct", 0)
                no_prof_pct = r.get("no_profitable_pct", 0)
                imbalance_pct = abs(yes_prof_pct - no_prof_pct) * 100
                yes_avg_pnl = r.get("yes_avg_overall_pnl") or 0
                no_avg_pnl = r.get("no_avg_overall_pnl") or 0
                pnl_diff = abs(yes_avg_pnl - no_avg_pnl)

                processed_data.append({
                    "Market": r.get("question", ""),
                    "Score": 0,
                    "Imbalance": imbalance_pct,
                    "PNL Diff": pnl_diff,
                    "Expires": hours_rem,
                    "market_id": r.get("market_id"),
                    "raw_data": r,
                    "price_yes": yes_price,
                    "price_no": no_price,
                    "is_flagged": r.get("is_flagged", 0)
                })

            df = pd.DataFrame(processed_data)
            _render_market_list_and_detail(df, repo, is_search=True)
            return
        else:
            st.warning(f"No markets found matching '{global_search}'")
            return

    # 2. Fetch Data (normal filtered view)
    latest_session = repo.get_latest_session_id()
    if not latest_session:
        st.info("Waiting for data...")
        return

    results = repo.get_all_results(session_id=latest_session, limit=5000)
    if not results:
        st.info("Scanning markets...")
        return

    # 3. Process & Filter
    processed_data = []
    for r in results:
        yes_price = r.get("current_yes_price", 0)
        no_price = r.get("current_no_price", 0)

        # Filter Price 0.15-0.85 (unless show_all is checked)
        if not show_all:
            if not ((0.15 <= yes_price <= 0.85) or (0.15 <= no_price <= 0.85)):
                continue

            # Filter Expired
            hours_rem = calculate_hours_remaining(r.get("end_date"))
            if hours_rem <= 0:
                continue
        else:
            hours_rem = calculate_hours_remaining(r.get("end_date"))

        # Calc Score
        yes_prof_pct = r.get("yes_profitable_pct", 0)
        no_prof_pct = r.get("no_profitable_pct", 0)
        imbalance_pct = abs(yes_prof_pct - no_prof_pct) * 100  # Convert to 0-100 scale
        yes_avg_pnl = r.get("yes_avg_overall_pnl") or 0
        no_avg_pnl = r.get("no_avg_overall_pnl") or 0
        pnl_diff = abs(yes_avg_pnl - no_avg_pnl)

        # Heuristic Score (imbalance_pct is already 0-100)
        time_factor = 100 / (hours_rem + 1) if hours_rem > 0 else 0
        imbalance_factor = imbalance_pct  # Already in 0-100 scale
        pnl_factor = min(pnl_diff / 1000, 50)
        score = (time_factor * 2.0) + (imbalance_factor * 1.5) + (pnl_factor * 1.0)

        processed_data.append({
            "Market": r.get("question", ""),
            "Score": score,
            "Imbalance": imbalance_pct,
            "PNL Diff": pnl_diff,
            "Expires": hours_rem,
            "market_id": r.get("market_id"),
            "raw_data": r,
            "price_yes": yes_price,
            "price_no": no_price
        })

    df = pd.DataFrame(processed_data)
    _render_market_list_and_detail(df, repo, is_search=False)


def _render_market_list_and_detail(df, repo, is_search=False):
    """Render the market list and detail panels."""
    # Layout: Side-by-Side (Table Left, Analysis Right)
    col_list, col_detail = st.columns([1.5, 1])

    with col_list:
        st.markdown("<div class='terminal-header'>MARKET OPPORTUNITIES</div>", unsafe_allow_html=True)

        if df.empty:
            st.info("No markets found.")
        else:
            # Initialize sort state
            if "sort_column" not in st.session_state:
                st.session_state["sort_column"] = "Score"
                st.session_state["sort_ascending"] = False

            # Column headers with sort buttons
            h1, h2, h3, h4 = st.columns([3, 1, 1, 1])

            def sort_indicator(col_name):
                if st.session_state["sort_column"] == col_name:
                    return " ↓" if not st.session_state["sort_ascending"] else " ↑"
                return ""

            with h1:
                if st.button(f"Market{sort_indicator('Market')}", key="sort_market", use_container_width=True):
                    if st.session_state["sort_column"] == "Market":
                        st.session_state["sort_ascending"] = not st.session_state["sort_ascending"]
                    else:
                        st.session_state["sort_column"] = "Market"
                        st.session_state["sort_ascending"] = True
                    st.rerun()
            with h2:
                if st.button(f"Imbal.{sort_indicator('Imbalance')}", key="sort_imbal", use_container_width=True):
                    if st.session_state["sort_column"] == "Imbalance":
                        st.session_state["sort_ascending"] = not st.session_state["sort_ascending"]
                    else:
                        st.session_state["sort_column"] = "Imbalance"
                        st.session_state["sort_ascending"] = False
                    st.rerun()
            with h3:
                if st.button(f"PNL Δ{sort_indicator('PNL Diff')}", key="sort_pnl", use_container_width=True):
                    if st.session_state["sort_column"] == "PNL Diff":
                        st.session_state["sort_ascending"] = not st.session_state["sort_ascending"]
                    else:
                        st.session_state["sort_column"] = "PNL Diff"
                        st.session_state["sort_ascending"] = False
                    st.rerun()
            with h4:
                if st.button(f"Exp.{sort_indicator('Expires')}", key="sort_exp", use_container_width=True):
                    if st.session_state["sort_column"] == "Expires":
                        st.session_state["sort_ascending"] = not st.session_state["sort_ascending"]
                    else:
                        st.session_state["sort_column"] = "Expires"
                        st.session_state["sort_ascending"] = True
                    st.rerun()

            # Sort and prepare data
            sort_col = st.session_state["sort_column"]
            sort_asc = st.session_state["sort_ascending"]
            table_df = df.sort_values(by=sort_col, ascending=sort_asc).head(100).reset_index(drop=True)

            # Create scrollable container with clickable market list
            with st.container(height=650):
                for idx, row in table_df.iterrows():
                    imbal = row["Imbalance"]
                    pnl_diff = row["PNL Diff"]
                    time_str = format_time_remaining(row["Expires"])
                    market_name = row["Market"][:70] + "..." if len(row["Market"]) > 70 else row["Market"]

                    # Check if this market is selected
                    is_selected = st.session_state.get("selected_market_id") == row["market_id"]

                    # Market row with columns
                    c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
                    with c1:
                        if st.button(
                            market_name,
                            key=f"market_{idx}",
                            use_container_width=True,
                            type="primary" if is_selected else "secondary"
                        ):
                            st.session_state["selected_market_id"] = row["market_id"]
                            st.session_state["selected_raw_data"] = row["raw_data"]
                            st.rerun()
                    with c2:
                        st.markdown(f"<div style='text-align:center;padding:8px;'>{imbal:.0f}%</div>", unsafe_allow_html=True)
                    with c3:
                        st.markdown(f"<div style='text-align:center;padding:8px;'>${pnl_diff:,.0f}</div>", unsafe_allow_html=True)
                    with c4:
                        st.markdown(f"<div style='text-align:center;padding:8px;'>{time_str}</div>", unsafe_allow_html=True)

    # 5. Analysis Panel (Right Side)
    with col_detail:
        selected_data = None

        # Get selected market data
        if "selected_raw_data" in st.session_state:
            selected_data = st.session_state["selected_raw_data"]
        elif not df.empty:
            # Default to first market
            selected_data = df.iloc[0]["raw_data"]
            st.session_state["selected_market_id"] = df.iloc[0]["market_id"]
            st.session_state["selected_raw_data"] = selected_data

        if selected_data is not None:
            render_market_detail_view(selected_data, repo=repo, key_prefix="all")
        else:
            st.info("Select a market to view analysis.")


def main():
    try:
        repo = get_repository()
    except Exception as e:
        st.error(f"Database error: {e}")
        return

    render_sidebar()

    # Tab navigation
    tab_opportunities, tab_portfolio, tab_all = st.tabs([
        "🎯 Top Opportunities",
        "💼 Portfolio",
        "📊 All Markets"
    ])

    with tab_opportunities:
        render_opportunities_tab(repo)

    with tab_portfolio:
        render_portfolio_tab(repo)

    with tab_all:
        render_dashboard(repo)

if __name__ == "__main__":
    main()
