"""Streamlit dashboard for PNL Imbalance Scanner."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
from pathlib import Path
import math

sys.path.insert(0, str(Path(__file__).parent))

from src.db.repository import ScannerRepository
from src.config.settings import DEFAULT_DB_PATH, IMBALANCE_THRESHOLD

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

def render_sidebar():
    """Render sidebar with controls and stats."""
    st.sidebar.markdown("### 📡 SCANNER CONTROL")
    
    if st.sidebar.button("RUN NEW SCAN", use_container_width=True, type="primary"):
        # We can't trigger the python script directly from button without subprocess/rerun complexity,
        # but displaying the command is what we did before.
        # Ideally this would trigger a background thread.
        pass
        
    st.sidebar.caption("Run in terminal: `python scripts/run_scan.py`")
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
        
        # Progress
        latest_session_id = repo.get_latest_session_id()
        if latest_session_id:
            sessions = repo.get_recent_sessions(limit=1)
            if sessions:
                s = sessions[0]
                if s.get("status") == "running" and s.get("total_markets", 0) > 0:
                    completed_count = repo._get_conn().execute(
                        "SELECT COUNT(*) FROM scan_results WHERE session_id = ?", 
                        (latest_session_id,)
                    ).fetchone()[0]
                    total = s.get("total_markets")
                    pct = completed_count / total if total else 0
                    st.sidebar.progress(pct, text=f"{completed_count}/{total} Markets")
                    
    except Exception as e:
        st.sidebar.error(f"DB Error: {e}")


def render_market_detail_view(data):
    """Render detailed interactive analysis for a selected market."""
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
            yaxis_tickformat=".0%"
        )
        st.plotly_chart(fig_prof, use_container_width=True)
        
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
        )
        st.plotly_chart(fig_pnl, use_container_width=True)

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


def render_dashboard(repo):
    """Main dashboard layout with split view."""
    
    # 1. Fetch Data
    latest_session = repo.get_latest_session_id()
    if not latest_session:
        st.info("Waiting for data...")
        return

    results = repo.get_all_results(session_id=latest_session, limit=5000)
    if not results:
        st.info("Scanning markets...")
        return

    # 2. Process & Filter
    processed_data = []
    for r in results:
        yes_price = r.get("current_yes_price", 0)
        no_price = r.get("current_no_price", 0)
        
        # Filter Price 0.15-0.85
        if not ((0.15 <= yes_price <= 0.85) or (0.15 <= no_price <= 0.85)):
            continue

        # Filter Expired
        hours_rem = calculate_hours_remaining(r.get("end_date"))
        if hours_rem <= 0:
            continue

        # Calc Score
        yes_prof_pct = r.get("yes_profitable_pct", 0)
        no_prof_pct = r.get("no_profitable_pct", 0)
        imbalance_pct = abs(yes_prof_pct - no_prof_pct) * 100  # Convert to 0-100 scale
        yes_avg_pnl = r.get("yes_avg_overall_pnl") or 0
        no_avg_pnl = r.get("no_avg_overall_pnl") or 0
        pnl_diff = abs(yes_avg_pnl - no_avg_pnl)
        
        # Heuristic Score (imbalance_pct is already 0-100)
        time_factor = 100 / (hours_rem + 1)
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

    # 3. Search Bar
    search_query = st.text_input("🔍 Search Markets", placeholder="Type to filter...", label_visibility="collapsed")

    if not df.empty:
        if search_query:
            df = df[df["Market"].str.lower().str.contains(search_query.lower(), na=False)]
        # Default sort by Score (highest first)
        df = df.sort_values(by="Score", ascending=False).head(100)

    # 4. Layout: Side-by-Side (Table Left, Analysis Right)
    col_list, col_detail = st.columns([1.5, 1])

    with col_list:
        st.markdown("<div class='terminal-header'>MARKET OPPORTUNITIES</div>", unsafe_allow_html=True)

        if df.empty:
            st.info("No markets found.")
        else:
            # Prepare table data
            table_df = df.copy().reset_index(drop=True)

            # Create scrollable container with clickable market list
            with st.container(height=700):
                for idx, row in table_df.iterrows():
                    imbal = row["Imbalance"]
                    pnl_diff = row["PNL Diff"]
                    time_str = format_time_remaining(row["Expires"])
                    market_name = row["Market"][:80] + "..." if len(row["Market"]) > 80 else row["Market"]

                    # Check if this market is selected
                    is_selected = st.session_state.get("selected_market_id") == row["market_id"]

                    # Create clickable button for each market
                    btn_label = f"**{market_name}**\n\n`Imbal: {imbal:.0f}%` · `PNL Δ: ${pnl_diff:,.0f}` · `Exp: {time_str}`"

                    if st.button(
                        btn_label,
                        key=f"market_{idx}",
                        use_container_width=True,
                        type="primary" if is_selected else "secondary"
                    ):
                        st.session_state["selected_market_id"] = row["market_id"]
                        st.session_state["selected_raw_data"] = row["raw_data"]
                        st.rerun()

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
            render_market_detail_view(selected_data)
        else:
            st.info("Select a market to view analysis.")


def main():
    try:
        repo = get_repository()
    except Exception as e:
        st.error(f"Database error: {e}")
        return

    render_sidebar()
    render_dashboard(repo)

if __name__ == "__main__":
    main()
