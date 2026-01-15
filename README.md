# Polymarket Top Holder PNL Analysis

A scanner that analyzes Polymarket prediction markets by examining the profit/loss distribution of top token holders. Identifies markets where profitable traders are disproportionately concentrated on one side (YES or NO), indicating potential "smart money" signals.

## Live Dashboard

**[View Dashboard](https://polyholderprofitanalysis-j4j5r2yypgq6qh45fpkzvi.streamlit.app/)**

## Features

- **Scans all active Polymarket markets** - Fetches data from Gamma API
- **Analyzes top holders per side** - Gets YES/NO token holders from Data API
- **Calculates realized PNL** - Uses positions API to determine trader profitability
- **Flags imbalanced markets** - Markets where 60%+ of holders on one side are profitable
- **Interactive dashboard** - Sort, filter, and analyze individual markets

## How It Works

1. Fetches all active markets from Polymarket
2. For each market, retrieves the top holders of YES and NO tokens
3. Looks up each holder's realized PNL from their trading history
4. Calculates the percentage of profitable traders on each side
5. Flags markets where one side has significantly more profitable traders

## Scoring Logic

A market is flagged when:
- One side has **60%+ profitable traders** (by realized PNL)
- That side has **higher average PNL** than the other side
- That side has **positive average PNL** (not just less negative)

## Local Setup

```bash
# Clone the repo
git clone https://github.com/FinkBig/poly_holder_profit_analysis.git
cd poly_holder_profit_analysis

# Install dependencies
pip install -r requirements.txt

# Run a scan
python scripts/run_scan.py --min-liquidity 5000

# Launch dashboard
streamlit run dashboard.py
```

## Project Structure

```
├── dashboard.py              # Streamlit dashboard
├── scripts/
│   ├── run_scan.py          # Main scan script
│   └── test_pipeline.py     # Integration tests
└── src/
    ├── fetchers/            # API fetchers (markets, holders, PNL)
    ├── analysis/            # Imbalance calculation logic
    ├── models/              # Data models
    ├── db/                  # SQLite database layer
    └── config/              # Settings and constants
```

## API Sources

- **Gamma API** - Market metadata (prices, volume, liquidity)
- **Data API** - Token holders per market
- **Positions API** - Trader PNL from position history

## Disclaimer

This tool is for informational purposes only. Past performance of traders does not guarantee future results. Always do your own research before making any trading decisions.

---

Built with Claude Code
