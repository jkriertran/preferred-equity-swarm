# Preferred Equity Analysis Swarm

**MSBA Capstone Project**

A multi-agent AI swarm system that analyzes preferred equity securities using LangGraph and Google Gemini. The swarm coordinates eight specialized agents to evaluate credit risk, interest rate sensitivity, call probability, tax treatment, regulatory exposure, and relative value across the preferred securities universe.

## Project Structure

```
preferred-equity-swarm/
├── src/
│   ├── agents/          # LangGraph agent definitions
│   ├── data/            # Data pipeline modules (SEC EDGAR, Yahoo Finance, FRED)
│   └── utils/           # Shared utilities (ticker normalization, config)
├── streamlit_app/       # Streamlit demo interface
├── tests/               # Unit and integration tests
├── notebooks/           # Jupyter notebooks for exploration
├── docs/                # Project documentation
├── data/
│   ├── raw/             # Raw data files (prospectuses, filings)
│   ├── processed/       # Processed/structured data
│   └── universe/        # Preferred securities universe database
└── requirements.txt     # Python dependencies
```

## Technology Stack

| Component | Technology |
|---|---|
| Agent Orchestration | LangGraph |
| LLM Backend | Google Gemini |
| SEC Filings | SEC EDGAR API + sec-downloader |
| Market Data | yfinance |
| Rate Data | FRED API (fredapi) |
| Demo UI | Streamlit |
| Visualization | Plotly |

## Setup

```bash
pip install langgraph langchain langchain-google-genai langchain-community yfinance fredapi streamlit plotly sec-downloader python-dotenv
```

## Status

Phase 0: Foundation and Learning (In Progress)
