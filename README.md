# Preferred Equity Analysis Swarm

**MSBA Capstone Project**

A multi-agent AI swarm system that analyzes preferred equity securities using LangGraph and Google Gemini. The swarm coordinates specialized agents to evaluate credit risk, interest rate sensitivity, call probability, tax treatment, regulatory exposure, and relative value across the preferred securities universe while combining live Alpha Vantage market data, FRED-backed rates, and local SEC prospectus caches.

## Project Structure

```
preferred-equity-swarm/
├── src/
│   ├── agents/          # LangGraph agent definitions (prospectus_agent, advanced_swarm)
│   ├── data/            # Data pipeline modules (edgar_pipeline, market_data, rate_data)
│   └── utils/           # Shared utilities (config)
├── streamlit_app/       # Streamlit demo interface
├── tests/               # Unit and integration tests
├── notebooks/           # Jupyter notebooks for exploration
├── docs/                # Project documentation and architectural walkthroughs
├── data/
│   ├── edgar_cache/     # Local cache for SEC filings to reduce API calls
│   ├── prospectus_terms/# Extracted and structured prospectus data (demo/runtime)
│   └── preferred_filing_registry.json # Curated registry of known preferred filings
└── requirements.txt     # Python dependencies
```

## Technology Stack

| Component | Technology |
|---|---|
| Agent Orchestration | LangGraph |
| LLM Backend | Google Gemini (`gemini-2.5-flash`) via `langchain-google-genai` |
| SEC Filings | SEC EDGAR EFTS and Submissions APIs |
| Market Data | Alpha Vantage |
| Rate Data | FRED API with local snapshot fallback |
| Symbology Resolution | Local preferred universe, runtime prospectus cache, Alpha Vantage listing status/search |
| Demo UI | Streamlit |
| Visualization | Plotly |

## Setup

```bash
# Clone the repository
git clone https://github.com/jkriertran/preferred-equity-swarm.git
cd preferred-equity-swarm

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env and add your GOOGLE_API_KEY and ALPHA_VANTAGE_API_KEY
```

Recommended `.env` keys:

```env
GOOGLE_API_KEY=your_google_api_key_here
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key_here
FRED_API_KEY=your_fred_api_key_here
MARKET_DATA_PROVIDER=alpha_vantage

# Optional: point to Alpha Vantage's official listing_status.csv file
ALPHA_VANTAGE_LISTING_STATUS_PATH=/absolute/path/to/listing_status.csv
```

If `ALPHA_VANTAGE_LISTING_STATUS_PATH` is not set, the app will still try to auto-discover a local `listing_status.csv` file.

## Running the Application

To run the interactive Streamlit dashboard:

```bash
cd preferred-equity-swarm
streamlit run streamlit_app/app.py
```

Ticker input notes:

- The app's internal canonical format is `ISSUER-PSERIES`, for example `ALL-PH` or `C-PN`.
- Alpha-style aliases such as `ALL-P-H`, `C-P-N`, and `ADC-P-A` are also accepted at the UI boundary.
- Local prospectus/runtime cache remains the source of truth for preferred structure terms, while provider-specific symbols are resolved inside the market-data adapter.

## Current Status: Phase 3 Completed

The project has successfully completed Phase 3, with the current production graph running 11 agent nodes across 6 cache-aware layers.

### Completed Capabilities

**Layer 1: Early Context**
*   **Prospectus Parsing Agent:** A staged extraction pipeline that uses deterministic parsing for standard terms and falls back to Gemini for complex legal language.
*   **Rate Context Agent:** Pulls live Treasury yield curves and SOFR benchmark rates from FRED, with local snapshot fallback.

**Layer 2: Security Enrichment**
*   **Market Data Agent:** Fetches live pricing from Alpha Vantage and derives preferred-specific yield fields from local structure data when provider dividend fields are incomplete.
*   **Dividend Analysis Agent:** Computes dividend consistency, payment frequency, and trailing yield from a mix of Alpha Vantage dividend data and cached prospectus terms.

**Layer 3: Deterministic Analysis**
*   **Interest Rate Sensitivity Agent:** Computes duration, DV01, and handles the transition from LIBOR to SOFR for legacy floating-rate securities.

**Layer 4: Parallel Analytical Agents**
*   **Call Probability Agent:** Estimates yield-to-call, yield-to-worst, and heuristic call probability based on refinancing incentives and premium to par.
*   **Tax and Yield Agent:** Classifies Qualified Dividend Income (QDI) eligibility and computes tax-equivalent yields.
*   **Regulatory and Sector Agent:** Assesses Basel III/IV AT1 capital treatment, G-SIB surcharges, and dividend deferral risk.
*   **Relative Value Agent:** Ranks the security against peers by yield, spread to Treasury, and structure.

**Layer 5: Quality Gate**
*   **Quality Gate:** Evaluates the outputs of all 8 upstream agents to determine if the data is sufficient for synthesis.

**Layer 6: Conditional Routing**
*   **Synthesis Agent:** Synthesizes the outputs into a professional, institutional-grade research note.
*   **Error Report Agent:** Generates a structured diagnostic report if the quality gate fails.

**User Interface**
*   A polished Streamlit dashboard that visualizes yield curves, price history, benchmark context, and the outputs of the full Phase 3 analytical graph.

## Data Design Notes

- The app no longer depends on `yfinance` or Twelve Data for live market data.
- Alpha Vantage is treated as a quote/history provider, not the authority on preferred security identity.
- The preferred resolver checks the curated universe and local prospectus/runtime cache first, then uses Alpha Vantage as supporting evidence for unresolved live lookups.
- For issues where Alpha symbology is inconsistent, the app supports local `provider_symbols.alpha_vantage` overrides and can also use Alpha's official `listing_status.csv` reference file.

## Next Phase: Phase 4 (Orchestration & Refinement)

The upcoming final phase will focus on:
1.  **Orchestrator Agent:** Adding a supervisor node for workflow management and conflict resolution.
2.  **Portfolio Analysis:** Expanding the swarm to analyze multiple securities simultaneously.
3.  **Final Polish:** Optimizing prompts, token usage, and UI presentation for the final Capstone deliverable.
