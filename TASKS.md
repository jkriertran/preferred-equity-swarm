# Tasks

This file is the persistent project task tracker for the repo.

## Active

- [ ] Validate live Alpha Vantage symbol coverage for a broader sample of preferred tickers and add explicit `provider_symbols.alpha_vantage` overrides where Alpha symbology is inconsistent.
- [ ] Validate the new EDGAR resolution cache behavior on a few uncached live tickers and seed committed registry entries where repeated SEC lookups are still too expensive.
- [ ] If paying later becomes acceptable, evaluate an `EODHD + FRED` adapter design.

## Completed

- [x] Fast-forward local `master` to match `origin/master`.
- [x] Review incoming GitHub changes and identify resolver validation issues.
- [x] Patch ticker normalization and Yahoo fallback validation in the security resolver.
- [x] Add targeted regression coverage for the resolver.
- [x] Inspect the `symbology-us-formats` repo and compare its preferred-symbol conventions to this app.
- [x] Review Public.com API docs and determine that it is not a clean fit for the public Streamlit app.
- [x] Research affordable and free alternatives to `yfinance` with deployment/licensing considerations.
- [x] Evaluate `datalab-to/marker` and determine that it is better suited as an optional PDF ingestion tool than as the core EDGAR prospectus parser for this repo.
- [x] Verify current Twelve Data Basic pricing and usage terms.
- [x] Decide to use Twelve Data Basic only for local/private development, not for public third-party display.
- [x] Integrate a Twelve Data local/private adapter for preferred quote/history lookup, cached dividend derivation, and live resolver validation.
- [x] Add `TWELVE_DATA_API_KEY` to local environment config and confirm the app is running in Twelve Data mode locally.
- [x] Fix local Streamlit startup so missing `secrets.toml` falls back cleanly to `.env` configuration.
- [x] Add local registry/cache support for `ALB-PA` so prospectus, dividend, and tax analysis can run without falling through to empty metadata.
- [x] Fix runtime prospectus cache discovery so accession-named files like `ALL-PI` feed the resolver, market-data, and dividend fallback paths.
- [x] Rework the analysis workflow for Twelve Data so cached prospectus terms are loaded before market/dividend analysis and Twelve mode no longer silently falls back to Yahoo.
- [x] Improve floating-rate and fixed-to-floating yield derivation so post-reset securities use a live benchmark-plus-spread estimate instead of the original fixed coupon.
- [x] Replace the mixed `twelve_data` / `yfinance` market-data stack with an Alpha Vantage-only adapter plus FRED-backed rates.
- [x] Add Alpha Vantage quote/history/dividend/common-overview helpers and targeted regression coverage.
- [x] Refresh the Streamlit architecture/help text and remove low-risk dead imports after the Alpha Vantage pivot.
- [x] Refresh the README so setup, provider architecture, ticker input guidance, and current swarm layers match the Alpha Vantage + FRED implementation.
- [x] Refactor the Alpha/provider seam around a shared local security context and a single Alpha symbol-resolution flow without changing the public market-data or resolver interfaces.
- [x] Commit and push the Alpha/provider seam refactor to GitHub.
- [x] Add EDGAR request pacing, retry/backoff, resolution caching, and failed-download cooldown handling for uncached prospectus lookups.
- [x] Add focused regression coverage for EDGAR cached resolution reuse and failed-download cooldown behavior.
