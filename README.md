# Yahoo Finance MCP Server

<div align="right">
  <a href="README.md">English</a> | <a href="README.zh.md">中文</a>
</div>

This is a Model Context Protocol (MCP) server that provides comprehensive financial data from Yahoo Finance. It allows you to retrieve detailed information about stocks, including historical prices, company information, financial statements, options data, and market news.

![GitHub last commit](https://img.shields.io/github/last-commit/modesty/pdf2json?color=olive)

## Demo

![MCP Demo](assets/demo.gif)

## MCP Tools

The server exposes the following tools through the Model Context Protocol:

### Stock Information

| Tool | Description |
|------|-------------|
| `get_historical_stock_prices` | Get historical OHLCV data for a stock with customizable period and interval |
| `get_stock_info` | Get comprehensive stock data including price, metrics, and company details |
| `get_yahoo_finance_news` | Get latest news articles for a stock |
| `get_stock_actions` | Get stock dividends and splits history |

### Financial Statements

| Tool | Description |
|------|-------------|
| `get_financial_statement` | Get income statement, balance sheet, or cash flow statement (annual/quarterly) |
| `get_holder_info` | Get major holders, institutional holders, mutual funds, or insider transactions |

### Options Data

| Tool | Description |
|------|-------------|
| `get_option_expiration_dates` | Get available options expiration dates |
| `get_option_chain` | Get options chain for a specific expiration date and type (calls/puts) |

### Analyst Information

| Tool | Description |
|------|-------------|
| `get_recommendations` | Get analyst recommendations or upgrades/downgrades history |

### Sharia Screening

| Tool | Description |
|------|-------------|
| `get_sharia_status` | Sharia classification from [Yaqeen](https://yaaqen.com/stocks) (Al-Rajhi committee standards), with its update date. When Yaqeen says "محل نظر" or has no rating, adds links for a manual check on [Chart Idea](https://chart-idea.com/filter/) (it blocks automated queries) and on [Stock Hunter](https://usastockhunteracademy.com/halal-stocks-usa/) (results need a login), plus indicative ratios from Yahoo data. Yaqeen is queried politely: robots.txt honoured, requests spaced, results cached for an hour. |

### Watchlists and Scans

| Tool | Description |
|------|-------------|
| `get_quotes` | Compact live quotes for up to 40 tickers in one call: session-aware price and change, volume, relative volume (plain, and paced against what a typical day has traded by this time), float rotation, market cap, short interest, the fields that place a stock in a category (sector, industry, country, first trade date, last split, earnings date), and the day's levels (pre-market high/low, VWAP, opening range, previous session high/low, ATR14, 20-session high/low), with an optional 10-minute sparkline. Built for dashboards that refresh a whole watchlist at once. |
| `get_market_movers` | Yahoo's predefined screeners (`day_gainers`, `day_losers`, `most_actives`, `small_cap_gainers`, `aggressive_small_caps`, `most_shorted_stocks`), Yahoo trending tickers (`trending`), or `premarket`: the trending and screener names ranked by their session-aware move, with levels and float rotation. `nasdaq_only` keeps Nasdaq listings. |

## Real-World Use Cases

With this MCP server, you can use Claude to:

### Stock Analysis

- **Price Analysis**: "Show me the historical stock prices for NOW over the last 6 months with daily intervals."
- **Financial Health**: "Get the quarterly balance sheet for Intuit."
- **Performance Metrics**: "What are the key financial metrics for Intuit from the stock info?"
- **Trend Analysis**: "Compare the quarterly income statements of ServiceNow and Intuit."
- **Cash Flow Analysis**: "Show me the annual cash flow statement for ServiceNow."

### Market Research

- **News Analysis**: "Get the latest news articles about Intuit."
- **Institutional Activity**: "Show me the institutional holders of NOW stock."
- **Insider Trading**: "What are the recent insider transactions for ServiceNow?"
- **Options Analysis**: "Get the options chain for INTU with expiration date 2026-01-30 for calls."
- **Analyst Coverage**: "What are the analyst recommendations for Intuit over the last 3 months?"

### Investment Research

- "Create a comprehensive analysis of Intuit's financial health using their latest quarterly financial statements."
- "Compare the dividend history and stock splits of Coca-Cola and PepsiCo."
- "Analyze the institutional ownership changes in ServiceNow over the past year."
- "Generate a report on the options market activity for Intuit stock with expiration in 30 days."
- "Summarize the latest analyst upgrades and downgrades in the tech sector over the last 6 months."

## Requirements

- Python 3.14.6 or higher
- Dependencies as listed in `pyproject.toml`, including:
  - mcp
  - yfinance
  - pandas
  - pydantic
  - and other packages for data processing

## Setup

### Recommended: run with `uvx`

Run the server directly from the repository without creating a local virtual environment:

```bash
uvx --from git+https://github.com/Alex2Yang97/yahoo-finance-mcp yahoo-finance-mcp
```

### Local development

1. Clone this repository:

   ```bash
   git clone https://github.com/Alex2Yang97/yahoo-finance-mcp.git
   cd yahoo-finance-mcp
   ```

2. Create and activate a virtual environment and install dependencies:

   ```bash
   uv sync
   ```

## Usage

### Quick Start

Run the packaged entrypoint with:

```bash
uvx --from git+https://github.com/Alex2Yang97/yahoo-finance-mcp yahoo-finance-mcp
```

For local changes in this checkout, use:

```bash
uvx --from . yahoo-finance-mcp
```

### Development Mode

If you are working inside a local clone and want to run the source tree directly:

```bash
uv run server.py
```

### Integration with Claude for Desktop

To integrate this server with Claude for Desktop:

1. Install Claude for Desktop to your local machine.
2. Install VS Code to your local machine. Then run the following command to open the `claude_desktop_config.json` file:
   - MacOS: `code ~/Library/Application\ Support/Claude/claude_desktop_config.json`
   - Windows: `code $env:AppData\Claude\claude_desktop_config.json`

3. Edit the Claude for Desktop config file, located at:
   - macOS:

     ```json
     {
       "mcpServers": {
         "yfinance": {
           "command": "uvx",
           "args": [
             "--from",
             "git+https://github.com/Alex2Yang97/yahoo-finance-mcp",
             "yahoo-finance-mcp"
           ]
         }
       }
     }
     ```

   - Windows:

     ```json
     {
       "mcpServers": {
         "yfinance": {
           "command": "uvx",
           "args": [
             "--from",
             "git+https://github.com/Alex2Yang97/yahoo-finance-mcp",
             "yahoo-finance-mcp"
           ]
         }
       }
     }
     ```

   - **Note**: You may need to put the full path to the uv executable in the command field. You can get this by running `which uv` on MacOS/Linux or `where uv` on Windows.

4. Restart Claude for Desktop

## Hosting on Render (remote connector)

The `Dockerfile` runs the server over HTTP (Render sets `PORT`). Endpoints:

| Path | Purpose |
|------|---------|
| `GET /sse` + `POST /messages/` | Legacy SSE transport |
| `POST /mcp` | Streamable HTTP (stateless, JSON responses) |
| `POST /sse` | Streamable HTTP on the legacy URL, so existing connector URLs keep working |
| `GET /health` | Status: uptime, keep-alive, and the state of each Yahoo endpoint |

### Resilience

Yahoo throttles some endpoints per IP. From shared cloud IPs the `quoteSummary`
endpoint behind `Ticker.info` and the news endpoint often answer
"Too Many Requests", while the chart endpoint keeps working. The server
therefore:

- builds the live quote in `get_stock_info` (price, change, pre/post-market,
  day range, volume, average volume, 52-week range, market state, last split)
  from the chart endpoint, and merges `quoteSummary` fundamentals (float, short
  interest, ownership, ratios) when available, cached for 6 hours. The
  `dataStatus` field reports the age of each part;
- falls back from yfinance news to Yahoo's RSS feed and search API, and from
  yfinance history to the chart API;
- runs every Yahoo call in a worker pool with a time budget, shares concurrent
  identical requests, caches results and serves the last good result when Yahoo
  fails;
- backs off automatically (circuit breakers) when an endpoint answers 429.

### Environment variables (all optional)

| Variable | Default | Meaning |
|----------|---------|---------|
| `KEEPALIVE` | `always` | Self-ping so Render's free plan does not sleep: `always`, `market` (weekdays 03:30-20:30 New York), or `off` |
| `KEEPALIVE_INTERVAL` | `600` | Seconds between pings (keep below 900) |
| `KEEPALIVE_URL` | `RENDER_EXTERNAL_URL` | Public URL to ping |
| `STREAMABLE_ON_SSE_PATH` | `1` | Serve Streamable HTTP on `POST /sse`; set `0` for legacy SSE only |
| `YF_MAX_WORKERS` | `8` | Worker threads for Yahoo calls |
| `YF_UPSTREAM_CONCURRENCY` | `4` | Simultaneous requests to Yahoo |
| `YF_HTTP_TIMEOUT` | `8` | Seconds per Yahoo HTTP request |
| `SHARIA_SELF_CHECK` | `AAPL` | Ticker looked up on Yaqeen 20 s after start (`off` to skip) |
| `DATA_SELF_CHECK` | `QQQ,AAPL` | Tickers fetched with `get_quotes` 35 s after start, plus two screeners (`off` to skip) |

A free Render workspace has 750 instance hours a month: one service kept awake
around the clock uses about 744. If you run other free services in the same
workspace, set `KEEPALIVE=market`.

## License

MIT
