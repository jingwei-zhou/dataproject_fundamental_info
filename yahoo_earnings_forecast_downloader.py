from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
DEFAULT_MODULES = ["price", "calendarEvents", "earningsTrend"]
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://finance.yahoo.com",
    "Referer": "https://finance.yahoo.com/",
}


def build_session(timeout_retries: int = 3, backoff_factor: float = 1.0) -> requests.Session:
    """Build a requests session with retry logic for transient failures."""
    session = requests.Session()
    retry = Retry(
        total=timeout_retries,
        connect=timeout_retries,
        read=timeout_retries,
        status=timeout_retries,
        allowed_methods=frozenset(["GET"]),
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(DEFAULT_HEADERS)
    return session


def unwrap(value: Any) -> Any:
    """Return the raw scalar from Yahoo's nested objects when available."""
    if isinstance(value, dict):
        if "raw" in value:
            return value["raw"]
        if "fmt" in value:
            return value["fmt"]
    return value



def stringify_date_entries(entries: Any) -> str | None:
    """Convert Yahoo date entry list into a readable string."""
    if entries is None:
        return None
    if not isinstance(entries, list):
        entries = [entries]

    values: list[str] = []
    for item in entries:
        if isinstance(item, dict):
            if item.get("fmt"):
                values.append(str(item["fmt"]))
            elif item.get("raw"):
                values.append(str(item["raw"]))
        elif item is not None:
            values.append(str(item))

    if not values:
        return None
    return " | ".join(values)



def normalize_period(period: str | None) -> str | None:
    if not period:
        return None
    period = period.strip().lower()
    mapping = {
        "0q": "current_q",
        "+1q": "next_q",
        "1q": "next_q",
        "0y": "current_y",
        "+1y": "next_y",
        "1y": "next_y",
    }
    return mapping.get(period)



def flatten_estimate(prefix: str, block: dict[str, Any] | None) -> dict[str, Any]:
    block = block or {}
    return {
        f"{prefix}_avg": unwrap(block.get("avg")),
        f"{prefix}_low": unwrap(block.get("low")),
        f"{prefix}_high": unwrap(block.get("high")),
        f"{prefix}_number_of_analysts": unwrap(block.get("numberOfAnalysts")),
        f"{prefix}_growth": unwrap(block.get("growth")),
        f"{prefix}_year_ago": unwrap(block.get("yearAgoRevenue"))
        if "yearAgoRevenue" in block
        else unwrap(block.get("yearAgoEps")),
    }



def parse_trend(trend_list: list[dict[str, Any]] | None) -> dict[str, Any]:
    trend_list = trend_list or []
    out: dict[str, Any] = {}

    for item in trend_list:
        bucket = normalize_period(item.get("period"))
        if not bucket:
            continue

        earnings_estimate = item.get("earningsEstimate") or {}
        revenue_estimate = item.get("revenueEstimate") or {}

        out[f"{bucket}_end_date"] = item.get("endDate")
        out.update(flatten_estimate(f"{bucket}_eps", earnings_estimate))
        out.update(flatten_estimate(f"{bucket}_revenue", revenue_estimate))

    return out



def fetch_quote_summary(
    session: requests.Session,
    ticker: str,
    modules: Iterable[str] | None = None,
    timeout: int = 20,
) -> dict[str, Any]:
    """Fetch Yahoo Finance quoteSummary JSON for a ticker."""
    modules = list(modules or DEFAULT_MODULES)
    url = BASE_URL.format(ticker=ticker)
    params = {
        "modules": ",".join(modules),
        "formatted": "false",
        "lang": "en-US",
        "region": "US",
        "corsDomain": "finance.yahoo.com",
    }

    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    error = (
        payload.get("quoteSummary", {})
        .get("error")
    )
    if error:
        raise RuntimeError(f"Yahoo returned an error for {ticker}: {error}")

    result = (
        payload.get("quoteSummary", {})
        .get("result")
    )
    if not result:
        raise RuntimeError(f"No quoteSummary result found for {ticker}")

    return result[0]



def extract_forecast_row(ticker: str, result: dict[str, Any]) -> dict[str, Any]:
    price = result.get("price") or {}
    calendar = result.get("calendarEvents") or {}
    earnings_info = calendar.get("earnings") or {}
    trend = (result.get("earningsTrend") or {}).get("trend") or []

    row: dict[str, Any] = {
        "ticker": ticker.upper(),
        "short_name": price.get("shortName"),
        "long_name": price.get("longName"),
        "exchange": price.get("exchangeName"),
        "currency": price.get("currency"),
        "regular_market_price": unwrap(price.get("regularMarketPrice")),
        "earnings_date": stringify_date_entries(earnings_info.get("earningsDate")),
        "calendar_eps_avg": unwrap(earnings_info.get("earningsAverage")),
        "calendar_eps_low": unwrap(earnings_info.get("earningsLow")),
        "calendar_eps_high": unwrap(earnings_info.get("earningsHigh")),
        "calendar_revenue_avg": unwrap(earnings_info.get("revenueAverage")),
        "calendar_revenue_low": unwrap(earnings_info.get("revenueLow")),
        "calendar_revenue_high": unwrap(earnings_info.get("revenueHigh")),
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "Yahoo Finance quoteSummary",
    }

    row.update(parse_trend(trend))
    return row



def maybe_write_raw_json(raw_dir: Path | None, ticker: str, data: dict[str, Any]) -> None:
    if raw_dir is None:
        return
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_file = raw_dir / f"{ticker.upper()}.json"
    raw_file.write_text(json.dumps(data, indent=2), encoding="utf-8")



def read_tickers_from_file(path: str | Path) -> list[str]:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Ticker file not found: {file_path}")

    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        with file_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            rows = [row for row in reader if row]

        if not rows:
            return []

        first_row = [cell.strip() for cell in rows[0]]
        header_candidates = {c.lower() for c in first_row}
        ticker_col_index = 0
        if "ticker" in header_candidates:
            ticker_col_index = first_row.index(next(c for c in first_row if c.lower() == "ticker"))
            rows = rows[1:]

        tickers = [row[ticker_col_index].strip().upper() for row in rows if len(row) > ticker_col_index and row[ticker_col_index].strip()]
        return dedupe_keep_order(tickers)

    lines = [line.strip().upper() for line in file_path.read_text(encoding="utf-8").splitlines()]
    tickers = [line for line in lines if line and not line.startswith("#")]
    return dedupe_keep_order(tickers)



def dedupe_keep_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out



def build_output_paths(output_dir: Path) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    snapshot = output_dir / f"earnings_forecast_snapshot_{ts}.csv"
    latest = output_dir / "earnings_forecast_latest.csv"
    history = output_dir / "earnings_forecast_history.csv"
    return snapshot, latest, history



def update_history(history_path: Path, df: pd.DataFrame) -> None:
    if history_path.exists():
        existing = pd.read_csv(history_path)
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["ticker", "fetched_at_utc"], keep="last")
    else:
        combined = df.copy()
    combined.to_csv(history_path, index=False)



def collect_forecasts(
    tickers: list[str],
    session: requests.Session,
    raw_dir: Path | None,
    min_sleep: float,
    max_sleep: float,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    rows: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for idx, ticker in enumerate(tickers, start=1):
        try:
            result = fetch_quote_summary(session=session, ticker=ticker)
            maybe_write_raw_json(raw_dir=raw_dir, ticker=ticker, data=result)
            rows.append(extract_forecast_row(ticker=ticker, result=result))
            print(f"[{idx}/{len(tickers)}] OK   {ticker}")
        except Exception as exc:  # noqa: BLE001
            print(f"[{idx}/{len(tickers)}] FAIL {ticker} -> {exc}", file=sys.stderr)
            errors.append({"ticker": ticker, "error": str(exc)})

        if idx < len(tickers):
            time.sleep(random.uniform(min_sleep, max_sleep))

    df = pd.DataFrame(rows)
    if not df.empty:
        preferred_cols = [
            "ticker",
            "short_name",
            "regular_market_price",
            "currency",
            "earnings_date",
            "current_y_revenue_avg",
            "next_y_revenue_avg",
            "current_y_eps_avg",
            "next_y_eps_avg",
            "current_q_revenue_avg",
            "next_q_revenue_avg",
            "current_q_eps_avg",
            "next_q_eps_avg",
            "fetched_at_utc",
            "source",
        ]
        ordered = [c for c in preferred_cols if c in df.columns]
        remaining = [c for c in df.columns if c not in ordered]
        df = df[ordered + remaining]

    return df, errors



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Yahoo Finance earnings / revenue forecast data for one or more tickers."
    )
    parser.add_argument(
        "--tickers",
        nargs="*",
        help="Ticker symbols passed inline, e.g. --tickers AAPL MSFT NVDA",
    )
    parser.add_argument(
        "--tickers-file",
        type=str,
        help="Path to a .txt or .csv file containing ticker symbols.",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="output",
        help="Directory to save snapshot/latest/history CSV files.",
    )
    parser.add_argument(
        "--raw-json-dir",
        type=str,
        default=None,
        help="Optional directory for saving one raw Yahoo JSON file per ticker.",
    )
    parser.add_argument(
        "--min-sleep",
        type=float,
        default=0.3,
        help="Minimum delay between ticker requests, in seconds.",
    )
    parser.add_argument(
        "--max-sleep",
        type=float,
        default=1.2,
        help="Maximum delay between ticker requests, in seconds.",
    )
    return parser.parse_args()



def resolve_tickers(args: argparse.Namespace) -> list[str]:
    tickers: list[str] = []

    if args.tickers:
        tickers.extend([t.strip().upper() for t in args.tickers if t.strip()])
    if args.tickers_file:
        tickers.extend(read_tickers_from_file(args.tickers_file))

    tickers = dedupe_keep_order(tickers)
    if not tickers:
        raise ValueError(
            "No tickers provided. Use --tickers AAPL MSFT or --tickers-file tickers.txt"
        )
    return tickers



def main() -> int:
    args = parse_args()
    tickers = resolve_tickers(args)
    output_dir = Path(args.output_dir)
    raw_dir = Path(args.raw_json_dir) if args.raw_json_dir else None

    session = build_session()
    df, errors = collect_forecasts(
        tickers=tickers,
        session=session,
        raw_dir=raw_dir,
        min_sleep=args.min_sleep,
        max_sleep=args.max_sleep,
    )

    snapshot_path, latest_path, history_path = build_output_paths(output_dir)

    if df.empty:
        print("No rows were collected successfully.", file=sys.stderr)
    else:
        df.to_csv(snapshot_path, index=False)
        df.to_csv(latest_path, index=False)
        update_history(history_path, df)
        print(f"\nSaved snapshot: {snapshot_path}")
        print(f"Saved latest:   {latest_path}")
        print(f"Updated history:{history_path}")
        print(f"Rows collected: {len(df)}")

    if errors:
        error_path = output_dir / "earnings_forecast_errors.csv"
        pd.DataFrame(errors).to_csv(error_path, index=False)
        print(f"Errors saved:   {error_path}")
        print(f"Tickers failed: {len(errors)}", file=sys.stderr)

    return 0 if not errors else 1



if __name__ == "__main__":
    raise SystemExit(main())
