"""
entso_client.py — ENTSO-E Transparency Platform client.

Fetches day-ahead electricity prices and cross-border physical flows
for Germany and its 6 neighbors (2019-2025) using the entsoe-py library.
Handles .env API key loading, CSV caching, Europe/Berlin timezone
normalization, and per-year error recovery.
"""

import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from entsoe import EntsoePandasClient

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "entso"
PROCESSED_PRICES = PROJECT_ROOT / "data" / "processed" / "entso_prices.csv"
PROCESSED_FLOWS = PROJECT_ROOT / "data" / "processed" / "entso_flows.csv"

# Area codes
EIC_DE = "DE_LU"  # Germany (DE-LU bidding zone code for entsoe-py)

NEIGHBORS = {
    "France":        "10YFR-RTE------C",
    "Austria":       "10YAT-APG------L",
    "Denmark_W":     "10YDK-1--------W",
    "Netherlands":   "10YNL----------L",
    "Poland":        "10YPL-AREA-----S",
    "Czech":         "10YCZ-CEPS-----N",
}

TZ = "Europe/Berlin"

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def get_entso_client() -> EntsoePandasClient:
    """
    Load API key from .env and return an authenticated EntsoePandasClient.

    Raises
    ------
    EnvironmentError if ENTSO_E_API_KEY is not set.
    """
    api_key = os.getenv("ENTSO_E_API_KEY")
    if not api_key or api_key == "paste_key_here":
        raise EnvironmentError(
            "ENTSO_E_API_KEY not set. "
            "Register at https://transparency.entsoe.eu/ and add the key to .env"
        )
    return EntsoePandasClient(api_key=api_key)


# ---------------------------------------------------------------------------
# Day-ahead prices
# ---------------------------------------------------------------------------

def fetch_day_ahead_prices(
    start_year: int = 2019,
    end_year: int = 2025,
    force: bool = False,
) -> pd.Series:
    """
    Fetch/cache day-ahead electricity prices for Germany (EUR/MWh).

    Prices are downloaded year-by-year. Each year is cached as
    ``data/raw/entso/prices_{year}.csv``. Failed years are logged and skipped.

    Returns
    -------
    pd.Series with tz-aware Europe/Berlin DatetimeIndex and float values.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    try:
        client = get_entso_client()
    except EnvironmentError as exc:
        log.warning("[entso] %s — prices will not be fetched.", exc)
        return _load_cached_prices()

    frames = []
    for year in range(start_year, end_year + 1):
        cache_file = RAW_DIR / f"prices_{year}.csv"

        if cache_file.exists() and not force:
            log.info("[entso] Prices %s — loading from cache.", year)
            s = _read_price_csv(cache_file)
            frames.append(s)
            continue

        start = pd.Timestamp(f"{year}-01-01", tz=TZ)
        end = pd.Timestamp(f"{year+1}-01-01", tz=TZ)

        try:
            log.info("[entso] Fetching DA prices %s …", year)
            s = client.query_day_ahead_prices(EIC_DE, start=start, end=end)
            s = s.tz_convert(TZ)
            s.to_csv(cache_file, header=["price_EUR_MWh"])
            log.info("[entso] Prices %s — %d rows cached → %s", year, len(s), cache_file)
            frames.append(s)
        except Exception as exc:
            log.error("[entso] Prices %s — FAILED: %s", year, exc)

    if not frames:
        return _load_cached_prices()

    prices = pd.concat(frames).sort_index()
    prices.name = "price_EUR_MWh"

    # Save combined processed file
    PROCESSED_PRICES.parent.mkdir(parents=True, exist_ok=True)
    prices.to_csv(PROCESSED_PRICES, header=["price_EUR_MWh"])
    log.info("[entso] Prices saved → %s  (%d rows)", PROCESSED_PRICES, len(prices))

    return prices


def _read_price_csv(path: Path) -> pd.Series:
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    s = df.iloc[:, 0]
    s.index = pd.to_datetime(s.index, utc=True).tz_convert(TZ)
    s.name = "price_EUR_MWh"
    return s


def _load_cached_prices() -> pd.Series:
    """Load the combined processed file if it exists, else return empty Series."""
    if PROCESSED_PRICES.exists():
        log.info("[entso] Loading prices from processed cache: %s", PROCESSED_PRICES)
        return _read_price_csv(PROCESSED_PRICES)
    log.warning("[entso] No price data available (no cache, no API key).")
    return pd.Series(name="price_EUR_MWh", dtype=float)


# ---------------------------------------------------------------------------
# Cross-border flows
# ---------------------------------------------------------------------------

def fetch_cross_border_flows(
    start_year: int = 2019,
    end_year: int = 2025,
    force: bool = False,
) -> pd.DataFrame:
    """
    Fetch/cache cross-border physical flows DE → each neighbor (MWh).

    Each combination (neighbor × year) is cached as
    ``data/raw/entso/flows_{neighbor}_{year}.csv``.

    Returns
    -------
    pd.DataFrame with columns ``flow_{neighbor}_MWh`` for each neighbor.
    Index is tz-aware Europe/Berlin DatetimeIndex.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    try:
        client = get_entso_client()
    except EnvironmentError as exc:
        log.warning("[entso] %s — flows will not be fetched.", exc)
        return _load_cached_flows()

    neighbor_frames: dict[str, list[pd.Series]] = {n: [] for n in NEIGHBORS}

    for year in range(start_year, end_year + 1):
        start = pd.Timestamp(f"{year}-01-01", tz=TZ)
        end = pd.Timestamp(f"{year+1}-01-01", tz=TZ)

        for neighbor, eic in NEIGHBORS.items():
            cache_file = RAW_DIR / f"flows_{neighbor}_{year}.csv"
            col_name = f"flow_{neighbor}_MWh"

            if cache_file.exists() and not force:
                log.info("[entso] Flows DE→%s %s — loading from cache.", neighbor, year)
                s = _read_flow_csv(cache_file, col_name)
                neighbor_frames[neighbor].append(s)
                continue

            try:
                log.info("[entso] Fetching flows DE→%s %s …", neighbor, year)
                s = client.query_crossborder_flows(EIC_DE, eic, start=start, end=end)
                s = s.tz_convert(TZ)
                s.name = col_name
                s.to_csv(cache_file, header=[col_name])
                log.info(
                    "[entso] Flows DE→%s %s — %d rows cached → %s",
                    neighbor, year, len(s), cache_file,
                )
                neighbor_frames[neighbor].append(s)
            except Exception as exc:
                log.error("[entso] Flows DE→%s %s — FAILED: %s", neighbor, year, exc)

    # Concatenate per neighbor then join all neighbors
    series_list = []
    for neighbor, frames in neighbor_frames.items():
        if frames:
            s = pd.concat(frames).sort_index()
            s.name = f"flow_{neighbor}_MWh"
            series_list.append(s)

    if not series_list:
        return _load_cached_flows()

    flows = pd.concat(series_list, axis=1).sort_index()

    # Net export column (positive = net exporter)
    flows["net_export_MWh"] = flows.sum(axis=1)

    # Save combined processed file
    PROCESSED_FLOWS.parent.mkdir(parents=True, exist_ok=True)
    flows.to_csv(PROCESSED_FLOWS)
    log.info("[entso] Flows saved → %s  (%d rows)", PROCESSED_FLOWS, len(flows))

    return flows


def _read_flow_csv(path: Path, col_name: str) -> pd.Series:
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    s = df.iloc[:, 0]
    s.index = pd.to_datetime(s.index, utc=True).tz_convert(TZ)
    s.name = col_name
    return s


def _load_cached_flows() -> pd.DataFrame:
    """Load the combined processed file if it exists, else return empty DataFrame."""
    if PROCESSED_FLOWS.exists():
        log.info("[entso] Loading flows from processed cache: %s", PROCESSED_FLOWS)
        df = pd.read_csv(PROCESSED_FLOWS, index_col=0, parse_dates=True)
        df.index = pd.to_datetime(df.index, utc=True).tz_convert(TZ)
        return df
    log.warning("[entso] No flow data available (no cache, no API key).")
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Convenience loader
# ---------------------------------------------------------------------------

def load_entso_data(force_rebuild: bool = False) -> tuple[pd.Series, pd.DataFrame]:
    """
    Return (prices, flows) loading from processed cache when available.

    Parameters
    ----------
    force_rebuild : Re-download everything from ENTSO-E even if cached.

    Returns
    -------
    prices : pd.Series  — hourly EUR/MWh, Europe/Berlin
    flows  : pd.DataFrame — hourly MWh per neighbor, Europe/Berlin
    """
    if (
        PROCESSED_PRICES.exists()
        and PROCESSED_FLOWS.exists()
        and not force_rebuild
    ):
        log.info("[entso] Both processed caches found — loading directly.")
        return _load_cached_prices(), _load_cached_flows()

    prices = fetch_day_ahead_prices(force=force_rebuild)
    flows = fetch_cross_border_flows(force=force_rebuild)
    return prices, flows
