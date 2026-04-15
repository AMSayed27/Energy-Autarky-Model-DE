"""
smard_client.py — SMARD API client.

Fetches Germany national electricity generation and consumption data
(2019-2025) from the SMARD platform (www.smard.de/app/chart_data).
Handles index-timestamp discovery, chunk downloads, CSV caching,
rate limiting, and master DataFrame assembly with renewable share columns.
"""

import time
import json
import logging
from pathlib import Path

import requests
import pandas as pd
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://www.smard.de/app/chart_data"
REGION = "DE"
RESOLUTION = "quarterhour"

# SMARD filter IDs for each energy type
FILTERS = {
    "solar":             4068,
    "wind_onshore":      4067,
    "wind_offshore":     1225,
    "nuclear":           1224,
    "hard_coal":         4069,
    "lignite":           4104,
    "natural_gas":       4071,
    "biomass":           4066,
    "total_consumption": 410,
}

# Columns that constitute "renewables"
RENEWABLE_COLS = ["solar_MWh", "wind_onshore_MWh", "wind_offshore_MWh", "biomass_MWh"]

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "smard"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_PATH = PROCESSED_DIR / "smard_master.csv"
RATE_LIMIT_S = 0.4  # seconds between HTTP requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_json(url: str) -> dict:
    """GET a URL and return parsed JSON, retrying once on failure."""
    for attempt in range(2):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            if attempt == 0:
                log.warning("Retrying %s after error: %s", url, exc)
                time.sleep(1.0)
            else:
                log.error("Failed to fetch %s: %s", url, exc)
                return {}


def _fetch_timestamps(filter_id: int) -> list[int]:
    """Return list of available epoch-ms timestamps for a filter."""
    url = f"{BASE_URL}/{filter_id}/{REGION}/index_{RESOLUTION}.json"
    data = _get_json(url)
    time.sleep(RATE_LIMIT_S)
    return data.get("timestamps", [])


def _fetch_chunk(filter_id: int, timestamp: int) -> list[list]:
    """Return raw series [[epoch_ms, value], ...] for one chunk."""
    url = (
        f"{BASE_URL}/{filter_id}/{REGION}/"
        f"{filter_id}_{REGION}_{RESOLUTION}_{timestamp}.json"
    )
    data = _get_json(url)
    time.sleep(RATE_LIMIT_S)
    return data.get("series", [])


def _ms_to_datetime(ms_series: pd.Series) -> pd.Series:
    """Convert epoch-ms to tz-aware Europe/Berlin datetime."""
    return pd.to_datetime(ms_series, unit="ms", utc=True).dt.tz_convert("Europe/Berlin")


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def fetch_smard_filter(
    name: str,
    filter_id: int,
    start_year: int = 2019,
    end_year: int = 2025,
    force: bool = False,
) -> pd.DataFrame:
    """
    Download all chunks for one SMARD filter, cache per year as CSV.

    Parameters
    ----------
    name        : Human-readable label, e.g. "solar"
    filter_id   : SMARD filter code, e.g. 4068
    start_year  : First year to include (inclusive)
    end_year    : Last year to include (inclusive)
    force       : Re-download even if cache exists

    Returns
    -------
    pd.DataFrame with columns ['datetime', 'value_MWh']
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Check per-year caches first
    cached_frames = []
    years_needed = list(range(start_year, end_year + 1))
    years_to_fetch = []

    for year in years_needed:
        cache_file = RAW_DIR / f"{filter_id}_{year}.csv"
        if cache_file.exists() and not force:
            df_cached = pd.read_csv(cache_file, parse_dates=["datetime"])
            df_cached["datetime"] = pd.to_datetime(df_cached["datetime"], utc=True).dt.tz_convert(
                "Europe/Berlin"
            )
            cached_frames.append(df_cached)
        else:
            years_to_fetch.append(year)

    if not years_to_fetch:
        log.info("[smard] %s — all years cached, skipping download.", name)
        return pd.concat(cached_frames, ignore_index=True)

    # Discover timestamps and download chunks
    log.info("[smard] %s (filter=%s) — fetching index …", name, filter_id)
    all_timestamps = _fetch_timestamps(filter_id)

    # Build a per-chunk DataFrame and partition by year
    year_rows: dict[int, list] = {y: [] for y in years_to_fetch}

    with tqdm(all_timestamps, desc=f"smard/{name}", unit="chunk", leave=False) as pbar:
        for ts_ms in pbar:
            chunk_dt = pd.Timestamp(ts_ms, unit="ms", tz="UTC")
            chunk_year = chunk_dt.year

            # Include chunk if it overlaps any needed year.
            # Chunks are ~1 week long. A chunk starting in late December of year N-1
            # will contain the first few days of year N.
            if chunk_year < start_year - 1 or chunk_year > end_year:
                continue

            series = _fetch_chunk(filter_id, ts_ms)
            for row_ms, value in series:
                if value is None:
                    continue
                # Use Europe/Berlin for partitioning to ensure Jan 1st 00:00 (which is Dec 31st 23:00 UTC)
                # is correctly filed under the new year.
                row_dt = pd.Timestamp(row_ms, unit="ms", tz="UTC").tz_convert("Europe/Berlin")
                row_year = row_dt.year
                if row_year in year_rows:
                    year_rows[row_year].append((row_ms, value))

    # Save per-year caches and accumulate
    downloaded_frames = []
    for year, rows in year_rows.items():
        if not rows:
            log.warning("[smard] %s — no data returned for %s", name, year)
            continue
        df_year = pd.DataFrame(rows, columns=["_ms", "value_MWh"])
        df_year["datetime"] = _ms_to_datetime(df_year["_ms"])
        df_year = df_year.drop(columns=["_ms"]).sort_values("datetime").drop_duplicates("datetime")
        cache_file = RAW_DIR / f"{filter_id}_{year}.csv"
        df_year.to_csv(cache_file, index=False)
        log.info("[smard] %s %s — %d rows cached → %s", name, year, len(df_year), cache_file)
        downloaded_frames.append(df_year)

    all_frames = cached_frames + downloaded_frames
    if not all_frames:
        return pd.DataFrame(columns=["datetime", "value_MWh"])
    return pd.concat(all_frames, ignore_index=True).sort_values("datetime")


def build_master_df(start_year: int = 2019, end_year: int = 2025, force: bool = False) -> pd.DataFrame:
    """
    Download all SMARD filters, resample to hourly, merge into one DataFrame.

    Returns
    -------
    pd.DataFrame indexed by hourly datetime (Europe/Berlin) with columns:
        solar_MWh, wind_onshore_MWh, wind_offshore_MWh, nuclear_MWh,
        hard_coal_MWh, lignite_MWh, natural_gas_MWh, biomass_MWh,
        total_consumption_MWh, total_renewables_MWh, renewable_share
    """
    merged: pd.DataFrame | None = None

    for name, filter_id in FILTERS.items():
        log.info("─" * 60)
        log.info("[smard] Processing: %s", name)
        df = fetch_smard_filter(name, filter_id, start_year, end_year, force)

        if df.empty:
            log.warning("[smard] %s returned empty — skipping.", name)
            continue

        # Filter to requested years
        df = df[
            (df["datetime"].dt.year >= start_year)
            & (df["datetime"].dt.year <= end_year)
        ].copy()

        # Set datetime as index and resample to hourly sums
        df = df.set_index("datetime").sort_index()
        col_name = f"{name}_MWh"
        df.columns = [col_name]
        df_hourly = df.resample("h").sum(min_count=1)

        if merged is None:
            merged = df_hourly
        else:
            merged = merged.join(df_hourly, how="outer")

    if merged is None or merged.empty:
        raise RuntimeError("No SMARD data was fetched. Check network connection.")

    # Fill gaps with NaN (don't zero-fill; caller decides)
    # Add derived columns
    available_renewable_cols = [c for c in RENEWABLE_COLS if c in merged.columns]
    merged["total_renewables_MWh"] = merged[available_renewable_cols].sum(axis=1, min_count=1)

    if "total_consumption_MWh" in merged.columns:
        merged["renewable_share"] = (
            merged["total_renewables_MWh"] / merged["total_consumption_MWh"]
        ).clip(lower=0, upper=2)  # cap at 200% to handle data artefacts
    else:
        merged["renewable_share"] = float("nan")

    merged = merged.reset_index()
    merged.rename(columns={"index": "datetime"}, inplace=True)

    # Save processed cache
    PROCESSED_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(PROCESSED_PATH, index=False)
    log.info("[smard] Master DataFrame saved → %s  (%d rows)", PROCESSED_PATH, len(merged))

    return merged


def load_smard_data(force_rebuild: bool = False) -> pd.DataFrame:
    """
    Return the SMARD master DataFrame.

    Loads from ``data/processed/smard_master.csv`` if it exists,
    otherwise runs build_master_df() to download and build it.

    Parameters
    ----------
    force_rebuild : Re-download everything from SMARD even if cached.
    """
    if PROCESSED_PATH.exists() and not force_rebuild:
        log.info("[smard] Loading from processed cache: %s", PROCESSED_PATH)
        df = pd.read_csv(PROCESSED_PATH, parse_dates=["datetime"])
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True).dt.tz_convert("Europe/Berlin")
        return df
    return build_master_df(force=force_rebuild)
