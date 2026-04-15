"""
energy_charts_client.py — Official BNetzA/MaStR Data Integrated via Energy-Charts.info API.

This module fetches aggregated state-level (Bundesländer) installed capacity 
directly from Fraunhofer ISE's Energy-Charts API. This data is the gold standard 
for German energy monitoring, as it integrates:
1. Marktstammdatenregister (MaStR) for historical up to 2021/2022.
2. BNetzA "Monatsberichte" (Monthly Reports) for most recent 2022-2025 growth.

License: CC BY 4.0 (requires attribution to Energy-Charts.info)
API Docs: https://api.energy-charts.info/
"""

import logging
import json
from pathlib import Path
import pandas as pd
import requests
from typing import Dict, List, Optional

# Setup logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("energy_charts")

# Constants
BASE_URL = "https://api.energy-charts.info/installed_power"
RAW_DIR = Path("data/raw/energy_charts")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# State mapping: Project Name -> Energy Charts Code
STATE_CODE_MAP = {
    "Bavaria":                  "by",
    "Baden-Württemberg":       "bw",
    "NRW":                     "nw",
    "Lower Saxony":            "ni",
    "Hesse":                   "he",
    "Saxony":                  "sn",
    "Berlin":                  "be",
    "Rhineland-Palatinate":     "rp",
    "Saxony-Anhalt":           "st",
    "Schleswig-Holstein":      "sh",
    "Brandenburg":             "bb",
    "Thuringia":               "th",
    "Hamburg":                 "hh",
    "Mecklenburg-Vorpommern":  "mv",
    "Saarland":                "sl",
    "Bremen":                  "hb",
}

# Production types to include in "Renewable MW"
# We focus on the big movers that drive Part 2 optimization
EC_RENEWABLE_CATEGORIES = [
    "Solar DC",
    "Wind onshore",
    "Wind offshore",
    "Biomass",
    "Hydro",
]

def fetch_state_installed_capacity(state_name: str, force: bool = False) -> Optional[pd.DataFrame]:
    """
    Fetch annual installed capacity for a specific state from Energy-Charts.
    Returns a DataFrame with columns: [year, technology, capacity_MW]
    """
    code = STATE_CODE_MAP.get(state_name)
    if not code:
        log.error(f"Unknown state name: {state_name}")
        return None

    cache_path = RAW_DIR / f"installed_capacity_{code}.json"
    
    if cache_path.exists() and not force:
        log.info(f"[{state_name}] Loading from cache: {cache_path}")
        with open(cache_path, "r") as f:
            data = json.load(f)
    else:
        log.info(f"[{state_name}] Fetching from API: {BASE_URL}?federal_state={code}")
        params = {
            "federal_state": code,
            "plot_all": "true"
        }
        try:
            r = requests.get(BASE_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            with open(cache_path, "w") as f:
                json.dump(data, f)
        except Exception as e:
            log.error(f"[{state_name}] API Failure: {e}")
            return None

    # Parse response
    years = data.get("time", [])
    series = data.get("production_types", [])
    
    rows = []
    for s in series:
        tech = s.get("name")
        values = s.get("data", [])
        # Iterate over years and values
        for year, val in zip(years, values):
            if val is not None:
                rows.append({
                    "year": int(year),
                    "technology": tech,
                    "capacity_MW": float(val) * 1000 if "GW" in data.get("unit", "GW") else float(val)
                })
    
    df = pd.DataFrame(rows)
    df["state"] = state_name
    return df

def get_all_states_capacity(years_filter: Optional[List[int]] = None) -> pd.DataFrame:
    """
    Fetch and combine data for all 16 states.
    """
    all_dfs = []
    for state_name in STATE_CODE_MAP.keys():
        df = fetch_state_installed_capacity(state_name)
        if df is not None:
            all_dfs.append(df)
    
    if not all_dfs:
        log.error("No data fetched from Energy-Charts.")
        return pd.DataFrame()
    
    full_df = pd.concat(all_dfs, ignore_index=True)
    
    if years_filter:
        full_df = full_df[full_df["year"].isin(years_filter)]
        
    return full_df

if __name__ == "__main__":
    # Test fetch
    test_df = get_all_states_capacity(years_filter=list(range(2018, 2026)))
    print(test_df.head())
    print(f"\nTotal rows: {len(test_df)}")
    print(f"States covered: {test_df['state'].nunique()}")
