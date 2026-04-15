"""
state_processor.py — Bundesland-level installed capacity and autonomy gap processor.

Loads OPSD renewable power plant data, assigns plants to all 16 German states,
applies nuclear retirement corrections, and calculates year-by-year energy
autonomy ratios using national-consumption × population-share as proxy demand.
Outputs a state×year panel DataFrame used by all Part 1 visualizations.
"""

import logging
from pathlib import Path
from typing import Optional, List

import geopandas as gpd
import pandas as pd

from src.data_ingestion.smard_client import PROCESSED_DIR
from src.data_ingestion.energy_charts_client import get_all_states_capacity
from src.data_ingestion.bnetza_reference import get_bnetza_capacity

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
OPSD_PATH = PROJECT_ROOT / "data" / "raw" / "opsd" / "renewable_power_plants_DE.csv"
GEOJSON_PATH = PROJECT_ROOT / "data" / "raw" / "bnetzA" / "germany_states.geojson"
PROCESSED_AUTONOMY = PROCESSED_DIR / "state_autonomy.csv"

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Map OPSD German state names → English labels used in this project
# Includes known typo variants found in OPSD data (e.g. missing umlaut)
STATE_NAME_MAP: dict[str, str] = {
    "Bayern":                  "Bavaria",
    "Baden-Württemberg":       "Baden-Württemberg",
    "Baden-Würtemberg":        "Baden-Württemberg",   # typo in OPSD
    "Nordrhein-Westfalen":     "NRW",
    "Niedersachsen":           "Lower Saxony",
    "Hessen":                  "Hesse",
    "Sachsen":                 "Saxony",
    "Berlin":                  "Berlin",
    "Rheinland-Pfalz":         "Rhineland-Palatinate",
    "Sachsen-Anhalt":          "Saxony-Anhalt",
    "Schleswig-Holstein":      "Schleswig-Holstein",
    "Brandenburg":             "Brandenburg",
    "Thüringen":               "Thuringia",
    "Hamburg":                 "Hamburg",
    "Mecklenburg-Vorpommern":  "Mecklenburg-Vorpommern",
    "Saarland":                "Saarland",
    "Bremen":                  "Bremen",
}

# NUTS-1 region codes mapping (more reliable in OPSD than federal_state)
NUTS_MAP: dict[str, str] = {
    "DE1": "Baden-Württemberg",
    "DE2": "Bavaria",
    "DE3": "Berlin",
    "DE4": "Brandenburg",
    "DE5": "Bremen",
    "DE6": "Hamburg",
    "DE7": "Hesse",
    "DE8": "Mecklenburg-Vorpommern",
    "DE9": "Lower Saxony",
    "DEA": "NRW",
    "DEB": "Rhineland-Palatinate",
    "DEC": "Saarland",
    "DED": "Saxony",
    "DEE": "Saxony-Anhalt",
    "DEF": "Schleswig-Holstein",
    "DEG": "Thuringia",
}

ALL_STATES = list(STATE_NAME_MAP.values())

# Nuclear plants with per-plant shutdown year (German exit in 3 waves)
NUCLEAR_PLANTS: dict[str, list[dict]] = {
    "Bavaria": [
        {"name": "Gundremmingen B", "shutdown_year": 2021, "capacity_MW": 1284},
        {"name": "Gundremmingen C", "shutdown_year": 2021, "capacity_MW": 1288},
        {"name": "Isar 2",          "shutdown_year": 2023, "capacity_MW": 1410},
    ],
    "Baden-Württemberg": [
        {"name": "Neckarwestheim 2", "shutdown_year": 2023, "capacity_MW": 1310},
    ],
    "Lower Saxony": [
        {"name": "Emsland",          "shutdown_year": 2023, "capacity_MW": 1329},
    ],
}

# Nuclear capacity active at base year (2018) — pre-retirement reference
BASE_NUCLEAR_MW: dict[str, float] = {
    "Bavaria":              1284 + 1288 + 1410,   # 3 plants
    "Baden-Württemberg":    1310,
    "Lower Saxony":         1329,
}

# ---------------------------------------------------------------------------
# Constants — Demand & Spatial Weights
# ---------------------------------------------------------------------------

# Official Electricity Consumption Shares per Bundesland (Estimated from LAK Energiebilanzen).
# Unlike population share, this accounts for Germany's industrial clusters.
# e.g., Bavaria and NRW have higher electricity demand than their population suggests.
CONSUMPTION_SHARE = {
    "NRW":                     0.245,  # Germany's industrial heart
    "Bavaria":                  0.182,  # Strong manufacturing & tech base
    "Baden-Württemberg":        0.138,  # Automotive & machine tooling
    "Lower Saxony":            0.112,  # Heavy energy use in chemical/food
    "Hesse":                   0.078,  # Data centers (Frankfurt) & logistics
    "Saxony":                  0.048,  # Microelectronics (Dresden)
    "Rhineland-Palatinate":     0.051,  # Chemical industry (Ludwigshafen)
    "Berlin":                  0.028,  # Services dominated
    "Schleswig-Holstein":      0.032,  
    "Brandenburg":             0.029,  
    "Saxony-Anhalt":           0.033,  # Chemical/energy clusters
    "Thuringia":               0.024,
    "Hamburg":                 0.025,  # Port & industrial use
    "Mecklenburg-Vorpommern":  0.016,
    "Saarland":                0.012,  # Steel/industrial
    "Bremen":                  0.009,
}

ALL_STATES = list(CONSUMPTION_SHARE.keys())

# Technologies considered "renewable" — matched against energy_source_level_2
# (actual OPSD values: 'Solar', 'Wind', 'Bioenergy', 'Hydro', 'Geothermal')
RENEWABLE_TYPES = {
    # energy_source_level_2 labels (lowercase)
    "solar",
    "wind",
    "bioenergy",
    "hydro",
    "geothermal",
    # technology column labels (lowercase) — used for fine-grained filtering
    "photovoltaics",
    "photovoltaics ground",
    "onshore",
    "offshore",
    "run-of-river",
    "biomass and biogas",
    "sewage and landfill gas",
}


# ---------------------------------------------------------------------------
# Module 1 — OPSD capacity loading
# ---------------------------------------------------------------------------

def load_official_state_capacity(years_filter: Optional[List[int]] = None) -> pd.DataFrame:
    """
    Fetch official state-level capacity from BNetzA/MASTR via Energy-Charts.
    This provides year-by-year accurate installed renewable MW.
    
    Source: Energy-Charts.info (Fraunhofer ISE) / Marktstammdatenregister
    """
    log.info("[state] Fetching official MaStR capacity data via Energy-Charts API...")
    df = get_all_states_capacity(years_filter=years_filter)
    
    if df.empty:
        log.warning("[state] Official capacity data is empty. Check internet connection.")
        return df

    # Map Energy-Charts tech names to our RENEWABLE_TYPES internal labels
    tech_map = {
        "Solar DC":      "solar",
        "Wind onshore":   "wind",
        "Wind offshore":  "wind",
        "Biomass":       "bioenergy",
        "Hydro":         "hydro",
    }
    df["tech_internal"] = df["technology"].map(tech_map)
    
    log.info("[state] Official capacity data loaded for %d years (%d states).", 
             df["year"].nunique(), df["state"].nunique())
    return df


def load_opsd_state_capacity(opsd_path: str | Path = OPSD_PATH) -> pd.DataFrame:
    """
    Load OPSD renewable power plants CSV and aggregate installed capacity
    by state × technology × commissioning year.

    Uses the actual OPSD schema:
      - ``electrical_capacity``    : installed capacity in MW (not kW)
      - ``energy_source_level_2``  : top-level type (Solar/Wind/Bioenergy/Hydro)
      - ``federal_state``          : German state name
      - ``commissioning_date``     : ISO date of grid connection

    Returns
    -------
    pd.DataFrame with columns:
        state, technology, capacity_MW, commissioned_year
    """
    opsd_path = Path(opsd_path)
    if not opsd_path.exists():
        raise FileNotFoundError(
            f"OPSD file not found: {opsd_path}\n"
            "Download from https://data.open-power-system-data.org/renewable_power_plants/ "
            "and save to data/raw/opsd/"
        )

    log.info("[opsd] Loading %s …", opsd_path)
    df = pd.read_csv(opsd_path, low_memory=False)
    log.info("[opsd] Raw columns: %s", list(df.columns))

    # ── Capacity column ───────────────────────────────────────────────────────
    # OPSD standard CSV: 'electrical_capacity' is already in MW
    cap_col = None
    for candidate in ["electrical_capacity", "Nettonennleistung"]:
        if candidate in df.columns:
            cap_col = candidate
            break
    if cap_col is None:
        cap_cols = [c for c in df.columns if "capacity" in c.lower() or "leistung" in c.lower()]
        if not cap_cols:
            raise ValueError("Cannot identify capacity column in OPSD file.")
        cap_col = cap_cols[0]

    df["capacity_MW"] = pd.to_numeric(df[cap_col], errors="coerce")
    # Guard: if values look like kW (median > 500), convert
    if df["capacity_MW"].median(skipna=True) > 500:
        log.info("[opsd] Detected kW values — converting to MW.")
        df["capacity_MW"] = df["capacity_MW"] / 1000

    # ── Technology column ─────────────────────────────────────────────────────
    # Prefer energy_source_level_2 as the normalised top-level label.
    # The OPSD file *also* has a 'technology' column with finer detail —
    # we don't rename into it to avoid duplicate columns.
    if "energy_source_level_2" in df.columns:
        tech_series = df["energy_source_level_2"]
    elif "technology" in df.columns:
        tech_series = df["technology"]
    else:
        tech_col = next(
            (c for c in df.columns if "energy_source" in c.lower()), None
        )
        tech_series = df[tech_col] if tech_col else pd.Series(["unknown"] * len(df))

    df["_tech"] = tech_series.astype(str).str.lower().str.strip()

    # ── State column ─────────────────────────────────────────────────────────
    # Prioritize NUTS-1 region codes as they are verified to be more accurate
    # than the federal_state column in this dataset.
    if "nuts_1_region" in df.columns:
        df["state"] = df["nuts_1_region"].map(NUTS_MAP)
    else:
        df["state"] = pd.NA

    # Fallback to federal_state if NUTS mapping failed or column missing
    if df["state"].isna().any():
        state_col = None
        for candidate in ["federal_state", "Bundesland", "state_name"]:
            if candidate in df.columns:
                state_col = candidate
                break
        
        if state_col:
            # Only fill missing ones
            mask = df["state"].isna()
            df.loc[mask, "state"] = df.loc[mask, state_col].map(STATE_NAME_MAP)

    if df["state"].isna().all():
        raise ValueError(
            "Cannot identify state using nuts_1_region or federal_state columns."
        )

    # ── Commissioning year ────────────────────────────────────────────────────
    date_col = next(
        (c for c in df.columns if "commissioning" in c.lower() or "inbetriebnahme" in c.lower()),
        None,
    )
    if date_col:
        df["commissioned_year"] = pd.to_datetime(df[date_col], errors="coerce").dt.year
    else:
        df["commissioned_year"] = pd.NA

    # ── Filter and output ─────────────────────────────────────────────────────
    # Drop non-German rows (e.g. offshore EEZ, foreign plants)
    df = df.dropna(subset=["state", "capacity_MW"])
    df = df[df["capacity_MW"] > 0]

    df["technology"] = df["_tech"]
    result = df[
        ["state", "technology", "capacity_MW", "commissioned_year"]
    ].reset_index(drop=True)

    log.info(
        "[opsd] Loaded %d plants across %d states (total: %.1f GW)",
        len(result),
        result["state"].nunique(),
        result["capacity_MW"].sum() / 1000,
    )
    return result


# ---------------------------------------------------------------------------
# Module 2 — GeoJSON loading
# ---------------------------------------------------------------------------

def load_germany_geodata(geojson_path: str | Path = GEOJSON_PATH) -> gpd.GeoDataFrame:
    """
    Load a GeoJSON file of German Bundesländer and normalise the state name column.

    Returns
    -------
    gpd.GeoDataFrame with columns: ``state``, ``geometry``
    Coordinate reference system: EPSG:4326
    """
    geojson_path = Path(geojson_path)
    if not geojson_path.exists():
        raise FileNotFoundError(
            f"GeoJSON not found: {geojson_path}\n"
            "Download from https://raw.githubusercontent.com/isellsoap/deutschlandGeoJSON/"
            "main/2_bundeslaender/4_niedrig.geojson and save to data/raw/bnetzA/"
        )

    log.info("[geo] Loading %s …", geojson_path)
    gdf = gpd.read_file(geojson_path)

    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # Detect state name column — common key names in various GeoJSON sources
    name_candidates = ["NAME_1", "name", "GEN", "state", "Bundesland", "VARNAME_1"]
    name_col = next((c for c in name_candidates if c in gdf.columns), None)
    if name_col is None:
        log.warning("[geo] Could not detect state name column. Columns: %s", list(gdf.columns))
        raise ValueError(f"No state name column found. Available: {list(gdf.columns)}")

    # Attempt direct map; fall back to fuzzy for common variants
    gdf["state"] = gdf[name_col].map(STATE_NAME_MAP)

    # Handle English or already-normalised names (e.g. some GeoJSONs use English)
    # Fill remaining NaN with direct value if it's already in ALL_STATES
    mask = gdf["state"].isna()
    gdf.loc[mask, "state"] = gdf.loc[mask, name_col].where(
        gdf.loc[mask, name_col].isin(ALL_STATES)
    )

    unmatched = gdf[gdf["state"].isna()][name_col].tolist()
    if unmatched:
        log.warning("[geo] Unmatched state names (will be dropped): %s", unmatched)

    gdf = gdf.dropna(subset=["state"])[["state", "geometry"]].reset_index(drop=True)
    log.info("[geo] Loaded %d state polygons.", len(gdf))
    return gdf


# ---------------------------------------------------------------------------
# Module 3 — Nuclear retirement correction
# ---------------------------------------------------------------------------

def calculate_nuclear_retirement_impact(
    state: str,
    year: int,
) -> float:
    """
    Return the nuclear capacity (MW) that was **already retired** in ``state``
    by the start of ``year`` relative to the base-year fleet.

    A positive return value means capacity has been subtracted.
    """
    plants = NUCLEAR_PLANTS.get(state, [])
    retired_mw = sum(
        p["capacity_MW"] for p in plants if p["shutdown_year"] <= year
    )
    return retired_mw


def _active_nuclear_mw(state: str, year: int) -> float:
    """Nuclear MW still operating in ``state`` during ``year``."""
    base = BASE_NUCLEAR_MW.get(state, 0.0)
    retired = calculate_nuclear_retirement_impact(state, year)
    return max(0.0, base - retired)


# ---------------------------------------------------------------------------
# Module 4 — State-year autonomy panel
# ---------------------------------------------------------------------------

def build_state_year_autonomy(
    smard_df: pd.DataFrame,
    opsd_path: str | Path = OPSD_PATH,
) -> pd.DataFrame:
    """
    Build a state × year panel with installed renewable capacity, nuclear MW,
    proxy demand, estimated generation, and autonomy index.

    Autonomy index = estimated_generation / estimated_demand
    Values > 1 indicate surplus (state is a net exporter).

    Parameters
    ----------
    smard_df  : Master SMARD DataFrame from smard_client.load_smard_data()
    opsd_path : Path to OPSD renewable_power_plants_DE.csv

    Returns
    -------
    pd.DataFrame with columns:
        state, year, installed_renewable_MW, nuclear_MW,
        estimated_demand_GWh, estimated_generation_GWh, autonomy_index
    """
    # Annual national production from SMARD
    smard_df = smard_df.copy()
    smard_df["year"] = pd.to_datetime(smard_df["datetime"]).dt.year
    years = sorted(smard_df["year"].dropna().unique().astype(int).tolist())

    annual_national = (
        smard_df.groupby("year")
        .agg(
            national_consumption_GWh=("total_consumption_MWh", lambda x: x.sum() / 1000),
            national_renewables_GWh=("total_renewables_MWh",  lambda x: x.sum() / 1000),
        )
        .reset_index()
    )

    # ── Source 1: Official BNetzA/MASTR (2019-2025 Growth) ───────────────────
    official_plants = load_official_state_capacity(years_filter=years)
    
    # ── Source 2: OPSD Legacy (Baseline 2018 mapping) ───────────────────────
    # We keep this as a secondary source or fallback
    try:
        legacy_plants = load_opsd_state_capacity(opsd_path)
    except Exception as e:
        log.warning("[state] OPSD Legacy baseline could not be loaded: %s", e)
        legacy_plants = pd.DataFrame()

    rows = []
    for state in ALL_STATES:
        demand_weight = CONSUMPTION_SHARE.get(state, 0.0)

        for year in years:
            # 1. Renewable Capacity: Prioritize Official BNetzA Reference
            ren_mw = get_bnetza_capacity(state, year)
            
            # If BNetzA reference returns 0 (e.g. year out of range), use OPSD legacy
            if ren_mw <= 0 and not legacy_plants.empty:
                ren_mw = legacy_plants[
                    (legacy_plants["state"] == state) &
                    (legacy_plants["commissioned_year"].fillna(0).astype(int) <= year)
                ]["capacity_MW"].sum()

            # 2. National Denominator for Capacity Factor: 
            # Use sum of ALL official state capacities for THIS year
            total_national_ren_mw = sum(get_bnetza_capacity(s, year) for s in ALL_STATES)
            
            if total_national_ren_mw <= 0 and not legacy_plants.empty:
                total_national_ren_mw = legacy_plants[
                    legacy_plants["commissioned_year"].fillna(0).astype(int) <= year
                ]["capacity_MW"].sum()
            
            if total_national_ren_mw <= 0:
                total_national_ren_mw = 1  # safety avoid zero-div

            # Nuclear capacity active this year
            nuc_mw = _active_nuclear_mw(state, year)

            # Annual national totals
            nat_row = annual_national[annual_national["year"] == year]
            if nat_row.empty:
                continue
            nat_consumption_gwh  = nat_row["national_consumption_GWh"].values[0]
            nat_renewables_gwh   = nat_row["national_renewables_GWh"].values[0]

            # Proxy demand = national consumption × state consumption share
            demand_gwh = nat_consumption_gwh * demand_weight

            # Estimated renewable generation = state renewable MW × national capacity factor
            # national CF = national_renewables_GWh / (total_national_ren_mw × 8.760)
            nat_cf = nat_renewables_gwh / (total_national_ren_mw * 8.760)  if total_national_ren_mw > 0 else 0
            nat_cf = min(nat_cf, 1.0)  # cap at 100 %

            ren_generation_gwh = ren_mw * nat_cf * 8.760  # × 8760h/1000

            # Add nuclear contribution as reliable firm power
            nuc_cf = 0.80  # typical German nuclear capacity factor
            nuc_generation_gwh = nuc_mw * nuc_cf * 8.760

            total_generation_gwh = ren_generation_gwh + nuc_generation_gwh

            autonomy_index = total_generation_gwh / demand_gwh if demand_gwh > 0 else float("nan")

            rows.append(
                {
                    "state":                   state,
                    "year":                    year,
                    "installed_renewable_MW":  round(ren_mw, 1),
                    "nuclear_MW":              round(nuc_mw, 1),
                    "estimated_demand_GWh":    round(demand_gwh, 1),
                    "estimated_generation_GWh": round(total_generation_gwh, 1),
                    "autonomy_index":           round(autonomy_index, 4),
                }
            )

    panel = pd.DataFrame(rows)

    PROCESSED_AUTONOMY.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(PROCESSED_AUTONOMY, index=False)
    log.info(
        "[state] Autonomy panel saved → %s  (%d rows, %d states × %d years)",
        PROCESSED_AUTONOMY,
        len(panel),
        panel["state"].nunique(),
        panel["year"].nunique(),
    )
    return panel


def load_state_autonomy(force_rebuild: bool = False, smard_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Return the state×year autonomy panel, loading from cache when available.

    Parameters
    ----------
    force_rebuild : Rebuild even if cache exists.
    smard_df      : Required if force_rebuild=True or cache missing.
    """
    if PROCESSED_AUTONOMY.exists() and not force_rebuild:
        log.info("[state] Loading autonomy panel from cache: %s", PROCESSED_AUTONOMY)
        return pd.read_csv(PROCESSED_AUTONOMY)
    if smard_df is None:
        raise ValueError(
            "smard_df must be provided when autonomy panel cache does not exist."
        )
    return build_state_year_autonomy(smard_df)
