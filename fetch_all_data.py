"""
fetch_all_data.py — Master data pipeline orchestrator.

Runs all three data-ingestion modules in sequence:
  1. SMARD national generation data (2019-2025)
  2. ENTSO-E day-ahead prices and cross-border flows (2019-2025)
  3. State-level capacity and autonomy gap assembly

Prints confirmation messages and validates four key numbers:
  - Germany renewable share 2019 vs 2024
  - Bavaria autonomy index 2019 vs 2024
  - Maximum day-ahead price in dataset
  - Negative price hours count in 2023
"""

import sys
from pathlib import Path

# Allow running from repo root without installing the package
sys.path.insert(0, str(Path(__file__).parent))

from src.data_ingestion.smard_client import load_smard_data
from src.data_ingestion.entso_client import load_entso_data
from src.data_ingestion.state_processor import load_state_autonomy


def section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def check(label: str, value: str) -> None:
    print(f"  [OK]  {label}: {value}")


def warn(label: str, reason: str) -> None:
    print(f"  [WARN]  {label}: {reason}")


# ---------------------------------------------------------------------------
# Step 1 — SMARD national generation
# ---------------------------------------------------------------------------

def run_smard() -> "pd.DataFrame":  # noqa: F821
    section("STEP 1 — SMARD National Generation (2019-2025)")
    import pandas as pd  # local import to keep top-level imports clean

    smard_df = load_smard_data()
    print(f"\n  Rows loaded : {len(smard_df):,}")
    print(f"  Date range  : {smard_df['datetime'].min()}  to  {smard_df['datetime'].max()}")
    print(f"  Columns     : {list(smard_df.columns)}")
    return smard_df


# ---------------------------------------------------------------------------
# Step 2 — ENTSO-E prices and flows
# ---------------------------------------------------------------------------

def run_entso() -> tuple:
    section("STEP 2 — ENTSO-E Day-Ahead Prices & Cross-Border Flows (2019-2025)")

    prices, flows = load_entso_data()

    if not prices.empty:
        print(f"\n  Prices rows : {len(prices):,}")
        print(f"  Price range : {prices.min():.1f}  to  {prices.max():.1f}  EUR/MWh")
    else:
        warn("Prices", "no data — check ENTSO_E_API_KEY in .env")

    if not flows.empty:
        print(f"\n  Flows rows  : {len(flows):,}")
        print(f"  Flow cols   : {list(flows.columns)}")
    else:
        warn("Flows", "no data — check ENTSO_E_API_KEY in .env")

    return prices, flows


# ---------------------------------------------------------------------------
# Step 3 — State-level autonomy panel
# ---------------------------------------------------------------------------

def run_state_processor(smard_df) -> "pd.DataFrame":  # noqa: F821
    section("STEP 3 — State Autonomy Panel (16 Bundesländer × 2019-2025)")

    panel = load_state_autonomy(force_rebuild=True, smard_df=smard_df)
    print(f"\n  Panel rows  : {len(panel):,}")
    print(f"  States      : {sorted(panel['state'].unique())}")
    return panel


# ---------------------------------------------------------------------------
# Step 4 — Validation prints
# ---------------------------------------------------------------------------

def run_validation(smard_df, prices, panel) -> None:
    import pandas as pd  # noqa: F811

    section("STEP 4 — Key Number Validation")

    # 1. Germany renewable share 2019 vs 2024
    for year in [2019, 2024]:
        yr_data = smard_df[pd.to_datetime(smard_df["datetime"]).dt.year == year]
        if yr_data.empty or "renewable_share" not in yr_data.columns:
            warn(f"Renewable share {year}", "no data")
        else:
            mean_share = yr_data["renewable_share"].mean()
            check(f"Germany renewable share {year}", f"{mean_share:.1%}")

    # 2. Bavaria autonomy 2019 vs 2024
    for year in [2019, 2024]:
        row = panel[(panel["state"] == "Bavaria") & (panel["year"] == year)]
        if row.empty:
            warn(f"Bavaria autonomy {year}", "missing from panel")
        else:
            check(f"Bavaria autonomy index {year}", f"{row['autonomy_index'].values[0]:.3f}")

    # 3. Maximum day-ahead price
    if not prices.empty:
        max_price = prices.max()
        max_ts = prices.idxmax()
        check("Max day-ahead price", f"{max_price:.1f} EUR/MWh  ({max_ts.date()})")
    else:
        warn("Max day-ahead price", "no ENTSO-E price data")

    # 4. Negative price hours 2023
    if not prices.empty:
        prices_2023 = prices[pd.to_datetime(prices.index).year == 2023]
        neg_hours = int((prices_2023 < 0).sum())
        check("Negative price hours 2023", str(neg_hours))
    else:
        warn("Negative price hours 2023", "no ENTSO-E price data")

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  Energy-Autarky-Model-DE — Data Pipeline")
    print("=" * 60)

    smard_df = run_smard()
    prices, flows = run_entso()
    panel = run_state_processor(smard_df)
    run_validation(smard_df, prices, panel)

    print("  Pipeline complete. Processed files saved to data/processed/")
    print("=" * 60 + "\n")
