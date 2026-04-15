# 🇩🇪 Germany Energy Autarky Model
### A Three-Layer Spatial Optimization Study of Germany's Energy Transition (2019–2025+)

A research-grade Python pipeline and optimization framework answering a single
question that current energy policy largely ignores:

> **It's not how much Germany builds, it's where.**

This repository models Germany's energy autarky gap at three spatial scales:
16 Bundesländer, 4 grid zones, and 1 national system revealing why the same
megawatt has fundamentally different system value depending on where it lands.

---

## 🧭 The Core Argument

Germany operates a **single national price zone** despite being geographically
and industrially asymmetric. Schleswig-Holstein is saturated with wind.
Bavaria lost its nuclear baseload with no local replacement. NRW carries
the heaviest industrial demand. Yet market signals treat all three identically.

This project quantifies that mismatch layer by layer.

| Layer | Scale | Core Question |
|-------|-------|---------------|
| **1** | 16 Bundesländer | Where does each state stand, and what closed its autarky gap fastest? |
| **2** | 4 Grid Zones | Where should the next MW go — per state — to maximize system value? |
| **3** | 1 Nation | How does zone-level coordination diverge from state-level optimization? |
| **4** | Full System | Least-cost national transition with transmission constraints + sector coupling |

---

## 🔍 What This Project Answers

**Status Quo (Part 1 — Complete)**
- **The Autarky Gap**: How close is each Bundesland to energy self-sufficiency,
  and how has the 2021–2024 expansion boom shifted that?
- **The Value of a Megawatt**: Why is 1 MW of wind in Bavaria often worth more
  to the system than 5 MW in already-saturated Schleswig-Holstein?
- **Nuclear Exit**: What was the measurable impact of Isar 2 and Gundremmingen
  shutdowns on regional energy security?
- **Market Dynamics**: Is the merit order effect empirically visible as
  renewable penetration grows from 2019 to 2025?
- **Data Limitation**: Getting a direct API account for the Marktstammdatenregister
  (MaStR) run by the Bundesnetzagentur (BNetzA) is currently not possible.
  Consequently, Part 1 uses hand-curated data from BNetzA's official PDF/CSV
  Monatsberichte, which will be optimized further by pulling the whole MaStR
  public data dump.

**Optimization (Parts 2–4 — In Progress)**
- What is the least-cost combination of generation and storage per Bundesland
  to close its autarky gap?
- How does the optimal solution change when states are grouped into grid zones
  and allowed to coordinate?
- What does a nationally optimal buildout look like — and how much does it
  diverge from 16 independent state-level solutions?

---

## 📊 Part 1 Visualizations (`notebooks/01_status_quo.ipynb`)

| # | Name | What It Shows |
|---|------|---------------|
| 1 | **Transition Spine** | Monthly generation mix (GWh) with consumption overlay, 2019–2025 |
| 2 | **Animated Autonomy Map** | Year-by-year choropleth of the North-South autarky divide across all 16 states |
| 3 | **Bavarian Deep Dive** | Capacity vs. Generation vs. Demand — the gap between hardware installed and energy produced |
| 4 | **Intermittency Heatmap** | Hour-of-day × day-of-year solar volatility patterns in the German grid |
| 5 | **Crisis Fingerprint** | 2021–2023 energy crisis: direct coupling of gas generation to day-ahead prices |
| 6 | **Merit Order Erosion** | Empirical price suppression during high renewable feed-in — the merit order effect made visible |

### ⚠️ Part 1 Limitations

1. **Generation proxy**: State-level generation is estimated using national capacity factors applied uniformly. This captures *temporal* variability but ignores *spatial* weather differences — Bavaria has higher solar irradiation, Schleswig-Holstein has stronger winds. **Part 2** will resolve this using ERA5 spatially-resolved weather data.

2. **Demand proxy**: State-level demand uses `national_consumption × industrial_consumption_share` (LAK Energiebilanzen). While more accurate than population-based proxies, hourly demand *shapes* are still scaled from the national average, masking intra-day regional differences.

3. **Cross-border flows**: ENTSO-E data represents scheduled *commercial* exchange, not physical loop flows. Germany's role as a transit corridor is not fully captured.

4. **Storage & flexibility**: Pumped hydro, batteries, and demand-side response are not modelled — they are explicitly added in the Part 2 PyPSA optimization.

5. **Data Source Disclosure (MaStR API)**: Getting a direct API account for the **Marktstammdatenregister (MaStR)** run by the Bundesnetzagentur (BNetzA) is currently not possible. Part 1 therefore uses a hand-curated reference layer manually aggregated from official BNetzA *Monatsberichte* (Monthly Reports) and *Zeitreihen zur Entwicklung der erneuerbaren Energien*. This will be optimized in later phases by ingesting the full MaStR public data dump.

---

## 🏗️ Architecture

```text
Energy-Autarky-Model-DE/
├── src/
│   └── data_ingestion/
│       ├── smard_client.py          # SMARD API — 15-min generation & consumption
│       ├── entso_client.py          # ENTSO-E — day-ahead prices & cross-border flows
│       ├── energy_charts_client.py  # Fraunhofer ISE — state-level capacity API
│       ├── bnetza_reference.py      # Official BNetzA/MaStR capacity growth data
│       ├── state_processor.py       # Autonomy index calculation per Bundesland/year
│       └── fetch_all_data.py        # Pipeline orchestrator + validation output
├── notebooks/
│   ├── 01_status_quo.ipynb          # Part 1: Status Quo Diagnostic
│   ├── 02_state_optimization.ipynb  # Part 2: Per-state least-cost optimization (in progress)
│   ├── 03_zone_coordination.ipynb   # Part 3: 4-zone coordination model  (planned)
│   └── 04_national_system.ipynb     # Part 4: Full national + sector coupling (planned)
├── data/
│   ├── raw/                         # API caches (JSON/CSV, per-year)
│   └── processed/                   # Standardized DataFrames
└── README.md
```

---

## 📡 Data Sources

| Source | What It Provides | Status |
|--------|-----------------|--------|
| [SMARD](https://www.smard.de/en) | National electricity generation & consumption (15-min) | Live API |
| [ENTSO-E](https://transparency.entsoe.eu/) | Day-ahead prices + cross-border commercial flows | Live API |
| [Fraunhofer ISE Energy-Charts](https://energy-charts.info/) | State-level installed capacity by technology | Live API |
| **BNetzA / MaStR** | Official German power plant registry — state-level capacity (2019–2025) | Primary reference |
| [OPSD](https://open-power-system-data.org/) | Power plant registry baseline | Legacy fallback |

---

## ⚙️ Key Engineering Features

- **High-Integrity Capacity Data** Replaced static snapshots with official
  BNetzA/MaStR growth trajectories covering 2019–2025. Data is sourced from
  aggregated BNetzA "Monatsberichte" (Monthly Reports) as a high-integrity
  hand-curated baseline.
- **API Maturity Disclosure** Getting a direct API account for the official
  Marktstammdatenregister (MaStR) is currently restricted; Part 1 uses a
  validated reference layer which will be optimized in later phases by
  ingesting the full multi-GB public data dump.
- **Industrial Demand Scaling**  Uses LAK Energiebilanzen consumption shares
  instead of population weights, correctly capturing industrial load centers
  like Bavaria and NRW
- **Smart Caching** Per-state, per-year JSON/CSV caching minimizes API load
  and enables full offline reproducibility
- **Timezone Normalization**  All timestamps standardized to `Europe/Berlin`
  with DST handling
- **Auto-Validation**  Pipeline prints 4 critical integrity checks on
  renewable share, autonomy indices, and price maxima before finalizing output

---

## 🚀 Quickstart

```bash
# 1. Clone and install
git clone https://github.com/amsayed27/energy-autarky-model-DE.git
cd energy-autarky-model-DE
pip install -r requirements.txt

# 2. Add API key
# Create a .env file and add: ENTSO_E_API_KEY=your_key_here

# 3. Fetch all data (SMARD, ENTSO-E, BNetzA)
python src/data_ingestion/fetch_all_data.py

# 4. Open Part 1 analysis
jupyter notebook notebooks/01_status_quo.ipynb
```

> First run fetches from live APIs and builds the local cache.
> All subsequent runs load from CSV — no API calls needed.

---

## 🗺️ Roadmap

| Part | Spatial Layer | Focus | Status |
|------|--------------|-------|--------|
| **1** | 16 Bundesländer | Status quo diagnostic — autonomy gap, merit order, crisis fingerprint | ✅ Complete |
| **2** | 16 Bundesländer | PyPSA least-cost optimization — generation + storage mix per state | 🔄 In progress |
| **3** | 4 Grid Zones | Zone-level coordination — where state-optimal diverges from zone-optimal | 📋 Planned |
| **4** | 1 Nation | Full national system — transmission constraints, sector coupling, heat + transport | 📋 Planned |

---

## 🔬 Research Context

Germany's Energiewende is not a generation problem it is an **allocation problem**.

Part 1 establishes the diagnostic layer: where capacity exists, where demand
sits, and where the gap is widening despite record installations. The key
finding is not a number it is a structural asymmetry. Germany's single
national price zone means a wind turbine in Schleswig-Holstein and a wind
turbine in Bavaria receive identical market signals, despite having
fundamentally different system value.

Parts 2–4 quantify that mismatch progressively: first at the state level,
then across coordinated zones, then as a full national system. The central
research question is how much efficiency is lost — in cost and in carbon —
by optimizing 16 states independently versus optimizing one interconnected
system.

This is the gap between how Germany's energy market is designed and how its
grid actually works.

---

## 👤 Author

**Abdelrahman Gaber**
[LinkedIn](https://www.linkedin.com/in/abdelrahman-mohamed-gaber/) 
[GitHub](https://github.com/amsayed27)