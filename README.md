# 🔺 TSS — Triangulation Signal System v1.0
### Apex-Engine | Système de Signal Autonome par Triangulation de Marchés

---

## 📐 PRINCIPE FONDAMENTAL

> *Si N marchés indépendants encodent la même réalité sous-jacente, leur intersection probabiliste converge vers une estimation plus précise qu'aucun marché pris seul.*

Le TSS est un système de signal **autonome et indépendant d'APEX-ENGINE**.
Il détecte les incohérences de prix entre marchés corrélés pour identifier des bets à valeur positive.

---

## 🏗️ ARCHITECTURE — 7 LAYERS

```
LAYER 0 — Data Intake          : Snapshot cotes multi-sources + détection mouvement ligne
LAYER 1 — Démarginalisation    : Conversion cotes → probabilités nettes (méthode Shin)
LAYER 2 — Triangulation Core   : 3 modules en parallèle (A=BTTS, B=O/U Poisson, C=Score Vector)
LAYER 3 — Signal Engine        : Δ + IC + EV + SDT → décision BET/NO BET
LAYER 4 — Calibration Layer    : Corrections par ligue, AH, mouvement, saison
LAYER 5 — Risk Engine          : Kelly fractionné + bankroll TSS séparée (30%)
LAYER 6 — Output               : Telegram (ApexSiriusBot) + SignalStore JSON
```

---

## ⚙️ INSTALLATION

```bash
git clone https://github.com/okomaleonce-commits/Apex-TSS.git
cd Apex-TSS
pip install -r requirements.txt
```

---

## 🚀 UTILISATION RAPIDE

```python
from tss.orchestrator import TSS
from tss.layer5_risk_engine import BankrollConfig

tss = TSS(bankroll_config=BankrollConfig(total_bankroll=500))

odds = {
    "1x2":         {"home": 2.10, "draw": 3.40, "away": 3.60},
    "over25":      {"over": 1.85, "under": 2.05},
    "over15":      {"over": 1.40, "under": 3.10},
    "btts":        {"yes": 1.90, "no": 2.00},
    "home_over05": {"over": 1.55, "under": 2.60},
    "away_over05": {"over": 2.10, "under": 1.80},
    "ah":          {"home_line": -0.5, "home_odds": 2.08, "away_odds": 1.85}
}

signals = tss.analyze_match(
    home="Napoli", away="Lazio",
    league="serie_a", kickoff="2026-04-09T20:45:00",
    odds_dict=odds,
    target_markets=["btts", "over25"]
)
```

Ou via CLI :
```bash
python main.py --bankroll 1000
python main.py --telegram-token TOKEN --chat-id CHAT_ID
```

---

## 📊 MÉTRIQUES DE SIGNAL

| Métrique | Description | Seuil minimal |
|---|---|---|
| **Δ (Delta)** | P_synth − P_réelle | ≥ 8% |
| **IC** | Indice de Convergence entre modules | ≥ 0.85 |
| **EV** | Valeur espérée nette | ≥ +5% |
| **SDT** | Score de Déclenchement Total | ≥ 0.60 |

---

## 📁 STRUCTURE DU PROJET

```
Apex-TSS/
├── tss/
│   ├── layer0_data_intake.py
│   ├── layer1_demarginalisation.py
│   ├── layer2_triangulation.py
│   ├── layer3_signal_engine.py
│   ├── layer4_calibration.py
│   ├── layer5_risk_engine.py
│   ├── layer6_output.py
│   └── orchestrator.py
├── main.py
├── backtesting.py
├── config.json
└── requirements.txt
```

---

## ⚠️ DISCLAIMER

Ce système est un outil d'analyse probabiliste.
Les performances passées ne garantissent pas les performances futures.

---

## v2.0 — Walk-Forward Backtesting Engine

### New modules

| File | Role |
|------|------|
| `tss/fbref_scraper.py` | FBref scraper + SQLite cache (10 leagues) |
| `tss/backtest_engine.py` | Dixon-Coles + Walk-Forward splitter + TSS gates |
| `tss/results_analyzer.py` | ROI / gate calibration / market heatmap |
| `tss/odds_loader.py` | football-data.co.uk CSV downloader + Shin demarg merger |
| `tss/alternative_odds_loader.py` | Brazil / A-League / AFC CL (OddsPortal + manual import + synthetic fallback) |
| `tss/pdf_report.py` | 6-page PDF report (equity curve, heatmap, gate grid, distributions) |
| `oddsportal_scraper.py` | Standalone Selenium scraper for alternative leagues |
| `backtesting.py` | Unified CLI orchestrator — all pipelines |
| `requirements_backtest.txt` | Python dependencies for backtest stack |

### Quick start

```bash
pip install -r requirements_backtest.txt

# Smoke test (synthetic data, ~2 min)
python backtesting.py --smoke-test

# Full pipeline: 7 FDCO leagues + 3 alternative leagues
python backtesting.py --all --seasons 2022-2023 2023-2024 2024-2025

# Calibrate gates on existing signals
python backtesting.py --calibrate

# Generate PDF report from existing signals CSV
python tss/pdf_report.py --signals reports/signals_<ts>.csv
```

### Architecture

```
FBref xG data + football-data.co.uk odds + OddsPortal (alt leagues)
                          ↓
                 Unified dataset (10 leagues)
                          ↓
          Dixon-Coles Walk-Forward (season splits)
          Gate-0 DCS → Gate-1 EV → Gate-2 Edge → Gate-3 Odds
          Kelly fractional staking
                          ↓
           PDF Report: ROI / Drawdown / Heatmap / Gate grid
```
