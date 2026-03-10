# 🔍 DQA — AI Data Quality Agent

> Point it at any PostgreSQL database. It profiles every column, detects anomalies using statistics + AI, and auto-generates dbt tests — all without touching your data.

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688?logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React-18-61DAFB?logo=react&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)
![Claude](https://img.shields.io/badge/Claude-3.5_Sonnet-D4A843?logo=anthropic&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-schema.yml-FF694B?logo=dbt&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

---

## 🚀 Run Locally (2 commands)

```bash
cp .env.example .env          # Add your ANTHROPIC_API_KEY
docker-compose up --build     # Starts at localhost:3000
```

Includes a **sample e-commerce database** with intentional data quality issues pre-seeded — so you can try the agent immediately without connecting a real database.

Sample DB connection string (pre-filled in the UI):
```
postgresql://sample:sample_secret@sample_db:5432/ecommerce
```

---

## 🎯 What It Does

Data quality monitoring is one of the most expensive unsolved problems in data engineering. Tools like Monte Carlo, Soda, and Bigeye charge $50k+/year for enterprise licenses. This agent does the core job:

1. **Connect** — add any PostgreSQL connection string
2. **Profile** — runs SQL statistics on every column: null rates, distributions, percentiles, cardinality
3. **Detect** — statistical anomaly detection using IQR, null thresholds, inconsistency checks
4. **Interpret** — Claude explains WHY each issue matters in business terms and HOW to fix it
5. **Generate** — auto-creates a dbt `schema.yml` with `not_null`, `unique`, `accepted_values`, and `relationships` tests
6. **Export** — download the schema.yml and drop it into your dbt project

**Issues the agent detects:** High null rates · Extreme statistical outliers · Negative values in non-negative columns · Inconsistent category casing (COMPLETED vs completed) · Zero values in quantity/price columns · Future dates · Constant columns · Empty tables

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                   React Frontend (port 3000)                  │
│  Dashboard → Connections → Run Analysis → History → Profiles │
└────────────────────────┬─────────────────────────────────────┘
                          │ HTTP (Axios)
┌────────────────────────▼─────────────────────────────────────┐
│                  FastAPI Backend (port 8000)                  │
│                                                               │
│  /connections  /runs  /profiles  /findings  /export/dbt       │
│                                                               │
│      ┌──────────────────────────────────────────────┐        │
│      │           _run_analysis() pipeline           │        │
│      │                                               │        │
│      │  DatabaseProfiler → AnomalyDetector          │        │
│      │         ↓                    ↓                │        │
│      │  ColumnProfile[]      AnomalyFlag[]           │        │
│      │         └──────────────────┘                  │        │
│      │                    ↓                          │        │
│      │           DataQualityAgent                    │        │
│      │         (Claude 3.5 Sonnet)                   │        │
│      │                    ↓                          │        │
│      │       Enriched findings + summary             │        │
│      └──────────────────────────────────────────────┘        │
└────────────────────────┬─────────────────────────────────────┘
                          │ SQLAlchemy ORM
┌────────────────────────▼─────────────────────────────────────┐
│            DQA Metadata DB — PostgreSQL (port 5433)           │
│  connections │ agent_runs │ column_profiles │ anomaly_findings │
└──────────────────────────────────────────────────────────────┘
                          
┌──────────────────────────────────────────────────────────────┐
│            Sample Target DB — PostgreSQL (port 5434)          │
│  customers │ products │ orders │ order_items                  │
│  (pre-seeded with intentional DQ issues for demo)            │
└──────────────────────────────────────────────────────────────┘
```

---

## 📁 File Structure & What Each File Does

This README is designed so you can read it BEFORE opening any file, and already understand what every piece does.

```
data-quality-agent/
├── docker-compose.yml            # Orchestrates 4 services: meta DB, sample DB, backend, frontend
├── .env.example                  # Copy to .env, add ANTHROPIC_API_KEY
│
├── sample_data/
│   └── seed.sql                  # ★ E-commerce schema with intentional DQ bugs seeded in
│                                 #   Issues: negative stock, null emails, zero orders, status casing
│
├── backend/
│   ├── Dockerfile                # Python 3.11 + libpq (postgres client)
│   ├── requirements.txt          # FastAPI, SQLAlchemy, anthropic, pandas, scipy, PyYAML
│   ├── main.py                   # ★ FastAPI app + all routes + _run_analysis() background task
│   │                             #   _run_analysis() is the full pipeline: profile → detect → AI → save
│   │
│   ├── database/
│   │   └── db.py                 # ★ 4 SQLAlchemy table models for the METADATA database
│   │                             #   Connection, AgentRun, ColumnProfile, AnomalyFinding
│   │                             #   NOTE: this is NOT the DB being analyzed — it stores results
│   │
│   ├── models/
│   │   └── schemas.py            # Pydantic models for API request/response shapes
│   │
│   ├── services/
│   │   ├── profiler.py           # ★★ SQL profiler — connects to target DB, runs stats queries
│   │   │                         #   Returns ColumnProfile dataclasses with null_rate, percentiles,
│   │   │                         #   top_values, etc. Handles numeric vs categorical separately.
│   │   │                         #   WHY: statistics first, AI second — faster + cheaper + auditable
│   │   │
│   │   ├── anomaly_detector.py   # ★★ Deterministic anomaly checks (no AI, pure math)
│   │   │                         #   Checks: null_rate, negative_values, IQR outliers, zero_values,
│   │   │                         #   inconsistent_categories (casing), constant columns, empty tables
│   │   │                         #   WHY: statistical detection before AI = cheaper + reproducible
│   │   │
│   │   └── dbt_generator.py      # ★ Converts ColumnProfile → dbt schema.yml YAML
│   │                             #   Generates: not_null, unique, accepted_values, relationships,
│   │                             #   expression_is_true tests based on what the profile shows
│   │
│   └── agents/
│       └── dq_agent.py           # ★★★ THE BRAIN — Claude interprets statistical findings
│                                 #   Phase 1: Per-table AI enrichment (business context + fix suggestions)
│                                 #   Phase 2: Cross-table synthesis → executive summary
│                                 #   WHY TWO PHASES: per-table catches column specifics,
│                                 #   cross-table catches systemic ETL patterns
│
└── frontend/
    ├── Dockerfile                # Node 20 + Vite
    ├── package.json              # React 18, Tailwind CSS, Axios
    ├── vite.config.js            # Vite config with /api proxy to backend
    ├── index.html                # HTML entry
    │
    └── src/
        ├── main.jsx              # React entry — renders <App />
        ├── App.jsx               # ★ Root — sidebar nav + imports all panels
        ├── index.css             # Tailwind + reusable component classes
        │
        ├── utils/
        │   └── api.js            # All backend API calls — never call axios directly from components
        │
        └── components/
            ├── Dashboard.jsx     # Stats cards + how-it-works flow + critical issue alert
            ├── Connections.jsx   # Add/delete DB connections + sample DB helper
            ├── RunPanel.jsx      # ★ Contains 3 exported components:
            │                     #   RunPanel     — trigger analysis + pipeline explainer
            │                     #   RunHistory   — list runs + view findings + poll while running
            │                     #   ProfileViewer — browse column stats from any completed run
            │                     #   WHY ONE FILE: these 3 share run state and polling logic
```

---

## 🧠 How the Analysis Pipeline Works

Read this section before your first interview about this project.

### Step 1: Statistical Profiling (`services/profiler.py`)

Connects to the target database (read-only) and runs SQL against every table. For each column:

- **All types:** `COUNT(*)`, null count, unique count → null_rate, unique_rate
- **Numeric columns:** `MIN`, `MAX`, `AVG`, `STDDEV`, `PERCENTILE_CONT` at p25/p50/p75/p99
- **Categorical columns:** `GROUP BY` value distribution → top_values dict

**Why percentiles instead of just min/max?**
Min/max are dominated by extreme outliers. If p99 = $800 but max = $999,999, the max is clearly an outlier. Claude uses the p99 vs max gap to identify extreme outlier rows without needing access to raw data.

### Step 2: Deterministic Detection (`services/anomaly_detector.py`)

Pure statistics — zero AI, zero API cost. Checks every ColumnProfile for:

| Check | Method |
|---|---|
| High null rate | null_rate > 5% → warning, > 20% → critical |
| Negative values | min_value < 0 on columns like price, stock, quantity |
| Statistical outliers | IQR method: max > Q3 + 3×IQR and max >> p99 |
| Zero values | min = 0 on amount/quantity columns where median > 0 |
| Inconsistent casing | Groups top_values by lowercase → finds COMPLETED/completed/Completed |
| Constant columns | unique_count = 1 across all rows |
| Empty tables | row_count = 0 |

### Step 3: AI Enrichment (`agents/dq_agent.py`)

Claude receives the statistical profile + pre-screened flags and:

1. **Per-table analysis** — interprets each flag in business context. "Column `email` has 8% nulls" becomes: "In a customers table, 8% null emails likely means users skipped optional signup fields — this will break email marketing campaigns."

2. **Executive summary** — synthesizes all tables to identify systemic patterns. A bad ETL job shows up as multiple tables with the same issue at the same timestamp — Claude recognizes this.

3. **Tool-calling** — uses structured output tools to guarantee findings are parseable JSON, stored in the DB, and displayable in the UI.

### Step 4: dbt Test Generation (`services/dbt_generator.py`)

Converts the column profiles into a `schema.yml`. Logic:
- `null_rate == 0.0` → generate `not_null` test
- `unique_rate == 1.0` → generate `unique` test  
- Low cardinality column → generate `accepted_values` with canonical (lowercase) values
- Column name contains price/amount/quantity + min ≥ 0 → `expression_is_true: col >= 0`
- Column name ends with `_id` → generate `relationships` template

---

## 🔑 Key Design Decisions

| Decision | Why |
|---|---|
| **Statistics first, AI second** | Deterministic checks are instant, free, and auditable. AI adds interpretation on top — not detection. |
| **Separate metadata DB from target DB** | The agent NEVER modifies the database it analyzes. All results go to the metadata DB. |
| **Background tasks for analysis** | Analysis takes 30–60s. Starting it async and polling prevents HTTP timeouts. |
| **Per-field percentiles** | Percentiles detect outliers without accessing raw rows — crucial for large/sensitive tables. |
| **IQR method for outliers** | More robust than z-score for non-normal distributions (most business data). |
| **dbt output as final deliverable** | Engineers can drop `schema.yml` directly into a dbt project — zero reformatting needed. |
| **Two-phase AI analysis** | Per-table first catches column-level detail, cross-table synthesis catches systemic patterns. |

---

## 📡 API Reference

Full interactive docs at `http://localhost:8000/docs` after running.

| Method | Endpoint | What it does |
|---|---|---|
| `POST` | `/connections` | Save + test a DB connection string |
| `GET` | `/connections` | List saved connections |
| `POST` | `/runs` | Start an analysis run (async — poll for results) |
| `GET` | `/runs` | List all runs |
| `GET` | `/runs/{id}` | Get run status + summary |
| `GET` | `/runs/{id}/profiles` | Get column profiles for a run |
| `GET` | `/runs/{id}/findings` | Get anomaly findings (filter by severity) |
| `POST` | `/export/dbt` | Download dbt schema.yml for a run |
| `GET` | `/stats` | Dashboard statistics |

---

## 🐛 Sample Issues the Agent Detects

The included `sample_data/seed.sql` seeds an e-commerce database with these intentional issues:

| Table | Column | Issue | What the agent reports |
|---|---|---|---|
| `customers` | `email` | ~8% null rate | "Elevated null rate — will break email marketing campaigns" |
| `customers` | `signup_date` | Future dates | "Date column contains future timestamps — likely data entry error" |
| `products` | `stock_count` | Negative values | "stock_count has negative values — impossible inventory count, likely ETL bug" |
| `orders` | `amount` | Extreme outlier ($999,999) | "Max value 999,999 is 1,249× the p99 value — extreme outlier" |
| `orders` | `amount` | Zero orders | "Zero values in amount column where median is $400 — invalid order records" |
| `orders` | `status` | COMPLETED/completed/Completed | "Inconsistent casing breaks GROUP BY queries and reporting" |
| `order_items` | `quantity` | Zero quantities | "Zero values in quantity column — invalid line items" |

---

## 🧰 Tech Stack

| Layer | Technology | Why |
|---|---|---|
| AI | Claude 3.5 Sonnet (Anthropic) | Tool-calling for structured output, business-context reasoning |
| Backend | FastAPI (Python) | Async background tasks, auto OpenAPI docs |
| ORM | SQLAlchemy 2.0 | Type-safe queries for metadata DB |
| DB | PostgreSQL 15 | JSONB for flexible stats storage |
| Stats | pandas + scipy | Percentile calculations, IQR outlier detection |
| dbt Output | PyYAML | Generates standards-compliant schema.yml |
| Frontend | React 18 + Vite | Fast dev, polling for async run status |
| Styling | Tailwind CSS | Consistent utility-first styling |
| HTTP | Axios | All API calls centralized in api.js |

---

## 💡 Skills Demonstrated

- **Data Engineering:** Column profiling pipeline, SQL stats queries, anomaly detection, dbt integration
- **AI Engineering:** Multi-phase Claude agent, tool-calling for structured output, prompt engineering for business interpretation
- **Backend:** Background task execution, async polling pattern, SQLAlchemy ORM, RESTful API design
- **Frontend:** React component architecture, real-time status polling, conditional rendering
- **DevOps:** Docker Compose multi-service setup with health checks and dependency ordering

---

## 📄 License
MIT
