# 🏔️ Data Lakehouse Platform — Production-grade

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-blue?logo=python">
  <img src="https://img.shields.io/badge/Go-1.22-cyan?logo=go">
  <img src="https://img.shields.io/badge/Apache%20Spark-3.5-orange?logo=apachespark">
  <img src="https://img.shields.io/badge/Apache%20Iceberg-1.4-blue">
  <img src="https://img.shields.io/badge/Apache%20Airflow-2.8-red?logo=apacheairflow">
  <img src="https://img.shields.io/badge/dbt-1.7-red?logo=dbt">
  <img src="https://img.shields.io/badge/ClickHouse-24.3-yellow?logo=clickhouse">
  <img src="https://img.shields.io/badge/MLflow-2.10-blue">
  <img src="https://img.shields.io/badge/MinIO-S3--compatible-purple">
  <img src="https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql">
  <img src="https://img.shields.io/badge/Streamlit-1.39-red?logo=streamlit">
  <img src="https://img.shields.io/badge/Docker-Compose-blue?logo=docker">
</p>

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           SOURCES                                   │
│   E-commerce Events — 80-200 orders/min                             │
└────────────────────────────┬────────────────────────────────────────┘
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE — MinIO (s3a://bronze/)                                  │
│   orders/year=*/month=*/day=*/*.parquet                             │
│   customers/snapshot_*.parquet  │  products/snapshot_*.parquet      │
│   Format: Parquet + Snappy  │  Raw landing  │  Immutable            │
└──────────────────────┬──────────────────────────────────────────────┘
                       │  Spark bronze_to_silver.py
                       │  Deduplication · Null filtering
                       │  Schema enforcement · 11 DQ checks
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥈 SILVER — Apache Iceberg (HadoopCatalog → MinIO)                 │
│   lakehouse.silver.orders  │  customers  │  products                │
│   ACID · Time travel · Schema evolution · Format v2                 │
└──────────────────────┬──────────────────────────────────────────────┘
                       │  Spark silver_to_gold.py
                       │  daily_sales · product_performance
                       │  customer_segments (RFM)
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥇 GOLD — Iceberg + PostgreSQL (synced)                            │
│                                                                     │
│   Iceberg Gold ──────────────────────────────────────────────────   │
│   lakehouse.gold.daily_sales                                        │
│   lakehouse.gold.product_performance                                │
│   lakehouse.gold.customer_segments                                  │
│                                                                     │
│   PostgreSQL Gold (serving layer) ────────────────────────────────  │
│   gold.daily_sales · gold.product_performance · gold.customer_ltv  │
│   gold.revenue_trends · gold.top_products  ← dbt transformations   │
│                                                                     │
│   ml.customer_features  ← ML Feature Pipeline (Spark + scikit)     │
└──────┬────────────────────────┬────────────────────────────────────┘
       │                        │
       ▼                        ▼
┌─────────────────┐   ┌──────────────────────────────┐
│  ClickHouse     │   │   MLflow :5001               │
│  OLAP Engine    │   │   Experiment tracking         │
│  :8123          │   │   Model registry              │
│  ReplacingMerge │   │   churn_model · ltv_model     │
│  Tree tables    │   └──────────────────────────────┘
│  <1s analytics  │
└────────┬────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────┐
│                        Go API :8080                              │
│   16 REST endpoints — PostgreSQL + ClickHouse + ML               │
└──────────────────┬───────────────────────────────────────────────┘
                   │
       ┌───────────┴──────────┐
       ▼                      ▼
┌──────────────┐    ┌─────────────────────────┐
│  Streamlit   │    │  Apache Airflow :8082    │
│   :8501      │    │  CeleryExecutor          │
│  6 tabs      │    │  3 DAGs                  │
│  dark theme  │    │  lakehouse_pipeline      │
└──────────────┘    │  ml_pipeline             │
                    │  lakehouse_maintenance   │
                    └─────────────────────────┘
```

---

## ⚙️ Tech Stack

| Layer | Technology | Rôle |
|-------|------------|------|
| Pipelines | Python 3.11 | Ingestion, Spark jobs, DAGs |
| API | Go 1.22 | REST API haute performance |
| Batch processing | Apache Spark 3.5 | Transformations Bronze→Silver→Gold |
| Table format | Apache Iceberg 1.4 | ACID, time travel, schema evolution |
| SQL transformations | dbt 1.7 | Modèles analytiques Gold |
| **OLAP analytics** | **ClickHouse 24.3** | **Requêtes analytiques <1s** |
| ML tracking | MLflow 2.10 | Experiment tracking + model registry |
| ML models | scikit-learn 1.3.2 | GradientBoosting churn + RF LTV |
| Orchestration | Apache Airflow 2.8 | CeleryExecutor, 3 DAGs |
| Celery broker | Redis 7 | Task queue |
| Object storage | MinIO | Stockage S3-compatible local |
| Serving layer | PostgreSQL 16 | API backend + Iceberg catalog |
| Dashboard | Streamlit 1.39 | Dashboard analytique 6 onglets |

---

## ⚡ ClickHouse — Couche Analytics OLAP

ClickHouse est le moteur OLAP columnar qui complète l'architecture. Là où PostgreSQL sert les requêtes API en `<5ms` sur des milliers de lignes, ClickHouse est optimisé pour les agrégations analytiques sur des **milliards de lignes en moins d'une seconde**.

### Rôle dans l'architecture

```
PostgreSQL Gold  →  ClickHouse (sync après chaque run dbt)
                        ↓
                 Dashboards analytiques
                 Requêtes ad-hoc BI
                 Agrégations massives
```

### Tables ClickHouse

| Table | Moteur | Usage |
|-------|--------|-------|
| `daily_sales` | ReplacingMergeTree | Revenus par jour |
| `product_performance` | ReplacingMergeTree | Performance produits |
| `customer_segments` | ReplacingMergeTree | Segments + scores churn |
| `revenue_trends` | ReplacingMergeTree | Tendances revenue |

### Endpoints API ClickHouse

```bash
GET /api/v1/clickhouse/stats          # Stats globales OLAP
GET /api/v1/clickhouse/top-products   # Top produits par revenue
GET /api/v1/clickhouse/churn-stats    # Distribution risque churn
```

### Interface HTTP native

```bash
# Requête directe ClickHouse
curl "http://localhost:8123/?query=SELECT+count()+FROM+lakehouse.daily_sales&user=lakehouse&password=lakehouse"
```

### Différence ClickHouse vs PostgreSQL

| | PostgreSQL | ClickHouse |
|---|---|---|
| **Optimisé pour** | Transactions OLTP | Analytics OLAP |
| **Latence API** | `<5ms` (index B-tree) | `<1s` (columnar scan) |
| **Agrégations massives** | Lent à 1B+ lignes | Rapide à 1B+ lignes |
| **UPDATE / DELETE** | Natif | Limité |
| **Cas d'usage** | API serving | Dashboards BI, ad-hoc |

### En production

ClickHouse remplace Redshift, BigQuery ou Snowflake pour les besoins analytiques — même pattern qu'Uber, Cloudflare ou Yandex qui traitent des pétaoctets avec ClickHouse.

---

## 🤖 ML Pipeline

```
Iceberg Silver (orders)
    ↓ Spark Feature Pipeline
    ↓ RFM features (recency, frequency, monetary)
    ↓ GradientBoosting → churn_probability
    ↓ RandomForest → LTV prediction
    ↓
MLflow (experiment tracking + model registry)
    +
PostgreSQL ml.customer_features (500 customers, scores en temps réel)
    ↓
Go API → Streamlit ML Insights
```

### Endpoints ML

```bash
GET /api/v1/ml/features/summary     # Vue globale churn/LTV
GET /api/v1/ml/churn-risk           # Clients à haut risque
GET /api/v1/ml/customer/{id}        # Analyse individuelle
```

---

## 🗃️ Apache Iceberg Features

| Feature | Usage |
|---------|-------|
| **ACID transactions** | Pas de corruption si Spark crash |
| **Time travel** | `SELECT * FROM orders FOR TIMESTAMP AS OF '...'` |
| **Schema evolution** | Ajout de colonnes sans réécriture |
| **Hidden partitioning** | `partitionedBy(months("created_at"))` |
| **Snapshot history** | `SELECT * FROM orders.snapshots` |

---

## 🔍 Data Quality — Silver Layer

| Table | Check | Règle |
|-------|-------|-------|
| orders | `no_null_order_id` | PK not null |
| orders | `positive_amount` | total_amount > 0 |
| orders | `valid_status` | completed/refunded/cancelled |
| orders | `valid_quantity` | 1 ≤ qty ≤ 100 |
| orders | `non_negative_discount` | 0 ≤ discount ≤ 1 |
| customers | `no_null_customer_id` | PK not null |
| customers | `valid_email` | Contient @ |
| customers | `valid_segment` | standard/premium/vip |
| products | `no_null_product_id` | PK not null |
| products | `positive_price` | base_price > 0 |
| products | `valid_rating` | 0 ≤ rating ≤ 5 |

---

## 📊 dbt Models (Gold)

| Modèle | Description |
|--------|-------------|
| `revenue_trends` | Revenue journalier + rolling avg 7j + croissance MoM |
| `top_products` | Top produits par revenue + part de marché |
| `customer_ltv` | RFM scoring → champion/loyal/new/at_risk/regular |

---

## ✈️ Airflow DAGs

### `lakehouse_pipeline` (toutes les heures)

```
check_bronze_data
    → bronze_to_silver
    → dq_gate ──→ dq_failed → end
             └─→ dq_passed
                    → silver_to_gold
                    → dbt_run
                    → dbt_test
                    → [health_check, clickhouse_sync]
                    → iceberg_maintenance
                    → log_completion
```

### `ml_pipeline` (toutes les 6h)

```
check_silver_ready → feature_pipeline → log_ml_results
```

### `lakehouse_maintenance` (daily 02:00 UTC)

Cleanup historique · MinIO stats · MAJ catalog

---

## 🚀 Quick Start

```powershell
# Windows
$env:DOCKER_BUILDKIT=0
docker compose up --build

# Lancer le ML pipeline
docker compose run --rm ml-feature-pipeline

# Sync ClickHouse
docker compose run --rm clickhouse-sync
```

---

## 🌐 Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit Dashboard | http://localhost:8501 | — |
| Go API | http://localhost:8080 | — |
| Airflow UI | http://localhost:8082 | admin / admin |
| Airflow Flower | http://localhost:5555 | — |
| MLflow | http://localhost:5001 | — |
| ClickHouse HTTP | http://localhost:8123 | lakehouse / lakehouse |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | lakehouse / lakehouse |

---

## 📡 API Endpoints (16 total)

```bash
# Core
GET /health
GET /api/v1/summary
GET /api/v1/daily-sales?days=30
GET /api/v1/revenue-trends
GET /api/v1/top-products?limit=10
GET /api/v1/customer-segments
GET /api/v1/customer/{id}/ltv
GET /api/v1/pipeline-runs
GET /api/v1/catalog
GET /api/v1/dq-results

# ML
GET /api/v1/ml/features/summary
GET /api/v1/ml/churn-risk
GET /api/v1/ml/customer/{id}

# ClickHouse OLAP
GET /api/v1/clickhouse/stats
GET /api/v1/clickhouse/top-products
GET /api/v1/clickhouse/churn-stats
```

---

## 📁 Project Structure

```
lakehouse/
├── ingestion/
│   ├── generate_data.py          # Générateur e-commerce → MinIO bronze
│   └── Dockerfile
├── spark/
│   ├── jobs/
│   │   ├── bronze_to_silver.py   # Nettoyage + DQ + écriture Iceberg
│   │   └── silver_to_gold.py     # Agrégations + sync PostgreSQL
│   └── Dockerfile                # Spark 3.5 + Iceberg 1.4 JARs
├── dbt_project/
│   ├── models/gold/
│   │   ├── revenue_trends.sql
│   │   ├── top_products.sql
│   │   ├── customer_ltv.sql
│   │   └── schema.yml
│   └── dbt_project.yml
├── clickhouse/                   # ← NOUVEAU
│   ├── init/01_schema.sql        # Tables MergeTree OLAP
│   ├── sync.py                   # Sync PostgreSQL → ClickHouse
│   └── Dockerfile
├── ml/
│   └── pipelines/
│       ├── feature_pipeline.py   # Spark RFM + GradientBoosting + RF
│       └── Dockerfile
├── mlflow/
│   └── Dockerfile
├── airflow/
│   ├── dags/
│   │   ├── lakehouse_pipeline.py    # DAG horaire (+ clickhouse_sync)
│   │   ├── ml_pipeline.py           # DAG ML toutes les 6h
│   │   └── lakehouse_maintenance.py # DAG maintenance quotidien
│   └── Dockerfile
├── api/
│   ├── main.go                   # Go REST API — 16 endpoints
│   └── Dockerfile
├── streamlit/
│   └── app.py                    # Dashboard 6 onglets dark
├── postgres/init/
│   └── 01_schema.sql
└── docker-compose.yml            # 32+ containers
```

---

## 📝 License

MIT
# Data-Lakehouse-Platform
