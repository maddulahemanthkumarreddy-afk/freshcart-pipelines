# FreshCart Pipelines: From Git to Gold

Data Engineering & Pipeline Orchestration (`CS-8037`)

## Project Overview

This repository implements an end-to-end modern data engineering platform spanning version control, CI/CD, Lakehouse ELT on Snowflake, quality-gated ML orchestration with Mage, and real-time topic-routed event streaming with RabbitMQ.

```
Working Folder -> Local Git -> Remote Git -> CI/CD -> Lakehouse ELT (Snowflake) -> ML Pipeline (Mage) -> Event Stream (RabbitMQ)
```

## Repository Structure

```
freshcart-pipelines/
├── README.md                      # Project documentation and architectural overview
├── .gitignore                     # Ignores .env, *.pkl, *.csv, mage.log, __pycache__/
├── requirements.txt               # Python package dependencies
├── elt/                           # FreshCart Snowflake ELT pipeline (Part B)
│   └── freshcart_elt.ipynb
├── iris_project/                  # Orchestrated ML pipeline with Mage (Part C)
│   ├── metadata.yaml
│   ├── data_loaders/
│   ├── transformers/
│   ├── estimators/
│   └── data_exporters/
├── stream/                        # Event streaming with RabbitMQ (Part D & Bonus)
│   ├── dispatcher.py
│   ├── driver.py
│   └── stream_to_lakehouse.py
├── tests/                         # Automated unit and quality tests (Part A3)
│   └── test_prepare_data.py
├── report/                        # Final technical report & deliverables (Part E)
└── .github/workflows/             # Continuous Integration workflows (Part A3)
    └── ci.yml
```

## System Architecture

1. **Part A: Git & CI/CD**:
   * Version control lifecycle management and automated pull/push testing.
   * GitHub Actions workflow running on Python 3.12 with Pytest verification.

2. **Part B: FreshCart ELT on Snowflake**:
   * Multi-source extraction (Web JSON + Mobile App JSON).
   * Snowflake raw Bronze landing layer preserving source schema fidelity.
   * Silver layer data cleansing, format unification, and quarantine routing.
   * Deduplication using `QUALIFY ROW_NUMBER()`.
   * Slowly Changing Dimensions (SCD Type 2) for customer tier history.
   * Enriched Gold dimensional fact table (`gold.fct_orders`) updated via idempotent `MERGE` with `row_hash`.

3. **Part C: Orchestrated ML Pipeline (Mage AI)**:
   * Block-based ML DAG: `load_iris` -> `prepare_data` -> `train_model` -> `save_results` / `evaluate_model`.
   * Block-level `@test` gatekeepers stopping bad models from exporting predictions.

4. **Part D: Event Streaming (RabbitMQ & Pika)**:
   * Topic exchange (`rapido-dispatch`) routing messages via binding patterns (`city.zone.type`).
   * Worker acknowledgments (`basic_ack`) and dead-letter/rejection handling (`basic_nack`).

5. **Bonus: Real-Time Stream into the Lakehouse**:
   * Consumer bridging RabbitMQ message batches into Snowflake raw landing stage and executing incremental transformation to Gold.
