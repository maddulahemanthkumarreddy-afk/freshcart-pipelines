# FreshCart Pipelines: From Git to Gold

**Data Engineering & Pipeline Orchestration (`CS-8037`)**

## Project Overview

This repository implements an end-to-end modern data engineering platform spanning version control, CI/CD automation, Lakehouse ELT on Snowflake, quality-gated machine learning orchestration with Mage AI, and real-time topic-routed event streaming with RabbitMQ.

The project demonstrates how data moves from source ingestion through validation, transformation, orchestration, and event processing into analytics-ready Gold tables.

```text
Working Folder
      |
      v
Local Git ---> Remote Git ---> GitHub Actions CI/CD
                                      |
                                      v
                              FreshCart ELT
                                (Snowflake)
                                      |
                         +------------+------------+
                         |                         |
                         v                         v
                  Gold Data Layer            Mage AI ML Pipeline
                         ^                         |
                         |                         v
                  RabbitMQ Stream <--------- Model Results
                         ^
                         |
                  FreshCart Orders
```

## Repository Structure

```text
freshcart-pipelines/
│
├── README.md                       # Project documentation
├── .gitignore                      # Excludes secrets, environments and temporary files
├── requirements.txt                # Python dependencies
│
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions CI workflow
│
├── tests/
│   └── test_prepare_data.py        # Part A automated tests
│
├── elt/
│   └── freshcart_elt.ipynb         # Part B Snowflake ELT and bonus workflow
│
├── iris_project/
│   ├── metadata.yaml               # Mage pipeline configuration
│   ├── iris_classifier.ipynb       # Iris pipeline execution notebook
│   ├── data_loaders/
│   │   └── load_iris.py
│   ├── transformers/
│   │   ├── prepare_data.py
│   │   ├── train_model.py
│   │   └── evaluate_model.py
│   ├── data_exporters/
│   │   ├── save_results.py
│   │   └── export_metrics.py
│   └── pipelines/
│       └── iris_classifier/
│
├── stream/
│   ├── dispatcher.py               # RabbitMQ event publisher
│   ├── driver.py                   # RabbitMQ consumer/driver
│   ├── bonus_publisher.py          # FreshCart order event publisher
│   └── bonus_consumer.py           # FreshCart order event consumer
│
├── report/                         # Final technical report and deliverables
│
└── screenshots                     # Project execution screenshots
    
```

*The structure represents the main project files. Individual generated Mage files and additional configuration files may vary.*

## System Architecture

### 1. Part A: Git and CI/CD

- Implemented version control using Git and GitHub.
- Maintained the project using meaningful commits and repository updates.
- Configured GitHub Actions for automated testing.
- Used Python 3.12 and Pytest in the CI workflow.
- Implemented `prepare_data` functionality and automated unit tests.
- Verified successful and intentionally failing test executions.
- Used CI validation to ensure code changes pass automated checks.

### 2. Part B: FreshCart ELT on Snowflake

Implemented a multi-layer ELT pipeline using Snowflake.

**Data Flow:**

```text
Web JSON + Mobile JSON
          |
          v
     Bronze Layer
          |
          v
     Silver Layer
          |
     +----+----+
     |         |
     v         v
  Cleaned   Quarantine
   Data     Invalid Data
     |
     v
 Deduplication
     |
     v
 Customer + FX + Geography Enrichment
     |
     v
     Gold Layer
  (gold.fct_orders)
```

Key features:

- Multi-source extraction from Web JSON and Mobile App JSON.
- Raw Bronze landing while preserving source information.
- Silver layer cleansing, validation and format unification.
- Quarantine routing for invalid records.
- Duplicate removal using `QUALIFY ROW_NUMBER()`.
- Latest-record selection using `updated_at`.
- Customer Slowly Changing Dimension Type 2 (SCD Type 2).
- Point-in-time customer tier enrichment.
- Currency conversion using daily FX reference data.
- Pincode-based geographical enrichment.
- Gold fact-table loading using Snowflake `MERGE`.
- Row-hash-based change detection and idempotent loading.
- Data-quality validation, including future order-date checks.

**Recorded baseline results:**

| Metric | Result |
|---|---:|
| Bronze records | 8 |
| Clean records | 6 |
| Quarantined records | 2 |
| Deduplicated records | 4 |
| Gold orders | 4 |
| Gold revenue | ₹7,785 |
| Gold loyalty points | 161 |
| Reject share | 25% |

### 3. Part C: Orchestrated ML Pipeline (Mage AI)

Developed a block-based machine learning pipeline using Mage AI and Scikit-learn for Iris classification.

**Pipeline DAG:**

```text
              load_iris
                  |
                  v
              prepare_data
                  |
                  v
               train_model
                  |
           +------+------+
           |             |
           v             v
       save_results  evaluate_model
                         |
                         v
                   export_metrics
```

Key features:

- Loaded the Iris dataset.
- Prepared training and testing datasets using an 80/20 split.
- Implemented a Decision Tree classification model.
- Used `max_depth` values from 1 to 5 for model comparison.
- Added block-level `@test` validation.
- Prevented prediction export when model validation fails.
- Implemented a separate evaluation branch.
- Generated a confusion matrix.
- Calculated precision, recall and F1-score.
- Exported evaluation metrics to `iris_metrics.csv`.
- Saved model predictions to `iris_predictions.csv`.

**Recorded model result:**

| Parameter | Result |
|---|---:|
| Algorithm | Decision Tree |
| Selected max_depth | 3 |
| Training records | 120 |
| Testing records | 30 |
| Test accuracy | 96.67% |

### 4. Part D: Event Streaming (RabbitMQ and Pika)

Implemented a real-time event-routing system using RabbitMQ and Python Pika.

Key features:

- Created a topic exchange named `rapido-dispatch`.
- Published JSON-based ride and food events.
- Implemented routing keys in the format `city.zone.type`.
- Configured topic-based consumer bindings.
- Demonstrated routing across different cities and zones.
- Implemented message acknowledgement using `basic_ack`.
- Implemented message rejection using `basic_nack`.
- Handled malformed messages using `requeue=False`.
- Demonstrated routing behaviour for matching and non-matching binding patterns.

Main scripts:

- `stream/dispatcher.py`
- `stream/driver.py`

### 5. Bonus: Real-Time FreshCart Stream into the Lakehouse

Extended the RabbitMQ streaming implementation to integrate FreshCart-style order events with the existing Snowflake ELT pipeline.

**Bonus Data Flow:**

```text
FreshCart Order Publisher
          |
          v
     RabbitMQ Exchange
          |
          v
     RabbitMQ Queue
          |
          v
     Bonus Consumer
          |
          v
     60-Second Collection
          |
          v
     write_landing()
          |
          v
     Landing JSON.GZ File
          |
          v
     load_file()
          |
          v
     Bronze Layer
          |
          v
     Silver Cleaning
          |
          v
     Deduplication
          |
          v
     Reference Enrichment
          |
          v
     Gold Fact Table
   (gold.fct_orders)
```

Key features:

- Published FreshCart-style JSON order messages through RabbitMQ.
- Consumed messages from the configured RabbitMQ queue.
- Collected incoming messages during a 60-second window.
- Used message acknowledgements after successful landing-file creation.
- Reused the existing `write_landing()` function.
- Created compressed JSON landing files.
- Loaded streamed data using `load_file()`.
- Reused existing Bronze, Silver, deduplication and enrichment transformations.
- Loaded streamed orders into `gold.fct_orders`.
- Verified the six streamed orders in the Gold fact table.
- Re-executed the Gold `MERGE` operation to validate idempotency.

**Recorded bonus validation:**

| Metric | Result |
|---|---:|
| Streamed orders received | 6 |
| Messages acknowledged | 6 |
| Orders verified in Gold | 6 |
| Streamed order revenue | ₹3,000 |
| Streamed loyalty points | 90 |
| Gold rows before first MERGE | 4 |
| Gold rows after first MERGE | 10 |
| Gold rows after MERGE replay | 10 |

The replay validation confirms that executing the Gold `MERGE` again with the same input does not increase the Gold row count, revenue or loyalty-point totals.

## Technologies Used

| Category | Technologies |
|---|---|
| Programming | Python, SQL |
| Version Control | Git, GitHub |
| CI/CD | GitHub Actions |
| Data Warehouse | Snowflake |
| Data Pipeline Orchestration | Mage AI |
| Machine Learning | Scikit-learn |
| Message Broker | RabbitMQ |
| Messaging Client | Pika |
| Data Processing | Pandas, NumPy |
| Testing | Pytest |

## Setup and Execution

### 1. Clone the repository

```bash
git clone https://github.com/maddulahemanthkumarreddy-afk/freshcart-pipelines.git
cd freshcart-pipelines
```

### 2. Create and activate a Python environment

```bash
python -m venv .venv
```

Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Install the required dependencies:

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Create a local `.env` file for the required Snowflake and RabbitMQ connection settings.

The RabbitMQ bonus scripts use:

```text
RABBITMQ_URL=your_rabbitmq_connection_url
```

Configure Snowflake credentials and connection parameters according to the ELT notebook.

**Important:** Do not commit `.env`, passwords, access tokens or other sensitive connection details to GitHub.

### 4. Execute the FreshCart ELT pipeline

Open:

```text
elt/freshcart_elt.ipynb
```

Configure the Snowflake connection and execute the notebook cells in sequence.

### 5. Execute the Mage AI Iris pipeline

Run the pipeline validation:

```bash
mage run iris_project iris_classifier --test
```

The pipeline loads the Iris dataset, prepares the data, trains the classifier, saves predictions and exports evaluation metrics.

### 6. Execute RabbitMQ event streaming

Start the dispatcher:

```bash
python stream/dispatcher.py
```

Run the consumer in a separate terminal:

```bash
python stream/driver.py
```

Ensure RabbitMQ is running and the required exchange, queues and bindings are configured.

### 7. Execute the FreshCart streaming bonus

Start the FreshCart order publisher:

```bash
python stream/bonus_publisher.py
```

Execute the bonus consumer:

```bash
python stream/bonus_consumer.py
```

The consumer collects order messages, creates a landing file and integrates the data with the existing Snowflake ELT workflow. Follow the notebook workflow for the Bronze-to-Gold transformation and validation.

## Validation and Results

The project includes validation across all major components:

- GitHub Actions automated test execution.
- Python unit testing with Pytest.
- Snowflake data-quality and quarantine checks.
- Deduplication and SCD Type 2 validation.
- Future order-date quality validation.
- Gold `MERGE` idempotency verification.
- Mage AI block-level testing.
- Decision Tree model evaluation.
- Confusion matrix and classification metrics.
- RabbitMQ topic-routing validation.
- Message acknowledgement and rejection handling.
- Verification of streamed FreshCart orders in the Gold fact table.

## Learning Outcomes

This project demonstrates practical implementation of:

1. Git-based version control and CI/CD automation.
2. Multi-source data ingestion and medallion architecture.
3. Snowflake-based ELT pipeline development.
4. Data quality, quarantine and deduplication.
5. Slowly Changing Dimensions Type 2.
6. Machine learning workflow orchestration using Mage AI.
7. Model evaluation and automated block testing.
8. Real-time event streaming using RabbitMQ.
9. Message routing, ACK and NACK handling.
10. Integration of streaming events with a Snowflake Gold data layer.

## Author

**Maddula Hemanth Kumar Reddy**

B.Tech – Computer Science and Engineering  
PDPM IIITDM Jabalpur  
Roll Number: 23BCS153

**GitHub Repository:**  
https://github.com/maddulahemanthkumarreddy-afk/freshcart-pipelines
