# Metadata Service

A lightweight metadata and lineage service designed to track datasets and their dependencies across a modern data platform using **Bronze → Silver → Gold** layers.


## 🚀 How to Run

### Using Docker (Recommended)

```bash
docker-compose up --build

API will be available at:

http://localhost:8000/api/v1

### Local (Without Docker)

pip install -r requirements.txt
python main.py


## 🧠 Architecture (Short & Clear)

* **FastAPI** for high-performance REST APIs and automatic OpenAPI docs
* **MySQL** as the metadata store (datasets, columns, lineage)
* **SQLAlchemy ORM** for clean persistence and portability
* **DAG-based lineage model** to represent data flow
* **Cycle detection** enforced at write-time to guarantee acyclic pipelines

### Why this design?

* Mirrors real-world data platforms (Airflow / dbt / OpenMetadata style)
* Prevents invalid pipelines (no self-references or circular dependencies)
* Enables impact analysis (upstream & downstream traversal)


## 📦 Core APIs

### Create Dataset

POST /datasets

Registers datasets with fully-qualified names (connection.database.schema.table).

Used across **Bronze (raw)**, **Silver (cleaned)**, and **Gold (analytics)** layers.


### Create Lineage

POST /lineage

Defines upstream → downstream dependencies between datasets.

Validation rules:

* No self-lineage
* No cycles (DAG enforced)


### Search Metadata

GET /search?query=<text>

Priority-based search:

1. Table name
2. Column name
3. Schema
4. Database

Supports case-insensitive and partial matches.


### Get Dataset Lineage

GET /datasets/{fqn}/lineage

Returns:

* Upstream datasets
* Downstream datasets

Useful for **impact analysis** and **root-cause tracing**.


## 🧪 Test Scenario

The service is validated using a **realistic e-commerce data pipeline**:

Raw (Bronze) → Cleaned (Silver) → Analytics (Gold)

Includes:

* Orders, Customers, Products, Payments, Web Events
* Enrichment & aggregation datasets
* Cycle-detection failure cases
* Metadata search scenarios


## ❌ Error Handling

* **409 Conflict** – Duplicate dataset
* **400 Bad Request** – Lineage cycle / self-reference
* **422 Unprocessable Entity** – Invalid FQN or source type

All APIs return structured responses with status codes and details.


## ✅ Why This Works Well

* Simple, extensible, and production-aligned
* Clean separation of API, service, and persistence layers
* Designed for scale (easy to add versions, ownership, tags, or policies)


