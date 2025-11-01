# **NOAA AIS Data Pipeline**

PySpark and AWS Glue pipeline processing ~455 GB of vessel tracking data from the **National Oceanic and Atmospheric Administration (NOAA)** via the **Automatic Identification System (AIS)**. Handles continuous AIS feeds from U.S. waters to produce analytics-ready datasets in Amazon S3 for large-scale maritime intelligence and backend API integration.

---

## **Overview**

This project implements a complete **data engineering workflow** to process high-volume maritime data. The pipeline ingests daily AIS files from NOAA’s public repository, performs schema standardization, geospatial transformations, and builds curated and analytical datasets optimized for downstream analytics and visualization.

The system is designed for scalability, modularity, testability, and cost-efficiency using AWS native services.

---

## **Architecture**

**Data Flow:**

NOAA AIS Data (Raw CSVs)
│
▼
S3 (Raw Layer)
│
▼
AWS Glue / PySpark
├── Raw → Staging (schema, cleaning)
└── Staging → Curated (enrichment, transformations)
│
▼
S3 (Curated Layer)
│
▼
FastAPI / Dashboard Integration

---

## **Key Features**

* Processes > 450 GB of AIS vessel data efficiently using distributed PySpark jobs.
* Modular three-stage pipeline: **Raw → Staging → Curated**.
* Integrates lookup mappings for vessel type, cargo, and transceiver classification.
* Produces analytics-ready Parquet datasets for querying and visualization.
* Implements **unit testing** for PySpark transformations to ensure data integrity and maintainability.
* Ready for integration with **FastAPI** or BI dashboards (React / PowerBI).

---

## **Repository Structure**

noaa-ais-pipeline/
│
├── pipelines/
│   ├── raw_to_staging.py
│   └── staging_to_curated.py
│
├── transformations/
│   ├── facts/
│   ├── dims/
│   └── notebooks/
│
├── views/
│   └── view_definitions.sql
│
├── ddl_scripts/
│   └── table_ddl_scripts.sql
│
├── utils/
│   ├── schema_definitions.py
│   ├── lookups.py
│   ├── config.py
│   └── common_functions.py
│
├── mappings/
│   └── column_mapping.yml
│
├── data/                     # sample and mock datasets for local testing
│
├── tests/                    # unit tests for PySpark jobs and schema validation
│
├── raw/                      # manual ingestion or setup notes
├── notebooks/                # exploration or validation
└── README.md

---

## **Technologies Used**

* **PySpark / AWS Glue** – distributed data transformation
* **Amazon S3** – data lake storage (raw, staging, curated)
* **AWS CloudShell** – ingestion automation from NOAA source
* **Python 3.10+** – core scripting and orchestration
* **pytest / PySpark testing framework** – validation and unit testing

---

## **Data Source**

AIS vessel position data provided by NOAA’s Office for Coastal Management
→ [https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024)
→ [https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2025)

---

## **Future Work**

* Real-time streaming integration (Kafka or Kinesis)
* Enrichment with external vessel registries (IMO, Equasis)
* FastAPI-based analytics API and React dashboard
* CI/CD integration for automated testing and deployment

---

## **Author**

Developed by **Siva Prasath** as a large-scale data engineering and analytics project showcasing advanced **ETL architecture, PySpark optimization, AWS data lake design, and engineering-grade testing practices**.

---

© 2025 NOAA AIS Pipeline Project by Siva Prasath. All rights reserved.
