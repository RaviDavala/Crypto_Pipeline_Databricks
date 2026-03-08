# 🚀 Crypto Data Engineering Pipeline on Azure

![Azure](https://img.shields.io/badge/Azure-Cloud-blue)
![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-orange)
![Python](https://img.shields.io/badge/Python-3.9-blue)
![PySpark](https://img.shields.io/badge/PySpark-BigData-yellow)
![Delta Lake](https://img.shields.io/badge/Delta-Lakehouse-red)

---

# 📌 Project Overview

This project implements an **end-to-end crypto data engineering pipeline** built on Azure cloud infrastructure.

The pipeline ingests cryptocurrency market data from external APIs, processes it using distributed computing in Azure Databricks, and stores it in Azure Data Lake Storage using the **Medallion Architecture (Bronze → Silver → Gold)**.

The processed datasets power interactive analytics dashboards built directly in Databricks.

---

# 🏗 Architecture

```mermaid
flowchart TD

A[Crypto Market API]

B[Azure Databricks<br>Data Ingestion & ETL<br>Python / PySpark]

subgraph DL[Azure Data Lake Storage Gen2]
    C[Bronze Layer<br>Raw API Data]
    D[Silver Layer<br>Cleaned & Structured Data]
    E[Gold Layer<br>Aggregated Analytics]
end

F[Databricks Dashboards<br>Crypto Market Insights]

A --> B
B --> C
C --> D
D --> E
E --> F
