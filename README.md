# SQL Server Data Warehouse — Medallion Architecture

A production-style data warehouse built in SQL Server implementing the **medallion (Bronze → Silver → Gold)** layered architecture. Ingests data from two heterogeneous source systems (CRM + ERP), applies systematic data cleansing, and delivers an analytics-ready star schema for downstream reporting.

---

## 📋 Tasks Overview

Full project management board (phases, tasks, and status) tracked in Notion:

👉 [Roxani Kyritsi DW Project Board](https://www.notion.so/Data-Warehouse-Project-358b6b0cb5908050a0aac67a260890ec)

---
## Architecture Overview

![Diagram](docs/High_Level_Architecture.png)

![Diagram](docs/Integration_Model.png)

![Diagram](docs/Data_Flow.png)

![Diagram](docs/Data_Model.png)
