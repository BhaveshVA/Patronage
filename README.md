# 🎖️ Patronage ETL Pipeline

A databricks Patronage data pipeline for ETL processing using Apache Spark and Databricks.

---

<!--
##### How to update these diagrams
- Edit the `.mmd` files in the `docs/` folder using a Mermaid live editor (https://mermaid.live/) or the VS Code Mermaid extension.
- Export each diagram as a PNG and save as `etl_flowchart_alt.png`, `data_lineage_png.png`, and `scd2_upsert_logic.png` in the `docs/` folder.
- The images will then appear below in this README.

---
-->
## 🏛️ High level Architecture
```mermaid
flowchart TB
    subgraph Sources
        direction LR
        CaregiverCSVs["Caregiver CSVs"]
        SCDCSVs["SCD CSVs"]
        IdentityCorrelations["Identity Correlations Delta"]
        NewPTDelta["New PT Delta"]
    end
    subgraph Processing
        direction LR
        SeedLoader["Seed Loader"]
        CaregiverAggregator["Caregiver Aggregator"]
        SCDPreparer["SCD Preparer"]
        PTIndicatorUpdater["PT Indicator Updater"]
        AuditLog["Audit Log"]
    end
    SCDType["Slowly Changing Dimensions Type 2"]
    subgraph Storage
        DeltaTable["Delta Table: Patronage"]
    end
    
    CaregiverCSVs --> SeedLoader
    CaregiverCSVs --> CaregiverAggregator
    SeedLoader --> CaregiverAggregator
    SCDCSVs --> SCDPreparer
    IdentityCorrelations --> SCDPreparer
    NewPTDelta --> PTIndicatorUpdater
    IdentityCorrelations --> SeedLoader
    
    SeedLoader --> SCDType
    SCDPreparer --> SCDType
    PTIndicatorUpdater --> SCDType
    CaregiverAggregator --> SCDType
    AuditLog --> SCDType
    SCDType --> DeltaTable
    
    SCDCSVs --> SeedLoader
    IdentityCorrelations --> CaregiverAggregator
    NewPTDelta --> AuditLog
    
    SeedLoader -.-> AuditLog
    SCDPreparer -.-> AuditLog
    PTIndicatorUpdater -.-> AuditLog
    CaregiverAggregator -.-> AuditLog
    linkStyle 0 stroke:#1f77b4,stroke-width:2px;
    linkStyle 1 stroke:#1f77b4,stroke-width:2px,stroke-dasharray: 4 2;
    linkStyle 2 stroke:#1f77b4,stroke-width:2px,stroke-dasharray: 2 4;
    linkStyle 3 stroke:#ff7f0e,stroke-width:2px;
    linkStyle 4 stroke:#2ca02c,stroke-width:2px;
    linkStyle 5 stroke:#d62728,stroke-width:2px;
    linkStyle 6 stroke:#2ca02c,stroke-width:2px,stroke-dasharray: 2 4;
    linkStyle 7 stroke:#9467bd,stroke-width:2px;
    linkStyle 8 stroke:#9467bd,stroke-width:2px;
    linkStyle 9 stroke:#9467bd,stroke-width:2px;
    linkStyle 10 stroke:#9467bd,stroke-width:2px;
    linkStyle 11 stroke:#9467bd,stroke-width:2px;
    linkStyle 12 stroke:#8c564b,stroke-width:2px;
    linkStyle 13 stroke:#ff7f0e,stroke-width:2px,stroke-dasharray: 4 2;
    linkStyle 14 stroke:#2ca02c,stroke-width:2px,stroke-dasharray: 4 2;
    linkStyle 15 stroke:#d62728,stroke-width:2px,stroke-dasharray: 4 2;
    linkStyle 16 stroke:#9467bd,stroke-width:2px,stroke-dasharray: 2 2;
    linkStyle 17 stroke:#9467bd,stroke-width:2px,stroke-dasharray: 2 2;
    linkStyle 18 stroke:#9467bd,stroke-width:2px,stroke-dasharray: 2 2;
    linkStyle 19 stroke:#9467bd,stroke-width:2px,stroke-dasharray: 2 2;
```
---

## 📊 ETL Workflow Diagram

```mermaid
flowchart TD
    A[Raw Data Sources] --> B[File Discovery]
    B --> C[Data Preparation]
    C -- Deduplication --> D["Delta Manager SCD2 Upsert"]
    D -- Change Tracking --> E[Delta Table]
    E --> F[Reporting & Analytics]
```

![ETL Workflow](docs/etl_flowchart_alt.png)
*Figure 1: High-level ETL workflow showing the flow from raw sources through processing steps to the Delta table and reporting.*

---

## 🧬 Data Lineage Diagram

```mermaid
flowchart LR
    subgraph Sources
        A1[Caregiver CSV]
        A2[SCD CSV]
        A3[PT Indicator Delta Table]
        A4[Identity Correlations Delta Table]
    end
    A1 --> B[Data Preparation]
    A2 --> B
    A3 --> B
    A4 --> B
    B --> C[SCD2 Upsert Logic]
    C --> D[Delta Table]
    D --> E[Reporting]
```

![Data Lineage](docs/data_lineage_png.png)
*Figure 2: Data lineage diagram showing how data moves from all sources, through each transformation, and into the Delta table for reporting.*

---

## 🔁 SCD2 Upsert Logic Flow

```mermaid
flowchart TD
    A[Incoming Record] --> B{Record Exists in Delta Table?}
    B -- No --> C[Insert as New Record]
    B -- Yes --> D{Data Changed?}
    D -- No --> E[No Action]
    D -- Yes --> F[Expire Old Record]
    F --> G[Insert as New Version]
    G --> H[Update Delta Table]
```

![SCD2 Upsert Logic Flow](docs/scd2_upsert_logic.png)
*Figure 3: SCD2 upsert logic flow showing how new, changed, and unchanged records are handled in the Delta table.*

---
<!--
 
## 🧩 Project Structure

\`\`\`mermaid
flowchart LR
    root((Project Root))
    src["src/"]
    tests["tests/"]
    config["config.yml -- Sample configuration"]
    reqs["requirements.txt -- Project dependencies"]
    github[".github/"]
    workflows["workflows/"]
    ci["ci.yml (CI/CD pipeline configuration)"]
    src_init["__init__.py"]
    src_config["config.py (Configuration management)"]
    src_fp["file_processor.py (Main ETL logic )"]
    src_schemas["schemas.py (All Spark schemas)"]
    src_main["main.py (Entrypoint)"]
    tests_config["test_config.py"]
    tests_transformer["test_transformer.py"]
    tests_fp["test_file_processor.py"]

    root -*-> src
    root -*-> tests
    root -*-> config
    root -*-> reqs
    root -*-> github
    src -*-> src_init
    src -*-> src_config
    src -*-> src_fp
    src -*-> src_schemas
    src -*-> src_main
    tests -*-> tests_config
    tests -*-> tests_transformer
    tests -*-> tests_fp
    github -*-> workflows
    workflows -*-> ci

\`\`\`
-->
<!--
## 🚀 Onboarding: Quick Start for New Team Members

1. **Clone the repository**
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```
3. **Configure Databricks connection:**
   - Set up Databricks CLI and configure your profile
   - Ensure you have access to all required data sources (see `config.yml`)
4. **Run the ETL pipeline:**
   ```bash
   python src/main.py --config config.yml
   ```
5. **Run tests:**
   ```bash
   pytest tests/
   ```
6. **Check CI/CD:**
   - All pushes and PRs are tested automatically via GitHub Actions

\`\`\`
-->

## 🧠 Business Logic Overview

- **Seed Load:** Loads initial caregivers from CSV, joins with identity correlations, and writes to Delta table.
- **Incremental Processing:**
  - Scans source directories for new/updated files (Caregiver, SCD, PT Indicator)
  - Prepares and deduplicates data using Spark DataFrames
  - Applies SCD2 logic (Slowly Changing Dimension Type 2) for upserts
  - Tracks changes and logs them in the Delta table
- **Delta Lake:** All data is stored in Delta format for ACID compliance and efficient upserts.
- **Partitioning:** Data is partitioned by batch and record status for performance.
- **Change Tracking:** Each upsert logs what changed, when, and why (for audit and reporting).

---
<!--
## 🧪 Testing & Development

- All ETL logic is modular and testable (see `tests/`)
- Use `pytest` for unit and integration tests
- Code style: `black` (formatting), `flake8` (linting)
- CI/CD: See `.github/workflows/ci.yml` for pipeline details

---

## 🤝 Contributing & Support

- Create a new branch for your feature or bugfix
- Add/modify tests as needed
- Open a pull request for review
- For help, see code comments, this README, or ask a senior team member
---
-->
## 📖 References
<!-- - Original Databricks notebook: `Patronage V4.ipynb` -->
- Delta Lake docs: https://docs.delta.io/latest/delta-intro.html
- PySpark docs: https://spark.apache.org/docs/latest/api/python/

---
## Contributions
- Umair Ahmed: QA Engineer
- Bhavesh Patel:Data Engineer
=======
# Patronage
