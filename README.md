# VA Patronage Data Pipeline

A robust, production-grade ETL pipeline for processing Veteran patronage data from Service Connected Disability (SCD) and Caregiver (CG) sources using PySpark and Delta Lake on Databricks.

---

## High-Level Architecture

```mermaid
flowchart TB
    subgraph Sources["Data Sources"]
        direction LR
        CaregiverCSVs["Caregiver CSVs"]
        SCDCSVs["SCD CSVs"]
        IdentityCorrelations["Identity Correlations Delta"]
        PTDelta["PT Indicator Delta Table"]
    end
    
    subgraph Processing["Processing Layer"]
        direction TB
        Initializer["Table Initializer"]
        IncrementalProcessor["Incremental Processor"]
        EDIPIBackfill["EDIPI Backfill<br/>(Last Friday of Month)"]
        DMDCGenerator["DMDC File Generator<br/>(Wed/Fri)"]
    end
    
    subgraph Storage["Storage Layer"]
        DeltaTable["Delta Table: Patronage"]
        IdentityTable["Identity Correlation Table"]
        CheckpointTable["DMDC Checkpoint Table"]
    end
    
    subgraph Outputs["Outputs"]
        DMDCFiles["DMDC Transfer Files<br/>(.txt fixed-width)"]
        EDIPIFiles["EDIPI Backfill Files<br/>(.txt fixed-width)"]
    end
    
    %% Source to Processing flows
    CaregiverCSVs --> Initializer
    SCDCSVs --> Initializer
    IdentityCorrelations --> Initializer
    
    CaregiverCSVs --> IncrementalProcessor
    SCDCSVs --> IncrementalProcessor
    IdentityCorrelations --> IncrementalProcessor
    PTDelta --> IncrementalProcessor
    
    IdentityCorrelations --> EDIPIBackfill
    IdentityCorrelations --> IdentityTable
    
    %% Processing to Storage flows
    Initializer --> DeltaTable
    IncrementalProcessor --> DeltaTable
    EDIPIBackfill --> DeltaTable
    
    %% Storage to Output flows
    DeltaTable --> DMDCGenerator
    DeltaTable --> EDIPIBackfill
    DMDCGenerator --> DMDCFiles
    DMDCGenerator --> CheckpointTable
    EDIPIBackfill --> EDIPIFiles
    
    %% Styling
    classDef sourceStyle fill:#e1f5fe,stroke:#01579b
    classDef processStyle fill:#fff3e0,stroke:#e65100
    classDef storageStyle fill:#e8f5e9,stroke:#2e7d32
    classDef outputStyle fill:#fce4ec,stroke:#c2185b
    
    class CaregiverCSVs,SCDCSVs,IdentityCorrelations,PTDelta sourceStyle
    class Initializer,IncrementalProcessor,EDIPIBackfill,DMDCGenerator processStyle
    class DeltaTable,IdentityTable,CheckpointTable storageStyle
    class DMDCFiles,EDIPIFiles outputStyle
```

---

## Processing Modes

The pipeline supports multiple processing modes to handle different operational scenarios:

```mermaid
flowchart LR
    subgraph Modes["Processing Modes"]
        direction TB
        M1["<b>rebuild</b><br/>Full table rebuild<br/>from all source files"]
        M2["<b>update</b><br/>Incremental processing<br/>of new files only"]
    end
    
    subgraph ScheduledTasks["Scheduled Tasks"]
        direction TB
        T1["<b>EDIPI Backfill</b><br/>Last Friday of Month<br/>Updates NULL EDIPIs"]
        T2["<b>DMDC Transfer</b><br/>Wednesday & Friday<br/>Generates export files"]
    end
    
    M1 --> Pipeline["run_pipeline()"]
    M2 --> Pipeline
    Pipeline --> T1
    Pipeline --> T2
    
    style M1 fill:#bbdefb,stroke:#1976d2
    style M2 fill:#c8e6c9,stroke:#388e3c
    style T1 fill:#ffe0b2,stroke:#f57c00
    style T2 fill:#f8bbd9,stroke:#c2185b
```

| Mode | Trigger | Description |
|------|---------|-------------|
| `rebuild` | Manual | Drops existing table and reinitializes from all source files |
| `update` | Manual/Scheduled | Processes only new files since last run (incremental) |
| **EDIPI Backfill** | Auto (Last Friday) | Retroactively updates records with EDIPI from Identity Correlations |
| **DMDC Transfer** | Auto (Wed/Fri) | Generates 42-character fixed-width files for DMDC downstream system |

---

## Databricks Job Execution Flow

The pipeline runs as a scheduled Databricks Job:

```mermaid
flowchart LR
    subgraph DatabricksJob["Databricks Job (Scheduled Daily)"]
        A["Job Trigger<br/>(Daily 9:00 AM EST)"]
    end
    
    subgraph Orchestrator["Pipeline_Runner.ipynb"]
        B["Import patronage_pipeline"]
        C["Detect Processing Mode"]
        D["run_pipeline(mode, verbose)"]
    end
    
    subgraph CoreLogic["patronage_pipeline.py"]
        E["process_patronage_data()"]
        F["Process SCD & CG Files"]
        G{"Check Scheduled Tasks"}
    end
    
    subgraph ScheduledTasks["Conditional Tasks"]
        H["EDIPI Backfill<br/>(Last Friday of Month)"]
        I["DMDC Transfer<br/>(Wednesday/Friday)"]
    end
    
    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G -->|"Last Friday?"| H
    G -->|"Wed/Fri?"| I
    
    %% Styling
    style A fill:#bbdefb,stroke:#1976d2
    style D fill:#fff3e0,stroke:#e65100
    style E fill:#e8f5e9,stroke:#2e7d32
    style H fill:#ffe0b2,stroke:#f57c00
    style I fill:#f8bbd9,stroke:#c2185b
```

### Key Operational Notes
- **Daily Processing**: The job runs every day in `update` mode, processing only new files since the last run
- **Automatic Task Triggers**: EDIPI Backfill and DMDC Transfer tasks are triggered automatically based on the day of the week/month
- **Monitoring**: Job status and logs are available in the Databricks Jobs UI
- **Alerts**: Email notifications are sent on job failure to the data engineering team
- **Manual Runs**: The pipeline can be triggered manually from the Databricks Jobs UI or by running the notebook directly

---

## ETL Workflow Diagram

```mermaid
flowchart LR
    A["Raw Data Sources<br/>(SCD CSVs, CG CSVs)"] --> B["File Discovery<br/>discover_unprocessed_files()"]
    B --> C["Data Preparation<br/>transform_scd_data() / transform_caregiver_data()"]
    C --> D["Identity Correlation<br/>Join with ICN/EDIPI lookup"]
    D --> E["Deduplication<br/>Window functions + ranking"]
    E --> F["SCD2 Upsert Logic<br/>execute_delta_merge()"]
    F --> G["Delta Table: Patronage"]
    G --> H["DMDC Export<br/>generate_dmdc_transfer_file()"]
    
    style A fill:#e3f2fd
    style G fill:#e8f5e9
    style H fill:#fce4ec
```

---

## Data Lineage Diagram

```mermaid
flowchart TD
    subgraph Sources["Sources"]
        A1["Caregiver CSV"]
        A2["SCD CSV"]
        A3["PT Indicator Delta"]
        A4["Identity Correlations Delta"]
    end
    
    subgraph Transform["Transformations"]
        B1["transform_caregiver_data()"]
        B2["transform_scd_data()"]
        B3["build_identity_correlation_table()"]
    end
    
    subgraph Merge["SCD2 Logic"]
        C1["identify_record_changes()"]
        C2["create_scd_records_with_audit()"]
        C3["execute_delta_merge()"]
    end
    
    subgraph Storage["Storage"]
        D1["Delta Table: Patronage"]
        D2["Identity Lookup Table"]
    end
    
    subgraph Output["Outputs"]
        E1["DMDC Transfer Files"]
        E2["EDIPI Backfill Files"]
        E3["Reporting & Analytics"]
    end
    
    A1 --> B1
    A2 --> B2
    A3 --> B2
    A4 --> B3
    
    B1 --> C1
    B2 --> C1
    B3 --> D2
    D2 --> B2
    
    C1 --> C2
    C2 --> C3
    C3 --> D1
    
    D1 --> E1
    D1 --> E2
    D1 --> E3
    
    %% Styling
    classDef sourceStyle fill:#e1f5fe,stroke:#01579b
    classDef transformStyle fill:#fff3e0,stroke:#e65100
    classDef mergeStyle fill:#f3e5f5,stroke:#7b1fa2
    classDef storageStyle fill:#e8f5e9,stroke:#2e7d32
    classDef outputStyle fill:#fce4ec,stroke:#c2185b
    
    class A1,A2,A3,A4 sourceStyle
    class B1,B2,B3 transformStyle
    class C1,C2,C3 mergeStyle
    class D1,D2 storageStyle
    class E1,E2,E3 outputStyle
```

---

## SCD2 Upsert Logic Flow

The pipeline implements Slowly Changing Dimension Type 2 (SCD2) to maintain complete historical records:

```mermaid
flowchart LR
    A["Incoming Record"] --> B{"Record Exists<br/>in Delta Table?"}
    B -- "No" --> C["INSERT<br/>as New Record<br/>(RecordStatus=True)"]
    B -- "Yes" --> D{"Data Changed?<br/>(Hash comparison)"}
    D -- "No" --> E["No Action<br/>(Skip record)"]
    D -- "Yes" --> F["EXPIRE Old Record<br/>(RecordStatus=False,<br/>End_Date=Today)"]
    F --> G["INSERT New Version<br/>(RecordStatus=True,<br/>Effective_Date=Today)"]
    
    C --> H["Delta Table Updated"]
    G --> H
    
    style A fill:#e3f2fd
    style C fill:#c8e6c9
    style F fill:#ffcdd2
    style G fill:#c8e6c9
    style H fill:#e8f5e9
```

### Change Detection
- **SCD Records**: Hash comparison of `SC_Combined_Disability_Percentage` and `PT_Indicator`
- **Caregiver Records**: Hash comparison of `Status_Begin_Date`, `Status_Termination_Date`, `Applicant_Type`, `Caregiver_Status`

---

## DMDC Transfer File Generation

The pipeline generates fixed-width transfer files for the DMDC (Defense Manpower Data Center) downstream system:

```mermaid
flowchart LR
    A["Scheduled Trigger<br/>(Wednesday/Friday)"] --> B["Get Last Run Date<br/>from Checkpoint Table"]
    B --> C["Query Eligible Records<br/>(Active, Has EDIPI)"]
    C --> D{"Records Found?"}
    D -- "No" --> E["Skip Export<br/>(No new records)"]
    D -- "Yes" --> F["Generate Fixed-Width File<br/>(42 chars per record)"]
    F --> G["Write to Blob Storage<br/>/mnt/ci-vba-edw-2/..."]
    G --> H["Update Checkpoint Table"]
    
    style A fill:#fff3e0
    style F fill:#e8f5e9
    style G fill:#e3f2fd
```

### DMDC Record Format (42 characters)
| Field | Width | Description |
|-------|-------|-------------|
| EDIPI | 10 | Electronic Data Interchange Personal Identifier |
| Batch_CD | 3 | Source type (SCD/CG) |
| SC_Combined_Disability_Percentage | 3 | Combined disability rating |
| Status_Begin_Date | 8 | YYYYMMDD format |
| PT_Indicator | 1 | Permanent/Total indicator |
| Individual_Unemployability | 1 | Unemployability flag |
| Status_Last_Update | 8 | YYYYMMDD format |
| Status_Termination_Date | 8 | YYYYMMDD format |

---

## EDIPI Backfill Process

Monthly process to retroactively populate EDIPI for records that were created without one:

```mermaid
flowchart LR
    A["Last Friday of Month"] --> B["Find Records<br/>with NULL EDIPI"]
    B --> C["Join with<br/>Identity Correlations"]
    C --> D{"Matches Found?"}
    D -- "No" --> E["No Updates Needed"]
    D -- "Yes" --> F["Generate Audit File<br/>(Before update)"]
    F --> G["UPDATE Patronage Table<br/>(Set EDIPI values)"]
    G --> H["Log Results"]
    
    style A fill:#fff3e0
    style F fill:#e8f5e9
    style G fill:#bbdefb
```

---

## Business Logic Overview

### Core Processing Pipeline

| Component | Function | Description |
|-----------|----------|-------------|
| **Table Initializer** | `initialize_patronage_table()` | Creates Delta table from SCD and CG seed files |
| **Identity Builder** | `build_identity_correlation_table()` | Daily refresh of ICN-to-EDIPI lookup table |
| **File Discovery** | `discover_unprocessed_files()` | Scans blob storage for new/unprocessed files |
| **SCD Transformer** | `transform_scd_data()` | Prepares SCD records with PT indicator enrichment |
| **CG Transformer** | `transform_caregiver_data()` | Prepares Caregiver records with status mapping |
| **Change Detector** | `identify_record_changes()` | Hash-based comparison for SCD2 logic |
| **Delta Merger** | `execute_delta_merge()` | Applies upserts/expirations to Delta table |
| **DMDC Generator** | `generate_dmdc_transfer_file()` | Creates fixed-width export files |
| **EDIPI Backfill** | `run_edipi_backfill()` | Monthly EDIPI population from identity correlations |

### Data Sources

| Source | Type | Description |
|--------|------|-------------|
| **SCD CSVs** | Service Connected Disability | Veteran disability ratings (`CPIDODIEX_*.csv`) |
| **Caregiver CSVs** | CARMA Exports | Caregiver program data (`*caregiver*.csv`) |
| **Identity Correlations** | Delta Table | MVI person-identifier mappings |
| **PT Indicator** | Delta Table | Permanent/Total disability indicators |

### Key Features

- **SCD Type 2**: Full history tracking with `RecordStatus`, `Effective_Date`, `End_Date`
- **Delta Lake**: ACID-compliant storage with efficient upserts and time travel
- **Liquid Clustering**: Optimized by `Batch_CD` and `RecordStatus` for query performance
- **Audit Trail**: Every change logged with `change_log` column documenting what changed
- **Checkpointing**: DMDC exports track last run timestamp to avoid duplicates
- **Broadcast Joins**: Performance optimization for small lookup tables

---

## Key Constants and Configuration

```python
# Source Types
SOURCE_TYPE_SCD = "SCD"   # Service Connected Disability
SOURCE_TYPE_CG = "CG"     # Caregiver

# Table Names
PATRONAGE_TABLE_NAME = "patronage"
IDENTITY_TABLE_NAME = "identity_correlation_lookup"
DMDC_CHECKPOINT_TABLE_NAME = "dmdc_export_checkpoint"

# Scheduled Tasks
# - EDIPI Backfill: Last Friday of each month
# - DMDC Export: Every Wednesday and Friday
```

---

## Usage

### Running the Pipeline

```python
import patronage_pipeline as pipeline

# Incremental update (processes new files only)
pipeline.run_pipeline("update", verbose_logging=False)

# Full rebuild (reinitializes from all source files)
pipeline.run_pipeline("rebuild", verbose_logging=True)
```

### From the Notebook Orchestrator

The `Pipeline_Runner.ipynb` notebook provides:
- Automatic mode detection (initialize vs update)
- Visual Mermaid diagrams
- File reconciliation tools
- Unit testing cells

---

## References

- Delta Lake Documentation: https://docs.delta.io/latest/delta-intro.html
- PySpark Documentation: https://spark.apache.org/docs/latest/api/python/
- Databricks Delta Lake Guide: https://docs.databricks.com/delta/index.html

---

## Contributors

- **Umair Ahmed**: QA Engineer
- **Bhavesh Patel**: Data Engineer

---

## Version History

| Date | Version | Changes |
|------|---------|---------|
| 2024-12-18 | 1.0 | Initial release with SCD and Caregiver processing |
| 2025-11-19 | 2.0 | Added EDIPI Backfill, DMDC Transfer, refactored for modularity |
| 2025-11-26 | 2.1 | Code quality improvements, DRY patterns, enhanced logging |

