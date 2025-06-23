flowchart TD
A[Azure Blob Storage] -->|Raw Data| B[File Discovery]
B --> C[Data Preparation]
C --> D[DeltaTableManager - SCD2 Upsert]
D --> E[Delta Lake Table]
E --> F[Downstream Analytics]
