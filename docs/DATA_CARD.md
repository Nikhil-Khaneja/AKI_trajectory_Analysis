# Data Card - AKI Training Set

## Dataset Description
- **Dataset Name**: Synthetic eICU AKI Training Set
- **Collection Date**: 2022-2023 (Simulated)
- **Data Source**: Synthetic generation based on eICU Collaborative Research Database patterns.

## Data Schema
- **Features**: 9 engineered features including `creatinine_ratio` and `urine_ml_per_kg_hr_24h`.
- **Target**: `target_progress_to_stage3_48h` (Binary).
- **Sensitive Attributes**: `age`, `gender`, `ethnicity`.

## Preprocessing
- **Cleaning**: Outlier removal and median imputation for creatinine values.
- **Engineering**: Ratios and rolling deltas calculated over 1h, 6h, 12h, 24h, and 48h windows.

## Privacy and Governance
- **Anonymization**: Data has been processed with **K-Anonymity (k=5)**.
- **Aggregation**: Aggregate reports are generated with **Differential Privacy (epsilon=0.5)** using the Laplace mechanism.
- **Lineage**: Raw -> Bronze (Validation) -> Silver (Engineered) -> Gold (Labeled) -> Mart (Privacy Protected).

## Known Limitations
- The data is synthetic and may not capture all physiological edge cases found in real-world ICU patients.
- Imbalance: The proportion of patients progressing to Stage 3 is relatively low, requiring stratified sampling.
