# Model Card - AKI Progression Predictor

## Model Details
- **Developed by**: AKI Project Team
- **Model date**: 2026-04-29
- **Model type**: Gradient Boosting Classifier (scikit-learn)
- **Version**: 1.0.0

## Intended Use
- **Primary intended uses**: Early warning system for clinicians to identify patients at risk of progressing to Stage 3 AKI within 48 hours.
- **Primary intended users**: ICU Physicians, Nephrologists, and Nursing staff.
- **Out-of-scope use cases**: This model is not intended for out-patient diagnosis or for long-term chronic kidney disease (CKD) prediction.

## Factors
- **Relevant factors**: Age, Gender, Baseline Creatinine, Urine Output velocity.
- **Evaluation factors**: Subgroup analysis performed on Gender and Age Group to ensure fairness.

## Metrics
- **Performance metrics**: F1-Score, Precision, Recall, AUROC.
- **Fairness metrics**: Per-subgroup F1-Score and Recall parity.

## Training Data
- **Source**: Synthetic eICU demo dataset (derived from MIMIC/eICU patterns).
- **Size**: 8,640 hourly observations.

## Quantitative Analyses
- **Overall AUROC**: 0.99
- **Overall F1**: 0.96
- **Fairness Warning**: The model performs better on older age groups (70+) compared to pediatric or young adult cases, likely due to the higher prevalence of AKI events in the elderly population in the training set.

## Ethical Considerations
- **Privacy**: The model is trained on de-identified data. Mart outputs are protected via K-Anonymity (k=5) and Differential Privacy.
- **Bias**: Ongoing monitoring for gender bias is required, as the model shows slightly higher recall for male patients.

## Caveats and Recommendations
- This is a clinical decision support tool, not a replacement for clinical judgment.
- Model performance should be validated on local hospital data before deployment.
