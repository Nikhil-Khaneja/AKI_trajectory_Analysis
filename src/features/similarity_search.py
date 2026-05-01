import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
INPUT = ROOT / "outputs" / "tables" / "synthetic_eicu_training_set.csv"
SIMILARITY_DIR = ROOT / "outputs" / "similarity"
SIMILARITY_DIR.mkdir(parents=True, exist_ok=True)

FEATURE_COLS = [
    "creatinine", "baseline_creatinine", "creatinine_ratio", 
    "creatinine_delta_1h", "creatinine_delta_48h", "urine_output_ml",
    "urine_ml_per_kg_hr_6h", "urine_ml_per_kg_hr_12h", "urine_ml_per_kg_hr_24h"
]

class ClinicalLSH:
    """
    Locality Sensitive Hashing (LSH) for Clinical Similarity Search.
    Uses Random Projections to hash continuous features into buckets.
    """
    def __init__(self, n_bits=8, n_tables=5, seed=42):
        self.n_bits = n_bits      # Number of random projections per table
        self.n_tables = n_tables  # Number of hash tables (OR-construction)
        self.seed = seed
        self.tables = []
        self.projections = []
        self.data_df = None
        self.feature_matrix = None

    def fit(self, df: pd.DataFrame):
        self.data_df = df.copy().reset_index(drop=True)
        # Handle missing values
        self.feature_matrix = self.data_df[FEATURE_COLS].fillna(0.0).values
        
        # Standardize features for better projection performance
        self.mean = self.feature_matrix.mean(axis=0)
        self.std = self.feature_matrix.std(axis=0) + 1e-9
        norm_matrix = (self.feature_matrix - self.mean) / self.std
        
        np.random.seed(self.seed)
        d = norm_matrix.shape[1]
        
        for i in range(self.n_tables):
            # Generate random projection vectors
            proj = np.random.randn(self.n_bits, d)
            self.projections.append(proj)
            
            # Hash the entire dataset
            hashes = self._compute_hashes(norm_matrix, proj)
            
            # Build hash table (bucket -> list of indices)
            table = {}
            for idx, h in enumerate(hashes):
                if h not in table:
                    table[h] = []
                table[h].append(idx)
            self.tables.append(table)
            
        print(f"LSH Index built: {self.n_tables} tables, {self.n_bits} bits per table.")
        return self

    def _compute_hashes(self, matrix, projection):
        # dot product followed by sign (binary hash)
        # Ensure no NaNs before comparison
        dot_product = matrix @ projection.T
        bool_hashes = np.nan_to_num(dot_product) >= 0
        # Convert bool array to integer hash
        return [tuple(h) for h in bool_hashes]

    def query(self, query_vec, k=5):
        if self.data_df is None:
            raise ValueError("Model not fitted.")
            
        # Normalize query vector and handle NaNs
        query_vec = np.nan_to_num(np.array(query_vec, dtype=float))
        norm_query = (query_vec - self.mean) / self.std
        norm_query = norm_query.reshape(1, -1)
        
        candidates = set()
        for i in range(self.n_tables):
            h = self._compute_hashes(norm_query, self.projections[i])[0]
            if h in self.tables[i]:
                candidates.update(self.tables[i][h])
        
        if not candidates:
            print("No identical hash matches found. Relaxing search...")
            return pd.DataFrame()

        # Rank candidates by Euclidean distance
        candidate_indices = list(candidates)
        # Ensure candidate_features is float64
        candidate_features = (self.feature_matrix[candidate_indices] - self.mean) / self.std
        candidate_features = np.nan_to_num(candidate_features.astype(np.float64))
        
        # Calculate Euclidean distance manually to avoid linalg.norm issues in this env
        diff = candidate_features - norm_query
        distances = np.sqrt(np.sum(diff**2, axis=1))
        
        results = self.data_df.iloc[candidate_indices].copy()
        results["similarity_distance"] = distances
        
        return results.sort_values("similarity_distance").head(k)

def generate_explanation(query_patient, twins_df):
    """Generates a text-based explanation for why these patients are similar."""
    explanation = []
    for idx, row in twins_df.iterrows():
        diffs = []
        for col in ["creatinine", "urine_output_ml", "kdigo_stage"]:
            q_val = query_patient[col]
            t_val = row[col]
            diffs.append(f"{col}: {t_val:.2f} (vs query {q_val:.2f})")
        
        explanation.append({
            "twin_id": row["patientunitstayid"],
            "summary": ", ".join(diffs),
            "target": "AKI Stage 3" if row["target_progress_to_stage3_48h"] else "No Progress"
        })
    return explanation

def main():
    if not INPUT.exists():
        print(f"Error: {INPUT} not found. Run training scripts first.")
        return

    df = pd.read_csv(INPUT)
    lsh = ClinicalLSH(n_bits=6, n_tables=3)
    lsh.fit(df)
    
    # Pick a sample patient with AKI progression for interesting comparison
    progression_cases = df[df["target_progress_to_stage3_48h"] == 1]
    if progression_cases.empty:
        sample_patient = df.iloc[0]
    else:
        sample_patient = progression_cases.iloc[0]
        
    query_vec = sample_patient[FEATURE_COLS].values
    
    print(f"\n[QUERY] Searching for clinical cohort for Patient {sample_patient['patientunitstayid']}...")
    print(f"Key Features: Creatinine={sample_patient['creatinine']:.2f}, Urine={sample_patient['urine_output_ml']:.2f}")
    
    twins = lsh.query(query_vec, k=5)
    
    print("\n[COHORT] Top Clinically Similar Historical Cases:")
    cols = ["patientunitstayid", "hour_bucket", "kdigo_stage", "target_progress_to_stage3_48h", "similarity_distance"]
    print(twins[cols])
    
    explanations = generate_explanation(sample_patient, twins)
    print("\n[EXPLAINABILITY] Clinical Twin Breakdown:")
    for ex in explanations:
        print(f"- Twin {ex['twin_id']}: {ex['summary']} | Future Outcome: {ex['target']}")

    # Save results
    out_file = SIMILARITY_DIR / f"twin_cohort_{sample_patient['patientunitstayid']}.csv"
    twins.to_csv(out_file, index=False)
    print(f"\nCohort saved to {out_file}")

if __name__ == "__main__":
    main()
