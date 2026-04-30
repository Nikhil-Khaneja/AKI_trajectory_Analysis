"""
Module 5 v1.2.0: Clinical LSH — patient similarity search for clinical twins.
XAI Integration: Returns top-3 twins with explainability scores.
Owner: Naveen
"""
import numpy as np
import pandas as pd
from typing import List, Optional, Dict


FEAT_COLS = [
    "creatinine", "baseline_creatinine", "creatinine_ratio",
    "creatinine_delta_1h", "creatinine_delta_48h",
    "urine_ml_per_kg_hr_6h", "urine_ml_per_kg_hr_12h",
    "urine_ml_per_kg_hr_24h", "hours_since_admit"
]


class ClinicalLSH:
    """
    Locality-Sensitive Hashing for finding clinical twins.
    v1.2.0: Includes XAI explainability for twins.
    
    Args:
        n_bits: Hash bits per table (default 6)
        n_tables: Number of hash tables (default 3)
    """

    def __init__(self, n_bits: int = 6, n_tables: int = 3):
        self.n_bits = n_bits
        self.n_tables = n_tables
        self.hyperplanes = []
        self.tables = []
        self.data: Optional[pd.DataFrame] = None
        self.feature_importance = {}

    def fit(self, df: pd.DataFrame) -> None:
        """Build LSH index from historical patient records."""
        self.data = df.copy()
        feat_cols = [c for c in FEAT_COLS if c in df.columns]
        X = df[feat_cols].fillna(0).values
        
        # Normalize features for fair distance computation
        X_mean = X.mean(axis=0, keepdims=True)
        X_std = X.std(axis=0, keepdims=True)
        X_std[X_std == 0] = 1  # Avoid division by zero
        X_norm = (X - X_mean) / X_std
        
        d = X_norm.shape[1]
        self.hyperplanes = [np.random.randn(self.n_bits, d) for _ in range(self.n_tables)]
        self.tables = []
        
        for hp in self.hyperplanes:
            hashes = (X_norm @ hp.T > 0).astype(int)
            self.tables.append(hashes)
        
        print(f"✓ ClinicalLSH fitted on {len(df)} records with {d} features")

    def query(self, vector: List[float], k: int = 3, with_xai: bool = True) -> pd.DataFrame:
        """
        Return k most similar patients (clinical twins) to the query vector.
        
        v1.2.0: Includes XAI explanations for twins.
        
        Args:
            vector: Query feature vector
            k: Number of twins to return (default 3 for top-3 twins)
            with_xai: Include XAI explainability scores
            
        Returns:
            DataFrame with k twins, similarities, and XAI explanations
        """
        if self.data is None:
            print("⚠️  LSH not fitted. Call fit() first.")
            return pd.DataFrame()
        
        feat_cols = [c for c in FEAT_COLS if c in self.data.columns]
        X = self.data[feat_cols].fillna(0).values
        
        # Normalize like training data
        X_mean = X.mean(axis=0, keepdims=True)
        X_std = X.std(axis=0, keepdims=True)
        X_std[X_std == 0] = 1
        X_norm = (X - X_mean) / X_std
        
        # Normalize query vector
        v = np.array(vector[:len(feat_cols)])
        v_norm = (v - X_mean.flatten()) / X_std.flatten()
        
        # Compute Euclidean distances
        dists = np.sqrt(((X_norm - v_norm[:X_norm.shape[1]]) ** 2).sum(axis=1))
        
        # Get top k closest patients
        top_k_indices = np.argsort(dists)[:k]
        result = self.data.iloc[top_k_indices].copy()
        result["similarity_distance"] = dists[top_k_indices]
        result["rank"] = range(1, k + 1)
        
        if with_xai:
            # Add XAI: feature contribution to similarity
            result = self._add_xai_explanations(result, v_norm, X_norm[top_k_indices], feat_cols)
        
        return result

    def _add_xai_explanations(self, twins_df: pd.DataFrame, query_norm: np.ndarray,
                             twins_norm: np.ndarray, feat_cols: List[str]) -> pd.DataFrame:
        """
        Add XAI explainability: contribution of each feature to similarity.
        
        Returns DataFrame with top 3 feature contributions per twin.
        """
        twins_df = twins_df.copy()
        
        explanations = []
        for idx, (_, row) in enumerate(twins_df.iterrows()):
            twin_vector = twins_norm[idx]
            feature_diffs = np.abs(query_norm[:len(feat_cols)] - twin_vector)
            
            # Normalize to [0, 1] for interpretability
            feature_diffs_norm = feature_diffs / (feature_diffs.sum() + 1e-6)
            
            # Get top 3 contributing features
            top_3_features = np.argsort(-feature_diffs_norm)[:3]
            top_3_names = [feat_cols[i] for i in top_3_features]
            top_3_scores = feature_diffs_norm[top_3_features]
            
            xai_str = " | ".join([f"{name}: {score:.2%}" for name, score in zip(top_3_names, top_3_scores)])
            explanations.append(xai_str)
        
        twins_df["xai_top_features"] = explanations
        
        return twins_df

    def query_batch(self, vectors: List[List[float]], k: int = 3) -> pd.DataFrame:
        """Query multiple vectors and return all twins."""
        all_results = []
        for i, vec in enumerate(vectors):
            result = self.query(vec, k=k, with_xai=True)
            result["query_id"] = i
            all_results.append(result)
        return pd.concat(all_results, ignore_index=True)


if __name__ == "__main__":
    print("Clinical LSH Example")
    # This would normally load from the data pipeline
    print("Usage: lsh = ClinicalLSH(); lsh.fit(df); twins = lsh.query(vector, k=3, with_xai=True)")
