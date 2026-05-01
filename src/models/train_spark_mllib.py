from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer
from src.utils.spark import get_spark

def train_mllib_models(training_data_path):
    """
    Step 8: Modelling (Spark MLlib).
    Trains large-scale clinical models using Apache Spark.
    """
    spark = get_spark("aki_mllib_training")
    
    # Load data from Iceberg/Parquet (Simulated here)
    df = spark.read.csv(training_data_path, header=True, inferSchema=True)
    
    feature_cols = [
        "creatinine", "baseline_creatinine", "creatinine_ratio", 
        "creatinine_delta_1h", "creatinine_delta_48h", "urine_output_ml",
        "urine_ml_per_kg_hr_6h", "urine_ml_per_kg_hr_12h", "urine_ml_per_kg_hr_24h"
    ]
    target_col = "target_progress_to_stage3_48h"
    
    # 1. Prepare Features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # 2. Define Models
    lr = LogisticRegression(labelCol=target_col, featuresCol="features")
    gbt = GBTClassifier(labelCol=target_col, featuresCol="features", maxIter=10)
    
    # 3. Build Pipelines
    lr_pipeline = Pipeline(stages=[assembler, lr])
    gbt_pipeline = Pipeline(stages=[assembler, gbt])
    
    # 4. Train
    print("Training Spark MLlib Logistic Regression...")
    lr_model = lr_pipeline.fit(df)
    
    print("Training Spark MLlib Gradient-Boosted Trees...")
    gbt_model = gbt_pipeline.fit(df)
    
    # 5. Save Models
    lr_model.save("outputs/models/spark_lr_model")
    gbt_model.save("outputs/models/spark_gbt_model")
    
    print("Spark MLlib models trained and saved to outputs/models/")

if __name__ == "__main__":
    # This script is intended for use in environments with PySpark installed
    print("Spark MLlib implementation ready.")
