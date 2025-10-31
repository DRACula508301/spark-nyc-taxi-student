import sys
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from jobs.nyc_taxi_etl_template import build_spark, clean_and_transform
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

def test_clean_and_transform(spark: SparkSession, input_df: DataFrame) -> DataFrame:
    cleaned_df = clean_and_transform(input_df)
    return cleaned_df

if __name__ == "__main__":
    spark = build_spark("Test Clean and Transform")
    
    # Load sample data for testing
    data_path = Path(__file__).parent.parent / "data" / "yellow_tripdata_2023-02.parquet"
    input_df = spark.read.parquet(str(data_path))
    
    cleaned_df = test_clean_and_transform(spark, input_df)
    
    print(f"Original row count: {input_df.count()}")
    print(f"Cleaned row count: {cleaned_df.count()}")
    cleaned_df.show(5)
    cleaned_df.printSchema()
    
    spark.stop()