import sys
from pathlib import Path
from pyspark.sql import SparkSession

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from jobs.nyc_taxi_etl_template import build_spark, read_trips, read_zones

def test_read_trips(spark: SparkSession):
    # Use absolute path based on script location
    data_path = Path(__file__).parent.parent / "data" / "yellow_tripdata_2023-02.parquet"
    input_paths = [str(data_path)]
    trips_df = read_trips(spark, input_paths)
    return trips_df

def test_read_zones(spark: SparkSession):
    zone_csv_path = Path(__file__).parent.parent / "data" / "taxi_zone_lookup.csv"
    zones_df = read_zones(spark, str(zone_csv_path))
    return zones_df
    
def select_vendor_ids(df):
    return df.select("VendorID").distinct().orderBy("VendorID")

def data_preview(df, n=5):
    # Print top 10 rows for visual inspection
    print(f"{df.count()} rows read in total \n \
          displaying top {n} rows:\n \
          {df.show(n)} \n \
          schema:\n \n \
          {df.printSchema()}")


if __name__ == "__main__":
    spark = build_spark("Test Read Trips")
    trips_df = test_read_trips(spark)
    zones_df = test_read_zones(spark)
    print("Trips Data Preview:")
    data_preview(trips_df, n=10)
    print("Zones Data Preview:")
    data_preview(zones_df, n=10)
    # print("Distinct VendorIDs in Trips Data:")
    # vendor_ids_df = select_vendor_ids(trips_df)
    # print(vendor_ids_df.show())
    spark.stop()