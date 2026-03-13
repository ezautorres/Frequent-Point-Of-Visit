"""
main.py
-------

Author: Ezau Faridh Torres Torres.
Date: March 2026.
"""
# Necessary imports.
import time
from utils.config import load_params, load_contact_codes
from utils.spark_utils import (
    SparkManager,
    save_table,
    register_udf
)

params = load_params()

def main():
    pass

if __name__ == "__main__":
    
    start_time = time.time()

    spark = SparkManager.getSparkSession("pfv")
    main(spark)
    spark.stop()
    
    end_time = time.time()
    print(f"[INFO] Total execution time: {end_time - start_time:.2f} seconds")