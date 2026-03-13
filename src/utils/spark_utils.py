"""
spark_utils.py
--------------
Module with utility functions for Spark session management and DataFrame
operations.

Author: Ezau Faridh Torres Torres.
Date: March 2026.

Functions
---------
- getSparkSession :
    Creates or retrieves a Spark Session.
- save_table :
    Saves a DataFrame as a Hive table.
"""
# Necessary imports.
from pyspark.sql import (
    SparkSession,
    DataFrame
)

class SparkManager:
    _spark = None

    @staticmethod
    def getSparkSession(app_name: str) -> SparkSession:
        """
        Creates a Spark Session with name 'app_name' and avoids recreate it
        is called.
    
        Parameters
        ----------
        app_name : str
            Name to the session.
        
        Return
        ------
        spark : SparkSession
            Spark Session created.
        """
        if SparkManager._spark is None:
            SparkManager._spark = (
                SparkSession.builder
                .appName(app_name)
                .enableHiveSupport()
                .getOrCreate()
            )
            
        return SparkManager._spark
    
def register_udf(
    spark: SparkSession,
    udf_name: str = "bdf_voltage_simpleapi_v2",
    udf_class: str = "mx.com.gsalinas.bdf.voltage.genericudf.simpleapi.BDF_VOLTAGE_SIMPLEAPI",
    jar_path: str ="BDF_HiveVoltageFunction-assembly-2.0.jar"
) -> None:
    """
    Register a temporary UDF in the Spark session.
    
    Parameters
    ----------
    spark : SparkSession
        The Spark session where the UDF will be registered.
    udf_name : str
        The name of the UDF to register.
    udf_class : str
        The class path of the UDF implementation.
    """
    spark.sql(
        f"""
        CREATE TEMPORARY FUNCTION {udf_name} AS 
        '{udf_class}' USING JAR '{jar_path}'
        """
    )
    
def save_table(table: DataFrame, name: str) -> None:
    """
    Save a DataFrame as a Hive table with configurable mode and partitions.

    Parameters
    ----------
    table : DataFrame
        Spark DataFrame to save.
    name : str
        Name of the Hive table.
    mode : str, optional
        Write mode ('overwrite', 'append'), by default 'overwrite'.
    partition_cols : list, optional
        Columns to partition the table by, e.g. ['num_periodo_sem'].
        If None, no partitioning is applied.
    partition_overwrite_mode : str, optional
        Overwrite mode for partitions ('dynamic' or 'static'), default 'dynamic'.
    """
    writer = table.write.mode("overwrite").option(
        "partitionOverwriteMode", "dynamic"
    )
    writer.saveAsTable(name)
    
    print(f"\nTable saved as:\n{name}")