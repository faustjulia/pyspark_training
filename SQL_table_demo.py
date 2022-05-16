from pyspark.sql import *
from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo") \
        .enableHiveSupport()\
        .getOrCreate()

    logger = Log4J(spark)

    flight_time_parquet = spark.read \
        .format("parquet") \
        .load("dataSource/")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flight_time_parquet.write\
        .format("csv")\
        .mode("overwrite")\
        .bucketBy(5, "OP_CARRIER", "ORIGIN")\
        .sortBy("OP_CARRIER", "ORIGIN")\
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))