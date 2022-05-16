from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4J(spark)

    flight_time_parquet = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    # flight_time_parquet.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/avro/") \
    #     .save()

    logger.info("Num Partitions before: " + str(
        flight_time_parquet.rdd.getNumPartitions()))
    flight_time_parquet.groupBy(spark_partition_id()).count().show()

    partitioned_df = flight_time_parquet.repartition(5)
    logger.info("Num Partitions after: " + str(
        partitioned_df.rdd.getNumPartitions()))
    partitioned_df.groupBy(spark_partition_id()).count().show()
    #
    # partitioned_df.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/avro/") \
    #     .save()

    flight_time_parquet.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "dataSink/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN")\
        .option("maxRecordsPerFile", 1000)\
        .save()
