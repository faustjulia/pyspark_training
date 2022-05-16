from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, \
    IntegerType

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4J(spark)

    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
              ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
              WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    flight_time_csv = spark.read \
        .format("csv")\
        .option("header", "true") \
        .schema(flightSchemaStruct)\
        .option("mode", "FAILFAST")\
        .option("dateFormat", "M/d/y")\
        .load("data/flight*.csv")

    flight_time_csv.show(5)
    logger.info("CSV Schema:" + flight_time_csv.schema.simpleString())

    flight_time_json = spark.read \
        .format("json") \
        .schema(flightSchemaDDL) \
        .option("dateFormat", "M/d/y") \
        .load("data/flight*.json")

    flight_time_json.show(5)
    logger.info("JSON Schema:" + flight_time_json.schema.simpleString())

    flight_time_parquet = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    flight_time_parquet.show(5)
    logger.info("Parquet Schema:" + flight_time_parquet.schema.simpleString())
