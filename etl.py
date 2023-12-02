# org.apache.spark:spark-avro_2.12:3.3.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2

import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import expr
from schema_registry.client import SchemaRegistryClient

TOPIC = "airbnb-reviews"

KAFKA_BOOTSTRAP_SERVERS = "broker:29092"

SCHEMA_REGISTRY_URL = "http://schema-registry:8099"

HDFS_URL = "hdfs://namenode:9000"

SCHEMA_REGISTRY_CLIENT = SchemaRegistryClient(SCHEMA_REGISTRY_URL)

avro_schema_object = SCHEMA_REGISTRY_CLIENT.get_schema(TOPIC + "-value")

if avro_schema_object is None:
    raise Exception("Schema for topic {} is not registered yet".format(TOPIC))

avro_schema = json.dumps(avro_schema_object.schema.raw_schema)

spark = SparkSession.builder.appName("AirbnbReviewsHDFS").getOrCreate()

spark_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
)


def deserialize_avro(
    df: DataFrame, in_col: str, out_col, avro_schema: str
) -> DataFrame:
    return df.withColumn(
        out_col, from_avro(expr(f"substring({in_col}, 6)"), avro_schema)
    )


df = spark_stream.load()

df = deserialize_avro(df, "value", "data", avro_schema)


def write_batch(df_batch: DataFrame, epoch_id: int):
    df_batch = df_batch.select("data.*")

    df_batch = df_batch.withColumn("datamonth", expr("substring(date, 0, 7)"))

    df_batch.show()

    (
        df_batch.write.option("header", True)
        .partitionBy("datamonth")
        .format("parquet")
        .mode("append")
        .save(HDFS_URL + "/data")
    )


df.writeStream.foreachBatch(write_batch).start().awaitTermination()
