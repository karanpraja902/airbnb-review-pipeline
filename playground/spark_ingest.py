# org.apache.spark:spark-avro_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1

import json

from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col 

from schema_registry.client import SchemaRegistryClient

TOPIC = "avro-schema-test"
KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8099"

spark = SparkSession.builder.appName("ReviewWordCount").getOrCreate()

SCHEMA_REGISTRY_CLIENT = SchemaRegistryClient(SCHEMA_REGISTRY_URL)


avro_schema_object = SCHEMA_REGISTRY_CLIENT.get_schema(TOPIC + "-value")

if avro_schema_object is None:
    raise Exception("Schema for topic {} is not registered yet".format(TOPIC))

avro_schema = json.dumps(avro_schema_object.schema.raw_schema)

print(type(avro_schema), avro_schema)

spark_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 1000)
)

df = spark_stream.load()

df.printSchema()

(
    # Remove the first 5 characters from the value column
    df.selectExpr("substring(value, 6) as avro_value")
    .select(from_avro(col("avro_value"), avro_schema).alias("data"))
    .select("data.*")
    .writeStream.format("console")
    .trigger(processingTime="1 seconds")
    .start()
    .awaitTermination()
)

