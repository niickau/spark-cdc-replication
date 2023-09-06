import json
import yaml
from os.path import join, abspath, dirname

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window


CONFIG_PATH = abspath(join(dirname(abspath(__file__)), "../configs"))


class RawHandler:
    def __init__(self, spark, settings):
        self.spark = spark
        self.config = CONFIG_PATH

        self.ckpt_dir = settings["ckpt_dir"]
        self.data_dir = settings["data_dir"]
        self.kafka_topic = settings["kafka_topic"]

        self.kafka_stream = self._get_kafka_streaming_object()

    def _load_kafka_config(self):
        with open(self.config + "/kafka_config.yml", 'r') as file:
            kafka_config = yaml.safe_load(file)

        return kafka_config

    def _get_kafka_streaming_object(self):
        kafka_config = self._load_kafka_config()
        kafka_jaas_conf = f'org.apache.kafka.common.security.scram.ScramLoginModule required \
                   username=\"{kafka_config.get("user")}\" \
                   password=\"{kafka_config.get("password")}\";'

        return (
            self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", kafka_config.get("brokers"))
                .option("kafka.sasl.jaas.config", kafka_jaas_conf)
                .option("kafka.security.protocol", 'SASL_PLAINTEXT')
                .option("startingOffsets", "earliest")
                .option("kafka.sasl.mechanism", 'SCRAM-SHA-256')
                .option("subscribe", self.kafka_topic)
                .option("failOnDataLoss", "false")
                .load()
                .repartition(int(self.spark.sparkContext.getConf().get("spark.sql.shuffle.partitions")))
        )

    def foreach_batch_function(self, df, epoch_id):
        try:
            raw_df = df.selectExpr("CAST(timestamp AS Timestamp)", "CAST(value AS STRING)")
            self._save_raw(raw_df)

        except Exception as e:
            raise e

    def kafka_sink(self):
        (
            self.kafka_stream.writeStream
                .outputMode("append")
                .foreachBatch(self.foreach_batch_function)
                .trigger(once=True)
                .option("checkpointLocation", self.ckpt_dir)
                .start()
                .awaitTermination()
        )

    def _make_partitioned(self, df):
        res = (
            df
                .withColumn("op_year", f.year(f.col("timestamp")))
                .withColumn("op_month", f.month(f.col("timestamp")))
                .withColumn("op_day", f.dayofmonth(f.col("timestamp")))
        )
        return res

    def _save_raw(self, df):
        res = self._make_partitioned(df)

        (
            res
                .select("timestamp", "value", "op_year", "op_month", "op_day")
                .write
                .partitionBy("op_year", "op_month", "op_day")
                .mode("append")
                .orc(self.data_dir)
        )
