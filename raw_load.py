import os
import ast

from utils.spark_session_creator import SparkCreator
from pipelines.raw_data_handler import RawHandler

table = os.environ['table_name']
settings = ast.literal_eval(os.environ['settings'])
raw = ast.literal_eval(os.environ['raw'])

spark_app_raw_config = raw['spark_app_raw_config']
spark_app_name = "cdc_" + table + "_raw"
spark_user = "stream"
spark_app_config = {"deploy-mode": "client",
                    "spark.executor.instances": spark_app_raw_config.get("executor_instances"),
                    "spark.executor.memory": spark_app_raw_config.get("executor_memory"),
                    "spark.executor.cores": spark_app_raw_config.get("executor_cores"),
                    "spark.driver.memory": spark_app_raw_config.get("driver_memory"),
                    "spark.sql.shuffle.partitions": spark_app_raw_config.get("shuffle_partitions")
                    }

spark_class = SparkCreator(spark_user, spark_app_name, spark_app_config)
spark = spark_class.get_spark_session()

handler = RawHandler(spark, settings)
handler.kafka_sink()
print("Ingestion finished.")

