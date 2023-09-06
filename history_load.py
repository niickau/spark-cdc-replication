import os
import ast
import argparse

from datetime import datetime

from utils.spark_session_creator import SparkCreator
from pipelines.history_data_handler import HistoryHandler


parser = argparse.ArgumentParser()
parser.add_argument("--mode")
args = parser.parse_args()

mode = args.mode
print("Execution mode: ", mode)

if mode == "airflow":
    ds = os.environ['ds']

elif mode == "manual":
    ds = input("Input calc_date in format 'YYYY-MM-DD': ")

ds = datetime.strptime(ds, '%Y-%m-%d')
table = os.environ['table_name']
db = os.environ['db']

settings = ast.literal_eval(os.environ['settings'])
history = ast.literal_eval(os.environ['history'])

spark_app_history_config = history['spark_app_history_config']
spark_app_name = "cdc_" + table + "_history"
spark_user = "user"
spark_app_config = {"deploy-mode": "client",
                    "spark.executor.instances": spark_app_history_config.get("executor_instances"),
                    "spark.executor.memory": spark_app_history_config.get("executor_memory"),
                    "spark.executor.cores": spark_app_history_config.get("executor_cores"),
                    "spark.driver.memory": spark_app_history_config.get("driver_memory"),
                    "spark.sql.shuffle.partitions": spark_app_history_config.get("shuffle_partitions"),
                    "spark.sql.autoBroadcastJoinThreshold": -1
                    }

spark_class = SparkCreator(spark_user, spark_app_name, spark_app_config)
spark = spark_class.get_spark_session()

handler = HistoryHandler(spark, ds, db, table, settings)
handler.merge_history_table()
print("History merge finished.")


