import os
import ast
import json

from datetime import datetime

from utils.spark_session_creator import SparkCreator
from pipelines.daily_data_handler import DailyHandler


ds = os.environ['ds']
ds = datetime.strptime(ds, '%Y-%m-%d')

table = os.environ['table_name']
db = os.environ['db']

settings = ast.literal_eval(os.environ['settings'])
daily = ast.literal_eval(os.environ['daily'])

spark_app_daily_config = daily['spark_app_daily_config']
spark_app_name = "cdc_" + table + "_daily"
spark_user = "user"
spark_app_config = {"deploy-mode": "client",
                    "spark.executor.instances": spark_app_daily_config.get("executor_instances"),
                    "spark.executor.memory": spark_app_daily_config.get("executor_memory"),
                    "spark.executor.cores": spark_app_daily_config.get("executor_cores"),
                    "spark.driver.memory": spark_app_daily_config.get("driver_memory"),
                    "spark.sql.shuffle.partitions": spark_app_daily_config.get("shuffle_partitions")
                    }

spark_class = SparkCreator(spark_user, spark_app_name, spark_app_config)
spark = spark_class.get_spark_session()

handler = DailyHandler(spark, ds, db, table, settings)
handler.merge_daily_table()
print("Daily merge finished.")

