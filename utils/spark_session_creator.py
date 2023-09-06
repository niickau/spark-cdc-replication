import os
import logging

import findspark

from pyspark.sql import SparkSession


findspark.init()
DEFAULT_CONFIG = {"spark.submit.deployMode": "client",
                  "spark.port.maxRetries": 100,
                  "spark.executor.instances": 4,
                  "spark.executor.memory": "4g",
                  "spark.driver.cores": 1,
                  "spark.driver.memory": "1g",
                  "spark.executor.cores": 2,
                  "spark.dynamicAllocation.enabled": False,
                  "spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation": True,
                  "spark.yarn.queue": "batch"}


class SparkCreator:
    def __init__(self,
                 spark_user,
                 spark_app_name,
                 spark_app_config=None
                 ):
        self._spark_user = spark_user
        self._spark_app_name = spark_app_name
        self._spark_app_config = DEFAULT_CONFIG
        if spark_app_config:
            for key, val in spark_app_config.items():
                self._spark_app_config[key] = val

    def get_spark_session(self):
        logging.info('Creating spark session...')

        os.environ['USERNAME'] = self._spark_user
        os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'
        os.environ['PYSPARK_PYTHON'] = 'python3'

        spark = (
            SparkSession
                .builder
                .master('yarn')
                .appName(f'{self._spark_app_name}')
            )

        if self._spark_app_config:
            for key in self._spark_app_config:
                spark = spark.config(key, self._spark_app_config[key])
                logging.info(f'set spark.conf option {key} {self._spark_app_config[key]}')

        spark = (
            spark
                .enableHiveSupport()
                .getOrCreate()
            )

        spark_logger = self._print_spark_app_name_into_logs(spark)
        spark_logger.warn(f"SPARK_APP_NAME: {self._spark_app_name}")

        logging.info('Spark session created.')
        return spark

    def _print_spark_app_name_into_logs(self, spark):
        log4jLogger = spark.sparkContext._jvm.org.apache.log4j
        spark_logger = log4jLogger.LogManager.getLogger(__name__)
        return spark_logger