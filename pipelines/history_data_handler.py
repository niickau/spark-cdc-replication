import json
import yaml
from os.path import join, abspath, dirname
from datetime import datetime, timedelta

import pyspark.sql.utils as u
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window


JSON_SCHEMA_PATH = abspath(join(dirname(abspath(__file__)), "../json_schemas"))


class HistoryHandler:
    def __init__(self, spark, ds, db, table, settings):
        self.spark = spark
        self.ds = ds
        self.table = table
        self.schema = db
        self.daily_table = table + "_daily"
        self.history_table = table + "_history"

        self.mode = settings["mode"]
        self.data_dir = settings["data_dir"]
        self.primary_keys = settings["primary_keys"]

        self.json_schemas_path = JSON_SCHEMA_PATH

    def merge_history_table(self):
        try:
            raw_df = self._get_raw_chunk()

            prepared_by_types_df = self._prepare_raw(raw_df)
            print("Number of all new rows: ", prepared_by_types_df.cache().count())

            if self.mode == "merge":
                insert_df = prepared_by_types_df.filter(f.col("__op").isin(["c", "r"])).drop("__op", "__deleted")
                update_df = prepared_by_types_df.filter(f.col("__op") == "u").drop("__op", "__deleted")
                delete_df = prepared_by_types_df.filter(f.col("__op") == "d").select(*self.primary_keys)

                last_updates = self._get_last_updates(update_df)
                self._overwrite_history_table(insert_df, last_updates, delete_df)
                
            elif self.mode == "increment":
                self._increment_history_table(prepared_by_types_df)

        except u.AnalysisException as analysis_e:
            if "Path does not exist" in str(analysis_e):
                print("Apparently, there's no new data to this date")
                pass
            else:
                raise analysis_e
        except Exception as e:
            raise e


    def _load_json_schema(self):
        with open(self.json_schemas_path + f"/{self.table}_schema.json", 'r') as file:
            json_schema = t.StructType.fromJson(json.load(file))

        return json_schema

    def _get_raw_chunk(self):
        day_before = self.ds - timedelta(days=1)
        year = day_before.year
        month = day_before.month
        day = day_before.day

        table_exists = self.spark._jsparkSession.catalog().tableExists(f'{self.schema}.{self.history_table}')

        if table_exists:
            print("Table exists")
            return (
                self.spark.read.orc(f"{self.data_dir}/op_year={year}/op_month={month}/op_day={day}")
            )
        else:
            print("Table doesn't exist")
            return (
                self.spark.read.orc(f"{self.data_dir}/*")
            )

    def _prepare_raw(self, raw_df):
        json_schema = self.spark.read.json(raw_df.rdd.map(lambda row: row.value)).schema

        res = (
            raw_df
                .withColumn("data", f.from_json(f.col("value"), schema=json_schema))
                .withColumnRenamed("timestamp", "kafka_timestamp")
                .select("kafka_timestamp", "data.*")
                .distinct()
        )

        for name in res.schema.names:
            res = res.withColumnRenamed(name, name.replace('/', '_').lower())

        if "timestamp" in res.schema.names:
            if "source_timestamp" not in res.schema.names:
                res = (
                    res
                    .withColumnRenamed("timestamp", "source_timestamp")
                )
            else:
                res = (
                    res
                    .withColumnRenamed("timestamp", "timestamp_in_source")
                )

        return res.withColumnRenamed("kafka_timestamp", "timestamp")

    def _overwrite_history_table(self, insert_df, update_df, delete_df):
        table_exists = self.spark._jsparkSession.catalog().tableExists(f'{self.schema}.{self.history_table}')

        if not table_exists:
            print("Table doesn't exist - creating...")
            self._create_table_from_chunk(insert_df, update_df)

        history = (
            self.spark
                .table(f'{self.schema}.{self.history_table}')
                .drop("op_year", "op_month", "op_day")
        )

        history_clear = (
            history
                .join(f.broadcast(delete_df.union(update_df.select(*self.primary_keys))),
                      self.primary_keys,
                      how="left_anti"
                      )
                .select(*[f.col(c) for c in history.columns])
        )

        final = (
            history_clear
                .union(insert_df.select(*[f.col(c) for c in history.columns]))
                .union(update_df.select(*[f.col(c) for c in history.columns]))
                .dropDuplicates()
        )

        self._save_as_history_table(final)
        return
    
    def _increment_history_table(self, df):
        res = self._make_partitioned(df)

        (
            res
                .dropDuplicates()
                .coalesce(1)
                .write
                .format("orc")
                .mode("append")
                .partitionBy(['op_year', 'op_month', 'op_day'])
                .saveAsTable(f"{self.schema}.{self.history_table}")
        )

        return

    def _get_last_updates(self, update_df):
        w = Window.partitionBy(*self.primary_keys).orderBy(f.desc("timestamp"))
        res = update_df.withColumn("row_number", f.row_number().over(w)).where("row_number = 1").drop("row_number").distinct()
        return res

    def _coalesce_updates(self, update_df):
        expr = [f.first(x, ignorenulls=True).alias(x) for x in update_df.columns if x not in self.primary_keys]
        res = update_df.groupBy(*self.primary_keys).agg(*expr)
        return res

    def _make_partitioned(self, df):
        res = (
            df
                .withColumn("op_year", f.year(f.col("timestamp")))
                .withColumn("op_month", f.month(f.col("timestamp")))
                .withColumn("op_day", f.dayofmonth(f.col("timestamp")))
        )
        return res

    def _save_as_history_table(self, df):
        df = self._make_partitioned(df)
        self._save_as_tmp_table(df)

        res = self._update_tmp_metadata_and_get_data()

        (
            res
                .write
                .format("orc")
                .mode("overwrite")
                .partitionBy(['op_year', 'op_month', 'op_day'])
                .saveAsTable(f"{self.schema}.{self.history_table}")
        )

        self._truncate_daily_and_tmp_tables()
        return

    def _save_as_tmp_table(self, df):
        (
            df
                .write
                .format("orc")
                .mode("overwrite")
                .saveAsTable(f"{self.schema}_tmp.{self.history_table}_tmp")
        )

        return

    def _update_tmp_metadata_and_get_data(self):
        self.spark.catalog.refreshTable(f"{self.schema}_tmp.{self.history_table}_tmp")
        return self.spark.table(f"{self.schema}_tmp.{self.history_table}_tmp")

    def _truncate_daily_and_tmp_tables(self):
        self.spark.sql(f"TRUNCATE TABLE {self.schema}_tmp.{self.history_table}_tmp")
        if self.spark._jsparkSession.catalog().tableExists(f'{self.schema}.{self.daily_table}'):
            self.spark.sql(f"TRUNCATE TABLE {self.schema}.{self.daily_table}")

    def _create_table_from_chunk(self, insert_df, update_df):
        chunk = insert_df if insert_df.count() > 0 else update_df
        self._save_as_history_table(chunk.limit(1))
        self.spark.catalog.refreshTable(f'{self.schema}.{self.history_table}')
        self.spark.sql(f"""TRUNCATE TABLE {self.schema}.{self.history_table}""")
        self.spark.catalog.refreshTable(f'{self.schema}.{self.history_table}')
