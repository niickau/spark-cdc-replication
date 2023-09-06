import json
import yaml
from os.path import join, abspath, dirname
from datetime import datetime, timedelta

import pyspark.sql.utils as u
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window


JSON_SCHEMA_PATH = abspath(join(dirname(abspath(__file__)), "../json_schemas"))


class DailyHandler:
    def __init__(self, spark, ds, db, table, settings):
        self.spark = spark
        self.ds = ds
        self.table = table
        self.schema = db
        self.daily_table = table + "_daily"
        self.data_dir = settings["data_dir"]
        self.primary_keys = settings["primary_keys"]
        self.json_schemas_path = JSON_SCHEMA_PATH

    def merge_daily_table(self):
        try:
            raw_df = self._get_raw_chunk()

            prepared_by_types_df = self._prepare_raw(raw_df)
            print("Number of all new rows: ", prepared_by_types_df.cache().count())

            insert_df = prepared_by_types_df.filter(f.col("__op").isin(["c", "r"])).drop("__op", "__deleted")
            update_df = prepared_by_types_df.filter(f.col("__op") == "u").drop("__op", "__deleted")
            delete_df = prepared_by_types_df.filter(f.col("__op") == "d").select(*self.primary_keys)

            last_updates = self._get_last_updates(update_df)
            self._overwrite_daily_table(insert_df, last_updates, delete_df)
        except u.AnalysisException:
            print("Apparently, there's no new data")
            pass
        except Exception as e:
            raise e

    def _load_json_schema(self):
        with open(self.json_schemas_path + f"/{self.table}_schema.json", 'r') as file:
            json_schema = t.StructType.fromJson(json.load(file))

        return json_schema

    def _get_raw_chunk(self):
        year = self.ds.year
        month = self.ds.month
        day = self.ds.day

        return (
            self.spark.read.orc(f"{self.data_dir}/op_year={year}/op_month={month}/op_day={day}")
        )

    def _prepare_raw(self, raw_df):
        json_schema = self.spark.read.json(raw_df.rdd.map(lambda row: row.value)).schema

        res = (
            raw_df
                .withColumn("data", f.from_json(f.col("value"), schema=json_schema))
                .select("timestamp", "data.*")

        )

        for name in res.schema.names:
            res = res.withColumnRenamed(name, name.replace('/', '_'))

        return res

    def _overwrite_daily_table(self, insert_df, update_df, delete_df):
        table_exists = self.spark._jsparkSession.catalog().tableExists(f'{self.schema}.{self.daily_table}')

        if not table_exists:
            print("Table doesn't exist - creating...")
            self._create_table_from_chunk(insert_df, update_df)

        daily = (
            self.spark
                .table(f'{self.schema}.{self.daily_table}')
                .drop("op_year", "op_month", "op_day")
        )

        daily_clear = (
            daily
                .join(f.broadcast(delete_df.union(update_df.select(*self.primary_keys))),
                      self.primary_keys,
                      how="left_anti"
                      )
                .select(*[f.col(c) for c in daily.columns])
        )

        final = (
            daily_clear
                .union(insert_df.select(*[f.col(c) for c in daily.columns]))
                .union(update_df.select(*[f.col(c) for c in daily.columns]))
                .dropDuplicates()
        )

        self._save_as_daily_table(final)

    def _get_last_updates(self, update_df):
        w = Window.partitionBy(*self.primary_keys).orderBy(f.desc("timestamp"))
        res = update_df.withColumn("row_number", f.row_number().over(w)).where("row_number = 1").drop("row_number")
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

    def _save_as_daily_table(self, df):
        df = self._make_partitioned(df)
        self._save_as_tmp_table(df)

        res = self._update_tmp_metadata_and_get_data()

        (
            res
                .write
                .format("orc")
                .mode("overwrite")
                .saveAsTable(f"{self.schema}.{self.daily_table}")
        )

        self._clear_tmp_table()

    def _save_as_tmp_table(self, df):
        (
            df
                .write
                .format("orc")
                .mode("overwrite")
                .saveAsTable(f"{self.schema}_tmp.{self.daily_table}_tmp")
        )

    def _update_tmp_metadata_and_get_data(self):
        self.spark.catalog.refreshTable(f"{self.schema}_tmp.{self.daily_table}_tmp")
        return self.spark.table(f"{self.schema}_tmp.{self.daily_table}_tmp")

    def _clear_tmp_table(self):
        self.spark.sql(f"""TRUNCATE TABLE {self.schema}_tmp.{self.daily_table}_tmp""")

    def _create_table_from_chunk(self, insert_df, update_df):
        chunk = insert_df if insert_df.count() > 0 else update_df
        self._save_as_daily_table(chunk.limit(1))
        self.spark.catalog.refreshTable(f'{self.schema}.{self.daily_table}')
        self.spark.sql(f"""TRUNCATE TABLE {self.schema}.{self.daily_table}""")
        self.spark.catalog.refreshTable(f'{self.schema}.{self.daily_table}')
