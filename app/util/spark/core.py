import os
import logging
from typing import Callable, Iterable, List, Optional

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import col
from pyspark import SparkConf

from app.util.spark.config import SparkConfig, DEFAULT_SPARK_CONFIG


logger = logging.getLogger(__name__)


def execute_sql(spark: SparkSession, sql) -> Optional[DataFrame]:
    logger.info("Execute sql: ")
    logger.info(f"""
        {sql}
    """)

    return spark.sql(sql)


def drop_table_if_exists(spark: SparkSession, table):
    drop_table_sql = f"DROP TABLE IF EXISTS {table}"
    execute_sql(spark, drop_table_sql)


def get_ddl_col_defs(df: 'DataFrame') -> str:
    return ',\n\t\t'.join([f'{column_name} {date_type}' for column_name, date_type in df.dtypes])


def get_spark_session2(app_name: str, spark_config: SparkConfig = DEFAULT_SPARK_CONFIG) -> SparkSession:
    os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf/"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/etc/gcp_keys/nitish.sahay-customer-analytics.json"

    spark_conf = SparkConf()

    spark_conf.setAppName(app_name)

    spark_conf.set("spark.shuffle.service.enabled", True)
    spark_conf.set("spark.dynamicAllocation.enabled", "false")
    # spark_conf.set("spark.dynamicAllocation.minExecutors", spark_config.min_executors)
    # spark_conf.set("spark.dynamicAllocation.maxExecutors", spark_config.max_executors)

    # Client-mode driver resources must also be passed to spark-submit.
    spark_conf.set("spark.driver.memory", spark_config.driver_memory)
    spark_conf.set("spark.driver.cores", spark_config.driver_cores)
    spark_conf.set("spark.executor.memory", spark_config.executor_memory)
    spark_conf.set("spark.executor.cores", spark_config.executor_cores)
    spark_conf.set("spark.executor.instances", spark_config.executor_instances)

    # Large range/history dimensions are commonly underestimated by the optimizer.
    # Keep automatic broadcast disabled and hint only known-small lookup tables.
    spark_conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark_conf.set("spark.sql.broadcastTimeout", "10800")
    spark_conf.set("hive.exec.dynamic.partition", "true")
    spark_conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark_conf.set("spark.sql.parquet.compression.codec", "snappy")
    spark_conf.set("spark.sql.crossJoin.enabled", "true")

    # ===== Network =====
    spark_conf.set("spark.network.timeout", "600s")
    spark_conf.set("spark.rpc.askTimeout", "600s")
    spark_conf.set("spark.port.maxRetries", "100")
    spark_conf.set("spark.executor.memoryOverhead", "3g")
    spark_conf.set("spark.driver.memoryOverhead", "4g")

    spark_conf.set("spark.sql.execution.arrow.enabled", "true")
    spark_conf.set("spark.network.timeout", "600s")
    spark_conf.set("spark.rpc.askTimeout", "600s")
    spark_conf.set("spark.default.parallelism", "800")
    spark_conf.set("spark.memory.fraction", 0.6)

    spark_conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    spark_conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")

    spark_conf.set("spark.sql.shuffle.partitions", spark_config.shuffle_partitions)

    # Enable Spark to read Hive subdirectory
    spark_conf.set("spark.sql.hive.convertMetastoreParquet", True)
    spark_conf.set("spark.sql.hive.convertMetastoreOrc", True)
    spark_conf.set("mapred.input.dir.recursive", True)
    spark_conf.set("spark.sql.parquet.binaryAsString", True)

    # Experiment
    spark_conf.set("spark.memory.offHeap.enabled", spark_config.memoryOffHeap)
    # spark_conf.set("spark.memory.offHeap.size", spark_config.memoryOffHeapSize)
    spark_conf.set("spark.port.maxRetries", spark_config.portMaxRetries)
    spark_conf.set("spark.sql.parquet.mergeSchema", False)

    # GC limits
    spark_conf.set("spark.sql.adaptive.enabled", "true")
    spark_conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark_conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark_conf.set("spark.eventLog.logSql", "false")  # prevent driver OOM from plan serialization on large tables
    # Event log listener thread was OOMing on large batched-loop jobs (many stages/tasks);
    # an uncaught error there kills the whole SparkContext, so disable it outright.
    spark_conf.set("spark.eventLog.enabled", "false")
    spark_conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
    spark_conf.set("spark.driver.extraJavaOptions", "-XX:+UseG1GC")

    # Bound in-memory job/stage/task bookkeeping (AppStatusStore) so long-running drivers
    # running many small jobs (e.g. batched loops) don't accumulate history until OOM.
    spark_conf.set("spark.ui.retainedJobs", "50")
    spark_conf.set("spark.ui.retainedStages", "100")
    spark_conf.set("spark.ui.retainedTasks", "2000")
    spark_conf.set("spark.sql.ui.retainedExecutions", "50")

    # ===== Write =====
    spark_conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    spark_conf.set("spark.sql.parquet.compression.codec", "snappy")
    spark = SparkSession.builder.config(conf=spark_conf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark


class SparkMixin:
    app_name = None

    def __init__(self, spark: SparkSession = None, spark_config: SparkConfig = None, **kwargs):
        super().__init__(**kwargs)

        if spark:
            logger.info(f'Using an existing SparkSession {spark}.')
            self.spark = spark
        else:
            logger.info(f'Creating a new SparkSession.')
            self._spark_config = spark_config or DEFAULT_SPARK_CONFIG
            self._spark = self._get_spark_session(self._spark_config)

        self._patch_transform()
        self._patch_withCustomColumn()

    @property
    def spark(self) -> SparkSession:
        return self._spark

    @spark.setter
    def spark(self, val: SparkSession):
        self._spark = val

    @staticmethod
    def inspect_df(df: DataFrame):
        df.printSchema()
        df.show(truncate=False)

    def _get_spark_session(self, spark_config: SparkConfig) -> SparkSession:
        _app_name = self.__class__.__name__
        return get_spark_session2(app_name=_app_name, spark_config=spark_config)

    def _patch_transform(self):
        """
        Monkey patch .transform function for Spark 2. It's not needed for Spark 3

        Example usage:
            def with_greeting(df):
                return df.withColumn("greeting", lit("hi"))
            def with_something(df, something):
                return df.withColumn("something", lit(something))

            data = [("jose", 1), ("li", 2), ("liz", 3)]
            source_df = spark.createDataFrame(data, ["name", "age"])
            actual_df = (source_df
                .transform(lambda df: with_greeting(df))
                .transform(lambda df: with_something(df, "crazy")))

            print(actual_df.show())
            +----+---+--------+---------+
            |name|age|greeting|something|
            +----+---+--------+---------+
            |jose|  1|      hi|    crazy|
            |  li|  2|      hi|    crazy|
            | liz|  3|      hi|    crazy|
            +----+---+--------+---------+

        """
        _spark = self.spark

        def transform(input_df: DataFrame, f: Callable, **kwargs):
            return f(input_df, spark=_spark, **kwargs)

        DataFrame.transform = transform

    @staticmethod
    def _patch_withCustomColumn():

        def withCustomColumn(input_df: DataFrame, column_name: str, f: Callable):
            return f(input_df, column_name)

        DataFrame.withCustomColumn = withCustomColumn

    @staticmethod
    def map_cols(column_names: Iterable[str], alias: str = None) -> List[Column]:
        if alias:
            return [col(f'{alias}.{column_name}') for column_name in column_names]
        else:
            return [col(column_name) for column_name in column_names]
