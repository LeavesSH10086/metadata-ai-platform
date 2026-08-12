class SparkConfig:
    def __init__(
            self,
            shuffle_partitions: int = 400,
            min_executors: int = 5,
            max_executors: int = 50,
            driver_memory: str = '16g',
            driver_cores: int = 4,
            executor_memory: str = '16g',
            executor_cores: int = 3,
            executor_instances: int = 35,
            maxResultSize: str = '4g',
            memoryOffHeap:str = False,
            # memoryOffHeapSize: str = '16g',
            portMaxRetries: int = 100,
            partitionOverwriteMode: str = 'dynamic',
            parquetOutputCommitterClass: str = 'org.apache.parquet.hadoop.ParquetOutputCommitter',
            commitProtocolClass: str = 'org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol',

    ):
        self.shuffle_partitions = shuffle_partitions
        self.min_executors = min_executors
        self.max_executors = max_executors
        self.driver_memory = driver_memory
        self.driver_cores = driver_cores
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.executor_instances = executor_instances
        self.maxResultSize = maxResultSize
        self.memoryOffHeap = memoryOffHeap
        # self.memoryOffHeapSize = memoryOffHeapSize
        self.portMaxRetries = portMaxRetries
        self.partitionOverwriteMode = partitionOverwriteMode
        self.parquetOutputCommitterClass = parquetOutputCommitterClass
        self.commitProtocolClass = commitProtocolClass


DEFAULT_SPARK_CONFIG = SparkConfig()