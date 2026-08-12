from typing import List, Callable, Iterable, Dict, Union
from functools import partial, reduce

from pyspark.sql import Window, Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import percent_rank, rand, asc, desc, row_number, col, substring, length, expr, lit, when

from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder, VectorSlicer, VectorIndexer, IndexToString
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
import pandas as pd

# .withCustomColumn functions ######


def percentile_rank_with_fallback(
        partition_cols: List[str],
        order_bys: Dict[str, Callable[[str], Column]],
        fallback_col: Column
) -> Callable[[DataFrame], DataFrame]:
    """
    order_bys: a dictionary whose
        - key is column name as a string
        - value is one of Spark SQL functions: asc, desc
        i.e.
        {
            'sales': desc,
            'qty': desc,
            'customer_id': asc
        }
    """
    if order_bys:
        _order_bys = (order_f(col) for col, order_f in order_bys.items())
        _window_spec = Window.partitionBy(partition_cols).orderBy(*_order_bys, fallback_col)
    else:
        _window_spec = Window.partitionBy(partition_cols).orderBy(fallback_col)

    def _(df: DataFrame, col_name: str) -> DataFrame:
        new_df = df \
            .withColumn(
            colName=col_name,
            col=percent_rank().over(_window_spec)
        )

        return new_df

    return _


def randomized_percentile_rank(partition_cols: List[str], seed: int) -> Callable[[DataFrame], DataFrame]:
    func = partial(percentile_rank_with_fallback, order_bys=None)

    return func(partition_cols=partition_cols, fallback_col=rand(seed))


def randomized_rank(partition_cols: Iterable[str] = None, seed: int = 0) -> Callable[[DataFrame], DataFrame]:
    if partition_cols:
        _partition_cols = list(partition_cols)
        _window_spec = Window.partitionBy(_partition_cols).orderBy(rand(seed))
    else:
        _window_spec = Window.orderBy(rand(seed))

    def _(df: DataFrame, col_name: str) -> DataFrame:
        return df \
            .withColumn(
                colName=col_name,
                col=row_number().over(_window_spec)
            )

    return _


# .transform functions ######

def dedupe(partition_cols: Iterable[str] = None, seed: int = 0) -> Callable[[DataFrame], DataFrame]:
    def _(input_df: DataFrame, **kwargs) -> DataFrame:
        if partition_cols:
            _partition_cols = list(partition_cols)
            _window_spec = Window.partitionBy(_partition_cols).orderBy(rand(seed))
        else:
            _window_spec = Window.orderBy(rand(seed))
        return input_df \
            .withColumn('_rank', row_number().over(_window_spec)) \
            .filter(col('_rank') == 1) \
            .drop('_rank')

    return _


def suppress_with_randomized_rank(
        rows: int, partition_cols: Iterable[str] = None, seed: int = 0, rank_col_name: str = None
) -> Callable[[DataFrame], DataFrame]:
    def _(input_df: DataFrame, **kwargs) -> DataFrame:
        _rank_col_name = rank_col_name or '_rank'
        return input_df \
            .withCustomColumn(_rank_col_name, randomized_rank(partition_cols, seed)) \
            .filter(col(_rank_col_name) <= rows)

    return _


def suppress_with_defined_order(
        rows: int,
        order_bys: Dict[str, Callable[[str], Column]],
        partition_cols: Iterable[str] = None,
        rank_col_name: str = None
) -> Callable[[DataFrame], DataFrame]:
    _order_bys = (order_f(col_) for col_, order_f in order_bys.items())
    if partition_cols:
        _window_spec = Window.partitionBy(partition_cols).orderBy(*_order_bys)
    else:
        _window_spec = Window.orderBy(*_order_bys)

    def _(input_df: DataFrame, **kwargs) -> DataFrame:
        _rank_col_name = rank_col_name or '_rank'
        return input_df \
            .withColumn(_rank_col_name, row_number().over(_window_spec)) \
            .filter(col(_rank_col_name) <= rows)

    return _


# .withColumn functions ######

def remove_trailing_chars(col: Union[Column, str], n: int) -> Column:
    if isinstance(col, Column):
        return col.substr(startPos=lit(0), length=length(col) - lit(n))

    if isinstance(col, str):
        return expr(f'substring({col}, 0, length({col}) - {n})')

    raise TypeError(
        "Invalid argument, not a string or column: "
        "{0} of type {1}. "
        "For column literals, use 'lit', 'array', 'struct' or 'create_map' "
        "function.".format(col, type(col))
    )


# other functions ######

def union_dfs(dfs: List[DataFrame]) -> DataFrame:
    return reduce(lambda df1, df2: df1.unionByName(df2), dfs)

def fill_na_dataframe(input_df: DataFrame) -> DataFrame:
    """Replace all null/NaN with 0 if numeric and 'unknown' if non-numeric 

    Args:
        input_df (DataFrame): dataframe with null values

    Returns:
        DataFrame: cleaned dataframe with null values replaced
    """
    df_types = [(x.name, x.dataType) for x in input_df.schema.fields]
    pd_df_types = pd.DataFrame(df_types, columns = ['feature', 'structType'])
    for cols in input_df.columns:
        if(str(pd_df_types[pd_df_types['feature'] == cols]['structType'].iloc[0]) == "StringType"):
            input_df = input_df.fillna('unknown', subset=cols)
            input_df = input_df.withColumn(cols, when(input_df[cols] == "", 'unknown').otherwise(input_df[cols]))
        if(str(pd_df_types[pd_df_types['feature'] == cols]['structType'].iloc[0]) == "IntegerType" or str(pd_df_types[pd_df_types['feature'] == cols]['structType'].iloc[0]) == "DoubleType" or str(pd_df_types[pd_df_types['feature'] == cols]['structType'].iloc[0]) == "LongType"):
            input_df = input_df.fillna(0, subset=cols)
    return(input_df)

def get_dummy(df,categoricalCols,continuousCols,labelCol=None,identifier=None):
    """Convert categorical variable into dummy/indicator variables

    Parameters
	----------
    	df (spark dataFrame): pyspark dataframe with columns to convert to dummy variables

    	categoricalCols (str): categorical column headers

		continuousCols (str): numeric/continuous column headers

		labelCol (str, default = None): label col header

        identifier (list of str, default = None): list of columns to keep apart from label and features

	Returns
	-------
    	dummy coded Spark DataFrame with 2 columns only. 
        features - a dense vector with feature variables
        label - label column
        identifier - columns marked as identifers 

	Dependant Libraries/Functions
	-----------------------------
	from pyspark.ml.feature import StringIndexer, VectorAssembler, OneHotEncoder
    from pyspark.ml import Pipeline
    import pyspark.sql.functions as f
    """
    # default setting: dropLast=True
    df_types = [(x.name, str(x.dataType)) for x in df.schema.fields]
    pd_df_types = pd.DataFrame(df_types, columns = ['feature', 'structType'])
    categoricalColsStrings = []
    categoricalColsIntegers = []
    for cols in categoricalCols:
        if(str(pd_df_types[pd_df_types['feature'] == cols]['structType'].iloc[0]) == "StringType"):
            categoricalColsStrings.append(cols)
        if(str(pd_df_types[pd_df_types['feature'] == cols]['structType'].iloc[0]) == "IntegerType"):
            categoricalColsIntegers.append(cols)
    indexers = [StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c)) for c in categoricalColsStrings]
    encoders_strings = [OneHotEncoder(inputCol=indexer.getOutputCol(), outputCol="{0}_encoded".format(indexer.getOutputCol())) for indexer in indexers]
    encoders_integers = [OneHotEncoder(inputCol=cols, outputCol="{0}_encoded".format(cols)) for cols in categoricalColsIntegers]
    assembler = VectorAssembler(inputCols=[encoders_string.getOutputCol() for encoders_string in encoders_strings] + [encoders_integer.getOutputCol() for encoders_integer in encoders_integers] + continuousCols, outputCol="features")
    pipeline = Pipeline(stages=indexers + encoders_strings + encoders_integers + [assembler])
    model=pipeline.fit(df)
    data = model.transform(df)
    if identifier != None:
        keepCols = identifier
        keepCols.append('features')
    else:
        keepCols = ["features"]
    if labelCol == None:
        return data.select(keepCols)
    else:
        keepCols.append('label')
        data = data.withColumn('label', col(labelCol))
        return data.select(keepCols)
    
def extract_features_training_df(dataset, categoricalCols, continuousCols, labelCol = "label", featuresCol = "features", cumulativeScore = 0.75):
    """Extract most important features based on cumulative featureImportance scores from a RandomForestClassifier.
	Parameters
	----------
    	dataset (spark dataFrame): dataFrame

    	categoricalCols (int): List of categorical feature names with values converted to int dtype using a stringIndexer already

		continuousCols (int): List of continuous feature names

		labelCol (str, optional): The name of the label column

		featuresCol (str, optional): The name of the features column

        cumulativeScore (double, optional): Include features with cumulative importance score below this value, after sorting them high to low

	Returns
	-------
    	1. varlist = pandas dataframe feature list and their corresponding importane sorted high to low and filtered based on cumulativeScore parameter
        2. dataset3 = spark dataFrame with columns features and features2, tyoe vector, having full and inportant feature list respectively

	Dependant Libraries/Functions
	-----------------------------
	from pyspark.ml.feature import VectorAssembler
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import OneHotEncoder, VectorAssembler, VectorSlicer
    from pyspark.sql.functions import col
    from pyspark.ml.classification import RandomForestClassifier
    import pandas as pd

	Raises
	------

	Examples
	--------
	varlist, df = ExtractFeatureImp(df_train, catCols, colnames_ctr, "label", "features", 0.85)

	"""
    df_types = [(x.name, str(x.dataType)) for x in dataset.schema.fields]
    pd_df_types = pd.DataFrame(df_types, columns = ['feature', 'structType'])
    categoricalColsStrings = []
    categoricalColsIntegers = []
    for cols in categoricalCols:
        if(str(pd_df_types[pd_df_types['feature'] == cols]['structType'].iloc[0]) == "StringType"):
            categoricalColsStrings.append(cols)
        if(str(pd_df_types[pd_df_types['feature'] == cols]['structType'].iloc[0]) == "IntegerType"):
            categoricalColsIntegers.append(cols)
    
    labelIndexer = StringIndexer(inputCol=labelCol, outputCol='indexedLabel').fit(dataset)            
    indexers = [StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c)) for c in categoricalColsStrings]
    encoders_strings = [OneHotEncoder(inputCol=indexer.getOutputCol(), outputCol="{0}_encoded".format(indexer.getOutputCol())) for indexer in indexers]
    encoders_integers = [OneHotEncoder(inputCol=cols, outputCol="{0}_encoded".format(cols)) for cols in categoricalColsIntegers]
    assembler = VectorAssembler(inputCols=[encoders_string.getOutputCol() for encoders_string in encoders_strings] + 
                                [encoders_integer.getOutputCol() for encoders_integer in encoders_integers] + 
                                continuousCols, 
                                outputCol="features")
    featureIndexer = VectorIndexer(inputCol='features', outputCol='indexedFeatures')
    rf = RandomForestClassifier(labelCol = 'indexedLabel', featuresCol = 'indexedFeatures')
    labelConverter = IndexToString(inputCol="prediction", 
                               outputCol="predictedLabel",
                               labels=labelIndexer.labels)
    pipe = Pipeline(stages = indexers + encoders_strings + encoders_integers + [labelIndexer, assembler, featureIndexer, rf, labelConverter])
    mod = pipe.fit(dataset)
    dataset2 = mod.transform(dataset)
    featureImp = mod.stages[-2].featureImportances
    list_extract = []
    for i in dataset2.schema[featuresCol].metadata["ml_attr"]["attrs"]:
        list_extract = list_extract + dataset2.schema[featuresCol].metadata["ml_attr"]["attrs"][i]
    varlist = pd.DataFrame(list_extract)
    varlist['score'] = varlist['idx'].apply(lambda x: featureImp[x])
    varlist = varlist.sort_values('score', ascending = False)
    varlist['cumScore'] = varlist['score'].cumsum()
    varlist = varlist[varlist['cumScore'] <= cumulativeScore]
    varidx  = [x for x in varlist['idx']]
    slicer = VectorSlicer(inputCol = "features", outputCol = "features2", indices=varidx)
    dataset3 = slicer.transform(dataset2)
    dataset3 = dataset3.drop('rawPrediction', 'probability', 'prediction')
    return varlist, dataset3