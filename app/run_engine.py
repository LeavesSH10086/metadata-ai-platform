from pathlib import Path

from app.util.spark.core import get_spark_session2
from app.util.logger import LogMixin
from app.pyspark.step0_yaml_loader import load_yaml
from app.pyspark.step1_build_base_df import build_base_df
from app.pyspark.step2_normalized_df import build_normalized_df
from app.pyspark.step3_recursive_graph_expansion import build_recursive_graph_expansion_df
from app.pyspark.step4_lifecycle_builder import build_family_summary_df
from app.pyspark.step5_scenario_classifier import ScenarioClassifier
from app.pyspark.step6_save_output import save_output

logger = LogMixin().logger
APP_DIR = Path(__file__).resolve().parent

def run_engine(spark):

    logger.info("Loading runtime parameters and scenarios...")
    runtime_parameters = load_yaml(APP_DIR/"metadata/runtime_parameters.yaml")
    scenarios_yaml = load_yaml(APP_DIR/"metadata/scenario_classification.yaml")
    banner_yaml = load_yaml(APP_DIR/"metadata/banner_capabilities.yaml")
    logger.info("Runtime parameters: %s", runtime_parameters)
    logger.info("Scenarios YAML: %s", scenarios_yaml)
    logger.info("Banner YAML: %s", banner_yaml)

    logger.info("Building base dataframe...")
    # the source of base_df is lms_point_detail. Several filters are applied to decrease the processing time.
    # Filters include: partition_batch_ts, record_type
    step1_base_extract_hql_path = runtime_parameters["hql_templates"]["step1_base_extract"]
    base_df = build_base_df(spark=spark,
                            runtime_parameters=runtime_parameters,
                            hql_file_path=step1_base_extract_hql_path)

    logger.info("Normalizing dataframe...")
    step2_normalize_events_hql_path = runtime_parameters["hql_templates"]["step2_normalize_events"]
    normalized_df = build_normalized_df(spark=spark, 
                                        base_df=base_df,
                                        hql_file_path=step2_normalize_events_hql_path)

    logger.info("Performing recursive graph expansion...")
    dynamic_expansion_df = build_recursive_graph_expansion_df(normalized_df=normalized_df)
    
    logger.info("Building family summary dataframe...")
    family_summary_df = build_family_summary_df(dynamic_expansion_df=dynamic_expansion_df, 
                                                normalized_df=normalized_df)
    
    logger.info("Classifying scenarios...")
    scenario_classifier = ScenarioClassifier(spark=spark, 
                                             summary_df=family_summary_df,
                                             scenarios_yaml=scenarios_yaml, 
                                             banner_yaml=banner_yaml)
    scenario_df = scenario_classifier.classify_scenarios()
    scenario_df.show(10, truncate=False)

    # logger.info("Saving output...")
    # save_output(scenario_df, runtime_parameters.get("output_path"))

if __name__ == "__main__":
    spark = get_spark_session2("Lifecycle Detection Engine")
    run_engine(spark)