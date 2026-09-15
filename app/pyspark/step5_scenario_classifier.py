from pyspark.sql.functions import Row, col

"""
This module classifies transactions into scenarios based on a set of conditions defined in a YAML configuration.
The main function, `classify_scenarios`, takes a Spark session, a summary DataFrame, and the scenarios configuration,
and returns a new DataFrame with an additional column for the scenario name.
"""

class ScenarioClassifier:
    def __init__(self, spark, scenarios_yaml, banner_yaml, summary_df):
        self.spark = spark
        self.scenarios = scenarios_yaml['scenarios']
        self.banner_yaml = banner_yaml
        self.summary_df = summary_df

    def evaluate_condition(self, value, operator, target):
        if operator == 'any':
            return True
        elif operator == 'equal':
            return value == target
        elif operator == 'greater_than_or_equal':
            return value >= target
        elif operator == 'less_than_or_equal':
            return value <= target
        elif operator == 'greater_than':
            return value > target
        elif operator == 'less_than':
            return value < target
        
        return False

    def get_scenarios_for_banner(self, banner):
        banner_exclusive = {"SEF", "SRF", "HOC", "BM", "FGLF", "CTP"}
        applicable_scenarios = {}
        for scenario_name, scenario_config in self.scenarios.items():
            normalized_name = scenario_name.lower()

            if banner != "CTP" and normalized_name.startswith("ctp_"):
                continue
            if banner in banner_exclusive and "void" in normalized_name:
                continue
            applicable_scenarios[scenario_name] = scenario_config

        return applicable_scenarios

    def match_scenario(self, row_dict, scenario_config):
        for metric, rule in scenario_config.get("conditions", {}).items():
            if metric not in row_dict:
                return False

            operator = rule.get("operator")
            target = rule.get("value")
            if not self.evaluate_condition(row_dict[metric], operator, target):
                return False

        return True

    def classify_row(self, row_dict, banner):
        applicable_scenarios = self.get_scenarios_for_banner(banner)
        for scenario_name, scenario_config in applicable_scenarios.items():
            if self.match_scenario(row_dict, scenario_config):
                return scenario_name

        return None

    def classify_scenarios(self, banner):
        classified_rows = []
        for row in self.summary_df.collect():
            row_dict = row.asDict()
            scenario_name = self.classify_row(row_dict, banner)
            if scenario_name:
                classified_rows.append(
                    Row(
                        cardnumber=row_dict["cardnumber"],
                        root_purchase_transnumber=row_dict[
                            "root_purchase_transnumber"
                        ],
                        scenario_name=scenario_name,
                    )
                )

        if not classified_rows:
            return self.summary_df.withColumn("scenario_name", col("cardnumber").cast("string"))

        classified_df = self.spark.createDataFrame(classified_rows)

        final_summary_with_scenario_df = self.summary_df.join(classified_df,
                                                              on=["cardnumber", "root_purchase_transnumber"],
                                                              how="left"
                                                              )
        return final_summary_with_scenario_df



