from pyspark.sql import Row

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

    def capability_supported(self, banner, scenario_config, banner_yaml):
        required_capabilities = scenario_config.get('requires', {})
        banner_capability = banner_yaml.get(banner, {})
        for capability, required in required_capabilities.items():
            actual_value = banner_capability.get(capability, False)
            if required != actual_value:
                return False
        return True

    def match_scenario(self, row_dict, scenario_config):
        conditions = scenario_config['conditions']
        for metric, rules in conditions.items():
            row_value = row_dict[metric]
            for operator, target in rules.items():
                if not self.evaluate_condition(row_value, operator, target):
                    return False
        return True

    def classify_row(self, row_dict):
        banner = row_dict.get('banner')
        for scenario_name, scenario_config in self.scenarios.items():
            if not self.capability_supported(banner, scenario_config, self.banner_yaml):
                continue
            if self.match_scenario(row_dict, scenario_config):
                return scenario_name
        return None

    def classify_scenarios(self):
        classified_rows = []
        for row in self.summary_df.collect():
            scenario = self.classify_row(row_dict = row.asDict()) # Collecting the row as a dictionary for easier access to column values

            if scenario:
                output = row.asDict()
                output['scenario_name'] = scenario
                classified_rows.append(Row(**output)
                                    )

        return self.spark.createDataFrame(classified_rows)