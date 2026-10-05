Purpose
--------
The engine identifies transaction lifecycle relationships from lms_point_detail

Processing model
-----------------
1. load yaml file (step0_yaml_loader.py)
2. build base dataframe from raw (step1_build_base_df.py)
3. normalize the base dataframe by performing necessary transformations and cleaning (step2_build_normailized_df.py)
4. build transaction families (step3_rcursive_graph_expansion.py)
5. build relationship (step4_lifecyle_builder.py)
6. classify each scenario (step5_scenario_classifier.py)
7. create final table (step6_final_table_builder.py)
8. save final table to datalake (step7_save_output.py)



