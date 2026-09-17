def save_output(output_df, output_path):
    return output_df.write.mode("overwrite").parquet(output_path)