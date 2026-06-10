from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import logging

logger = logging.getLogger(__name__)
def clean_user_data(df: DataFrame) -> DataFrame:
    logger.info("Staring to clean user data")
    cleaned_df = df \
        .fillna({"age": 0}) \
        .withColumn("name", F.upper(F.col("name"))) \
        .filter(F.col("age") >= 0)
    logger.info("Finished cleaning user data")
    return cleaned_df