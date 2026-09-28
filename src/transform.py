from pyspark.sql import functions as F

# the following function cleans the dataset, adds time-based features, adds route-level features, 
# add delay severity flags and categorizes weather conditions.

def clean_and_derive(df):
    df_clean = (
        df.select(
            "FL_DATE", "OP_CARRIER", "ORIGIN", "DEST",
            "DEP_DELAY", "ARR_DELAY",
            "O_PRCP", "D_PRCP", "O_TEMP", "D_TEMP", "O_WSPD", "D_WSPD"
        )
        .withColumn("FL_DATE", F.to_date(F.col("FL_DATE")))
        .filter(F.col("ARR_DELAY").isNotNull())
        .withColumn("YEAR", F.year("FL_DATE"))
        .withColumn("MONTH", F.month("FL_DATE"))
        .withColumn("DAY_OF_WEEK", F.date_format("FL_DATE", "EEEE"))
        .withColumn("ROUTE", F.concat_ws("-", "ORIGIN", "DEST"))
        .withColumn(
            "DELAYED_15",
            F.when(F.col("ARR_DELAY") >= 15, 1).otherwise(0)
        )
        .withColumn(
            "SEVERE_DELAY_60",
            F.when(F.col("ARR_DELAY") >= 60, 1).otherwise(0)
        )
        .withColumn(
            "PRECIPITATION_CATEGORY",
            F.when(F.col("O_PRCP").isNull() | F.col("D_PRCP").isNull(), "Missing")
             .when((F.col("O_PRCP") > 0) | (F.col("D_PRCP") > 0), "Precipitation")
             .otherwise("Dry")
        )
    )
    return df_clean