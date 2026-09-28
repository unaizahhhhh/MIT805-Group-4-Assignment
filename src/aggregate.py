from pyspark.sql import functions as F


def overall_summary(df):
    return df.agg(
        F.count("*").alias("total_flights"),
        F.sum("DELAYED_15").alias("delayed_flights"),
        F.sum("SEVERE_DELAY_60").alias("severe_delays"),
        F.round(F.avg("ARR_DELAY"), 2).alias("average_delay"),
        F.round(F.avg("DELAYED_15") * 100, 2).alias("delay_percentage"),
        F.round(F.avg("SEVERE_DELAY_60") * 100, 2).alias("severe_delay_percentage"),
    )


def carrier_delay_summary(df):
    return (
        df.groupBy("OP_CARRIER")
          .agg(
              F.count("*").alias("total_flights"),
              F.sum("DELAYED_15").alias("delayed_flights"),
              F.round(F.avg("ARR_DELAY"), 2).alias("average_delay"),
              F.round(F.avg("DELAYED_15") * 100, 2).alias("delay_percentage"),
          )
          .orderBy(F.desc("delay_percentage"))
    )


def weather_delay_comparison(df, group_col):
    dry = (
        df.filter(F.col("PRECIPITATION_CATEGORY") == "Dry")
          .groupBy(group_col)
          .agg(
              F.count("*").alias("dry_flights"),
              F.round(F.avg("DELAYED_15") * 100, 2).alias("dry_delay_percentage"),
              F.round(F.avg("ARR_DELAY"), 2).alias("dry_average_delay"),
          )
    )
    rain = (
        df.filter(F.col("PRECIPITATION_CATEGORY") == "Precipitation")
          .groupBy(group_col)
          .agg(
              F.count("*").alias("rain_flights"),
              F.round(F.avg("DELAYED_15") * 100, 2).alias("rain_delay_percentage"),
              F.round(F.avg("ARR_DELAY"), 2).alias("rain_average_delay"),
          )
    )
    return (
        dry.join(rain, on=group_col, how="inner")
           .withColumn(
               "delay_percentage_increase",
               F.round(F.col("rain_delay_percentage") - F.col("dry_delay_percentage"), 2)
           )
           .withColumn(
               "average_delay_increase",
               F.round(F.col("rain_average_delay") - F.col("dry_average_delay"), 2)
           )
           .orderBy(F.desc("delay_percentage_increase"))
    )


def yearly_trend(df):
    return (
        df.groupBy("YEAR")
          .agg(
              F.count("*").alias("total_flights"),
              F.round(F.avg("ARR_DELAY"), 2).alias("average_delay"),
              F.round(F.avg("DELAYED_15") * 100, 2).alias("delay_percentage"),
          )
          .orderBy("YEAR")
    )

def weather_correlation(df):
    return df.select(
        F.corr("O_PRCP", "ARR_DELAY").alias("corr_origin_precip_delay"),
        F.corr("O_WSPD", "ARR_DELAY").alias("corr_origin_wind_delay"),
        F.corr("O_TEMP", "ARR_DELAY").alias("corr_origin_temp_delay"),
        F.corr("D_PRCP", "ARR_DELAY").alias("corr_dest_precip_delay"),
        F.corr("D_WSPD", "ARR_DELAY").alias("corr_dest_wind_delay"),
    )

def day_of_week_summary(df):
    return (
        df.groupBy("DAY_OF_WEEK")
          .agg(
              F.count("*").alias("total_flights"),
              F.round(F.avg("ARR_DELAY"), 2).alias("average_delay"),
              F.round(F.avg("DELAYED_15") * 100, 2).alias("delay_percentage"),
          )
          .orderBy(F.desc("delay_percentage"))
    )

def origin_airport_summary(df, min_flights=5000):
    return (
        df.groupBy("ORIGIN")
          .agg(
              F.count("*").alias("total_flights"),
              F.round(F.avg("DEP_DELAY"), 2).alias("average_dep_delay"),
              F.round(F.avg("DELAYED_15") * 100, 2).alias("delay_percentage"),
          )
          .filter(F.col("total_flights") >= min_flights)
          .orderBy(F.desc("delay_percentage"))
    )

def carrier_delay_percentiles(df):
    result = (
        df.groupBy("OP_CARRIER")
          .agg(
              F.expr("percentile_approx(ARR_DELAY, 0.5)").alias("median_delay"),
              F.expr("percentile_approx(ARR_DELAY, 0.9)").alias("p90_delay"),
              F.round(F.avg("ARR_DELAY"), 2).alias("mean_delay"),
          )
          .orderBy(F.desc("median_delay"))
    )
    return result
