from pyspark.sql import functions as F


def load_data(spark, path):
    """CSV жүктеу (header=True, inferSchema=True)."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("nullValue", "")
        .option("emptyValue", "")
        .csv(path)
    )


def clean_data(df):
    """'' -> NULL, содан соң known_for_department/name NULL жолдарын жою."""
    df = df.withColumn(
        "known_for_department",
        F.when(F.trim(F.col("known_for_department")) == "", None)
         .otherwise(F.col("known_for_department")),
    )
    df = df.withColumn(
        "name",
        F.when(F.col("name").isNull() | (F.trim(F.col("name")) == ""), None)
         .otherwise(F.trim(F.col("name"))),
    )
    return df.dropna(subset=["known_for_department", "name"])


def decode_gender(df):
    """gender (0/1/2/3) -> gender_label."""
    gender_value = F.col("gender").cast("int")
    return df.withColumn(
        "gender_label",
        F.when(gender_value == 1, "Female")
         .when(gender_value == 2, "Male")
         .when(gender_value == 3, "Other")
         .otherwise("Unknown"),
    )


def add_popularity_tier(df):
    """popularity_tier бағанын қосу."""
    popularity = F.col("popularity")
    return df.withColumn(
        "popularity_tier",
        F.when(popularity.isNull(), "Unknown")
         .when(popularity >= 40, "A-list")
         .when(popularity >= 20, "B-list")
         .when(popularity >= 10, "C-list")
         .otherwise("Unknown"),
    )


def normalize_names(df):
    """name бағанын trim + initcap арқылы өңдеу."""
    return df.withColumn("name", F.initcap(F.trim(F.col("name"))))
