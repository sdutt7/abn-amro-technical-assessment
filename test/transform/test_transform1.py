from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from framework.transform.transform1 import Transform1

spark: SparkSession = (
    SparkSession.builder.appName("test_technical_assessment")
    .enableHiveSupport()
    .getOrCreate()
)


def test_transform1():
    source_data1 = [
        (1, "IT", 120, 100),
        (2, "IT", 100, 80),
        (3, "Marketing", 90, 60),
        (4, "IT", 200, 180),
        (5, "Sales", 150, 130),
        (6, "IT", 110, 90),
        (7, "Marketing", 70, 50),
        (8, "IT", 180, 150),
        (9, "Sales", 160, 120),
        (10, "IT", 220, 200),
    ]
    df1 = spark.createDataFrame(
        source_data1, schema=["id", "area", "calls_made", "calls_successful"]
    )
    source_data2 = [
        (1, "John Doe", "123 Street", 1500),
        (2, "Jane Smith", "234 Street", 1800),
        (3, "Bob Brown", "345 Street", 900),
        (4, "Alice Grey", "456 Street", 2100),
        (5, "Mark White", "567 Street", 1300),
        (6, "Emily Green", "678 Street", 1600),
        (7, "Joe Black", "789 Street", 800),
        (8, "Lucy Blue", "890 Street", 2200),
        (9, "Matt Pink", "901 Street", 1700),
        (10, "Eve White", "012 Street", 2300),
    ]
    df2 = spark.createDataFrame(
        source_data2, schema=["id", "name", "address", "sales_amount"]
    )

    actual_df = Transform1(df=df1).transform(df2)

    expected_data = [
        (10, "IT", 220, 200, "Eve White", "012 Street", 2300),
        (8, "IT", 180, 150, "Lucy Blue", "890 Street", 2200),
        (4, "IT", 200, 180, "Alice Grey", "456 Street", 2100),
        (2, "IT", 100, 80, "Jane Smith", "234 Street", 1800),
        (6, "IT", 110, 90, "Emily Green", "678 Street", 1600),
        (1, "IT", 120, 100, "John Doe", "123 Street", 1500),
    ]

    expected_df = spark.createDataFrame(
        expected_data,
        schema=[
            "id",
            "area",
            "calls_made",
            "calls_successful",
            "name",
            "address",
            "sales_amount",
        ],
    )
    assert_df_equality(actual_df, expected_df)


test_transform1()
