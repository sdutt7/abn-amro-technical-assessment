from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from framework.transform.transform4 import Transform4

spark: SparkSession = (
    SparkSession.builder.appName("test_technical_assessment")
    .enableHiveSupport()
    .getOrCreate()
)


def test_transform4():
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
        (1, "John Doe", "123 AB, Street", 1500),
        (2, "Jane Smith", "234 CD,  Street", 1800),
        (3, "Bob Brown", "345 XY, Street", 900),
        (4, "Alice Grey", "456 HZ,  Street", 2100),
        (5, "Mark White", "567 NN,  Street", 1300),
        (6, "Emily Green", "678 BB, Street", 1600),
        (7, "Joe Black", "789 BV, Street", 800),
        (8, "Lucy Blue", "890 MN, Street", 2200),
        (9, "Matt Pink", "901 WQ, Street", 1700),
        (10, "Eve White", "012 AS, Street", 2300),
    ]
    df2 = spark.createDataFrame(
        source_data2, schema=["id", "name", "address", "sales_amount"]
    )

    actual_df = Transform4(df=df1).transform(df2)

    expected_data = [
        ("IT", "Eve White", 0.9090909090909091, 2300),
        ("IT", "Alice Grey", 0.9, 2100),
        ("IT", "John Doe", 0.8333333333333334, 1500),
        ("IT", "Lucy Blue", 0.8333333333333334, 2200),
        ("Sales", "Mark White", 0.8666666666666667, 1300),
    ]

    expected_df = spark.createDataFrame(
        expected_data,
        schema=[
            "area",
            "name",
            "total_percentage",
            "sales_amount",
        ],
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


test_transform4()
