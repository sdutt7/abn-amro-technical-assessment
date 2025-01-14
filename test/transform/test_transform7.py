from chispa.dataframe_comparer import *
from pyspark.sql import SparkSession
from framework.transform.transform7 import Transform7
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark: SparkSession = (
    SparkSession.builder.appName("test_technical_assessment")
    .enableHiveSupport()
    .getOrCreate()
)


def test_transform7():
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

    # Define the data
    source_data3 = [
    (1, 1, "Verbruggen-Vermeulen CommV", "Anny Claessens", 45, "Belgium", "Banner", 50),
    (2, 2, "Hendrickx CV", "Lutgarde Van Loock", 41, "Belgium", "Sign", 23),
    (3, 3, "Buysse-Van Dessel VOF", "Georges Jacobs", 22, "Belgium", "Scanner", 48),
    (4, 4, "Heremans VOF", "Josephus Torfs Lemmens", 45, "Belgium", "Desktop", 36),
    (5, 5, "Koninklijke Aelftrud van Wessex", "Mustafa Ehlert", 34, "Netherlands", "Headset", 1),
    (6, 6, "Ardagh Group", "Mila Adriaense-Maas", 30, "Netherlands", "Billboard", 31),
    (7, 7, "Claessens, Verfaillie en Dewulf CommV", "Hasan Claeys", 60, "Belgium", "Social Media Ad", 48),
    (8, 8, "Schenk Kohl e.V.", "Irmela Dörschner B.A.", 60, "Germany", "Scanner", 4),
    (9, 9, "Brizee BV", "Olaf van Beek", 53, "Netherlands", "Printer", 41),
    (10, 10, "de Ruiter Groep", "Tom van Dooren-van der Ven", 38, "Netherlands", "Scanner", 14),
    (11, 1, "Smits, Goris en Hendrickx CommV", "Alina Cuypers", 27, "Belgium", "Business Card", 27),
    (12, 2, "Moenen & Ponci", "Keano Beernink", 50, "Netherlands", "Business Card", 14),
    (13, 3, "Martens VOF", "Jill Desmet", 40, "Belgium", "Banner", 37),
    (14, 4, "Aalts NV", "Sil Slagmolen", 31, "Netherlands", "Business Card", 13),
    (15, 5, "Textor KG", "Klaus Peter Johann", 32, "Germany", "Brochure", 14),
    (16, 6, "van de Coterlet & Vertoor", "Elisabeth Perck", 54, "Netherlands", "Website Design", 12),
    (17, 7, "Bruder Fröhlich GmbH & Co. KG", "Ada Neureuther", 50, "Germany", "Brochure", 15),
    (18, 8, "van der Ven Groep", "Jason de Beer", 36, "Netherlands", "Website Design", 10),
    (19, 9, "Demuynck-Vrancken CV", "Camille Wuyts", 46, "Belgium", "Monitor", 9),
    (20, 10, "Celis, Van Campenhout en Dewulf BV", "Liliane Moerman", 33, "Belgium", "Social Media Ad", 12),
    ]

    # Define the schema
    df3 = spark.createDataFrame(
        source_data3,
        schema=[
            "id",
            "caller_id",
            "company",
            "recipient",
            "age",
            "country",
            "product_sold",
            "quantity",
        ],
    )

    actual_df = Transform7(df=df1).transform(df2=df2, df3=df3)

    expected_data = [
        ("IT", "40 - 60 years", 135),
        ("Sales", "40 - 60 years", 50),
        ("Marketing", "40 - 60 years", 52),
    ]

    expected_df = spark.createDataFrame(
        expected_data,
        schema=StructType(
            [
                StructField("area", StringType(), True),
                StructField("age_group", StringType(), False),
                StructField("quantity", LongType(), True),
            ]
        ),
    )
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


test_transform7()
