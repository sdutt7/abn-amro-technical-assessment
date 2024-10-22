import argparse
import sys

from framework.reader.csv_reader import CSVReader
from framework.writer.csv_writer import CSVWriter

from framework.transform.transform1 import Transform1
from framework.transform.transform2 import Transform2
from framework.transform.transform3 import Transform3
from framework.transform.transform4 import Transform4
from framework.transform.transform5 import Transform5
from framework.transform.transform6 import Transform6
from framework.transform.transform7 import Transform7
from framework.transform.transform8 import Transform8

from pyspark.sql import SparkSession


def read_source_file(spark, source_path):
    df = CSVReader(full_path=source_path).read(spark)
    return df


# Main function to parse arguments and execute corresponding functions
def main():
    spark: SparkSession = (
        SparkSession.builder.appName("technical_assessment")
        .enableHiveSupport()
        .getOrCreate()
    )
    parser = argparse.ArgumentParser(description="Process different outputs based on input arguments.")

    # Define the arguments (one for each task)
    parser.add_argument('--dataset_one_path', type=str, help='Process IT Data')
    parser.add_argument('--dataset_two_path', type=str, help='Process IT Data')
    parser.add_argument('--dataset_three_path', type=str, help='Process IT Data')

    parser.add_argument('--transform_name', type=str, help='Process IT Data')

    parser.add_argument('--target_path', type=str, help='Process IT Data')

    # Parse the arguments
    args = parser.parse_args()

    src_dataset1 = read_source_file(spark, args.dataset_one_path)
    src_dataset2 = read_source_file(spark, args.dataset_two_path)
    src_dataset3 = read_source_file(spark, args.dataset_three_path)

    if args.transform_name == "Transform1":
        df = Transform1(df=src_dataset1).transform(src_dataset2)
    elif args.transform_name == "Transform2":
        df = Transform2(df=src_dataset1).transform(src_dataset2)
    elif args.transform_name == "Transform3":
        df = Transform3(df=src_dataset1).transform(src_dataset2)
    elif args.transform_name == "Transform4":
        df = Transform4(df=src_dataset1).transform(src_dataset2)
    elif args.transform_name == "Transform5":
        df = Transform5(df=src_dataset1).transform(src_dataset3)
    elif args.transform_name == "Transform6":
        df = Transform6(df=src_dataset2).transform(src_dataset3)
    elif args.transform_name == "Transform7":
        df = Transform7(df=src_dataset1).transform(src_dataset2, src_dataset3)
    elif args.transform_name == "Transform8":
        df = Transform8(df=src_dataset1).transform(src_dataset2, src_dataset3)
    else:
        raise ValueError("Incorrect transformation name provided")

    CSVWriter(df=df, full_path=args.target_path, ).write()


if __name__ == "__main__":
    main()
