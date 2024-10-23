"""
This script provides a framework for reading, transforming, and writing CSV datasets using PySpark.

It reads data from multiple CSV files, applies a selected transformation, and writes the resulting
data to a target location. The user specifies the input datasets, the transformation to apply, and
the output file path via command-line arguments.

Usage:
python3 main.py --dataset_one_path <file1 full_path>
--dataset_two_path <file2 full_path>
--dataset_three_path <file3 full_path>
--transform_name <name of transformation>
--target_path <target full_path>

For example:
python3 main.py --dataset_one_path /assignment_files/dataset_one.csv
--dataset_two_path /assignment_files/dataset_two.csv
--dataset_three_path /assignment_files/dataset_three.csv
--transform_name Transform1
--target_path output/it_data
"""

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
    """
    Reads a CSV file from the given source path using the CSVReader class.

    :param spark: Active Spark session used to read the CSV file.
    :param source_path: Full path to the CSV file.
    :return: A PySpark DataFrame containing the data from the CSV file.
    """
    df = CSVReader(full_path=source_path).read(spark)
    return df


# Main function to parse arguments and execute corresponding functions
def main():
    """
    Main function to parse command-line arguments, read datasets, apply the specified transformation,
    and write the transformed data to the target file.

    This function initializes a Spark session, reads the input datasets from the provided paths, and
    applies one of the available transformations based on the user input. The transformed DataFrame
    is then written to the specified output file path.

    Command-Line Arguments:
        --dataset_one_path (str): Full path to the first CSV dataset.
        --dataset_two_path (str): Full path to the second CSV dataset.
        --dataset_three_path (str): Full path to the third CSV dataset.
        --transform_name (str): Name of the transformation to apply (Transform1 to Transform8).
        --target_path (str): Full path to the output file.

    Raises:
        ValueError: If an incorrect transformation name is provided.
    """
    spark: SparkSession = (
        SparkSession.builder.appName("technical_assessment")
        .enableHiveSupport()
        .getOrCreate()
    )
    parser = argparse.ArgumentParser(
        description="Process different outputs based on input arguments."
    )

    # Define the arguments (one for each task)
    parser.add_argument(
        "--dataset_one_path", type=str, help="Full path to dataset_one.csv"
    )
    parser.add_argument(
        "--dataset_two_path", type=str, help="Full path to dataset_two.csv"
    )
    parser.add_argument(
        "--dataset_three_path", type=str, help="Full path to dataset_three.csv"
    )

    parser.add_argument(
        "--transform_name",
        type=str,
        help="Name of the transformation to be applied. For example: Transform1",
    )

    parser.add_argument("--target_path", type=str, help="Target path to write output")

    # Parse the arguments
    args = parser.parse_args()

    # read all source csv files
    src_dataset1 = read_source_file(spark, args.dataset_one_path)
    src_dataset2 = read_source_file(spark, args.dataset_two_path)
    src_dataset3 = read_source_file(spark, args.dataset_three_path)

    # call transformation
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

    # Write to target path as csv
    CSVWriter(
        df=df,
        full_path=args.target_path,
    ).write()


if __name__ == "__main__":
    main()
