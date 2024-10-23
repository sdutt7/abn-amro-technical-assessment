from __future__ import annotations

from framework.base.base import BaseReader
from pydantic import Field, validator
from typing import Optional, Dict
from pyspark.sql import DataFrame


class CSVReader(BaseReader):
    """CSVReader class for reading CSV files using PySpark.

    This class extends the `BaseReader` class and is designed to load CSV files into a PySpark DataFrame.
    The class provides configuration options such as whether the CSV file includes a header and additional
    parameters that can be passed to the reader.

    """

    format: str = "csv"
    header_option: Optional[bool] = Field(
        True, description="Whether to select header as first row"
    )
    additional_params: Optional[Dict[str, str]] = Field(
        {}, description="additional parameters to be passed to the reader"
    )

    @validator("full_path", allow_reuse=True)
    def validate_path(cls, full_path) -> str:
        """
        Validates the `full_path` parameter to ensure it's a valid CSV file path.

        This method checks if `full_path` is a non-empty string and that it ends with the `.csv` extension. It raises an
        assertion error if the provided path is `None`, not a string, or doesn't have the correct file extension.

        :param full_path: Path to the csv file to be validated
        :return: The validated full file path.
        """
        assert full_path is not None, "full_path parameter cannot be null or blank"
        assert isinstance(full_path, str), "full_path cannot be a numeric"
        assert full_path[-4:] == ".csv", "Not a .csv file"
        return full_path

    def read(self, spark) -> DataFrame:
        """
        Reads the CSV file using the PySpark DataFrame reader.

        This method reads the CSV file located at `full_path` using the PySpark `spark.read` function. It applies the
        header and additional parameters specified in the class attributes. The resulting DataFrame is returned.

        :param spark: The SparkSession object used to read the CSV file.
        :return: A PySpark DataFrame containing the data read from the CSV file.
        """
        df = (
            spark.read.format(self.format)
            .option("header", self.header_option)
            .options(**self.additional_params)
            .load(self.full_path)
        )

        return df
