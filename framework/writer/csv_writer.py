from __future__ import annotations

from framework.base.base import BaseWriter
from pydantic import Field, validator
from typing import Optional, Dict
from pyspark.sql import DataFrame


class CSVWriter(BaseWriter):
    """
    CSVWriter class for writing PySpark DataFrames to CSV files.

    This class extends the `BaseWriter` and provides the functionality to write a PySpark DataFrame to a CSV file,
    with options to specify additional parameters like delimiter and write mode. The class ensures that the DataFrame
    being written is valid and can be saved in CSV format.
    """

    format: str = "csv"
    additional_params: Optional[Dict[str, str]] = Field(
        {"header": True}, description="additional parameters to be passed to the reader"
    )

    @validator("df", allow_reuse=True, always=True, check_fields=False)
    def dataframe_validator(cls, df) -> DataFrame:
        """
        Ensures that the provided DataFrame is a valid PySpark DataFrame.

        This method verifies that the `df` attribute is an instance of PySpark's `DataFrame`. If the provided object
        is not a valid DataFrame, an assertion error is raised.

        :param df: The DataFrame to validate.
        :return: The validated PySpark DataFrame.
        """
        assert isinstance(
            df, DataFrame
        ), f"Expecting pyspark.sql.DataFrame type, received {type(df)}"
        return df

    def write(self):
        """
        Writes the DataFrame to a CSV file.

        This method coalesces the DataFrame into a single file and writes it to the location specified by `full_path`.
        It applies the delimiter and additional write options such as the CSV header or any other configurations set
        in the `additional_params`. The mode used for writing (e.g., overwrite, append) is also applied.

        The final CSV is saved at the `full_path` location.

        :return: N/A
        """
        self.df.coalesce(1).write.option("delimiter", self.delimiter).options(
            **self.additional_params
        ).mode(self.mode).csv(self.full_path)
