from __future__ import annotations

from framework.base.base import BaseReader
from pydantic import Field, validator
from typing import Optional, Dict
from pyspark import SparkConf, SparkFiles
from pyspark.sql import SparkSession


class CSVReader(BaseReader):
    format: str = "csv"
    header_option: Optional[bool] = Field(
        True, description="Whether to select header as first row"
    )
    additional_params: Optional[Dict[str, str]] = Field(
        {}, description="additional parameters to be passed to the reader"
    )

    @validator("full_path", allow_reuse=True)
    def validate_path(cls, full_path):
        assert full_path is not None, "full_path parameter cannot be null or blank"
        assert isinstance(full_path, str), "full_path cannot be a numeric"
        assert full_path[-4:] == ".csv", "Not a .csv file"
        return full_path

    def read(self, spark):
            # spark: SparkSession = (
            #     SparkSession.builder.appName("technical_assessment")
            #     .enableHiveSupport()
            #     .getOrCreate()
            # )

        df = (
            spark.read.format(self.format)
            .option("header", self.header_option)
            .options(**self.additional_params)
            .load(self.full_path)
        )

        return df
