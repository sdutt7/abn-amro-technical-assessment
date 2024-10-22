from __future__ import annotations

from framework.base.base import BaseWriter
from pydantic import Field, validator
from typing import Optional, Dict
from pyspark.sql import DataFrame


class CSVWriter(BaseWriter):
    format: str = "csv"
    limit_rows: Optional[int] = Field(
        100, description="Write only limited number of rows"
    )
    additional_params: Optional[Dict[str, str]] = Field(
        {}, description="additional parameters to be passed to the reader"
    )

    @validator("df", allow_reuse=True, always=True, check_fields=False)
    def dataframe_validator(cls, df):
        assert isinstance(
            df, DataFrame
        ), f"Expecting pyspark.sql.DataFrame type, received {type(df)}"
        return df

    def write(self):
        df = self.df.limit(100)
        df.coalesce(1).write.option("delimiter", self.delimiter).options(
            **self.additional_params
        ).mode(self.mode).csv(self.full_path)
