from __future__ import annotations

from framework.base.base import BaseTransform
from pydantic import Field, validator
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import Optional, Any


class TransformOption2(BaseTransform):
    @validator("df", allow_reuse=True, always=True, check_fields=False)
    def dataframe_validator(cls, df):
        assert isinstance(
            df, DataFrame
        ), f"Expecting pyspark.sql.DataFrame type, received {type(df)}"
        return df

    def transform(self, df2: DataFrame):
        df1 = self.df.filter(f.col("area") == "Marketing")
        join_df = df1.join(df2, on=["id"], how="inner")

        df = join_df.withColumn(
            "zip_code", f.trim(f.split_part(f.col("address"), f.lit(","), f.lit(-2)))
        )

        return df
