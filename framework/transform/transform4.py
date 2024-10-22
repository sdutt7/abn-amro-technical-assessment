from __future__ import annotations

from framework.base.base import BaseTransform
from pydantic import Field, validator
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import Optional, Any
from pyspark.sql.window import Window


class TransformOption4(BaseTransform):
    @validator("df", allow_reuse=True, always=True, check_fields=False)
    def dataframe_validator(cls, df):
        assert isinstance(
            df, DataFrame
        ), f"Expecting pyspark.sql.DataFrame type, received {type(df)}"
        return df

    def transform(self, df2: DataFrame):
        join_df = self.df.join(df2, on=["id"], how="inner")

        window_spec = Window.partitionBy(*["area"]).orderBy(
            (f.col("calls_successful") / f.col("calls_made")).desc()
        )
        target_df = (
            join_df.withColumn(
                "total_percentage", f.col("calls_successful") / f.col("calls_made")
            )
            .withColumn("rank", f.rank().over(window_spec))
            .filter(f.col("rank") <= 3)
            .select("area", "name", "total_percentage", "sales_amount")
            .orderBy(f.col("area").desc(), f.col("total_percentage").desc())
        )

        return target_df
