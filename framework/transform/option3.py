from __future__ import annotations

from framework.base.base import BaseTransform
from pydantic import Field, validator
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import Optional, Any


class TransformOption3(BaseTransform):
    @validator("df", allow_reuse=True, always=True, check_fields=False)
    def dataframe_validator(cls, df):
        assert isinstance(
            df, DataFrame
        ), f"Expecting pyspark.sql.DataFrame type, received {type(df)}"
        return df

    def transform(self, df2: DataFrame):
        join_df = self.df.join(df2, on=["id"], how="inner")

        agg_df = join_df.groupBy(*["area"]).agg(
            f.sum("sales_amount").alias("sales_amount"),
            f.sum("calls_made").alias("calls_made"),
            f.sum("calls_successful").alias("calls_successful"),
        )

        target_df = agg_df.withColumn(
            "total_percentage", f.col("calls_successful") / f.col("calls_made")
        ).select("area", "sales_amount", "total_percentage")

        return target_df
