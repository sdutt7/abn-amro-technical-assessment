from __future__ import annotations

from framework.base.base import BaseTransform
from pydantic import Field, validator
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import Optional, Any
from pyspark.sql.window import Window


class TransformOption6(BaseTransform):
    @validator("df", allow_reuse=True, always=True, check_fields=False)
    def dataframe_validator(cls, df):
        assert isinstance(
            df, DataFrame
        ), f"Expecting pyspark.sql.DataFrame type, received {type(df)}"
        return df

    def transform(self, df2: DataFrame):

        join_df = self.df.join(df2, on=["id"], how="inner")

        window_spec = Window.partitionBy(*["country"]).orderBy(f.col("sales_amount").desc())
        target_df = (
            join_df.withColumn("rank", f.rank().over(window_spec))
            .filter(f.col("rank") == 1)
            .select("country", "id", "name", "sales_amount")
        )

        return target_df
