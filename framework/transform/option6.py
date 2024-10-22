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
        df1 = self.df.withColumnRenamed("id", "caller_id")

        agg_df = df2.groupBy(*["country", "caller_id"]).agg(
            f.sum("quantity").alias("quantity")
        )
        join_df = df1.join(agg_df, on=["caller_id"], how="inner")

        window_spec = Window.partitionBy(*["country"]).orderBy(f.col("quantity").desc())
        target_df = (
            join_df.withColumn("rank", f.rank().over(window_spec))
            .filter(f.col("rank") == 1)
            .select("country", f.col("caller_id").alias("id"), "name", "quantity")
        )

        return target_df
