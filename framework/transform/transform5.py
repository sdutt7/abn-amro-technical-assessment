from __future__ import annotations

from framework.base.base import BaseTransform
from pydantic import Field, validator
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import Optional, Any
from pyspark.sql.window import Window


class TransformOption5(BaseTransform):
    @validator("df", allow_reuse=True, always=True, check_fields=False)
    def dataframe_validator(cls, df):
        assert isinstance(
            df, DataFrame
        ), f"Expecting pyspark.sql.DataFrame type, received {type(df)}"
        return df

    def transform(self, df2: DataFrame):
        df1 = self.df.withColumnRenamed("id", "caller_id")
        df2 = df2.filter(f.col("country") == "Netherlands")
        join_df = df1.join(df2, on=["caller_id"], how="inner")

        agg_df = join_df.groupBy(*["area", "product_sold"]).agg(
            f.sum("quantity").alias("quantity")
        )

        window_spec = Window.partitionBy(*["area"]).orderBy(f.col("quantity").desc())
        target_df = (
            agg_df.withColumn("rank", f.rank().over(window_spec))
            .filter(f.col("rank") == 1)
            .select("area", "product_sold", "quantity")
        )

        return target_df
