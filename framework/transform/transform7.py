from __future__ import annotations

from framework.base.base import BaseTransform
from pydantic import Field, validator
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import Optional, Any
from pyspark.sql.window import Window


class TransformOption7(BaseTransform):
    @validator("df", allow_reuse=True, always=True, check_fields=False)
    def dataframe_validator(cls, df):
        assert isinstance(
            df, DataFrame
        ), f"Expecting pyspark.sql.DataFrame type, received {type(df)}"
        return df

    def transform(self, df2: DataFrame, df3: DataFrame):
        df1 = self.df.withColumnRenamed("id", "caller_id")
        df2 = df2.withColumnRenamed("id", "caller_id")
        df3 = df3.withColumn(
            "age_group",
            f.when(f.col("age") < 20, f.lit("0 - 20 years"))
            .when((f.col("age") >= 20) & (f.col("age") < 40), f.lit("20 - 40 years"))
            .when((f.col("age") >= 40) & (f.col("age") < 60), f.lit("40 - 60 years"))
            .otherwise(f.lit("60 years & above")),
        )

        join_df = df1.join(df2, on=["caller_id"], how="inner").join(
            df3, df1.caller_id == df3.caller_id, how="inner"
        )

        agg_df = join_df.groupBy(*["area", "age_group"]).agg(
            f.sum("quantity").alias("quantity")
        )

        window_spec = Window.partitionBy(*["area"]).orderBy(f.col("quantity").desc())

        target_df = (
            agg_df.withColumn("rank", f.rank().over(window_spec))
            .filter(f.col("rank") == 1)
            .select("area", "age_group", "quantity")
        )

        return target_df
