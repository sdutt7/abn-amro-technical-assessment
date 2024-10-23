from __future__ import annotations

from framework.base.base import BaseTransform
from pydantic import Field, validator
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import Optional, Any
from pyspark.sql.window import Window


class Transform5(BaseTransform):
    """This class contains transformation method(s) required for problem statement as described
    in Output #5 - Top 3 most sold products per department in the Netherlands"""

    @validator("df", allow_reuse=True, always=True, check_fields=False)
    def dataframe_validator(cls, df: DataFrame) -> DataFrame:
        """
        This method is a Pydantic validator that ensures the input `df` provided to the class is of type
        `pyspark.sql.DataFrame`. If the validation fails, it raises an AssertionError.

        :param df: The input DataFrame to be validated (dataset_one.csv)
        :return: Pyspark Dataframe
        """
        assert isinstance(
            df, DataFrame
        ), f"Expecting pyspark.sql.DataFrame type, received {type(df)}"
        return df

    def transform(self, df2: DataFrame) -> DataFrame:
        """
        This method performs the actual transformations as described
        in Output #5 - Top 3 most sold products per department in the Netherlands

        :param df2: pyspark dataframe created from <<dataset_three.csv>>
        :return: Pyspark Dataframe
        """
        # Rename columns to avoid any column conflict during join
        df1 = self.df.withColumnRenamed("id", "caller_id")

        # Filter data only for netherlands and combine dataset_one and dataset_three
        df2 = df2.filter(f.col("country") == "Netherlands")
        join_df = df1.join(df2, on=["caller_id"], how="inner")

        # Evaluate sum of quantity sold of a product per department
        agg_df = join_df.groupBy(*["area", "product_sold"]).agg(
            f.sum("quantity").alias("quantity")
        )
        # Create window/group to rank most sold products per department
        window_spec = Window.partitionBy(*["area"]).orderBy(f.col("quantity").desc())
        target_df = (
            agg_df.withColumn("rank", f.rank().over(window_spec))
            .filter(f.col("rank") == 1)
            .select("area", "product_sold", "quantity")
        )

        return target_df
