from __future__ import annotations

from framework.base.base import BaseTransform
from pydantic import Field, validator
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import Optional, Any
from pyspark.sql.window import Window


class Transform8(BaseTransform):
    """This class contains transformation method(s) required for problem statement as described
    in Output #8 - Top 3 companies those have placed the most order quantity per department
    """

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

    def transform(self, df2: DataFrame, df3: DataFrame) -> DataFrame:
        """
        This method performs the actual transformations as described
        in Output #8 - Top 3 companies those have placed the most order quantity per department

        :param df2: pyspark dataframe created from <<dataset_two.csv>>
        :param df3: pyspark dataframe created from <<dataset_three.csv>>
        :return: Pyspark Dataframe
        """
        # Rename columns to avoid any column conflict during join
        df1 = self.df.withColumnRenamed("id", "caller_id")
        df2 = df2.withColumnRenamed("id", "caller_id")

        # Join all datasets to combine information
        join_df = df1.join(df2, on=["caller_id"], how="inner").join(
            df3, df1.caller_id == df3.caller_id, how="inner"
        )

        # aggregate quantities to find total quantity sold to per company and area
        agg_df = join_df.groupBy(*["area", "company"]).agg(
            f.sum("quantity").alias("quantity")
        )

        # Evaluate the name of the company who placed most orders
        window_spec = Window.partitionBy(*["area"]).orderBy(f.col("quantity").desc())
        target_df = (
            agg_df.withColumn("rank", f.rank().over(window_spec))
            .filter(f.col("rank") <= 3)
            .select("area", "company", "quantity")
            .orderBy(f.col("area"), f.col("quantity").desc())
        )

        return target_df
