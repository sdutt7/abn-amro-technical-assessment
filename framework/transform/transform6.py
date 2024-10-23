from __future__ import annotations

from framework.base.base import BaseTransform
from pydantic import Field, validator
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from pyspark.sql.window import Window


class Transform6(BaseTransform):
    """This class contains transformation method(s) required for problem statement as described
    in Output #6 - Who is the best overall salesperson per country"""

    @validator("df", allow_reuse=True, always=True, check_fields=False)
    def dataframe_validator(cls, df: DataFrame) -> DataFrame:
        """
        This method is a Pydantic validator that ensures the input `df` provided to the class is of type
        `pyspark.sql.DataFrame`. If the validation fails, it raises an AssertionError.

        :param df: The input DataFrame to be validated (dataset_two.csv)
        :return: Pyspark Dataframe
        """
        assert isinstance(
            df, DataFrame
        ), f"Expecting pyspark.sql.DataFrame type, received {type(df)}"
        return df

    def transform(self, df2: DataFrame) -> DataFrame:
        """
        This method performs the actual transformations as described
        in Output #6 - Who is the best overall salesperson per country

        :param df2: pyspark dataframe created from <<dataset_three.csv>>
        :return: Pyspark Dataframe
        """
        # Rename columns to avoid any column conflict during join
        df1 = self.df.withColumnRenamed("id", "caller_id")

        # aggregate quantities to find total quantity sold by the sales person
        agg_df = df2.groupBy(*["country", "caller_id"]).agg(
            f.sum("quantity").alias("quantity")
        )
        # Join dataset_two and dataset_three to combine information from both datasets
        join_df = df1.join(agg_df, on=["caller_id"], how="inner")

        # Create window/group to rank sales persons best on total sold quantities
        window_spec = Window.partitionBy(*["country"]).orderBy(f.col("quantity").desc())
        target_df = (
            join_df.withColumn("rank", f.rank().over(window_spec))
            .filter(f.col("rank") == 1)
            .select("country", f.col("caller_id").alias("id"), "name", "quantity")
        )

        return target_df
