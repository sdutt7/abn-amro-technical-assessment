from __future__ import annotations

from framework.base.base import BaseTransform
from pydantic import Field, validator
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from pyspark.sql.window import Window


class Transform4(BaseTransform):
    """This class contains transformation method(s) required for
    problem statement as described in Output #4 - Top 3 best performers per department
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

    def transform(self, df2: DataFrame) -> DataFrame:
        """
        This method performs the actual transformations
        as described in Output #4 - Top 3 best performers per department

        :param df2: pyspark dataframe created from <<dataset_two.csv>>
        :return: output data as pyspark dataframe
        """
        # Join dataset_one and dataset_two to combine information from both datasets
        join_df = self.df.join(df2, on=["id"], how="inner")

        # Create window/group to rank best performers of the department
        # based on successful call percentage
        window_spec = Window.partitionBy(*["area"]).orderBy(
            (f.col("calls_successful") / f.col("calls_made")).desc()
        )

        # Apply window rank and extract top 3 performer
        # only if total percentage is higher than 75%
        target_df = (
            join_df.withColumn(
                "total_percentage", f.col("calls_successful") / f.col("calls_made")
            )
            .withColumn("rank", f.rank().over(window_spec))
            .filter((f.col("rank") <= 3) & (f.col("total_percentage") > 0.75))
            .select("area", "name", "total_percentage", "sales_amount")
            .orderBy(f.col("area").desc(), f.col("total_percentage").desc())
        )

        return target_df
