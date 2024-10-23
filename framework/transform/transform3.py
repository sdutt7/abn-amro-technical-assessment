from __future__ import annotations

from framework.base.base import BaseTransform
from pydantic import Field, validator
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


class Transform3(BaseTransform):
    """This class contains transformation method(s) required for
    problem statement as described in Output #3 - Department Breakdown"""

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
        as described in Output #3 - Department Breakdown

        :param df2: pyspark dataframe created from <<dataset_two.csv>>
        :return: output data as pyspark dataframe
        """
        # Join dataset_one and dataset_two to combine information from both datasets
        join_df = self.df.join(df2, on=["id"], how="inner")

        # aggregate sales and call statistics at area level
        agg_df = join_df.groupBy(*["area"]).agg(
            f.sum("sales_amount").alias("sales_amount"),
            f.sum("calls_made").alias("calls_made"),
            f.sum("calls_successful").alias("calls_successful"),
        )

        # Evaluate call success percentage
        target_df = agg_df.withColumn(
            "total_percentage", f.col("calls_successful") / f.col("calls_made")
        ).select("area", "sales_amount", "total_percentage")

        return target_df
