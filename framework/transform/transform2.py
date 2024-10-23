from __future__ import annotations

from framework.base.base import BaseTransform
from pydantic import Field, validator
from pyspark.sql import DataFrame
import pyspark.sql.functions as f


class Transform2(BaseTransform):
    """This class contains transformation method(s) required for
    problem statement as described in Output #2 - Marketing Address Information"""

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
        as described in Output #2 - Marketing Address Information

        :param df2: pyspark dataframe created from <<dataset_two.csv>>
        :return: output data as pyspark dataframe
        """
        # filter out data only for Marketing department
        df1 = self.df.filter(f.col("area") == "Marketing")

        # Join dataset_one and dataset_two combine information from both datasets
        join_df = df1.join(df2, on=["id"], how="inner")

        # Extract zip_code from address field.
        # zip code can be extracted by splitting address field based on comma(,)
        # take 2nd split from end
        df = join_df.withColumn(
            "zip_code", f.trim(f.split_part(f.col("address"), f.lit(","), f.lit(-2)))
        ).select(*["address", "zip_code"])

        return df
