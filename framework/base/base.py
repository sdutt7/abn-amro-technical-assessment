from __future__ import annotations

from pydantic import BaseModel, Field
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Optional, Any


class BaseReader(BaseModel):
    full_path: str = Field(..., description="full path to the file with filename")
    format: str = Field(..., description="format of the file to be read")

    @abstractmethod
    def read(self, **kwargs):
        raise NotImplementedError


class BaseWriter(BaseModel):
    df: Any = Field(..., description="Dataframe to be written")
    full_path: str = Field(..., description="full path to the file with filename")
    format: str = Field(..., description="format of the file to be write")
    mode: Optional[str] = Field(
        "overwrite", description="whether to overwrite or append"
    )
    delimiter: Optional[str] = Field(",", description="delimited to be used")

    @abstractmethod
    def write(self, **kwargs):
        raise NotImplementedError


class BaseTransform(BaseModel):
    df: Any = Field(..., description="source dataframe")

    @abstractmethod
    def transform(self, **kwargs):
        raise NotImplementedError
