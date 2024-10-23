from __future__ import annotations

from pydantic import BaseModel, Field
from abc import ABC, abstractmethod
from typing import Optional, Any


class BaseReader(BaseModel):
    """
    Abstract Base Class for reading data.

    This class defines the structure for data readers, ensuring that derived classes implement the `read` method.
    It also provides attributes to hold essential information about the file being read, such as its path and format.
    """
    full_path: str = Field(..., description="full path to the file with filename")
    format: str = Field(..., description="format of the file to be read")

    @abstractmethod
    def read(self, **kwargs):
        """
        Abstract method for reading data.

        This method must be implemented by subclasses. It will contain the logic to read data from the specified
        file path and format into a usable format such as a DataFrame.

        Args:
            **kwargs: Additional parameters for reading the file.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
        raise NotImplementedError


class BaseWriter(BaseModel):
    """
    Abstract Base Class for writing data.

    This class defines the structure for data writers, ensuring that derived classes implement the `write` method.
    It holds attributes like the DataFrame to be written, the destination file path, format, mode (overwrite/append),
    and delimiter to be used when writing the file.
    """
    df: Any = Field(..., description="Dataframe to be written")
    full_path: str = Field(..., description="full path to the file with filename")
    format: str = Field(..., description="format of the file to be write")
    mode: Optional[str] = Field(
        "overwrite", description="whether to overwrite or append"
    )
    delimiter: Optional[str] = Field(",", description="delimited to be used")

    @abstractmethod
    def write(self, **kwargs):
        """
        Abstract method for writing data.

        This method must be implemented by subclasses. It will handle the logic for writing the DataFrame to the
        specified file path and format, using the provided mode and delimiter.

        Args:
            **kwargs: Additional parameters for writing the file.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
        raise NotImplementedError


class BaseTransform(BaseModel):
    """
    Abstract Base Class for transforming data.

    This class provides a template for data transformations, ensuring that derived classes implement the
    `transform` method. It holds the source DataFrame to be transformed.
    """
    df: Any = Field(..., description="source dataframe")

    @abstractmethod
    def transform(self, **kwargs):
        """
        Abstract method for transforming data.
        This method must be implemented by subclasses. It will handle the logic for transforming the DataFrame.

        Args:
            **kwargs: Additional parameters for transforming the data.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
        raise NotImplementedError
