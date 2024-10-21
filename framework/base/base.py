from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Optional, Dict
from abc import ABC, abstractmethod


class BaseReader(BaseModel):
    full_path: str = Field(..., description="full path to the file with filename")
    format: str = Field(..., description="format of the file to be read")


    # @abstractmethod
    # def reader(self):
    #     raise NotImplementedError
