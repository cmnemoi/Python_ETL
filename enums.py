"""
This file contains useful enums for the ETL class.
"""

from enum import Enum

class ColumnTypesEnum(Enum):
    """
    Enum for the column types.
    """
    id = str
    scientific_title = str
    title = str
    date = str
    journal = str
    atccode = str
    drug = str
