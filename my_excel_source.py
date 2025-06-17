import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Union

import numpy as np
import pandas as pd
from pydantic import AnyUrl

from bitfount.data.datasources.base_source import BaseSource
from bitfount.types import _Dtypes

logger = logging.getLogger(__name__)


class MyExcelSource(BaseSource):
    """Data source for loading excel files.

    Args:
        path: The path to the excel file.
        **read_excel_kwargs: Additional arguments to be passed to `pandas.read_excel`.
    """

    def __init__(
        self,
        path: Union[os.PathLike, AnyUrl, str],
        read_excel_kwargs: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        if not str(path).endswith((".xls", ".xlsx")):
            raise TypeError("Please provide a Path or URL to an Excel file.")
        self.path = str(path)
        if not read_excel_kwargs:
            read_excel_kwargs = {}
        self.read_excel_kwargs = read_excel_kwargs

    def get_data(self, **kwargs: Any) -> pd.DataFrame:
        """Loads and returns data from Excel dataset.

        Returns:
            A DataFrame-type object which contains the data.
        """
        df: pd.DataFrame = pd.read_excel(self.path, **self.read_excel_kwargs)
        return df

    def get_values(
        self, col_names: List[str], **kwargs: Any
    ) -> Dict[str, Iterable[Any]]:
        """Get distinct values from columns in Excel dataset.

        Args:
            col_names: The list of the columns whose distinct values should be
                returned.

        Returns:
            The distinct values of the requested column as a mapping from col name to
            a series of distinct values.

        """
        return {col: self.get_data()[col].unique() for col in col_names}

    def get_column(self, col_name: str, **kwargs: Any) -> Union[np.ndarray, pd.Series]:
        """Loads and returns single column from Excel dataset.

        Args:
            col_name: The name of the column which should be loaded.

        Returns:
            The column request as a series.
        """
        df: pd.DataFrame = self.get_data()
        return df[col_name]

    def get_dtypes(self, **kwargs: Any) -> _Dtypes:
        """Loads and returns the columns and column types of the Excel dataset.

        Returns:
            A mapping from column names to column types.
        """
        df: pd.DataFrame = self.get_data()
        return self._get_data_dtypes(df)

    def __len__(self) -> int:
        return len(self.get_data())

    @property
    def multi_table(self) -> bool:
        """Attribute to specify whether the datasource is multi table."""
        return False
