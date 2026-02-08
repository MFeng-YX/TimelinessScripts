import logging
import time
import pandas as pd

from alive_progress import alive_bar
from pathlib import Path
from tqdm import tqdm


class DataConversion:
    """数据转换类"""

    def __init__(self, project_name: str = "DataConversion", method: str = "dir"):
        """初始化 DataConversion 类实例

        Args:
            project_name (str, optional): 项目名称. Defaults to "DataConversion".
            method (str, optional): 该类的转换模式, 可选的值有: "dir", "file". Defaults to "dir".
        """

        self.logger: logging.Logger = logging.getLogger(f"{project_name}.{__name__}")
        self.method: str | None = self.__verify_params(method)

    def __verify_params(self, method: str) -> str | None:
        """检验类的初始化参数是否正确"""

        method_list: list[str] = ["dir", "file"]
        if method not in method_list:
            self.logger.error(
                f"DataConversion类的method参数没有{method}值, method参数值有: 'dir', 'file'."
            )
            raise ValueError()
        else:
            return method

    def path_exists(self, file_path: str) -> Path:
        """判断输入的文件路径是否符合规范或存在

        Args:
            path (str): 文件/文件夹路径

        Returns:
            Path: 路径的Path对象
        """

        try:
            path: Path = Path(file_path)
        except TypeError as T:
            self.logger.error(T, f"输入的路径: {file_path} 不是文字路径, 请重新输入")
            raise T
        except ValueError as V:
            self.logger.error(V, f"输入的路径: {file_path} 不是规范的路径, 请重新输入")

        if self.method == "dir":
            if path.is_file():
                self.logger.error(f"输入的路径: {path} 不是文件夹路径, 请重新输入")
                raise ValueError()

        elif self.method == "file":
            if path.is_dir():
                self.logger.error(f"输入的路径: {path} 不是文件路径, 请重新输入")
                raise ValueError()

        return path

    def read_data(self, path: Path, dtype: list[str] = ["xlsx"]) -> pd.DataFrame:
        """_summary_

        Args:
            path (Path): 读取的文件路径
            dtype (str | list[str], optional): 变量类型. Defaults to "xlsx".

        Returns:
            pd.DataFrame: 读取的数据
        """
