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
            raise V

        if self.method == "dir":
            if path.is_file():
                self.logger.error(f"输入的路径: {path} 不是文件夹路径, 请重新输入")
                raise ValueError()

        elif self.method == "file":
            if path.is_dir():
                self.logger.error(f"输入的路径: {path} 不是文件路径, 请重新输入")
                raise ValueError()

        return path

    def __data_read(self, path: Path, dtype: str) -> pd.DataFrame:
        """读取数据文件

        Args:
            path (Path): 文件路径
            dtype (str): 文件类型

        Returns:
            pd.DataFrame: 读取的数据表
        """
        if dtype not in ["xlsx", "csv", "parquet"]:
            self.logger.error(
                "输入的文件类型不符合要求, 请输入: xlsx, csv, parquet 中的一种"
            )

        self.logger.info(f"\n--读取 '{path.name}' --")
        with alive_bar(title=f"读取 '{path.name}' ", spinner="waves") as bar:
            start_time: float = time.time()

            # 读取 excel 文件
            if dtype == "xlsx":
                df: pd.DataFrame = pd.read_excel(path)
            # 读取 csv 文件
            elif dtype == "csv":
                df: pd.DataFrame = pd.read_csv(path)
            # 读取 parquet 文件
            elif dtype == "parquet":
                df: pd.DataFrame = pd.read_parquet(path)

            end_time: float = time.time()
        # 读取过程耗时
        elapsed: float = end_time - start_time
        self.logger.info(
            f"读取完成! 耗时: {elapsed:.2f}秒 |"
            f"行数: {df.shape[0]:: ,} , 列数: {df.shape[1]: ,}"
        )

        return df

    def __conversion(self, df: pd.DataFrame, cvsdtype: str, path: Path) -> Path:
        """转换数据

        Args:
            df (pd.DataFrame): 读取到的数据
            cvsdtype (str): 需要转换成为的文件类型
            path (Path): 读取的文件路径

        Returns:
            Path: 转换后的文件路径
        """

        conver_path: Path = path.with_suffix(cvsdtype)

    def read_data(self, path: Path, dtype: str = "xlsx") -> dict[str, pd.DataFrame]:
        """_summary_

        Args:
            path (Path): 读取的文件路径
            dtype (str , optional): 变量类型, 可选值: xlsx, csv, parquet. Defaults to "xlsx".

        Returns:
            dict[str, pd.DataFrame]: 读取的数据
        """

        if dtype not in ["xlsx", "csv", "parquet"]:
            self.logger.error(
                "输入的文件类型不符合要求, 请输入: xlsx, csv, parquet 中的一种"
            )
        # 返回的数据字典
        df_dict: dict[str, pd.DataFrame] = dict()

        # dir-文件夹模式
        if self.method == "dir":
            # 遍历指定类型的文件
            path_list = [p for p in path.rglob(f"*.{dtype}")]
            self.logger.info(f"一共读取到: {len(path_list)} 个文件.")

            # 读取文件数据
            for p in path_list:
                df: pd.DataFrame = self.__data_read(p, dtype)
                df_dict[p.name] = df

        # file-文件模式
        elif self.method == "file":
            df: pd.DataFrame = self.__data_read(path, dtype)
            df_dict[path.name] = df

        return df_dict
