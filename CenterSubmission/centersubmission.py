import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
import logging

from CenterS_config import DataConfig
from Package.CsvConversion import ExcelToCsv
from Package.FilePathReading import PathReading


class CenterSubmission():
    """计算中心每一日各分公司各时段交件量
    """
    
    config = DataConfig()
    def __init__(self):
        """初始化 CenterSubmission 类实例
        """
        
        self.project_name: str = self.config.project_name
        self.logger: logging.Logger = logging.getLogger(f"{self.project_name}.{__name__}")
    

    def path_read(self, conversion: int = 1) -> list[Path]:
        """读取数据文件

        Args:
            conversion (int, optional): 是否需要转换数据格式(0: 不需要, 1: 需要). Defaults to 1.

        Returns:
            list[Path]: 文件路径
        """
        
        if conversion not in [0, 1]:
            self.logger.error(f"conversion参数没有 {conversion} 值, conversion参数有: 0, 1")
            raise ValueError()
        
        if conversion == 1:
            exceltocsv: ExcelToCsv = ExcelToCsv(project_name=self.project_name)
            csv_path: list[Path] = exceltocsv.operation()
            
        if conversion == 0:
            pathreading: PathReading = PathReading(project_name=self.project_name)
            csv_path: list[Path] = pathreading.operation()
        
        return csv_path
    
    
    def single_calculate(self, path: Path) -> pd.DataFrame:
        """计算单日各分公司各时段交件量

        Args:
            path (Path): 单日数据文件路径

        Returns:
            pd.DataFrame: 单日计算表格
        """
        
        self.logger.info(f"-正在计算 '{path.name}' ")
        
        col_need: list[str] = self.config.col_need
        df: pd.DataFrame = pd.read_csv(path)
        df = df.loc[:, col_need]
        
        # --数据预处理--
        df_process = df.copy()
        # 去除空值
        df_process = df_process.dropna(subset=['实际交件时间'])
        # 转换时间格式
        df_process['实际交件时间'] = pd.to_datetime(df_process['实际交件时间'], format="mixed")
        df_process['实际交件日期'] = df_process['实际交件时间'].dt.date
        df_process['实际交件时段'] = df_process['实际交件时间'].dt.hour
        
        # --计算各时段交件量--
        col_group: list[str] = ["揽收网点代码", "揽收网点名称", "实际交件日期", "实际交件时段"]
        df_group = df_process.groupby(col_group).size().rename("交件量").reset_index()
        df_group = df_group.sort_values(by=col_group, ascending=True)
        df_pivot = df_group.pivot(
            index=["揽收网点代码", "揽收网点名称", "实际交件日期"],
            columns="实际交件时段",
            values="交件量"
        )
        
        # --计算交件总量--
        df_pivot['单日总量'] = df_pivot.sum(axis=1, numeric_only=True)
        
        # --计算延误量--
        df_delay = df_process[df_process["0:及时,1延误"] == 1]
        df_delay = df_delay.groupby(["揽收网点代码", "揽收网点名称", "实际交件日期"]).size().rename("单日延误量").reset_index()
        
        # --两表进行连接--
        df_pivot = pd.merge(
            df_pivot, df_delay,
            how="left",
            on=["揽收网点代码", "揽收网点名称", "实际交件日期"]
        )
        
        self.logger.info(f"-'{path.name}' 计算完成")
        
        return df_pivot
    
    
    def rooling_calculate(self, csv_list: list[Path]) -> pd.DataFrame:
        """滚动计算每一日各分公司各时段交件量

        Args:
            csv_list (list[Path]): 文件数据路径

        Returns:
            pd.DataFrame: 中心多日计算表格
        """
        
        self.logger.info("-单日数据汇总-开始-")
        
        # 滚动计算单日表
        df_list: list[pd.DataFrame] = list()
        for p in csv_list:
            df_single = self.single_calculate(p)
            df_list.append(df_single)
        
        # 将单日表汇总成多日表
        df_multi = pd.concat(df_list, axis=0, join="outer", ignore_index=True, sort=True)
        df_multi = (
            df_multi
            .groupby(["揽收网点代码", "揽收网点名称", "实际交件日期"])
            .sum()
            .reset_index()
            .sort_values(by=["揽收网点代码", "揽收网点名称", "实际交件日期"]))
        
        # --计算延误量占比
        df_multi['延误量占比'] = df_multi['单日延误量'] / df_multi['单日总量']
        df_multi["延误量占比"] = df_multi["延误量占比"].apply(lambda x: f"{x: .2%}")
        
        self.logger.info("-单日数据汇总-结束-")
        
        return df_multi
    
    
    def operation(self, conversion: int = 1) -> pd.DataFrame:
        """该类的主运行方法
        
        Args:
            conversion (int, optional): 是否需要转换数据格式(0: 不需要, 1: 需要). Defaults to 1. 

        Returns:
            pd.DataFrame: 中心多日计算表格
        """
        
        self.logger.info("\n--中心交件量表格-制作流程-开始")
        
        csv_list: list[Path] = self.path_read(conversion)
        
        df_multi: pd.DataFrame = self.rooling_calculate(csv_list)
        
        self.logger.info("\n--中心交件量表格-制作流程-结束")
        
        return df_multi