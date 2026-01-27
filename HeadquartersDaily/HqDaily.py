import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd 
import logging

import Package as pkg
from HeadquartersDaily.HqDaily_config import DataConfig

class HeadquartersDaily():
    """总部日报报表
    """
    
    config: DataConfig = DataConfig()
    
    def __init__(self):
        """初始化 HeadquartersDaily 类实例
        """
        self.project_name: str = self.config.project_name
        self.logger: logging.Logger = logging.getLogger(f"{self.project_name}.{__name__}")
    
    
    def read_path(self) -> Path:
        """读取需要的文件路径

        Returns:
            Path: 文件路径
        """
        self.logger.info("开始读取文件路径")
        
        read = pkg.PathReading(project_name=self.project_name, method="excel")
        path_list = read.operation()
        
        self.logger.info(f"一共读取到 {len(path_list)} 个文件")
        
        return path_list
    
    
    def city_cal(self, df: pd.DataFrame, category: str) -> pd.DataFrame:
        """当日TOP线路拆分延误占比TOP3影响环节

        Args:
            df (pd.DataFrame): 当日TOP线路数据表
            category (str): 计算类别

        Returns:
            pd.DataFrame: 拆分后表格
        """
        
        city_cal = df.copy()
        city_cal_melt = city_cal.melt(
            id_vars=["日期","城市线路名称"],
            value_vars=self.config.cal_col_1[1:],
            var_name="核心影响环节",
            value_name=category
        )
        
        city_cal_melt["rank"] = city_cal_melt.groupby("城市线路名称").rank(method="min")
        for s in ["延误占比", "网点"]:
            city_cal_melt["核心影响环节"] = city_cal_melt["核心影响环节"].str.replace(s, "", regex=False)
        mask = (city_cal_melt["rank"] <= 3) | (city_cal_melt["核心影响环节"]=="派签")
        
        return city_cal_melt.loc[mask]   
    
    
    def report_production(self, path_list: list[Path]) -> pd.DataFrame:
        """报表制作逻辑

        Args:
            path_list (list[Path]): 文件路径
        
        Returns:
            pd.DataFrame: 制作好的报表
        """
        
        for p in path_list:
            if "GPT" in p.name:
                gpt1 = pd.read_excel(p, sheet_name="改善方案", skiprows=1)
            if "城市线路汇总" in p.name:
                cityroute = pd.read_excel(p)
        
        cityroute['日期'] = pd.to_datetime(cityroute["日期"], format="mixed").dt.date
        
        # 筛选占比
        city_cal_1 = cityroute.copy().loc[:, self.config.cal_col_1]
        city_cal_1 = self.city_cal(city_cal_1, "延误占比")
        
        # 筛选延误量
        city_cal_2 = cityroute.copy().loc[:, self.config.cal_col_2]
        city_cal_2 = self.city_cal(city_cal_2, "延误量")
        
        # 合并延误量和延误占比
        city_day = pd.merge(
            city_cal_1, city_cal_2,
            how="inner",
            on=["城市线路名称", "核心影响环节"]
        )