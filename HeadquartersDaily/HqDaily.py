import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
import logging

import Package as pkg
from HeadquartersDaily.HqDaily_config import DataConfig


class HeadquartersDaily:
    """总部日报报表"""

    config: DataConfig = DataConfig()

    def __init__(self):
        """初始化 HeadquartersDaily 类实例"""
        self.project_name: str = self.config.project_name
        self.logger: logging.Logger = logging.getLogger(
            f"{self.project_name}.{__name__}"
        )

    def read_path(self) -> list[Path]:
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

        if category == "延误占比":
            col_need = self.config.cal_col_1[1:]
        elif category == "延误量":
            col_need = self.config.cal_col_2[1:]

        city_cal = df.copy()
        city_cal_melt = city_cal.melt(
            id_vars=["日期", "城市线路名称"],
            value_vars=col_need,
            var_name="核心影响环节",
            value_name=category,
        )

        city_cal_melt["rank"] = city_cal_melt.groupby("城市线路名称")[category].rank(
            method="min", ascending=False
        )
        for s in ["延误占比", "网点", "延误量"]:
            city_cal_melt["核心影响环节"] = city_cal_melt["核心影响环节"].str.replace(
                s, "", regex=False
            )
        mask = (city_cal_melt["rank"] <= 3) | (city_cal_melt["核心影响环节"] == "派签")

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
                gpt2 = pd.read_excel(p, sheet_name="GPT", skiprows=[1, 2])
            if "城市线路汇总" in p.name:
                cityroute = pd.read_excel(p)

        cityroute["日期"] = pd.to_datetime(cityroute["日期"], format="mixed").dt.date
        row_set = set(cityroute["城市线路名称"]) - set(gpt2["城市线路"])

        # 筛选占比
        city_cal_1 = cityroute.copy().loc[:, self.config.cal_col_1]
        city_cal_1 = self.city_cal(city_cal_1, "延误占比")

        # 筛选延误量
        city_cal_2 = cityroute.copy().loc[:, self.config.cal_col_2]
        city_cal_2 = self.city_cal(city_cal_2, "延误量")

        # 合并延误量和延误占比
        city_day = pd.merge(
            city_cal_1,
            city_cal_2,
            how="inner",
            on=["日期", "城市线路名称", "核心影响环节", "rank"],
        )

        mask_route = (city_day["核心影响环节"] == "路由") & (city_day["延误量"] <= 100)
        mask_trans = (city_day["核心影响环节"] == "运输") & (city_day["延误量"] <= 10)
        mask_day = ~(mask_route | mask_trans)

        city_day = city_day.copy().loc[mask_day]
        city_day = city_day.merge(
            cityroute.loc[:, ["城市线路名称", "与第一差值(%)", "未达成量"]],
            how="left",
            on=["城市线路名称"],
        )

        # 重命名列名
        city_day = city_day.rename(
            columns={
                "日期": "GPT展示日期",
                "城市线路名称": "线路名称",
                "与第一差值(%)": "与第一差值",
            }
        )

        # 优化当日匹配最终数据, 剔除除派签外延误占比小于0.05的核心影响环节
        city_day["延误占比"] = city_day["延误占比"] / 100
        city_day["与第一差值"] = city_day["与第一差值"] / 100
        mask_data = (city_day["延误占比"] < 0.05) & (city_day["核心影响环节"] != "派签")
        city_day = city_day.loc[~mask_data]

        # 开始和原表进行匹配
        city_match = city_day.copy()
        city_match = city_match.rename(
            columns={
                "与第一差值": "与第一差值-复盘",
                "未达成量": "未达成量-复盘",
                "延误量": "延误量-复盘",
                "延误占比": "延误占比-复盘",
            }
        )

        match_col = [
            col for col in city_match.columns if col not in ["GPT展示日期", "rank"]
        ]
        city_match = city_match.loc[:, match_col]

        gpt_match = gpt1.loc[:, self.config.gpt_col]

        # 连接两表
        report_match = pd.merge(
            gpt_match, city_match, how="left", on=["线路名称", "核心影响环节"]
        )

        report_match["环比"] = (
            report_match["与第一差值-复盘"] - report_match["与第一差值"]
        )
        report_match["路线消除情况"] = "自动消除"

        # 区分消除情况
        mask_expand = report_match["环比"] > 0
        report_match.loc[mask_expand, "路线消除情况"] = "差距扩大"

        mask_reduce = report_match["环比"] < 0
        report_match.loc[mask_reduce, "路线消除情况"] = "差距缩小"

        mask_eliminate = (report_match["环比"].isna()) & (
            ~report_match["主要点位"].isna()
        )
        report_match.loc[mask_eliminate, "路线消除情况"] = "消除"

        # 优化最终表格
        report = report_match.copy().loc[:, self.config.report_col]
        col_cityday = [col for col in city_day.columns if col not in ["rank"]]
        mask_cityday = city_day["线路名称"].isin(row_set)
        report = pd.concat(
            [report, city_day.loc[mask_cityday, col_cityday]], ignore_index=True, axis=0
        )

        return report

    def operation(self) -> pd.DataFrame:
        """该类方法的主运行方法

        Returns:
            pd.DataFrame: 做好的数据表
        """

        self.logger.info("--报表制作流程开始--")

        path_list: list[Path] = self.read_path()
        report = self.report_production(path_list)

        self.logger.info("--报表制作流程结束--")

        return report
