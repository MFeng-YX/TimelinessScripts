from dataclasses import dataclass, field

@dataclass(frozen=True)
class DataConfig():
    project_name: str = field(default="HeadquartersDaily")
    
    cal_col_1: list[str] = field(default_factory=lambda: [
        "日期",
        "城市线路名称",
        "路由延误占比",
        "网点交件延误占比",
        "出港延误占比",
        "运输延误占比",
        "进港延误占比",
        "派签延误占比"
    ])
    
    cal_col_2: list[str] = field(default_factory=lambda: [
        "日期",
        "城市线路名称",
        "路由延误量",
        "网点交件延误量",
        "出港延误量",
        "运输延误量",
        "进港延误量",
        "派签延误量"
    ])
    
    gpt_col: list[str] = field(default_factory=lambda: [
        "GPT展示日期",
        "线路名称",
        "与第一差值",
        "未达成量",
        "核心影响环节",
        "延误量",
        "延误占比",
        "主要点位",
        "改善举措",
        "责任部门",
        "责任人",
        "完成日期",
        "备注"
    ])
    
    report_col: list[str] = field(default_factory=lambda: [
        "GPT展示日期",
        "线路名称",
        "与第一差值",
        "未达成量",
        "核心影响环节",
        "延误量",
        "延误占比",
        "路线消除情况",
        "与第一差值-复盘",
        "环比",
        "未达成量-复盘",
        "延误量-复盘",
        "延误占比-复盘",
        "主要点位",
        "改善举措",
        "责任部门",
        "责任人",
        "完成日期",
        "备注"
    ])