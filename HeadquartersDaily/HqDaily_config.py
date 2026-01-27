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