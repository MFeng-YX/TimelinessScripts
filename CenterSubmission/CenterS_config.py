from dataclasses import dataclass, field

@dataclass(frozen=True)
class DataConfig():
    col_need: list[str] = field(default_factory=lambda: [
        "揽收网点代码",
        "揽收网点名称",
        "实际交件时间",
        "0:及时,1延误"
    ])
    
    project_name: str = field(default="CenterSubmission")