import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
import logging

import Package as pkg
from HeadquartersDaily.HqDaily_config import DataConfig
from HeadquartersDaily.HqDaily import HeadquartersDaily


if __name__ == "__main__":
    config: DataConfig = DataConfig()
    project_name: str = config.project_name
    
    log_config: pkg.LogConfig = pkg.LogConfig(project_name=project_name)
    logger: logging.Logger = log_config.setup_logger()
    
    logger.info("--程序开始--")
    
    # number: str = input("是否需要将数据文件转换为CSV文件\n(是: 1, 否: 0, 请输入数字)\t")
    # if number == "1":
    #     exceltocsv = pkg.ExcelToCsv()
    #     exceltocsv.operation()
    # elif number == "0":
    #     pass
    # else:
    #     sys.exit()
            
    hqdaily: HeadquartersDaily = HeadquartersDaily()
    report: pd.DataFrame = hqdaily.operation()
    
    path = r"c:\Users\admin\Desktop\改善方案.xlsx"
    report.to_excel(path, index=False)
    
    logger.info("--程序结束--")