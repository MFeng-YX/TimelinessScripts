import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
import logging

from CenterSubmission.CenterS_config import DataConfig
from Package.LogConfig import LogConfig
from CenterSubmission.centersubmission import CenterSubmission

if __name__ == "__main__":
    dataconfig: DataConfig = DataConfig()
    logconfig: LogConfig = LogConfig(dataconfig.project_name)
    logconfig.setup_logger()
    
    logger: logging.Logger = logging.getLogger(dataconfig.project_name)
    
    logger.info("--程序启动--")
    
    production: CenterSubmission = CenterSubmission()
    df_multi: pd.DataFrame = production.operation(conversion=0)
    df_multi.to_excel(r"c:\Users\admin\Desktop\西宁中心各时段交件量-0101-0107.xlsx", index=False)
    
    logger.info("--程序结束--")