import os
import sys
import datetime as dt
import numpy as np
import pandas as pd
import sqlite3 as sql3
from skyrim.winterhold import timer, check_and_mkdir
from skyrim.whiterun import CCalendar
from skyrim.configurationOffice import SKYRIM_CONST_CALENDAR_PATH

"""
Project: Project_2022_08_HDF_Test
Author : HUXO
Created: 10:07, 周五, 2022/8/26
"""

pd.set_option("display.width", 0)
pd.set_option("display.float_format", "{:.2f}".format)

data_root_dir = os.path.join("/Data")
project_name = os.getcwd().split("\\")[-1]
project_data_dir = os.path.join(data_root_dir, project_name)
if not os.path.exists(project_data_dir):
    os.mkdir(project_data_dir)
