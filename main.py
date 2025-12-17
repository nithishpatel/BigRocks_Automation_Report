import os
import time
import pandas as pd
# import openpyxl
import numpy as np
from datetime import datetime as dt
import datetime
import psycopg2

import warnings
warnings.simplefilter("ignore")

from back_office import *
from cust_care import *
from onboarding import *
from cust_service import *
from diy import *
from calls_data import *
from productivity import *
from extras import *

os.chdir("C:/Users/nithish.patel/OneDrive - Verisure/Big Rocks")

def main(results_dir):
    back_office_calculation(week_start,week_end,month_start,month_end,results_dir)
    print ("Back Office completed!")
    cust_care_calculation(week_start,week_end,month_start,month_end,results_dir)
    print ("Customer Care completed!")
    diy_calculation(week_start,week_end,month_start,month_end,results_dir)
    print ("DIY completed!")
    cust_service_calculation(week_start,week_end,month_start,month_end,results_dir)
    print ("Customer Service completed!")
    onboarding_calculation(week_start,week_end,month_start,month_end,results_dir)
    print ("Onboarding completed!")
    calls_data_calculation(week_start,week_end,month_start,month_end,results_dir)
    print ("Calls Data completed!")
    rate_calculations(week_start,week_end,month_start, month_end,results_dir)
    print ("All KPIs updated!!!!")

if __name__ == '__main__':
    start_time = time.time()
    week_start = '2025-12-08'
    month_start = '2025-12-01'
    week_end = month_end = '2025-12-14'
    results_dir = "C:/Users/nithish.patel/OneDrive - Verisure/Big Rocks/SAP BO data downloads/results.csv"
    
    main(results_dir)
    end_time = time.time()

    print(f"Program duration: {(end_time - start_time)/60:.2f} minutes")