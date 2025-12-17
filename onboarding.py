import pandas as pd
import numpy as np
import datetime
import psycopg2
from datetime import datetime, timedelta

dsn_database2 = 'operations'
dsn_hostname2 = "es1ukpbigw01v.vsdpro.local"
dsn_port2 = "5432"
dsn_uid2 = "UK_BI"
dsn_pwd2 = "S|.{U0O:wVEC"

try:
        
        print("Connecting to Database2...")
        connection = psycopg2.connect(
            dbname=dsn_database2,
            host=dsn_hostname2,
            port=dsn_port2,
            user=dsn_uid2,
            password=dsn_pwd2
        )
        print("Database Connected!\n")
except Exception as e:
        print(f"Unable to connect to Database.{e} ")

cursor = connection.cursor()



def onboarding_calculation(week_start,week_end,month_start,month_end, results_dir):

    def Nweekdays(start_date, end_date):
        holidays = ["2024-01-01","2024-03-29","2024-04-01","2024-05-06","2024-05-27","2024-08-26","2024-12-25","2024-12-26","2025-01-01","2025-04-18","2025-04-21","2025-05-05","2025-05-26","2025-08-25","2025-12-25","2025-12-26"]
        days = np.busday_count(start_date, end_date, holidays=holidays)
        return days

    results = pd.read_csv(results_dir)

    onboarding_main_installtions = pd.read_sql('select * from big_rocks.bigrocks_onboarding',connection)


    onboarding_main_installtions['network_days'] = np.nan
    onboarding_main_installtions['network_days'] = onboarding_main_installtions.apply(lambda x: Nweekdays(x['installation_date'].date(),x['min_comp/attempt_date'].date()), axis=1)
    onboarding_main_installtions['network_days'] = onboarding_main_installtions['network_days'] - np.sign(onboarding_main_installtions['network_days'])
    onboarding_main_installtions['V_Week_of_the_year'] = onboarding_main_installtions['min_comp/attempt_date'].dt.strftime('%U')

    def calculations(x,y,range):
    
        # month = int(datetime.datetime.strptime(x, '%Y-%m-%d').strftime('%m'))
        # week = int(datetime.datetime.strptime(x, '%Y-%m-%d').strftime('%U')) + 1


        # monthly_installations = onboarding_total_installtions[onboarding_total_installtions['vInsta_Month'] == month].sum()['Total_Insta'].astype(int)
        # weekly_installations = onboarding_total_installtions[(onboarding_total_installtions['V_Week_of_the_year'] == week) & (onboarding_total_installtions['vInsta_Month'] == month)]['Total_Insta'].sum().astype(int)
        
        summary = onboarding_main_installtions[
            (onboarding_main_installtions['min_comp/attempt_date'] >= x) &
            (onboarding_main_installtions['min_comp/attempt_date'] <= y)
        ].groupby('network_days').agg(inst=('installation_number', 'nunique')).reset_index()


        installations_under5days = summary[summary['network_days'] < 5]['inst'].sum()

# Calculate total installations
        total_inst = summary['inst'].sum()

# Calculate KPI
        KPI = round((installations_under5days / total_inst) * 100, 2)

        results[range].iloc[91:94] = [total_inst, np.nan,f"{float(KPI)}%"]

        results.to_csv(results_dir, index= False)

    calculations(week_start,week_end,'Week')
    calculations(month_start,month_end,'Month')

