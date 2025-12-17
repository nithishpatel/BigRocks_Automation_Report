import pandas as pd
import numpy as np
from datetime import datetime as dt
import datetime
#import os
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
def rate_calculations(week_start,week_end,month_start, month_end,results_dir):

    results = pd.read_csv(results_dir)
    date_filter = str((datetime.now() - timedelta(days=40)).date())
    #ROOT_DIR ="C:/Users/nithish.patel/OneDrive - Verisure/Big Rocks/SAP BO data downloads/FS retention"
    ftf_40 = pd.read_sql(f"""select * from ops_field_services.fsm_closed_maintenances fcm where fcm."closing/finishing_date" > '{date_filter}' """, connection )
    backlog_fs = pd.read_sql("select * from big_rocks.big_rocks_fs_backlog_kpi", connection) 
    total_installation_count= pd.read_sql('select * from big_rocks.fs_backlog_installation_count WHERE audit_insert_timestamp = (SELECT MAX(audit_insert_timestamp) FROM big_rocks.fs_backlog_installation_count)', connection) 
    # files = os.listdir(ROOT_DIR)
    # paths = [os.path.join(ROOT_DIR, basename) for basename in files]
    # latest_file = max(paths, key=os.path.getctime)
    FS_retention_added = pd.read_sql('select * from big_rocks.fspa_added_removed_details',connection)
    FS_retention_unchanged = pd.read_sql('select * from big_rocks.fspa_unchanged_details', connection)
    call_type_codes = ["MOVE", "WORK", "ARLO", "TAKE", "RESC", "SATI", "INST", "EXTE", "INFO", "CURE"]

    def calculations(x, y, range,FS_retention_added,FS_retention_unchanged,ftf_40,backlog_fs):
        ftf_40['v_work_requested'] = np.where(ftf_40['call_type_code'].isin(call_type_codes), 'WORK', 'TECHNICAL')
        ftf_40['v_incident'] = np.where(ftf_40['call_type_code'] == 'ROBB', 'INCIDENT', 'NO INCIDENT')
        technician_list = ["230020", "270009", "270019", "270023", "270026", "270024", "270025", "270026", "270030"]
        date_y = datetime.strptime(y, '%Y-%m-%d')
        z = date_y - timedelta(days=41)
        ftf_40['technician_line_code'] = ftf_40['technician_line_code'].astype(str).str.strip()
        ftf_40['technician_line_code'] = [str(value).replace('.0', '') if isinstance(value, str) else value for value in ftf_40['technician_line_code']]
        ftf_40_filtered  = ftf_40[(ftf_40['closing/finishing_date']>= z) & (ftf_40['closing/finishing_date']<= y)]
        ftf_40_filtered  = ftf_40_filtered [ftf_40_filtered ['call_type_code'] != '100']
        ftf_40_filtered  = ftf_40_filtered [ftf_40_filtered ['status_maintenance'] != 'CERR']
        ftf_40_filtered  = ftf_40_filtered [~ftf_40_filtered ['installation_number'].astype(str).str.startswith('7')]
        ftf_40_filtered ['v_TECH'] = np.where(ftf_40_filtered ['technician_line_code'].str.strip().isin(technician_list), 'Y', 'N')
        ftf_40_filtered  = ftf_40_filtered [ftf_40_filtered ['v_TECH'] == 'Y']
        ftf_40_filtered .drop_duplicates(['code_maintenance_ibs'], inplace=True)
        ftf_40_filtered  = ftf_40_filtered .sort_values(by='installation_number', ascending=True)
        
        ftf_40_filtered ['FTF_values'] = np.where((ftf_40_filtered ['installation_number'] == ftf_40_filtered ['installation_number'].shift()) & (ftf_40_filtered ['v_work_requested'] == "TECHNICAL") & (ftf_40_filtered ['v_incident'] == "NO INCIDENT"), 'NO FTF', 'FTF')
        days_40_first = round(ftf_40_filtered ['FTF_values'][ftf_40_filtered ['FTF_values'] == "FTF"].count() / ftf_40_filtered ['code_maintenance_ibs'].count() * 100 , 2)
        today = dt.now()
        monday = (today - timedelta(days=today.weekday())).date()  # Ensure 'monday' is a date object

# Convert column to datetime
        total_installation_count['audit_insert_timestamp'] = pd.to_datetime(
            total_installation_count['audit_insert_timestamp'], errors='coerce'
        )

        # Filter rows where audit_insert_timestamp is on Monday
        Total_count = total_installation_count[
            total_installation_count['audit_insert_timestamp'].dt.date == monday
        ]

        # Count unique installation numbers
        # Total_count_IE = Total_count[Total_count['installation_number'].astype(str).str.startswith('7')]
        # Total_count_IE = Total_count_IE['installation_number'].nunique()
        Total_count = Total_count['installation_number'].nunique()
        

        max_audit_timestamp = backlog_fs['audit_insert_timestamp'].max()
        backlog_fs = backlog_fs[backlog_fs['audit_insert_timestamp']==max_audit_timestamp]
        # backlog_fs_IE = backlog_fs[backlog_fs['country']=="IRELAND"]
        backlog_fs = backlog_fs[backlog_fs['country']=="UK"]
        # Further filter to keep only rows where 'audit_insert_timestamp' is equal to the max
        backlog_fs_summary = backlog_fs[(backlog_fs['creating_date'] >= x) &
                                                    (backlog_fs['creating_date'] <= y)]
        backlog_fs_summary = backlog_fs_summary[backlog_fs_summary['installation_number']!='0']
        backlog_aware=backlog_fs[backlog_fs['req_by']!='GTI']
        backlog_aware_max = backlog_aware.groupby('code_maintenance_ibs')['age'].max().reset_index()
        backlog_aware_count= backlog_fs_summary[backlog_fs_summary['req_by']!='GTI']['code_maintenance_ibs'].count()
        backlog_not_aware= backlog_fs[backlog_fs['req_by']=='GTI']
        # backlog_not_aware_IE= backlog_fs_IE[backlog_fs_IE['req_by']=='GTI']
        backlog_not_aware_max = backlog_not_aware.groupby('code_maintenance_ibs')['age'].max().reset_index()
        backlog_not_aware_total=backlog_not_aware['code_maintenance_ibs'].nunique()
        # backlog_not_aware_total_IE=backlog_not_aware_IE['code_maintenance_ibs'].nunique()
        backlog_not_aware_count= backlog_fs_summary[backlog_fs_summary['req_by']=='GTI']['code_maintenance_ibs'].count()

        Aware_avg_age = round(backlog_aware_max['age'].mean() , 2)
        Not_Aware_avg_age = round(backlog_not_aware_max['age'].mean() , 2)
        Backlog_percent=round((backlog_not_aware_total/Total_count)*100,2)
        #Backlog_percent_IE=round((backlog_not_aware_total_IE/Total_count_IE)*100,2)
        FS_retention_added=FS_retention_added[(FS_retention_added['date']>= x) & (FS_retention_added['date']<= y)]
        # FS_retention_added_IE= FS_retention_added[FS_retention_added['wzprefix'].astype(str).str.startswith(('IE', 'IN'))]
        FS_retention_added= FS_retention_added[~FS_retention_added['wzprefix'].astype(str).str.startswith(('IE', 'IN'))]
        FS_retention_unchanged=FS_retention_unchanged[(FS_retention_unchanged['date']>= x) & (FS_retention_unchanged['date']<= y)]
        FS_retention_unchanged_IE= FS_retention_unchanged[FS_retention_unchanged['wzprefix'].astype(str).str.startswith(('IE', 'IN'))]
        FS_retention_unchanged= FS_retention_unchanged[~FS_retention_unchanged['wzprefix'].astype(str).str.startswith(('IE', 'IN'))]

        numerator_1 = FS_retention_added[FS_retention_added['d1_service_status']=='completed']['service_number'].count()
        # numerator_1_IE = FS_retention_added_IE[FS_retention_added_IE['d1_service_status']=='completed']['service_number'].count()
        numerator_2 = FS_retention_unchanged[FS_retention_unchanged['d1_service_status']=='completed']['service_number'].count()
        # numerator_2_IE = FS_retention_unchanged_IE[FS_retention_unchanged_IE['d1_service_status']=='completed']['service_number'].count()

        numerator=numerator_1+numerator_2
        # numerator_IE=numerator_1_IE+numerator_2_IE
        Denominator_1 = FS_retention_added[FS_retention_added['d1_service_status'].isin(['completed','started'])]['service_number'].count()
        # Denominator_1_IE = FS_retention_added_IE[FS_retention_added_IE['d1_service_status'].isin(['completed','started'])]['service_number'].count()
        Denominator_2 = FS_retention_unchanged[FS_retention_unchanged['d1_service_status'].isin(['completed','suspended','notdone','pending'])]['service_number'].count()
        # Denominator_2_IE = FS_retention_unchanged_IE[FS_retention_unchanged_IE['d1_service_status'].isin(['completed','suspended','notdone','pending'])]['service_number'].count()
        Denominator_3 = FS_retention_added[FS_retention_added['compare']=='Removed']['service_number'].count()
        # Denominator_3_IE = FS_retention_added_IE[FS_retention_added_IE['compare']=='Removed']['service_number'].count()
        Denominator = Denominator_1+Denominator_2+Denominator_3
        # Denominator_IE = Denominator_1_IE+Denominator_2_IE+Denominator_3_IE

        print(numerator_1,numerator_2,Denominator_1,Denominator_2,Denominator_3)
        FSE= round((numerator/ Denominator)*100,2)
        # FSE_IE= round((numerator_IE/ Denominator_IE)*100,2)
        results[range].iloc[57:58] = [f"{FSE}%"]
        results[range].iloc[59:66] = [f"{days_40_first}%",np.nan,f"{Backlog_percent}%",backlog_aware_count,Aware_avg_age,backlog_not_aware_count,Not_Aware_avg_age]
        #results['Month'].iloc[37:42] = [f"{days_40_first}%",backlog_aware_count,Aware_avg_age,backlog_not_aware_count,Not_Aware_avg_age]
        results.to_csv(results_dir, index= False)
    calculations(week_start, week_end, 'Week', FS_retention_added, FS_retention_unchanged, ftf_40,backlog_fs)
    calculations(month_start, month_end, 'Month', FS_retention_added, FS_retention_unchanged, ftf_40,backlog_fs)

