import pandas as pd
import numpy as np
import datetime
import psycopg2
from datetime import datetime, timedelta


def diy_calculation(week_start,week_end,month_start,month_end, results_dir):
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

    date_filter = str((datetime.now() - timedelta(days=40)).date())

    results = pd.read_csv(results_dir)
    # reading all the data 
    #serv_batteries_finalised = pd.read_excel("SAP BO data downloads/DIY__SBN_services__V2.xlsx", 
                                #skiprows = 1, usecols=lambda x: 'Unnamed: 0' not in x )

    #Processing_Time_diy_offering = pd.read_excel("SAP BO data downloads/Q11_DIY_Backlog_Processing_Time_-Horacio_KPI.xlsx", 
                                #skiprows = 3, usecols=lambda x: 'Unnamed: 0' not in x )

    #batteries_sent = pd.read_excel("SAP BO data downloads/DIY_FSM_Services_Booked_-BIG_ROCKS_KPIS.xlsx", 
                                #skiprows = 1, usecols=lambda x: 'Unnamed: 0' not in x )


    # pulling a table which has the duplicated maintainance IDs, cos robot creates them again
    #Pre_diy_created_and_accepted_by_the_Robot = pd.read_excel("SAP BO data downloads/Pre_diy_created_and_accepted_by_the_Robot.xlsx", 
                                #skiprows = 0, usecols=lambda x: 'Unnamed: 0' not in x )

    # need to append the pre diy data to the main one

    Promoted_to_Field = pd.read_sql('select * from big_rocks.pre_diy_offering_promoted_to_field',connection)

    Offering_list = pd.read_sql('select * from big_rocks.pre_diy_offering_pre_diy_created',connection)

    Services_finalised = pd.read_sql('select * from big_rocks.pre_diy_offering_services_finalised',connection)


    # pulling another sheet that has promoted to field with no delivery
    Chasing_list = pd.read_sql('select * from big_rocks.pre_diy_offering_chasing_list',connection)
    Closed_services= pd.read_sql(f'''select * from ops_field_services.fsm_closed_maintenances
                                where "closing/finishing_date" >= '{date_filter}';''',connection)
    starkeys_services= pd.read_sql(f'''select * from big_rocks.star_keys_and_remotes
                                where "closing/finishing_date" >= '{date_filter}';''',connection)
    


    Services_finalised['maintenance_closing_date'] = pd.to_datetime(Services_finalised['maintenance_closing_date'])
    Services_finalised['creation_date'] = pd.to_datetime(Services_finalised['creation_date'])

    # Calculate days_to_close
    Services_finalised['days_to_close'] = (Services_finalised['maintenance_closing_date'] - Services_finalised['creation_date']).dt.days
    # Services_finalised['days_to_close']= Services_finalised[Services_finalised['days_to_close']!=0]
    # Create new columns based on conditions
    Services_finalised['Bracket_<10'] = np.where(Services_finalised['days_to_close'] <= 10, 'not done in 10 days', 'done in 10 days')
    Services_finalised['Bracket_>14'] = np.where(Services_finalised['days_to_close'] > 14, 'done after 14 days', 'done before 14 days')


    def calculations(x,y,range):


        # Filter rows based on date conditions
        filtered_df = Services_finalised[
            (Services_finalised['maintenance_closing_date'] >= x) &
            (Services_finalised['maintenance_closing_date'] <= y)
        ]

        total_batteries_changed = filtered_df['maintenance_id'].nunique()
        without_agent_df= filtered_df.copy()
        filtered_df= filtered_df[filtered_df['days_to_close'] != 0]
        total_services = filtered_df['maintenance_id'].nunique()
        # Group by bracket and calculate incidences
        filtered_df['less_than_10']= np.where(filtered_df['days_to_close'] <= 10, "yes", "no")

        # # Calculate ratio
        # summary_battery_changes['ratio'] = round((summary_battery_changes['incidences'] / total_batteries_changed) * 100, 2)
        changed_less_than_10 = round((filtered_df['less_than_10'] == 'yes').sum() / ((filtered_df['less_than_10'] == 'yes').sum() + (filtered_df['less_than_10'] == 'no').sum()) * 100, 2)

        #batteries_sent2 = batteries_sent_non_duplicate_contract
        Opened_Services = Offering_list[
            (Offering_list['creation_date'] >= x) &
            (Offering_list['creation_date'] <= y)
        ]
        Total_opened_services= Opened_Services['maintenance_id'].nunique()
        
        Backlogs = Chasing_list[
            (Chasing_list['order_creation_date'] >= x) &
            (Chasing_list['order_creation_date'] <= y)
        ]
        Total_Backlogs= Backlogs['maintenance_id'].count()

        to_field = Promoted_to_Field[
            (Promoted_to_Field['field_promotion_date'] >= x) &
            (Promoted_to_Field['field_promotion_date'] <= y)
        ]
        Total_field= to_field['maintenance_id'].count()
        DIY_to_field=round((Total_field/(Total_field+total_batteries_changed))*100,2)
        Closed_services_summary=Closed_services[(Closed_services['closing/finishing_date']>= x ) & (Closed_services['closing/finishing_date']<= y)]
        Closed_services_summary['technician_line_code'] = Closed_services_summary['technician_line_code'].fillna(0).astype(int).astype(str).str.replace('.', '', regex=False).str.strip()
        Closed_services_summary = Closed_services_summary[
            (Closed_services_summary['status_maintenance'] == 'FINL') &
            (Closed_services_summary['installation_number'] != '0') &
            (~Closed_services_summary['installation_number'].astype(str).str.startswith('7')) &
            (Closed_services_summary['technician_line_code'].isin(['230020', '270009', '270019', '270023', '270030', '265009','270013','270017','270020', '270024','270016','270025']))]
        Closed_services_summary['creating_date'] = pd.to_datetime(Closed_services_summary['creating_date'])
        Closed_services_summary['closing/finishing_date'] = pd.to_datetime(Closed_services_summary['closing/finishing_date'])
        starkeys_services['closing/finishing_date'] = pd.to_datetime(starkeys_services['closing/finishing_date'])
        starkeys_summary = starkeys_services[(starkeys_services['closing/finishing_date']>= x ) & (starkeys_services['closing/finishing_date']<= y)]
        Closed_services_summary['Days']=(Closed_services_summary['closing/finishing_date']-Closed_services_summary['creating_date']).dt.days
        Aware_services = Closed_services_summary[Closed_services_summary['req_by']!='GTI']
        Aware_count = Closed_services_summary[Closed_services_summary['req_by']!='GTI']['code_maintenance_ibs'].nunique()
        Aware_services_SLA=Aware_services[~Aware_services['activity_type'].isin(['Retention oppertunity','Installations','Moonshot Upgrade','System Dismantle'])]
        Total_Aware_services_SLA=Aware_services_SLA['code_maintenance_ibs'].nunique()
        SLA_7days= Aware_services_SLA[Aware_services_SLA['Days']<=7]['code_maintenance_ibs'].nunique()
        SLA_7days_percent=round((SLA_7days/Total_Aware_services_SLA)*100,2)

        Not_Aware_services = Closed_services_summary[Closed_services_summary['req_by']=='GTI']
        Not_Aware_count = Closed_services_summary[Closed_services_summary['req_by']=='GTI']['code_maintenance_ibs'].nunique()
        Not_Aware_services_SLA=Not_Aware_services[~Not_Aware_services['activity_type'].isin(['Retention oppertunity','Installations','Moonshot Upgrade','System Dismantle'])]
        
        Total_Not_Aware_services_SLA=Not_Aware_services_SLA['code_maintenance_ibs'].nunique()
        Not_SLA_7days= Not_Aware_services_SLA[Not_Aware_services_SLA['Days']<=30]['code_maintenance_ibs'].nunique()
        Not_SLA_30day_percent=round((Not_SLA_7days/Total_Not_Aware_services_SLA)*100,2)
        Finalized= Aware_count+Not_Aware_count
        starkeys_count=starkeys_summary['code_maintenance_ibs'].nunique()
        
        DIY_to_FieldServicepercent= round(((total_batteries_changed+starkeys_count)/(total_batteries_changed+Finalized))*100,2)
        without_agent= without_agent_df[without_agent_df['closing/finishing_identifier'].isnull()]
        without_agent_total = without_agent.shape[0]
        total_installations = without_agent_df.shape[0]
        
        without_agent_percent = round((without_agent_total/total_installations)*100,2)
        print(without_agent_total, total_installations)
        
        # results[range].iloc[54:59] = [total_batteries_offered, np.nan, f"{float(summary_battery_offered[summary_battery_offered ['Bracket_7'] == 'done in 7 days']['ratio'])}%" ,
        #                     f"{float(summary_battery_offered[summary_battery_offered ['Bracket_14'] == 'done after 14 days']['ratio'])}%", f"{float(summary_diy_acceptance_rate[summary_diy_acceptance_rate ['Cancellation SubType'] == 'DIYAC']['ratio'])}%"] 
        results[range].iloc[95:104] = [Total_opened_services,total_batteries_changed,np.nan,f"{DIY_to_FieldServicepercent}%",f"{without_agent_percent}%",np.nan,Total_Backlogs,np.nan,f"{DIY_to_field}%"]
        #results[range].iloc[101:103] = [f"{DIY_to_FieldServicepercent}%",f"{without_agent_percent}%"]
        results.to_csv(results_dir, index= False)

        
    calculations(week_start,week_end,'Week')
    calculations(month_start,month_end,'Month')
    # print (results.iloc[54:65])