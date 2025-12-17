import pandas as pd
import numpy as np
import datetime
import psycopg2
from datetime import datetime, timedelta
import warnings
warnings.simplefilter("ignore")


def cust_service_calculation(week_start,week_end,month_start,month_end,results_dir):
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

    InfoPoint_Ticket_new_built_escalations_time = pd.read_sql(f"select * from operations.infopoint_tickets.tickets_closed tc where closing_date >='{date_filter}' and ticket_problem='CVIP' ;",connection)
    InfoPoint_Ticket_new_built_escalations_time=InfoPoint_Ticket_new_built_escalations_time[~InfoPoint_Ticket_new_built_escalations_time['installation_number_closed'].astype(str).str.startswith('7')]
    InfoPoint_Ticket_new_built_escalations_time['hours_closed'] = ((InfoPoint_Ticket_new_built_escalations_time['closing_date_time']
                                                            - InfoPoint_Ticket_new_built_escalations_time['creation_datetime']) / pd.Timedelta(hours=1)).round(decimals=2)
    InfoPoint_Ticket_new_built_escalations_time['Bracket_24'] = InfoPoint_Ticket_new_built_escalations_time.apply(lambda row: 'done in 24hr' if row['hours_closed'] < 24 
                                                                                                                else 'not done in 24hr', axis=1)
    InfoPoint_Ticket_new_built_escalations_time['Bracket_48'] = InfoPoint_Ticket_new_built_escalations_time.apply(lambda row: 'done in 48hr' if row['hours_closed'] < 48 
                                                                                                                else 'not done in 48hr', axis=1)

    def calculations(x,y,range):

        summary_escalation = InfoPoint_Ticket_new_built_escalations_time[(InfoPoint_Ticket_new_built_escalations_time['closing_date'] >= x) & 
                                                                        (InfoPoint_Ticket_new_built_escalations_time['closing_date'] <= y )]
        total_escalations = int(len(pd.unique(summary_escalation['ticket_number_closed'])))
        summary_escalation = pd.concat([summary_escalation.groupby('Bracket_24').agg({'ticket_number_closed': 'nunique'}).reset_index().rename(columns={'Bracket_24': 'Brackets'}),
                                        summary_escalation.groupby('Bracket_48').agg({'ticket_number_closed': 'nunique'}).reset_index().rename(columns={'Bracket_48': 'Brackets'})]).reset_index() 

        summary_escalation['ratio'] = round((summary_escalation['ticket_number_closed']/total_escalations)*100,2)
        # print(total_escalations)
        # print(summary_escalation['ratio'])
        # print(summary_escalation['ratio'].index)
        results[range].iloc[40:43] = [total_escalations,f"{summary_escalation['ratio'][0]}%", f"{summary_escalation['ratio'][2]}%"]
        results.iloc[40:43]
        results.to_csv(results_dir, index= False)

    calculations(week_start,week_end,'Week')
    calculations(month_start,month_end,'Month')
