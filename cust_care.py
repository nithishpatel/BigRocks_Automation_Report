import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def cust_care_calculation(week_start,week_end,month_start,month_end, results_dir): 
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
    results = pd.read_csv(results_dir)

    def Nweekdays(start_date, end_date):
        holidays = ["2024-01-01","2024-03-29","2024-04-01","2024-05-06","2024-05-27","2024-08-26","2024-12-25","2024-12-26","2025-01-01","2025-04-18","2025-04-21","2025-05-05","2025-05-26","2025-08-25","2025-12-25","2025-12-26","2026-01-01"]
        days = np.busday_count(start_date, end_date, holidays=holidays)
        return days

    #still_open = pd.read_excel("SAP BO data downloads/Retention_Dashboard_v7.xlsx", sheet_name="still open")
    open_current_time_period = pd.read_sql("select * from big_rocks.open_current_time_closed", connection)
    open_current_time_period['closing_date'] = pd.to_datetime(open_current_time_period['closing_date']).dt.date
    open_current_time_period['opening_date'] = pd.to_datetime(open_current_time_period['opening_date']).dt.date

    still_open = open_current_time_period[open_current_time_period['closing_date'].isnull() & (open_current_time_period['type']=='CANC')]
    still_open['age'] = still_open.apply(lambda x: Nweekdays(x['opening_date'],(datetime.today() - pd.Timedelta(days=1)).date() ), axis=1)
    still_open = still_open.assign(age = lambda x: np.where(x['age'] < 0, 0, x['age']))

    avg_age = still_open['age'].mean(skipna=True)

    closed_current_time_period = pd.read_sql("select * from big_rocks.closed_current_time_closed", connection)
    closed_current_time_period['fecha_desde_asignacion'].fillna(pd.Timestamp('20300101'), inplace=True)

    closed_current_time_period['network_days'] = closed_current_time_period.apply(lambda x: Nweekdays(x['opening_date'].date(),x['fecha_desde_asignacion'].date() ) , axis=1)
    # Apply the function to each row and create new columns
    closed_current_time_period["clean_wd"] = closed_current_time_period.apply(lambda row: "" if row["fecha_desde_asignacion"].date() == datetime(2030, 1, 1).date() 
                                                                              else 0 if row["network_days"] < 0 else row["network_days"] , axis=1)
    closed_current_time_period["internal_flag"] = closed_current_time_period.apply(lambda row: "internal" if row["fecha_desde_asignacion"].date() == datetime(2030, 1, 1).date()
                                                                                    and not pd.isna(row["close_employee_id"]) else "use assignacion date", axis=1)
    
    CC_agent_calls = pd.read_sql("select * from big_rocks.big_rocks_cc_agent_call_performance", connection) 
    cc_staff = pd.read_sql("select * from operations.customer_care_db.cc_commercial_operations_staff", connection)


    def calculations(x,y,range):
        y_date = datetime.strptime(y, '%Y-%m-%d').date()
        y_date = str(y_date)
        day_30 = str(datetime.strptime(y, '%Y-%m-%d').date() - timedelta(days=30))
        # Filter the rows where opening_date is between y and x
        open_current_time_period['opening_date'] = pd.to_datetime(open_current_time_period['opening_date']).dt.date.astype(str)
        open_current_time_period_IE = open_current_time_period[open_current_time_period['pais']=='IRELAND']
        last_30_days_opened_ticket = open_current_time_period[(open_current_time_period['opening_date'] > day_30) & (open_current_time_period['opening_date'] <= y_date)]
        last_30_days_opened_ticket = last_30_days_opened_ticket[last_30_days_opened_ticket['type']=='CANC']
        # total_opened is the number of unique values in ticker_number column
        # still_open is the number of unique values in ticker_number column where closing_date is missing
        # ratio is the percentage of still_open to total_opened
        last_30_days_opened_ticket_IE = last_30_days_opened_ticket[last_30_days_opened_ticket['pais']=='IRELAND']
        last_30_days_opened_ticket_summary = last_30_days_opened_ticket.agg(
            total_opened = ('ticker_number', pd.Series.nunique),
            still_open = ('ticker_number', lambda x: x[open_current_time_period['closing_date'].isna()].nunique())
        )
        last_30_days_opened_ticket_summary_IE = last_30_days_opened_ticket_IE.agg(
            total_opened = ('ticker_number', pd.Series.nunique),
            still_open = ('ticker_number', lambda x: x[open_current_time_period_IE['closing_date'].isna()].nunique())
        )

        backlog_size = round((last_30_days_opened_ticket_summary['ticker_number'][1] / last_30_days_opened_ticket_summary['ticker_number'][0]) * 100, 2)
        backlog_size_IE = round((last_30_days_opened_ticket_summary_IE['ticker_number'][1] / last_30_days_opened_ticket_summary_IE['ticker_number'][0]) * 100, 2)

        closed_current_time_summary = closed_current_time_period[(closed_current_time_period['opening_date'] >= x) & (closed_current_time_period['opening_date'] <= y)]
        # Calculate the summary statistics
        # closed_assignation is the number of unique values in ticker_number column where internal_flag is "use assignacion date" and clean_wd is 0, 1, or 2
        # closed_internal is the number of unique values in ticker_number column where internal_flag is "internal"
        # total_closed is the sum of closed_assignation and closed_internal
        closed_current_time=closed_current_time_summary[closed_current_time_summary['vstatus']=='CLOSED']
        closed_current_time_IE = closed_current_time_summary[
            (closed_current_time_summary['vstatus'] == 'CLOSED') &
            (closed_current_time_summary['pais'] == 'IRELAND')]
        closed_current_time_summary=closed_current_time_summary[closed_current_time_summary['type']=='CANC']
        closed_current_time_summary=closed_current_time_summary[closed_current_time_summary['pais']=='UK']
        closed_current_time_summary_IE=closed_current_time_IE[closed_current_time_IE['type']=='CANC']
        closed_current_time_summary = closed_current_time_summary.agg(
            closed_assignation = ('ticker_number', lambda x: x[(closed_current_time_summary['internal_flag'] == "use assignacion date") & closed_current_time_summary['clean_wd'].isin([0, 1, 2])].nunique()),
            closed_internal = ('ticker_number', lambda x: x[closed_current_time_summary['internal_flag'] == 'internal'].nunique())
        )
        closed_current_time_summary_IE = closed_current_time_summary_IE.agg(
            closed_assignation = ('ticker_number', lambda x: x[(closed_current_time_summary_IE['internal_flag'] == "use assignacion date") & closed_current_time_summary_IE['clean_wd'].isin([0, 1, 2])].nunique()),
            closed_internal = ('ticker_number', lambda x: x[closed_current_time_summary_IE['internal_flag'] == 'internal'].nunique())
        )
        
        open_current_time_period['opening_date'] = pd.to_datetime(open_current_time_period['opening_date']).dt.date.astype(str)
        total_opened = open_current_time_period[(open_current_time_period['opening_date'] >= x) & (open_current_time_period['opening_date'] <= y)]
        total_opened_IE = total_opened[total_opened['pais']=='IRELAND']
        total = total_opened.agg(
            opens = ('ticker_number', pd.Series.nunique)
        )
        total_IE = total_opened_IE.agg(
            opens = ('ticker_number', pd.Series.nunique)
        )
        still_open['opening_date'] = pd.to_datetime(still_open['opening_date']).dt.date.astype(str)
        opened= total_opened['ticker_number'].nunique()
        opened_IE= total_opened_IE['ticker_number'].nunique()
        Backlog_summary= still_open[(still_open['opening_date'] >= x) & (still_open['opening_date']<=y)]
        Total_Backlog = Backlog_summary['ticker_number'].nunique()

        #CC agents calls
        CC_agent_calls['start_time'] = pd.to_datetime(CC_agent_calls['start_timestamp'], format='%Y-%m-%d')
        CC_agent_calls_summary = CC_agent_calls[(CC_agent_calls['start_time'] >= x) & (CC_agent_calls['start_time'] <= y)]
        Inbound_total_calls = CC_agent_calls_summary[CC_agent_calls_summary['last_vqueue']=='UK_CustomerCare_Target_VQ']['interaction_id'].nunique()
        filtered = CC_agent_calls_summary[(CC_agent_calls_summary['last_vqueue'] == 'UK_CustomerCare_Target_VQ') & (CC_agent_calls_summary['technical_result'].isin(['Completed', 'Transferred', 'Conferenced']) |(CC_agent_calls_summary['technical_result_reason'] == 'AbandonedFromHold')) &(CC_agent_calls_summary['routing_point_time'] < 40)]
        Inbound_answered_response= filtered['interaction_id'].nunique()
        Inbound_answered_response_lessthan_40 = round(Inbound_answered_response/Inbound_total_calls*100,2)
        filtered_120sec = CC_agent_calls_summary[(CC_agent_calls_summary['last_vqueue'] == 'UK_CustomerCare_Target_VQ') & (CC_agent_calls_summary['technical_result'].isin(['Completed', 'Transferred', 'Conferenced']) |(CC_agent_calls_summary['technical_result_reason'] == 'AbandonedFromHold')) & (CC_agent_calls_summary['routing_point_time'] > 120)]
        Inbound_answered_response_120sec= filtered_120sec['interaction_id'].nunique()
        Inbound_answered_response_greaterthan_120 = round(Inbound_answered_response_120sec/Inbound_total_calls*100,2)
        Inbound_abandoned = CC_agent_calls_summary[(CC_agent_calls_summary['last_vqueue'] == 'UK_CustomerCare_Target_VQ') &((CC_agent_calls_summary['technical_result']== 'Abandoned') | (CC_agent_calls_summary['technical_result_reason'].isin(['AbandonedWhileQueued', 'AbandonedWhileRinging']))) ]
        Inbound_abandoned_count = Inbound_abandoned['interaction_id'].nunique()
        Inbound_abandoned_greaterthan_10sec = CC_agent_calls_summary[(CC_agent_calls_summary['last_vqueue'] == 'UK_CustomerCare_Target_VQ') & ((CC_agent_calls_summary['technical_result']== 'Abandoned') |(CC_agent_calls_summary['technical_result_reason'].isin(['AbandonedWhileQueued', 'AbandonedWhileRinging']))) & (CC_agent_calls_summary['routing_point_time'] > 10)]
        Inbound_abandoned_greaterthan_10sec = Inbound_abandoned_greaterthan_10sec['interaction_id'].nunique()
        Inbound_abandoned_greaterthan_10sec_per = round(Inbound_abandoned_greaterthan_10sec/Inbound_total_calls*100,2)
        filtered_IE = CC_agent_calls_summary[(CC_agent_calls_summary['last_vqueue'] == 'UK_CustomerCare_Target_VQ') & (CC_agent_calls_summary['from'].str.startswith('08')) &(~CC_agent_calls_summary['from'].str.startswith('080'))]

        Inbound_total_calls_IE = filtered_IE['interaction_id'].nunique()
        Inbound_total_calls_lessthan_40sec_IE = filtered_IE[filtered_IE['routing_point_time'] < 40]['interaction_id'].nunique()
        Inbound_total_calls_lessthan_40sec_per_IE = round(Inbound_total_calls_lessthan_40sec_IE / Inbound_total_calls_IE * 100, 2) if Inbound_total_calls_IE != 0 else 0

        total_closed = closed_current_time_summary['ticker_number'].sum()
        total_closed_IE = closed_current_time_summary_IE['ticker_number'].sum()
        Cancellation_resp = round((total_closed / total['ticker_number'].sum() ) * 100, 2)
        Cancellation_resp_IE = round((total_closed_IE / total_IE['ticker_number'].sum() ) * 100, 2)

        closed_current_time_ret = closed_current_time_period[(closed_current_time_period['closing_date'] >= x) & (closed_current_time_period['closing_date'] <= y)]
        closed_current_time_ret = closed_current_time_ret[closed_current_time_ret['vstatus'].isin(['CLOSED', 'ERROR'])]        
        closed_current_time_ret =closed_current_time_ret[closed_current_time_ret['vclosing_id']!='ERROR']

        merged_df = pd.merge(closed_current_time_ret,cc_staff,left_on='close_employee_id',right_on='employee_id',how='left',  indicator=True)
        merged_df['ClosedByCC'] = (merged_df['_merge'] == 'both').astype(int)
        # Filter ClosedByCC == 1
        closed_by_cc = merged_df[merged_df['ClosedByCC'] == 1]

# Count for CANC + Retained + ClosedByCC=1
        CancRet = closed_by_cc[(closed_by_cc['type'] == 'CANC') & (closed_by_cc['vresolution'] == 'Retained')].shape[0]

# Count for RECO + Retained + ClosedByCC=1
        RecoRet = closed_by_cc[(closed_by_cc['type'] == 'RECO') & (closed_by_cc['vresolution'] == 'Retained')].shape[0]

# Count for CCN + Retained + ClosedByCC=1
        CCNRet = closed_by_cc[(closed_by_cc['type'] == 'CCN') & (closed_by_cc['vresolution'] == 'Retained')].shape[0]

# Count for CANC + Cancelled + ClosedByCC=1
        CancCan = closed_by_cc[(closed_by_cc['type'] == 'CANC') & (closed_by_cc['vresolution'] == 'Cancelled')].shape[0]

# Final calculation, with zero division fallback to 0
        try:
            OverallRetRate = round(((CancRet + RecoRet + CCNRet) / (CancRet + RecoRet + CancCan))*100,2)
        except ZeroDivisionError:
            OverallRetRate = 0

        print(Inbound_answered_response,Inbound_answered_response_lessthan_40,Inbound_answered_response_greaterthan_120,Inbound_abandoned_count,Inbound_abandoned_greaterthan_10sec_per,Inbound_total_calls_IE,Inbound_total_calls_lessthan_40sec_per_IE)
# Create ClosedByCC flag
        merged_df['ClosedByCC'] = (merged_df['_merge'] == 'both').astype(int)

        results[range].iloc[67:88] = [opened,total_closed,opened_IE,np.nan,f"{Cancellation_resp}%",f"{Cancellation_resp_IE}%",np.nan,Inbound_answered_response,f"{Inbound_answered_response_lessthan_40}%",f"{Inbound_answered_response_greaterthan_120}%",Inbound_abandoned_count,f"{Inbound_abandoned_greaterthan_10sec_per}%",Inbound_total_calls_IE,f"{Inbound_total_calls_lessthan_40sec_per_IE}%",np.nan,Total_Backlog,f"{backlog_size}%", round(avg_age, 2) , f"{backlog_size_IE}%",np.nan,f"{OverallRetRate}%" ]

        results.to_csv(results_dir, index= False)

    calculations(week_start,week_end,'Week')
    calculations(month_start,month_end,'Month')
