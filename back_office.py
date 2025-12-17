import pandas as pd
import numpy as np
import datetime
import psycopg2
from datetime import datetime, timedelta


def back_office_calculation(week_start, week_end, month_start, month_end, results_dir):
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

    m_test_generated = pd.read_sql('select * from big_rocks.missing_test_zv_mtest_generated',connection)
    m_test_closed = pd.read_sql('select * from big_rocks.missing_test_zv_mt_closed',connection)
    m_test_generated.columns = m_test_generated.columns.str.strip()
    m_test_closed.columns = m_test_closed.columns.str.strip()
    m_test_generated = m_test_generated.applymap(
        lambda x: x.strip() if isinstance(x, str) else x)
    m_test_generated_IE = m_test_generated[m_test_generated['country']=='IRELAND']
    m_test_generated = m_test_generated[m_test_generated['country']=='UK']
    m_test_backlog = pd.read_sql('select * from big_rocks.missing_test_zv_mtest_backlog',connection)
    
    m_test_backlog.columns = m_test_backlog.columns.str.strip()
    
    m_test_backlog = m_test_backlog.applymap(
        lambda x: x.strip() if isinstance(x, str) else x)
    
    zv_m_test_generated = pd.read_sql('Select * from big_rocks.zvision_pending_created_zv_mt_generated',connection)
    
    zv_m_test_generated.columns = zv_m_test_generated.columns.str.strip()
    
    zv_m_test_generated = zv_m_test_generated.applymap(
        lambda x: x.strip() if isinstance(x, str) else x)
    
    zv_m_test_generated_IE = zv_m_test_generated[zv_m_test_generated['country']=='IRELAND']
    zv_m_test_generated = zv_m_test_generated[zv_m_test_generated['country']=='UK']
    zv_m_test_backlog = pd.read_sql('select * from big_rocks.zvision_pending_created_zv_mt_backlog',connection)
    zv_m_test_closed = pd.read_sql('select * from etl.back_office_zv_mtest_closed',connection)
    zv_m_test_backlog.columns = zv_m_test_backlog.columns.str.strip()
    zv_m_test_closed.columns = zv_m_test_closed.columns.str.strip()
    zv_m_test_backlog = zv_m_test_backlog.applymap(
        lambda x: x.strip() if isinstance(x, str) else x)
    



# Read the Excel file
    portfolio_actuals = pd.read_sql('select * from big_rocks.portfolio_pull_actuals',connection)

    # Trim white spaces in monitoring_status and contract_status
    portfolio_actuals['monitoring_status'] = portfolio_actuals['monitoring_status'].str.strip()
    portfolio_actuals['contract_status'] = portfolio_actuals['contract_status'].str.strip()

    # Map the active portfolio
    def active_portfolio_flag(row):
        if row['monitoring_status'] == 'OP' and row['contract_status'] in ['CUEM', 'CUOP', 'GRAT']:
            return 1
        elif row['monitoring_status'] == 'STBY' and row['contract_status'] in ['CUEM', 'CUOP', 'STBY']:
            return 1
        elif row['monitoring_status'] == 'XCAN' and row['contract_status'] in ['CUEM', 'CUOP']:
            return 1
        elif row['monitoring_status'] in ['EDOC', 'INST', 'N/A'] and row['contract_status'] == 'CUOP':
            return 1
        else:
            return 0

    portfolio_actuals['active_portfolio_flag'] = portfolio_actuals.apply(active_portfolio_flag, axis=1)

    # Filter active customers and remove panel group errors
    Cust_info_for_portfolio_active = portfolio_actuals[(portfolio_actuals['active_portfolio_flag'] == 1) & (portfolio_actuals['panel_group'] != 'ERROR')]

    # Read Zero Vision Installations
    ZV_portfolio = pd.read_sql('select * from big_rocks.portfolio_pull_zero_vision_inst',connection)
    ZV_portfolio = ZV_portfolio[ZV_portfolio['device'] == 'FG']
    ZV_portfolio['zv_flag'] = 1

    # Create a summary of Zero Vision installations
    ZV_portfolio_summary = ZV_portfolio[['ins_no', 'zv_flag']].drop_duplicates()
    ZV_portfolio_summary = ZV_portfolio_summary.rename(columns={'ins_no': 'installation_number'})
    ZV_portfolio_summary['installation_number'] = ZV_portfolio_summary['installation_number'].astype(str)

    Cust_info_for_portfolio_active['installation_number'] = Cust_info_for_portfolio_active['installation_number'].astype(str)

    # Map the ZV installations to the monthly active portfolio
    Cust_info_for_portfolio_active_zv = Cust_info_for_portfolio_active.merge(ZV_portfolio_summary, on='installation_number', how='left')

    # Summarize the data
    zv_summary = Cust_info_for_portfolio_active_zv.groupby('zv_flag')['installation_number'].nunique().reset_index()
    ZV_portfolio_no = Cust_info_for_portfolio_active_zv[Cust_info_for_portfolio_active_zv['zv_flag'] == 1]['installation_number'].nunique()
    panel_inst = Cust_info_for_portfolio_active_zv['installation_number'].nunique()

    # Print the results
    print(f"Panel active installations to be used as the denominator value is: {panel_inst}")
    print(f"ZV active installations to be used as denominator is: {ZV_portfolio_no}")


    # # the purpose is to find the number of incidences in the SLA 2 for week/ month date range
    def calculations(x, y, range,ZV_portfolio_no,panel_inst):
        MT_summary = m_test_generated[(m_test_generated['incidence_creation_date'] >= x) & (
            m_test_generated['incidence_creation_date'] <= y)]
        MT_closed_summary = m_test_closed[(m_test_closed['cancellation_date'] >= x) & (
            m_test_closed['cancellation_date'] <= y)]
        MT_closed_summary = MT_closed_summary[MT_closed_summary['cancellation_datetime'] != pd.Timestamp('1980-01-01 00:00:00')]
        MT_summary_IE = m_test_generated_IE[(m_test_generated_IE['incidence_creation_date'] >= x) & (
            m_test_generated_IE['incidence_creation_date'] <= y)]
        MT_summary['cancellation_datetime'] = pd.to_datetime(MT_summary['cancellation_datetime'], errors='coerce')
        MT_summary['incidence_creation_datetime'] = pd.to_datetime(MT_summary['incidence_creation_datetime'], errors='coerce')

        ZV_summary = zv_m_test_generated[(zv_m_test_generated['incidence_creation_date'] >= x) & (
            zv_m_test_generated['incidence_creation_date'] <= y)]
        ZV_closed_summary = zv_m_test_closed[(zv_m_test_closed['incidence_cancellation_date'] >= x) & (
            zv_m_test_closed['incidence_cancellation_date'] <= y)]
        ZV_summary_IE = zv_m_test_generated_IE[(zv_m_test_generated_IE['incidence_creation_date'] >= x) & (
            zv_m_test_generated_IE['incidence_creation_date'] <= y)]
        
        MT_backlog_summary = m_test_backlog[(m_test_backlog['incidence_creation_date'] >= x) & (
            m_test_backlog['incidence_creation_date'] <= y)]
        
        ZV_backlog_summary = zv_m_test_backlog[(zv_m_test_backlog['incidence_creation_date'] >= x) & (
            zv_m_test_backlog['incidence_creation_date'] <= y)]
        MT_closed_summary['cancellation_subtype'] = MT_closed_summary['cancellation_subtype'].str.strip()
        ZV_closed_summary['cancellation_subtype'] = ZV_closed_summary['cancellation_subtype'].str.strip()
        total_received = len(
            MT_summary['incidence_number']) + len(ZV_summary['incidence_number'])
        total_closed = len(
            MT_closed_summary['incidence_number']) + len(ZV_closed_summary['incidence_number'])
        total_received_IE = len(
            MT_summary_IE['incidence_number']) + len(ZV_summary_IE['incidence_number'])
        
        total = len(MT_closed_summary['incidence_number'])
        
        field_Service_MT = len(
            MT_closed_summary[MT_closed_summary['cancellation_subtype'] == "OPSE"])
        
        field_Service_ZV = len(
            ZV_closed_summary[ZV_closed_summary['cancellation_subtype'] == "OPSE"])
        
        sent_to_field = round(
            ((field_Service_MT+field_Service_ZV)/total_closed)*100, 2)
        
        MT_closed_summary['hours_closed'] = ((MT_closed_summary['cancellation_datetime'] -
                                      MT_closed_summary['incidence_creation_datetime']) / pd.Timedelta(hours=1)).round(decimals=2)
        
        MT_closed_summary['SLA'] = MT_closed_summary.apply(lambda row: '24h' if row['hours_closed'] < 24 else '48h' if row['hours_closed'] < 48 else
                                             '72h' if row['hours_closed'] < 72 else '3 to 7 days' if row['hours_closed'] < 168 else 'Over 7 days', axis=1)
        
        MT_closed_summary['SLA2'] = np.where(
            MT_closed_summary['hours_closed'] <= 48, "Yes", "No")
        
        Captured_48h = (MT_closed_summary['SLA2'] == 'Yes').sum()
        
        captured = round((Captured_48h/total)*100, 2)
        
        Panel_backlog = MT_backlog_summary['incidence_number'].nunique()
        
        panel_size = round((Panel_backlog/panel_inst)*100,2)

        ZV_backlog = ZV_backlog_summary['incidence_number'].nunique()

        ZV_size = round((ZV_backlog/ZV_portfolio_no)*100,2)
        
    

        results[range].iloc[105:115] = [total_received, f"{sent_to_field}%",total_received_IE,np.nan,np.nan,np.nan, Panel_backlog,f"{panel_size}%", ZV_backlog, f"{ZV_size}%"]
        results.to_csv(results_dir, index=False)

    calculations(week_start, week_end, 'Week',ZV_portfolio_no,panel_inst)
    calculations(month_start, month_end, 'Month',ZV_portfolio_no,panel_inst)
