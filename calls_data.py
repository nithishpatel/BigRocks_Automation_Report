import pandas as pd
import numpy as np
import datetime
import psycopg2
from datetime import datetime, timedelta

def calls_data_calculation(week_start,week_end,month_start,month_end,results_dir):
        

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

    ops_queue = pd.read_sql('select * from operations.ops_telephony.ops_queues', connection)
    agents_extract = pd.read_sql('select * from operations.workday_details.ops_agents_extract_new', connection)
    ARC_Incidences=pd.read_sql(f'''select * from arc_department.v_arc_alarm_incidences where incidence_creation_date >= '{date_filter}'
                                ''',connection)
    
    print ("1")
    calls_accepted = "select report_date ,report_year ,report_month ,interval_30min, service_received ,accepted_agent ,accepted_under40sec ,abandoned_waiting_higher10sec,accepted_over120sec, calls_queue  from operations.ops_telephony.llam_inbound_times where report_date > '"+date_filter+"'"

    Closed_services= pd.read_sql(f'''select * from ops_field_services.fsm_closed_maintenances
                                where "closing/finishing_date" >= '{date_filter}';''',connection)
    
    Opened_services= pd.read_sql(f'''select * from ops_field_services.fsm_created_maintenances
                                where creating_date >= '{date_filter}';''',connection)
    print ("1")
    Field_Retention= pd.read_sql(f'''select * from big_rocks.bigrocks_field_retentions
                                where "closing/finishing_date" >= '{date_filter}';''',connection)
    
    commlogs = pd.read_sql(f"select * from  commlogs_info.v_commlogs_created_keys where creation_date >= '{date_filter}'",connection)

    workday = pd.read_sql("select * from workday_details.ops_agents_extract_new where formula1='Active'",connection)

    Whatsapp =pd.read_sql(f''' select * from operations.ops_telephony.whatsapp_agent_interactions
                                    where ini_resource_time >= '{date_filter}'
                                    ''',connection)
    FAQ_data =pd.read_sql(f''' select * from big_rocks.big_rocks_faq_data
                                    where date >= '{date_filter}'
                                    ''',connection)
    
    print ("1")
    Whatsapp['Date'] = pd.to_datetime(Whatsapp['ini_resource_time'],format='%Y-%b-%d %I:%M:%S')
    Whatsapp = Whatsapp.dropna(subset=['Date'])
    Whatsapp['Date'] = Whatsapp['Date'].dt.strftime('%Y-%m-%d')
    min_time = Whatsapp.groupby('ixn_id')['ini_resource_time'].transform('min')
    Whatsapp['first_session_flag'] = (Whatsapp['ini_resource_time'] == min_time).astype(int)

    print ("1")
    ARC_Guard_Callouts=pd.read_sql("Select * from arc_department.verisure_callouts",connection)
    ARC_Guard_Callouts['callout_created_at']=pd.to_datetime(ARC_Guard_Callouts['callout_created_at'],errors='coerce',dayfirst=True)
    ARC_Guard_Callouts = ARC_Guard_Callouts.dropna(subset=['callout_created_at'])
    ARC_Guard_Callouts['callout_created_at'] = ARC_Guard_Callouts['callout_created_at'].dt.strftime('%Y-%m-%d')

    Police_callouts = pd.read_sql(f'''select * from big_rocks.arc_police_data where incidence_creation_date >= '{date_filter}';
                                 ''',connection)
    print ("1")
    print ("All tables read!")

    calls = pd.read_sql(calls_accepted, connection)

    chat_interactions_Q = "select report_date ,agent_id , chat_hour,chat_queue ,interaction_id ,flag_rerouted,response_time ,queue_time from operations.ops_telephony.chat_interactions where report_date > '"+date_filter+"'"
    chat_interactions = pd.read_sql(chat_interactions_Q, connection)
    chat_interactions = chat_interactions.assign(under_40_sec = np.where((chat_interactions['response_time'] - chat_interactions['queue_time'] < 40) & (chat_interactions['flag_rerouted'] == 0), 1, 0),
                                                under_60_sec = np.where((chat_interactions['response_time'] - chat_interactions['queue_time'] < 60) & (chat_interactions['flag_rerouted'] == 0), 1, 0),
                                                over_120_sec = np.where((chat_interactions['response_time'] - chat_interactions['queue_time'] > 120) & (chat_interactions['flag_rerouted'] == 0), 1, 0),
                                                over_40_sec = np.where((chat_interactions['response_time'] - chat_interactions['queue_time'] > 40) & (chat_interactions['flag_rerouted'] == 0), 1, 0),
                                                under_180_sec = np.where((chat_interactions['response_time'] - chat_interactions['queue_time'] < 180) & (chat_interactions['flag_rerouted'] == 0), 1, 0))

    email_interactions_Q= "select report_day  ,employee_id , interaction_id ,interaction_duration ,email_response_time ,stop_action , report_queue from operations.ops_telephony.email_interactions where report_day >'"+date_filter+"'"
    email_interactions = pd.read_sql(email_interactions_Q, connection)
    email_interactions = email_interactions.assign(under_12h = np.where((email_interactions['interaction_duration'] < 43200) & (email_interactions['stop_action']==1) , 1, 0),
                                                under_24h = np.where((email_interactions['interaction_duration'] < 86400) & (email_interactions['stop_action']==1), 1, 0),
                                                under_48h = np.where((email_interactions['interaction_duration'] < 172800) & (email_interactions['stop_action']==1), 1, 0))
    print ("1")

    results = pd.read_csv(results_dir)


    calls_mapped = calls.merge(ops_queue, left_on='calls_queue', right_on='queues', how='left')

    # Filter and Summarize
    cs_calls_mapped_summary = calls_mapped[calls_mapped['calls_queue'].isin(['UK_CS_Billing_Target_VQ',
                                                                            'UK_CS_Cancel_Target_VQ','UK_CS_Resolutions_Target_VQ','UK_CS_Complaint_Target_VQ',
                                                                            'UK_CS_Moonshot_Target_VQ', 'UK_CS_Tech_Target_VQ','UK_CS_Unfeasible',
                                                                            'UK_CTS_Transfer_Target_VQ', 'UK_CustomerSupport_Target_VQ',
                                                                            'UK_CustomerSupport_VIP_Target_VQ','UK_IQ_MobileApps','UK_IQ_CS_Moonshot','UK_IQ_CS_IntOffice','UK_IQ_CS_FieldTech','UK_IQ_CS_ENQUIRIES','UK_IQ_CS_DIRECTORSOFFICE','UK_IQ_CS_BRANCH','UK_IQ_CS','UK_ARLO_GENIUS_BAR_Target_VQ','UK_ARLO_GENIUS_BAR'])] \
        .groupby(['report_date', 'report_year', 'report_month', 'calls_queue', 'skills', 'departments', 'media_type', 'manager']) \
        .agg({
            'abandoned_waiting_higher10sec': 'sum',
            'accepted_under40sec': 'sum',
            'accepted_over120sec': 'sum',
            'accepted_agent': 'sum',
            'service_received': 'sum'
        }).reset_index()


    cs_calls_mapped_summary_IE = calls_mapped[calls_mapped['calls_queue'].isin(['IE_CustomerServices_Target_VQ', 'IE_CustomerServices_VIP_Target_VQ'])] \
        .groupby(['report_date', 'report_year', 'report_month', 'calls_queue', 'skills', 'departments', 'media_type', 'manager']) \
        .agg({
            'abandoned_waiting_higher10sec': 'sum',
            'accepted_under40sec': 'sum',
            'accepted_over120sec': 'sum',
            'accepted_agent': 'sum',
            'service_received': 'sum'
        }).reset_index()

    def calculations(x,y,range):

        print (f'Calculating from {x} to {y} for {range}ly data')
        # Filter and Summarize
        cs_summary = cs_calls_mapped_summary[(cs_calls_mapped_summary['report_date'] >= x) &
                                            (cs_calls_mapped_summary['report_date'] <= y)].groupby('report_year').agg({
            'service_received': 'sum',
            'accepted_agent': 'sum',
            'accepted_under40sec': 'sum',
            'abandoned_waiting_higher10sec': 'sum',
            'accepted_over120sec': 'sum'
        }).reset_index()
        cs_summary_IE = cs_calls_mapped_summary_IE[(cs_calls_mapped_summary_IE['report_date'] >= x) &
                                            (cs_calls_mapped_summary_IE['report_date'] <= y)].groupby('report_year').agg({
            'service_received': 'sum',
            'accepted_agent': 'sum',
            'accepted_under40sec': 'sum',
            'abandoned_waiting_higher10sec': 'sum',
            'accepted_over120sec': 'sum'
        }).reset_index()

        # Calculate SLA percentages
        SLA_40_sec = round(cs_summary['accepted_under40sec'] / cs_summary['accepted_agent']*100, 2)[0]
        SLA_40_sec_IE = round(cs_summary_IE['accepted_under40sec'] / cs_summary_IE['accepted_agent']*100, 2)[0]
        SLA_120_sec = round(cs_summary['accepted_over120sec'] / cs_summary['accepted_agent']*100, 2)[0]
        # cs_summary['SLA_120_sec'] = (cs_summary['accepted_over120sec'] / cs_summary['accepted_agent']).apply(lambda x: f"{Fraction(x):.2%}")
        SLA_abandonment = round(cs_summary['abandoned_waiting_higher10sec'] / cs_summary['service_received'] * 100, 2)[0]

        # Print results
        total_calls_answered = cs_summary['accepted_agent'].sum()
        total_calls_answered_IE = cs_summary_IE['accepted_agent'].sum()
        print(f"Total calls answered: {total_calls_answered}, Less than 40sec SLA: {SLA_40_sec}, Less than 120sec SLA: {SLA_120_sec} ,Abandonment rate: {SLA_abandonment}")
        results[range].iloc[26:32] = [total_calls_answered, f"{SLA_40_sec}%",f"{SLA_120_sec}%",  f"{SLA_abandonment}%", total_calls_answered_IE,f"{SLA_40_sec_IE}%"]


        commlogs_filtered = commlogs[(commlogs['creation_source'].isin(['CS', 'CUDE', 'CUSV', 'ATT'])) & 
                            (~commlogs['creation_id'].isin(['Automatic', 'BUSSOA', 'COMMLOG', 'IBS'])) &  
                            (commlogs['creation_media'].isin(['   ', 'CHA', 'EMA', 'PHO', 'TEL', 'WAP'])) & 
                              (commlogs['code_key_three'] != 'GDPR')]

        commlogs_filtered['Custom'] = np.where((commlogs_filtered['code_key_one'].isna()) & (commlogs_filtered['code_key_two'].isna()) & (commlogs_filtered['code_key_three'].isna()), 1, 0)

        commlogs_filtered = commlogs_filtered[(commlogs_filtered['Custom'] == 0) & (commlogs_filtered['creation_direction']=='Incoming')]

        date_str = y  # Date in DD-MM-YYYY format
        date_format = "%Y-%m-%d"
        date_obj = datetime.strptime(date_str, date_format)
        date_30 = date_obj - timedelta(days=30)

        df = commlogs_filtered[commlogs_filtered['country']=='UK']
        df = df[(df['creation_date'] >=date_30) & (df['creation_date']<=y)]
        df['date_plus30'] = df['creation_date_hour'] + pd.Timedelta(days=30)
        
        df['creation_date_hour'] = pd.to_datetime(df['creation_date_hour'],format='%d/%m/%Y %I:%M:%S')

        # Function to calculate the flag for duplicates
        def flag_duplicates(row):
            # Reference values for the current row
            ref_ins_no = row['installation_number']
            ref_date = row['creation_date_hour']
            ref_date2 = ref_date - pd.Timedelta(minutes=10)  # Equivalent to subtracting 1/144 days

            # Filter dataframe to find max comm_log_number based on conditions
            commlogmax = df.loc[
                (df['installation_number'] == ref_ins_no) &
                (df['creation_date_hour'] <= ref_date) &
                (df['creation_date_hour'] > ref_date2),
                'comm_log_number'
            ].max()

            # Return 1 if comm_log_number equals commlogmax, otherwise 0
            return 1 if row['comm_log_number'] == commlogmax else 0

        # Apply the function to each row to create the Flag Duplicates column
        df['Flag_Duplicates'] = df.apply(flag_duplicates, axis=1)
        def calculate_fcr_flag_optimized(df):
            # Ensure 'date_plus30' and 'creation_date_hour' are in datetime format
            df['date_plus30'] = pd.to_datetime(df['date_plus30'])
            df['creation_date_hour'] = pd.to_datetime(df['creation_date_hour'])

            # Sort the DataFrame to ensure proper comparison
            df = df.sort_values(by=['installation_number', 'creation_date_hour'])

            # Initialize FCR_Flag as 0
            df['FCR_Flag'] = 0

            # Handle NaNs in code_key_two and code_key_three
            df['code_key_two'] = df['code_key_two'].fillna('')
            df['code_key_three'] = df['code_key_three'].fillna('')

            # Create an empty listz to store counts
            fcr_flag_counts = []

            # Iterate over each unique customer group
            for customer, group in df.groupby('installation_number'):
                # Iterate over each row in the customer group
                for idx, row in group.iterrows():
                    # Create a mask to filter rows that match the conditions for FCR Flag
                    mask = (
                        (group['creation_date_hour'] <= row['date_plus30']) &
                        (group['creation_date_hour'] >= row['creation_date_hour']) &
                        (group['code_key_two'] == row['code_key_two']) &
                        (group['code_key_three'] == row['code_key_three']) &
                        (group['Flag_Duplicates'] == 1)
                    )
                    # Count how many rows match the conditions
                    count = mask.sum()
                    fcr_flag_counts.append((idx, count))

            # Assign the calculated counts back to the DataFrame
            for idx, count in fcr_flag_counts:
                df.at[idx, 'FCR_Flag'] = count

            return df

        # Example usage:
        # Assuming df is your DataFrame with appropriate columns and types
        df = calculate_fcr_flag_optimized(df)

        def calculate_fcr(fcr_flag):
            if fcr_flag > 1:
                return 0
            elif fcr_flag == 0:
                return None 
            else:
                return 1


        df['FCR'] = df['FCR_Flag'].apply(calculate_fcr)

        def calculate_rc_flag(row):
            _date_plus30 = row['date_plus30']
            _date = row['creation_date_hour']
            _customer = row['installation_number']
            
            # Filter the DataFrame based on the row-specific conditions
            count = df[
                (df['installation_number'] == _customer) &
                (df['creation_date_hour'] <= _date_plus30) &
                (df['creation_date_hour'] >= _date) &
                (df['Flag_Duplicates'] == 1)
            ].shape[0]  # Counting rows that match the criteria

            return count

        # Create the RC_Flag column using the apply function
        df['RC_Flag'] = df.apply(calculate_rc_flag, axis=1)
        df['RC'] = df['RC_Flag'].apply(lambda x: 1 if x >= 3 else 0)
       

        workday_f = workday[workday['supervisory_organization'].str.contains('Customer Service Team',case=False,na=False)]
        workday_f = workday_f[workday_f['supervisory_organization'].str.contains('Brook Geddes',case=False,na=False)]
        workday_f = workday_f[workday_f["worker's_manager"]!='Malcolm Bell']
        workday_f = workday_f[~workday_f['supervisory_organization'].str.contains('Rehema Ayiro',case=False,na=False)]

        df= df.merge(workday_f, left_on='creation_id',right_on='employee_id',how='left',suffixes=('', '_workday'))

        distinct_count_rc_1 = df[df['RC']==1]['installation_number'].nunique()
        total_distinct_count = df['installation_number'].nunique()
        percentage_rc = round((distinct_count_rc_1 / total_distinct_count if total_distinct_count != 0 else 0)*100,2)
        sum_fcr = df['FCR'].sum()
        count_fcr = df['FCR'].notna().sum()
        percentage_fcr = round((sum_fcr / count_fcr)*100,2)

        results[range].iloc[44:46] = [f"{percentage_fcr}%",f"{percentage_rc}%"]

        #-------- Tech Support ----------

        # Filter and Summarize for Technical Support Global
        ts_calls_mapped_summary = calls_mapped[(calls_mapped['departments'] == 'Technical Support Global') &
                                            (calls_mapped['calls_queue'] != 'UK_TECH_QUEUE_Target_VQ')].groupby(
            ['report_date', 'report_year', 'report_month', 'calls_queue', 'skills', 'departments', 'media_type', 'manager']
        ).agg({
            'abandoned_waiting_higher10sec': 'sum',
            'accepted_under40sec': 'sum',
            'accepted_agent': 'sum',
            'service_received': 'sum'
        }).reset_index()

        # Change the date based on the week and month
        ts_summary = ts_calls_mapped_summary[(ts_calls_mapped_summary['report_date'] >= x) &
                                            (ts_calls_mapped_summary['report_date'] <= y)].groupby(
            'report_year').agg({
            'service_received': 'sum',
            'accepted_agent': 'sum',
            'accepted_under40sec': 'sum',
            'abandoned_waiting_higher10sec': 'sum'
        }).reset_index()

        FAQ_data['date'] = pd.to_datetime(FAQ_data['date'])
        FAQ_data_df= FAQ_data[(FAQ_data['date']>= x) & (FAQ_data['date'] <= y)]

        FAQ_data_df=FAQ_data_df[~FAQ_data_df['created'].isna()]
        FAQ_data_df = FAQ_data_df[FAQ_data_df['fse_installer'].isin(['FSE','Installer'])]
        # Calculate SLA percentages
        SLA_40_sec= round(ts_summary['accepted_under40sec'] / ts_summary['accepted_agent']*100 ,2)[0]
        SLA_abandonment = round(ts_summary['abandoned_waiting_higher10sec'] / ts_summary['service_received']*100 ,2)[0]

        # Print results
        total_calls_answered_ts = ts_summary['accepted_agent'].sum()
        
        adherence= round(FAQ_data_df['not_faqable'].count()/(FAQ_data_df['faqable_'].count() + FAQ_data_df['not_faqable'].count())*100,2)

        results[range].iloc[116:122] = [total_calls_answered_ts,np.nan, f"{SLA_40_sec}%",  f"{SLA_abandonment}%",np.nan,f"{adherence}%"]

        # Mapping the agent ids
        chat_interactions['agent_id'] = chat_interactions['agent_id'].astype(str)

        # Left Join
        chat_interactions_mapped = chat_interactions.merge(agents_extract, left_on='agent_id', right_on='employee_id', how='left')

        # Filter and Summarize
        chats_summary = chat_interactions_mapped[(chat_interactions_mapped['chat_queue'].isin(['UK_IQ_CH_TRANSFER_SUPP', 'UK_ChatBotsGo-3-RouteQueue'])) &
                                                (chat_interactions_mapped['report_date'] >= x) &
                                                (chat_interactions_mapped['report_date'] <= y)]

    
        total_chats	 = chats_summary['interaction_id'][chats_summary['flag_rerouted'] == 0].nunique()
        # under_60_sec_sum = chats_summary['under_60_sec'].sum()
        # under_60_sec_count = chats_summary['interaction_id'][chats_summary['under_60_sec'] == 1].count()
        under_60_sec_nunique = chats_summary['interaction_id'][chats_summary['under_60_sec'] == 1].nunique()
        # over_120_sec_sum = chats_summary['over_120_sec'].sum()
        over_120_sec_nunique = chats_summary['interaction_id'][chats_summary['over_120_sec'] == 1].nunique()


        SLA_60_sec = round(under_60_sec_nunique/total_chats * 100, 2)
        SLA_120_sec = round(over_120_sec_nunique / total_chats	 * 100, 2)

        results[range].iloc[33:36] = [total_chats, f"{SLA_60_sec}%",  f"{SLA_120_sec}%" ]

        # Assuming chat_interactions_mapped, email_interactions, agents_extract, ops_queue are DataFrames in Python

        # WhatsApp Summary
        
        whatsapp_summary = Whatsapp[(Whatsapp['Date'] >= x) &
                                                    (Whatsapp['Date'] <= y )]
        whatsapp_summary['queue_time']=whatsapp_summary['mediation_time']-whatsapp_summary['queue/workbin_duration']
        whatsapp_summary['under_180_sec']=whatsapp_summary['queue_time']<=180

        whatsapp_summary = whatsapp_summary[whatsapp_summary['queue/workbin'].isin(["UK_IQ_WAPP_MAIN", "UK_IQ_WAPP_NEW_ROUTE", "UK_IQ_WAPP_OOH", "UK_IQ_WAPP_RETURN","UK_IQ_WAPP_ROUTE", "UK_IQ_WAPP_SUPERVISOR", "UK_IQ_WAPP_TRANSFER", "UK_IQ_WAPP_VIP","UK_IQ_WAPP_VIP_OOH", "UK_IQ_WAPP_WB_OOH", "UK_IQ_WAPP_WB_ROUTE", "UK_WB_PERSONAL","UK_WB_WAPP_Personal", "WB_ATC_Personal","UK_ARLO_GENIUS_BAR_Target_VQ","UK_ARLO_GENIUS_BAR"])]
        Total_whatsapps = whatsapp_summary['ixn_id'].nunique()
        filtered_df = whatsapp_summary[whatsapp_summary['queue/workbin'].isin(['UK_IQ_WAPP_ROUTE', 'UK_IQ_WAPP_NEW_ROUTE'])]

    
    # Calculate the sum of 'under_180_sec' where 'first_session_flag' is 1
        sum_under_180_sec = filtered_df[filtered_df['first_session_flag'] == 1]['under_180_sec'].sum()
    
    # Calculate the distinct count of interaction IDs
        distinct_count_ixn_id = filtered_df['ixn_id'].nunique()
    
    # Calculate WA SLA 180s
        wa_sla_180s = round((sum_under_180_sec / distinct_count_ixn_id if distinct_count_ixn_id > 0 else 0)*100,2)

        results[range].iloc[37:39] = [Total_whatsapps,f"{wa_sla_180s}%"]

        print ("chats and calls data processed")

       #-------- Alarm Receiving Centre ----------
        ARC_Incidence = ARC_Incidences.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        ARC_Incidence = ARC_Incidence[~(((ARC_Incidence['incidence_type'].astype(str) == 'TA') & (ARC_Incidence['cancellation_type'].astype(str) == 'CONFIA') & (ARC_Incidence['cancellation_subtype'].astype(str).isin(['INTIME', 'TAMRES'])))) & (ARC_Incidence['cancellation_subtype'].astype(str) != 'RETREP')]

        arc_incidences_summary=ARC_Incidence[(ARC_Incidence['incidence_creation_date']>= x) & (ARC_Incidence['incidence_creation_date']<= y)]
        
        arc_incidences_summary_IE=ARC_Incidence[(ARC_Incidence['incidence_creation_date']>= x) & (ARC_Incidence['incidence_creation_date']<= y) & (ARC_Incidence['country']=='IRELAND')]
        arc_incidences_summary = arc_incidences_summary[arc_incidences_summary['country'] == 'UK']
        Total_Incidences = arc_incidences_summary['incidence_number'].count()
        Total_Incidences_IE = arc_incidences_summary_IE[arc_incidences_summary_IE['country'] == 'IRELAND']['incidence_number'].count()

        # Total_Incidences=arc_incidences_summary['incidence_number'].count()
        vericon_incidences = arc_incidences_summary[arc_incidences_summary['cancellation_type'].isin(['CONFIA', 'CONFORT'])]
        vericon_incidences_IE = arc_incidences_summary_IE[arc_incidences_summary_IE['cancellation_type'].isin(['CONFIA', 'CONFORT'])]
        managed_by_vericon = vericon_incidences['incidence_number'].count()
        managed_by_vericon_IE = vericon_incidences_IE['incidence_number'].count()
        Total_Incidences_agent = Total_Incidences-managed_by_vericon
        Total_Incidences_agent_IE = Total_Incidences_IE - managed_by_vericon_IE
        # print(Total_Incidences_agent_IE)
        vericon = round((managed_by_vericon/Total_Incidences)*100,2)
        arc_incidences_summary['tm_attended'].fillna(0,inplace=True)
        arc_incidences_summary['tm_attended'] = arc_incidences_summary['tm_attended'].astype('int')
        less_than_60 = len(arc_incidences_summary[arc_incidences_summary['tm_attended']<60])
        less_than_60_IE = len(arc_incidences_summary_IE[arc_incidences_summary_IE['tm_attended']<60])
        greater_than_180 = len(arc_incidences_summary[arc_incidences_summary['tm_attended']>180])
        greater_than_180_IE = len(arc_incidences_summary_IE[arc_incidences_summary_IE['tm_attended']>180])
        Incidents_response_time_less_than_60 = round((less_than_60/Total_Incidences)*100,2)
        Incidents_response_time_less_than_60_IE = round((less_than_60_IE/Total_Incidences_IE)*100,2)
        Incidents_response_time_greater_than_180 = round((greater_than_180/Total_Incidences)*100,2)
        Incidents_response_time_greater_than_180_IE = round((greater_than_180_IE/Total_Incidences_IE)*100,2)

        arc_incidences_summary = arc_incidences_summary[arc_incidences_summary['panel'].isin(['SDVECU','SDVFAST'])]
        

        arc_incidences_summary['Repeat Alarm'] = 0
        arc_incidences_counts = arc_incidences_summary.groupby(['installation_number', 'incidence_creation_date']).size().reset_index(name='count')
        arc_incidences_counts['Repeat_alarm']=arc_incidences_counts['count'].apply(lambda x: 1 if x >= 3 else 0)

        customers_with_repeat_alarm = arc_incidences_counts[arc_incidences_counts['Repeat_alarm'] == 1]['installation_number'].nunique()
        total_customers = arc_incidences_counts['installation_number'].nunique()

        repeat_alarm_percentage = (customers_with_repeat_alarm / total_customers) * 100

        results[range].iloc[6:16] = [Total_Incidences,Total_Incidences_agent,f"{vericon}%",Total_Incidences_IE,Total_Incidences_agent_IE,np.nan,f"{Incidents_response_time_less_than_60}%",f"{Incidents_response_time_greater_than_180}%",f"{Incidents_response_time_less_than_60_IE}%",f"{Incidents_response_time_greater_than_180_IE}%"]
        results[range].iloc[24:25] = [f"{repeat_alarm_percentage}%"]
        # Filter and Summarize for Alarm Receiving Centre calls
        arc_calls_mapped_summary = calls_mapped[calls_mapped['calls_queue'].isin(['UK_ARC_Target_VQ','UK_ARC_GR2','UK_ARC_GR2_Target_VQ','UK_ARC_GR1','UK_ARC_GR1_Target_VQ'])].groupby(
            ['report_date', 'report_year', 'report_month', 'calls_queue', 'skills', 'departments', 'media_type', 'manager']
        ).agg({
            'accepted_under40sec' : 'sum',
            'accepted_agent' : 'sum'
        }).reset_index()

        # Change the date based on the week and month
        arc_summary = arc_calls_mapped_summary[(arc_calls_mapped_summary['report_date'] >= x) &
                                            (arc_calls_mapped_summary['report_date'] <= y)].groupby(
            'report_year').agg({
            'accepted_agent': 'sum',
            'accepted_under40sec': 'sum'
        }).reset_index()
        # Calculate SLA percentages
        SLA_40_sec_arc= round(arc_summary['accepted_under40sec'] / arc_summary['accepted_agent']*100 ,2)[0]
        total_inbound_calls = arc_summary['accepted_agent'].sum()
        results[range].iloc[17:19] = [total_inbound_calls, f"{SLA_40_sec_arc}%" ]

        #-------- Guard Callouts ----------
        arc_guard_callout_summary=ARC_Guard_Callouts[((ARC_Guard_Callouts['callout_created_at']) >= x) & ((ARC_Guard_Callouts['callout_created_at']) <= y)]
        Guard_Callouts = arc_guard_callout_summary[arc_guard_callout_summary['responder_dispatch_completed'] == True]
        Callout_guards=Guard_Callouts['callout_id'].count()
        Guard_Callouts['guard_40_mins'] = np.where((Guard_Callouts['avg_true_resp_time'] < 2400) & (Guard_Callouts['avg_true_resp_time'] > 0), 1, 0)
        filtered_df_guard = Guard_Callouts[Guard_Callouts['avg_true_resp_time'].notna()]

        # Calculate numerator and denominator
        numerator = filtered_df_guard['guard_40_mins'].sum()
        denominator = filtered_df_guard['callout_id'].count()

        # Safe division: avoid division by zero
        g_less_than_40_pct = (numerator / denominator if denominator != 0 else 0)*100
        #-------- Police Callouts ----------
        Police_callouts_summary = Police_callouts[(Police_callouts['incidence_creation_date'] >= x ) & (Police_callouts['incidence_creation_date'] <= y) ]
        Police_callouts_summary = Police_callouts_summary[Police_callouts_summary['country']=='UK']

        Police_callouts_summary['total_incidences_with_police_(net)'] = Police_callouts_summary['total_incidences_with_police_(net)'].fillna(0)
        Total_Police_callouts=Police_callouts_summary['total_incidences_with_police_(net)'].astype(int).sum()
        results[range].iloc[20:23] = [Callout_guards, f"{g_less_than_40_pct}%",Total_Police_callouts]

        #-------- Field Services ----------
        Opened_services_summary=Opened_services[(Opened_services['creating_date']>= x ) & (Opened_services['creating_date']<= y)]
        Closed_services_summary=Closed_services[(Closed_services['closing/finishing_date']>= x ) & (Closed_services['closing/finishing_date']<= y)]
        Opened_services_summary['Warranty_Period'] = Opened_services_summary['installation_cost_center_code'].str.strip().apply(
            lambda x: 0 if x == "UK270" else (30 if x in ["UK191", "UK192", "UK197", "UK198", "UK320", "UK330"] else 90))
        
        Opened_services_summary['Warranty_filter'] = Opened_services_summary.apply(lambda row: 'BRANCH' if row['Warranty_Period'] >= row['days_install_to_creation'] else 'FIELD', axis=1)
        Opened_services_summary=Opened_services_summary[Opened_services_summary['Warranty_filter']=='FIELD']
        Opened_services_summary_IE= Opened_services_summary[Opened_services_summary['installation_number'].astype(str).str.startswith('7')]
        Opened_services_summary= Opened_services_summary[~Opened_services_summary['installation_number'].astype(str).str.startswith('7')]
        Opened_services_summary=Opened_services_summary[Opened_services_summary['call_type_code']!='100']
        Opened_services_count= Opened_services_summary['code_maintenance_ibs'].nunique()
        
        Opened_services_summary_IE=Opened_services_summary_IE[Opened_services_summary_IE['call_type_code']!='100']
        Opened_services_count_IE= Opened_services_summary_IE['code_maintenance_ibs'].nunique()

        #Opened_services_summary=Opened_services_summary[(Opened_services_summary[Installation Number]!=0)&(Opened_services_summary['Warranty filter'])]
        #Opened_services_count= Opened_services_summary['code_maintenance_ibs'].nunique()
        # Replace NaN values with 0 and convert to string, removing unnecessary periods and spaces
        Closed_services_summary['technician_line_code'] = Closed_services_summary['technician_line_code'].fillna(0).astype(int).astype(str).str.replace('.', '', regex=False).str.strip()
# Filter the DataFrame based on the given conditions
        Closed_services_summary_IE = Closed_services_summary[
            (Closed_services_summary['status_maintenance'] == 'FINL') &
            (Closed_services_summary['installation_number'] != '0') &
            (Closed_services_summary['installation_number'].astype(str).str.startswith('7')) &
            (Closed_services_summary['technician_line_code'].isin(['230020', '270009', '270019', '270023', '270030','270013','270017','270020', '270024','270016','270025']))]


        Closed_services_summary = Closed_services_summary[
            (Closed_services_summary['status_maintenance'] == 'FINL') &
            (Closed_services_summary['installation_number'] != '0') &
            (~Closed_services_summary['installation_number'].astype(str).str.startswith('7')) &
            (Closed_services_summary['technician_line_code'].isin(['230020', '270009', '270019', '270023', '270030','270013','270017','270020', '270024','270016','270025']))]


                
        Closed_services_summary['creating_date'] = pd.to_datetime(Closed_services_summary['creating_date'])
        Closed_services_summary['closing/finishing_date'] = pd.to_datetime(Closed_services_summary['closing/finishing_date'])
        Closed_services_summary_IE['creating_date'] = pd.to_datetime(Closed_services_summary_IE['creating_date'])
        Closed_services_summary_IE['closing/finishing_date'] = pd.to_datetime(Closed_services_summary_IE['closing/finishing_date'])
        Closed_services_summary['Days']=(Closed_services_summary['closing/finishing_date']-Closed_services_summary['creating_date']).dt.days
        Closed_services_summary_IE['Days']=(Closed_services_summary_IE['closing/finishing_date']-Closed_services_summary_IE['creating_date']).dt.days
        Aware_services = Closed_services_summary[Closed_services_summary['req_by']!='GTI']
        Aware_services_IE = Closed_services_summary_IE[Closed_services_summary_IE['req_by']!='GTI']
        Aware_count = Closed_services_summary[Closed_services_summary['req_by']!='GTI']['code_maintenance_ibs'].nunique()
        Aware_services_SLA=Aware_services[~Aware_services['activity_type'].isin(['Retention oppertunity','Installations','Moonshot Upgrade','System Dismantle'])]
        Aware_services_SLA_IE=Aware_services_IE[~Aware_services_IE['activity_type'].isin(['Retention oppertunity','Installations','Moonshot Upgrade','System Dismantle'])]
        Total_Aware_services_SLA=Aware_services_SLA['code_maintenance_ibs'].nunique()
        Total_Aware_services_SLA_IE=Aware_services_SLA_IE['code_maintenance_ibs'].nunique()
        SLA_7days= Aware_services_SLA[Aware_services_SLA['Days']<=7]['code_maintenance_ibs'].nunique()
        SLA_7days_IE= Aware_services_SLA_IE[Aware_services_SLA_IE['Days']<=7]['code_maintenance_ibs'].nunique()
        SLA_7days_percent=round((SLA_7days/Total_Aware_services_SLA)*100,2)
        SLA_7days_percent_IE=round((SLA_7days_IE/Total_Aware_services_SLA_IE)*100,2)
        Not_Aware_services = Closed_services_summary[Closed_services_summary['req_by']=='GTI']
        Not_Aware_count = Closed_services_summary[Closed_services_summary['req_by']=='GTI']['code_maintenance_ibs'].nunique()
        Not_Aware_services_SLA=Not_Aware_services[~Not_Aware_services['activity_type'].isin(['Retention oppertunity','Installations','Moonshot Upgrade','System Dismantle'])]
        
        Total_Not_Aware_services_SLA=Not_Aware_services_SLA['code_maintenance_ibs'].nunique()
        Not_SLA_7days= Not_Aware_services_SLA[Not_Aware_services_SLA['Days']<=30]['code_maintenance_ibs'].nunique()
        Not_SLA_30day_percent=round((Not_SLA_7days/Total_Not_Aware_services_SLA)*100,2)
        Finalized= Aware_count+Not_Aware_count
        results[range].iloc[47:56] = [Opened_services_count,Finalized,Aware_count,Not_Aware_count,Opened_services_count_IE,np.nan,f"{SLA_7days_percent}%",f"{Not_SLA_30day_percent}%",f"{SLA_7days_percent_IE}%"]

    #-------- Customer Care ----------
        Field_Retention['closing/finishing_date'] = pd.to_datetime(Field_Retention['closing/finishing_date'],errors='coerce')
        Field_Retention_summary=Field_Retention[(Field_Retention['closing/finishing_date']>= x ) & (Field_Retention['closing/finishing_date']<= y)]

        Field_Retention_summary = Field_Retention_summary[(Field_Retention_summary['status_maintenance'] == 'FINL') &
                                                            (Field_Retention_summary['technician_identifier'].isin([
                                                                '306047', '256784', '301674', '301660', '291828', '230952','246860', '259177', '264919', '226979', '219321', '262885', 'RC4980'
                                                            ]))]
                                                        


        Field_Retention_workreti = Field_Retention_summary.copy()
        Field_Retention_workreti['RETI takeover']=Field_Retention_summary.apply(lambda row:'RETI takeover' if row['customer_retained']=='NOT RETAINED' and row['reti']=='RETI TAKEOVER' else None,axis=1)
        #Field_Retention_workreti=Field_Retention_workreti.drop_duplicates('code_maintenance_ibs',keep='first')
                                                                          
        Field_Retention_retained = Field_Retention_summary.copy()
        Field_Retention_retained['Retained'] = Field_Retention_summary.apply(lambda row:'Retained' if row['customer_retained']=='RETAINED' else None,axis=1)
        #Field_Retention_retained=Field_Retention_retained.drop_duplicates('code_maintenance_ibs',keep='first')

        Field_Retention_pending = Field_Retention_summary.copy()
        Field_Retention_pending['pending']=Field_Retention_summary.apply(lambda row:'Pending' if row['customer_retained']=='PENDING' else None,axis=1)
        #Field_Retention_pending=Field_Retention_pending.drop_duplicates('code_maintenance_ibs',keep='first')

        Field_Retention_retained = Field_Retention_retained[['code_maintenance_ibs', 'Retained']]
        Field_Retention_workreti = Field_Retention_workreti[['code_maintenance_ibs', 'RETI takeover']]
        Field_Retention_pending = Field_Retention_pending[['code_maintenance_ibs', 'pending']]

        merged_df1 = Field_Retention_summary.merge(Field_Retention_retained,on='code_maintenance_ibs',how='left')
        merged_df2 = merged_df1.merge(Field_Retention_workreti,on='code_maintenance_ibs',how='left')
        merged_df3 = merged_df2.merge(Field_Retention_pending,on='code_maintenance_ibs',how='left')

        merged_df3['Customer Retention']=merged_df3.apply(lambda row:'Retained' if row['Retained']=='Retained' else
                                                                            'Retained' if row['RETI takeover']=='RETI takeover' else
                                                                            'Pending' if row['pending']=='Pending' else
                                                                            'Not Retained',axis=1)
        merged_df3 = merged_df3[merged_df3['Customer Retention']!='Pending']
        merged_df3_IE = merged_df3[merged_df3['installation_number'].astype(str).str.startswith('7')]
        Field_Retention_summary = Field_Retention_summary[Field_Retention_summary['customer_retained'] != 'PENDING']
        Field_Retention_summary_IE = Field_Retention_summary[Field_Retention_summary['installation_number'].astype(str).str.startswith('7')]
        Retained = merged_df3[merged_df3['Customer Retention']=='Retained']['code_maintenance_ibs'].nunique()
        Retained_IE = merged_df3_IE[
            (merged_df3_IE['Customer Retention'] == 'Retained') &
            (merged_df3_IE['installation_number'].astype(str).str.startswith('7'))]['code_maintenance_ibs'].nunique()
        # total_merged = merged_df3.drop_duplicates('code_maintenance_ibs')
        # Not_Retained=Field_Retention_summary[Field_Retention_summary['customer_retained']=='Not Retained']['installation_number'].nunique()
        total = merged_df3['installation_number'].nunique()
        total_IE = merged_df3_IE['installation_number'].nunique()
        Field_Retention_rate = round((Retained/total)*100,2)
        #Retention_rate_IE = round((Retained_IE/total_IE)*100,2)
        results[range].iloc[88:89] = [f"{Field_Retention_rate}%"]
        #results[range].iloc[82:83] = [f"{Retention_rate_IE}%"]
        results.to_csv(results_dir, index= False)

    calculations(week_start,week_end,'Week')
    calculations(month_start,month_end,'Month')
