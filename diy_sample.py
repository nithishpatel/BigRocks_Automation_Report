import pandas as pd
import numpy as np

import warnings
warnings.simplefilter("ignore")

x = '2024-01-01'
y = '2024-01-31'

serv_batteries_finalised = pd.read_excel("./DIY__SBN_services__V2.xlsx", 
                            skiprows = 1, usecols=lambda x: 'Unnamed: 0' not in x )

batteries_sent = pd.read_excel("./DIY_FSM_Services_Booked_-BIG_ROCKS_KPIS.xlsx", 
                                skiprows = 1, usecols=lambda x: 'Unnamed: 0' not in x )

Promoted_to_Field = pd.read_excel("./Pre_DIY_Offering_&_Chasing_&_Finalised_Updated(1).xlsx", 
                                sheet_name='Promoted to Field BR', skiprows = 3, usecols=lambda x: 'Unnamed: 0' not in x )

Services_finalised = pd.read_excel("./Pre_DIY_Offering_&_Chasing_&_Finalised_Updated(1).xlsx", 
                            sheet_name='Services finalised BR', skiprows = 3, usecols=lambda x: 'Unnamed: 0' not in x )


Promoted_to_Field.columns = batteries_sent.columns.values
batteries_sent = pd.concat([batteries_sent,Promoted_to_Field])

batteries_sent_non_duplicate_contract = (
    batteries_sent.groupby('Número Contrato')
    .agg({'Número Aviso': 'max'})
    .reset_index()
    .merge(batteries_sent)
)

batteries_sent2 = batteries_sent_non_duplicate_contract

Services_finalised.columns = serv_batteries_finalised.columns.values
serv_batteries_finalised = pd.concat([serv_batteries_finalised,Services_finalised])

serv_batteries_finalised['Closing/Finishing Date'] = pd.to_datetime(serv_batteries_finalised['Closing/Finishing Date'])
serv_batteries_finalised['Creating Date'] = pd.to_datetime(serv_batteries_finalised['Creating Date'])

# Calculate days_to_close
serv_batteries_finalised['days_to_close'] = (serv_batteries_finalised['Closing/Finishing Date'] - serv_batteries_finalised['Creating Date']).dt.days

# serv_batteries_finalised = serv_batteries_finalised[serv_batteries_finalised['Sub Type Description'] == 'N']
# Create new columns based on conditions

filtered_df = serv_batteries_finalised[
    (serv_batteries_finalised['Closing/Finishing Date'] >= x) &
    (serv_batteries_finalised['Closing/Finishing Date'] <= y)
]

# Filter rows based on date conditions
filtered_batteries_sent = batteries_sent2[
    (batteries_sent2['Fecha_Cierre_Aviso'] >= x) &
    (batteries_sent2['Fecha_Cierre_Aviso'] <= y)
]

# Calculate summary_services_to_field
summary_services_to_field = (
    pd.concat([
        filtered_batteries_sent.agg(services_to_field_service=('Número Aviso', 'nunique')).reset_index(),
        filtered_df.agg(total_diy_maintainances=('Code Maintenance IBS', 'nunique')).reset_index()
    ], axis=1).reset_index()
)
total_service = int(summary_services_to_field['Número Aviso'] + summary_services_to_field['Code Maintenance IBS'])

print (total_service,summary_services_to_field['Número Aviso'][0], f"{round(summary_services_to_field['Número Aviso'][0] / total_service *100, 2)}%")