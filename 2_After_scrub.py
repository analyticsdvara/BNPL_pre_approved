import pandas as pd
import numpy as np
import numpy_financial as npf
from datetime import datetime
import datetime as dt
from builder import BNPL
bnpl = BNPL()

# input parameters
curr_dte = dt.date(2023, 10, 31)
to_date = curr_dte.strftime('%Y-%m-%d')
fom_date = curr_dte.strftime('%Y-%m-01')
timestamp = curr_dte.strftime('%Y%m')
product = 'jlg_topup'
# --------

scrub_csv_file_name = "2_Dvara_Custom_Combo_Output766.csv"
BNPL_file_name = "Boon_Box_Eligible_Nov23.xlsx"

scrub_data = bnpl.scrub_from_csv_to_excel(scrub_csv_file_name)
jlg_topup_loan = pd.read_excel(f'{BNPL_file_name}', dtype={'urn':'str', 'Mobile_Number':'str'})
final = pd.merge(jlg_topup_loan, scrub_data, how='inner', left_on='Mobile_Number', right_on='LOS_APP_ID')
print('Data imported...')

# change datatype
final['TOTAL_LAST_2YEAR_WRITE_OFF_AMT'] = final['TOTAL_LAST_2YEAR_WRITE_OFF_AMT'].fillna(0)
final['TOTAL_BEFORE_2YEAR_WRITE_OFF_AMT'] = final['TOTAL_BEFORE_2YEAR_WRITE_OFF_AMT'].fillna(0)
final['TOTAL_OVERDUE_AMT'] = final['TOTAL_OVERDUE_AMT'].fillna(0)
final['TOTAL_LAST_2YEAR_WRITE_OFF_AMT'] = final['TOTAL_LAST_2YEAR_WRITE_OFF_AMT'].astype(int)
final['TOTAL_BEFORE_2YEAR_WRITE_OFF_AMT'] = final['TOTAL_BEFORE_2YEAR_WRITE_OFF_AMT'].astype(int)
final['TOTAL_OVERDUE_AMT'] = final['TOTAL_OVERDUE_AMT'].astype(int)
print('Datatype correction is completed....')
final.to_excel('sample.xlsx')
print(final.dtypes)

# Manupulation
final = final[final['TOTAL_OVERDUE_AMT'] == 0].reset_index(drop=True)
final = final[final['TOTAL_LAST_2YEAR_WRITE_OFF_AMT'] == 0].reset_index(drop=True)
final = final[final['TOTAL_BEFORE_2YEAR_WRITE_OFF_AMT'] <= 5000].reset_index(drop=True)
print('Manupulation is completed....')
final.to_excel('after_scrub.xlsx')

#
# # calculations
#     # ROI
# conditions = [
#     final['risk_grade'] == 'Low Risk',
#     final['risk_grade'] == 'Medium Risk',
#     final['risk_grade'] == 'High Risk']
# choices = [24.5, 26, 27.5]
# final['Top up ROI'] = np.select(conditions, choices)
# print('Calculation ROI is completed....')
#
#     # Base Top up amount
# final['Base Top up amount'] = final['SanctionedAmount'].apply(lambda x: 20000 if (x * 0.4) > 20000 else (x * 0.4))
# print('Calaculation "Base Top up amount" completed....')
#     # FINAL Top up amount as per Risk Grade
# final['FINAL Top up amount as per Risk Grade'] = final.apply(lambda row: row['Base Top up amount'] * 1.15 if row['risk_grade'] == 'Low Risk' else
#                        (row['Base Top up amount'] * 1.10 if row['risk_grade'] == 'Medium Risk' else row['Base Top up amount']), axis=1)
#
# final['FINAL Top up amount as per Risk Grade'] = final['FINAL Top up amount as per Risk Grade'].apply(lambda x: 10000 if x < 10000 else x)
#
# print('Calculation "FINAL Top up amount as per Risk Grade" is completed...')
#
#     # Processing Fee + GST
# final['Processing Fee + GST'] = final['FINAL Top up amount as per Risk Grade'] * (1.18/100)
# print('Calclation "Processing Fee + GST" is completed...')
#
# print(final.dtypes)
#     # Top up EMI for 6 month tenure
# final['Top up EMI for 6 month tenure'] = npf.pmt((final['Top up ROI']/100)/12, 6, final['FINAL Top up amount as per Risk Grade'])
# final['Top up EMI for 6 month tenure'] = final['Top up EMI for 6 month tenure'].round(2)
# print('Calculation "Top up EMI for 6 month tenure" is completed....')
#
#     # Top up EMI for 12 month tenure
# final['Top up EMI for 12 month tenure'] = npf.pmt((final['Top up ROI']/100)/12, 12, final['FINAL Top up amount as per Risk Grade'])
# final['Top up EMI for 12 month tenure'] = final['Top up EMI for 12 month tenure'].round(2)
# print('Calculation "Top up EMI for 12 month tenure" is completed....')
#
#     # Remaining Tenure
# final['MaturityDate'] = pd.to_datetime(final['MaturityDate'])
# current_date = pd.to_datetime(datetime.now().date())
# final['Remaining Tenure'] = (final['MaturityDate'] - current_date) / 30
# final['Max tenure allowed'] = (final['MaturityDate'] - current_date) / 30
# final.to_excel('BNPL_after_scrub.xlsx')
# print('Exported to excel...')


