import pandas as pd
import datetime as dt
from builder import BNPL
bnpl = BNPL()

def data_peparation(data, fom_date, to_date, product):
    # Preparing template for final upload....
    data['pre-Approved'] = "Y"
    data['Start_Date'] = fom_date
    data['End_Date'] = to_date
    data['product'] = product
    data['risk_score'] = 0
    data['Top_up_ROI'] = "17"
    data['ID'] = data['urn'].astype(str) + "_" + data['branch_code'].astype(str) + "_" + timestamp_for_id
    data['time_stamp'] = timestamp
    print(data.dtypes)
    data = data[
        ['ID', 'urn', 'branch_code', 'pre-Approved', 'product', 'Start_Date', 'End_Date', 'risk_score', 'Top_up_ROI', 'Max_amount_approved', 'Min_amount_approved', 'time_stamp']]
    # data.rename(columns={'': ''}, inplace=True)


    # data['Max_amount_approved'] = data['Max_amount_approved'].round().astype(int)
    # data['Min_amount_approved'] = 15000
    # data['Min_amount_approved'] = data['Min_amount_approved'].astype(int)

    # upload data to table...
    print(f'valid from: {fom_date}')
    print(f'valid to: {to_date}')
    return data


# Declaration
curr_dte = dt.date(2023,10,31)
to_date = curr_dte.strftime('%Y-%m-%d')
fom_date = curr_dte.strftime('%Y-%m-01')
timestamp_for_id = dt.datetime.today().strftime('%Y%m')
timestamp = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
product = "BNPL"

print(timestamp)

# 1. data Preparation
data = pd.read_excel("upload.xlsx", dtype={'urn':str})
final = data_peparation(data, fom_date, to_date, product)
final.to_excel('final.xlsx')

# 2. Upload Data to table analytics.pre_approved_loan
bnpl.upload_input_data(final, "pre_approved_loan")
