from sqlalchemy import *
import pandas as pd
from urllib.parse import quote
import pyodbc
from datetime import datetime, timedelta

class BNPL:
    def __init__(self):
        self.conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                                   'Server=34.93.141.31\KGFSMITRA,2907;'
                                   'Database=TNAPP;'
                                   'Uid=MitraCDR;'
                                   'Pwd=c#w7EKjM8^5$;')
        #self.cnx_analytics = 'mysql+pymysql://analytics:%s@34.93.197.76:61306/analytics' % quote('Secure@123')
        self.cnx_analytics = 'mysql+pymysql://prakesh:%s@10.10.0.33:61306/analytics' % quote('Prakesh_033')


    def get_data_mitra(self, query):
        data = pd.read_sql(query, self.conn)
        print('data fetched from mitra...')
        return data

    def get_data_analytics(self, query):
        cnx = create_engine(self.cnx_analytics)
        data = pd.read_sql(text(query), cnx, index_col=None)
        cnx.dispose()
        print('Data fetched from analytics....')
        return data

    def vintage(self, d1, d2):
        result = ((d1 - d2).days) / 30.44
        if result >= 3:
            grp = '>=3 Month'
            return grp
        else:
            grp = '< 3 Months'
            return grp

    def six_mnth_vintage(self, d1, d2):
        result = ((d1 - d2).days) / 30.44
        if result >= 6:
            grp = 'Yes'
            return grp
        else:
            grp = 'No'
            return grp

    def genrate_last_three_month_list(self, current_date):
        last_three_months = []
        last_three_months.append(current_date.strftime("%b-%y"))
        for i in range(2):
            last_month = current_date - timedelta(days=current_date.day)
            last_three_months.append(last_month.strftime("%b-%y"))
            current_date = last_month - timedelta(days=1)
        mnth1 = last_three_months[0]
        mnth2 = last_three_months[1]
        mnth3 = last_three_months[2]
        return mnth1, mnth2, mnth3

    def genrate_last_three_month_str(self, current_date):
        last_three_months_end = []
        last_three_months_open = []
        last_three_months_end.append(current_date.strftime("%d%b%Y"))
        last_three_months_open.append(current_date.strftime("01%b%Y"))
        for i in range(2):
            last_month = current_date - timedelta(days=current_date.day)
            last_three_months_end.append(last_month.strftime("%d%b%Y"))
            last_three_months_open.append(last_month.strftime("01%b%Y"))
            current_date = last_month - timedelta(days=1)
        mnth_cl_1 = last_three_months_end[0]
        mnth_cl_2 = last_three_months_end[1]
        mnth_cl_3 = last_three_months_end[2]

        mnth_op_1 = last_three_months_open[0]
        mnth_op_2 = last_three_months_open[1]
        mnth_op_3 = last_three_months_open[2]
        return mnth_cl_1, mnth_cl_2, mnth_cl_3, mnth_op_1, mnth_op_2, mnth_op_3

    def upload_input_data(self, data, table_name):
        cnx = create_engine(self.cnx_analytics)
        data.to_sql(con=cnx, name=table_name, if_exists='append', index=False)
        cnx.dispose()
        print('upoaded completed')

    def filter_criteria(self, data):
        data = data[data['Vintage Bucket'] == '>=3 Month']
        data = data[data['3 month Avg>=25000'] == 'Yes']
        data = data[(data['no_of_emi_paid'] >= 12) | (data['no_of_emi_paid'].isna())]
        data['DPD_24Oct2023'] = data['DPD_24Oct2023'].astype(float)
        data['DPD_30Sep2023'] = data['DPD_30Sep2023'].astype(float)
        data['DPD_31Aug2023'] = data['DPD_31Aug2023'].astype(float)
        data = data[(data['DPD_24Oct2023'] == 0) | (data['DPD_24Oct2023'].isna())]
        data = data[(data['DPD_30Sep2023'] == 0) | (data['DPD_30Sep2023'].isna())]
        data = data[(data['DPD_31Aug2023'] == 0) | (data['DPD_31Aug2023'].isna())]
        data = data[(data['max_overdue_days'] <= 30) | (data['max_overdue_days'].isna())]
        data = data[(data['Writeoff'] == 'No')]
        data = data[(data['OTR'] == 'No')]
        data = data[(data['DLS'] == 'No')]
        return data


    def scrub_from_csv_to_excel(self, csv_file_name):
        scrub_data = pd.read_csv(f"{csv_file_name}", delimiter="|",
                                 usecols=['REFERENCE_NO', 'WOF_Amt_Before_2Years', 'WOF_Amt_InLast_2Years',
                                          'Overdue_Amt_Non_CLSDLoans', 'Monthly_EMI_AMT_Non_CLSDLoans',
                                          'Oustanding_AMT_Non_CLSDLoans'], dtype={'REFERENCE_NO': 'str'})
        scrub_data.rename(
            columns={'REFERENCE_NO': 'LOS_APP_ID', 'WOF_Amt_Before_2Years': 'TOTAL_BEFORE_2YEAR_WRITE_OFF_AMT',
                     'WOF_Amt_InLast_2Years': 'TOTAL_LAST_2YEAR_WRITE_OFF_AMT',
                     'Overdue_Amt_Non_CLSDLoans': 'TOTAL_OVERDUE_AMT',
                     'Monthly_EMI_AMT_Non_CLSDLoans': 'INSTALLMENTAMOUNT',
                     'Oustanding_AMT_Non_CLSDLoans': 'Remaning_POS'}, inplace=True)

        scrub_data = scrub_data.pivot_table(index='LOS_APP_ID', values=['TOTAL_BEFORE_2YEAR_WRITE_OFF_AMT',
                                                                        'TOTAL_LAST_2YEAR_WRITE_OFF_AMT',
                                                                        'TOTAL_OVERDUE_AMT', 'INSTALLMENTAMOUNT',
                                                                        'Remaning_POS'], aggfunc='sum', fill_value=0)
        scrub_data.reset_index(inplace=True)
        scrub_data.fillna(0, inplace=True)
        scrub_data['TOTAL_WRITE_OFF_AMT'] = scrub_data['TOTAL_BEFORE_2YEAR_WRITE_OFF_AMT'] + scrub_data[
            'TOTAL_LAST_2YEAR_WRITE_OFF_AMT']
        scrub_data['LOS_APP_ID'] = scrub_data['LOS_APP_ID'].astype('str')
        scrub_data.to_excel(f'scrub_data.xlsx')
        return scrub_data