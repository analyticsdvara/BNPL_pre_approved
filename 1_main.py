from builder import BNPL
import warnings
warnings.filterwarnings('ignore')
from datetime import *
import pandas as pd
bnpl = BNPL()

from_dte = "2021-04-01" # (Fixed no need to change)
to_dte = "2023-09-25"
dte = date(2023, 10, 24)
to_dte = dte.strftime("%d%b%Y")
frm_dte = dte.strftime("01%b%Y")
cl_dte = dte.strftime("%Y-%m-%d")

# 1. Get Agent Data -----------------
query = f"""SELECT UserKey, BranchName, BranchCode as branch_code, UserMob as Mobile_Number, CONVERT(varchar, ApprovedOn, 23) as Onboard_Date, urn 
FROM UserDetails where UserKey>=3036 and cast(ApprovedOn as date) between '{from_dte}' and '{to_dte}'"""
data = bnpl.get_data_mitra(query)
print('Fetched data of mitra agent..')
# ------- Get Agent Data -------------

# 2. vintage calcaulations -----------
data['Onboard_Date'] = pd.to_datetime(data['Onboard_Date']).dt.date
data['Vintage 6 Month'] = data.apply(lambda x: bnpl.six_mnth_vintage(dte, x["Onboard_Date"]), axis = 1)
data['Vintage Bucket'] = data.apply(lambda x: bnpl.vintage(dte, x["Onboard_Date"]), axis = 1)
print('Calculated vintage of agents...')
# ---- Vintage Calcaulations-----------

# 3. Last 3 month GTV -----------------
mnth1, mnth2, mnth3 = bnpl.genrate_last_three_month_list(dte)
query_gtv = f"""select UserKey,
sum(case when year_date='{mnth3}' then ITD_Amount else 0 end) as '{mnth3}_GTV',
sum(case when year_date='{mnth2}' then ITD_Amount else 0 end) as '{mnth2}_GTV',
sum(case when year_date='{mnth1}' then ITD_Amount else 0 end) as '{mnth1}_GTV' 
from mitra_sale_mis_data group by UserKey"""

gtv_data = bnpl.get_data_analytics(query_gtv)
final = pd.merge(data, gtv_data, how='left', left_on='UserKey', right_on='UserKey')
final['3 month GTV AVG'] = final.apply(lambda x: (x[f'{mnth3}_GTV'] + x[f'{mnth2}_GTV'] + x[f'{mnth1}_GTV'])/3, axis=1)
final['3 month Avg>=25000'] = final['3 month GTV AVG'].apply(lambda x: 'Yes' if x >= 25000 else 'No')
print('GTV Calculation is completed.....')
# ---- Last 3 month GTV ------------------

# 4. CBS Loan dump live accounts identification
cbs_live_accounts_query = f"""
select urn, count(*) as no_of_Accounts_live_as_on_{to_dte} 
from perdix_cdr.quick_cbs_loan_dump_{frm_dte}to{to_dte}
where account_status not like '%close%' group by urn"""
cbs_cbs_live_accounts = bnpl.get_data_analytics(cbs_live_accounts_query)
cbs_cbs_live_accounts['urn'] = cbs_cbs_live_accounts['urn'].astype('str')
cbs_cbs_live_accounts.to_excel('cbs_cbs_live_accounts.xlsx')
final = pd.merge(final, cbs_cbs_live_accounts, how='left', left_on='urn', right_on='urn')
final[f'no_of_Accounts_live_as_on_{to_dte}'] = final[f'no_of_Accounts_live_as_on_{to_dte}'].fillna(0)
final[f'Customer_Active/Inactive_as_on_{to_dte}'] = final[f'no_of_Accounts_live_as_on_{to_dte}'].apply(lambda x: 'Inactive' if x == 0 else 'Active')
print('Live loans data fetched...')
# 4. ------------ end -------------

# 5. Closed Accounts in past -----------
closed_accounts_query = f"""select urn, count(*) as  No_of_Loans_closed_in_past 
from perdix_cdr.all_cms_loans_dump_unique
where account_closed=1 and urn is not null
and encore_account_closed_date<='{cl_dte}' group by urn"""
no_of_closed_accounts = bnpl.get_data_analytics(closed_accounts_query)
no_of_closed_accounts['urn'] = no_of_closed_accounts['urn'].astype('str')
final = pd.merge(final, no_of_closed_accounts, how='left', left_on='urn', right_on='urn')
#final[f'No_of_Loans_closed_in_past'] = final[f'No_of_Loans_closed_in_past'].fillna('NA')
print('Closed accounts fetched........')
# Closed Accounts in past ---------------

# 6.Three Month DPD from CBS Loan Dump -----------
mnth_cl_1, mnth_cl_2, mnth_cl_3, mnth_op_1, mnth_op_2, mnth_op_3 = bnpl.genrate_last_three_month_str(dte)
list_cl = [mnth_cl_1, mnth_cl_2, mnth_cl_3]
list_op = [mnth_op_1, mnth_op_2, mnth_op_3]

for o, c in zip(list_op, list_cl):
    three_mnt_DPD_query = f"""
    select urn, max(Overdue_Days_as_on_{c}) as DPD_{c}
    from perdix_cdr.quick_cbs_loan_dump_{o}to{c} group by urn"""
    the_mn_data = bnpl.get_data_analytics(three_mnt_DPD_query)
    the_mn_data['urn'] = the_mn_data['urn'].astype('str')
    final = pd.merge(final, the_mn_data, how='left', left_on='urn', right_on='urn')
    #final[f'DPD_{c}'] = final[f'DPD_{c}'].fillna('NA')
    print(f'------DPD_{c} is completed..')
print('three month DPD completed')
# ----- Three Month DPD from CBS Loan Dump -----------

# 7.No of emi paid -----------------------------------
emi_paid_query = f"""select urn, min(emi_paid) as no_of_emi_paid, max(max_overdue_days) as max_overdue_days 
from perdix_cdr.quick_cbs_loan_dump_{frm_dte}to{to_dte} group by urn"""
emi_paid_data = bnpl.get_data_analytics(emi_paid_query)
final = pd.merge(final, emi_paid_data, how='left', left_on='urn', right_on='urn')
#final['no_of_emi_paid'] = final['no_of_emi_paid'].fillna('NA')
#final['max_overdue_days'] = final['max_overdue_days'].fillna('NA')
print('no of emi paid fetched...')
# --------- no of  emi paid ---------------------------

# writeoff check ----------------------------------------
writeoff_query = """select urn, "Yes" as Writeoff from analytics.all_writeoff_fy22to23 
where written_back_status=0
and urn is not null group by urn"""
writeoff_data = bnpl.get_data_analytics(writeoff_query)
final = pd.merge(final, writeoff_data, how='left', left_on='urn', right_on='urn')
final['Writeoff'] = final['Writeoff'].fillna('No')
print('Writeoff check from analytics.all_writeoff_fy22to23')
# ------ Writeoff Check ------------------------------------

# ------------ OTR check -----------------------
otr_query = """select urn, "Yes" as OTR from analytics.OTR_loan_details where urn is not null group by urn"""
otr_data = bnpl.get_data_analytics(otr_query)
final = pd.merge(final, otr_data, how='left', left_on='urn', right_on='urn')
final['OTR'] = final['OTR'].fillna('No')
print('OTR check from analytics.OTR_loan_details')
# ------------ OTR Check -------------------------

# ---------------DLS
otr_query = """select urn, "Yes" as DLS from analytics.dls"""
otr_data = bnpl.get_data_analytics(otr_query)
final = pd.merge(final, otr_data, how='left', left_on='urn', right_on='urn')
final['DLS'] = final['DLS'].fillna('No')
print('DLS check from analytics.OTR_loan_details')
# ---------------DLS Check

# Agent as customer criteria ....................
# ..........Agent as customer criteria............
final.to_pickle('before_filter.pickle')
final  = bnpl.filter_criteria(final)
final.to_excel('final_data_after_filter_criteria_applied.xlsx')
