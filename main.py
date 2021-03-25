import requests
import pandas as pd
import numpy as np
from simple_salesforce import Salesforce

from config import *
from utils import *


def download_reports():
    sf = Salesforce(
        username=salesforce_email,
        password=salesforce_password,
        instance_url=salesforce_instance_url,
        security_token=salesforce_token
    )

    for report_item in reports:
        url = f"{salesforce_instance_url}{report_item['id']}?view=d&snip&export=1&enc=UTF-8&xf=csv"
        response = requests.get(url, headers=sf.headers, cookies={'sid' : sf.session_id})

        with open('data/'+report_item['file_name'], 'w') as f:
            f.write(response.text)


def process_lead_tab():
    df = pd.read_csv('data/lead.csv')
    df = remove_footer(df)

    # handle country
    country_value = {'Country': 'Not reportable'}
    df.fillna(country_value, inplace=True)
    # add seller name
    df = df.reindex(columns=['Seller Company Name']+list(df.columns))
    df.loc[:, 'Seller Company Name'] = 'Matillion'
    # remove duplicates
    df.drop_duplicates(subset=['Campaign ID'], inplace=True)
    # handle status
    status_values = {
        'Disqualified': 'Junk',
        'Junk – Not a fit': 'Junk',
        'SDR Qualified': 'Valid',
        'Sales Rejected': 'Lost',
        'Marketing On-point': 'Valid',
        'Sales Working': 'Valid',
        'MQL': 'Valid', 'Sales On-Point':
        'Valid', 'SQL': 'Valid',
        'SDR Following Up': 'Valid',
        'Added to Sequence': 'Valid'
    }

    df['Handoff Status'].replace(status_values, inplace=True)

    df.to_excel('data/lead.xlsx', sheet_name='Lead Level', index=False)


def process_opp_tab_1():
    df_opp_revenue = pd.read_csv('data/opp-revenue.csv')
    df_opp_revenue = remove_footer(df_opp_revenue)
    df_contacts = pd.read_csv('data/contacts.csv')
    df_contacts = remove_footer(df_contacts)

    df_opp_revenue.drop(['BI Cloud Data Warehouse', 'Amount (ACV) Currency'], axis=1, inplace=True)
    df_mark_down = pd.pivot_table(df_contacts, values=['Contact 18 Character ID'], index=['Record Unique ID'], aggfunc=np.count_nonzero)

    df_opp_revenue = df_opp_revenue.merge(df_mark_down, on='Record Unique ID', how='left')
    df_opp_revenue = df_opp_revenue[df_opp_revenue['Contact 18 Character ID'] > 4]
    # remove columns
    # df.drop(['BI Cloud Data Warehouse', 'Amount (ACV) Currency'], axis=1, inplace=True)
    df_opp_revenue.to_excel('data/opp.xlsx', sheet_name='Lead Level', index=False)


def process_opp_tab_3():
    df = pd.read_csv('data/opp-pipeline.csv')
    df = remove_footer(df)

    # remove columns
    df.drop(['BI Cloud Data Warehouse', 'Amount (ACV) Currency'], axis=1, inplace=True)
    df.to_excel('data/opp-2.xlsx', sheet_name='Lead Level', index=False)


def main():
    # download_reports()
    # process_lead_tab()
    process_opp_tab_1()
    # process_opp_tab_2()
    # process_opp_tab_3()


if __name__ == '__main__':
    main()
