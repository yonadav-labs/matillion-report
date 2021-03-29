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
    df.insert(loc=0, column='Seller Company Name', value='Matillion')

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
    df_opp_pipeline = pd.read_csv('data/opp-pipeline.csv')
    df_opp_pipeline = remove_footer(df_opp_pipeline)
    df_contacts = pd.read_csv('data/contacts.csv')
    df_contacts = remove_footer(df_contacts)

    # filter by marketing touches
    df_opp_revenue.drop(['BI Cloud Data Warehouse', 'Amount (ACV) Currency'], axis=1, inplace=True)
    df_mark_down = pd.pivot_table(df_contacts, values=['Contact 18 Character ID'], index=['Record Unique ID'], aggfunc=np.count_nonzero)

    df_opp_revenue = df_opp_revenue.merge(df_mark_down, on='Record Unique ID', how='left')
    df_opp_revenue = df_opp_revenue[df_opp_revenue['Contact 18 Character ID'] > 4]

    df_opp_revenue['Stage'] = 'Closed'
    df_opp_pipeline['Stage'] = 'Valid'

    # remove columns
    df_opp_revenue.drop(['Contact 18 Character ID'], axis=1, inplace=True)
    df_opp_pipeline.drop(['Amount (ACV) Currency'], axis=1, inplace=True)

    df_opp = pd.concat([df_opp_revenue, df_opp_pipeline])
    marketplace_opp_values = {
        0: 'Not reportable',
        1: 'Yes'
    }
    df_opp['AWS Marketplace Opportunity?'].replace(marketplace_opp_values, inplace=True)
    df_opp.insert(loc=1, column='Seller Company Name', value='Matillion')

    df_contacts.drop(['Member First Associated Date', 'Contact 18 Character ID'], axis=1, inplace=True)
    df_contacts = df_contacts[df_contacts['Campaign Source'] != 'Other']
    df_contacts.sort_values(by=['Created Date'], inplace=True)
    df_contacts.drop_duplicates(subset=['Record Unique ID'], inplace=True)
    new_column_names = {
        'Created Date': 'Campaign Create Date',
        'Campaign Source': 'GTM Campaign Source',
        'Campaign ID': 'CRM System Campaign ID'
    }
    df_contacts.rename(new_column_names, axis=1, inplace=True)

    df_opp = df_opp.merge(df_contacts, on='Record Unique ID', how='left')
    df_opp.dropna(subset=['GTM Campaign Source'], inplace=True)

    # change column name
    new_column_names = {
        'First Usage Date': 'Win Date',
        'Opportunity 18 Character ID': 'Opportunity ID',
        'Created Date': 'Convert Date',
        'Billing Country': 'Opportunity Country',
        'Stage': 'Opportunity Status',
        'Amount (ACV)': 'Pipeline Revenue'
    }
    df_opp.rename(new_column_names, axis=1, inplace=True)
    df_opp.drop_duplicates(subset=['Opportunity ID'], inplace=True)

    # reorder columns
    columns = list(df_opp.columns)
    new_columns = columns[:2] + columns[12:] + columns[2:12]
    df_opp = df_opp[new_columns]
    df_opp.insert(loc=14, column='CPPO', value='')
    df_opp.insert(loc=15, column='Consulting Partner Name', value='')

    df_opp.to_excel('data/opp-1.xlsx', sheet_name='Opportunity Level', index=False)


def main():
    # download_reports()
    # process_lead_tab()
    process_opp_tab_1()
    # process_opp_tab_3()


if __name__ == '__main__':
    main()
