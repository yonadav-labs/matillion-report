from datetime import datetime, timedelta

import requests
import pandas as pd
import numpy as np
from simple_salesforce import Salesforce
from openpyxl import load_workbook

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
    df_lead = pd.read_csv('data/lead.csv')
    df_lead = remove_footer(df_lead)
    df_lead = df_lead[df_lead['Campaign Source'] != 'Sales']

    # handle country
    country_value = {'Country': 'Not reportable'}
    df_lead.fillna(country_value, inplace=True)

    # add seller name
    df_lead.insert(loc=0, column='Seller Company Name', value='Matillion')

    # remove duplicates
    df_lead.drop_duplicates(subset=['Related Record ID'], inplace=True)

    # handle status
    status_values = {
        'Disqualified': 'Junk',
        'Junk – Not a fit': 'Junk',
        'SDR Qualified': 'Valid',
        'Sales Rejected': 'Lost',
        'Marketing On-point': 'Valid',
        'Marked Finished - No Reply': 'Lost',
        'Sales Working': 'Valid',
        'MQL': 'Valid', 'Sales On-Point':
        'Valid', 'SQL': 'Valid',
        'SDR Following Up': 'Valid',
        'Added to Sequence': 'Valid'
    }

    df_lead['Handoff Status'].replace(status_values, inplace=True)

    # change column name
    new_column_names = {
        'Created Date': 'Campaign Create Date',
        'Campaign Source': 'GTM Campaign Source',
        'Campaign ID': 'CRM System Campaign ID'
    }
    df_lead.rename(new_column_names, axis=1, inplace=True)

    return df_lead


def process_opp_tab():
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
    df_opp.drop(['Record Unique ID'], axis=1, inplace=True)
    df_opp.dropna(subset=['GTM Campaign Source'], inplace=True)
    df_opp = df_opp[df_opp['GTM Campaign Source'] != 'Sales']

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

    revenue_value = {'Pipeline Revenue': 0}
    df_opp.fillna(revenue_value, inplace=True)

    # handle win date
    df_opp['Convert Date'] = pd.to_datetime(df_opp['Convert Date'])
    df_opp['Win Date'] = df_opp['Convert Date'] + timedelta(days=1)
    df_opp['Convert Date'] = df_opp['Convert Date'].dt.strftime('%m/%d/%Y')
    this_year = datetime.now().year
    df_opp['Win Date'] = df_opp['Win Date'].dt.strftime(f'%m/%d/{this_year}')
    df_opp.loc[df_opp['YTD Billed Revenue'].isnull(), 'Win Date'] = None

    # reorder columns
    columns = list(df_opp.columns)
    new_columns = columns[:1] + columns[11:] + columns[1:11]
    df_opp = df_opp[new_columns]
    df_opp.insert(loc=13, column='CPPO', value='')
    df_opp.insert(loc=14, column='Consulting Partner Name', value='')

    return df_opp


def process_campaign_tab(df_lead, df_opp):
    columns = ['Seller Company Name', 'GTM Campaign Source', 'Campaign Name', 'CRM System Campaign ID', 'Campaign Create Date']
    df_lead_ = df_lead[columns]
    df_opp_ = df_opp[columns]
    df_campaign = pd.concat([df_lead_, df_opp_])
    df_campaign.drop_duplicates(subset=['CRM System Campaign ID'], inplace=True)

    # insert new columns
    df_campaign['Campaign Region'] = ''
    df_campaign['Campaign Sub-region'] = ''
    df_campaign['Consulting Partner Name'] = ''
    df_campaign['Investment*'] = 0
    df_campaign['Investment Geo'] = ''

    return df_campaign


def save_report(df_lead, df_opp, df_campaign):
    file_name = datetime.now().strftime('data/SellerGTMReport_Matillion_%m%d%Y.xlsx')
    wb = load_workbook('report-template.xlsx')

    sheet_map = {
        'Lead Level': df_lead,
        'Opportunity Level': df_opp,
        'Campaign Level': df_campaign
    }    

    for sheet_name, frame in sheet_map.items():
        ws = wb[sheet_name]
        for index, row in frame.iterrows():
            ws.append(list(row))

    wb.save(file_name)

    return file_name


def main():
    download_reports()

    df_lead = process_lead_tab()
    df_opp = process_opp_tab()
    df_campaign = process_campaign_tab(df_lead, df_opp)

    report_file_name = save_report(df_lead, df_opp, df_campaign)


if __name__ == '__main__':
    main()
