import requests
from simple_salesforce import Salesforce

from config import *


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


def main():
    download_reports()


if __name__ == '__main__':
    main()
