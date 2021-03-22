import requests
from simple_salesforce import Salesforce

from config import *


def download_reports():
    sf = Salesforce(username=salesforce_email, password=salesforce_password, instance_url=salesforce_instance_url, security_token=salesforce_token)
    reportId = '00O4G0000081z2O'
    response = requests.get(f"{salesforce_instance_url}{reportId}?view=d&snip&export=1&enc=UTF-8&xf=csv",
                      headers = sf.headers, cookies = {'sid' : sf.session_id})

    with open('123.csv', 'w') as f:
        f.write(response.text)


def main():
    download_reports()


if __name__ == '__main__':
    main()
