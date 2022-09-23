import argparse
import pandas as pd
from typing import List
from datetime import datetime
import logging

import awswrangler as wr

logging.basicConfig(
    format="%(levelname)s - %(asctime)s - %(filename)s - %(message)s",
    level=logging.INFO,
    filename="salary-run-{start_time}.log".format(
        start_time=datetime.now().strftime("%Y-%m-%d")
    ),
)


def prep_s3_path(s3_bucket: str, df: pd.DataFrame) -> str:
    company = df['employer'].iloc[0].replace(" ", "_")
    submit_year = str(df['submit_year'].iloc[0])
    fname = f"{company}_{submit_year}.csv"
    s3_path = f"s3://{s3_bucket}/data/{fname}"
    return s3_path


def create_urls(companies: dict) -> dict:
    """_summary_

    Args:
        companies (dict): {"actual name": "name used in the url"}

    Returns:
        {'apple': ['https://h1bdata.info/index.php?em=apple+inc&job=&city=&year=2018',
                    'https://h1bdata.info/index.php?em=apple+inc&job=&city=&year=2019']
        }

    """
    all_urls = dict()
    year_range = list(range(2018, datetime.now().year + 1))
    for company_name, company_url_name in companies.items():
        company_year_urls = list()
        for year in year_range:
            url = f"https://h1bdata.info/index.php?em={company_url_name}&job=&city=&year={year}"
            company_year_urls.append(url)
        all_urls[company_name] = company_year_urls
    return all_urls


def clean_salary_data(company_name: str, df: pd.DataFrame) -> pd.DataFrame:
    # convert and clean column names
    df.columns = [x.lower().replace(" ", "_") for x in df.columns]
    # drop unused data
    df = df.drop(columns=['unnamed:_6', 'start_date'])
    # drop empty rows
    df = df.dropna()
    # drop all rows where the salary is not a number
    df = df[df['base_salary'].apply(lambda x: x.isnumeric())]
    # convert submit date to datetime
    df['submit_date'] = pd.to_datetime(df['submit_date'], format='%m/%d/%Y')
    # extract year
    df['submit_year'] = df['submit_date'].dt.year
    # extract month - for validation
    df['submit_month'] = df['submit_date'].dt.month
    # drop submit date
    df = df.drop(columns=['submit_date'])
    # extract state from location field. State is last two characters
    df['state'] = df['location'].apply(lambda x: x[-2:])
    # extract city from location field
    df['city'] = df['location'].apply(lambda x: x[:-4])
    # drop location
    df = df.drop(columns=['location'])
    # add in cleaned employer name
    df['employer'] = company_name
    return df

# "amazon", "google",
#              "linkedin", "uber", "salesforce",
#              "microsoft", "facebook", "netflix",
#              "airbnb", "twitter", "oracle",
#              "samsung", "intel", "ibm",
#              "qualcomm", "nvidia", "amd",
#              "paypal", "snapchat", "lyft",
#              "spotify", "dropbox", "atlassian",
#              "slack", "pinterest", "square",
#              "yelp", "zillow", "cisco",
#              "vmware", "adobe", "box", "workday", "twilio",
#              "okta", "splunk", "docusign", "zoom", "cloudera",
#              "mongodb", "snowflake", "databricks", "hashicorp",
#              "newrelic", "datadog", "sailthru", "salesloft",
#              "looker", "segment", "stripe", "instacart",
#              "lyft", "doordash", "postmates",
#              "robinhood", "coinbase", "roku",
#              "asana", "tesla", "palantir",

#              }


def main():
    companies = {"apple": "apple+inc",
                 "amazon": "amazon",
                 "google": "google+llc",
                 "microsoft": "microsoft+corporation",
                 "linkedin": "linkedin",
                 "netflix": "netflix",
                 "uber": "uber",
                 "salesforce": "salesforcecom+inc",
                 "airbnb": "airbnb+inc"
                 }

    parser = argparse.ArgumentParser()
    parser.add_argument("--s3_bucket", type=str,
                        help="Destiation S3 Bucket Name")
    args = parser.parse_args()
    url_dict = create_urls(companies=companies)
    for company_name, url_lst in url_dict.items():
        for url in url_lst:
            try:
                salary_df = pd.read_html(url)[0]
                salary_df_clean = clean_salary_data(company_name=company_name,
                                                    df=salary_df)
                s3_path = prep_s3_path(
                    s3_bucket=args.s3_bucket, df=salary_df_clean)
                wr.s3.to_csv(salary_df_clean, s3_path, index=False)
            except Exception as e:
                logging.error(f"Failed to read url: {url}")
                logging.error(e)
                continue


if __name__ == "__main__":
    main()
