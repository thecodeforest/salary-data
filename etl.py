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


def create_urls(companies: List[str]) -> List[str]:
    year_range = list(range(2018, datetime.now().year + 1))
    urls = list()
    for company in companies:
        for year in year_range:
            urls.append(
                f"https://h1bdata.info/index.php?em={company}&job=&city=&year={year}")
    return urls


def clean_salary_data(df: pd.DataFrame) -> pd.DataFrame:
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
    # extract year, month, day from submit_date
    df['submit_year'] = df['submit_date'].dt.year
    # drop submit date
    df = df.drop(columns=['submit_date'])
    # extract state from location field. State is last two characters
    df['state'] = df['location'].apply(lambda x: x[-2:])
    # extract city from location field
    df['city'] = df['location'].apply(lambda x: x[:-4])
    # drop location
    df = df.drop(columns=['location'])
    return df


def main():
    COMPANIES = ["apple+inc", "amazon", "google",
                 "linkedin", "uber", "salesforce"]
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3_bucket", type=str,
                        help="Destiation S3 Bucket Name")
    args = parser.parse_args()
    urls = create_urls(companies=COMPANIES)
    for url in urls:
        try:
            salary_df = pd.read_html(url)[0]
            salary_df_clean = clean_salary_data(salary_df)
            s3_path = prep_s3_path(
                s3_bucket=args.s3_bucket, df=salary_df_clean)
            wr.s3.to_csv(salary_df_clean, s3_path, index=False)
        except Exception as e:
            logging.error(f"Failed to read url: {url}")
            logging.error(e)
            continue


if __name__ == "__main__":
    main()
