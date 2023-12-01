# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from datetime import datetime
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

url_link = ('https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_'
            '%28nominal%29')
attributes = ['Country', 'GDP_USD_millions']
db = 'World_Economies.db'
table = 'Countries_by_GDP'
path = 'Countries_by_GDP.csv'

log_file = 'log.txt'


def extract(url, table_attribs) -> pd.DataFrame:
    """ This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. """

    df = pd.DataFrame(columns=table_attribs)
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows[3:]:
        tds = row.find_all('td')
        gdp = str(tds[2].contents[0])
        countries = tds[0].find_all('a')
        country = [a.text for a in countries]
        if gdp != '—':
            data_dict = {
                'Country': str(country[0]),
                'GDP_USD_millions': str(gdp)
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df) -> pd.DataFrame:
    """ This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe."""

    df['GDP_USD_millions'] = df['GDP_USD_millions'].str.replace(',', '').astype(float)
    df['GDP_USD_millions'] = df['GDP_USD_millions'] / 1000
    df['GDP_USD_millions'] = df['GDP_USD_millions'].round(2)
    df.rename(columns={'GDP_USD_millions': 'GDP_USD_billions'}, inplace=True)

    return df


def load_to_csv(df, csv_path):
    """ This function saves the final dataframe as a `CSV` file
    in the provided path. Function returns nothing."""
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    """ This function saves the final dataframe as a database table
    with the provided name. Function returns nothing."""
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    """ This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. """
    print(query_statement)
    output = pd.read_sql(query_statement, sql_connection)
    print(output)


def log_progress(message):
    """ This function logs the mentioned message at a given stage of the code execution to a log file. Function
    returns nothing"""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')


''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress('Preliminaries complete. Initiating ETL process.')

df = extract(url_link,attributes)

log_progress('Data extraction complete. Initiating Transformation process.')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process.')

load_to_csv(df, path)

log_progress('Data saved to CSV file.')

conn = sqlite3.connect(db)

log_progress('SQL Connection initiated.')

load_to_db(df, conn, table)

log_progress('Data loaded to Database as table. Running the query.')

run_query(f"SELECT * from {table} WHERE GDP_USD_billions >= 100", conn)

log_progress('Process Complete.')

conn.close()





























