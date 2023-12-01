# Importing the required libraries
import sqlite3
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup

URL = ('https://web.archive.org/web/20230908091635%20/'
       'https://en.wikipedia.org/wiki/List_of_largest_banks')
ATTRIBS = ['Name', 'MC_USD_Billion']
DB = 'Banks.db'
TABLE = 'Largest_banks'
PATH = 'Largest_banks_data.csv'
LOG_FILE = 'code_log.txt'


def extract(url, table_attribs) -> pd.DataFrame:
    """ This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. """

    df = pd.DataFrame(columns=table_attribs)
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            names = col[1].find_all('a')
            name = [a.text for a in names]
            data_dict = {"Name": str(name[1]),
                         "MC_USD_Billion": float(col[2].contents[0]),
                         }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df):
    """ This function converts the GDP information from Currency
        format to float value, transforms the information of GDP from
        USD (Millions) to USD (Billions) rounding to 2 decimal places.
        The function returns the transformed dataframe."""

    usd = df['MC_USD_Billion']
    df['MC_GBP_Billion'] = (usd * 0.8).round(2)
    df['MC_EUR_Billion'] = (usd * 0.93).round(2)
    df['MC_INR_Billion'] = (usd * 82.95).round(2)

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
    """ This function logs the mentioned message at a
    given stage of the code execution to a log file. Function
    returns nothing"""
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(LOG_FILE, "a") as f:
        f.write(timestamp + ',' + message + '\n')


log_progress('Preliminaries complete. Initiating ETL process')

dataframe = extract(URL, ATTRIBS)

log_progress('Data extraction complete. Initiating Transformation process')

# dataframe = transform(dataframe)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(dataframe, PATH)

log_progress('Data saved to CSV file')

conn = sqlite3.connect(DB)

log_progress('SQL Connection initiated')

load_to_db(dataframe, conn, TABLE)

log_progress('Data loaded to Database as table, Executing queries')

run_query(f"SELECT * FROM Largest_banks", conn)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)
run_query(f"SELECT Name from Largest_banks LIMIT 5", conn)

log_progress('Process Complete')

conn.close()

log_progress('Server Connection closed')
