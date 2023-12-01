import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = 'log_file.txt'
target_file = 'transformed_data.csv'


def extract_from_csv(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    try:
        temp_dataframe = pd.read_csv(file_to_process)
        if not temp_dataframe.empty:
            dataframe = pd.concat([dataframe, temp_dataframe], ignore_index=True)
    except pd.errors.EmptyDataError:
        print(f"Warning: {file_to_process} is empty.")
    return dataframe


def extract_from_json(file):
    df = pd.read_json(file, lines=True)
    return df


def extract_from_xml(file):
    df = pd.read_xml(file)
    return df


def extract():
    # create an empty dataframe to hold extracted data
    data = pd.DataFrame(columns=['name', 'height', 'weight'])

    # process all csv
    for csvfile in glob.glob('*.csv'):
        data = pd.concat([data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    # process all csv
    for jsonfile in glob.glob('*.json'):
        data = pd.concat([data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    # process all csv
    for xmlFile in glob.glob('*.xml'):
        data = pd.concat([data, pd.DataFrame(extract_from_xml(xmlFile))], ignore_index=True)

    return data


def transform(data):
    # convert inch to meter
    # 1 inch = 0.0254 meter
    data['height'] = round(data.height * 0.0254, 2)

    # convert pound to kg
    # 1 pound = 0.45359237 kg
    data['weight'] = round(data.weight * 0.45359237, 2)

    return data


def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')


# Log the initialization of the ETL process
log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
extracted_data = extract()

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process
log_progress("Load phase Started")
load_data(target_file, transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")
