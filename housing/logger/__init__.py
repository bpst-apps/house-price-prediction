# Importing required packages
import os
import logging
import pandas as pd

from housing.constant import get_current_timestamp

# Define logging directory
APP_LOG_DIR = 'app_logs'


# Function to return log file name
def get_log_file_name():
    return f"log_{get_current_timestamp()}.log"


# Define log file path
LOG_FILE_NAME = get_log_file_name()
os.makedirs(APP_LOG_DIR, exist_ok=True)
LOG_FILE_PATH = os.path.join(APP_LOG_DIR, LOG_FILE_NAME)

# Define logging basic configuration
logging.basicConfig(
    filename=LOG_FILE_PATH,
    filemode='w',
    format='[%(asctime)s]^;%(levelname)s^;%(lineno)d^;%(filename)s^;%(funcName)s()^;%(message)s',
    level=logging.INFO
)


# Function to return log dataframe
def get_log_dataframe(file_path):
    data = []
    with open(file_path) as log_file:
        for line in log_file.readlines():
            data.append(line.split("^;"))

    log_df = pd.DataFrame(data)
    columns = ["timestamp", "log Level", "line number", "file name", "function name", "message"]
    log_df.columns = columns

    log_df["log_message"] = log_df['timestamp'].astype(str) + ":$" + log_df["message"]

    return log_df[["log_message"]]
