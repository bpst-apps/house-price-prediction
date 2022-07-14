# Importing required packages
import os
import sys
import tarfile
import numpy as np
import pandas as pd
from six.moves import urllib
from sklearn.model_selection import StratifiedShuffleSplit

from housing.logger import logging
from housing.exception import HousingException
from housing.entity.config_entity import DataIngestionConfig
from housing.entity.artifact_entity import DataIngestionArtifact


class DataIngestion:

    def __int__(self, data_ingestion_config: DataIngestionConfig):
        try:
            # Logging information to log file
            logging.info(f"{'>>' * 20} data ingestion process started {'<<' * 20}")
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise HousingException(e, sys) from e

    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        """
        We will basically do three tasks as mentioned below,
        - firstly we will download dataset
        - then we will extract downloaded dataset
        - and finally we will split dataset as train and test datasets
        :return:
        """
        try:
            # Get downloaded dataset directory path
            tgz_file_path = self.download_housing_data()

            # Extract downloaded dataset
            self.extract_tgz_file(tgz_file_path=tgz_file_path)

            # Return updated data ingestion artifact
            return self.split_data_as_train_test()
        except Exception as e:
            raise HousingException(e, sys) from e

    def download_housing_data(self, ) -> str:
        try:
            # Get dataset download url
            download_url = self.data_ingestion_config.dataset_download_url

            # Get directory path to store downloaded dataset
            tgz_download_dir = self.data_ingestion_config.tgz_download_dir

            # Create directory to store downloaded dataset
            os.makedirs(tgz_download_dir, exist_ok=True)

            # Extract file name from download url
            housing_file_name = os.path.basename(download_url)

            # Define directory path to store downloaded data
            tgz_file_path = os.path.join(
                tgz_download_dir,
                housing_file_name
            )

            # Logging information to log file
            logging.info(f"downloading file from: [{download_url}] into: [{tgz_file_path}]")

            # Downloading dataset
            urllib.request.urlretrieve(download_url, tgz_file_path)

            # Logging information to log file
            logging.info(f"file: [{tgz_file_path}] has been downloaded successfully")

            # Returning downloaded data directory path
            return tgz_file_path
        except Exception as e:
            raise HousingException(e, sys) from e

    def extract_tgz_file(self, tgz_file_path: str) -> None:
        try:
            # Get directory path to save extracted dataset
            raw_data_dir = self.data_ingestion_config.raw_data_dir

            # Check and remove raw data directory path if exists
            if os.path.exists(raw_data_dir):
                os.remove(raw_data_dir)

            # Create raw data directory
            os.makedirs(raw_data_dir, exist_ok=True)

            # Logging information to log file
            logging.info(f"extracting tgz file: [{tgz_file_path}] into directory: [{raw_data_dir}]")

            # Extracting raw data
            with tarfile.open(tgz_file_path) as housing_tgz_file_obj:
                housing_tgz_file_obj.extractall(path=raw_data_dir)

            # Logging information to log file
            logging.info(f"downloaded dataset data extraction completed")
        except Exception as e:
            raise HousingException(e, sys) from e

    def split_data_as_train_test(self) -> DataIngestionArtifact:
        try:
            # Get raw data directory path
            raw_data_dir = self.data_ingestion_config.raw_data_dir

            # Get first file name in list of files present in directory
            file_name = os.listdir(raw_data_dir)[0]

            # Define housing file path
            housing_file_path = os.path.join(
                raw_data_dir,
                file_name
            )

            # Logging information to log file
            logging.info(f"reading csv file: [{housing_file_path}]")

            # Reading housing data file into a pandas dataframe
            housing_data_frame = pd.read_csv(housing_file_path)

            # Create new feature "income_cat" from "median_income" feature
            housing_data_frame["income_cat"] = pd.cut(
                housing_data_frame["median_income"],
                bins=[0.0, 1.5, 3.0, 4.5, 6.0, np.inf],
                labels=[1, 2, 3, 4, 5]
            )

            # Logging information to log file
            logging.info("splitting extracted data into train and test sets")

            # Create stratified train and test set variables
            stratified_train_set = None
            stratified_test_set = None

            # Instantiate StratifiedShuffleSplit
            stratified_split = StratifiedShuffleSplit(n_splits=1, test_size=0.2, random_state=42)

            for train_index, test_index in stratified_split.split(housing_data_frame, housing_data_frame["income_cat"]):
                stratified_train_set = housing_data_frame.loc[train_index].drop(["income_cat"], axis=1)
                stratified_test_set = housing_data_frame.loc[test_index].drop(["income_cat"], axis=1)

            # Define directory path to store train dataset file
            train_file_path = os.path.join(
                self.data_ingestion_config.ingested_train_dir,
                file_name
            )

            # Define directory path to store train dataset file
            test_file_path = os.path.join(
                self.data_ingestion_config.ingested_test_dir,
                file_name
            )

            # Save train dataset
            if stratified_train_set is not None:
                # Create directory to store train dataset if not exits
                os.makedirs(self.data_ingestion_config.ingested_train_dir, exist_ok=True)

                # Logging information to log file
                logging.info(f"exporting training dataset to file: [{train_file_path}]")
                stratified_train_set.to_csv(train_file_path, index=False)

            # Save test dataset
            if stratified_test_set is not None:
                # Create directory to store train dataset if not exits
                os.makedirs(self.data_ingestion_config.ingested_test_dir, exist_ok=True)

                # Logging information to log file
                logging.info(f"exporting test dataset to file: [{test_file_path}]")
                stratified_test_set.to_csv(test_file_path, index=False)

            # Update data ingestion artifact
            data_ingestion_artifact = DataIngestionArtifact(
                train_file_path=train_file_path,
                test_file_path=test_file_path,
                is_ingested=True,
                message='data ingestion completed successfully'
            )

            # Logging information to log file
            logging.info(f"data ingestion artifact: [{data_ingestion_artifact}]")

            # Returning updated data ingestion artifact
            return data_ingestion_artifact
        except Exception as e:
            raise HousingException(e, sys) from e

    def __del__(self):
        # Logging
        logging.info(f"{'>>' * 20} data ingestion process completed {'<<' * 20}")
