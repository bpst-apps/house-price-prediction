# Importing required packages
import sys

from housing.constant import *
from housing.logger import logging
from housing.util import read_yaml_file
from housing.exception import HousingException

from housing.entity.config_entity import TrainingPipelineConfig, DataIngestionConfig


class Configuration:

    def __int__(self,
                config_file_path: str = CONFIG_FILE_PATH,
                current_timestamp: str = CURRENT_TIMESTAMP
                ) -> None:
        try:
            # Read configuration file "config.yaml'
            self.config_info = read_yaml_file(file_path=config_file_path)

            # Get train pipeline configuration
            self.training_pipeline_config = self.get_training_pipeline_configuration()

            # Get current timestamp
            self.timestamp = current_timestamp
        except Exception as e:
            raise HousingException(e, sys) from e

    def get_data_ingestion_configuration(self) -> DataIngestionConfig:
        """
        artifact ----- (1)
        - data_ingestion
            - <current timestamp> -> example <2022-07-14-11-05-47> ----- (2)
                - ingested_data ----- (5)
                    - test ----- (6)
                        - <directory content>
                    - train ----- (7)
                        - <directory content>
                - raw_data ----- (4)
                    - <directory content>
                - tgz_data ----- (3)
                    - <directory content>
        :return:
        """
        try:
            # 1. Get artifact directory path
            artifact_dir = self.training_pipeline_config.artifact_dir

            # 2. Create data ingestion artifact directory
            data_ingestion_artifact_dir = os.path.join(
                artifact_dir,
                DATA_INGESTION_ARTIFACT_DIR,
                self.timestamp
            )

            # Get data ingestion config section from configuration
            data_ingestion_info = self.config_info[DATA_INGESTION_CONFIG_KEY]

            # Get data download url as define in configuration
            dataset_download_url = data_ingestion_info[DATA_INGESTION_DOWNLOAD_URL_KEY]

            # 3. Create directory to store downloaded dataset
            # Current dataset is in ".tgz" format so naming directory accordingly
            tgz_download_dir = os.path.join(
                data_ingestion_artifact_dir,
                data_ingestion_info[DATA_INGESTION_TGZ_DOWNLOAD_DIR_KEY]
            )

            # 4. Creating directory path to store raw data
            # This raw data is the obtained after extracting ".tgz" file
            raw_data_dir = os.path.join(
                data_ingestion_artifact_dir,
                data_ingestion_info[DATA_INGESTION_RAW_DATA_DIR_KEY]
            )

            # 5. Create ingested data directory path
            ingested_data_dir = os.path.join(
                data_ingestion_artifact_dir,
                data_ingestion_info[DATA_INGESTION_INGESTED_DIR_NAME_KEY]
            )

            # 6. Create directory to store train dataset for ingested data
            ingested_train_dir = os.path.join(
                ingested_data_dir,
                data_ingestion_info[DATA_INGESTION_TRAIN_DIR_KEY]
            )

            # 7. Create directory to store test dataset for ingested data
            ingested_test_dir = os.path.join(
                ingested_data_dir,
                data_ingestion_info[DATA_INGESTION_TEST_DIR_KEY]
            )

            # Update data ingestion configuration with above created paths
            data_ingestion_config = DataIngestionConfig(
                dataset_download_url=dataset_download_url,
                tgz_download_dir=tgz_download_dir,
                raw_data_dir=raw_data_dir,
                ingested_train_dir=ingested_train_dir,
                ingested_test_dir=ingested_test_dir
            )

            # logging updated data ingestion configuration
            logging.info(f"data ingestion configuration: [{data_ingestion_config}]")

            # Returning updated data ingestion configuration
            return data_ingestion_config
        except Exception as e:
            raise HousingException(e, sys) from e

    def get_training_pipeline_configuration(self) -> TrainingPipelineConfig:
        try:
            # Load training pipeline configuration
            training_pipeline_config = self.config_info[TRAINING_PIPELINE_CONFIG_KEY]

            # Create artifact directory path
            artifact_dir = os.path.join(
                ROOT_DIR,
                training_pipeline_config[TRAINING_PIPELINE_NAME_KEY],
                training_pipeline_config[TRAINING_PIPELINE_ARTIFACT_DIR_KEY]
            )

            # Update training pipeline configuration with artifact directory path
            training_pipeline_config = TrainingPipelineConfig(artifact_dir=artifact_dir)

            # Logging updated training pipeline configuration
            logging.info(f"training pipeline configuration: {training_pipeline_config}")

            # Return training pipeline configuration
            return training_pipeline_config
        except Exception as e:
            raise HousingException(e, sys) from e
