# Importing required packages
import os
import sys
import uuid
import pandas as pd
from threading import Thread
from collections import namedtuple

from housing.constant import *
from housing.logger import logging
from housing.exception import HousingException
from housing.configuration import Configuration

from housing.component.data_ingestion import DataIngestion
from housing.entity.artifact_entity import DataIngestionArtifact

# Define experiment
Experiment = namedtuple(
    'Experiment',
    ['experiment_id', 'initialization_timestamp', 'artifact_timestamp', 'running_status', 'start_time',
     'stop_time', 'execution_time', 'message', 'experiment_filepath', 'accuracy', 'is_model_accepted']
)


class Pipeline(Thread):
    # Create experiment
    experiment: Experiment = Experiment(*([None] * 11))

    # Create experiment file path variable
    experiment_file_path = None

    def __int__(self, config: Configuration) -> None:
        try:
            # Create artifact directory
            os.makedirs(config.training_pipeline_config.artifact_dir, exist_ok=True)

            # Define experiment file path
            Pipeline.experiment_file_path = os.path.join(
                config.training_pipeline_config.artifact_dir,
                EXPERIMENT_DIR_NAME,
                EXPERIMENT_FILE_NAME
            )
            super().__init__(daemon=False, name="pipeline")
            self.config = config
        except Exception as e:
            raise HousingException(e, sys) from e

    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            # Initialize data ingestion
            data_ingestion = DataIngestion(data_ingestion_config=self.config.get_data_ingestion_config())
            return data_ingestion.initiate_data_ingestion()
        except Exception as e:
            raise HousingException(e, sys) from e

    def run(self):
        try:
            self.run_pipeline()
        except Exception as e:
            raise e

    def run_pipeline(self):
        try:
            if Pipeline.experiment.running_status:
                # Logging information to log file
                logging.info("pipeline is already running")

                # Returning currently running experiment
                return Pipeline.experiment

            # Logging information to log file
            logging.info("housing price prediction pipeline started")

            # Create pipeline experiment
            Pipeline.experiment = Experiment(
                experiment_id=str(uuid.uuid4()),
                initialization_timestamp=self.config.timestamp,
                artifact_timestamp=self.config.timestamp,
                running_status=True,
                start_time=datetime.now(),
                stop_time=None,
                execution_time=None,
                experiment_file_path=Pipeline.experiment_file_path,
                is_model_accepted=None,
                message='pipeline has been started',
                accuracy=None
            )

            # Logging information to log file
            logging.info(f"pipeline experiment (starting configuration): {Pipeline.experiment}")

            # Save experiment
            self.save_experiment()

            # Starting data ingestion
            data_ingestion_artifact = self.start_data_ingestion()

            # Get experiment stop time
            stop_time = datetime.now()

            # Update experiment
            Pipeline.experiment = Experiment(
                experiment_id=Pipeline.experiment.experiment_id,
                initialization_timestamp=self.config.timestamp,
                artifact_time_stamp=self.config.timestamp,
                running_status=False,
                start_time=Pipeline.experiment.start_time,
                stop_time=stop_time,
                execution_time=stop_time - Pipeline.experiment.start_time,
                message='pipeline has been completed',
                experiment_file_path=Pipeline.experiment_file_path,
                is_model_accepted=None,
                accuracy=None
            )

            # Logging information to log file
            logging.info(f"pipeline experiment (ending configuration): {Pipeline.experiment}")

            # Save experiment
            self.save_experiment()
        except Exception as e:
            raise HousingException(e, sys) from e

    def save_experiment(self):
        try:
            if Pipeline.experiment.experiment_id is not None:
                # Get experiment as dictionary
                experiment = Pipeline.experiment
                experiment_dict = experiment._asdict()
                experiment_dict: dict = {key: [value] for key, value in experiment_dict.items()}

                # Update experiment dictionary
                experiment_dict.update({
                    "created_time_stamp": [datetime.now()],
                    "experiment_file_path": [os.path.basename(Pipeline.experiment.experiment_file_path)]
                })

                # Create experiment report
                experiment_report = pd.DataFrame(experiment_dict)

                # Create directory to store experiment details if not exists
                os.makedirs(os.path.dirname(Pipeline.experiment_file_path), exist_ok=True)
                if os.path.exists(Pipeline.experiment_file_path):
                    experiment_report.to_csv(Pipeline.experiment_file_path, index=False, header=False, mode="a")
                else:
                    experiment_report.to_csv(Pipeline.experiment_file_path, mode="w", index=False, header=True)
            else:
                print('experiment is not started')
        except Exception as e:
            raise HousingException(e, sys) from e
