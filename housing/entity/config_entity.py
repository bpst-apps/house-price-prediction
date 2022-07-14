# Importing required packages
from collections import namedtuple


# Define training pipline configuration
TrainingPipelineConfig = namedtuple(
    'TrainingPipelineConfig',
    ['artifact_dir']
)


# Define data ingestion configuration
DataIngestionConfig = namedtuple(
    'DataIngestionConfig',
    ['dataset_download_url', 'tgz_download_dir', 'raw_data_dir', 'ingested_train_dir',
     'ingested_test_dir']
)
