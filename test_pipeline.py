# Importing required packages
import os
from housing.logger import logging
from housing.pipeline import Pipeline
from housing.configuration import Configuration


def main():
    try:
        # Get configuration file path
        config_path = os.path.join("config", "config.yaml")
        pipeline = Pipeline(Configuration(config_file_path=config_path))
    except Exception as e:
        logging.error(f"{e}")
        print(e)


if __name__ == "__main__":
    main()
