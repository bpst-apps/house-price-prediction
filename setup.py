# Importing required packages
from setuptools import setup, find_packages
from typing import List

# Declaring variables for setup functions
PROJECT_NAME = 'housing=predictor'
PROJECT_VERSION = '0.0.1'
PROJECT_AUTHOR = 'Bhanu'
PROJECT_DESCRIPTION = 'House Price Prediction Project'

REQUIREMENT_FILE_NAME = "requirements.txt"
HYPHEN_E_DOT = "-e ."


# Get list of requirements from requirements.txt file
def get_requirements_list() -> List[str]:
    """
    Description: This function is going to return list of requirement
    mention in requirements.txt file
    return This function is going to return a list which contain name
    of libraries mentioned in requirements.txt file
    """
    with open(REQUIREMENT_FILE_NAME) as requirement_file:
        requirement_list = requirement_file.readlines()
        requirement_list = [requirement_name.replace("\n", "") for
                            requirement_name in requirement_list]
        if HYPHEN_E_DOT in requirement_list:
            requirement_list.remove(HYPHEN_E_DOT)
        return requirement_list


# Do project setup
setup(
    name=PROJECT_NAME,
    version=PROJECT_VERSION,
    author=PROJECT_AUTHOR,
    description=PROJECT_DESCRIPTION,
    packages=find_packages(),
    install_requires=get_requirements_list()
)
