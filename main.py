"""
This file contains the main function. 
Launch the program with the following command: `python main.py`
"""

from etl.etl import ETL

if __name__ == '__main__':
    data_folder = input('Enter the path to the data folder: ')
    if_exists = input('Do you want to overwrite the existing data? (y/[n]): ')
    if_exists = "replace" if if_exists == "y" else "append"

    data = ETL().extract(data_folder, if_exists)
    data = ETL().transform(data)