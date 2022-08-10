"""
This file contains the main function. 
Launch the program with the following command: `python main.py`
"""

from etl.etl import ETL

def main() -> bool:
    """
    Main function.
    """
    return ETL().run()

if __name__ == '__main__':
    if main():
        print('ETL completed successfully.')
    