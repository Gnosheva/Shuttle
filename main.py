from csv_io import CsvReaderWriter
from database_io import DatabaseConnector
import logging

logging.basicConfig(
    level=logging.DEBUG,
    filename="mylog.log",
    format="%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
    datefmt='%H:%M:%S',
)


def main():
    exit_code = 0
    csvrw = CsvReaderWriter()
    dbc = DatabaseConnector()
    dbc.define_data()
    path_to_transaction = 'source/transactions/transactions_current-datetime.csv'
    path_to_price = 'source/prices/price_file_date_unixtimestamp.csv'

    transaction = csvrw.read(path_to_transaction)
    price = csvrw.read(path_to_price)

    dbc.add_transaction(transaction)
    dbc.add_price(price)
    return exit_code


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.info(e)
