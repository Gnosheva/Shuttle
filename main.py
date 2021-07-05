from csv_io import CsvReaderWriter
from database_io import DatabaseConnector

csvrw = CsvReaderWriter()
dbc = DatabaseConnector()
dbc.define_data()
path_to_transaction = 'source/transactions/transactions_current-datetime.csv'
path_to_price = 'source/prices/price_file_datestamp.csv'

transaction = csvrw.read(path_to_transaction)
price = csvrw.read(path_to_price)

dbc.add_transaction(transaction)
dbc.add_price(price)

