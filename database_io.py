from cassandra.cluster import Cluster
import logging


class DatabaseConnector:

    def __init__(self):
        try:
            self.cluster = Cluster()
            self.session = self.cluster.connect()
        except Exception:
            logging.info('No connection')

    def define_data(self):
        self.session.execute(
            "CREATE KEYSPACE if not exists keyspace_surveillance_system WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} ")
        self.session.execute("USE keyspace_surveillance_system")
        self.session.execute("drop table if exists transaction")
        self.session.execute("drop table if exists price")
        self.session.execute(
            """
            CREATE TABLE transaction(
                TransactionID text ,
                ExecutionEntityName text,
                InstrumentName text,
                InstrumentClassification text,
                Quantity text,
                Price text,
                Currency text,
                Datestamp text,
                NetAmount text,
                PRIMARY KEY (ExecutionEntityName, TransactionID)
            )""")
        self.session.execute(
            """
            CREATE TABLE price(
            InstrumentName text,
            Datestamp text,
            Currency text,
            AVGPrice text,
            NetAmountPerDay text,
            PRIMARY KEY (Currency, InstrumentName)
            )""")

    def __insert_transaction_record(self, record):
        self.session.execute(
            """
            INSERT INTO transaction (TransactionID, ExecutionEntityName, InstrumentName, InstrumentClassification,
                                     Quantity, Price, Currency, Datestamp, NetAmount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (*record,))

    def __insert_price_record(self, record):
        self.session.execute(
            """
            INSERT INTO price (InstrumentName, Datestamp, Currency, AVGPrice, NetAmountPerDay)
            VALUES (%s, %s, %s, %s, %s)
            """, (*record,))

    def add_transaction(self, transaction_list):
        for record in transaction_list:
            self.__insert_transaction_record(record)

    def add_price(self, price_list):
        for record in price_list:
            self.__insert_price_record(record)

    # def select_from_transaction(self):
    #     res = self.session.execute("""SELECT * FROM transaction""")
    #     return [row for row in res]

    def shutdown(self):
        self.cluster.shutdown()
