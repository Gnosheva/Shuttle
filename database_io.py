from cassandra.cluster import Cluster


class DatabaseConnector:

    def __init__(self):
        try:
            self.cluster = Cluster()
            self.session = self.cluster.connect()
        except Exception:
            print('No connection')

    def define_data(self):
        self.session.execute("CREATE KEYSPACE if not exists keyspace_surveillance_system WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} ")
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
                Quantity int,
                Price float,
                Currency text,
                Datestamp text,
                NetAmount float,
                PRIMARY KEY ( (ExecutionEntityName , InstrumentName, Currency), Price, Datestamp )
            )""")
        self.session.execute(
            """
            CREATE TABLE price(
            InstrumentName text,
            Datestamp text,
            Currency text,
            AVGPrice float,
            NetAmountPerDay float,
            PRIMARY KEY ((InstrumentName, Currency, Datestamp), AVGPrice)
            )""")

    def __insert_transaction_record(self, record):
            self.session.execute(
            """
            INSERT INTO transaction (TransactionID, ExecutionEntityName, InstrumentName, InstrumentClassification,
                                     Quantity, Price, Currency, Datestamp, NetAmount)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (record[0], record[1], record[2], record[3], int(record[4]), float(record[5]), record[6], record[7], float(record[8]))
        )

    def __insert_price_record(self, record):
        self.session.execute(
            """
            INSERT INTO price (InstrumentName, Datestamp, Currency, AVGPrice, NetAmountPerDay)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (record[0], record[1], record[2], float(record[3]), float(record[4]))
        )

    def add_transaction(self, transaction_list):
        for record in transaction_list:
            self.__insert_transaction_record(record)

    def add_price(self, price_list):
        for record in price_list:
            self.__insert_price_record(record)

    def select_from_transaction(self):
        res = self.session.execute("""SELECT * FROM transaction""")
        return [row for row in res]


    def shutdown(self):
        self.cluster.shutdown()
