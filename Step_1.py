import csv
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('keyspace_surveillance_system')

with open('source/transactions/test.csv') as File:
    reader = csv.reader(File, delimiter=',')
    first_row = True
    for row in reader:
        if first_row:
            first_row = False
            continue
        session.execute(
            """
            INSERT INTO transaction (TransactionID, ExecutionEntityName)
            VALUES (%s, %s)
            """,
            (row[0], row[1])
        )

res = session.execute('SELECT * FROM transaction')
for line in res:
    print(line)
