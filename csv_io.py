import csv
from dataclasses import dataclass


@dataclass
class CsvReaderWriter:
    separator: str = ','

    def read(self, path):
        with open(path, 'r') as File:
            reader = csv.reader(File, delimiter=self.separator)
            read = [row for row in reader]
            read.pop(0)
            return read

    def write(self):
        pass

    # def get_data_from_db(self):
    #     return self.db_connection.session.execute("""SELECT * FROM transaction""")
