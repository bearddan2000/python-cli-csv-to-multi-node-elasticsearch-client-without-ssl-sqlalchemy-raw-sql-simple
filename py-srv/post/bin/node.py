import logging, os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker


logging.basicConfig(level=logging.INFO)

INDEX_NAME = os.environ["INDEX_NAME"]

class Cluster():
    def __init__(self) -> None:
        self.hive = [
            Node("es1"),
            Node('es2'),
            Node('es3')
        ]

    def filter_query(self):
        for node in self.hive:
            node.filter_query()
    
    def get_all_query(self):
        for node in self.hive:
            node.get_all_query()
    
class Node():
    def __init__(self,server) -> None:
        self.server = server
        ELASTICSEARCH = {
            'engine': 'elasticsearch',
            'host': server,
            'port': 9200,
            'user': 'elastic',
            'password': 'changeme'
        }

        self.engine = create_engine("{engine}://{host}:{port}".format(**ELASTICSEARCH))
    
        self.session_local = sessionmaker(
            bind=self.engine
        )

    def get_db(self):
        db = self.session_local()
        try:
            yield db
        finally:
            db.close()

    def print_result(self, collection: list, func_name: str):
        for item in collection:
            logging.info("{} {}: {}".format(self.server,func_name, str(item)))

    def filter_query(self):
        db = next(self.get_db())
        collection = db.execute(text("SELECT * FROM {} WHERE id > 3".format(INDEX_NAME)))
        self.print_result(collection, "filter_query")

    def get_all_query(self):
        db = next(self.get_db())
        collection = db.execute(text("SELECT * FROM {}".format(INDEX_NAME)))
        self.print_result(collection, "get_all_query")
