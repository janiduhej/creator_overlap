import json
import logging
import os
from pathlib import Path
from threading import Semaphore

import psycopg2.extras as pgextra
from psycopg2.pool import ThreadedConnectionPool

BASE_PATH = str(Path(__file__).parent)
CFG_PATH = os.path.join(BASE_PATH, 'db_credentials')


class PostgresHandler(ThreadedConnectionPool):
    _logger = logging.getLogger(str(Path(__file__).stem))

    def __init__(self, min_pool=2, max_pool=100, chunk_size=1000, write_access=True, logger=_logger):
        self.logger = logger
        if write_access:
            self.db_credentials = CFG_PATH + '/write_db.json'
            self.logger.info(":: PostgresHandler :: using write access on prod")
        else:
            self.db_credentials = CFG_PATH + '/read_db.json'
            self.logger.info(":: PostgresHandler :: using read access on replica")
        self.config = dict()
        self.configure(self.db_credentials)
        self.min_pool = min_pool
        self.max_pool = max_pool
        self.chunk_size = chunk_size
        self.harvest_data = list()
        self._semaphore = Semaphore(self.min_pool)
        super().__init__(self.min_pool, self.max_pool, host=self._property('host'),
                         port=self._property('port'),
                         database=self._property('database'), user=self._property('user'),
                         password=self._property('password'), connect_timeout=30, keepalives=1)

    def configure(self, file_path):
        if not os.path.isfile(file_path):
            self.logger.warning('db credentials could not read ! not a valid / existing file: %s!' % file_path)
            raise ValueError('%s is not a valid / existing file!' % file_path)

        with open(file_path) as file:
            self.config = json.loads(file.read())

    def _property(self, key):
        return self.config.get(key, None)

    def _chunks(self, source_list):
        for i in range(0, len(source_list), self.chunk_size):
            yield source_list[i:i + self.chunk_size]

    def get_conn(self, *args, **kwargs):
        self.logger.debug('get_conn()...')
        self._semaphore.acquire()
        return super().getconn(*args, **kwargs)

    def put_conn(self, *args, **kwargs):
        self.logger.debug('put_conn()...')
        super().putconn(*args, **kwargs)
        self._semaphore.release()

    def _read_sql_file(self, sql_file):
        self.logger.debug(":: PostgresHandler :: reading sql-file %s" % sql_file)
        if not os.path.isfile(sql_file):
            self.logger.warning('Not a valid / existing sql file: %s!' % sql_file)
            raise ValueError('Not a valid / existing sql file: %s!' % sql_file)
        with open(sql_file) as file:
            return file.read()

    def retrieve_db_records_from_sql_pool(self, sql_statement):
        """
        Retrieve DB-Records from Postgres, using Connection Pooling
        :param sql_statement: str
        :return: list
        """
        list_of_records_retrieved = []
        connection = self.get_conn()

        if connection:
            self.logger.debug(":: execute sql-statement, return: list")
            cursor = connection.cursor()
            cursor.execute(sql_statement)  # tuple, because no cursor_factory used
            for row in cursor:
                list_of_records_retrieved.append(row[0])
            connection.close()
            self.put_conn(connection)
            return list_of_records_retrieved

    def retrieve_db_records_from_sql(self, sql_statement):
        """
        Retrieve DB-Records from Postgres, using Connection Pooling and a Cursor Factory
        :param sql_statement: str
        :return: dict
        """
        list_of_records_retrieved = []
        connection = self.get_conn()

        if connection:
            self.logger.debug(":: execute sql-statement, return: dict")
            cursor = connection.cursor(cursor_factory=pgextra.RealDictCursor)
            cursor.execute(sql_statement)
            for row in cursor:
                list_of_records_retrieved.append(row)
            connection.close()
            self.put_conn(connection)
            return list_of_records_retrieved

    def insert_data(self, sql_statement, chunk):
        connection = self.get_conn()
        if connection:
            self.logger.debug(":: PostgresHandler :: execute sql-statement")
            cursor = connection.cursor()
            cursor.execute(sql_statement, chunk)
            connection.commit()
            connection.close()
            self.put_conn(connection)

    def insert_chunk_pool(self, list_of_records, sql_file):
        for chunk in list(self._chunks(list_of_records)):
            self.insert_chunk_data_pooling(sql_file, chunk)

    def insert_chunk_data_pooling(self, sql_file, chunk):
        sql_statement = self._read_sql_file(sql_file)
        connection = self.get_conn()
        connection.set_client_encoding('utf8')
        if connection:
            self.logger.debug(":: execute sql-statement, insert data")
            cursor = connection.cursor()
            for record in chunk:
                cursor.execute(sql_statement, record)
            connection.commit()
            connection.close()
            self.put_conn(connection)

    def harvest_db_records(self, data_set, insert_sql):
        try:
            self.harvest_data.extend(data_set)
            if len(self.harvest_data) >= self.chunk_size:
                self.insert_chunk_pool(self.harvest_data, insert_sql)
                self.logger.info("%s intermediate rows inserted/updated in the database" % len(self.harvest_data))
                self.harvest_data.clear()
        except Exception as e:
            self.logger.error('Error during db inseration %s' % e)
            raise e

    def exec_sql(self, sql_statement):
        connection = self.get_conn()
        if connection:
            self.logger.debug(":: PostgresHandler :: execute sql-statement")
            cursor = connection.cursor()
            cursor.execute(sql_statement)
            connection.commit()
            connection.close()
            self.put_conn(connection)

    def get_token(self, page):
        sql = """SELECT token FROM credentials.api_token WHERE page = '%s';""" % page
        connection = self.get_conn()
        if connection:
            cursor = connection.cursor(cursor_factory=pgextra.RealDictCursor)
            cursor.execute(sql)
            record = cursor.fetchone()
            connection.close()
            self.put_conn(connection)
            return record
