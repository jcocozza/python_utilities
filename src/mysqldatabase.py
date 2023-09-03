import sqlalchemy
import pymysql # needed by sqlalchemy for connection
import pandas
import logging
import time
logging.basicConfig(level=logging.INFO)

class MySQLConnector:
    """
    Connect to a MySQL database via sqlalchemy
    """
    def __init__(self, username: str, password: str, host: str, database_name: str) -> None:
        """
        :param username: Your username
        :param password: Your password
        :param host: The database host address
        :param database_name: [Optional] The name of the database to connect to
        """
        self.username = username
        self.password = password
        self.host = host
        self.database_name = database_name
        self.engine_string = self.generate_engine_string()
        self.engine = self.create_engine()

    def generate_engine_string(self) -> str:
        """
        Create the engine string for connection with sqlalchemy
        """
        if self.database_name:
            string = f'mysql+pymysql://{self.username}:{self.password}@{self.host}/{self.database_name}'
        else:
            string = f'mysql+pymysql://{self.username}:{self.password}@{self.host}/'
        logging.info(f'engine string:: {string}')
        return string

    def create_engine(self) -> sqlalchemy.Engine:
        """
        Create an sqlalchemy engine connection
        """
        try:
            engine = sqlalchemy.create_engine(self.engine_string)
            return engine
        except Exception as ex:
            logging.error(f'Failed to create engine connection: {ex}')
            raise(ex)

    def execute(self, sql: str) -> sqlalchemy.CursorResult:
        """
        Run a sql statement.

        No return.

        Use for INSERTS, UPDATES AND DELETES because it will actually commit the changes to the db.

        :param sql: A string of valid sql syntax
        """
        with self.engine.connect() as conn:
            res = conn.execute(sqlalchemy.text(sql))
            conn.commit()
        return res

    def query(self, sql: str) -> sqlalchemy.CursorResult:
        """
        Run a sql statement.

        Used with:
        - .fetchall()
        - .fetchmany()
        - .fetchone()

        To get into the weeds more, see:
        https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.CursorResult

        :param sql: A string of valid sql syntax
        :returns: A cursor result, accessed with the above .fetch*()
        """
        res = self.engine.connect().execute(sqlalchemy.text(sql))
        return res

    def pandas_query(self, sql: str) -> pandas.DataFrame:
        """
        Run a query.

        :param sql: A string of valid sql syntax
        :returns: A pandas dataframe of the result.
        """
        result = pandas.read_sql(sql, self.engine)
        return result

    def batch_query(self, sql: str, batch_size: int, output_file_path: str) -> None:
        """
        Query in batch using pandas. Use the batch_size param to control how many rows are written at once.
        - Higher batch size ~ faster process time(fewer queries), but risk running out of memory
        - Lower batch size ~ slower process time because have to run more queries, but won't run out of memory

        Will write/append the output of each batch to a csv file(output_file_path)

        The point of this is to deal with queries that return too many rows to be stored in memory

        :param sql: A string of valid sql syntax
        :param batch_size: Number of records to get in one batch
        :param output_file_path: where to save the results of the query
        :returns: None, but writes output to a csv file (in batch)
        """
        start_time = time.time()
        try:
            connection = self.engine.connect().execution_options(stream_results=True, max_row_buffer=batch_size)
            logging.info(f'~~~~~~~~~~ Writing batches for {output_file_path} ~~~~~~~~~~')
            i = 0
            for chunk_df in pandas.read_sql(sql, connection, chunksize=batch_size):
                header = (i == 0) # only write the header for the first file

                logging.info(f'Writing batch # {i + 1}...')
                chunk_df.to_csv(output_file_path, mode='a', header=header, index=False) # write dataframe to csv in chunks
                i += 1

            logging.info(f'Successfully wrote {i} batches')
            logging.info(f'~~~~~~~~~~ All batches written for {output_file_path} ~~~~~~~~~~')

            end_time = time.time()
            logging.info(f'Batch query finished in {end_time - start_time} seconds')
        except Exception as ex:
            fail_time = time.time()
            logging.info(f'Batch query fail in {fail_time - start_time} seconds')
            logging.error(f'Batch query failed!: {ex}')
        return None

