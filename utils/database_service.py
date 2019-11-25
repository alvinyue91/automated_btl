import pandas as pd
from sqlalchemy import create_engine
import psycopg2

class DataService:

    def __init__(self, database_config):
        self.engine = create_engine('postgresql+psycopg2://' + database_config['user'] + ':' +\
                                    database_config['password'] + '@' + database_config['host'] + ':' +\
                                    database_config['port'] + '/' + database_config['database'])

    def query_postgres(self, sql_query):

        df = pd.read_sql_query(sql_query, self.engine)

        return df

    def write_to_postgres(self, df, if_exists, index, database_schema, database_table):

        if df.shape[0] > 0:

            df.to_sql(database_table, con=self.engine, if_exists=if_exists,
                             schema=database_schema, index=index)

    def execute_sql(self, sql):

        connection = self.engine.connect()
        connection.execute(sql)
        connection.close()
