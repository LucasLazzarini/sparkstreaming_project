import sqlite3
import pandas as pd


class SQLite3Helper:

    @staticmethod
    def create_connection(db_file: str):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        con = None

        con = sqlite3.connect(db_file)
        print(f'Conexão criada, novo database: {db_file}')
        con.close()

    @staticmethod
    def create_table(df: pd.DataFrame, db_file: str, table_name: str) -> None:
        """Cria a tabela em database utilizando um DataFrame Pandas

        Args:
            df (pd.DataFrame): DataFrame Pandas que será fixado na tabela
            db_file (str): Database que que receberá a tabela
            table_name (str): Nome da tabela que será criada ou substituida
        """
        print(f"Criando tabela {table_name} no database {db_file}")
        df = df.applymap(str)
        con = sqlite3.connect(db_file)
        df.to_sql(table_name, con, if_exists="replace")
        print("Tabela criada")

        con.close()

    @staticmethod
    def run_query(db_file: str, query: str) -> pd.DataFrame:
        """
        Query all rows in the tasks table
        :param conn: the Connection object
        :return:
        """
        print("Rodando Query")
        con = sqlite3.connect(db_file)
        df = pd.read_sql_query(query, con)
        con.close()
        print("DataFrame disponível com resultado da query")

        return df
