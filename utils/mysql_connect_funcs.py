from sqlalchemy import create_engine, text, inspect, MetaData, Table
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv

load_dotenv()

port = int(os.getenv("mysql_port"))
user = os.getenv("mysql_user")
password = os.getenv("mysql_password")
host = os.getenv("mysql_host")
database = os.getenv("mysql_database")


def get_df_tblName(table):
    try:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            df = pd.read_sql(f"SELECT * FROM {table}", connection)
        engine.dispose()
        return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()


def get_df_query(query):
    try:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
        engine.dispose()
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()

def get_cursor(query, params=None):
    try:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            result = connection.execute(text(query), params).fetchone()  # Fetch the first result
        engine.dispose()
        return result

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def fetch_tables_for_screener():
    try:
        database_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(database_url)
        patterns = [
            "annual_income_statement",
            "annual_balance_sheet",
            "annual_cash_flow_statement",
            "annual_sup_IS",
            "annual_sup_BS",
            "annual_sup_CF",
            "annual_key_ratios"
        ]
        like_conditions = " OR ".join([f"Tables_in_{database} LIKE '%{pattern}%'" for pattern in patterns])
        query = f"SHOW TABLES WHERE {like_conditions}"

        with engine.connect() as conn:
            result = conn.execute(text(query))
            tables = result.fetchall()

        tables = [table[0] for table in tables]
        return tables

    except Exception as e:
        print(f"Error: {e}")
        return []


def write_df_tblName(table_name,df,print_success=True):
    try:
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        if print_success == True:
            print(f"DataFrame successfully written to table '{table_name}'.")
    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")
    finally:
        engine.dispose()


def insert_row_FR(table, data, columns):
    try:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            # Enclose column names with backticks to handle spaces
            column_conditions = " AND ".join([f"`{col}` = :{col.replace(' ', '_')}" for col in columns])
            query = text(f"SELECT COUNT(*) FROM `{table}` WHERE {column_conditions}")

            params = {col.replace(' ', '_'): data[i] for i, col in enumerate(columns)}
            result = connection.execute(query, params).scalar()

            if result > 0:
                print("Row is already in the table")
            else:
                column_names = ", ".join([f"`{col}`" for col in columns])
                placeholders = ", ".join([f":{col.replace(' ', '_')}" for col in columns])
                insert_query = text(f"INSERT INTO `{table}` ({column_names}) VALUES ({placeholders})")

                connection.execute(insert_query, params)
                connection.commit()
                print("Row inserted")

    except Exception as e:
        print(f"An error occurred: {e}")


def insert_row_SC(table, data, columns, element_index=0, column_index=0):
    try:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        element = data[element_index]  # Get the specified element from the list
        column_name = columns[column_index]  # Get the specified column name

        query = text(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE {column_name} = :value) AS exists_flag")

        with engine.connect() as conn:
            result = conn.execute(query, {"value": element})
            exists = result.scalar()  # Fetch the single boolean result

            if exists:
                print("row is already in the table")
            else:
                insert_query = text(
                    f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join([':' + col for col in columns])})")
                conn.execute(insert_query, dict(zip(columns, data)))
                conn.commit()
                print("row inserted")

    except Exception as e:
        print(f"An error occurred: {e}")


def replace_row(table, data, columns, element_index=0, column_index=0):
    try:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        element = data[element_index]  # Get the specified element from the list
        column_name = columns[column_index]  # Get the specified column name

        query = text(f"SELECT EXISTS(SELECT 1 FROM {table} WHERE {column_name} = :value) AS exists_flag")

        with engine.connect() as conn:
            result = conn.execute(query, {"value": element})
            exists = result.scalar()  # Fetch the single boolean result

            if exists:
                delete_query = text(f"DELETE FROM {table} WHERE {column_name} = :value")
                conn.execute(delete_query, {"value": element})
                print("Existing row deleted")

            insert_query = text(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join([':' + col for col in columns])})"
            )
            conn.execute(insert_query, dict(zip(columns, data)))
            conn.commit()
            print("Row inserted")

    except Exception as e:
        print(f"An error occurred: {e}")


"""def delete_table(table_name):
    try:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            print(f"Table '{table_name}' has been deleted.")

    except SQLAlchemyError as e:
        print(f"Error occurred: {e}")

    finally:
        engine.dispose()"""


def filter_table(table_name, column_name, values_list):

    try:
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        with engine.connect() as connection:
            query = f"SELECT * FROM {table_name} WHERE {column_name} IN ({','.join(['%s'] * len(values_list))}) ORDER BY FIELD({column_name}, {','.join(['%s'] * len(values_list))})"
            df = pd.read_sql(query, connection, params=tuple(values_list + values_list))
            return df

    except SQLAlchemyError as e:
        print(f"Error occurred: {e}")
        return pd.DataFrame()

    finally:
        engine.dispose()


def get_table_names():
    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    inspector = inspect(engine)
    return inspector.get_table_names()

def delete_table(table_name):
    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    metadata = MetaData()
    inspector = inspect(engine)

    try:
        if table_name in inspector.get_table_names():
            table = Table(table_name, metadata, autoload_with=engine)
            table.drop(engine)
            print(f"Table '{table_name}' has been deleted from database '{database}'.")
        else:
            print(f"Table '{table_name}' does not exist in database '{database}'.")
    except Exception as e:
        print(f"Error deleting table '{table_name}': {e}")