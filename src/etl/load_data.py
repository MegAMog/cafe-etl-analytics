import psycopg2 as psycopg
import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
import os

import sqlalchemy.exc

#Convert DataFrame object to a list and load it into the database
def load_dataframe_converted_to_list(table_name:str, column_names:list[str], data:pd.DataFrame):
    # Load environment variables
    load_dotenv()

    host_name = os.environ.get("POSTGRES_HOST")
    database_name = os.environ.get("POSTGRES_DB")
    user_name = os.environ.get("POSTGRES_USER")
    user_password = os.environ.get("POSTGRES_PASSWORD")

    try:
        #Establish connection with DB and INSERT rows from DataFrame object
        with psycopg.connect(
            host=host_name,
            dbname=database_name,
            user=user_name,
            password=user_password
        ) as connection:
            
            cursor = connection.cursor()

            columns = ', '.join(column_names)
            placeholders = ', '.join(['%s'] * len(column_names))
            
            sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders});
            """
            
            data_list = data.values.tolist()
            cursor.executemany(sql, data_list)
            connection.commit()

            cursor.close()
        
    except Exception as ex:
        message=str(ex)
        if 'duplicate key value' in message:
            print("Row with this unique field already exists. Please check your input.")

            #https://www.geeksforgeeks.org/python/python-get-the-string-after-occurrence-of-given-substring/
            start_idx=message.find("DETAIL:")
            details = message[start_idx + len("DETAIL:"):] if start_idx != -1 else ""
            if details:
                print(f"Details: {details}")

        else:
            print('Failed to:', ex)

 

#Load DataFrame it into the database
# https://www.geeksforgeeks.org/python/how-to-insert-a-pandas-dataframe-to-an-existing-postgresql-table/
# https://www.linkedin.com/pulse/import-data-postgres-table-using-pandas-itversity-gjzzc/
def load_dataframe(table_name:str, column_names:list[str], data:pd.DataFrame):
    # Load variables from .env into environment
    load_dotenv()

    host_name = os.environ.get("POSTGRES_HOST")
    database_name = os.environ.get("POSTGRES_DB")
    user_name = os.environ.get("POSTGRES_USER")
    user_password = os.environ.get("POSTGRES_PASSWORD")


    conn_string = f'postgresql+psycopg://{user_name}:{user_password}@{host_name}:5432/{database_name}'
    engine = sqlalchemy.create_engine(conn_string)

    with engine.connect() as connection:
        data.columns = column_names

        try:
            data.to_sql(
                table_name, 
                con=connection, 
                if_exists='append', 
                index=False,
                method='multi')
            
            print(f"Data has been successfully uploaded to {table_name}.\n")

        except sqlalchemy.exc.IntegrityError:
            print(f"Failed to insert into {table_name} due to unique constraint violation.\n")

        except sqlalchemy.exc.StatementError:
            print(f"Failed to insert into {table_name} due to data type mismatch.\n")
        
        except sqlalchemy.exc.SQLAlchemyError as ex:
            print('Failed to insert due to SQLAlchemy error:', ex)
