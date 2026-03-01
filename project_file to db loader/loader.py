import pandas as pd
import os
from sqlalchemy import create_engine
import configparser

def get_engine():
    config = configparser.ConfigParser()
    config.read('config.ini')

    creds = config['mysql']

    url = f"mysql+pymysql://{creds['users']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"

    return create_engine(url)

def load_files_to_db(data_dir):
    engine = get_engine()

    for file in os.listdir(data_dir):
        if file.endswith('.csv'):
            table_name = os.path.splitext(file)[0]

            print(f"Loading {file} into table {table_name}")

            file_path = os.path.join(data_dir, file)

            df = pd.read_csv(file_path)

            df.to_sql(table_name, con=engine, if_exists='replace', index=False)