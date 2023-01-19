""" Module docstring """
#!/usr/bin/env python

import os
import argparse
import pandas as pd
from sqlalchemy import create_engine


def main(params):
    """Function docstring"""
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table = params.table
    url = params.url

    os.system(f"wget {url} -O output.parquet")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    engine.connect()

    dataframe = pd.read_parquet("output.parquet", engine="pyarrow")
    dataframe.columns = map(str.lower, dataframe.columns.to_list())

    dataframe.to_sql(
        name=table, con=engine, if_exists="append", index=False, chunksize=5000
    )
    print("Data inserted successfully")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest parquet data to Postgres")

    parser.add_argument("--user", required=True, type=str, help="Postgres user")
    parser.add_argument("--password", required=True, type=str, help="Postgres password")
    parser.add_argument("--host", required=True, type=str, help="Postgres host")
    parser.add_argument("--port", required=True, type=str, help="Postgres port")
    parser.add_argument("--database", required=True, type=str, help="Postgres database")
    parser.add_argument("--table", required=True, type=str, help="Postgres table")
    parser.add_argument("--url", required=True, type=str, help="Parquet file url")

    args = parser.parse_args()

    main(args)
