import logging
import pandas as pd
from datetime import datetime
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy as sa
import json
import os

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Getting the number of views per category.')

    connection_string = os.environ['AZURE_FUNCTION_CONNECTION_STRING']
    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)

    with engine.begin() as conn:
        start_date = datetime(year=2022, month=12, day=31).strftime("%m/%d/%Y")
        end_date = datetime.now().strftime("%m/%d/%Y")
        query = "SELECT category, COUNT(*) AS views FROM events WHERE dateTime BETWEEN '{}' AND '{}' GROUP BY category".format(
            start_date, end_date)

        data = pd.read_sql_query(sa.text(query), conn)

        category_views = data.groupby('category').sum().reset_index()

        # Alternative
        # category_views = data.pivot_table(values='category', aggfunc=[sum], fill_value=0, margins= False)

        category_views = category_views.sort_values(
            by=['views'], ascending=False)

        top_categories = category_views.head(10)

        print(type(top_categories))
        parsed = json.loads(top_categories.to_json(orient='records'))
        return func.HttpResponse(
            json.dumps(parsed),
            status_code=200
        )
