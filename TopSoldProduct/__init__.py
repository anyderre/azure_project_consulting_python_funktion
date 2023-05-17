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
    logging.info('Getting the number of sales per product.')

    connection_string = os.environ['AZURE_FUNCTION_CONNECTION_STRING']
    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)

    with engine.begin() as conn:
        query = "SELECT top 5 product_id, sum(product_price * product_quantity) as total_sales FROM [dbo].[bestellung] group by product_id order by total_sales desc"

        data = pd.read_sql_query(sa.text(query), conn)

        parsed = json.loads(data.to_json(orient='records'))
        return func.HttpResponse(
            json.dumps(parsed),
            status_code=200
        )
