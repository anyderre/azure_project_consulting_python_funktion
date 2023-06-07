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
    logging.info('Getting the best product by category.')
    connection_string = os.environ['AZURE_FUNCTION_CONNECTION_STRING']
    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)

    category = req.params.get('category')
    with engine.begin() as conn:
        sql_query = "SELECT TOP 3 * FROM [dbo].[product] where category = '{}' and reviews_count > 0 ORDER BY average_rating DESC, reviews_count DESC".format(
            category.replace("'", "''"))
        products = pd.read_sql(sql_query, conn)

        parsed = json.loads(products.to_json(orient='records'))
        return func.HttpResponse(
            json.dumps(parsed),
            status_code=200
        )
