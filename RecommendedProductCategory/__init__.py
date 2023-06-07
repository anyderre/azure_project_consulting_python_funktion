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
    logging.info(
        'Getting the last 3 suggested products based on product viewed')
    connection_string = os.environ['AZURE_FUNCTION_CONNECTION_STRING']

    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)

    # visitor_id = req.params.get('visitor_id')
    # session_id = req.params.get('session_id')
    # # user_id = req.params.get('user_id')
    # event_type = req.params.get('event_type')

    with engine.begin() as conn:
        # .format(
        query = "SELECT TOP (3) * FROM [dbo].[category_predictions] ORDER BY dateTime DESC"
        # session_id, visitor_id, event_type)

        data = pd.read_sql_query(sa.text(query), conn)

        parsed = json.loads(data.to_json(orient='records'))
        return func.HttpResponse(
            json.dumps(parsed),
            status_code=200
        )
