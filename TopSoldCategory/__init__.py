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
    logging.info('Getting the number product sold per category.')
    connection_string = os.environ['AZURE_FUNCTION_CONNECTION_STRING']
    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)

    visitor_id = req.params.get('visitor_id')
    session_id = req.params.get('session_id')
    event_type = req.params.get('event_type')

    with engine.begin() as conn:
        query = "SELECT product_category AS category,  SUM(product_price * product_quantity) AS price_sold  FROM [dbo].[bestellung] GROUP BY product_category ORDER BY price_sold DESC"

        category_sold = pd.read_sql_query(sa.text(query), conn)

        sql_query = "SELECT TOP 1000 category, title FROM events e WHERE e.sessionId <> '{}'  AND e.visitorId = '{}' AND eventType='{}' ORDER BY e.dateTime DESC".format(
            session_id, visitor_id, event_type)
        product_views = pd.read_sql(sql_query, conn)

        user_categories = product_views['category'].unique()

        # Possible Solution
        top_sellers = category_sold[category_sold['category'].isin(
            user_categories)]

        # Alternative
        # top_sellers = .loc[user_categories]

        parsed = json.loads(top_sellers.to_json(orient='records'))
        return func.HttpResponse(
            json.dumps(parsed),
            status_code=200
        )
