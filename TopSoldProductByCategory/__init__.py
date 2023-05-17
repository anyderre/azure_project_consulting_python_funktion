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
    logging.info('Getting the top product visited product sold by category.')
    connection_string = os.environ['AZURE_FUNCTION_CONNECTION_STRING']

    connection_url = URL.create(
        "mssql+pyodbc", query={"odbc_connect": connection_string})
    engine = create_engine(connection_url)

    visitor_id = req.params.get('visitor_id')
    session_id = req.params.get('session_id')
    # user_id = req.params.get('user_id')
    event_type = req.params.get('event_type')

    # print (os.environ['IsEncrypted'])
    with engine.begin() as conn:
        query = "SELECT b.*, p.images FROM bestellung b LEFT JOIN Product p on p.productId = b.product_id"

        # data = pd.read_sql(query, conn)
        data = pd.read_sql_query(sa.text(query), conn)

        product_images = data[['product_id', 'images']].drop_duplicates()

        product_counts = data.groupby(
            ['product_category', 'product_id', 'product_name']).size().reset_index(name='count')

        sorted_product = product_counts.sort_values(
            by=['product_category', 'count'], ascending=[True, False])

        sum_sold = data.groupby(['product_category', 'product_id', 'product_name'])[
            'order_total'].sum().reset_index()

        sorted_sum = sum_sold.sort_values(
            by=['order_total', 'product_category'], ascending=False)

        ####################### Getting the user Viewed Categories #################################

        sql_query = "SELECT TOP 10 category, title FROM events e WHERE e.sessionId <> '{}'  AND e.visitorId = '{}' AND eventType='{}' ORDER BY e.dateTime DESC".format(
            session_id, visitor_id, event_type)
        product_views = pd.read_sql(sql_query, conn)

        sorted_sum = sorted_sum.rename(
            columns={'product_category': 'category'})

        top_sale_order = sorted_sum[sorted_sum['category'].isin(
            product_views['category'])]

        sorted_product = sorted_product.rename(
            columns={'product_category': 'category'})

        top_most_sold = sorted_product[sorted_product['category'].isin(
            product_views['category'])]

        top_sale_order['count'] = top_most_sold['count']

        images = product_images[product_images['product_id'].isin(
            top_sale_order['product_id'])]

        top_sale_order = top_sale_order.merge(
            images, on='product_id', right_index=False,  how='inner',)

        parsed = json.loads(top_sale_order.to_json(orient='records'))
        return func.HttpResponse(
            json.dumps(parsed),
            status_code=200
        )
