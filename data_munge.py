import sqlite3 as sq
import numpy as np
import pandas as pd

# connect to the database
shipment_connection = sq.connect('shipment_database.db')
shipment_cursor = shipment_connection.cursor()

# import the data
shipping_data_0 = pd.read_csv('data/shipping_data_0.csv')
shipping_data_1 = pd.read_csv('data/shipping_data_1.csv')
shipping_data_2 = pd.read_csv('data/shipping_data_2.csv')


def add_products_and_shipment_info(shipping_data, database_cursor, database_connection):
    for row in range(len(shipping_data)):
        # add the products from the dataset into the database
        try:
            database_cursor.execute("INSERT INTO product ('name') VALUES ('{}')".format(shipping_data['product'][row]))
            database_connection.commit()
        except sq.IntegrityError:
            pass

        # add the shipments from the dataset into the database
        product_id_sql = "SELECT * FROM product WHERE name = '{}'".format(shipping_data['product'][row])
        product_id = database_cursor.execute(product_id_sql).fetchone()[0]
        quantity = shipping_data['product_quantity'][row]
        origin = shipping_data['origin_warehouse'][row]
        destination = shipping_data['destination_store'][row]

        try:
            sql_exec = "INSERT INTO shipment ('product_id','quantity','origin','destination')" \
                       "VALUES ('{}','{}','{}','{}')"
            sql_exec = sql_exec.format(product_id, quantity, origin, destination)
            database_cursor.execute(sql_exec)
            database_connection.commit()
        except sq.IntegrityError:
            pass


add_products_and_shipment_info(shipping_data_0, shipment_cursor, shipment_connection)

# add the product quantities to dataset 1
last_unique_entry_index = 0
shipping_data_1['product_quantity'] = np.zeros(len(shipping_data_1))
shipping_data_1['origin_warehouse'] = pd.Series()
shipping_data_1['origin_warehouse'].fillna("", inplace=True)
shipping_data_1['destination_store'] = pd.Series()
shipping_data_1['destination_store'].fillna("", inplace=True)

for i in range(len(shipping_data_1)):
    if not shipping_data_1[['shipment_identifier', 'product']].duplicated()[i]:
        last_unique_entry_index = i
    shipping_data_1['product_quantity'][last_unique_entry_index] += 1

# remove duplicate lines from dataset 1
shipping_data_1 = \
    shipping_data_1.drop_duplicates(keep=False)[shipping_data_1['product_quantity'] > 0.0].reset_index(drop=True)

# combine the origin and destination information from dataset 2 with the product information in dataset 1
for i in range(len(shipping_data_1)):
    shipment_id = shipping_data_1['shipment_identifier'][i]
    shipping_data_1['origin_warehouse'][i] = \
        shipping_data_2['origin_warehouse'][shipping_data_2['shipment_identifier'] == shipment_id].tolist()[0]
    shipping_data_1['destination_store'][i] = \
        shipping_data_2['destination_store'][shipping_data_2['shipment_identifier'] == shipment_id].tolist()[0]

add_products_and_shipment_info(shipping_data_1, shipment_cursor, shipment_connection)

# close connection to the database
# the database now has 45 rows of product info, and 154 rows of shipment info
shipment_cursor.close()
shipment_connection.close()
