import database_credentials as dc
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text

def copy_the_dataset_to_table(table_name, connection, cursor):
    # to copy from a file:
    copy_from = f"""
    COPY {table_name} 
    FROM STDIN
    WITH (
        FORMAT CSV,
        DELIMITER ',',
        HEADER
    );
    """
    # running the copy statement
    path = f'{dc.directory_path}/{table_name}_dataset.csv'
    with open(path) as f:
         cursor.copy_expert(copy_from, file=f)

    # don't forget to commit the changes.
    connection.commit()
    
def main():
    # Create a connection to postgres database. All sensitive information are hidden.
    #engine = create_engine(f"{dc.dialect}://{dc.username}:{dc.password}@{dc.hostname}:{dc.port}/{dc.db_name}")
    engine = create_engine(f"{dc.dialect}://{dc.username}:{dc.password}@{dc.hostname}:{dc.port}/toy_db")

    # get a psycopg2 connection
    connection = engine.connect().connection

    # get a cursor on that connection
    cursor = connection.cursor()

    # create tables
    query = """
    CREATE TABLE IF NOT EXISTS customers (
        "customer_id" VARCHAR(32),
        "customer_unique_id" VARCHAR(32),
        "customer_zip_code_prefix" VARCHAR(5),
        "customer_city" TEXT,
        "customer_state" VARCHAR(2)
    );

    CREATE TABLE IF NOT EXISTS geolocation (
        "geolocation_zip_code_prefix" VARCHAR(5),
        "geolocation_lat" DECIMAL,
        "geolocation_lng" DECIMAL,
        "geolocation_city" TEXT,
        "geolocation_state" VARCHAR(2)
    );

    CREATE TABLE IF NOT EXISTS order_items (
        "order_id" VARCHAR(32),
        "order_item_id" INTEGER,
        "product_id" VARCHAR(32),
        "seller_id" VARCHAR(32),
        "shipping_limit_date" TIMESTAMP,
        "price" DECIMAL,
        "freight_value" DECIMAL
    );

    CREATE TABLE IF NOT EXISTS order_payments (
        "order_id" VARCHAR(32),
        "payment_sequential" INTEGER,
        "payment_type" TEXT,
        "payment_installments" INTEGER,
        "payment_value" DECIMAL
    );

    CREATE TABLE IF NOT EXISTS order_reviews (
        "review_id" VARCHAR(32),
        "order_id" VARCHAR(32),
        "review_score" INTEGER,
        "review_comment_title" TEXT,
        "review_comment_message" TEXT,
        "review_creation_date" DATE,
        "review_answer_timestamp" TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS orders (
        "order_id" VARCHAR(32),
        "customer_id" VARCHAR(32),
        "order_status" TEXT,
        "order_purchase_timestamp" TIMESTAMP,
        "order_approved_at" TIMESTAMP,
        "order_delivered_carrier_date" TIMESTAMP,
        "order_delivered_customer_date" TIMESTAMP,
        "order_estimated_delivery_date" DATE
    );

    CREATE TABLE IF NOT EXISTS product (
        "idx" INTEGER,
        "product_id" VARCHAR(32),
        "product_category_name" TEXT,
        "product_name_lenght" FLOAT,
        "product_description_lenght" FLOAT,
        "product_photos_qty" FLOAT,
        "product_weight_g" FLOAT,
        "product_length_cm" FLOAT,
        "product_height_cm" FLOAT,
        "product_width_cm" FLOAT
    );

    CREATE TABLE IF NOT EXISTS sellers (
        "seller_id" VARCHAR(32),
        "seller_zip_code_prefix" VARCHAR(5),
        "seller_city" TEXT,
        "seller_state" VARCHAR(2)
    );
    """
    print("Create new tables......")
    engine.execute(query)
    
    # importing csv files to existing table
    tables = [
        "customers", "geolocation", "order_items", "order_payments",
        "order_reviews", "orders", "product", "sellers"
    ]
    print("Copying csv files to tables......")
    for table_name in tables:
        copy_the_dataset_to_table(table_name, connection, cursor)
    
    # add primary and foreign keys
    query = """
    ALTER TABLE customers ADD PRIMARY KEY (customer_id);
    ALTER TABLE orders ADD PRIMARY KEY (order_id);
    ALTER TABLE product ADD PRIMARY KEY (product_id);
    ALTER TABLE sellers ADD PRIMARY KEY (seller_id);
    
    ALTER TABLE order_items ADD FOREIGN KEY (order_id) REFERENCES orders;
    ALTER TABLE order_items ADD FOREIGN KEY (product_id) REFERENCES product;
    ALTER TABLE order_items ADD FOREIGN KEY (seller_id) REFERENCES sellers;

    ALTER TABLE order_payments ADD FOREIGN KEY (order_id) REFERENCES orders(order_id);
    ALTER TABLE order_reviews ADD FOREIGN KEY (order_id) REFERENCES orders(order_id);
    ALTER TABLE orders ADD FOREIGN KEY (customer_id) REFERENCES customers(customer_id);
    """
    print("Add primary keys and foreign keys......")
    engine.execute(query)
    
    print("Completed")
        
main()