import sqlite3
import pandas as pd
from os import listdir


def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        print(e)


def upload_files_to_db(directory, connection):
    tables = listdir(directory)
    for file in tables:
        pd.read_csv(f"task1/{file}").to_sql(name=file.replace('.csv', ''), con=connection)
        print(pd.read_sql(f"SELECT * FROM {file.replace('.csv', '')}", connection))


def query1(connection):
    query = """
    SELECT product_name, COUNT(sales.product_id) AS number_sold
    FROM sales
    INNER JOIN products
    ON products.product_id = sales.product_id
    GROUP BY product_name
    ORDER BY product_name;
    """
    print(query)
    print(pd.read_sql(query, con=connection))


def query2(connection):
    query = """
    SELECT DISTINCT customer_name
    FROM customers
    INNER JOIN sales
    ON customers.customer_id = sales.customer_id
    WHERE sale_amount > 1000;
    """
    print(query)
    print(pd.read_sql(query, con=connection))


def query3(connection):
    query = """
    SELECT category_name, product_name, COUNT(products.product_id) AS number_sold
    FROM sales
    INNER JOIN products
    ON products.product_id = sales.product_id
    INNER JOIN categories
    ON products.category_id = categories.category_id
    GROUP BY category_name, product_name
    ORDER BY category_name
    LIMIT 5;
    """
    print(query)
    print(pd.read_sql(query, con=connection))


def query4(connection):
    query = """
    SELECT strftime('%Y', sale_date) AS year, strftime('%m', sale_date) AS month, COUNT(sale_id) AS sales_this_month
    FROM sales 
    GROUP BY year, month
    """
    queryy = """
    SELECT strftime('%Y', a.sale_date) AS year, strftime('%m', a.sale_date) AS month, COUNT(a.sale_id) AS sales_this_year_so_far
    FROM sales a, sales b
    WHERE strftime('%Y', a.sale_date) = strftime('%Y', b.sale_date)
    AND strftime('%m', a.sale_date) >= strftime('%m', b.sale_date)
    GROUP BY year, month
    """
    print(query)
    print(pd.read_sql(query, con=connection))
    print(queryy)
    print(pd.read_sql(queryy, con=connection))


def query5(connection):
    query = """
    SELECT customer_name, AVG(sale_amount) AS average_sale_amount, RANK() OVER (ORDER BY AVG(sale_amount) DESC) AS rank
    FROM sales
    INNER JOIN customers
    ON sales.customer_id = customers.customer_id
    GROUP BY customer_name
    ORDER BY average_sale_amount DESC;
    """
    print(query)
    print(pd.read_sql(query, con=connection))


if __name__ == '__main__':
    con = create_connection("task1.db")
    upload_files_to_db("task1", con)
    query1(con)
    query2(con)
    query3(con)
    query4(con)
    query5(con)
    con.close()
