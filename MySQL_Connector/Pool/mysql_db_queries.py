from mysql_db_config import create_connection_pool, close_connection
from logger import logger_mysql
import mysql.connector
from mysql.connector.errors import Error as MySQL_Error
from mysql.connector.errors import PoolError
import time


# Create connection pool on app init
pool = create_connection_pool()


def insert_customer_mysql(first_name, last_name, email, employee_id):
    connection, cursor = None, None
    try:
        connection = pool.get_connection()
        cursor = connection.cursor()

        query = "INSERT INTO customers (first_name, last_name, email, served_by_employee_id) VALUES (%s, %s, %s, %s)"
        data = (first_name, last_name, email, employee_id)

        cursor.execute(query, data)
        connection.commit()

        # Print lastrowid
        last_row_id = cursor.lastrowid
        logger_mysql.info(f'⚙️ Last row id from the cursor object: {last_row_id}')

        # Check if the insert was successful
        if cursor.rowcount > 0:
            logger_mysql.info("✅ New record inserted successfully.")
            message = "✅ New record inserted successfully."
            return message, 201
        else:
            raise RuntimeError("❌ Failed to insert a new record.")

    except PoolError as e:
        logger_mysql.warning(
            "❌ An error occurred during performing an insert query. Couldn't obtain a connection from the pool.")
        logger_mysql.warning(f"Error message: {e}")
        message = "❌ Failed to insert a new record."
        return message, 500

    except Exception as e:
        logger_mysql.warning("❌ An error occurred during performing an insert query.")
        logger_mysql.warning(f"Error message: {e}")
        message = "❌ Failed to insert a new record."
        return message, 500

    finally:
        close_connection(connection, cursor)


def get_products_mysql(category):
    connection, cursor = None, None
    try:
        connection = pool.get_connection()
        cursor = connection.cursor()

        query = "SELECT * FROM products WHERE category = %s"
        data = (category,)
        cursor.execute(query, data)

        rows = cursor.fetchall()
        if rows:
            message = f"We have the following products which belong to {category} category:\n"
            for row in rows:
                id, product_name, category, price, stock_quantity = row
                message += f"\t{product_name}: ${price}\n"
        else:
            message = f"Unfortunately we don't have any products which belong to {category} category in our catalog."

        # # TEST TEST TEST
        # insert_customer_mysql(
        #     first_name="Alice",
        #     last_name="Johnson",
        #     email="alice.johnson@example.com",
        #     employee_id=1
        # )

        print('\n', message, '\n')
        logger_mysql.info("✅ SELECT query executed successfully.")
        return message, 200

    except PoolError as e:
        logger_mysql.warning(
            "❌ An error occurred during querying the database. Couldn't obtain a connection from the pool.")
        logger_mysql.warning(f"Error message: {e}")
        message = "❌ Failed to retrieve the data."
        return message, 500

    except Exception as e:
        logger_mysql.warning("❌ An error occurred during querying the database.")
        logger_mysql.warning(f"Error message: {e}")
        message = "❌ Failed to retrieve the data."
        return message, 500

    finally:
        close_connection(connection, cursor)