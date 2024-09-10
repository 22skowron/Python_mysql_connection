from mysql_db_config import connect_to_mysql
from logger import logger_mysql
import mysql.connector
from mysql.connector.errors import Error as MySQL_Error


def insert_customer_mysql(first_name, last_name, email, employee_id):
    connection, cursor = connect_to_mysql()
    try:
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

    except Exception as e:
        logger_mysql.warning("❌ An error occurred during performing an insert query.")
        logger_mysql.warning(f"Error message: {e}")
        message = "❌ Failed to insert a new record."
        return message, 500

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_products_mysql(category):
    connection, cursor = connect_to_mysql()
    try:
        query = "SELECT * FROM products WHERE category = %s"
        data = (category,)
        cursor.execute(query, data)

        rows = cursor.fetchall()
        if rows:
            message = f"We have the following products which belong to {category} category:"
            for row in rows:
                id, product_name, category, price, stock_quantity = row
                message += f" 💠{product_name}: ${price}"
        else:
            message = f"Unfortunately we don't have any products which belong to {category} category in our catalog."

        print('\n', message, '\n')
        return message, 200

    except Exception as e:
        logger_mysql.warning("❌ An error occurred during querying the database.")
        logger_mysql.warning(f"Error message: {e}")
        message = "❌ Failed to retrieve the data.."
        return message, 500

    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()