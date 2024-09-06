from logger import logger_main
from dotenv import load_dotenv
import mysql.connector
import os

load_dotenv()


def connect_to_mysql():
    connection = None
    try:
        # Establish the connection
        connection = mysql.connector.connect(
            host=os.environ.get('MYSQL_HOST'),
            user=os.environ.get('MYSQL_USER'),
            port=os.environ.get('MYSQL_PORT'),
            password=os.environ.get('MYSQL_PASSWORD'),
            database=os.environ.get('MYSQL_DATABASE'),
        )

        if connection.is_connected():
            logger_main.info("✅ Successfully connected to the database.")

            cursor = connection.cursor()
            return connection, cursor

        else:
            raise RuntimeError("Database connection established but is not active.")

    except Exception as e:
        logger_main.critical("❌ Failed to establish a database connection")
        logger_main.critical(f"Error message: {e}")
        if connection:
            connection.close()
        return None, None


def insert_query_data_mysql(user_id, user_query, ai_answer):
    try:
        insert_query = "INSERT INTO queries (user_id, query, answer) VALUES (%s, %s, %s)"
        insert_data = (user_id, user_query, ai_answer)

        cursor.execute(insert_query, insert_data)
        connection.commit()

        # Check if the insert was successful
        if cursor.rowcount > 0:
            logger_main.info("✅ New record inserted successfully.")
        else:
            logger_main.warning("❌ Failed to insert a new record.")

    except Exception as e:
        logger_main.warning("❌ An error occurred during performing an insert query.")
        logger_main.warning(f"Error message: {e}")


def get_data_mysql(table_name):
    try:
        select_query = f"SELECT * FROM {table_name}"

        cursor.execute(select_query)
        results = cursor.fetchall()

        logger_main.info(f"✅ Select query executed successfully.")
        return results

    except Exception as e:
        logger_main.warning("❌ An error occurred during performing a select query.")
        logger_main.warning(f"Error message: {e}")
        return None


def get_user_id(whatsapp_number_id):
    try:
        query = "SELECT id FROM users WHERE whatsapp_number_id = %s"
        data = (whatsapp_number_id,)

        cursor.execute(query, data)
        result = cursor.fetchone()
        print(result[0])

        # If a result is found, return the user_id, otherwise return None
        if result:
            return result[0]

        else:
            raise RuntimeError("No matching results found.")

    except Exception as e:
        logger_main.warning("❌ An error occurred during fetching a user id.")
        logger_main.warning(f"Error message: {e}")
        return None


connection, cursor = connect_to_mysql()
# insert_query_data_mysql(3, "How many bits are in a byte?", "There are 8 bits in a byte.")
#
# results = get_data_mysql("queries")
# if results:
#     print(f"TYPE OF RESULTS: {type(results)}\n")
#     for row in results:
#         print(row)

get_user_id(12345678901234567)