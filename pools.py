from logger import logger_main
from dotenv import load_dotenv
from mysql.connector import pooling
import mysql.connector
import os

db_config = {
    'host': os.environ.get('MYSQL_HOST'),
    'user': os.environ.get('MYSQL_USER'),
    'port': os.environ.get('MYSQL_PORT'),
    'password': os.environ.get('MYSQL_PASSWORD'),
    'database': os.environ.get('MYSQL_DATABASE'),
}


def create_connection_pool(pool_name='mysql_pool', pool_size=10, pool_reset_session=True, **kwargs):
    connection = None
    try:
        connection_pool = pooling.MySQLConnectionPool(
            pool_name=pool_name,
            pool_size=pool_size,
            pool_reset_session=pool_reset_session,
            host=kwargs['localhost'],
            user=kwargs['username'],
            password=kwargs['password'],
            database=kwargs['your_database']
        )

        # Create test connection

        connection = connection_pool.get_connection()
        if connection.is_connected():
            logger_main("✅ Successfully connected to the MySQL database.")
            return connection_pool

        else:
            raise RuntimeError("Database connection established but is not active.")

    except Exception as e:
        logger_main.critical('❌ Failed to create a connection pool.')
        logger_main.critical(f'Error message: {e}.')
        return None
    finally:
        if connection:
            connection.close()
        logger_main('Connection returned to the pool.')


def get_user_id(whatsapp_number_id):
    connection = None
    try:
        connection = connection_pool.get_connection()
        cursor = connection.cursor()

        query = "SELECT id FROM users WHERE whatsapp_number_id = %s"
        data = (whatsapp_number_id,)

        cursor.execute(query, data)
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            raise RuntimeError("No matching ids found.")

    except Exception as e:
        logger_main.warning(f"❌ An error occurred during fetching a user id: {e}")
        raise RuntimeError("Cannot perform further queries without user_id")
    finally:
        if connection:
            connection.close()
        logger_main('Connection returned to the pool.')


def insert_data_mysql(whatsapp_number_id, user_query, ai_answer):
    connection = None
    try:
        connection = connection_pool.get_connection()
        cursor = connection.cursor()

        user_id = get_user_id(whatsapp_number_id)
        query = "INSERT INTO queries (user_id, query, answer) VALUES (%s, %s, %s)"
        data = (user_id, user_query, ai_answer)

        cursor.execute(query, data)
        connection.commit()

        if cursor.rowcount > 0:
            logger_main.info("✅ New record inserted successfully into MySQL.")
        else:
            logger_main.warning("❌ Failed to insert a new record into MySQL.")

    except Exception as e:
        logger_main.warning(f"❌ An error occurred during performing an insert query to MySQL: {e}")
    finally:
        if connection:
            connection.close()
        logger_main('Connection returned to the pool.')


connection_pool = create_connection_pool(**db_config)