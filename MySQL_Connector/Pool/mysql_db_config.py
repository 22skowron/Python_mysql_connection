from logger import logger_mysql
from dotenv import load_dotenv
from mysql.connector import pooling
from mysql.connector.errors import Error as MySQL_Error
import os

load_dotenv()


def create_connection_pool():
    connection = None
    try:
        # Establish the connection
        pool = pooling.MySQLConnectionPool(
            pool_name='mysql_pool',
            pool_size=5,
            pool_reset_session=True,
            connection_timeout=15,
            host=os.environ.get('MYSQL_HOST'),
            user=os.environ.get('MYSQL_USER'),
            port=os.environ.get('MYSQL_PORT'),
            password=os.environ.get('MYSQL_PASSWORD'),
            database=os.environ.get('MYSQL_DATABASE'),
        )

        # Create test connection
        connection = pool.get_connection()
        if connection.is_connected():
            logger_mysql.info("✅ Successfully established a test connection. Returning pool object.")
        else:
            logger_mysql.critical("❌ Failed to establish a test connection. Pool probably isn't working correctly.")

        return pool

    except (MySQL_Error, Exception) as e:
        logger_mysql.critical('❌ An error occurred during creating a connection pool.')
        logger_mysql.critical(f'Error message: {e}')
        return None

    finally:
        close_connection(connection)


def close_connection(connection=None, cursor=None):
    try:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    except (MySQL_Error, Exception) as e:
        logger_mysql.warn('❌ An error occurred during closing the connection.')
        logger_mysql.warn(f'Error message: {e}')