from logger import logger_mysql
import asyncio
import aiomysql
from dotenv import load_dotenv
from typing import Optional
import os

load_dotenv()

# Global MySQL connection pool
pool: Optional[aiomysql.Pool] = None


async def initialize_connection_pool():
    global pool
    try:
        pool = await aiomysql.create_pool(
            minsize=2,
            maxsize=2,
            connect_timeout=10,
            host=os.environ.get('MYSQL_HOST'),
            user=os.environ.get('MYSQL_USER'),
            port=int(os.environ.get('MYSQL_PORT')),
            password=os.environ.get('MYSQL_PASSWORD'),
            db=os.environ.get('MYSQL_DATABASE')
        )

        # Create test connection
        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    result = await cur.fetchone()
                    if result[0] == 1:
                        logger_mysql.info("✅ Successfully established a test connection. Returning pool object.")
                    else:
                        raise RuntimeError("result[0] != 1")

        except Exception as e:
            logger_mysql.critical(
                "❌ Failed to establish a test connection. Pool probably isn't working correctly.")
            logger_mysql.critical(f'Error message: {e}')

        return pool

    except Exception as e:
        logger_mysql.critical('❌ An error occurred during creating a connection pool.')
        logger_mysql.critical(f'Error message: {e}')
        return None


async def close_connection_pool():
    global pool
    if pool:
        pool.close()
        await pool.wait_closed()
        logger_mysql.info("🚪️ MySQL connection pool closed.")


def get_pool():
    global pool
    return pool

