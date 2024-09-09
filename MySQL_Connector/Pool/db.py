import aiomysql
import asyncio
from dotenv import load_dotenv
from logger import logger_mysql
import os
from typing import Optional

# Global MySQL connection pool
pool: Optional[aiomysql.Pool] = None
# pool = None

load_dotenv()


# Function to initialize the connection pool
async def init_mysql_pool():
    global pool
    pool = await aiomysql.create_pool(
            minsize=3,
            maxsize=5,
            connect_timeout=10,
            host=os.environ.get('MYSQL_HOST'),
            user=os.environ.get('MYSQL_USER'),
            port=int(os.environ.get('MYSQL_PORT')),
            password=os.environ.get('MYSQL_PASSWORD'),
            db=os.environ.get('MYSQL_DATABASE')
        )
    logger_mysql.info("MySQL connection pool initialized.")


# Function to close the pool (optional, can be used in app cleanup)
async def close_mysql_pool():
    global pool
    if pool:
        pool.close()
        await pool.wait_closed()
        logger_mysql.info("MySQL connection pool closed.")


# Function to insert a customer into the MySQL database
async def insert_customer_mysql(first_name, last_name, email, employee_id):
    logger_mysql.info("⚙️ Inserting a new customer...")
    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = "INSERT INTO customers (first_name, last_name, email, served_by_employee_id) VALUES (%s, %s, %s, %s)"
                data = (first_name, last_name, email, employee_id)

                await cur.execute(query, data)
                await conn.commit()

                # Check if the insert was successful
                if cur.rowcount > 0:
                    logger_mysql.info("✅ New record inserted successfully.")
                    message = "✅ New record inserted successfully."
                    return message, 201
                else:
                    raise RuntimeError("❌ Failed to insert a new record.")

    except Exception as e:
        logger_mysql.error("❌ An error occurred during performing an insert query.")
        logger_mysql.error(f"Error message: {e}")
        message = "❌ Failed to insert a new record."
        return message, 500



async def main():
    await init_mysql_pool()
    await insert_customer_mysql('John', 'Doe', 'jdoe@gmail.com', 2)
    await close_mysql_pool()


asyncio.run(main())