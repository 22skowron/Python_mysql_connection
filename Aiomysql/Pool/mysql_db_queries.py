from logger import logger_mysql, logger_main
from mysql_db_config import initialize_connection_pool, close_connection_pool, get_pool
import aiomysql
import asyncio
import time


async def insert_customer_mysql(first_name, last_name, email, employee_id):
    logger_mysql.info("⚙️ Inserting a new customer...")
    await asyncio.sleep(4)
    try:
        pool = get_pool()  # Get the current value of `pool` from mysql_db_config.py
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


async def get_products_info(category):
    logger_mysql.info("⚙️ Getting products for requested category...")
    await asyncio.sleep(2)
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = "SELECT * FROM products WHERE category = %s"
                data = (category,)
                await cur.execute(query, data)

                rows = await cur.fetchall()
                if rows:
                    message = f"We have the following products which belong to {category} category:\n"
                    for row in rows:
                        id, product_name, category, price, stock_quantity = row
                        message += f"\t{product_name}: ${price}\n"
                else:
                    message = f"Unfortunately we don't have any products which belong to {category} category in our catalog."

                print('\n', message, '\n')
                logger_mysql.info("✅ SELECT query executed successfully.")
                return message, 200

    except Exception as e:
        logger_mysql.error("❌ An error occurred during querying the database.")
        logger_mysql.error(f"Error message: {e}")
        message = "❌ Failed to retrieve the data."
        return message, 500


async def main_async():
    await initialize_connection_pool()

    logger_main.info("⏱️ Start of db interaction.")
    async with asyncio.TaskGroup() as tg:
        task_insert = tg.create_task(insert_customer_mysql('John', 'Doe', 'jdoe@gmail.com', 2))
        task_query = tg.create_task(get_products_info('furniture'))
    logger_main.info("⏱️ End of db interaction.")

    await close_connection_pool()


async def main():
    await initialize_connection_pool()
    await insert_customer_mysql('John', 'Doe', 'jdoe@gmail.com', 2)
    await get_products_info('furniture')
    await close_connection_pool()


asyncio.run(main_async())
