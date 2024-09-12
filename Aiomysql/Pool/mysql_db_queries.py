import time

from logger import logger_mysql, logger_main
from mysql_db_config import initialize_connection_pool, close_connection_pool, get_pool
import asyncio


async def insert_customer_mysql(first_name, last_name, email, employee_id):
    conn, pool = None, None

    logger_mysql.info("⚙️ Inserting a new customer...")
    await asyncio.sleep(3)
    try:
        pool = get_pool()
        logger_mysql.info(f"Pool size before conn block: 🧵 Pool size - {pool.size}, 🧵 Pool freesize - {pool.freesize}, 🧵 Pool object - {pool}")

        # SET MAX WAIT TIME for acquiring the connection
        try:
            conn = await asyncio.wait_for(pool.acquire(), timeout=2)
        except asyncio.TimeoutError:
            logger_mysql.error("❌ Timed out while waiting to acquire a connection from the pool.")
            message = "❌ Timed out while waiting to acquire a connection from the pool."
            return message, 500

        logger_mysql.info(
            f"Pool size inside conn block: 🧵 Pool size - {pool.size}, 🧵 Pool freesize - {pool.freesize}, 🧵 Pool object - {pool}")

        logger_mysql.info(f"🥱 Sleeping inside of connection... 🔗 Connection - {conn} 🔗")
        await asyncio.sleep(3)

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

    finally:
        if pool and conn:
            try:
                pool.release(conn)
            except Exception as e:
                logger_mysql.error("❌ An error occurred during releasing the connection.")
                logger_mysql.error(f"Error message: {e}")

        logger_mysql.info(f"Pool size after conn block: 🧵 Pool size - {pool.size}, 🧵 Pool freesize - {pool.freesize}, 🧵 Pool object - {pool}")


async def get_products_mysql(category):
    conn, pool = None, None

    logger_mysql.info("⚙️ Getting products for requested category...")
    await asyncio.sleep(3)
    try:
        pool = get_pool()

        # SET MAX WAIT TIME for acquiring the connection
        try:
            conn = await asyncio.wait_for(pool.acquire(), timeout=4)
        except asyncio.TimeoutError:
            logger_mysql.error("❌ Timed out while waiting to acquire a connection from the pool.")
            message = "❌ Timed out while waiting to acquire a connection from the pool."
            return message, 500

        logger_mysql.info(f"🥱 Sleeping inside of connection... 🔗 Connection - {conn} 🔗")
        await asyncio.sleep(3)

        async with conn.cursor() as cur:
            query = "SELECT * FROM products WHERE category = %s"
            data = (category,)
            await cur.execute(query, data)

            rows = await cur.fetchall()
            if rows:
                message = f"We have the following products which belong to {category} category:"
                for row in rows:
                    id, product_name, category, price, stock_quantity = row
                    message += f" 💠{product_name}: ${price}"
            else:
                message = f"Unfortunately we don't have any products which belong to {category} category in our catalog."

            logger_mysql.info("✅ SELECT query executed successfully.")
            return message, 200

    except Exception as e:
        logger_mysql.error("❌ An error occurred during querying the database.")
        logger_mysql.error(f"Error message: {e}")
        message = "❌ Failed to retrieve the data."
        return message, 500

    finally:
        if pool and conn:
            try:
                pool.release(conn)
            except Exception as e:
                logger_mysql.error("❌ An error occurred during releasing the connection.")
                logger_mysql.error(f"Error message: {e}")


async def insert_customer_mysql_without_timeout(first_name, last_name, email, employee_id):
    logger_mysql.info("⚙️ Inserting a new customer...")
    await asyncio.sleep(3)
    try:
        pool = get_pool()

        async with pool.acquire() as conn:
            logger_mysql.info(f"🥱 Sleeping inside of connection... 🔗 Connection - {conn} 🔗")
            await asyncio.sleep(2)

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


async def get_products_mysql_without_timeout(category):
    logger_mysql.info("⚙️ Getting products for requested category...")
    await asyncio.sleep(5)
    try:
        pool = get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = "SELECT * FROM products WHERE category = %s"
                data = (category,)
                await cur.execute(query, data)

                rows = await cur.fetchall()
                if rows:
                    message = f"We have the following products which belong to {category} category:"
                    for row in rows:
                        id, product_name, category, price, stock_quantity = row
                        message += f" 💠{product_name}: ${price}"
                else:
                    message = f"Unfortunately we don't have any products which belong to {category} category in our catalog."

                logger_mysql.info("✅ SELECT query executed successfully.")
                return message, 200

    except Exception as e:
        logger_mysql.error("❌ An error occurred during querying the database.")
        logger_mysql.error(f"Error message: {e}")
        message = "❌ Failed to retrieve the data."
        return message, 500


async def print_pool_queries():
    # Access the pool using get_pool
    pool_test = get_pool()
    print("Queries Pool Test:", pool_test)
