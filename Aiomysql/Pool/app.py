from quart import Quart, request
from quart.utils import run_sync
from logger import logger_main
from mysql_db_config import initialize_connection_pool, close_connection_pool, get_pool
from mysql_db_queries import insert_customer_mysql, get_products_mysql, print_pool_queries
import asyncio
import time

app = Quart(__name__)


# Function to validate input
def validate_input(required_properties, data):
    for prop in required_properties:
        if prop not in data:
            return f"Missing property: {prop}"
    return None


# Example of an additional asynchronous task
async def some_async_task():
    logger_main.info("⏱️ Start of the dummy task.")
    await asyncio.sleep(5)
    logger_main.info("⏱️ End of the dummy task.")


# Initialize MySQL pool before the app starts serving requests
@app.before_serving
async def before_serving():
    await initialize_connection_pool()


# Close MySQL pool after the app shuts down
@app.after_serving
async def after_serving():
    await close_connection_pool()


# Example asynchronous endpoint for inserting a customer
@app.post('/insert_customer')
async def insert_customer():
    logger_main.info('📩 Received a new POST request: [/insert_customer]')
    await asyncio.sleep(1)

    required_properties = ['first_name', 'last_name', 'email', 'employee_id']
    try:
        data = await request.get_json()

        # Short-circuit if any property is missing
        validation_error = validate_input(required_properties, data)
        if validation_error:
            return {"message": validation_error}, 400

        # Extract data
        first_name = data['first_name']
        last_name = data['last_name']
        email = data['email']
        employee_id = data['employee_id']

        # Execute the tasks asynchronously
        try:
            async with asyncio.TaskGroup() as tg:
                task1 = tg.create_task(insert_customer_mysql(first_name, last_name, email, employee_id))
                # task2 = tg.create_task(some_async_task())

            message, status = task1.result()

            # # Dummy CPU-bound task wrapped in run_sync
            # logger_main.info("Sleeping for 5 ...")
            # await run_sync(time.sleep)(5)

            return {"message": message}, status

        except Exception as e:
            logger_main.error(f"❌ An error occurred during the execution of asynchronous tasks.")
            logger_main.error(f"Error message: {e}.")
            message = "❌ An error occurred during the execution of asynchronous tasks."
            return {"message": message}, 500

    except Exception as e:
        logger_main.error(f"❌ An error occurred during processing the request.")
        logger_main.error(f"Error message: {e}.")
        message = "❌ An error occurred during processing the request."
        return {"message": message}, 400


@app.post('/query_products')
async def query_products():
    logger_main.info('📩 Received a new POST request: [/query_products]')
    await asyncio.sleep(1)

    required_properties = ['category']
    try:
        data = await request.get_json()

        # Shortcircuit if any property is missing
        validation_error = validate_input(required_properties, data)
        if validation_error:
            return {"message": validation_error}, 400

        # Extract data
        category = data['category']

        # Query the database for the products of that category
        message, status = await get_products_mysql(category)
        return {"message": message}, status

    except Exception as e:
        logger_main(f"❌ An error occurred during processing the request.")
        logger_main(f"Error message: {e}.")
        message = "❌ An error occurred during processing the request."
        return {"message": message}, 400


@app.post('/print_pool')
async def print_pool():
    try:
        # Access the pool using get_pool
        pool_test = get_pool()
        print("App Pool Test:", pool_test)

        # Call the function in mysql_db_queries.py, which also uses get_pool
        await print_pool_queries()

        return { "message": "Done"}, 200
    except Exception as e:
        return { "message": "Error"}, 500


if __name__ == '__main__':
    app.run()
