from quart import Quart, request, jsonify
import asyncio
import logging
from MySQL_Connector.Pool.db import init_mysql_pool, insert_customer_mysql, close_mysql_pool
from logger import logger_main

app = Quart(__name__)


# Initialize MySQL pool before the app starts serving requests
@app.before_serving
async def before_serving():
    await init_mysql_pool()


# Close MySQL pool after the app shuts down
@app.after_serving
async def after_serving():
    await close_mysql_pool()


# Function to validate input
def validate_input(required_properties, data):
    for prop in required_properties:
        if prop not in data:
            return f"Missing property: {prop}"
    return None


# Example asynchronous endpoint for inserting a customer
@app.post('/insert_customer')
async def insert_customer():
    logger_main.info('📩 Received a new POST request: [/insert_customer]')

    required_properties = ['first_name', 'last_name', 'email', 'employee_id']
    try:
        data = await request.get_json()

        # Short-circuit if any property is missing
        validation_error = validate_input(required_properties, data)
        if validation_error:
            return jsonify({"message": validation_error}), 400

        # Extract data
        first_name = data['first_name']
        last_name = data['last_name']
        email = data['email']
        employee_id = data['employee_id']

        # Insert a customer into the database asynchronously
        message, status = await insert_customer_mysql(first_name, last_name, email, employee_id)

        # Asynchronous task for future actions (dummy async task here)
        await asyncio.create_task(some_async_task())

        return jsonify({"message": message}), status

    except Exception as e:
        logger_main.warning(f"❌ An error occurred during processing the request: {e}")
        return jsonify({"message": "An error occurred during processing the request."}), 400

# Example of an additional asynchronous task
async def some_async_task():
    # Simulate async work
    await asyncio.sleep(2)
    logger_main.info('Async task completed.')

if __name__ == '__main__':
    app.run()
