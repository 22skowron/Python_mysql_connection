import aiohttp
import asyncio
from logger import logger_requests

url_products = "http://127.0.0.1:5000/query_products"
url_customers = "http://127.0.0.1:5000/insert_customer"

categories = ['nature', 'furniture', 'wood', 'art', 'electronics', 'lego']
customers = [
    {"first_name": "John", "last_name": "Doe", "email": "johndoe@example.com", "employee_id": 1},
    {"first_name": "Jane", "last_name": "Smith", "email": "janesmith@example.com", "employee_id": 2},
    {"first_name": "Alice", "last_name": "Johnson", "email": "alicejohnson@example.com", "employee_id": 3},
    {"first_name": "Bob", "last_name": "Williams", "email": "bobwilliams@example.com", "employee_id": 4},
    {"first_name": "Charlie", "last_name": "Brown", "email": "charliebrown@example.com", "employee_id": 5}
]


async def request_products(session, category):
    logger_requests.info(f"↖️ Sending POST request for category: {category}...")
    try:
        async with session.post(url_products, json={"category": category}) as response:
            status = response.status
            data = await response.json()
            logger_requests.info(f"R status: {status}, R json: {data}")

    except Exception as e:
        logger_requests.error(f"❌ Error sending request for category {category}.")
        logger_requests.error(f"❌ Error message: {e}")


async def request_customers(session, customer):
    logger_requests.info(f"↖️ Sending POST request for customer: {customer['first_name']}...")
    try:
        async with session.post(url_customers, json=customer) as response:
            status = response.status
            data = await response.json()
            logger_requests.info(f"R status: {status}, R json: {data}")

    except Exception as e:
        logger_requests.error(f"❌ Error sending request for customer {customer['first_name']}.")
        logger_requests.error(f"❌ Error message: {e}")


# Main asynchronous function to send all requests concurrently
async def send_requests():
    async with aiohttp.ClientSession() as session:
        async with asyncio.TaskGroup() as tg:

            for category in categories:
                tg.create_task(request_products(session, category))

            for customer in customers:
                tg.create_task(request_customers(session, customer))


# Run the asynchronous code
if __name__ == '__main__':
    asyncio.run(send_requests())
