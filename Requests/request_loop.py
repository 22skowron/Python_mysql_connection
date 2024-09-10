import aiohttp
import asyncio
from logger import logger_requests

url_products = "http://127.0.0.1:5000/query_products"
url_customers = "http://127.0.0.1:5000/insert_customer"

dummy_customer = {"first_name": "Josh", "last_name": "Joshnson", "email": "john.joshnson@example.com", "employee_id": 1}
dummy_category = "Nike shoes"

async def request_products(session, category, index):
    logger_requests.info(f"↖️ {index}. Sending POST request for category: {category}...")
    try:
        async with session.post(url_products, json={"category": category}) as response:
            status = response.status
            data = await response.json()
            logger_requests.info(f"↘️ {index}. R status: {status}, R json: {data}")

    except Exception as e:
        logger_requests.error(f"❌ {index}. Error sending request for category {category}.")
        logger_requests.error(f"❌ {index}. Error message: {e}")


async def request_customers(session, customer, index):
    logger_requests.info(f"↖️ {index}. Sending POST request for customer: {customer['first_name']}...")
    try:
        async with session.post(url_customers, json=customer) as response:
            status = response.status
            data = await response.json()
            logger_requests.info(f"↘️↘️ {index}. R status: {status}, R json: {data}")

    except Exception as e:
        logger_requests.error(f"❌ {index}. Error sending request for customer {customer['first_name']}.")
        logger_requests.error(f"❌ {index}. Error message: {e}")


async def send_requests():
    async with aiohttp.ClientSession() as session:
        async with asyncio.TaskGroup() as tg:

            for i in range(10):
                # tg.create_task(request_products(session, dummy_category, i))
                tg.create_task(request_customers(session, dummy_customer, i))


# Run the asynchronous code
if __name__ == '__main__':
    asyncio.run(send_requests())
