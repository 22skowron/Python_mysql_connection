import requests
from logger import logger_requests

url = "http://127.0.0.1:5000/query_products"

categories = ['nature', 'furniture', 'wood', 'art', 'electronics', 'lego']

for category in categories:
    r = requests.post(url=url, json={"category": category})
    logger_requests.info(f"R status: {r.status_code}")
    logger_requests.info(f"R json: {r.json()}")
