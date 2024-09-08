from flask import Flask, request
from mysql_db_queries import insert_customer_mysql, get_products_mysql
from logger import logger_main

app = Flask(__name__)


def validate_input(required_properties, data):
    for property in required_properties:
        if not data.get(property):
            return f"Required property: {property} is missing."

    return None


@app.post('/insert_customer')
def insert_customer():
    required_properties = ['first_name', 'last_name', 'email', 'employee_id']
    try:
        data = request.get_json()

        # Shortcircuit if any property is missing
        validation_error = validate_input(required_properties, data)
        if validation_error:
            return {"message": validation_error}, 400

        # Extract data
        first_name = data['first_name']
        last_name = data['last_name']
        email = data['email']
        employee_id = data['employee_id']

        # Insert a customer to the database
        message, status = insert_customer_mysql(first_name, last_name, email, employee_id)
        return {"message": message}, status

    except Exception as e:
        logger_main(f"❌ An error occurred during processing the request.")
        logger_main(f"Error message: {e}.")
        message = "❌ An error occurred during processing the request."
        return {"message": message}, 400


@app.post('/query_products')
def query_products():
    required_properties = ['category']
    try:
        data = request.get_json()

        # Shortcircuit if any property is missing
        validation_error = validate_input(required_properties, data)
        if validation_error:
            return {"message": validation_error}, 400

        # Extract data
        category = data['category']

        # Query the database for the products of that category
        message, status = get_products_mysql(category)
        return {"message": message}, status

    except Exception as e:
        logger_main(f"❌ An error occurred during processing the request.")
        logger_main(f"Error message: {e}.")
        message = "❌ An error occurred during processing the request."
        return {"message": message}, 400


if __name__ == '__main__':
    app.run(port=5000, debug=True)








