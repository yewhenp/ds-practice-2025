# Import Flask.
# Flask is a web framework for Python.
# It allows you to build a web application quickly.
# For more information, see https://flask.palletsprojects.com/en/latest/
from flask import Flask, request
from flask_cors import CORS

import json
import asyncio

from fraud_detection import check_fraud

# Create a simple Flask app.
app = Flask(__name__)
# Enable CORS for the app.
CORS(app, resources={r'/*': {'origins': '*'}})

# Define a GET endpoint.
@app.route('/', methods=['GET'])
def index():
    """
    Responds with 'Hello, [name]' when a GET request is made to '/' endpoint.
    """
    # Test the fraud-detection gRPC service.
    # response = greet(name='orchestrator')
    # # Return the response.
    # return response
    return "hello from orchestrator"

def transform_results(results: list[dict]):
    return {
        result["service"]: result["data"]
        for result in results
    }

@app.route('/checkout', methods=['POST'])
async def checkout():
    """
    Responds with a JSON object containing the order ID, status, and suggested books.
    """
    # Get request object data to json
    request_data = json.loads(request.data)
    # Print request object data
    print("Request Data:", request_data.get('items'))

    order_id = '12345'

    parallel_results = await asyncio.gather(
        check_fraud(request_data),
    )
    results = transform_results(parallel_results)

    suggested_books = [
        {'bookId': '123', 'title': 'The Best Book', 'author': 'Author 1'},
        {'bookId': '456', 'title': 'The Second Best Book', 'author': 'Author 2'}
    ]

    order_response = {
        'orderId': order_id,
        'suggestedBooks': suggested_books
    }

    if results["fraud_detection"]["is_fraud"]:
        order_response["status"] = "Order Denied"
        order_response["errorMessage"] = results['fraud_detection']['error_message']
    else:
        order_response["status"] = "Order Approved"
    
    return order_response



if __name__ == '__main__':
    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    app.run(host='0.0.0.0')
