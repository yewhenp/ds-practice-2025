import sys
import os

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
fraud_detection_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/fraud_detection'))
sys.path.insert(0, fraud_detection_grpc_path)
import fraud_detection_pb2 as fraud_detection
import fraud_detection_pb2_grpc as fraud_detection_grpc

import grpc

# def greet(name='you'):
#     # Establish a connection with the fraud-detection gRPC service.
#     with grpc.insecure_channel('fraud_detection:50051') as channel:
#         # Create a stub object.
#         stub = fraud_detection_grpc.HelloServiceStub(channel)
#         # Call the service through the stub object.
#         response = stub.SayHello(fraud_detection.HelloRequest(name=name))
#     return response.greeting

async def check_fraud(card_number, order_amount):
    print("Startying check_fraud with ", card_number, order_amount)
    async with grpc.aio.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudDetectionServiceStub(channel)
        # Call the service through the stub object.
        response = await stub.CheckFraud(fraud_detection.FraudRequest(card_number=card_number, order_amount=order_amount))
    
    if response.is_fraud:
        print("Got answer, response.is_fraud = ", response.is_fraud, ", response.error_message = ", response.error_message)
    else:
        print("Got answer, response.is_fraud = ", response.is_fraud)
    return {
        "service": "fraud_detection",
        "data": { "is_fraud": response.is_fraud, "error_message": response.error_message }
    }
