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

async def check_fraud(request_data):
    print("Starting check_fraud with user:", request_data.get("user"))
    async with grpc.aio.insecure_channel('fraud_detection:50051') as channel:
        stub = fraud_detection_grpc.FraudDetectionServiceStub(channel)
        user_info = fraud_detection.User(**request_data["user"])
        credit_card_info = fraud_detection.CreditCard(
            number=request_data["creditCard"]["number"],
            expiration_date=request_data["creditCard"]["expirationDate"],
            cvv=request_data["creditCard"]["cvv"]
        )
        items = [fraud_detection.OrderItem(**item) for item in request_data["items"]]
        billing_address_info = fraud_detection.BillingAddress(**request_data["billingAddress"])
        fraud_request = fraud_detection.FraudRequest(
            user=user_info,
            credit_card=credit_card_info,
            user_comment=request_data["userComment"] or "",
            items=items,
            billing_address=billing_address_info,
            shipping_method=request_data["shippingMethod"],
            gift_wrapping=request_data["giftWrapping"],
            terms_accepted=request_data["termsAccepted"]
        )
        # Call the service through the stub object.
        response = await stub.CheckFraud(fraud_request)
    
    if response.is_fraud:
        print("Got answer, response.is_fraud = ", response.is_fraud, ", response.error_message = ", response.error_message)
    else:
        print("Got answer, response.is_fraud = ", response.is_fraud)
    return {
        "service": "fraud_detection",
        "data": { "is_fraud": response.is_fraud, "error_message": response.error_message }
    }
