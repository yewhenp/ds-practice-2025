import sys
import os

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
transaction_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, transaction_verification_grpc_path)
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

import grpc

async def verify_transaction(request_data):
    print("Starting verify_transaction with user:", request_data.get("user"))
    async with grpc.aio.insecure_channel('transaction_verification:50052') as channel:
        stub = transaction_verification_grpc.TransactionVerificationServiceStub(channel)
        credit_card_info = transaction_verification.CreditCard(
            number=request_data["creditCard"]["number"],
            expiration_date=request_data["creditCard"]["expirationDate"],
            cvv=request_data["creditCard"]["cvv"]
        )
        items = [transaction_verification.OrderItem(**item) for item in request_data["items"]]
        billing_address_info = transaction_verification.BillingAddress(**request_data["billingAddress"])
        rpc_request = transaction_verification.TransactionVerficationRequest(
            credit_card=credit_card_info,
            items=items,
            billing_address=billing_address_info
        )
        response = await stub.VerifyTransaction(rpc_request)
    
    if not response.transaction_valid:
        print("Got answer, response.transaction_valid = ", response.transaction_valid, ", response.error_message = ", response.error_message)
    else:
        print("Got answer, response.transaction_valid = ", response.transaction_valid)
    return {
        "service": "transaction_verification",
        "data": { "transaction_valid": response.transaction_valid, "error_message": response.error_message }
    }
