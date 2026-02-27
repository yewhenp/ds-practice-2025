import sys
import os

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
transaction_verification_grpc_path = os.path.abspath(os.path.join(FILE, '../../../utils/pb/transaction_verification'))
sys.path.insert(0, transaction_verification_grpc_path)
import transaction_verification_pb2 as transaction_verification
import transaction_verification_pb2_grpc as transaction_verification_grpc

utils_path = os.path.abspath(os.path.join(FILE, '../../../utils/'))
sys.path.insert(0, utils_path)
from log_utils.logger import setup_logger

import grpc
from concurrent import futures
import datetime
from geopy.geocoders import Nominatim


logger = setup_logger("TransactionVerificationService")


def validate_credit_card(card_number):
    # credit card verification - Luhn's Algorithm
    # reference - https://dev.to/seraph776/validate-credit-card-numbers-using-python-37j9

    logger.info(f"Verification of credit card number {card_number}")

    if len(card_number) != 16:
        return False

    card_number = [int(num) for num in card_number]
    check_digit = card_number.pop(-1)
    card_number.reverse()
    card_number = [num * 2 if idx % 2 == 0 else num for idx, num in enumerate(card_number)]
    card_number = [num - 9 if idx % 2 == 0 and num > 9 else num for idx, num in enumerate(card_number)]
    card_number.append(check_digit)
    check_sum = sum(card_number)
    return check_sum % 10 == 0


def validate_credit_card_vendor(card_number):
    logger.info(f"Verification of credit card vendor {card_number}")
    is_mastercard = card_number[0] == "5" and (card_number[1] == "1" or card_number[1] == "5")
    is_visa = card_number[0] == "4"
    return is_mastercard or is_visa


def validate_expiration_date(in_date):
    logger.info(f"Verification of credit card date {in_date}")
    expr_month = in_date.split("/")[0].strip()
    expr_year = in_date.split("/")[1].strip()

    if len(expr_month) == 1:
        expr_month = f"0{expr_month}"
    if len(expr_year) == 2:
        expr_year = f"20{expr_year}"

    if len(expr_month) != 2 or len(expr_year) != 4:
        return False
    
    expr_month = int(expr_month)
    expr_year = int(expr_year)

    if expr_month < 1 or expr_month > 12:
        return False
    
    present = datetime.date.today()

    next_month = (expr_month + 1) % 13
    if next_month == 0:
        next_month += 1
        next_year = expr_year + 1
    else:
        next_year = expr_year
        
    next_expr_date = datetime.date(next_year, next_month, 1)

    if present >= next_expr_date:  # credit card expired
        return False
    
    age = (next_expr_date - present).days
    if age > 365 * 3:  # credit card cant have more than 3 years of validity
        return False
    
    return True


def validate_cvv(cvv):
    logger.info(f"Verification of credit card cvv {cvv}")
    return len(cvv) == 3 and cvv.isdigit()


def validate_location(location_obj):
    address = f"{location_obj.street} {location_obj.city} {location_obj.state} {location_obj.country}"
    logger.info(f"Verification of address {address}")

    n_tries = 5

    for i in range(n_tries):
        try:
            geolocator = Nominatim(user_agent="transaction_verification")
            location = geolocator.geocode(address)
            if location is None:
                logger.error("Location is not found")
                return False
            logger.info(f"Location is resolved: {location}")
            return True
        except Exception as e:
            logger.error(f"Location resolution iter {i} error: {e}")
    return False


def validate_order_list(orders):
    logger.info(f"Verification of order list: {orders}")
    
    if len(orders) == 0:
        return False
    
    for elem in orders:
        if elem.quantity < 0 or elem.quantity > 100:
            return False
        if len(elem.name) == 0:
            return False
    
    return True


def verify_transaction(input_data):
    if not validate_credit_card(input_data.credit_card.number):
        return transaction_verification.TransactionVerficationResponse(
            transaction_valid=False, 
            error_message=f"Luhn's Algorithm failed, wrong card number {input_data.credit_card.number}"
        )
    if not validate_credit_card_vendor(input_data.credit_card.number):
        return transaction_verification.TransactionVerficationResponse(
            transaction_valid=False, 
            error_message=f"Unrecognised credit card vendor of {input_data.credit_card.number}, supports only MasterCard or Visa"
        )
    if not validate_expiration_date(input_data.credit_card.expiration_date):
        return transaction_verification.TransactionVerficationResponse(
            transaction_valid=False, 
            error_message=f"Credit card expired {input_data.credit_card.expiration_date}"
        )
    if not validate_cvv(input_data.credit_card.cvv):
        return transaction_verification.TransactionVerficationResponse(
            transaction_valid=False, 
            error_message=f"Invalid CVV {input_data.credit_card.cvv}"
        )
    if not validate_location(input_data.billing_address):
        return transaction_verification.TransactionVerficationResponse(
            transaction_valid=False, 
            error_message=f"Invalid address {input_data.billing_address.street} {input_data.billing_address.city} {input_data.billing_address.state} {input_data.billing_address.country}"
        )
    if not validate_order_list(input_data.items):
        return transaction_verification.TransactionVerficationResponse(
            transaction_valid=False, 
            error_message=f"Invalid order list {input_data.items}"
        )
    return transaction_verification.TransactionVerficationResponse(transaction_valid=True)
    


class TransactionVerificationService(transaction_verification_grpc.TransactionVerificationService):
    def VerifyTransaction(self, request, context):
        try:
            result = verify_transaction(request)
            if not result.transaction_valid:
                logger.error(f"Transaction is invalid: {result.error_message}")
            return result
        except Exception as e:
            logger.error(f"Failed to do transaction verification: {str(e)}")
            return transaction_verification.TransactionVerficationResponse(
                transaction_valid=False, 
                error_message=f"Failed to do transaction verification: {str(e)}"
            )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    transaction_verification_grpc.add_TransactionVerificationServiceServicer_to_server(TransactionVerificationService(), server)
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info(f"Server started. Listening on port {port}.")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()