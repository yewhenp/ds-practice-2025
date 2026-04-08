import sys
import os
import threading

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
utils_path = os.path.abspath(os.path.join(FILE, '../../../utils/'))
sys.path.insert(0, utils_path)
from log_utils.logger import setup_logger

from service_wrappers.base_service_wrapper import BaseServiceWrapper, init_grpc_pathes
init_grpc_pathes()


import pb.services.order_details_pb2 as order_details

import pb.services.transaction_verification_pb2 as transaction_verification
import pb.services.transaction_verification_pb2_grpc as transaction_verification_grpc

import pb.services.fraud_detection_pb2_grpc as fraud_detection_grpc
import pb.services.recommendation_system_pb2_grpc as recommendation_system_grpc

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


class TransactionVerificationService(BaseServiceWrapper, transaction_verification_grpc.TransactionVerificationService):
    def __init__(self, service_id, n_services):
        super().__init__(service_id, n_services)
        self.logger = logger

    def _do_verification(self, request, verify_function):
        data = self.order_details.get(request.order_id, None)

        if data is None:
            logger.error(f"Order id {request.order_id} is not found")
            return False, f"Order id {request.order_id} is not found"

        for fn, fname in verify_function:
            result = fn(data)
            if not result:
                return result, f"Verification failed for {fname}"
        return True, ""

    def _send_request_to_service_treaded(self, request, stub_class, connection_string, method_name, result_container, index):
        result_container[index] = self._send_request_to_service(
                    stub_class=stub_class,
                    connection_string=connection_string,
                    method_name=method_name,
                    message=request
                )

    def VerifyItems(self, request, context):
        try:
            logger.info(f"Verifying items for order id {request.order_id} with vector clock {self.vector_clocks[request.order_id]}")
            self._update_vector_clock(request.order_id, request.vector_clock)

            status = order_details.StatusMessage(
                success=True,
                order_id=request.order_id,
                vector_clock=self.vector_clocks[request.order_id]
            )

            # Event FraudDetectionService.CheckKnownFraudUsers
            result_container = [None]
            event1 = threading.Thread(target=self._send_request_to_service_treaded, kwargs={
                "request": request,
                "stub_class": fraud_detection_grpc.FraudDetectionServiceStub,
                "connection_string": "fraud_detection:50051",
                "method_name": "CheckKnownFraudUsers",
                "result_container": result_container,
                "index": 0
            })

            event1.start()
        
            # Event TransactionVerificationService.VerifyItems
            res, err_message = self._do_verification(request, [(lambda data: validate_order_list(data.items), "Validation of order list")])
            status.success = res
            status.error_message = err_message


            event1.join()

            if status.success and result_container[0].status.success:
                return self.VerifyCreditCard(request, context)
            
            if not status.success:
                result = order_details.OrderResponce()
                result.status.CopyFrom(status)
                return result
            else:
                return result_container[0]
        except Exception as e:
            logger.error(f"Failed to do items verification: {str(e)}")
            status.success = False
            status.error_message = f"Failed to do items verification: {str(e)}"

            result = order_details.OrderResponce()
            result.status.CopyFrom(status)
            return result


    def VerifyCreditCard(self, request, context):
        try:
            logger.info(f"Verifying credit card for order id {request.order_id} with vector clock {self.vector_clocks[request.order_id]}")
            self._update_vector_clock(request.order_id, request.vector_clock)

            status = order_details.StatusMessage(
                success=True,
                order_id=request.order_id,
                vector_clock=self.vector_clocks[request.order_id]
            )

            # Event FraudDetectionService.CheckKnownFraudLocations
            result_container = [None]
            event1 = threading.Thread(target=self._send_request_to_service_treaded, kwargs={
                "request": request,
                "stub_class": fraud_detection_grpc.FraudDetectionServiceStub,
                "connection_string": "fraud_detection:50051",
                "method_name": "CheckKnownFraudLocations",
                "result_container": result_container,
                "index": 0
            })

            event1.start()
        
            # Event TransactionVerificationService.VerifyCreditCard
            res, err_message = self._do_verification(request, [
                (lambda data: validate_credit_card(data.credit_card.number), "Validation of credit card number"),
                (lambda data: validate_credit_card_vendor(data.credit_card.number), "Validation of credit card vendor"),
                (lambda data: validate_expiration_date(data.credit_card.expiration_date), "Validation of expiration date"),
                (lambda data: validate_cvv(data.credit_card.cvv), "Validation of CVV")
            ])
            status.success = res
            status.error_message = err_message


            event1.join()

            if status.success and result_container[0].status.success:
                return self.VerifyBillingAddress(request, context)
            
            if not status.success:
                result = order_details.OrderResponce()
                result.status.CopyFrom(status)
                return result
            else:
                return result_container[0]
        except Exception as e:
            logger.error(f"Failed to do items verification: {str(e)}")
            status.success = False
            status.error_message = f"Failed to do items verification: {str(e)}"

            result = order_details.OrderResponce()
            result.status.CopyFrom(status)
            return result

    
    def VerifyBillingAddress(self, request, context):
        try:
            logger.info(f"Verifying billing address for order id {request.order_id} with vector clock {self.vector_clocks[request.order_id]}")
            self._update_vector_clock(request.order_id, request.vector_clock)

            status = order_details.StatusMessage(
                success=True,
                order_id=request.order_id,
                vector_clock=self.vector_clocks[request.order_id]
            )

            # Event FraudDetectionService.CheckGeneralFraud
            result_container = [None]
            event1 = threading.Thread(target=self._send_request_to_service_treaded, kwargs={
                "request": request,
                "stub_class": fraud_detection_grpc.FraudDetectionServiceStub,
                "connection_string": "fraud_detection:50051",
                "method_name": "CheckGeneralFraud",
                "result_container": result_container,
                "index": 0
            })

            event1.start()
        
            # Event TransactionVerificationService.VerifyBillingAddress
            res, err_message = self._do_verification(request, [(lambda data: validate_location(data.billing_address), "Validation of billing address")])
            status.success = res
            status.error_message = err_message


            event1.join()

            if status.success and result_container[0].status.success:
                result = order_details.OrderResponce()
                result.status.CopyFrom(status)
                for i in range(len(result_container)):
                    if len(result_container[i].recommended_books):
                        result.recommended_books.extend(result_container[i].recommended_books)
                        break
                return result

            if not status.success:
                result = order_details.OrderResponce()
                result.status.CopyFrom(status)
                return result
            else:
                return result_container[0]
        except Exception as e:
            logger.error(f"Failed to do items verification: {str(e)}")
            status.success = False
            status.error_message = f"Failed to do items verification: {str(e)}"

            result = order_details.OrderResponce()
            result.status.CopyFrom(status)
            return result


    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    transaction_verification_grpc.add_TransactionVerificationServiceServicer_to_server(TransactionVerificationService(0, 3), server)
    port = "50052"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logger.info(f"Server started. Listening on port {port}.")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()