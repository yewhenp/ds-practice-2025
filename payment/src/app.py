import sys
import os
import threading

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
utils_path = os.path.abspath(os.path.join(FILE, '../../../utils/'))
sys.path.insert(0, utils_path)

import pb.services.order_details_pb2 as order_details_pb2

import pb.services.payment_pb2_grpc as payment_grpc
import pb.services.commit_protocol_pb2 as commit_protocol_pb2

import grpc
from concurrent import futures

from log_utils.logger import setup_logger
logger = setup_logger("PaymentService")

import socket


port = "50065"

class PaymentService(payment_grpc.PaymentService):
    def __init__(self):
        self.lock = threading.Lock()
        self.orders_table = {}
    
    def Prepare(self, request, context):
        logger.info(f"Prepare : {request}")
        with self.lock:
            self.orders_table[request.book_key] = (request, True)
            logger.info(f"Prepare (impl): key={request.book_key}, value={request.stock_value}")
            return commit_protocol_pb2.CommitStatus(prepare=True)

    def Commit(self, in_request, context):
        logger.info(f"Commit : {in_request}")
        if in_request.book_key not in self.orders_table:
            return commit_protocol_pb2.CommitStatus(abort=True)
        
        request, prepared = self.orders_table.get(in_request.book_key)
        if not prepared:
            return commit_protocol_pb2.CommitStatus(abort=True)

        with self.lock:
            key = request.book_key
            value = request.stock_value
            logger.info(f"Commit (impl): key={key}, value={value}")
            return commit_protocol_pb2.CommitStatus(success=True)

    def Abort(self, request, context):
        logger.warning(f"Abort : {request}")
        with self.lock:
            if request.book_key in self.orders_table:
                del self.orders_table[request.book_key]
            return commit_protocol_pb2.CommitStatus(abort=True)


def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add PaymentService
    payment_grpc.add_PaymentServiceServicer_to_server(PaymentService(), server)
    
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.info("Server started. Listening on port 50065.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
