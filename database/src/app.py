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

import pb.services.database_pb2 as database_pb2
import pb.services.database_pb2_grpc as database_grpc

import grpc
from concurrent import futures

from log_utils.logger import setup_logger
logger = setup_logger("DatabaseService")

import socket


port = "50060"

class DatabaseService(database_grpc.DatabaseService):
    def __init__(self):
        self.lock = threading.Lock()
        self.db = {
            "Book A": 10,
            "Book B": 5,
        }

    def ReadData(self, request, context):
        with self.lock:
            key = request.book_key
            value = self.db.get(key, 0)
            logger.info(f"ReadData: key={key}, value={value}")
            return database_pb2.DBMessage(book_key=key, stock_value=value)
        

    def _nslookup(self):
        ip_list = []
        ais = socket.getaddrinfo("database",0,0,0,0)
        for result in ais:
            ip_list.append(result[-1][0])
            ip_list = list(set(ip_list)) 
        logger.info(f"_nslookup: resolved IP addresses for 'database': {ip_list}")
        return ip_list

    def WriteData(self, request, context):
        for ip in self._nslookup():
            logger.info(f"WriteData: sending request to {ip}")
            with grpc.insecure_channel(ip + ":" + port) as channel:
                stub = database_grpc.DatabaseServiceStub(channel)
                response = stub.WriteData_Impl(request)
        logger.info(f"WriteData: key={request.book_key}, value={request.stock_value}")
        return response


    def WriteData_Impl(self, request, context):
        with self.lock:
            key = request.book_key
            value = request.stock_value
            self.db[key] = value
            logger.info(f"WriteData_Impl: key={key}, value={value}")
            return order_details_pb2.StatusMessage(success=True)


def serve():
    # Create a gRPC server
    server = grpc.server(futures.ThreadPoolExecutor())
    # Add DatabaseService
    database_grpc.add_DatabaseServiceServicer_to_server(DatabaseService(), server)
    # Listen on port 50060
    
    server.add_insecure_port("[::]:" + port)
    # Start the server
    server.start()
    logger.info("Server started. Listening on port 50060.")
    # Keep thread alive
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
