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
import pb.services.commit_protocol_pb2 as commit_protocol_pb2

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
        self.orders_table = {}

    def ReadData(self, request, context):
        with self.lock:
            key = request.book_key
            value = self.db.get(key, 0)
            logger.info(f"ReadData: key={key}, value={value}")
            return commit_protocol_pb2.BookDataMessage(book_key=key, stock_value=value)

    def _nslookup(self):
        ip_list = []
        ais = socket.getaddrinfo("database",0,0,0,0)
        for result in ais:
            ip_list.append(result[-1][0])
            ip_list = list(set(ip_list)) 
        logger.info(f"_nslookup: resolved IP addresses for 'database': {ip_list}")
        return ip_list
    
    def Prepare(self, request, context):
        if request.do_impl:
            with self.lock:
                self.orders_table[request.book_key] = (request, True)
                logger.info(f"Prepare (impl): key={request.book_key}, value={request.stock_value}")
                return commit_protocol_pb2.CommitStatus(prepare=True)
        else:
            request_impl = commit_protocol_pb2.BookDataMessage(
                book_key=request.book_key,
                stock_value=request.stock_value,
                do_impl=True
            )
            for ip in self._nslookup():
                responces = []
                logger.info(f"Prepare (leader): sending request to {ip}")
                with grpc.insecure_channel(ip + ":" + port) as channel:
                    stub = database_grpc.DatabaseServiceStub(channel)
                    response = stub.Prepare(request_impl)
                    responces.append(response)

            final_response = commit_protocol_pb2.CommitStatus(
                prepare = all([one_resp.prepare for one_resp in responces]),
                success = all([one_resp.success for one_resp in responces]),
                abort = any([one_resp.abort for one_resp in responces]),
            )

            logger.info(f"Prepare (leader): key={request_impl.book_key}, value={request_impl.stock_value}")
            return final_response


    def Commit(self, in_request, context):
        if in_request.book_key not in self.orders_table:
            return commit_protocol_pb2.CommitStatus(abort=True)
        
        request, prepared = self.orders_table.get(in_request.book_key)
        if not prepared:
            return commit_protocol_pb2.CommitStatus(abort=True)

        if in_request.do_impl:
            with self.lock:
                key = request.book_key
                value = request.stock_value
                self.db[key] = value
                logger.info(f"Commit (impl): key={key}, value={value}")
                return commit_protocol_pb2.CommitStatus(success=True)
        else:
            request_impl = commit_protocol_pb2.BookDataMessage(
                book_key=request.book_key,
                stock_value=request.stock_value,
                do_impl=True
            )
            for ip in self._nslookup():
                responces = []
                logger.info(f"Commit (leader): sending request to {ip}")
                with grpc.insecure_channel(ip + ":" + port) as channel:
                    stub = database_grpc.DatabaseServiceStub(channel)
                    response = stub.Commit(request_impl)
                    responces.append(response)

            final_response = commit_protocol_pb2.CommitStatus(
                prepare = all([one_resp.prepare for one_resp in responces]),
                success = all([one_resp.success for one_resp in responces]),
                abort = any([one_resp.abort for one_resp in responces]),
            )

            logger.info(f"Commit (leader): key={request_impl.book_key}, value={request_impl.stock_value}")
            return final_response


    def Abort(self, request, context):
        if request.do_impl:
            with self.lock:
                if request.book_key in self.orders_table:
                    del self.orders_table[request.book_key]
                return commit_protocol_pb2.CommitStatus(abort=True)
        else:
            request_impl = commit_protocol_pb2.BookDataMessage(
                book_key=request.book_key,
                stock_value=request.stock_value,
                do_impl=True
            )
            for ip in self._nslookup():
                responces = []
                logger.warning(f"Abort (leader): sending request to {ip}")
                with grpc.insecure_channel(ip + ":" + port) as channel:
                    stub = database_grpc.DatabaseServiceStub(channel)
                    response = stub.Abort(request_impl)
                    responces.append(response)

            final_response = commit_protocol_pb2.CommitStatus(abort = True)
            logger.warning(f"Abort (leader): key={request_impl.book_key}, value={request_impl.stock_value}")
            return final_response




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
