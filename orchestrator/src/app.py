# Import Flask.
# Flask is a web framework for Python.
# It allows you to build a web application quickly.
# For more information, see https://flask.palletsprojects.com/en/latest/
import threading

from flask import Flask, request
from flask_cors import CORS
from flask.logging import default_handler
from waitress import serve

import json
import asyncio
import sys
import os
import uuid
import time
from contextlib import contextmanager, asynccontextmanager

import grpc

FILE = __file__ if '__file__' in globals() else os.getenv("PYTHONFILE", "")
utils_path = os.path.abspath(os.path.join(FILE, '../../../utils/'))
sys.path.insert(0, utils_path)
from log_utils.logger import setup_logger
logger = setup_logger("Orchestrator")
logger.addHandler(default_handler)

# import pb.transaction_verification.transaction_verification_pb2 as transaction_verification
# import pb.transaction_verification.transaction_verification_pb2_grpc as transaction_verification_grpc
# import pb.order_details.order_details_pb2 as order_details

import pb.services.transaction_verification_pb2 as transaction_verification
import pb.services.transaction_verification_pb2_grpc as transaction_verification_grpc
import pb.services.fraud_detection_pb2 as fraud_detection
import pb.services.fraud_detection_pb2_grpc as fraud_detection_grpc
import pb.services.recommendation_system_pb2 as recommendation_system
import pb.services.recommendation_system_pb2_grpc as recommendation_system_grpc
import pb.services.order_queue_pb2 as order_queue
import pb.services.order_queue_pb2_grpc as order_queue_grpc
import pb.services.order_details_pb2 as order_details


from telemetry.telemetry import get_telemetry
from opentelemetry.metrics import Observation
tracer, meter = get_telemetry("orchestrator")

successfull_order_counter = meter.create_counter(name="SuccessfulOrders")
order_verification_duration = meter.create_histogram(name="DurationOfTheOrderVerification", unit="ms")




class ProcessCPUUtilization:
    def __init__(self):
        self._lock = threading.Lock()
        self._last_wall = time.monotonic()
        self._last_cpu = time.process_time()
        self._cpu_count = os.cpu_count() or 1

    def observe(self, options):
        with self._lock:
            now_wall = time.monotonic()
            now_cpu = time.process_time()

            wall_delta = now_wall - self._last_wall
            cpu_delta = now_cpu - self._last_cpu

            self._last_wall = now_wall
            self._last_cpu = now_cpu

        if wall_delta <= 0:
            utilization = 0.0
        else:
            utilization = cpu_delta / (wall_delta * self._cpu_count)

        yield Observation(
            max(0.0, min(utilization, 1.0)),
            {"process": "current"},
        )

cpu_util = ProcessCPUUtilization()

meter.create_observable_gauge(
    name="OrchestratorCpuUtilization",
    callbacks=[cpu_util.observe],
    unit="1",
    description="Current process CPU utilization as a fraction of total CPU capacity",
)

class ActiveRequestLoad:
    def __init__(
        self,
        capacity = 1,
        mode = "capacity",
        clamp = True,
        attributes = None,
    ):
        if capacity <= 0:
            raise ValueError("capacity must be positive")

        if mode not in {"capacity", "busy"}:
            raise ValueError("mode must be either 'capacity' or 'busy'")

        self.capacity = capacity
        self.mode = mode
        self.clamp = clamp
        self.attributes = dict(attributes or {})

        self._lock = threading.Lock()
        self._active_requests = 0

        now = time.monotonic()
        self._last_update = now
        self._window_start = now
        self._load_area = 0.0

    def _current_load_locked(self):
        if self.mode == "busy":
            return 1.0 if self._active_requests > 0 else 0.0

        load = self._active_requests / self.capacity

        if self.clamp:
            return min(load, 1.0)

        return load

    def _advance_locked(self, now):
        dt = now - self._last_update

        if dt <= 0:
            return

        current_load = self._current_load_locked()
        self._load_area += current_load * dt
        self._last_update = now

    def start_request(self):
        now = time.monotonic()

        with self._lock:
            self._advance_locked(now)
            self._active_requests += 1

    def finish_request(self):
        now = time.monotonic()

        with self._lock:
            self._advance_locked(now)

            if self._active_requests <= 0:
                raise RuntimeError("finish_request() called without matching start_request()")

            self._active_requests -= 1

    @contextmanager
    def track_request(self):
        self.start_request()
        try:
            yield
        finally:
            self.finish_request()

    @asynccontextmanager
    async def track_request_async(self):
        return self.track_request()

    def observe_load(self, options):
        now = time.monotonic()

        with self._lock:
            self._advance_locked(now)

            elapsed = now - self._window_start

            if elapsed > 0:
                value = self._load_area / elapsed
            else:
                value = self._current_load_locked()

            self._window_start = now
            self._load_area = 0.0

        print(f"ActiveRequestLoad.observe_load: active_requests={self._active_requests}, load={value:.2f}")
        yield Observation(value)

MAX_TREADS = 16

request_load = ActiveRequestLoad(
    capacity=MAX_TREADS
)

meter.create_observable_gauge(
    name="RequestLimitUtilization",
    callbacks=[request_load.observe_load],
    unit="1",
    description="Time-weighted active request load as a fraction of capacity",
)



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

def create_input_order_details(request_data, order_id):
    user_info = order_details.User(**request_data["user"])
    credit_card_info = order_details.CreditCard(
        number=request_data["creditCard"]["number"],
        expiration_date=request_data["creditCard"]["expirationDate"],
        cvv=request_data["creditCard"]["cvv"]
    )
    items = [order_details.OrderItem(**item) for item in request_data["items"]]
    billing_address_info = order_details.BillingAddress(**request_data["billingAddress"])
    return order_details.InputOrderDetails(
        order_id=order_id,
        user=user_info,
        credit_card=credit_card_info,
        user_comment=request_data["userComment"] or "",
        items=items,
        billing_address=billing_address_info,
        shipping_method=request_data["shippingMethod"],
        gift_wrapping=request_data["giftWrapping"],
        terms_accepted=request_data["termsAccepted"]
    )

async def add_to_order_queue(order_details):
    async with grpc.aio.insecure_channel('order_queue:50054') as channel:
        stub = order_queue_grpc.OrderQueueServiceStub(channel)
        response = await stub.Enqueue(order_details)
    logger.info(f"Added order with ID: {order_details.order_id} to the queue")
    return response

async def init_transaction(request_data, order_id, connection_string, stub_class):
    async with grpc.aio.insecure_channel(connection_string) as channel:
        stub = stub_class(channel)
        input_order_details = create_input_order_details(request_data, order_id)
        response = await stub.InitTransaction(input_order_details)
    logger.info(f"InitTransaction - Order ID: {order_id}, Service: {connection_string}, Done")
    return response

async def clear_transaction(order_id, vector_clock, connection_string, stub_class):
    async with grpc.aio.insecure_channel(connection_string) as channel:
        stub = stub_class(channel)
        request = order_details.OperationalMessage(
            order_id=order_id,
            vector_clock=vector_clock
        )
        response = await stub.ClearTransaction(request)
    logger.info(f"ClearTransaction - Order ID: {order_id}, Service: {connection_string}, Done")
    return response

async def call_action(order_id, connection_string, stub_class, method_name, vector_clock=[0,0,0]):
    async with grpc.aio.insecure_channel(connection_string) as channel:
        stub = stub_class(channel)
        fraud_request = order_details.OperationalMessage(
            order_id=order_id,
            vector_clock=vector_clock,
        )
        method = getattr(stub, method_name)
        response = await method(fraud_request)

    return response

def merge_into_general_vector_clock(general_vector_clock, *results):
    for result in results:
        general_vector_clock = [max(general_vector_clock[i], result.status.vector_clock[i]) for i in range(len(general_vector_clock))]
    logger.info(f"Vector clock after operations: {general_vector_clock}")
    return general_vector_clock

async def call_parallel_services(general_vector_clock, *services):
    results = await asyncio.gather(*services)
    all_success = all(result.status.success for result in results)
    if not all_success:
        error_messages = [result.status.error_message for result in results if not result.status.success]
        return general_vector_clock, "; ".join(error_messages), results
    general_vector_clock = merge_into_general_vector_clock(general_vector_clock, *results)
    return general_vector_clock, "", results

async def clear_parallel_services(order_id, vector_clock):
    return await asyncio.gather(
        clear_transaction(order_id, vector_clock, "transaction_verification:50052", transaction_verification_grpc.TransactionVerificationServiceStub),
        clear_transaction(order_id, vector_clock, "fraud_detection:50051", fraud_detection_grpc.FraudDetectionServiceStub),
        clear_transaction(order_id, vector_clock, "recommendation_system:50053", recommendation_system_grpc.RecommendationServiceStub),
    )

@app.route('/checkout', methods=['POST'])
async def checkout():
    """
    Responds with a JSON object containing the order ID, status, and suggested books.
    """
    with request_load.track_request():
        start_time = time.time()
        with tracer.start_as_current_span("process_checkout_request") as span:
            order_id = str(uuid.uuid4())
            span.set_attribute("order_id", order_id)

            with tracer.start_as_current_span("parse_request_data"):
                request_data = json.loads(request.data)
                logger.info(f"Request Data: {request_data.get('items')}")

                general_vector_clock = [0, 0, 0]
                suggested_books = []
                order_response = {
                    'orderId': order_id,
                    'suggestedBooks': suggested_books
                }

            with tracer.start_as_current_span("init_order_validation"):
                _ = await asyncio.gather(
                    init_transaction(request_data, order_id, "transaction_verification:50052", transaction_verification_grpc.TransactionVerificationServiceStub),
                    init_transaction(request_data, order_id, "fraud_detection:50051", fraud_detection_grpc.FraudDetectionServiceStub),
                    init_transaction(request_data, order_id, "recommendation_system:50053", recommendation_system_grpc.RecommendationServiceStub),
                )

            with tracer.start_as_current_span("validate_order"):
                general_vector_clock, error_message, results = await call_parallel_services(
                    general_vector_clock,
                    call_action(order_id, "transaction_verification:50052", transaction_verification_grpc.TransactionVerificationServiceStub, "VerifyItems", vector_clock=general_vector_clock),
                )
            
            if error_message:
                with tracer.start_as_current_span("return_failed_order_validation"):
                    _ = await clear_parallel_services(order_id, general_vector_clock)
                    order_response["status"] = "Order Denied"
                    order_response["errorMessage"] = error_message
                    return order_response

            with tracer.start_as_current_span("prepare_approved_order_ui_response"):
                order_response["status"] = "Order Approved"
                recommended_books = results[0].recommended_books
                order_response["suggestedBooks"] = [{
                    "title": book.title,
                    "author": book.author,
                    "description": book.description
                } for book in recommended_books]

            with tracer.start_as_current_span("clear_order_validation"):
                _ = await clear_parallel_services(order_id, general_vector_clock)

            successfull_order_counter.add(1)

            end_time = time.time()
            order_verification_duration.record((end_time - start_time) * 1000)

            with tracer.start_as_current_span("add_order_to_queue"):
                await add_to_order_queue(create_input_order_details(request_data, order_id))

            return order_response



if __name__ == '__main__':

    # Run the app in debug mode to enable hot reloading.
    # This is useful for development.
    # The default port is 5000.
    # app.run(host='0.0.0.0')
    serve(app, host='0.0.0.0', port=5000, threads=MAX_TREADS)
