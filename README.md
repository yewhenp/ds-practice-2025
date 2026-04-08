# Distributed Systems Project - Bookstore

**Team:** Yevhen Pankevych, Yehor Bachynsky, Merili Pihlak

This repository contains the code for the practice sessions of the Distributed Systems course at the University of Tartu.

## Checkpoint 1

### Overview

An online bookstore system composed of multiple microservices that communicate via **gRPC**, and have a general orchestrator with the REST API provided. When a user places an order through the frontend, the orchestrator concurrently invokes all backend services and returns a combined response.

### Architecture

![Architecture Diagram](img/architectural_diagram_checkpoint1.png)

### Services

| Service | Description | Port |
| --- | --- | --- |
| **Frontend** | Static HTML page, running in a Docker container, with the exposed port | REST `8080` |
| **Orchestrator** | Flask REST API to do checkout; orchestrates calls to all services via async gRPC | REST `8081` |
| **Fraud Detection** | Uses OpenAI to detect fraud orders (prompt injection, suspicious fields, etc.) | gRPC `50051` |
| **Transaction Verification** | Validates credit card number (Luhn's algorithm), card vendor (Visa/Mastercard), expiry date, billing address (validates address is real using GeoPy), and item list (items are not empty and do not exceed reasonable quantities) | gRPC `50052` |
| **Recommendation System** | Uses OpenAI to suggest books from the catalog based on the user's order | gRPC `50053` |

### gRPC Interfaces

Each service returns a response with the direct answer (is_fraud, is_valid, recommendations) and an optional error message if something went wrong. The orchestrator combines the responses and returns a single JSON object to the frontend.

Methods:
- `FraudDetectionService.CheckFraud(FraudRequest(user, credit_card, user_comment, List(item), billing_address, shipping_method, gift_wrapping, terms_accepted))` -> `FraudResponse(is_fraud, error_message)`
- `TransactionVerificationService.VerifyTransaction(TransactionVerificationRequest(credit_card, List(item), billing_address))` -> `TransactionVerficationResponse(is_valid, error_message)`
- `RecommendationService.GetRecommendations(RecommendationRequest(user_comment, List(item)), top_k))` -> `RecommendationResponse(suggested_books, error_message)`

![System Diagram](img/system_diagram_checkpoint1.png)

## Checkpoint 2

### Vector Clock Diagram

![Vector Clock Diagram](img/VC_Diagram.png)

### Leader Election Diagram
![Leader Election Diagram](docs/leader-election-initial.png)
![Leader Election Diagram](docs/leader-election-reclaim.png)

### System Model

The system follows a **microservice architecture with centralized orchestration**. The orchestrator is the control plane for checkout validation, while order execution is decoupled through an order queue and replicated executors.

#### Architecture Type

- Orchestrated microservices with partial-order event flow.
- Hybrid communication model: synchronous gRPC for validation path, asynchronous queue/executor path for order execution.
- Vector-clock-based causal tracking across transaction-verification, fraud-detection, and recommendation services.

#### Service Roles

- **Frontend**: sends checkout requests over HTTP.
- **Orchestrator**: entry point, generates unique `OrderID`, dispatches event calls, merges vector clocks, returns response.
- **Transaction Verification**: validates items, credit card format/vendor/expiry/CVV, and billing address.
- **Fraud Detection**: checks known fraud signals and runs AI-based fraud analysis.
- **Recommendation System**: generates recommendations with AI and deterministic fallback.
- **Order Queue**: provides `Enqueue`/`Dequeue` queueing operations.
- **Order Executor replicas**: run leader election; only leader dequeues and executes queued orders.

#### Connections Between Services

- `frontend -> orchestrator` over REST (`/checkout`).
- `orchestrator -> transaction_verification`, `fraud_detection`, `recommendation_system` over gRPC.
- `orchestrator -> order_queue` over gRPC (`Enqueue`) after successful validation flow.
- `order_executor leader -> order_queue` over gRPC (`Dequeue`).
- `order_executor replicas <-> each other` over gRPC (`Election`, `AnnounceLeader`, `Heartbeat`).

#### Event Ordering and Vector Clocks

- For each new `OrderID`, all backend services initialize local cached order state and local vector clock `(0,0,0)`.
- Each event updates vector clock with the rule: component-wise `max(local, incoming)`, then increments own service index.
- Events are partially ordered with concurrency (for example, `a || b` and dependent follow-up events), and the orchestrator merges returned clocks using component-wise max.
- On successful completion, the final vector clock `VCf` represents a causally complete order flow.

#### Failure Modes and Handling

- **Validation failure**: any failed intermediate event is propagated immediately to orchestrator, order is denied.
- **Service/RPC timeout or unavailability**: treated as failed event in request flow.
- **AI failure**:
  - Fraud detection is fail-closed in current behavior (can deny order).
  - Recommendation system falls back to deterministic recommendations when AI is unavailable.
- **Leader failure (executors)**: heartbeat timeout triggers bully-election rerun.
- **Queue empty**: non-fatal; leader keeps polling.
- **State durability limitation**: order cache/vector-clock state is in memory; service restart can lose transient per-order state.

#### Consistency and Trade-offs

- Vector clocks provide explicit causal ordering and concurrency visibility between service events.
- Queue + leader executor ensures mutual exclusion for dequeue/execution in normal operation.
- Without persistent storage/retry semantics, processing guarantees are practical but not fully fault-tolerant in crash scenarios.

### Prerequisites

- Docker & Docker Compose
- An OpenAI API key

### Running

#### Generate gRPC Stubs

```bash
python recompile_proto.py
```

#### Run the system

```bash
export OPENAI_API_KEY=<api-key>
export OPENAI_MODEL=<model-name>
docker compose up --build
```

#### Visit UI

Navigate to [http://localhost:8080](http://localhost:8080) in the browser.

### Logs

Each service uses python `logging` library to write structured logs to the `logs/` directory (volume-mounted into every container) and into the console. Each service creates own log file with the name `<ServiceName>.log` in the `logs/` directory.

