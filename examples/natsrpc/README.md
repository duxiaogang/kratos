# NATS RPC Example

This example demonstrates how to use NATS as an RPC transport layer in Kratos.

## Prerequisites

1. Install and run NATS server:

```bash
# Using Docker
docker run -p 4222:4222 nats:latest

# Or install locally: https://docs.nats.io/running-a-nats-service/introduction/installation
nats-server
```

## Project Structure

```
examples/natsrpc/
├── api/
│   ├── greeter.proto           # Service definition
│   ├── greeter.pb.go           # Generated protobuf code
│   └── greeter.natsrpc.pb.go   # Generated NATS RPC code
├── server/
│   └── main.go                 # Server implementation
├── client/
│   └── main.go                 # Client implementation
└── README.md
```

## Generate Proto Code

```bash
# Install protoc-gen-natsrpc
go install github.com/byebyebruce/natsrpc/cmd/protoc-gen-natsrpc@v0.7.0

# Generate code
cd examples/natsrpc/api
protoc --proto_path=. --proto_path=<path-to-natsrpc-proto> \
    --go_out=paths=source_relative:. \
    --natsrpc_out=paths=source_relative:. \
    greeter.proto
```

## Run

1. Start the server:

```bash
go run ./examples/natsrpc/server
```

2. In another terminal, run the client:

```bash
go run ./examples/natsrpc/client
```

## Expected Output

**Server:**
```
INFO [NATS] server listening on: nats://localhost:4222?namespace=example
INFO Received SayHello request: name=Kratos
INFO Received Notify: content=Hello from client!
```

**Client:**
```
INFO Calling SayHello...
INFO SayHello response: Hello, Kratos!
INFO Calling Notify...
INFO Notify sent successfully
INFO Client finished
```

## Features Demonstrated

- **Request/Response RPC**: `SayHello` method - client sends request and waits for response
- **Publish (Fire-and-forget)**: `Notify` method - client publishes message without waiting for response
- **Kratos Middleware**: Recovery and logging middleware applied to server
- **Namespace Isolation**: Both server and client use "example" namespace
