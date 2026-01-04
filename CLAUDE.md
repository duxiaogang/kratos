# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Build all CLI tools
make all

# Run all tests across all modules
make test

# Run tests with coverage
make test-coverage

# Lint all modules
make lint

# Auto-fix lint issues
make fix

# Tidy all go.mod files
make clean

# Run single package test
go test ./transport/grpc/... -v

# Run single test function
go test ./transport/grpc/... -run TestServerName -v
```

## Architecture Overview

Kratos is a Go microservice framework with a pluggable, interface-driven design.

### Core Abstractions

**App (`app.go`)**: The application lifecycle manager that:
- Manages multiple transport servers (gRPC, HTTP, NATS)
- Handles service registration/deregistration
- Coordinates graceful shutdown via signals

**Transport Layer (`transport/`)**: Abstracted server/client interfaces:
- `transport.Server`: Interface with `Start(ctx)` and `Stop(ctx)` methods
- `transport.Endpointer`: Returns service endpoint URL for registry
- `transport.Transporter`: Context carrier for middleware (Kind, Endpoint, Operation, Headers)
- Implementations: `transport/grpc/`, `transport/http/`, `transport/nats/`

**Middleware (`middleware/`)**: Chain-able handlers with signature `func(Handler) Handler`:
- Applied via `middleware.Chain()`
- Server-side matching via `matcher.Matcher` for selective application by operation path
- Built-in: recovery, logging, tracing, metrics, ratelimit, circuitbreaker, auth, validate

**Registry (`registry/`)**: Service discovery interface:
- `Registrar`: Register/Deregister service instances
- `Discovery`: Watch for service changes
- Implementations in `contrib/registry/` (consul, etcd, nacos, zookeeper, etc.)

**Config (`config/`)**: Multi-source configuration with hot reload:
- `Source` interface for config providers
- Atomic value updates
- Implementations in `contrib/config/`

### Contrib Modules

The `contrib/` directory contains optional integrations with separate go.mod files:
- `contrib/registry/`: Service discovery backends
- `contrib/config/`: Configuration sources
- `contrib/log/`: Logger implementations (zap, logrus, zerolog)
- `contrib/transport/`: Additional transports (MCP)

### Code Generation

Kratos uses protobuf for API definitions with custom protoc plugins:
- `protoc-gen-go-http`: Generates HTTP server/client from proto
- `protoc-gen-go-errors`: Generates error enums from proto
- Standard `protoc-gen-go` and `protoc-gen-go-grpc` for gRPC

### Key Interfaces Pattern

Most components follow the options pattern:
```go
func NewServer(opts ...ServerOption) *Server
func NewClient(opts ...ClientOption) *Client
```

Transport implementations must satisfy:
```go
var _ transport.Server = (*Server)(nil)
var _ transport.Endpointer = (*Server)(nil)
```
