# k-rest-proxy

A secure, production-ready REST API proxy for accessing Kafka messages with time-based and execution-based filtering.

## Features

- 🔒 **Secure**: TLS/HTTPS with API key authentication
- ⚡ **Performant**: Connection pooling for efficient Kafka consumer management
- 📊 **Observable**: Health checks, metrics, and comprehensive logging
- 🎯 **Flexible**: Multiple query modes (time-based, execution-based, multi-topic)
- 🔄 **Production-Ready**: Structured error handling, validation, and configuration management

## Quick Start

### Prerequisites

- Java 21+
- Docker & Docker Compose (for local development)
- Maven 3.9+

### Run with Docker Compose

```bash
# Start all services (Kafka, Zookeeper, Schema Registry, Application)
docker-compose up -d

# Check health
curl -k https://localhost:8443/actuator/health

# Query messages (requires X-API-KEY header)
curl -k -H "X-API-KEY: secret-api-key" \
  "https://localhost:8443/api/v1/messages/my-topic?startTime=2024-01-01T00:00:00Z&endTime=2024-01-01T01:00:00Z"
```

### Run Locally

```bash
# Start dependencies
docker-compose up -d zookeeper kafka schema-registry

# Run application
mvn spring-boot:run
```

## API Endpoints

All endpoints require the `X-API-KEY` header for authentication.

### Get Messages by Time Range

```http
GET /api/v1/messages/{topic}?startTime={ISO8601}&endTime={ISO8601}&cursor={cursor}
```

Fetches all messages from a topic within the specified time window. Returns a paginated response with a `nextCursor`.

### Filter by Execution ID

```http
GET /api/v1/messages/{topic}/filter?startTime={ISO8601}&endTime={ISO8601}&execId={id}&cursor={cursor}
```

Filters messages by execution ID (from Avro key field) within a time range. Returns a paginated response.

### Query Multiple Topics

```http
GET /api/v1/messages/filter?topics={topic1,topic2}&startTime={ISO8601}&endTime={ISO8601}&execId={id}
```

Query multiple topics with optional execution ID filtering.

### Get Messages by Execution

```http
GET /api/v1/messages/by-execution?topics={topic1,topic2}&execId={id}
```

Automatically finds execution time window from `execids` topic and retrieves messages.

## Configuration

Key configuration properties (see [application.yml](src/main/ resources/application.yml)):

```yaml
app:
  security:
    api-key: "your-secret-key"
  kafka:
    poll-timeout-ms: 100
    max-messages-per-request: 10000
    consumer-pool:
      max-total: 10
      max-idle: 5
      min-idle: 1
```

For production deployment, set configuration via environment variables:
- `APP_SECURITY_API_KEY`
- `APP_KAFKA_CONSUMER_POOL_MAX_TOTAL`
- etc.

## Monitoring

### Health Checks

```bash
# Overall health
curl https://localhost:8443/actuator/health

# Detailed health (requires authentication)
curl -H "X-API-KEY: secret-api-key" https://localhost:8443/actuator/health
```

### Metrics

Prometheus metrics available at:
```bash
curl https://localhost:8443/actuator/prometheus
```

## Development

See [DEVELOPER.md](DEVELOPER.md) for detailed development instructions including:
- Building and testing
- Performance testing with Gatling
- Deployment procedures
- Security best practices

## Security

- All API endpoints require API key authentication via `X-API-KEY` header
- TLS/HTTPS enabled by default
- Constant-time API key comparison prevents timing attacks
- Security headers (HSTS, X-Frame-Options) configured
- Health endpoints accessible without authentication for Kubernetes probes

## Architecture

```
Client Request
     ↓
API Key Filter → Security Headers
     ↓
Message Controller → Request Validation
     ↓
Kafka Message Service → Consumer Pool
     ↓
Kafka Cluster
```

## License

[Add your license here]

## Contributing

[Add contribution guidelines here]
