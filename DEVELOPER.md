# Developer Guide

This guide provides detailed instructions on how to build, test, run, package, and deploy the `k-rest-proxy` project.

## Dev Box Prerequisites

To successfully run and develop this project, your development environment (Dev Box) needs the following:

### Hardware
-   **RAM**: Minimum 8GB (16GB recommended) to run the full stack (Kafka, Zookeeper, Schema Registry, Application).
-   **Disk Space**: At least 10GB free space for Docker images and logs.

### Software
-   **OS**: Linux, macOS, or Windows (with WSL2).
-   **Java 21**: The project uses Java 21.
-   **Maven 3.9+**: For building the project.
-   **Docker & Docker Compose**: For containerization and running dependencies.
-   **Git**: For version control.

## Project Structure

-   `src/main/java`: Application source code.
-   `src/test/java`: Unit and integration tests.
-   `Dockerfile`: Docker image definition.
-   `docker-compose.yml`: Orchestration for local development and running the app with dependencies.

## Build

To build the application and generate the JAR file, run:

```bash
mvn clean package
```

This will create the executable JAR in the `target/` directory.

## Test

The project uses **Testcontainers** for integration testing, which requires Docker to be running.

### Run All Tests

```bash
mvn test
```

### Run Integration Tests

```bash
mvn verify
```

### Run Performance Tests

The project includes Gatling performance tests in `src/gatling/java`. You can run them using Maven.

**Prerequisites:** The application and Kafka environment must be running (e.g., via `docker-compose` or locally) and accessible.

```bash
mvn gatling:test
```

**Configuration:**
You can configure the test parameters using system properties (`-Dproperty=value`):

| Property | Description | Default |
| :--- | :--- | :--- |
| `baseUrl` | Base URL of the application | `https://localhost:8443` |
| `apiKey` | API Key for authentication | `default-api-key` |
| `topic` | Kafka topic to query | `test-topic` |
| `execId` | Execution ID for filtering | `test-exec-id` |
| `startTime` | Start of time window (ISO 8601) | 1 hour ago |
| `endTime` | End of time window (ISO 8601) | Now |

**Example:**

```bash
mvn gatling:test \
    -DbaseUrl=https://localhost:8443 \
    -DapiKey=my-secret-key \
    -Dtopic=my-data-topic
```

## Run Locally

### 1. Start Dependencies

Start Kafka, Zookeeper, and Schema Registry using Docker Compose:

```bash
docker-compose up -d zookeeper kafka schema-registry
```

Wait for the services to be healthy.

### 2. Run Application

You can run the application using Maven or the built JAR.

**Using Maven:**

```bash
mvn spring-boot:run
```

**Using JAR:**

```bash
java -jar target/k-rest-proxy-0.0.1-SNAPSHOT.jar
```

The application will start on port **8443** (HTTPS).

## Run with Docker

You can run the entire stack, including the application, using Docker Compose.

### 1. Build Docker Image

```bash
docker build -t k-rest-proxy .
```

### 2. Start All Services

```bash
docker-compose up -d
```

This will start:
-   Zookeeper
-   Kafka
-   Schema Registry
-   k-rest-proxy (accessible at `https://localhost:8443`)

## API Usage

The application exposes a REST API to fetch Kafka messages.

### Fetch Messages

**Endpoint:** `GET /api/v1/messages/{topic}`

**Parameters:**
-   `startTime`: Start timestamp (ISO 8601 format, e.g., `2023-10-27T10:00:00Z`).
-   `startTime`: Start timestamp (ISO 8601 format, e.g., `2023-10-27T10:00:00Z`).
-   `endTime`: End timestamp (ISO 8601 format, e.g., `2023-10-27T10:05:00Z`).
-   `cursor`: (Optional) Cursor for pagination, returned in the previous response.

**Authentication:**
Requires an API Key header: `X-API-KEY`.

**Example Request:**

```bash
curl -k -H "X-API-KEY: your-api-key" \
     "https://localhost:8443/api/v1/messages/my-topic?startTime=2023-10-27T10:00:00Z&endTime=2023-10-27T10:05:00Z"
```

**Response:**
The response is a JSON object containing `data` (list of messages) and `nextCursor` (string).

```json
{
  "data": [...],
  "nextCursor": "eyIwIjoxMjN9"
}
```

To fetch the next page, pass the `nextCursor` value as the `cursor` parameter:

```bash
curl -k -H "X-API-KEY: your-api-key" \
     "https://localhost:8443/api/v1/messages/my-topic?startTime=2023-10-27T10:00:00Z&endTime=2023-10-27T10:05:00Z&cursor=eyIwIjoxMjN9"
```

### Filter Messages by Execution ID

**Endpoint:** `GET /api/v1/messages/{topic}/filter`

**Parameters:**
-   `startTime`: Start timestamp.
-   `endTime`: End timestamp.
-   `startTime`: Start timestamp.
-   `endTime`: End timestamp.
-   `execId`: Execution ID to filter by.
-   `cursor`: (Optional) Cursor for pagination.

**Example Request:**

```bash
curl -k -H "X-API-KEY: your-api-key" \
     "https://localhost:8443/api/v1/messages/my-topic/filter?startTime=2023-10-27T10:00:00Z&endTime=2023-10-27T10:05:00Z&execId=12345"
```

*(Note: `-k` is used to skip SSL validation for self-signed certificates in development)*

## Deploy

### Docker Deployment

The application is designed to be deployed as a Docker container.

1.  **Build the Image**: Use the `Dockerfile` to build the image.
2.  **Push to Registry**: Tag and push the image to your container registry (e.g., Docker Hub, ECR).
    ```bash
    docker tag k-rest-proxy:latest myregistry/k-rest-proxy:v1.0.0
    docker push myregistry/k-rest-proxy:v1.0.0
    ```
3.  **Run**: Deploy using your orchestration tool (Kubernetes, ECS, Docker Swarm) ensuring the following environment variables are set:
    -   `SPRING_KAFKA_BOOTSTRAP_SERVERS`: Kafka broker address.
    -   `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_URL`: Schema Registry URL.
    -   `SERVER_SSL_KEY_STORE`: Path to the keystore file (default: `/app/keystore.p12`).

### Environment Variables

| Variable | Description | Default |
| :--- | :--- | :--- |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | Kafka bootstrap servers | `kafka:29092` |
| `SPRING_KAFKA_PROPERTIES_SCHEMA_REGISTRY_URL` | Schema Registry URL | `http://schema-registry:8081` |
| `SERVER_SSL_KEY_STORE` | Path to SSL Keystore | `file:/app/keystore.p12` |

## Architecture Overview

### Component Diagram

```
┌──────────────────────────────────────────────────────────────┐
│                      API Layer                                │
│  ┌──────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │ Security     │→ │ Message         │→ │ Request         │ │
│  │ Filter       │  │ Controller      │  │ Validator       │ │
│  └──────────────┘  └─────────────────┘  └─────────────────┘ │
└────────────────────────────┬─────────────────────────────────┘
                             ↓
┌──────────────────────────────────────────────────────────────┐
│                   Service Layer                               │
│  ┌────────────────────────────────────────────────────────┐  │
│  │  KafkaMessageService                                   │  │
│  │  - Message retrieval logic                             │  │
│  │  - Execution time caching (ConcurrentHashMap)          │  │
│  │  - Avro to JSON conversion                             │  │
│  └──────────────────────────┬─────────────────────────────┘  │
└──────────────────────────────┼─────────────────────────────────┘
                               ↓
┌──────────────────────────────────────────────────────────────┐
│              Infrastructure Layer                             │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │  Consumer Pool (Apache Commons Pool2)                  │ │
│  │  - Reusable Kafka consumers                            │ │
│  │  - Configurable pool size (max/min/idle)               │ │
│  └────────────────────┬────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
                        ↓
              ┌─────────────────┐
              │ Kafka Cluster   │
              └─────────────────┘
```

### Key Design Decisions

1. **Consumer Pooling**: Reuses Kafka consumers to avoid expensive connection overhead
2. **Execution Time Caching**: Caches start/end times from execids topic to minimize redundant reads
3. **Request Validation**: Enforces topic naming conventions, time window limits (24h max)
4. **Structured Error Handling**: Custom exception hierarchy with GlobalExceptionHandler
5. **Configuration Externalization**: All settings configurable via application.yml or env vars

## Performance Tuning Guide

### Consumer Pool Configuration

The consumer pool is the primary performance lever. Tune based on concurrency needs:

```yaml
app:
  kafka:
    consumer-pool:
      max-total: 20      # Increase for high concurrency
      max-idle: 10       # Keep warm consumers available  
      min-idle: 2        # Ensure minimum ready consumers
```

**Guidelines:**
- **Low traffic** (<10 req/s): `max-total: 5, max-idle: 2, min-idle: 1`
- **Medium traffic** (10-50 req/s): `max-total: 10, max-idle: 5, min-idle: 1`  
- **High traffic** (>50 req/s): `max-total: 20, max-idle: 10, min-idle: 2`

### Kafka Operation Tuning

```yaml
app:
  kafka:
    poll-timeout-ms: 100           # Lower for faster response, higher for efficiency
    max-messages-per-request: 10000 # Prevent OOM on large result sets
```

### Cache Configuration

```yaml
app:
  cache:
    exec-time-ttl-minutes: 60  # Keep execution times cached longer
    max-size: 1000             # Increase if many unique execution IDs
```

### JVM Tuning

For production, tune JVM heap based on message volume:

```bash
java -Xms512m -Xmx2g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -jar app.jar
```

## Security Best Practices

### API Key Management

1. **Never commit API keys** to source control
2. **Rotate keys regularly** (e.g., every 90 days)
3. **Use different keys** for different environments 
4. **Set via environment variables** in production:

   ```bash
   export APP_SECURITY_API_KEY="$(openssl rand -hex 32)"
   ```

   **Local Development:**
   1. Copy `.env.example` to `.env`:
      ```bash
      cp .env.example .env
      ```
   2. Edit `.env` and set your `API_KEY`.
   3. `docker-compose` will automatically pick up the `API_KEY` variable.

### TLS/SSL Configuration

Generate production certificates (don't use self-signed in prod):

```bash
# Generate keystore
keytool -genkeypair -alias k-rest-proxy \
  -keyalg RSA -keysize 2048 -keystore keystore.p12 \
  -storetype PKCS12 -validity 365
```

Configure in application.yml:

```yaml
server:
  ssl:
    key-store: file:/path/to/keystore.p12
    key-store-password: ${KEYSTORE_PASSWORD}
    key-store-type: PKCS12
```

### Security Headers

The application automatically configures:
- **HSTS**: Forces HTTPS for 1 year
- **X-Frame-Options**: Prevents clickjacking
- Custom headers disabled for API-only service

### Network Security

- **Deploy behind API Gateway** for rate limiting, DDoS protection
- **Use VPC/private networks** for Kafka communication
- **Enable mTLS** for Kafka connections in sensitive environments

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Consumer Pool Metrics**
   - `hikaricp.connections.active` - Active consumers
   - `hikaricp.connections.idle` - Idle consumers
   - Alert if active > 80% of max for extended period

2. **Request Metrics**
   - `http.server.requests` - Request rate and latency
   - Alert if P99 latency > 5s

3. **Kafka Metrics**
   - Custom metrics exposed via Micrometer
   - Monitor cache hit rate

4. **Health Status**
   - `actuator/health` - Overall health
   - Alert if DOWN for > 1 minute

### Sample Prometheus Alerts

```yaml
groups:
  - name: k-rest-proxy
    rules:
      - alert: HighLatency
        expr: histogram_quantile(0.99, http_server_requests_seconds_bucket) > 5
        for: 5m
        annotations:
          summary: "P99 latency above 5s"
          
      - alert: PoolExhaustion
        expr: rate(hikaricp.connections.pending[5m]) > 0
        for: 2m
        annotations:
          summary: "Consumer pool exhausted, requests waiting"
```

## Troubleshooting

### Common Issues

**Problem**: `ExecutionNotFoundException` frequently
- **Cause**: execids topic not populated or retention too short  
- **Solution**: Verify execids topic exists and has sufficient retention

**Problem**: High latency on first request
- **Cause**: Consumer pool warming up
- **Solution**: Increase `min-idle` to keep warm consumers ready

**Problem**: OOM errors
-   **Cause**: Large result sets
-   **Solution**: Decrease `max-messages-per-request` or use cursor-based pagination

**Problem**: Authentication fails with correct API key  
- **Cause**: Key contains special characters or whitespace
- **Solution**: Ensure key is properly escaped/quoted in HTTP headers

## Contributing

1. Create feature branch from `master`
2. Make changes with tests
3. Ensure `mvn verify` passes
4. Submit pull request

## Additional Resources

- [Spring Boot Actuator Docs](https://docs.spring.io/spring-boot/docs/current/reference/html/actuator.html)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
