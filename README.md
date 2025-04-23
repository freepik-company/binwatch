# BinWatch

<!-- markdownlint-disable MD033 -->

<img src="https://raw.githubusercontent.com/freepik-company/binwatch/master/docs/img/logo.png" alt="BinWatch Logo (Main) logo." width="150">

![GitHub go.mod Go version (subdirectory of monorepo)](https://img.shields.io/github/go-mod/go-version/freepik-company/binwatch)
![GitHub](https://img.shields.io/github/license/freepik-company/binwatch)

BinWatch is a tool designed to subscribe to a MySQL database's binlog and track changes that occur in database tables.
These changes are processed and sent to supported connectors in real-time.

## Motivation

The motivation behind this tool stems from the need for a system that allows simple, real-time tracking of changes in
a MySQL database without requiring complex external tools that might complicate the process.

We use the [go-mysql](https://github.com/go-mysql-org/go-mysql) library to read MySQL binlogs, exactly the
[replication](https://github.com/go-mysql-org/go-mysql/tree/master/replication) library of this reposiotory. This library enables us
to monitor MySQL binlogs and capture changes occurring in database tables.

## Configuration

> [!NOTE]
> You can use environment variables in the configuration file. The environment variables must be written in the
> format `${ENV:<VAR_NAME>}$`. The environment variables will be replaced by their values in the initialzation.

List of fields in v1alpha2 configuration:

| Field                                | Type                  | Description                                                                           |
|:-------------------------------------|:----------------------|:--------------------------------------------------------------------------------------|
| `logger.level`                       | `string`              | Log verbosity level (debug, info, warn, error).                                       |
| `server.id`                          | `string`              | Unique identifier for this server instance.                                           |
| `server.host`                        | `string`              | Hostname or IP address where the server runs.                                         |
| `server.port`                        | `uint32`              | TCP port the server listens on.                                                       |
| `server.stopInError`                 | `bool`                | Whether to stop the server when a fatal error occurs.                                 |
| `server.pool.size`                   | `uint32`              | Size of the internal worker pool.                                                     |
| `server.pool.itemByRow`              | `bool`                | Boolean to add in pool an item by row in a single operation or the full list of rows. |
| `server.cache.enabled`               | `bool`                | Enables or disables caching.                                                          |
| `server.cache.type`                  | `string`              | Cache type (local or redis).                                                          |
| `server.cache.local.path`            | `string`              | Filesystem path for the local cache.                                                  |
| `server.cache.redis.host`            | `string`              | Redis server hostname or IP.                                                          |
| `server.cache.redis.port`            | `uint32`              | Redis server port.                                                                    |
| `server.cache.redis.password`        | `string`              | Redis authentication password.                                                        |
| `source.flavor`                      | `string`              | Type of MySQL-compatible source (mysql or mariadb).                                   |
| `source.serverID`                    | `uint32`              | Unique server ID for replication.                                                     |
| `source.host`                        | `string`              | Hostname or IP of the source database.                                                |
| `source.port`                        | `uint32`              | Port of the source database.                                                          |
| `source.user`                        | `string`              | Username for the source database.                                                     |
| `source.password`                    | `string`              | Password for the source database.                                                     |
| `source.dbTables`                    | `map[string][]string` | Map of database names to tables to monitor (e.g., { "db": ["table"] }).               |
| `source.readTimeout`                 | `duration`            | Maximum time to wait for a read operation.                                            |
| `source.heartbeatPeriod`             | `duration`            | Interval between heartbeat messages.                                                  |
| `source.startLocation.file`          | `string`              | (Optional) Binlog file to start from.                                                 |
| `source.startLocation.position`      | `uint32`              | (Optional) Binlog position to start from.                                             |
| `connectors[].name`                  | `string`              | Name of the connector.                                                                |
| `connectors[].type`                  | `string`              | Connector type (webhook or google_pubsub).                                            |
| `connectors[].webhook.url`           | `string`              | Target URL for the webhook.                                                           |
| `connectors[].webhook.method`        | `string`              | HTTP method used for webhook requests.                                                |
| `connectors[].webhook.headers`       | `map[string]string`   | Headers to include in the webhook request.                                            |
| `connectors[].webhook.tlsSkipVerify` | `bool`                | Skip TLS certificate verification.                                                    |
| `connectors[].pubsub.projectID`      | `string`              | Google Cloud project ID for Pub/Sub.                                                  |
| `connectors[].pubsub.topicID`        | `string`              | Pub/Sub topic ID.                                                                     |
| `routes[].name`                      | `string`              | Name of the route.                                                                    |
| `routes[].connector`                 | `string`              | Name of the connector this route uses.                                                |
| `routes[].operations`                | `[]string`            | List of database operations to route (INSERT, UPDATE, DELETE).                        |
| `routes[].template`                  | `string`              | Go template used to render the message sent by the route.                             |
| `routes[].dbTable`                   | `string`              | Database.Table name to send to the connector (e.g., "db.table" ).                     |

## Standalone

This service is designed to be executed as a single instance.
Adding replication would cause the binlogs sent to lose their
order, which is crucial for this tool to function correctly.
Additionally, replication would negatively impact performance,
as the service would need to spend time synchronizing with
other replicas continuously. No standalone approach also
increases the risk of losing binlogs during the replicas
synchronization process.

### Cache

The cache can be enabled to ensure that, in the event of a
sudden stop, the instance can resume from where it left off.
This cache is updated with the binlog position after each
binlog is sent.

> [!IMPORTANT]
> The config field `source.startLocation` overwrite the cache
> binlog location in the initialization. Remove this field from
> the configuration to use the cache location.

## Sources

| Sources    | Status |
|------------|---|
| MySQL      | ✅|
| PostgreSQL | 🔜|

> [!IMPORTANT]
> For MySQL connector just supports binlog format ROW. For binlog format STATEMENT or MIXED, the connector will not work,
> we are working on it :D.

## Connectors

| Connectors | Status |
|------------|--------|
| Webhook    | ✅|
| GCP PubSub | ✅|
| Kafka      | 🔜|
| RabbitMQ   | 🔜|
| AWS SQS    | 🔜|
| Nats       | 🔜|

## Running BinWatch

For running binwatch you need to create a configuration file and run the binary with the configuration file as a parameter.

```shell
go run cmd/main.go sync --config config.yaml
```

## Deployment

We recommend to deploy BinWatch application with our [Helm registry](https://freepik-company.github.io/binwatch/).

```cmd
helm repo add binwatch https://freepik-company.github.io/binwatch/
```

```cmd
helm install binwatch binwatch/binwatch
```

Example `values.yaml` file for helm deploying:

```yaml
replicaCount: 2

image:
  repository: ghcr.io/freepik-company/binwatch
  pullPolicy: IfNotPresent
  tag: "latest"

serviceAccount:
  annotations: {}

resources:
   limits:
     memory: 256Mi
   requests:
     cpu: 100m
     memory: 256Mi

volumes:
  - name: config-volume
    configMap:
      name: binwatch-config
      items:
        - key: config.yaml
          path: config.yaml

volumeMounts:
  - name: config-volume
    mountPath: /app/config.yaml
    subPath: config.yaml
    readOnly: true

env:
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
  - name: MYSQL_HOST
    value: mysql
  - name: MYSQL_PORT
    value: "3306"
  - name: MYSQL_USER
    value: root
  - name: MYSQL_PASSWORD
    secretKeyRef:
      name: mysql-secret
      key: password
  - name: WEBHOOK_URL
    value: https://webhook.site/<id>

annotations:
  reloader.stakater.com/auto: "true"

configMap:
  enabled: true
  data:
    config.yaml: |-
      logger:
        level: debug

      server:
        id: local
        host: "127.0.0.1"
        port: 8080
        stopInError: true
        pool:
          size: 20
        cache:
          enabled: true
          type: local
          local:
            path: int.test/cache

      source:
        flavor: mysql
        serverID: 100
        host: "127.0.0.1"
        port: 3306
        user: root
        password: test
        dbTables:
          testdb: [users]
        readTimeout: 90s
        heartbeatPeriod: 60s
        startLocation:
          file: "mysql-bin.000001"
          position: 4


      connectors:
      - name: webhook-upsert
        type: webhook
        webhook:
          url: http://127.0.0.1:8085/api/v1/data
          method: POST
          headers:
            "Content-Type": "application/json"
          tlsSkipVerify: true

      routes:
      - name: testdb-users-operations
        connector: webhook-upsert
        operations: ["INSERT", "UPDATE", "DELETE"]
        dbTable: ""
        # dbTable: "testdb.users"
        template: |
          {
            "index": "testdb-users-v1",
            "itemID":"{{ .ItemID }}",
            "operation":"{{ .Data.Operation }}",
            "rows": {{- .Data.Rows | toJson }}
          }
```

## How to collaborate

We are open to external collaborations for this project. For doing it you must fork the
repository, make your changes to the code and open a PR. The code will be reviewed and tested (always).

> We are developers and hate bad code. For that reason we ask you the highest quality on each line of code to improve
> this project on each iteration.

## Contributors

* 🧔🏽‍♂️[@dfradehubs](https://github.com/dfradehubs) - Daniel Fradejas
* 🧔🏻‍♂️[@achetronic](https://github.com/achetronic) - Alby Hernandez
* 🧑🏻[@sebastocorp](https://github.com/sebastocorp) - Sebastián Vargas

## License

<!-- markdownlint-disable MD046 -->

Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
