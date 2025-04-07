# BinWatch
<img src="https://raw.githubusercontent.com/freepik-company/binwatch/master/docs/img/logo.png" alt="BinWatch Logo (Main) logo." width="150">

![GitHub go.mod Go version (subdirectory of monorepo)](https://img.shields.io/github/go-mod/go-version/freepik-company/binwatch)
![GitHub](https://img.shields.io/github/license/freepik-company/binwatch)

BinWatch is a tool designed to subscribe to a MySQL database's binlog and track changes that occur in database tables. 
These changes are processed and sent to supported connectors in real-time.

<img src="https://raw.githubusercontent.com/freepik-company/binwatch/master/docs/img/flow.png" alt="BinWatch (Main) Flow." width="800">

## Motivation
The motivation behind this tool stems from the need for a system that allows simple, real-time tracking of changes in
a MySQL database without requiring complex external tools that might complicate the process.

We use the [go-mysql](https://github.com/go-mysql-org/go-mysql) library to read MySQL binlogs, exactly the 
[canal](https://github.com/go-mysql-org/go-mysql/tree/master/canal) library of this reposiotory. This library enables us
to monitor MySQL binlogs (and sync data using mysqldump if we want) and capture changes occurring in database tables.

## Configuration

To configure the tool, you need to create a YAML configuration file. Below is a configuration example:
```yaml
---
# BinWatch configuration file
# This file is written in YAML format

# ATTENTION!
# You can use environment variables in the configuration file. The environment variables must be written in the
# format $ENV_VAR_NAME. The environment variables will be replaced by their values at runtime.

# Logger configuration
logger:
  # debug, info, warn, error, dpanic, panic, fatal
  level: debug
  # console or json
  encoding: json

# Server id of the binwatch instance. Can be set via environment variable which will be replaced at runtime
# It must have the format <hostname>:<port> or <ip>:<port> to be used in the hashring
server_id: "$HOSTNAME:8080"

# Number of workers to process the events
max_workers: 10

# Flow control configuration to control the memory usage by the event connectors queue
flow_control:
  # Check interval to check the queue size
  check_interval: 100ms
  # List of thresholds to control the sleep time of the workers to avoid memory overflow
  thresholds:
    - queue_size: 1
      sleep_time: 10ms
    - queue_size: 10
      sleep_time: 100ms
    - queue_size: 100
      sleep_time: 1s
      
# Hashring configuration for HA and load balancing purposes
hashring:

  # Confident mode
  # This mode stores the position of the binlog in a redis server to avoid losing events when a new node is added to the ring
  # confident_mode:
  #  enabled: true
  #
  #  redis:
  #    host: redis
  #    port: 6379
  #    password: ""
  #    key_prefix: "binwatch"
  
  # Sync worker time to sync the ring nodes when a new node is added or removed from the ring
  sync_worker_time: 300ms
  
  # API port to expose the hashring API and health check
  api_port: 8080
    
  # For static ring discovery we need to define the server names that are part of the ring
  static_ring_discovery:
    hosts:
      - test:8080
      - test2:8080

  # For DNS ring discovery we need to define the domain and the port of the headless service to get
  # the list of server names that are part of the ring
  # Recommended for kubernetes deployments with headless services
  # dns_ring_discovery:
  #   domain: "dns.example.com"
  #   port: 8080

# Sources configuration
# List of sources to watch for changes in the database binlog
sources:

  # MySQL source configuration
  mysql:
    host: "$MYSQL_HOST"
    port: $MYSQL_PORT
    user: "$MYSQL_USER"
    password: "$MYSQL_PASSWORD"

    # Server ID must be unique across all MySQL servers
    server_id: 100

    # Read timeout
    read_timeout: 90s

    # Heartbeat period
    heartbeat_period: 60s

    # Flavor of the database. MySQL or MariaDB
    flavor: mysql

    # Just listen for events in these database-table pairs
    filter_tables:
      - database: test
        table: test

    # Start binlog position if you want to start reading from a specific position of binlog or mysqldump
    # Set file mysqldump if you want to start from a specific position in the mysqldump.
    # NOTE: mysqldump dumps in order of the primary key
    start_position:
      file: mysqldump
      position: 4
      
    # Mysqldump configuration for sync command. Just can dump an entire database, many databases or many tables for ONE database.
    # If many databases are specefied with tables, the tables will be ignored.
    # Empty dump_config disables the previous mysql dump feature.
    dump_config:
      databases:
        - test
      tables:
        - test
      # Extra options for mysqldump command
      # Recommended --single-transaction to avoid blocking the database
      # mysqldump_extra_options:
      #   - "--single-transaction"
      # Default value is /usr/bin/mysqldump, path where mysqldump is located in the Docker image
      mysqldump_bin_path: "/opt/homebrew/bin/mysqldump"

# Data connectors configuration
# List of connectors to send the data to
connectors:

  # Routes configuration for the connectors. It determines the events that will be sent to each connector and
  # the data format that will be sent to the connector.
  # Events can be insert, update or delete
  # Data format is in golang template format. The data read from the binlog will be passed to the template as .data
  # IMPORTANT: This part of configuration does not allow environment variables!!
  routes:
    - events: ["insert", "update"]
      database: test
      table: test
      connector: pubsub-add
      data: |
        {{- printf `{ "index": "test", "id": %v, "data": %s}` .data.id (toJson .data) }}
    - events: ["delete"]
      database: test
      table: test
      connector: webhook-delete
      data: |
        {{- printf `{ "index": "test", "id": %v }` .data.id }}

  # Connectors configuration. Currently only pubsub and webhook are supported.

  # PubSub connector configuration
  pubsub:
    - name: pubsub-add
      project_id: "test-project"
      topic_id: "test-topic"

  # Webhook connector configuration
  webhook:
    - name: webhook-delete
      tls_skip_verify: false
      url: "https://webhook.site/<id>"
      method: "POST"
      headers:
        X-BinWatch: "true"
      # credentials:
      #  username: "$WEBHOOK_USERNAME"
      #  password: "$WEBHOOK_PASSWORD"
```

## Hashring - Load Balancing and High Availability Approach
For load balancing and high availability, we've implemented a hashring mechanism that operates across static nodes 
or those auto-discovered via DNS. With this hashring, all service replicas read all binlog entries, but only process 
those entries for which they have responsibility (the "cheese" is equally divided among them).

### High Availability and Recovery Mechanism
The high availability works with a memory store server (Redis) that stores the binlog position of each node.

> [!IMPORTANT]
> This hashring solution may lead to duplicate events during brief periods of time, but this approach was deliberately 
> chosen to ensure high availability and recovery from failures. Furthermore, we've determined that duplicating events 
> is preferable to losing them, as duplicates can be handled by the destination connector.

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

```
helm repo add binwatch https://freepik-company.github.io/binwatch/
```

```
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
        encoding: json
      
      server_id: "$POD_IP:8080"
        
      max_workers: 10

      flow_control:
        check_interval: 1s
        thresholds:
          - queue_size: 100
            sleep_time: 10ms
          - queue_size: 1000
            sleep_time: 100ms
          - queue_size: 10000
            sleep_time: 1s
      
      hashring:
        sync_worker_time: 300ms
        api_port: 8080
        dns_ring_discovery:
          domain: "binwatch-headless.binwatch.svc.cluster.local"
          port: 8080
      
      sources:
        mysql:
          host: "$MYSQL_HOST"
          port: $MYSQL_PORT
          user: "$MYSQL_USER"
          password: "$MYSQL_PASSWORD"
          server_id: 100
          read_timeout: 90s
          heartbeat_period: 60s
          flavor: mysql
          filter_tables:
            - database: test
              table: test    
          dump_config:
              databases:
                - test
              tables:
                - test
      
      connectors:
        routes:
          - events: ["insert", "update"]
            database: test
            table: test
            connector: webhook-test
            data: |
              {{- printf `{ "index": "test", "id": %v, "data": %s }` .data.id ( toJson .data ) }}
          - events: ["delete"]
            database: test
            table: test
            connector: webhook-test
            data: |
              {{- printf `{ "index": "test", "id": %v }` .data.id }}

        webhook:
          - name: webhook-test
            tls_skip_verify: false
            url: "$WEBHOOK_URL"
            method: "POST"
            headers:
              X-BinWatch: "true"
```
## How to collaborate

We are open to external collaborations for this project. For doing it you must fork the
repository, make your changes to the code and open a PR. The code will be reviewed and tested (always).

> We are developers and hate bad code. For that reason we ask you the highest quality on each line of code to improve
> this project on each iteration.

## Contributors
* 🧔🏽‍♂️[@dfradehubs](https://github.com/dfradehubs) - Daniel Fradejas
* 🧔🏻‍♂️[@achetronic](https://github.com/achetronic) - Alby Hernandez

## License

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