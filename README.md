# BinWatch
<img src="https://raw.githubusercontent.com/freepik-company/binwatch/master/docs/img/logo.png" alt="BinWatch Logo (Main) logo." width="150">

![GitHub go.mod Go version (subdirectory of monorepo)](https://img.shields.io/github/go-mod/go-version/freepik-company/binwatch)
![GitHub](https://img.shields.io/github/license/freepik-company/binwatch)

BinWatch is a tool designed to subscribe to a MySQL database's binlog and track changes that occur in database tables. These changes are processed and sent to supported connectors in real-time.

## Motivation
The motivation behind this tool stems from the need for a system that allows simple, real-time tracking of changes in a MySQL database without requiring complex external tools that might complicate the process.

We use the [go-mysql](https://github.com/go-mysql-org/go-mysql) library to read MySQL binlogs. This library enables us to monitor MySQL binlogs and capture changes occurring in database tables.

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

# Hashring configuration for HA and load balancing purposes
hashring:

  # Sync worker time in milliseconds to sync the ring nodes when a new node is added or removed from the ring
  sync_worker_time_ms: 300
  
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

    # Read timeout in seconds
    read_timeout: 90

    # Heartbeat period in seconds
    heartbeat_period: 60

    # Flavor of the database. MySQL or MariaDB
    flavor: mysql

    # Timeout for syncing the events in milliseconds
    sync_timeout_ms: 200

    # Just listen for events in these database-table pairs
    filter_tables:
      - database: test
        table: test

# Data connectors configuration
# List of connectors to send the data to
connectors:

  # Routes configuration for the connectors. It determines the events that will be sent to each connector and
  # the data format that will be sent to the connector.
  # Events can be insert, update or delete
  # Data format is in golang template format. The data read from the binlog will be passed to the template as .data
  routes:
    - events: ["insert", "update"]
      connector: pubsub
      data: |
        {{- printf `{ "index": "test", "id": %v, "data": %s}` .data.id (toJson .data) }}
    - events: ["delete"]
      connector: webhook
      data: |
        {{- printf `{ "index": "test", "id": %v }` .data.id }}

  # Connectors configuration. Currently only pubsub and webhook are supported.

  # PubSub connector configuration
  pubsub:
    project_id: "test-project"
    topic_id: "test-topic"

  # Webhook connector configuration
  webhook:
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
The high availability and failure recovery process works as follows:

* When three nodes (A, B, and C) exist in the hashring, they operate in synchronization up to, for example, position 30 
in the binlog. They maintain synchronization by communicating with each other via http://<ip>:<port>/position at 
intervals defined by the sync_worker_time_ms parameter.
* If node A fails, the hashring automatically reorganizes so nodes B and C redistribute the workload. These nodes 
detect node A's failure and immediately begin reading from the binlog at A's last known position (position 30). 
From this point, B and C divide A's workload between them while continuing to process the binlog.
* When node A recovers, the hashring reorganizes again. Node A begins reading from the binlog starting at the lowest 
position found between nodes B and C. From this point forward, the workload is once again distributed equally among 
all three nodes.

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
        
      hashring:
        sync_worker_time_ms: 300
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
          read_timeout: 90
          heartbeat_period: 60
          flavor: mysql
          sync_timeout_ms: 200
          filter_tables:
            - database: test
              table: test
      
      connectors:
        routes:
          - events: ["insert", "update"]
            connector: webhook
            data: |
              {{- printf `{ "index": "test", "id": %v, "data": %s }` .data.id ( toJson .data ) }}
          - events: ["delete"]
            connector: webhook
            data: |
              {{- printf `{ "index": "test", "id": %v }` .data.id }}

        webhook:
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