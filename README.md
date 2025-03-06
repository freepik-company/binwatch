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

# Server name of the binwatch instance. Can be set via environment variable which will be replaced at runtime
server_name: "$HOSTNAME"

# Hashring configuration for HA and load balancing purposes
hashring:

  # Sync worker time in milliseconds to sync the ring nodes when a new node is added or removed from the ring
  sync_worker_time_ms: 300

  # Health check configuration for the hashring
  health_check:
    # Health check type. Supported types are http and icmp
    type: icmp
    # Just needed for http health check. The path to check the health of the server
    # path: "/health"
    # Timeout in seconds for the health check
    timeout: 1
    
  # For static ring discovery we need to define the server names that are part of the ring
  static_ring_discovery:
    hosts:
      - test
      - test2

  # For DNS ring discovery we need to define the domain and the port of the headless service to get
  # the list of server names that are part of the ring
  # Recommended for kubernetes deployments with headless services
  # dns_ring_discovery:
  #   domain: "dns.example.com"

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

headlessService:
  enabled: true

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
      
      server_name: "$POD_IP"
        
      hashring:
        sync_worker_time_ms: 300
        health_check:
          type: icmp
          timeout: 1
        dns_ring_discovery:
          domain: "binwatch-headless.binwatch.svc.cluster.local"
      
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
