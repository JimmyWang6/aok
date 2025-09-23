# AMQP Protocol on Kafka Queues
[![Apache License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Implement AMQP protocol base on Kafka Queues.([KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka))
Aok broker is a stateless process, backed by Apache Kafka, serving as the storage layer for messages and metadata.

## Quick Start
### Prerequisites
In order to run AOK, you must first start Apache Kafka version 4.1 or above and enable the share group feature.
You can follow the instructions in the [Kafka Quick Start](https://kafka.apache.org/quickstart) to set up a Kafka cluster.
Notice that should set the following configurations in `server.properties`:
```
group.share.enable=true
```

### Run Aok locally
Aok runs on all major operating systems and requires only a Java JDK version 8 or higher to be installed.
To check, run `java -version`:
```shell
$ java -version
java version "1.8.0_121"
```

## Code structure


## About