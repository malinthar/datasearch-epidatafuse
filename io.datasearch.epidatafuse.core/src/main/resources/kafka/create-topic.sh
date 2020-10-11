#!/usr/bin/env bash

## Create topics
/opt/kafka_2.12-2.5.0/bin/kafka-topics.sh --create \
    --replication-factor 1 \
    --partitions 2 \
    --topic diseasedata \
    --zookeeper  localhost:2181


## List created topics
/opt/kafka_2.12-2.5.0/bin/kafka-topics.sh --list \
    --zookeeper localhost:2181