#!/bin/bash


# args:
#      $1 rep factor
#      $2 num of partitions

KAFKA_PATH=/opt/kafka

# first delete partition

$KAFKA_PATH/bin/kafka-topics.sh --zookeeper 14.0.0.1:2181,14.0.0.3:2181,14.0.0.6:2181 --delete --topic test-topic

sshpass -p "lsc" ssh -t lsc@14.0.0.1 "/opt/kafka/bin/zookeeper-shell.sh localhost:2181 delete /brokers/topics/test-topic"

sleep 5

# then, create partition

$KAFKA_PATH/bin/kafka-topics.sh --create --bootstrap-server 14.0.0.1:9092,14.0.0.3:9092,14.0.0.6:9092 \
        --replication-factor $1 --partitions $2 --topic test-topic # --config message.timestamp.type=CreateTime

sleep 5
#$KAFKA_PATH//bin/kafka-configs.sh --zookeeper localhost:2181 \
##      --alter --entity-type topics --entity-name test-topic \
##      --add-config min.insync.replicas=3

# list the topic details
kafkacat -L -b 14.0.0.1:9092,14.0.0.3:9092,14.0.0.6:9092 -t test-topic
