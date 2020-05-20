#!/bin/bash


# args:
#      $1 rep factor
#      $2 num of partitions

KAFKA_PATH=/opt/kafka

echo "Setando retention.ms para 1 seg."

$KAFKA_PATH//bin/kafka-configs.sh --zookeeper 14.0.0.1:2181,14.0.0.3:2181,14.0.0.6:2181 \
        --alter --entity-type topics --entity-name test-topic \
        --add-config retention.ms=10


echo "Esperando 3 minutos segundos para resetar topico."
for i in seq {0..240}
do
	printf .
	sleep 1
done

echo ""
echo "Restaurando config  default."
$KAFKA_PATH//bin/kafka-configs.sh --zookeeper 14.0.0.1:2181,14.0.0.3:2181,14.0.0.6:2181 \
        --alter --entity-type topics --entity-name test-topic \
	--delete-config retention.ms

# list the topic details
kafkacat -L -b 14.0.0.1:9092,14.0.0.3:9092,14.0.0.6:9092 -t test-topic
