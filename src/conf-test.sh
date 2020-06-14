#!/bin/bash

KAFKA_PATH=/opt/kafka

for ack in -1 # 0 1
do
	for size in 10000 # 20000 30000
	do
		for i in {0..1}
		do
			#n=$(( i % 5))
			#if [ $n -eq 0 ]; then
			#	./admin/refreshTopic.sh
			#fi
			time ./execute.sh test-topic $ack 8000 $size > logs/log-$ack-$size-$i.out
			waterMark=$( $KAFKA_PATH/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
				--broker-list 14.0.0.1:9092,14.0.0.3:9092,14.0.0.6:9092 --time -1 \
				--topic test-topic | awk -F':' '{sum+=$3;} END {print sum;}' )
			echo "*****Produção finalizada, esperando 180 para buscar WM*****" >> logs/log-$ack-$size-$i.out
			sleep 180
			echo "Water mark: $waterMark" >> logs/log-$ack-$size-$i.out
		done
	done
done

