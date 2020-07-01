#!/bin/bash

KAFKA_PATH=/opt/kafka

for ack in -2 #
do
	for size in 1000
	do
		for i in {0..25}
		do
			n=$(( i % 10 ))
			if [ $n -eq 0 ]; then
				./admin/refreshTopic.sh
			fi			
			time ./execute.sh test-topic $ack 80000 $size > logs/log-$ack-$size-$i.out
			echo "*****Produção finalizada, esperando 180 s para buscar WM*****" >> logs/log-$ack-$size-$i.out
			sleep 180 
			waterMark=$( $KAFKA_PATH/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
				--broker-list 14.0.0.1:9092,14.0.0.3:9092,14.0.0.6:9092 --time -1 \
				--topic test-topic | awk -F':' '{sum+=$3;} END {print sum;}' )
			echo "Water mark: $waterMark" >> logs/log-$ack-$size-$i.out
		done
	done
done

