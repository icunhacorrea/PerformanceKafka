#!/bin/bash

for ack in -2 -1 0 1
do
	for size in 250 500 750 1000
	do
		for i in {0..2}
		do
			./admin/refreshTopic.sh
			time ./execute.sh test-topic $ack 50000 $size > logs/log-$ack-$size-$i.out
			sleep 20
		done
	done
done
