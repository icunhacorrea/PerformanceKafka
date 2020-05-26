#!/bin/bash

for ack in -2 # -1 0 1
do
	for size in 30000 # 30000
	do
		for i in {0..20}
		do
			n=$(( i % 5))
			if [ $n -eq 0 ]; then
				./admin/refreshTopic.sh
			fi
			#./admin/createTopic.sh 3 3
			#./admin/refreshTopic.sh
			#./admin/deleteZnodes.sh
			time ./execute.sh test-topic $ack 80000 $size > logs/log-$ack-$size-$i.out
			sleep 30
		done
	done
done
