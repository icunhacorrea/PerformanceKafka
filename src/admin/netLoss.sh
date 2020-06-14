#!/bin/bash

PASS="lsc"

if [ $1 == "loss" ]; then
	
	sshpass -p $PASS ssh lsc@14.0.0.1 "sudo tc qdisc add dev eno1:0 root netem delay 60000ms 0ms $2%"
	sshpass -p $PASS ssh lsc@14.0.0.3 "sudo tc qdisc add dev enp2s0:1 root netem delay 60000ms 0ms $2%"
	sshpass -p $PASS ssh lsc@14.0.0.6 "sudo tc qdisc add dev enp2s0:0 root netem delay 60000ms 0ms $2%"


	#sshpass -p $PASS ssh lsc@14.0.0.1 "sudo tc qdisc add dev eno1:0 root netem loss $2%"
	#sshpass -p $PASS ssh lsc@14.0.0.3 "sudo tc qdisc add dev enp2s0:1 root netem loss $2%"
	#sshpass -p $PASS ssh lsc@14.0.0.6 "sudo tc qdisc add dev enp2s0:0 root netem loss $2%"

elif [ $1 == "delete" ]; then

	sshpass -p $PASS ssh lsc@14.0.0.1 "sudo tc qdisc del dev eno1:0 root netem delay 60000ms 0ms $2%"
	sshpass -p $PASS ssh lsc@14.0.0.3 "sudo tc qdisc del dev enp2s0:1 root netem delay 60000ms 0ms $2%"
	sshpass -p $PASS ssh lsc@14.0.0.6 "sudo tc qdisc del dev enp2s0:0 root netem delay 60000ms 0ms $2%"


	#sshpass -p $PASS ssh lsc@14.0.0.1 "sudo tc qdisc del dev eno1:0 root netem loss $2%"
	#sshpass -p $PASS ssh lsc@14.0.0.3 "sudo tc qdisc del dev enp2s0:1 root netem loss $2%"
	#sshpass -p $PASS ssh lsc@14.0.0.6 "sudo tc qdisc del dev enp2s0:0 root netem loss $2%"

fi

