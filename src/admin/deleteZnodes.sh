#!/bin/bash

IFS=$' '

CLI_PATH=/opt/zookeeper/bin/zkCli.sh
TOPIC_PATH=/brokers/topics/test-topic
SERVER="127.0.0.1:2181"
SUB="node"

znodes=( $( $CLI_PATH -server $SERVER ls $TOPIC_PATH | grep node ) )

for znode in "${znodes[@]}"; do
	if [[ $znode =~ $SUB ]]; then
		zN=$( echo $znode | tr -d , | tr -d [ | tr -d ] )
		echo "deleteall $TOPIC_PATH/$zN" >> znodePaths.txt
		printf .
	fi
done

$CLI_PATH -server $SERVER < znodePaths.txt
rm znodePaths.txt

