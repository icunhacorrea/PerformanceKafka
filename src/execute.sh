#!/bin/bash

java -cp "ProducerPerformance.jar:../jars/*" main.ProducerPerformance $1 $2 $3 $4 -Xms2048M -Xmx4096M

