#!/bin/bash

arr=( $( jps | grep ProducerPerformance | cut -f 1 -d ' ' ) )

for i in ${arr[@]}; do
        kill -9 $i
        echo $i
done

