#!/bin/bash

javac -cp .:../jars/*  ./main/*.java -Xlint
jar -cmf MANIFEST.MF ProducerPerformance.jar ./main/*.class
rm ./main/*.class

