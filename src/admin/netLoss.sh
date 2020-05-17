#!/bin/bash


if [ $1 == "loss" ]; then
        sudo tc qdisc add dev enp3s0:0 root netem loss $2
elif [ $1 == "delete" ]; then
        sudo tc qdisc del dev enp3s0:0 root netem loss $2
fi
