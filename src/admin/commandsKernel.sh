#!/usr/bin/bash

sudo sysctl -w net.ipv4.tcp_retries1=1
sudo sysctl -w net.ipv4.tcp_retries2=1
sudo sysctl -w net.ipv4.tcp_synack_retries=0
sudo sysctl -w net.ipv4.tcp_syn_retries=1
sudo sysctl -w net.ipv4.tcp_fack=0
sudo sysctl -w net.ipv4.tcp_dsack=0
sudo sysctl -w net.ipv4.tcp_sack=0
