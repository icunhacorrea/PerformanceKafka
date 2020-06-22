#!/usr/bin/python

from netfilterqueue import NetfilterQueue
import socket

def print_and_accept(pkt):
    print(pkt)
    pkt.accept()

nfqueue = NetfilterQueue()
nfqueue.bind(1, print_and_accept)
sock = socket.fromfd(nfqueue.get_fd(), socket.AF_UNIX,
        socket.SOCK_STREAM)


try:
    nfqueue.run_socket(sock)
except:
    print('')

s.close()
nfqueue.unbind()

