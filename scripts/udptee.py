#!/usr/bin/env python3
'''
udptee.py - like `tee` but duplicates output to a udp destination instead of a
file
'''

import argparse
import sys
import os
import socket

def main(argv=['udptee.py']):
    arg_parser = argparse.ArgumentParser(
            prog=os.path.basename(argv[0]), description=(
                'like `tee` but duplicates output to a udp destination '
                'instead of a file'))
    arg_parser.add_argument(
            metavar='ADDRESS', dest='addresses', nargs='+', help=(
                'destination address "host:port"'))
    args = arg_parser.parse_args(args=argv[1:])

    addrs = []
    for address in args.addresses:
        host, port = address.split(':')
        port = int(port)
        addrs.append((host, port))

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    stdin = open(0, mode='rb', buffering=0)
    stdout = open(1, mode='wb', buffering=0)

    while True:
        line = stdin.readline()
        if not line:
            break
        stdout.write(line)
        for addr in addrs:
            # 1400 byte chunks to avoid EMSGSIZE
            for chunk in (line[i*1400:(i+1)*1400]
                          for i in range((len(line) - 1) // 1400 + 1)):
                sock.sendto(chunk, addr)

if __name__ == '__main__':
    main(sys.argv)
