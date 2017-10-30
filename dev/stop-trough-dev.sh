#!/bin/bash

if [ -z "$VIRTUAL_ENV" ] ; then
    echo '$VIRTUAL_ENV is not set (please activate your trough virtualenv)'
    exit 1
fi

python -c 'import trough'
if [ $? -ne 0 ]; then
    echo "trough module could not be imported. Are you in the right virtualenv?"
    exit 1
fi

pkill -f $VIRTUAL_ENV/bin/reader.py
pkill -f $VIRTUAL_ENV/bin/writer.py
pkill -f $VIRTUAL_ENV/bin/sync.py
pkill -f trough.wsgi.segment_manager:local
pkill -f trough.wsgi.segment_manager:server

# XXX see start-trough-dev.sh
hadoop_container_ip=$(docker exec -it hadoop ifconfig eth0 | egrep -o 'addr:[^ ]+' | awk -F: '{print $2}')
sudo ifconfig lo0 -alias $hadoop_container_ip
