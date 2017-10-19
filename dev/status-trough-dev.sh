#!/bin/bash

if [ -z "$VIRTUAL_ENV" ] ; then
    echo '$VIRTUAL_ENV is not set (please activate your trough virtualenv)'
    exit 1
fi

for svc in $VIRTUAL_ENV/bin/reader.py $VIRTUAL_ENV/bin/writer.py trough.wsgi.segment_manager:local trough.wsgi.segment_manager:server $VIRTUAL_ENV/bin/sync.py ;
do
    echo === $svc ===
    pids=$(pgrep -f $svc)
    if [ -n "$pids" ] ; then
        ps $pids
    else
        echo not running
    fi
done

