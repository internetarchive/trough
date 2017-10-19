#!/bin/bash

if [ -z "$VIRTUAL_ENV" ] ; then
    echo '$VIRTUAL_ENV is not set (please activate your trough virtualenv)'
    exit 1
fi

pkill -f $VIRTUAL_ENV/bin/reader.py
pkill -f $VIRTUAL_ENV/bin/writer.py
pkill -f $VIRTUAL_ENV/bin/sync.py
pkill -f trough.wsgi.segment_manager:local
pkill -f trough.wsgi.segment_manager:server
