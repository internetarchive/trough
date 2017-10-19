#!/bin/bash

if [ -z "$VIRTUAL_ENV" ] ; then
    echo '$VIRTUAL_ENV is not set (please activate your trough virtualenv)'
    exit 1
fi

source $VIRTUAL_ENV/bin/activate

set -x

rethinkdb >>/tmp/rethinkdb.log 2>&1 &
docker run --detach --rm --name=hadoop --publish=8020:8020 --publish=50070:50070 --publish=50010:50010 --publish=50020:50020 --publish=50075:50075 chalimartines/cdh5-pseudo-distributed && sleep 30

$VIRTUAL_ENV/bin/sync.py >>/tmp/trough-sync-local.out 2>&1 &
sleep 5
python -c "import doublethink ; from trough.settings import settings ; rr = doublethink.Rethinker(settings['RETHINKDB_HOSTS']) ; rr.db('trough_configuration').wait().run()"

uwsgi --venv=$VIRTUAL_ENV --http :6444 --master --processes=2 --harakiri=3200 --socket-timeout=3200 --max-requests=50000 --vacuum --die-on-term --wsgi-file $VIRTUAL_ENV/bin/reader.py >>/tmp/trough-read.out 2>&1 &
uwsgi --venv=$VIRTUAL_ENV --http :6222 --master --processes=2 --harakiri=240 --max-requests=50000 --vacuum --die-on-term --wsgi-file $VIRTUAL_ENV/bin/writer.py >>/tmp/trough-write.out 2>&1 &
$VIRTUAL_ENV/bin/sync.py --server >>/tmp/trough-sync-server.out 2>&1 &
uwsgi --venv=$VIRTUAL_ENV --http :6112 --master --processes=2 --harakiri=20 --max-requests=50000 --vacuum --die-on-term --mount /=trough.wsgi.segment_manager:local >>/tmp/trough-segment-manager-local.out 2>&1 &
uwsgi --venv=$VIRTUAL_ENV --http :6111 --master --processes=2 --harakiri=20 --max-requests=50000 --vacuum --die-on-term --mount /=trough.wsgi.segment_manager:server >>/tmp/trough-segment-manager-server.out 2>&1 &
