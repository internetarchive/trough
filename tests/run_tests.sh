#!/bin/bash

set -e

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker build -t internetarchive/trough-tests $script_dir

docker run --rm -it --volume="$script_dir/..:/trough" internetarchive/trough-tests /sbin/my_init -- bash -c \
    $'bash -x -c "cd /tmp && git clone /trough \
            && cd /tmp/trough \
            && (cd /trough && git diff HEAD) | patch -p1 \
            && virtualenv -p python3 /tmp/venv \
            && source /tmp/venv/bin/activate \
            && pip install pytest -e /trough --no-input --upgrade" \
    && bash -x -c "source /tmp/venv/bin/activate \
            && sync.py >>/tmp/trough-sync-local.out 2>&1 &" \
    && bash -x -c "source /tmp/venv/bin/activate \
            && sleep 5 \
            && python -c \\"import doublethink ; from trough.settings import settings ; rr = doublethink.Rethinker(settings[\'RETHINKDB_HOSTS\']) ; rr.db(\'trough_configuration\').wait().run()\\"" \
    && bash -x -c "source /tmp/venv/bin/activate \
            && sync.py --server >>/tmp/trough-sync-server.out 2>&1 &" \
    && bash -x -c "source /tmp/venv/bin/activate \
            && uwsgi --daemonize2 --venv=/tmp/venv --http :6444 --master --processes=2 --harakiri=3200 --http-timeout=3200 --socket-timeout=3200 --max-requests=50000 --vacuum --die-on-term --wsgi-file /tmp/venv/bin/reader.py >>/tmp/trough-read.out 2>&1 \
            && uwsgi --daemonize2 --venv=/tmp/venv --http :6222 --master --processes=2 --harakiri=240 --http-timeout=240 --max-requests=50000 --vacuum --die-on-term --wsgi-file /tmp/venv/bin/writer.py >>/tmp/trough-write.out 2>&1 \
            && uwsgi --daemonize2 --venv=/tmp/venv --http :6112 --master --processes=2 --harakiri=7200 --http-timeout=7200 --max-requests=50000 --vacuum --die-on-term --mount /=trough.wsgi.segment_manager:local >>/tmp/trough-segment-manager-local.out 2>&1 \
            && uwsgi --daemonize2 --venv=/tmp/venv --http :6111 --master --processes=2 --harakiri=7200 --max-requests=50000 --vacuum --die-on-term --mount /=trough.wsgi.segment_manager:server >>/tmp/trough-segment-manager-server.out 2>&1 \
            && cd /tmp/trough \
            && py.test -v tests"'
