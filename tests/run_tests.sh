#!/bin/bash

set -e

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker build -t internetarchive/trough-tests $script_dir

# TODO some of these uwsgi services are not what we run anymore

for python in python3
do
    docker run --rm -it --volume="$script_dir/..:/trough" internetarchive/rethinkdb-plus-hdfs /sbin/my_init -- \
        bash -x -c "cd /tmp && git clone /trough \
                && cd /tmp/trough \
                && (cd /trough && git diff HEAD) | patch -p1 \
                && virtualenv -p $python /tmp/venv \
                && source /tmp/venv/bin/activate \
                && pip install -e /trough --no-input --upgrade --pre --index-url https://devpi.archive.org/ait/packages/+simple/ \
                && sync.py >>/tmp/trough-sync-local.out 2>&1
                & sleep 5
                && sync.py --server >>/tmp/trough-sync-server.out 2>&1
                & uwsgi --venv=/tmp/venv --http :6444 --master --processes=2 --harakiri=3200 --socket-timeout=3200 --max-requests=50000 --vacuum --die-on-term --wsgi-file /tmp/venv/bin/reader.py >>/tmp/trough-read.out 2>&1
                & uwsgi --venv=/tmp/venv --http :6222 --master --processes=2 --harakiri=240 --max-requests=50000 --vacuum --die-on-term --wsgi-file /tmp/venv/bin/writer.py >>/tmp/trough-write.out 2>&1
                & uwsgi --venv=/tmp/venv --http :6112 --master --processes=2 --harakiri=20 --max-requests=50000 --vacuum --die-on-term --wsgi-file /tmp/venv/bin/write_provisioner_local.py >>/tmp/trough-write-provisioner-local.out 2>&1
                & uwsgi --venv=/tmp/venv --http :6111 --master --processes=2 --harakiri=20 --max-requests=50000 --vacuum --die-on-term --wsgi-file /tmp/venv/bin/write_provisioner_server.py >>/tmp/trough-write-provisioner-server.out
                && pip install pytest \
                && py.test -v tests"
done


