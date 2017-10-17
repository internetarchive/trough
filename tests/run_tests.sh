#!/bin/bash

set -e

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker build -t internetarchive/rethinkdb-plus-hdfs $script_dir

for python in python3
do
    docker run --rm -it --volume="$script_dir/..:/trough" internetarchive/rethinkdb-plus-hdfs /sbin/my_init -- \
        bash -x -c "cd /tmp && git clone /trough \
                && cd /tmp/trough \
                && (cd /trough && git diff HEAD) | patch -p1 \
                && virtualenv -p $python /tmp/venv \
                && source /tmp/venv/bin/activate \
                && pip install -e /trough --no-input --upgrade --pre --index-url https://devpi.archive.org/ait/packages/+simple/ \
                && pip install pytest \
                && py.test -v tests"
done
