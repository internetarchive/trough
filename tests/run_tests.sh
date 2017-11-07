#!/bin/bash

set -e

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker build -t internetarchive/rethinkdb $script_dir

for python in python3
do
    docker run --rm -it --volume="$script_dir/..:/trough" internetarchive/rethinkdb /sbin/my_init -- \
        bash -x -c "cd /tmp && git clone /trough \
                && cd /tmp/trough \
                && (cd /trough && git diff) | patch -p1 \
                && virtualenv -p $python /tmp/venv \
                && source /tmp/venv/bin/activate \
                && pip install pytest git+https://github.com/jkafader/snakebite@feature/python3-version-string -e /trough --no-input --upgrade \
                && py.test -v tests \
                "
done
