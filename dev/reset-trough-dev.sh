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

script_dir=$(dirname $VIRTUAL_ENV)/dev
$script_dir/stop-trough-dev.sh

rm -vrf /tmp/trough-*.out
rm -vrf /var/tmp/trough
python -c "import doublethink ; from trough.settings import settings ; rr = doublethink.Rethinker(settings['RETHINKDB_HOSTS']) ; print(rr.db_drop('trough_configuration').run())"
