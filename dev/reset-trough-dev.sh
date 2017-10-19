#!/bin/bash

venv=/Users/nlevitt/workspace/trough/trough-ve3
source $venv/bin/activate

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
$script_dir/stop-trough-dev.sh

rm -vrf /tmp/trough-*.out
rm -vrf /var/tmp/trough
python -c "import doublethink ; from trough.settings import settings ; rr = doublethink.Rethinker(settings['RETHINKDB_HOSTS']) ; print(rr.db_drop('trough_configuration').run())"
