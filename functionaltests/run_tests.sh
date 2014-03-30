#!/bin/bash
#This script will run tepmest
TEMPEST_DIR=${TEMPEST_DIR:-/opt/stack/new/magnetodb/tempest}
cd $TEMPEST_DIR

echo '============== Start stable tests ==============='
nosetests -v api/keyvalue/stable/
RETVAL=$?
echo '============ Start in_progress tests ============'
nosetests -v api/keyvalue/in_progress/ || true
echo '============ Start not_ready tests =============='
nosetests -v api/keyvalue/not_ready/ || true
exit $RETVAL
