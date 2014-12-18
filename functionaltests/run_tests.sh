#!/bin/bash -x
#This script will run tepmest
TEMPEST_DIR=${TEMPEST_DIR:-/opt/stack/new/tempest/tempest}
LOGS_DIR=/opt/stack/logs
cd $TEMPEST_DIR

echo '============== Start stable tests ==============='
tox -e magnetodb-stable 2>&1 | tee $LOGS_DIR/tempest-stable.txt
RETVAL=${PIPESTATUS[0]}
echo '============ Start in_progress tests ============'
tox -e magnetodb-in-progress 2>&1 | tee $LOGS_DIR/tempest-in-progress.txt
echo '============ Start not_ready tests =============='
tox -e magnetodb-not-ready 2>&1 | tee $LOGS_DIR/tempest-not-ready.txt
exit $RETVAL
