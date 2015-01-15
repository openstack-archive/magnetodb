#!/bin/bash -x
#This script will run tepmest
TEMPEST_DIR=${TEMPEST_DIR:-/opt/stack/new/tempest/tempest}
LOGS_DIR=/opt/stack/logs
cd $TEMPEST_DIR

echo '============== Start stable tests ==============='
tox -e all tempest.api.keyvalue.stable -- --parallel --concurrency=2 2>&1 | tee $LOGS_DIR/magnetodb-stable.txt
find . -name testr_results.html 2>&1
#mv testr_results.html.gz magnetodb-stable.html.gz 
RETVAL=${PIPESTATUS[0]}
echo '============ Start in_progress tests ============'
tox -e all tempest.api.keyvalue.in_progress -- --parallel --concurrency=2 2>&1 | tee $LOGS_DIR/tempest-in-progress.txt
#mv testr_results.html.gz magnetodb-in-progress.html.gz 
echo '============ Start not_ready tests =============='
tox -e all tempest.api.keyvalue.not_ready -- --parallel --concurrency=2 2>&1 | tee $LOGS_DIR/tempest-not-ready.txt
#mv testr_results.html.gz magnetodb-not-ready.html.gz 
find . -name testr_results.html 2>&1
exit $RETVAL
