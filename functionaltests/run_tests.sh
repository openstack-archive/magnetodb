#!/bin/bash -x
#This script will run tepmest
TEMPEST_DIR=${TEMPEST_DIR:-/opt/stack/new/magnetodb/tempest}
LOGS_DIR=/opt/stack/logs
cd $TEMPEST_DIR

echo '============== Start stable tests ==============='
sudo bash -c "nosetests -s --with-subunit api/keyvalue/stable/ 2>&1 | tee /opt/stack/logs/tempest-stable.txt"
RETVAL=${PIPESTATUS[0]}
echo '============ Start in_progress tests ============'
sudo nosetests -s --with-subunit api/keyvalue/in_progress/ 2>&1 | sudo tee $LOGS_DIR/tempest-in-progress.txt
echo '============ Start not_ready tests =============='
nosetests --with-subunit api/keyvalue/not_ready/ 2>&1 | tee $LOGS_DIR/tempest-not-ready.txt
exit $RETVAL
