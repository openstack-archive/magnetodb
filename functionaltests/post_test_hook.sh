#!/bin/bash -x
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

# This script is executed inside post_test_hook function in devstack gate.

DEST_DIR=/opt/stack/new
LOGS_DIR=/opt/stack/logs
CCI_BUILD_PATH=$DEST_DIR/magnetodb/contrib/cassandra/magnetodb-cassandra-custom-indices/build

# Run tempest tests
cd $DEST_DIR/magnetodb/functionaltests

sudo ./run_tests.sh
RETVAL=$?

# Convert to html
FILES=`ls $LOGS_DIR/tempest-[ins]*`
echo "$FILES"

if [ -n "$FILES" ]; then
    for i in $FILES; do
        echo $i
        sudo python /usr/local/jenkins/slave_scripts/subunit2html.py $i $i.html
    done
fi

# Preparing artifacts for publishing

cd $DEST_DIR/magnetodb/contrib/
sudo cp tempest/tempest.conf $LOGS_DIR/magnetodb_tempest_conf

if [ -f tempest/tempest.log ] ; then
    sudo cp tempest/tempest.log $LOGS_DIR/magnetodb_tempest.log
fi

#Saving cassandra logs
echo `ls -la $DEST_DIR/.ccm/`
echo "Saving Cassandra logs"
CASSANDRA_NODES=`ls $DEST_DIR/.ccm/test/|grep node`

if [ -n "$CASSANDRA_NODES" ]; then
    for i in $CASSANDRA_NODES; do
        echo $i
        CASSANDRA_LOG_FILES=`ls $DEST_DIR/.ccm/test/${i}/logs/`
        for l in $CASSANDRA_LOG_FILES;do
            echo $l
            sudo cp $DEST_DIR/.ccm/test/${i}/logs/$l $LOGS_DIR/cassandra_${i}_${l}
        done
    done
fi

#Saving magnetodb-cassandra-custom-indices.jar
CCI_JAR=`ls $CCI_BUILD_PATH/libs/magnetodb-cassandra-custom-indices-*.jar`
if [ -n $CCI_JAR ]; then
  echo "Saving magnetodb-cassandra-custom-indices.jar"
  sudo cp $CCI_JAR $LOGS_DIR
fi

exit $RETVAL
