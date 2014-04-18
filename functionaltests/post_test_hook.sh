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

# Install packages from test-requirements.txt

sudo pip install -r $DEST_DIR/magnetodb/test-requirements.txt

# Preparing tempest.conf

cd $DEST_DIR/magnetodb/functionaltests
ip=$(/sbin/ip a | grep eth0|grep inet|awk '{print $2}'|sed 's/\/.*//g')
sudo cp $DEST_DIR/tempest/etc/tempest.conf $DEST_DIR/magnetodb/tempest/tempest.conf

sudo sed -e '{ /\[boto\]/ a\
magnetodb_url = http://'$ip':8480
}' -i $DEST_DIR/magnetodb/tempest/tempest.conf
sudo sed -e "s/#aws_secret=<None>/aws_secret = ''/" -e "s/#aws_access=<None>/aws_access = ''/" -i $DEST_DIR/magnetodb/tempest/tempest.conf
sudo bash -c "cat <<EOF >>$DEST_DIR/magnetodb/tempest/tempest.conf
[magnetodb]
service_type = kv-storage
EOF"

# Run tempest tests

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

cd $DEST_DIR/magnetodb/
sudo cp tempest/tempest.conf $LOGS_DIR/magnetodb_tempest_conf

if [ -f tempest/tempest.log ] ; then
    sudo cp tempest/tempest.log $LOGS_DIR/magnetodb_tempest.log
fi

exit $RETVAL
