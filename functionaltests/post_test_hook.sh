#!/bin/bash
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

# Install packages from test-requirements.txt

sudo pip install -r /opt/stack/new/magnetodb/test-requirements.txt
cd /opt/stack/new/magnetodb/functionaltests
ip=$(/sbin/ip a | grep eth0|grep inet|awk '{print $2}'|sed 's/\/.*//g')
sudo cp /opt/stack/new/tempest/etc/tempest.conf /opt/stack/new/magnetodb/tempest/tempest.conf
sudo sed -e '{
/\[boto\]/ a\
magnetodb_url = http://'$ip':8480
}' -i /opt/stack/new/tempest/etc/tempest.conf
sudo ./run_tests.sh
RETVAL=$?

#preparing artifacts for publishing

LOGS_DIR=/opt/stack/logs
cd /opt/stack/new/magnetodb/
sudo cp tempest/tempest.conf $LOGS_DIR/magnetodb_tempest_conf

if [ -f tempest/tempest.log ] ; then
    sudo cp tempest/tempest.log $LOGS_DIR/magnetodb_tempest.log
fi

sudo mkdir $LOGS_DIR/magnetodb_etc/
sudo cp -r etc/* $LOGS_DIR/magnetodb_etc/
exit $RETVAL
