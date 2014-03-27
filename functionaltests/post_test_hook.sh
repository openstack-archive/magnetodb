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
ip=$(/sbin/ip a | grep eth0|grep inet|awk '{print $2}'|sed 's/\/.*//g')

sudo pip install -r /opt/stack/new/magnetodb/test-requirements.txt
sudo sed -e 's|magnetodb_url.*$|magnetodb_url = http://'$ip':8480|' -i /opt/stack/new/magnetodb/tempest/tempest.conf
cd /opt/stack/new/magnetodb/functionaltests
sudo ./run_tests.sh

