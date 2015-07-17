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

# This script is executed inside pre_test_hook function in desvstack gate.

# Install magnetodb devstack integration
DEST_DIR=/opt/stack/new
DEVSTACK_GATE=$DEST_DIR/devstack-gate

export DEVSTACK_LOCAL_CONFIG="enable_plugin magnetodb https://github.com/stackforge/magnetodb"
sed -e 's/ERROR_ON_CLONE=True/ERROR_ON_CLONE=False/' -i $DEVSTACK_GATE/devstack-vm-gate.sh
