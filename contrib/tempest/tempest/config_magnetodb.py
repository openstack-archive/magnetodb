# Copyright 2014 Mirantis Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from oslo_config import cfg

from tempest import config  # noqa

CONF = config.CONF


magnetodb_group = cfg.OptGroup(name="magnetodb",
                               title="Key-Value storage options")

magnetodb_url_opt = cfg.StrOpt('magnetodb_url',
                               default="http://localhost:8480",
                               help="MagnetoDB URL")

MagnetoDBGroup = [
    cfg.StrOpt('catalog_type',
               default="kv-storage",
               help="The name of the MagnetoDB service type"),
    cfg.StrOpt('region',
               default='RegionOne',
               help="The magnetodb region name to use. If empty, the value "
                    "of identity.region is used instead. If no such region "
                    "is found in the service catalog, the first found one is "
                    "used."),
]

magnetodb_streaming_group = cfg.OptGroup(
    name="magnetodb_streaming",
    title="Key-Value storage steaming API options")

MagnetoDBStreamingGroup = [
    cfg.StrOpt('catalog_type',
               default="kv-streaming",
               help="The name of the MagnetoDB streaming API service type"),
]

magnetodb_monitoring_group = cfg.OptGroup(
    name="magnetodb_monitoring",
    title="Key-Value storage monitoring API options")

MagnetoDBMonitoringGroup = [
    cfg.StrOpt('catalog_type',
               default="kv-monitoring",
               help="The name of the MagnetoDB monitoring API service type"),
]

magnetodb_management_group = cfg.OptGroup(
    name="magnetodb_management",
    title="Key-Value storage management API options")

MagnetoDBManagementGroup = [
    cfg.StrOpt('catalog_type',
               default="kv-management",
               help="The name of the MagnetoDB management API service type"),
]


class TempestConfigPrivateMagnetoDB(config.TempestConfigPrivate):

    def __init__(self, parse_conf=True):
        config.register_opt_group(cfg.CONF, magnetodb_group, MagnetoDBGroup)
        config.register_opt_group(cfg.CONF, magnetodb_streaming_group,
                                  MagnetoDBStreamingGroup)
        config.register_opt_group(cfg.CONF, magnetodb_monitoring_group,
                                  MagnetoDBMonitoringGroup)
        config.register_opt_group(cfg.CONF, magnetodb_management_group,
                                  MagnetoDBManagementGroup)
        cfg.CONF.register_opt(magnetodb_url_opt, group='boto')
        self.magnetodb = cfg.CONF.magnetodb
        self.magnetodb_streaming = cfg.CONF.magnetodb_streaming
        self.magnetodb_monitoring = cfg.CONF.magnetodb_monitoring
        self.magnetodb_management = cfg.CONF.magnetodb_management


class TempestConfigProxyMagnetoDB(object):
    _config = None

    def __getattr__(self, attr):
        if not self._config:
            self._config = TempestConfigPrivateMagnetoDB()
        return getattr(self._config, attr)


CONF = TempestConfigProxyMagnetoDB()
