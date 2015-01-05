# Copyright 2014 Symantec Corp.
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

from boto.dynamodb2 import layer1 as dynamodb
import urlparse

from tempest import config_magnetodb as config
from tempest.services import botoclients


CONF = config.CONF
dynamodb.DynamoDBConnection.NumberRetries = 0


class APIClientDynamoDB(botoclients.BotoClientBase):

    def connect_method(self, *args, **kwargs):
        return dynamodb.DynamoDBConnection(**kwargs)

    def __init__(self, *args, **kwargs):
        super(APIClientDynamoDB, self).__init__(*args, **kwargs)
        aws_access = CONF.boto.aws_access
        aws_secret = CONF.boto.aws_secret
        purl = urlparse.urlparse(CONF.boto.magnetodb_url)
        port = purl.port
        if port is None:
            if purl.scheme is not "https":
                port = 80
            else:
                port = 443
        else:
            port = int(port)
        self.connection_data = {"aws_access_key_id": aws_access,
                                "aws_secret_access_key": aws_secret,
                                "is_secure": purl.scheme == "https",
                                "host": purl.hostname,
                                "port": port}

    ALLOWED_METHODS = set(('create_table', 'delete_table', 'list_tables',
                           'update_table', 'describe_table',
                           'batch_get_item', 'batch_write_item',
                           'put_item', 'update_item', 'delete_item',
                           'get_item', 'query', 'scan'))
