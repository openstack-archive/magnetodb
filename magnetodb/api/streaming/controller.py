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
import Queue
from Queue import Empty
import logging
from threading import Event
import ujson as json
from magnetodb import storage
from magnetodb.api.openstack.v1 import parser
from magnetodb.storage.models import PutItemRequest
import webob

LOG = logging.getLogger(__name__)


class StreamingController(object):
    @webob.dec.wsgify
    def __call__(self, request):
        project_id = request.urlvars["project_id"]
        table_name = request.urlvars["table_name"]
        request.context.tenant = project_id
        count_sent = 0
        count_done = [0]

        event = Event()
        ready_queue = Queue.Queue()

        stream = request.environ['wsgi.input']
        for chunk in stream:
            count_sent += 1
            data = json.loads(chunk)
            item = parser.Parser.parse_item_attributes(data)
            future = storage.put_item_async(request.context,
                                            PutItemRequest(table_name, item))

            def callback(future):
                count_done[0] += 1
                ready_queue.put_nowait(future)
                event.set()

            future.add_done_callback(callback)
            try:
                while True:
                    finished_future = ready_queue.get_nowait()

                    try:
                        r = finished_future.result()
                        LOG.debug('Future result:' + str(r))
                    except Exception as e:
                        LOG.exception(str(e))
                    finished_future.result()
            except Empty:
                pass

            if count_sent % 1000 == 0:
                print "sent: {}, done: {}".format(count_sent, count_done[0])

        while count_done[0] < count_sent:
            event.wait()
            event.clear()
        print "sent: {}, done: {}".format(count_sent, count_done[0])
        return "Done"
