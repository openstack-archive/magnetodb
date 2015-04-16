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
import re
import threading

from oslo_context import context as req_context
from oslo_serialization import jsonutils as json

from magnetodb import api
from magnetodb import notifier
from magnetodb.openstack.common import log as logging
from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils
from magnetodb import storage

LOG = logging.getLogger(__name__)

MAX_FUTURES = 100


@api.with_global_env(default_program='magnetodb-streaming-api')
def app_factory(global_conf, **local_conf):
    return bulk_load_app


def make_callback(queue, event, done_count, chunk):
    def callback(future):
        done_count[0] += 1
        queue.put_nowait((future, chunk))
        event.set()
    return callback


def make_put_item(item):
    data = json.loads(item)
    return parser.Parser.parse_item_attributes(data)


def bulk_load_app(environ, start_response):
    context = req_context.get_current()

    path = environ['PATH_INFO']

    LOG.debug('Request received: %s', path)

    _notifier = notifier.get_notifier()

    if not re.match("^/v1/data/\w+/tables/\w+/bulk_load$", path):
        start_response('404 Not found', [('Content-Type', 'text/plain')])
        yield 'Incorrect url. Please check it and try again\n'
        _notifier.error(
            context,
            notifier.EVENT_TYPE_STREAMING_PATH_ERROR,
            {'path': path})
        return

    url_comp = path.split('/')
    project_id = url_comp[3]
    table_name = url_comp[5]

    LOG.debug('Tenant: %s, table name: %s', project_id, table_name)

    utils.check_project_id(project_id)

    _notifier.info(
        context,
        notifier.EVENT_TYPE_STREAMING_DATA_START,
        {'path': path})

    read_count = 0
    processed_count = 0
    unprocessed_count = 0
    failed_count = 0
    put_count = 0
    done_count = [0]
    last_read = None
    failed_items = {}

    dont_process = False

    future_ready_event = threading.Event()
    future_ready_queue = Queue.Queue()

    stream = environ['wsgi.input']
    while True:
        chunk = stream.readline()

        if not chunk:
            break

        read_count += 1

        if dont_process:
            LOG.debug('Skipping item #%d', read_count)
            unprocessed_count += 1
            continue

        last_read = chunk

        try:
            future = storage.put_item_async(
                project_id, table_name, make_put_item(chunk)
            )

            put_count += 1

            future.add_done_callback(make_callback(
                future_ready_queue,
                future_ready_event,
                done_count,
                chunk
            ))

            # try to get result of finished futures
            try:
                while True:
                    finished_future, chunk = future_ready_queue.get_nowait()
                    finished_future.result()
                    processed_count += 1
            except Queue.Empty:
                pass

        except Exception as e:
            failed_items[chunk] = repr(e)
            dont_process = True
            LOG.debug('Error inserting item: %s, message: %s',
                      chunk, repr(e))

            _notifier.error(
                context,
                notifier.EVENT_TYPE_STREAMING_DATA_ERROR,
                {'path': path, 'item': chunk, 'error': e.message})

    LOG.debug('Request body has been read completely')

    # wait for all futures to be finished
    while done_count[0] < put_count:
        LOG.debug('Waiting for %d item(s) to be processed...',
                  put_count - done_count[0])
        future_ready_event.wait()
        future_ready_event.clear()

    LOG.debug('All items are processed. Getting results of item processing...')

    # get results of finished futures
    while done_count[0] > processed_count + failed_count:
        LOG.debug('Waiting for %d result(s)...',
                  done_count[0] - processed_count - failed_count)
        chunk = None
        try:
            finished_future, chunk = future_ready_queue.get_nowait()
            finished_future.result()
            processed_count += 1
        except Queue.Empty:
            break
        except Exception as e:
            failed_count += 1
            failed_items[chunk] = repr(e)
            LOG.debug('Error inserting item: %s, message: %s',
                      chunk, repr(e))

            _notifier.error(
                context,
                notifier.EVENT_TYPE_STREAMING_DATA_ERROR,
                {'path': path, 'item': chunk, 'error': e.message})

    # Update count if error happened before put_item_async was invoked
    if dont_process:
        failed_count += 1

    start_response('200 OK', [('Content-Type', 'application/json')])

    resp = {
        'read': read_count,
        'processed': processed_count,
        'unprocessed': unprocessed_count,
        'failed': failed_count,
        'last_item': last_read,
        'failed_items': failed_items
    }

    _notifier.info(
        context, notifier.EVENT_TYPE_STREAMING_DATA_END,
        {'path': path, 'response': resp})

    yield json.dumps(resp)
