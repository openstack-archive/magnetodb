import time
from cassandra import OperationTimedOut
import gevent
from gevent import select, socket
from gevent.event import Event
from gevent.queue import Queue

from collections import defaultdict
from functools import partial
import logging
import os
import sys
import traceback

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO # ignore flake8 warning: # NOQA

from errno import EALREADY, EINPROGRESS, EWOULDBLOCK, EINVAL

from cassandra.connection import (Connection, ResponseWaiter, ConnectionShutdown,
                                  ConnectionBusy, MAX_STREAM_PER_CONNECTION)
from cassandra.decoder import RegisterMessage
from cassandra.marshal import int32_unpack


log = logging.getLogger(__name__)


def is_timeout(err):
    return (
        err in (EINPROGRESS, EALREADY, EWOULDBLOCK) or
        (err == EINVAL and os.name in ('nt', 'ce'))
    )


class GeventConnection(Connection):
    """
An implementation of :class:`.Connection` that utilizes ``gevent``.
"""

    _total_reqd_bytes = 0
    _read_watcher = None
    _write_watcher = None
    _socket = None

    @classmethod
    def factory(cls, *args, **kwargs):
        timeout = kwargs.pop('timeout', 5.0)
        conn = cls(*args, **kwargs)
        conn.connected_event.wait(timeout)
        if conn.last_error:
            raise conn.last_error
        elif not conn.connected_event.is_set():
            conn.close()
            raise OperationTimedOut("Timed out creating connection")
        else:
            return conn

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)

        self.connected_event = Event()
        self._iobuf = StringIO()
        self._write_queue = Queue()

        self._callbacks = {}
        self._push_watchers = defaultdict(set)

        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.ssl_options:
            if not ssl:
                raise Exception("This version of Python was not compiled with SSL support")
            self._socket = ssl.wrap_socket(self._socket, **self.ssl_options)
        self._socket.settimeout(1.0)  # TODO potentially make this value configurable
        self._socket.connect((self.host, self.port))

        if self.sockopts:
            for args in self.sockopts:
                self._socket.setsockopt(*args)

        self._read_watcher = gevent.spawn(lambda: self.handle_read())
        self._write_watcher = gevent.spawn(lambda: self.handle_write())
        self._send_options_message()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s", id(self), self.host)
        if self._read_watcher:
            self._read_watcher.kill()
        if self._write_watcher:
            self._write_watcher.kill()
        if self._socket:
            self._socket.close()
        log.debug("Closed socket to %s" % (self.host,))

        # don't leave in-progress operations hanging
        self.connected_event.set()
        if not self.is_defunct:
            self._error_all_callbacks(
                ConnectionShutdown("Connection to %s was closed" % self.host))

    def defunct(self, exc):
        with self.lock:
            if self.is_defunct or self.is_closed:
                return
            self.is_defunct = True

        trace = traceback.format_exc(exc)
        if trace != "None":
            log.debug("Defuncting connection (%s) to %s: %s\n%s",
                      id(self), self.host, exc, traceback.format_exc(exc))
        else:
            log.debug("Defuncting connection (%s) to %s: %s",
                      id(self), self.host, exc)

        self.last_error = exc
        self._error_all_callbacks(exc)
        self.connected_event.set()
        return exc

    def _error_all_callbacks(self, exc):
        with self.lock:
            callbacks = self._callbacks
            self._callbacks = {}
        new_exc = ConnectionShutdown(str(exc))
        for cb in callbacks.values():
            try:
                cb(new_exc)
            except Exception:
                log.warn("Ignoring unhandled exception while erroring callbacks for a "
                         "failed connection (%s) to host %s:",
                         id(self), self.host, exc_info=True)

    def handle_error(self):
        self.defunct(sys.exc_info()[1])

    def handle_close(self):
        log.debug("connection closed by server")
        self.close()

    def handle_write(self):
        wlist = (self._socket,)

        while True:
            try:
                next_msg = self._write_queue.get()
                select.select((), wlist, ())
            except Exception as err:
                log.debug("Write loop: got error %s" % err)
                return

            try:
                self._socket.sendall(next_msg)
            except socket.error as err:
                log.debug("Write loop: got error, defuncting socket and exiting")
                self.defunct(err)
                return # Leave the write loop

    def handle_read(self):
        rlist = (self._socket,)

        while True:
            try:
                select.select(rlist, (), ())
            except Exception as err:
                return

            try:
                buf = self._socket.recv(self.in_buffer_size)
            except socket.error as err:
                if not is_timeout(err):
                    self.defunct(err)
                    return # leave the read loop

            if buf:
                self._iobuf.write(buf)
                while True:
                    pos = self._iobuf.tell()
                    if pos < 8 or (self._total_reqd_bytes > 0 and pos < self._total_reqd_bytes):
                        # we don't have a complete header yet or we
                        # already saw a header, but we don't have a
                        # complete message yet
                        break
                    else:
                        # have enough for header, read body len from header
                        self._iobuf.seek(4)
                        body_len_bytes = self._iobuf.read(4)
                        body_len = int32_unpack(body_len_bytes)

                        # seek to end to get length of current buffer
                        self._iobuf.seek(0, os.SEEK_END)
                        pos = self._iobuf.tell()

                        if pos - 8 >= body_len:
                            # read message header and body
                            self._iobuf.seek(0)
                            msg = self._iobuf.read(8 + body_len)

                            # leave leftover in current buffer
                            leftover = self._iobuf.read()
                            self._iobuf = StringIO()
                            self._iobuf.write(leftover)

                            self._total_reqd_bytes = 0
                            self.process_msg(msg, body_len)
                        else:
                            self._total_reqd_bytes = body_len + 8
                            break
            else:
                log.debug("connection closed by server")
                self.close()

    def handle_pushed(self, response):
        log.debug("Message pushed from server: %r", response)
        for cb in self._push_watchers.get(response.event_type, []):
            try:
                cb(response.event_args)
            except Exception:
                log.exception("Pushed event handler errored, ignoring:")

    def push(self, data):
        chunk_size = self.out_buffer_size
        for i in xrange(0, len(data), chunk_size):
            self._write_queue.put(data[i:i+chunk_size])

    def send_msg(self, msg, cb, wait_for_id=False):
        if self.is_defunct:
            raise ConnectionShutdown("Connection to %s is defunct" % self.host)
        elif self.is_closed:
            raise ConnectionShutdown("Connection to %s is closed" % self.host)

        if not wait_for_id:
            try:
                request_id = self._id_queue.get_nowait()
            except Queue.Empty:
                raise ConnectionBusy(
                    "Connection to %s is at the max number of requests" % self.host)
        else:
            request_id = self._id_queue.get()

        self._callbacks[request_id] = cb
        self.push(msg.to_string(request_id, compression=self.compressor))
        return request_id

    def wait_for_response(self, msg, timeout=None):
        return self.wait_for_responses(msg, timeout=timeout)[0]

    def wait_for_responses(self, *msgs, **kwargs):
        timeout = kwargs.get('timeout')
        waiter = ResponseWaiter(self, len(msgs))

        # busy wait for sufficient space on the connection
        messages_sent = 0
        while True:
            needed = len(msgs) - messages_sent
            with self.lock:
                available = min(needed, MAX_STREAM_PER_CONNECTION - self.in_flight)
                self.in_flight += available

            for i in range(messages_sent, messages_sent + available):
                self.send_msg(msgs[i], partial(waiter.got_response, index=i), wait_for_id=True)
            messages_sent += available

            if messages_sent == len(msgs):
                break
            else:
                if timeout is not None:
                    timeout -= 0.01
                    if timeout <= 0.0:
                        raise OperationTimedOut()
                time.sleep(0.01)

        try:
            return waiter.deliver(timeout)
        except OperationTimedOut:
            raise
        except Exception, exc:
            self.defunct(exc)
            raise

    def register_watcher(self, event_type, callback):
        self._push_watchers[event_type].add(callback)
        self.wait_for_response(RegisterMessage(event_list=[event_type]))

    def register_watchers(self, type_callback_dict):
        for event_type, callback in type_callback_dict.items():
            self._push_watchers[event_type].add(callback)
        self.wait_for_response(RegisterMessage(event_list=type_callback_dict.keys()))