===================
Configuration guide
===================

---------------------
Configuring MagnetoDB
---------------------

Once MagnetoDB is installed, it is configured via configuration files
(etc/magnetodb/magnetodb-api-server.conf, etc/magnetodb/magnetodb-api.conf),
a PasteDeploy configuration file (etc/magnetodb/api-paste.ini).

By default, MagnetoDB starts a service on port 8480 (you can change bind port
and host in `magnetodb-api-server.conf`).

Also you can run MagnetoDB with gunicorn (multithread). Use configs *-gunicorn*

Starting and Stopping MagnetoDB
===============================

Start MagnetoDB services using the command::

$ magnetodb-api-server --config-file /etc/magnetodb/magnetodb-api-server.conf

For start MagnetoDB with gunicorn WSGI server use command::

$ magnetodb-api-server-gunicorn --config-file /etc/magnetodb/magnetodb-api-server-gunicorn.conf

Also you can specify number of workers in magnetodb-api-server-gunicorn.conf
(example magnetodb_api_workers = 4).

Invoking these commands starts up wsgi.Server instance (the primary/public
API interface). Stop the process using Control-C.


Configuration Files
===================

The MagnetoDB configuration files are an `ini` file format based on Paste, a
common system used to configure Python WSGI based applications. The
PasteDeploy configuration entries (WSGI pipeline definitions) can be provided
in a separate `api-paste.ini` file, while general and driver-specific
configuration parameters are in the primary configuration file
`magnetodb-api.conf`. The api-paste.ini configuration file is organized into
the following sections:

[pipeline:main] is used when you need apply a number of filters. It takes one configuration key pipeline (plus any global configuration overrides you want). pipeline is a list of filters ended by an application.

Two main filters are ec2authtoken and tokenauth:

[filter:ec2authtoken] - check EC2 credentials in request headers (for DynamoDB API)
 - auth_uri: complete public Identity API endpoint (string value) for checking EC2 credentials (http://127.0.0.1:5000/v3)

[filter:tokenauth] - checks the validity of the token in Keystone from the service user ( usually “magnetodb”). For this action role should be “admin”:
 - auth_host: host providing the admin Identity API endpoint (string value)
 - auth_port: port of the admin Identity API endpoint (integer value)
 - auth_protocol: protocol of the admin Identity API endpoint(http or https)
 - admin_tenant_name: Keystone service account tenant name to validate user tokens (string value)
 - admin_user: Keystone account username (string value)
 - admin_password: Keystone account password (string value)
 - auth_version: API version of the admin Identity API endpoint (string value)
 - admin_token: single shared secret with the Keystone configuration used for bootstrapping a Keystone installation, or otherwise bypassing the normal authentication process (string value)
 - signing_dir: directory used to cache files related to PKI tokens (string value)

Note: signing_dir is configurable, but the default behavior of the authtoken
middleware should be sufficient.  It will create a temporary directory in the
home directory for the user the MagnetoDB process is running as.

.conf file
----------

The magnetodb-api.conf configuration file is organized into the following sections:

| DEFAULT (logging configuration)
| RPC Configuration Options
| Notification System Options
| Storage Manager Config


[DEFAULT]
`````````

 - verbose: show more verbose log output (sets INFO log level output) <boolean value>
 - debug: show debugging output in logs (sets DEBUG log level output) <boolean value>
 - log_file: path to log file <string value>
 - log_config: path to logging config file, if it is specified, options 'verbose', 'debug', 'log_file', 'use_syslog', 'use_stderr', 'publish_errors', 'log_format', 'default_log_levels', 'log_date_format' will be ignored
 - use_syslog: use Syslog for logging <boolean value>
 - syslog_log_facility: Syslog facility to receive log lines <string value>
 - logging_exception_prefix: format exception prefix <string value>


[PROBE]
`````````

 - enabled: enables additional diagnostic log output
 - suppress_args: suppresses args output


[RPC Configuration Options]
```````````````````````````

 - rpc_backend: the messaging module to use (one of rabbit, qpid, zmq; rabbit by default)
 - rpc_thread_pool_size: size of rpc thread pool
 - rpc_conn_pool_size: size of RPC connection pool
 - rpc_response_timeout: seconds to wait for a response from call or multicall
 - rpc_cast_timeout: seconds to wait before a cast expires (only supported by impl_zmq)
 - allowed_rpc_exception_modules: modules of exceptions that are permitted to be recreated upon receiving exception data from an rpc call (neutron.openstack.common.exception, nova.exception)
 - control_exchange: AMQP exchange to connect to if using RabbitMQ or QPID
 - fake_rabbit: if passed, use a fake RabbitMQ provider

Configuration options if sending notifications via rabbit rpc (these are the defaults):

 - kombu_ssl_version: SSL version to use (valid only if SSL enabled)
 - kombu_ssl_keyfile: SSL key file (valid only if SSL enabled)
 - kombu_ssl_certfile: SSL cert file (valid only if SSL enabled)
 - kombu_ssl_ca_certs: SSL certification authority file (valid only if SSL enabled)
 - rabbit_host: IP address of the RabbitMQ installation
 - rabbit_password: password of the RabbitMQ server
 - rabbit_port: port where RabbitMQ server is running/listening
 - rabbit_hosts: RabbitMQ single or HA cluster (host:port pairs i.e: host1:5672, host2:5672) rabbit_hosts is defaulted to '$rabbit_host:$rabbit_port'
 - rabbit_userid: user ID used for RabbitMQ connections
 - rabbit_virtual_host: location of a virtual RabbitMQ installation.
 - rabbit_max_retries: maximum retries with trying to connect to RabbitMQ (the default of 0 implies an infinite retry count)
 - rabbit_retry_interval:  RabbitMQ connection retry interval
 - rabbit_ha_queues: use HA queues in RabbitMQ (x-ha-policy: all). You need to wipe RabbitMQ database when changing this option (boolean value)

QPID (rpc_backend=qpid):

 - qpid_hostname: Qpid broker hostname
 - qpid_port: Qpid broker port
 - qpid_hosts: Qpid single or HA cluster (host:port pairs i.e: host1:5672, host2:5672) qpid_hosts is defaulted to '$qpid_hostname:$qpid_port'
 - qpid_username: username for qpid connection
 - qpid_password: password for qpid connection
 - qpid_sasl_mechanisms: space separated list of SASL mechanisms to use for auth
 - qpid_heartbeat: seconds between connection keepalive heartbeats
 - qpid_protocol: transport to use, either 'tcp' or 'ssl'
 - qpid_tcp_nodelay: disable Nagle algorithm


ZMQ (rpc_backend=zmq):

 - rpc_zmq_bind_address: ZeroMQ bind address. Should be a wildcard (*), an ethernet interface, or IP. The "host" option should point or resolve to this address.


[Notification System Options]
`````````````````````````````

Notifications can be sent when tables are created, or deleted, or data items are inserted/deleted/updated/retrieved. There are three methods of sending notifications: logging (via the log_file directive), rpc (via a message queue) and noop (no notifications sent, the default):

<magnetodb property>
 - notification_service: together with default_publisher_id, this becomes the publisher_id (for example: magnetodb.myhost.com)

<notification engine property>
 - notification_driver = no_op (do nothing driver)
 - notification_driver = log (logging driver)
 - notification_driver = messaging (RPC driver)
 - default_publisher_id: default_publisher_id is a part of the notification payload
 - notification_topics: defined in messaging driver , can be comma separated values. AMQP topic used for OpenStack notifications.

Note: notification_driver can be defined multiple times.


[Storage Manager Config]
````````````````````````

Storage manager config it is a simple string from the point of view of
oslo.config. But this string should be a well-formed JSON which is a map of
object specifications for object instantiation. Each element of this map is
object specification and it is JSON of tne next format::

    {
        "type": "<factory method or class object name>",
        "args": [<position arguments for object initialization >],
        "kwargs": {<keyword arguments map for object initialization>}
    }


Each of these objects will be created and added to result context (map of
object name to object).
You can specify name of object in context as argument value to initialize
another object in context using "@" prefix. For example if you define context like::

    {
        "cluster_params": {
            "type": "cassandra.cluster.Cluster",
            "kwargs": {
                "contact_points": ["localhost"],
                "control_connection_timeout": 60,
                "max_schema_agreement_wait": 300
            }
        },
        "cluster_handler": {
            "type": "magnetodb.common.cassandra.cluster_handler.ClusterHandler",
            "kwargs": {
                "cluster_params": "@cluster_params",
                "query_timeout": 60,
                "concurrent_queries": 100
            }
        }
    }


Object with name “cluster_params” will be created at the beginning and then
this object will be used for initialization of object with name
"cluster_handler".

Also you can escape you "@" using "@@" if you need to specify string which
starts with @, not a link to another object from context.

cassandra_connection:

 - type: <factory method or class object name>
 - args: <position arguments for object initialization >
 - kwargs: <keyword arguments map for object initialization>
    - in_buffer_size
    - out_buffer_size
    - cql_version: if a specific version of CQL should be used, this may be set to that string version. Otherwise, the highest CQL version supported by the server will be automatically used.
    - protocol_version: the version of the native protocol to use (with Cassandra 2.0+ you should use protocol version 2).
    - keyspace
    - compression: controls compression for communications between the driver and Cassandra. If left as the default of True, either lz4 or snappy compression may be used, depending on what is supported by both the driver and Cassandra. If both are fully supported, lz4 will be preferred. You may also set this to ‘snappy’ or ‘lz4’ to request that specific compression type. Setting this to False disables compression.
    - compressor
    - decompressor
    - ssl_options: a optional dict which will be used as kwargs for ssl.wrap_socket() when new sockets are created. This should be used when client encryption is enabled in Cassandra. By default, a ca_certs value should be supplied (the value should be a string pointing to the location of the CA certs file), and you probably want to specify ssl_version as ssl.PROTOCOL_TLSv1 to match Cassandra’s default protocol.
    - last_error
    - in_flight
    - is_defunct
    - is_closed
    - lock
    - is_control_connection

cluster_params:

 - type: <factory method or class object name>
 - args: <position arguments for object initialization >
 - kwargs: <keyword arguments map for object initialization>
    - connection_class - Cassandra connection class.
    - contact_points
    - port: the server-side port to open connections to (defaults to 9042).
    - compression: controls compression for communications between the driver and Cassandra. If left as the default of True, either lz4 or snappy compression may be used, depending on what is supported by both the driver and Cassandra. If both are fully supported, lz4 will be preferred. You may also set this to ‘snappy’ or ‘lz4’ to request that specific compression type. Setting this to False disables compression.
    - auth_provider: when `protocol_version`_ is 2 or higher, this should be an instance of a subclass of `AuthProvider`_, such as `PlainTextAuthProvider`_. When not using authentication, this should be left as None.
    - load_balancing_policy: an instance of `policies.LoadBalancingPolicy`_ or one of its subclasses. Defaults to `RoundRobinPolicy`_.
    - reconnection_policy: an instance of `policies.ReconnectionPolicy`_. Defaults to an instance of `ExponentialReconnectionPolicy`_ with a base delay of one second and a max delay of ten minutes.
    - default_retry_policy: a default `policies.RetryPolicy`_ instance to use for all `Statement`_ objects which do not have a `retry_policy`_ explicitly set.
    - conviction_policy_factory: a factory function which creates instances of `policies.ConvictionPolicy`_. Defaults to `policies.SimpleConvictionPolicy`_ ;
    - metrics_enabled: whether or not metric collection is enabled. If enabled, `cluster_metrics`_ will be an instance of `Metrics`_.
    - connection_class: this determines what event loop system will be used for managing I/O with Cassandra. These are the current options:
        - `cassandra.io.asyncorereactor.AsyncoreConnection`_
        - `cassandra.io.libevreactor.LibevConnection`_
        - cassandra.io.libevreactor.GeventConnection (requires monkey-patching)
        - cassandra.io.libevreactor.TwistedConnection

          By default, AsyncoreConnection will be used, which uses the asyncore
          module in the Python standard library. The performance is slightly
          worse than with libev, but it is supported on a wider range of systems.
          If libev is installed, LibevConnection will be used instead.
          If gevent monkey-patching of the standard library is detected,
          GeventConnection will be used automatically.

    - ssl_options: a optional dict which will be used as kwargs for ssl.wrap_socket() when new sockets are created. This should be used when client encryption is enabled in Cassandra. By default, a ca_certs value should be supplied (the value should be a string pointing to the location of the CA certs file), and you probably want to specify ssl_version as ssl.PROTOCOL_TLSv1 to match Cassandra’s default protocol.
    - sockopts: an optional list of tuples which will be used as arguments to socket.setsockopt() for all created sockets.
    - cql_version: if a specific version of CQL should be used, this may be set to that string version. Otherwise, the highest CQL version supported by the server will be automatically used.
    - protocol_version: the version of the native protocol to use (with Cassandra 2.0+ you should use protocol version 2).
    - executor_threads
    - max_schema_agreement_wait: the maximum duration (in seconds) that the driver will wait for schema agreement across the cluster. Defaults to ten seconds.
    - control_connection_timeout: a timeout, in seconds, for queries made by the control connection, such as querying the current schema and information about nodes in the cluster. If set to None, there will be no timeout for these queries.

cluster_handler:

 - type: <factory method or class object name>
 - args: <position arguments for object initialization >
 - kwargs: <keyword arguments map for object initialization>
    - cluster - Cluster object
    - query_timeout - Seconds count to wait for CQL query completion
    - concurrent_queries - max number of started but not completed CLQ queries

table_info_repo:

 - type: <factory method or class object name>
 - args: <position arguments for object initialization >
 - kwargs: <keyword arguments map for object initialization>
    - cluster_handler - ClusterHandler object

storage_driver:

 - type: <factory method or class object name>
 - args: <position arguments for object initialization >
 - kwargs: <keyword arguments map for object initialization>
    - cluster_handler - ClusterHandler object
    - default_keyspace_opts - map of Cassandra keyspace properties, which will be used for tenant's keyspace creation if it doesn't exist

storage_manager:

 - type: <factory method or class object name>
 - args: <position arguments for object initialization >
 - kwargs: <keyword arguments map for object initialization>
    - storage_driver - StorageDriver object
    - table_info_repo - TableInfoRepo object
    - concurrent_tasks - max number of started but not completed storage_driver methods invocations
    - batch_chunk_size - size of internal chunks to which original batch will be split. It is needed because large batches may impact Cassandra latency for another concurrent queries
    - schema_operation_timeout - timeout in seconds, after which CREATING or DELETING table state will be changed to CREATE_FAILURE or DELETE_FAILURE respectively



.. _protocol_version:
   http://datastax.github.
   io/python-driver/api/cassandra/cluster.html#cassandra.
   cluster.Cluster.protocol_version

.. _AuthProvider:
   http://datastax.github.
   io/python-driver/api/cassandra/auth.html#cassandra.
   auth.AuthProvider

.. _PlainTextAuthProvider:
   http://datastax.github.
   io/python-driver/api/cassandra/auth.html#cassandra.
   auth.PlainTextAuthProvider

.. _policies.LoadBalancingPolicy:
   http://datastax.github.
   io/python-driver/api/cassandra/policies.html#cassandra.
   policies.LoadBalancingPolicy

.. _RoundRobinPolicy:
   http://datastax.github.
   io/python-driver/api/cassandra/policies.html#cassandra.
   policies.RoundRobinPolicy

.. _policies.ReconnectionPolicy:
   http://datastax.github.
   io/python-driver/api/cassandra/policies.html#cassandra.
   policies.ReconnectionPolicy

.. _ExponentialReconnectionPolicy:
   http://datastax.github.
   io/python-driver/api/cassandra/policies.html#cassandra.
   policies.ExponentialReconnectionPolicy

.. _policies.RetryPolicy:
   http://datastax.github.
   io/python-driver/api/cassandra/policies.html#cassandra.
   policies.RetryPolicy

.. _Statement:
   http://datastax.github.
   io/python-driver/api/cassandra/query.html#cassandra.
   query.Statement

.. _retry_policy:
   http://datastax.github.
   io/python-driver/api/cassandra/query.html#cassandra.
   query.Statement.retry_policy

.. _policies.ConvictionPolicy:
   http://datastax.github.
   io/python-driver/api/cassandra/policies.html#cassandra.
   policies.ConvictionPolicy

.. _policies.SimpleConvictionPolicy:
   http://datastax.github.
   io/python-driver/api/cassandra/policies.html#cassandra.
   policies.SimpleConvictionPolicy

.. _cluster_metrics:
   http://datastax.github.
   io/python-driver/api/cassandra/cluster.html#cassandra.
   cluster.Cluster.metrics

.. _Metrics:
   http://datastax.github.
   io/python-driver/api/cassandra/metrics.html#cassandra.
   metrics.Metrics

.. _cassandra.io.asyncorereactor.AsyncoreConnection:
   http://datastax.github.
   io/python-driver/api/cassandra/io/asyncorereactor.html#cassandra.
   io.asyncorereactor.AsyncoreConnection

.. _cassandra.io.libevreactor.LibevConnection:
   http://datastax.github.
   io/python-driver/api/cassandra/io/libevreactor.html#cassandra.io.libevreactor.LibevConnection


Default storage manager config
``````````````````````````````

::

    storage_manager_config =
        {
            "cassandra_connection": {
                "type": "eval",
                "args": [
                    "importutils.import_class('magnetodb.common.cassandra.io.eventletreactor.EventletConnection')"
                ]
            },
            "cluster_params": {
                "type": "dict",
                "kwargs": {
                    "connection_class": "@cassandra_connection",
                    "contact_points": ["localhost"],
                    "control_connection_timeout": 60,
                    "max_schema_agreement_wait": 300
                }
            },
            "cluster_handler": {
                "type": "magnetodb.common.cassandra.cluster_handler.ClusterHandler",
                "kwargs": {
                    "cluster_params": "@cluster_params",
                    "query_timeout": 60,
                    "concurrent_queries": 100
                }
            },
            "table_info_repo": {
                "type": "magnetodb.storage.table_info_repo.cassandra_impl.CassandraTableInfoRepository",
                "kwargs": {
                    "cluster_handler": "@cluster_handler"
                }
            },
            "storage_driver": {
                "type": "magnetodb.storage.driver.cassandra.cassandra_impl.CassandraStorageDriver",
                "kwargs": {
                    "cluster_handler": "@cluster_handler",
                    "table_info_repo": "@table_info_repo",
                    "default_keyspace_opts": {
                        "replication": {
                            "replication_factor": 3,
                            "class": "SimpleStrategy"
                        }
                    }
                }
            },
            "storage_manager": {
                "type": "magnetodb.storage.manager.queued_impl.QueuedStorageManager",
                "kwargs": {
                    "storage_driver": "@storage_driver",
                    "table_info_repo": "@table_info_repo",
                    "concurrent_tasks": 1000,
                    "batch_chunk_size": 25,
                    "schema_operation_timeout": 300
                }
            }
        }


-----------------------------------------
Configuring MagnetoDB Async Task Executor
-----------------------------------------

Along with MagnetoDB package comes MagnetoDB Async Task Executor which is required for the service to operate properly
if MagnetoDB configured to use QueuedStorageManager (default). Usually only one instance of MagnetoDB Async Task Executor
is required per MagnetoDB cluster.

It is started using the following command::

$ magnetodb-async-task-executor --config-file /etc/magnetodb/magnetodb-async-task-executor.conf

It is configured via configuration file `etc/magnetodb/magnetodb-async-task-executor.conf`.
It's structure mostly coincides with the structure of `magnetodb-api.conf.`
