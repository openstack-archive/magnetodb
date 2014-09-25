=============
MagnetoDB CLI
=============

MagnetoDB CLI client is dedicated to simplify communication with magnetodb server. It uses keystone v2/v3 for authentication. To provide auth info you need to export environment variables OS_TENANT_NAME, OS_USERNAME, OS_PASSWORD and OS_AUTH_URL or provide them on command call, e.g.::

$ magnetodb --os-auth-url http://127.0.0.1:5000/v3 --os-password pass --os-tenant-name admin --os-username admin <command> <args>

MagnetoDB CLI client provides following commands:

table-create
------------

Creates a new table in MagnetoDB.

Command syntax::

$ magnetodb table-create --request-file <FILE>

<FILE> - path to file that contains json request:

.. literalinclude:: ../api/openstack/samples/create_table_request_syntax.json
   :language: javascript

Sample::

$ magnetodb table-create --request-file ~/table-create-request.json

~/table-create-request.json contains:

.. literalinclude:: ../api/openstack/samples/create_table_sample_request.json
    :language: javascript

table-list
----------

Prints a list of tables in tenant.

Command syntax::

$ magnetodb table-list [--limit <max-tables-to-show>] [--start-table-name <table-name>]

--limit - max tables to show in a list
--start-table-name - table name, after which tables will be listed

Sample::

$ magnetodb table-list --limit 3 --start-table-name Thread


table-delete
------------

Deletes a table.

Command syntax::

$ magnetodb table-delete <table-name>

Sample::

$ magnetodb table-delete Thread


table-describe
--------------

Prints a description of a table.

Command syntax::

$ magnetodb table-describe <table-name>

Sample::

$ magnetodb table-describe Thread


index-list
----------

Prints list of indexes of a given table.

Command syntax::

$ magnetodb index-list <table-name>

Sample::

$ magnetodb index-list Thread


index-show
----------

Describes index of a table.

Command syntax::

$ magnetodb index-show <table-name> <index-name>

Sample::

$ magnetodb index-show Thread LastPostIndex


item-put
--------

Puts item to a given table.

Command syntax::

$ magnetodb item-put <table-name> --request-file <FILE>

<FILE> - path to file that contains json request:

.. literalinclude:: ../api/openstack/samples/put_item_request_syntax.json
   :language: javascript

Sample::

$ magnetodb item-put Thread --request-file ~/item-put-request.json

~/item-put-request.json contains:

.. literalinclude:: ../api/openstack/samples/put_item_sample_request.json
   :language: javascript


item-update
-----------

Updates item.

Command syntax::

$ magnetodb item-update <table-name> --request-file <FILE>

<FILE> - path to file that contains json request:

.. literalinclude:: ../api/openstack/samples/update_item_request_syntax.json
   :language: javascript

Sample::

$ magnetodb item-update Thread --request-file ~/item-update-request.json

~/item-put-request.json contains:

.. literalinclude:: ../api/openstack/samples/update_item_sample_request.json
   :language: javascript


item-delete
-----------

Deletes item from a given table.

Command syntax::

$ magnetodb item-delete <table-name> --request-file <FILE>

<FILE> - path to file that contains json request:

.. literalinclude:: ../api/openstack/samples/delete_item_request_syntax.json
   :language: javascript

Sample::

$ magnetodb item-delete Thread --request-file ~/item-delete-request.json

~/item-delete-request.json contains:

.. literalinclude:: ../api/openstack/samples/update_item_sample_request.json
   :language: javascript


item-get
--------

Gets item from a given table.

Command syntax::

$ magnetodb item-get <table-name> --request-file <FILE>

<FILE> - path to file that contains json request:

.. literalinclude:: ../api/openstack/samples/get_item_request_syntax.json
   :language: javascript

Sample::

$ magnetodb item-get Thread --request-file ~/item-get-request.json

~/item-get-request.json contains:

.. literalinclude:: ../api/openstack/samples/get_item_sample_request.json
   :language: javascript


query
-----

Makes query request to a given table.

Command syntax::

$ magnetodb query <table-name> --request-file <FILE>

<FILE> - path to file that contains json request:

.. literalinclude:: ../api/openstack/samples/query_request_syntax.json
   :language: javascript

Sample::

$ magnetodb query Thread --request-file ~/query-request.json

~/query-request.json contains:

.. literalinclude:: ../api/openstack/samples/query_sample_request.json
   :language: javascript


scan
----

Makes scan request to a given table.

Command syntax::

$ magnetodb scan <table-name> --request-file <FILE>

<FILE> - path to file that contains json request:

.. literalinclude:: ../api/openstack/samples/scan_request_syntax.json
   :language: javascript

Sample::

$ magnetodb scan Thread --request-file ~/scan-request.json

~/scan-request.json contains:

.. literalinclude:: ../api/openstack/samples/scan_sample_request.json
   :language: javascript


batch-write
-----------

Makes batch write item request.

Command syntax::

$ magnetodb batch-write --request-file <FILE>

<FILE> - path to file that contains json request:

.. literalinclude:: ../api/openstack/samples/batch_write_item_request_syntax.json
   :language: javascript

Sample::

$ magnetodb batch-write --request-file ~/batch-write-request.json

~/batch-write-request.json contains:

.. literalinclude:: ../api/openstack/samples/batch_write_item_sample_request.json
   :language: javascript


batch-get
---------

Makes batch get item request.

Command syntax::

$ magnetodb batch-get --request-file <FILE>

<FILE> - path to file that contains json request:

.. literalinclude:: ../api/openstack/samples/batch_get_item_request_syntax.json
   :language: javascript

Sample::

$ magnetodb batch-get --request-file ~/batch-get-request.json

~/batch-get-request.json contains:

.. literalinclude:: ../api/openstack/samples/batch_get_item_sample_request.json
   :language: javascript
