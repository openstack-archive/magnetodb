--------------------
RESTful Web API (v1)
--------------------

MagnetoDB API is a RESTful API what uses JSON media type for interaction between client and server

Authentication
==============

**Headers**

Each request is expected to have following headers:

- User-Agent
- Content-Type: application/json
- Accept: application/json
- X-Auth-Token keystone auth token


Common Errors
=============

This section lists the common errors that all actions return. Any action-specific errors will be listed in the topic for the action.

.. automodule:: magnetodb.common.exception

    .. autoexception:: MagnetoError

    .. autoexception:: BackendInteractionException

    .. autoexception:: ValidationError

    .. autoexception:: Forbidden

    .. autoexception:: RequestQuotaExceeded

    .. autoexception:: TableNotExistsException

    .. autoexception:: TableAlreadyExistsException

    .. autoexception:: ResourceInUseException

    .. autoexception:: InvalidQueryParameter

    .. autoexception:: ConditionalCheckFailedException

    .. autoexception:: ConfigNotFound

    .. autoexception:: BackupNotExists


Operation details
=================

- table_name parameter will be provided via URL
- API will use different HTTP methods for different operations (POST for create, PUT for update, etc)

**Note**: operations with items in the table(GetItem, PutItem, Scan, etc) will use POST method.


MagnetoDB actions
=================

.. toctree::
   :maxdepth: 1

   create_table.rst
   update_table.rst
   describe_table.rst
   delete_table.rst
   list_tables.rst
   put_item.rst
   get_item.rst
   update_item.rst
   delete_item.rst
   query.rst
   scan.rst
   batch_get_item.rst
   batch_write_item.rst
