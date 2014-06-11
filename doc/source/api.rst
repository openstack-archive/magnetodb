====================
RESTful Web API (v1)
====================

Headers
=======

| User-Agent
| Content-Type: application/json
| Accept: application/json
| X-Auth-Token keystone auth token


Common Errors
=============

This section lists the common errors that all actions return. Any action-specific errors will be listed in the topic for the action.

.. automodule:: magnetodb.common.exception

    .. autoexception:: InternalFailure

    .. autoexception:: RequestQuotaExceeded

    .. autoexception:: OverLimit

    .. autoexception:: InvalidClientToken

    .. autoexception:: Forbidden

    .. autoexception:: InvalidParameterCombination

    .. autoexception:: InvalidParameterValue

    .. autoexception:: InvalidQueryParameter

    .. autoexception:: MalformedQueryString

    .. autoexception:: MissingParameter

    .. autoexception:: ServiceUnavailable

    .. autoexception:: ValidationError











Operation details
=================
Requests and responses should be very similar to the Amazon DynamoDB. But there should be some differences:

- table_name parameter will be provided via URL

- GlobalSecondaryIndexes will be added in future

- API will use different HTTP methods for different operations (POST for create, PUT for update, etc)

**Note**: operations with items in the table(Getitem, Putitem, Scan, etc) will use POST method.

Actions
=======

.. toctree::
   :maxdepth: 1

   create_table.rst
   update_table.rst
   describe_table.rst
