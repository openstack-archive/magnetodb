DescribeTable
=============

.. automodule:: magnetodb.api.openstack.v1.data.describe_table
   :members:

.. http:get:: v1/data/{project_id}/tables/{table_name}

--------------
Request Syntax
--------------

   This operation does not require a request body

---------------
Response Syntax
---------------

.. literalinclude:: ../api/openstack/samples/describe_table_response_syntax.json
    :language: javascript

Response Elements
`````````````````

   **table**
      | Represents the properties of a table.
      | Type: table_description object


Table Statuses
``````````````

- ACTIVE
- CREATING
- CREATE_FAILURE
- DELETING
- DELETE_FAILURE

------
Errors
------

   | BackendInteractionException
   | ClusterIsNotConnectedException
   | TableNotExistsException
   | ValidationError

---------------
Sample Response
---------------

.. literalinclude:: ../api/openstack/samples/describe_table_sample_response.json
       :language: javascript
