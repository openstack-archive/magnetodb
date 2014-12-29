DeleteTable
===========

.. automodule:: magnetodb.api.openstack.v1.data.delete_table
   :members:

.. http:delete:: v1/data/{project_id}/tables/{table_name}

--------------
Request Syntax
--------------

   This operation does not require a request body

---------------
Response Syntax
---------------

.. literalinclude:: ../api/openstack/samples/delete_table_response_syntax.json
    :language: javascript

Response Elements
`````````````````

   **table_description**
      | Represents the properties of a table.
      | Type: table_description object

------
Errors
------

   | BackendInteractionException
   | ClusterIsNotConnectedException
   | ResourceInUseException
   | TableNotExistsException
   | ValidationError

---------------
Sample Response
---------------

.. literalinclude:: ../api/openstack/samples/delete_table_sample_response.json
    :language: javascript
