ListTables
==========

.. automodule:: magnetodb.api.openstack.v1.data.list_tables
   :members:

.. http:get:: v1/data/{project_id}/tables

--------------
Request Syntax
--------------

   This operation does not require a request body

Request Parameters
``````````````````

   Parameters should be provided via GET query string.

   **exclusive_start_table_name**
      | The first table name that this operation will evaluate. Use the value that was returned for last_evaluated_table_name in the previous operation.
      | Type: xsd:string
      | Required: No

   **limit**
      | A maximum number of the items to return.
      | Type: xsd:int
      | Required: No

---------------
Response Syntax
---------------

.. literalinclude:: ../api/openstack/samples/list_tables_response_syntax.json
    :language: javascript

Response Elements
`````````````````

   **last_evaluated_table_name**
      | The name of the last table in the current page of results.
      | Type: String

   **tables**
     | Array of the table info items
     | Type: array of structs

------
Errors
------

   | BackendInteractionException
   | ClusterIsNotConnectedException

---------------
Sample Response
---------------

.. literalinclude:: ../api/openstack/samples/list_tables_sample_response.json
    :language: javascript