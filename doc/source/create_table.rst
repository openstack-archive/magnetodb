CreateTable
===========

.. automodule:: magnetodb.api.openstack.v1.data.create_table
   :members:

.. http:post:: v1/data/{project_id}/tables

--------------
Request Syntax
--------------

.. literalinclude:: ../api/openstack/samples/create_table_request_syntax.json
   :language: javascript

Request Parameters
``````````````````

   **table_name**
     | The name of the table. Unique per project.
     | Type: string
     | Required: Yes

   **attribute_definitions**
     | An array of attributes that describe the key schema for the table and indexes.
     | Type: array of AttributeDefinition objects
     | Required: Yes

   **key_schema**
     | Specifies the attributes that make up the primary key for a table or an index.
     | Type: array of key_schemaElement objects
     | Required: Yes

   **local_secondary_indexes**
     | One or more local secondary indexes to be created on the table.
     | Type: array of objects
     | Required: No

---------------
Response Syntax
---------------

.. literalinclude:: ../api/openstack/samples/create_table_response_syntax.json
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
   | TableAlreadyExistsException
   | ValidationError

--------------
Sample Request
--------------

.. literalinclude:: ../api/openstack/samples/create_table_sample_request.json
    :language: javascript

---------------
Sample Response
---------------

.. literalinclude:: ../api/openstack/samples/create_table_sample_response.json
    :language: javascript
