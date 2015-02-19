Query
=====

.. automodule:: magnetodb.api.openstack.v1.data.query
   :members:

.. http:post:: v1/data/{project_id}/tables/{table_name}/query

--------------
Request Syntax
--------------

.. literalinclude:: ../api/openstack/samples/query_request_syntax.json
   :language: javascript

Request Parameters
``````````````````

   **attributes_to_get**
      | Type: array of Strings
      | Required: No

   **consistent_read**
      | Type: Boolean
      | Required: No

   **exclusive_start_key**
      | The primary key of the first item that this operation will evaluate.
      | Type: String to object map
      | Required: No

   **index_name**
      | The name of an index to query.
      | Type: String
      | Required: No

   **key_conditions**
      | The selection criteria for the query.
      | Type: String to Condition object map
      | Required: Yes

   **limit**
      | Type: Number
      | Required: No

   **scan_index_forward**
      | Type: Boolean
      | Required: No

   **select**
      | The attributes to be returned in the result.
      | Type: String
      | Valid values: ALL_ATTRIBUTES | ALL_PROJECTED_ATTRIBUTES | SPECIFIC_ATTRIBUTES | COUNT
      | Required: No

---------------
Response Syntax
---------------

.. literalinclude:: ../api/openstack/samples/query_response_syntax.json
    :language: javascript


Response Elements
`````````````````

   **count**
      | The number of items in the response.
      | Type: Number

   **items**
      | An array of items.
      | Type: array of items

   **last_evaluated_key**
      | The primary key of the item where the operation stopped.
      | Type: String to AttributeValue object map

------
Errors
------

   | BackendInteractionException
   | ClusterIsNotConnectedException
   | ValidationError

--------------
Sample Request
--------------

.. literalinclude:: ../api/openstack/samples/query_sample_request.json
    :language: javascript

---------------
Sample Response
---------------

.. literalinclude:: ../api/openstack/samples/query_sample_response.json
    :language: javascript
