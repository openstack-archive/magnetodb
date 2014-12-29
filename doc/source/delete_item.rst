DeleteItem
==========

.. automodule:: magnetodb.api.openstack.v1.data.delete_item
   :members:

.. http:post:: v1/data/{project_id}/tables/{table_name}/delete_item

--------------
Request Syntax
--------------

.. literalinclude:: ../api/openstack/samples/delete_item_request_syntax.json
   :language: javascript

Request Parameters
``````````````````

   **key**
      | Primary key of the item to delete.
      | Type: String to object map
      | Required: Yes

   **expected**
      | The conditional block for the DeleteItem operation. All the conditions must be met for the operation to succeed.
      | Type: String to object map
      | Required: No

   **return_values**
      | Type: String
      | Valid values: NONE | ALL_OLD | UPDATED_OLD | ALL_NEW | UPDATED_NEW
      | Required: No

---------------
Response Syntax
---------------

.. literalinclude:: ../api/openstack/samples/delete_item_response_syntax.json
    :language: javascript


Response Elements
`````````````````

   **attributes**
      | Item attributes
      | Type: String to Attributevalue object map

------
Errors
------

   | BackendInteractionException
   | ClusterIsNotConnectedException
   | ConditionalCheckFailedException
   | ValidationError

--------------
Sample Request
--------------

.. literalinclude:: ../api/openstack/samples/delete_item_sample_request.json
    :language: javascript

---------------
Sample Response
---------------

.. literalinclude:: ../api/openstack/samples/delete_item_sample_response.json
    :language: javascript
