BatchWriteItem
==============

.. automodule:: magnetodb.api.openstack.v1.data.batch_write_item
   :members:

.. http:post:: v1/data/{project_id}/batch_write_item

--------------
Request Syntax
--------------

.. literalinclude:: ../api/openstack/samples/batch_write_item_request_syntax.json
   :language: javascript

Request Parameters
``````````````````

   **request_items**
      | Type: String to object map
      | Required: Yes

---------------
Response Syntax
---------------
.. literalinclude:: ../api/openstack/samples/batch_write_item_response_syntax.json
    :language: javascript


Response Elements
`````````````````

   **unprocessed_keys**
      | Type: String to object map

------
Errors
------

   | BackendInteractionException
   | ClusterIsNotConnectedException
   | NotImplementedError
   | ValidationError

--------------
Sample Request
--------------

.. literalinclude:: ../api/openstack/samples/batch_write_item_sample_request.json
    :language: javascript

---------------
Sample Response
---------------

.. literalinclude:: ../api/openstack/samples/batch_write_item_sample_response.json
    :language: javascript
