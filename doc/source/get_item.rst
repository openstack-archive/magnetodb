GetItem
=======

.. automodule:: magnetodb.api.openstack.v1.data.get_item
   :members:

.. http:post:: v1/data/{project_id}/tables/{table_name}/get_item

--------------
Request Syntax
--------------

.. literalinclude:: ../api/openstack/samples/get_item_request_syntax.json
   :language: javascript

Request Parameters
``````````````````

   **key**
      | The primary key of the item to retrieve.
      | Type: String to object map
      | Required: Yes

   **attributes_to_get**
      | The names of one or more attributes to retrieve.
      | Type: array of Strings
      | Required: No

   **consistent_read**
      | Use or not use strongly consistent read.
      | Type: Boolean
      | Required: No

---------------
Response Syntax
---------------

.. literalinclude:: ../api/openstack/samples/get_item_response_syntax.json
    :language: javascript


Response Elements
`````````````````

   **item**
      | An itemi with attributes.
      | Type: String to object map

------
Errors
------
   | BackendInteractionException
   | ClusterIsNotConnectedException
   | ValidationError

--------------
Sample Request
--------------

.. literalinclude:: ../api/openstack/samples/get_item_sample_request.json
    :language: javascript

---------------
Sample Response
---------------
.. literalinclude:: ../api/openstack/samples/get_item_sample_response.json
    :language: javascript
