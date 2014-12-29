PutItem
=======

.. automodule:: magnetodb.api.openstack.v1.data.put_item
   :members:

.. http:post:: v1/data/{project_id}/tables/{table_name}/put_item

-----------
Description
-----------

Adds or rewrites existing item in table. If item already exists and it is just replaced if no other option is
specified. It also supports conditions what are applied before inserting/updating item. If condition passes the item
is updated or inserted.

Conditions support
``````````````````

You can define list of conditions what should be checked before applying insert or update. You can evaluate if specific
item attribute value is equal to given in request or check its existing. Please look at sample below where ForumName is
a hash key and Subject is a range key.


.. literalinclude:: ../api/openstack/samples/condition_exist_equal_sample.json
   :language: javascript

Also you can check if item with given primary key exists in table at all. To do it, you have to build condition if
hash key exists. Technically the items is queried by hash and range key (if range key is defined) and after that
condition is  applied, so adding range key to condition is useless and ValidationError will be rose.

--------------
Request Syntax
--------------
.. literalinclude:: ../api/openstack/samples/put_item_request_syntax.json
   :language: javascript

Request Parameters
``````````````````

   **item**
      | A map of attribute name/value pairs, one for each attribute. Only the primary key attributes are required.
      | Type: String to Attributevalue object map
      | Required: Yes

   **expected**
      | The conditional block for the PutItem operation.
      | Type: String to expected Attributevalue object map
      | Required: No

   **time_to_live**
      | Defines time to live for item
      | Type: number
      | Valid values: 0 - MAX_NUMBER
      | Required: No

   **return_values**
      | Use return_values if you want to get the item attributes as they appeared before they were updated.
      | Type: String
      | Valid values: NONE | ALL_OLD
      | Required: No

-----------------
Response Syntax
-----------------
.. literalinclude:: ../api/openstack/samples/put_item_response_syntax.json
    :language: javascript


Response Elements
`````````````````

   **attributes**
      | The attribute values as they appeared before the PutiItem operation.
      | Type: String to attribute struct

------
Errors
------
   | BackendInteractionException
   | ClusterIsNotConnectedException
   | ConditionalCheckFailedException
   | ValidationError

-------
Samples
-------
**Sample Request**

.. literalinclude:: ../api/openstack/samples/put_item_sample_request.json
    :language: javascript

**Sample Response**

.. literalinclude:: ../api/openstack/samples/put_item_sample_response.json
    :language: javascript
