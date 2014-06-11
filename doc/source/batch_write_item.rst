BatchWriteItem
==============

.. automodule:: magnetodb.api.openstack.v1.batch_write_item
   :members:

.. http:post:: v1/{project_id}/data/batch_write_item

**Request Syntax**

.. literalinclude:: ../api/openstack/samples/batch_write_item_request_syntax.json
   :language: javascript

**Request Parameters**:

   **request_items**
      | Type: String to object map
      | Required: Yes

**Response Syntax**

.. literalinclude:: ../api/openstack/samples/batch_write_item_response_syntax.json
    :language: javascript


**Response Elements**

   **unprocessed_keys**
      | Type: String to object map

**Errors**
   TBW

**Sample Request**

.. literalinclude:: ../api/openstack/samples/batch_write_item_sample_request.json
    :language: javascript

**Sample Response**

.. literalinclude:: ../api/openstack/samples/batch_write_item_sample_response.json
    :language: javascript
