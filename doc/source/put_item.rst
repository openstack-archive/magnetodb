PutItem
=======

.. automodule:: magnetodb.api.openstack.v1.put_item
   :members:

.. http:post:: v1/{project_id}/data/tables/{table_name}/put_item

**Request Syntax**

.. literalinclude:: ../api/openstack/samples/put_item_request_syntax.json
   :language: javascript

**Request Parameters**:

   **item**
      | A map of attribute name/value pairs, one for each attribute. Only the primary key attributes are required.
      | Type: String to Attributevalue object map
      | Required: Yes

   **expected**
      | The conditional block for the PutItem operation.
      | Type: String to expectedAttributevalue object map
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

**Response Syntax**

.. literalinclude:: ../api/openstack/samples/put_item_response_syntax.json
    :language: javascript


**Response Elements**

   **attributes**
      | The attribute values as they appeared before the PutiItem operation.
      | Type: String to attribute struct

**Errors**
   TBW

**Sample Request**

.. literalinclude:: ../api/openstack/samples/put_item_sample_request.json
    :language: javascript

**Sample Response**

.. literalinclude:: ../api/openstack/samples/put_item_sample_response.json
    :language: javascript
