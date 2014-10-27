TableUsageDetails
=================

.. automodule:: magnetodb.api.openstack.v1.request.table_usage_details.py
   :members:

.. http:get:: v1/{project_id}/monitoring/tables/{table_name}

**Request Syntax**

   This operation does not require a request body

**Request Parameters**:

   This operation does not require a request parameters


**Response Syntax**

.. literalinclude:: ../api/openstack/samples/table_usage_details_response_syntax.json
    :language: javascript

**Response Elements**

   **size**
      | Table size in bytes.
      | Type: Number

   **item_count**
     | Number of items in table.
     | Type: Number

**Errors**
   TBW

**Sample Response**

.. literalinclude:: ../api/openstack/samples/table_usage_details_sample_response.json
    :language: javascript