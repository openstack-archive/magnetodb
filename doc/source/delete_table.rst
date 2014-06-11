DeleteTable
===========

.. automodule:: magnetodb.api.openstack.v1.delete_table
   :members:

.. http:delete:: v1/{project_id}/data/tables/{table_name}

**Request Syntax**

   This operation does not require a request body

**Response Syntax**

.. literalinclude:: ../api/openstack/samples/delete_table_response_syntax.json
    :language: javascript

**Response Elements**

   **table_description**
      | Represents the properties of a table.
      | Type: table_description object

**Errors**
   TBW

**Sample Response**

.. literalinclude:: ../api/openstack/samples/delete_table_sample_response.json
    :language: javascript
