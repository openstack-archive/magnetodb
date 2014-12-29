UpdateItem
==========

.. automodule:: magnetodb.api.openstack.v1.data.update_item
   :members:

.. http:post:: v1/data/{project_id}/tables/{table_name}/update_item

--------------
Request Syntax
--------------

.. literalinclude:: ../api/openstack/samples/update_item_request_syntax.json
   :language: javascript

Request Parameters
``````````````````

   **key**
      | The primary key of the item to retrieve.
      | Type: String to object map
      | Required: Yes

   **attribute_updates**
      | The names of attributes to be modified, the action to perform on each, and the new value for each. If you are updating an attribute that is an index key attribute for any indexes on that table, the attribute type must match the index key type defined in the attribute_definition of the table description. You can use UpdateItem to update any non-key attributes.

      | Attribute values cannot be null. String and binary type attributes must have lengths greater than zero. Set type attributes must not be empty. Requests with empty values will be rejected with a ValidationError exception.

      | Each attribute_updates element consists of an attribute name to modify, along with the following:

        - value - the new value, if applicable, for this attribute;

        - action - specifies how to perform the update. Valid values for action are PUT, DELETE, and ADD. The behavior depends on whether the specified primary key already exists in the table.

         | **If an item with the specified key is found in the table:**

          - PUT - Adds the specified attribute to the item. If the attribute already exists, it is replaced by the new value.

          - DELETE - If no value is specified, the attribute and its value are removed from the item. The data type of the specified value must match the existing value's data type. If a set of values is specified, then those values are subtracted from the old set. For example, if the attribute value was the set [a,b,c] and the DELETE action specified [a,c], then the final attribute value would be [b]. Specifying an empty set is an error.

          - ADD - If the attribute does not already exist, then the attribute and its values are added to the item. If the attribute does exist, then the behavior of ADD depends on the data type of the attribute:

             - if the existing attribute is a number, and if value is also a number, then the value is mathematically added to the existing attribute. If value is a negative number, then it is subtracted from the existing attribute;

             - if the existing data type is a set, and if the value is also a set, then the value is added to the existing set. (This is a set operation, not mathematical addition.) For example, if the attribute value was the set [1,2], and the ADD action specified [3], then the final attribute value would be [1,2,3]. An error occurs if an Add action is specified for a set attribute and the attribute type specified does not match the existing set type.

            | Both sets must have the same primitive data type. For example, if the existing data type is a set of strings, the value must also be a set of strings. The same holds true for number sets and binary sets.

         | This action is only valid for an existing attribute whose data type is number or is a set. Do not use ADD for any other data types.

         | **If no item with the specified key is found:**

          - PUT - MagnetoDB creates a new item with the specified primary key, and then adds the attribute.

          - DELETE - Nothing happens; there is no attribute to delete.

          - ADD - MagnetoDB creates an item with the supplied primary key and number (or set of numbers) for the attribute value. The only data types allowed are number and number set; no other data types can be specified.

      | If you specify any attributes that are part of an index key, then the data types for those attributes must match those of the schema in the table's attribute definition.
      | Type: String to object map
      | Required: No

   **time_to_live**
      | Defines time to live for item
      | Type: number
      | Valid values: 0 - MAX_NUMBER
      | Required: No

   **expected**
      | The conditional block for the Updateitem operation. All the conditions must be met for the operation to succeed.
      | Type: String to object map
      | Required: No

   **return_values**
      | Type: String
      | Valid values: NONE | ALL_OLD | UPDATED_OLD | ALL_NEW | UPDATED_NEW
      | Required: No


---------------
Response Syntax
---------------

.. literalinclude:: ../api/openstack/samples/update_item_response_syntax.json
    :language: javascript


Response Elements
`````````````````

   **attributes**
      | Item attributes
      | Type: String to object map


------
Errors
------

   | BackendInteractionException
   | ClusterIsNotConnectedException
   | ConditionalCheckFailedException
   | TableNotExistsException
   | ValidationError

--------------
Sample Request
--------------

.. literalinclude:: ../api/openstack/samples/update_item_sample_request.json
    :language: javascript

---------------
Sample Response
---------------

.. literalinclude:: ../api/openstack/samples/update_item_sample_response.json
    :language: javascript
