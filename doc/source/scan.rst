Scan
=====

.. automodule:: magnetodb.api.openstack.v1.data.scan
   :members:

.. http:post:: v1/data/{project_id}/tables/{table_name}/scan

--------------
Request Syntax
--------------

.. literalinclude:: ../api/openstack/samples/scan_request_syntax.json
   :language: javascript

Request Parameters
``````````````````

   **attributes_to_get**
      | Type: array of Strings
      | Required: No

   **exclusive_start_key**
      | The primary key of the first item that this operation will evaluate.
      | Type: String to object map
      | Required: No

   **limit**
      | Type: Number
      | Required: No

   **scan_filter**
      | Scan conditions.
      | Type: String to Condition object map
      | Required: No

   **segment**
      | Segment for parallel scan.
      | Type: Number
      | Required: No

   **select**
      | The attributes to be returned in the result.
      | Type: String
      | Valid values: ALL_ATTRIBUTES | ALL_PROJECTED_ATTRIBUTES | SPECIFIC_ATTRIBUTES | COUNT
      | Required: No

   **total_segments**
      | Number of segments for parallel scan.
      | Type: Number
      | Required: No

---------------
Response Syntax
---------------

.. literalinclude:: ../api/openstack/samples/scan_response_syntax.json
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

   **scanned_count**
      | Type: Number

------
Errors
------

   | BackendInteractionException
   | ClusterIsNotConnectedException
   | ValidationError

--------------
Sample Request
--------------

.. literalinclude:: ../api/openstack/samples/scan_sample_request.json
    :language: javascript

---------------
Sample Response
---------------

.. literalinclude:: ../api/openstack/samples/scan_sample_response.json
    :language: javascript
