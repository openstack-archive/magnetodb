AllProjectsUsageDetails
=================

.. automodule:: magnetodb.api.openstack.v1.monitoring.all_projects_usage_details
   :members:

.. http:get:: v1/monitoring/projects?metrics=metric1,metric2

**Request Syntax**

   This operation does not require a request body

**Request Parameters**:

    Parameters should be provided via GET query string.

   **metrics**
      * Names of metrics to get
      * Type: string
      * Required: No

   **last_evaluated_project**
      * Last evaluated project ID (for pagination)
      * Type: string
      * Required: No

   **last_evaluated_table**
      * Last evaluated table name (for pagination)
      * Type: string
      * Required: No

   **limit**
      * Limit for response items count (for pagination)
      * Type: Number
      * Required: No

**Response Syntax**

.. literalinclude:: ../api/openstack/samples/all_projects_usage_details_response_syntax.json
    :language: javascript

**Response Elements**

   **tenant**
      | Project ID
      | Type: String

   **name**
      | Table name
      | Type: String

   **status**
      | Table status
      | Type: String

   **usage_detailes**
      | Table usage detailes
      | Type: Object

   **size**
      | Table size in bytes.
      | Type: Number

   **item_count**
     | Number of items in table.
     | Type: Number

**Errors**

   500

**Sample Response**

.. literalinclude:: ../api/openstack/samples/all_projects_usage_details_sample_response.json
    :language: javascript
