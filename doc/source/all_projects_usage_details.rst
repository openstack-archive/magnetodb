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


**Response Syntax**

.. literalinclude:: ../api/openstack/samples/all_projects_usage_details_response_syntax.json
    :language: javascript

**Response Elements**

   **project_id**
      | Project ID
      | Type: String

   **tables**
      | List of tables in a project
      | Type: List

   **table_name**
      | Table name
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
