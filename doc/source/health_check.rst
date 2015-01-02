HealthCheck
=============

.. automodule:: magnetodb.api.openstack.health_check

.. http:get:: health_check

**Request Parameters**

    **fullcheck**
       | If fullcheck is 'true' subsystems availability will be checked
       | Syntax: healthcheck?fullcheck={true,false}
       | default: false

**Request Syntax**

   This operation does not require a request body

**Response Syntax**

    **Response Status**
        | 200 or 503

    **Response Body**
        | Content-Type: application/json
        | {"API": "string", "Keystone": "string", "RabbitMQ": "string", "Cassandra": "string"}

    **Sample Response Body**
        | {"API": "OK", "Keystone": "ERROR", "RabbitMQ": "OK", "Cassandra": "OK"}
