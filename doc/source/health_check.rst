HealthCheck
=============

.. automodule:: magnetodb.api.openstack.health_check

.. http:get:: health_check

**Request Parameters**

    **fullcheck**
       | If fullcheck is 'true' keystone and back-end availability will be checked
       | Syntax: healthcheck?fullcheck={true, false}
       | default: false

**Request Syntax**

   This operation does not require a request body

**Response Syntax**

    **Response Status**
        | 200 or 503

    **Response Body**
        | Content-Type: text/plain
        | "OK" or "Cassanra: ERROR" or "Keystone: ERROR" or "Keystone: ERROR, Cassandra: ERROR"
