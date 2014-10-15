HealthCheck
=============

Health check request is a lightweight request that allows to check availability
of magnetodb-api service and its subsystems (Keystone, DB back-end)

.. automodule:: magnetodb.api.openstack.health_check

.. http:get:: health_check

**Request Parameters**

.. ?fullcheck={true, false}
   default: false
   If fullcheck is 'true' keystone and back-end availability will be checked

**Request Syntax**

   This operation does not require a request body

**Response Syntax**

.. OK|Cassanra: ERROR|Keystone: ERROR|Keystone: ERROR, Cassandra: ERROR
