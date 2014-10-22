------------
Introduction
------------
MagnetoDB is a key-value store service for OpenStack. It provides horizontally scalable, queryable  storage, accessible
via REST API. MagnetoDB supports Amazon DynamoDB API as well.

MagnetoDB has been designed and developed in order to provide:

* **Easy integration** REST-like API. Support Amazon DynamoDB API
* **OpenStack interoperability** Integrated with Keystone. Follows OpenStack design tenets, packaging and distribution
* **Database pluggability** Supports Cassandra,any other databases like MongoDB, HBase could be easily added
* **Horizontal scalability** MagnetoDB is scalable lineary by amount of data and requests
* **High availability** Just one working API node is enough to continue handling requests