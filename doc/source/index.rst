.. MagnetoDB documentation master file, created by
   sphinx-quickstart on Tue Mar  4 14:41:26 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

=====================================
Welcome to MagnetoDB's documentation!
=====================================

------------
Introduction
------------
MagnetoDB is a key-value store service for OpenStack. It provides horizontally scalable, queryable  storage, accessible
via REST API. MagnetoDB supports Amazon DynamoDB API as well.

MagnetoDB has been designed and developed in order to provide:

* **Easy integration** REST-like API. Support Amazon DynamoDB API
* **OpenStack interoperability** Integrated with Keystone. Follows OpenStack design tenets, packaging and distribution
* **Database pluggability** Supports Cassandra,any other databases like MongoDB, HBase could be easily added
* **Horizontal scalability** MagnetoDB is scalable linearly by amount of data and requests
* **High availability** Just one working API node is enough to continue handling requests


-----------------------
Developer documentation
-----------------------

Developer guide
===============
.. toctree::
   :maxdepth: 1

   developer_guide.rst


API Reference
=============
.. toctree::
   :maxdepth: 1

   api_reference.rst

   monitoring_api.rst

   dynamodb_api.rst

   health_check.rst

------------------
User documentation
------------------

User guide
==========
.. toctree::
   :maxdepth: 1

   user_guide.rst

Magnetodb CLI
=============
.. toctree::
   :maxdepth: 1

   cli.rst

------------
Admin guides
------------
.. toctree::
   :maxdepth: 1

   admin_guide.rst

   configuration_guide.rst

------------------
Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

