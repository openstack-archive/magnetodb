==============
Monitoring API
==============

As a magnetodb user I need to know how much data I have in table. As a
magnetodb administrator I need to know now much space is used with user's
table. As a accountant department I need to know how big user's table is
in order to create a bill.

MagnetoDB monitoring actions
============================

.. toctree::
   :maxdepth: 1

   list_tables_monitoring.rst
   table_usage_details.rst


Monitoring API metric list
==========================

+------------+-------------------+------------------+
|    Name    |              Description             |
+============+===================+==================+
|    Size    |  Represents the total  of the space  |
|            |  used by table in bytes.             |
+------------+-------------------+------------------+
| Item Count |  Represents count of items in table. |
+------------+-------------------+------------------+


--------
Examples
--------

All metrics:

http://{host}:8480/v1/monitoring/{project_id}/tables/table_name

One metric:

http://{host}:8480/v1/monitoring/{project_id}/tables/table_name?metrics=size

Few metrics:

http://{host}:8480/v1/monitoring/{project_id}/
tables/table_name?metrics=size,item_count