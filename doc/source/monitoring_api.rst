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

   table_usage_details.rst
   project_usage_details.rst
   all_projects_usage_details.rst


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

All metrics for all tables in all projects:

http://{host}:8480/v1/monitoring/projects

All metrics for all tables in specified project:

http://{host}:8480/v1/monitoring/projects/{project_id}

All metrics for specified table:

http://{host}:8480/v1/monitoring/projects/{project_id}/tables/table_name

One metric for specified table:

http://{host}:8480/v1/monitoring/projects/{project_id}/
tables/table_name?metrics=size

Few metrics for specified table:

http://{host}:8480/v1/monitoring/projects/{project_id}/
tables/table_name?metrics=size,item_count
