--------------------
MagnetoDB Data Model
--------------------

Data Model Concepts - Tables, Items, and Attributes
===================================================
The MagnetoDB data model concepts include tables, items and attributes.
In MagnetoDB, a database is a collection of tables. A table is a collection
of items and each item is a collection of attributes. Except for the required
primary key, a MagnetoDB table is schema-less. Individual items in a
MagnetoDB table can have any number of attributes, although there is a limit
of 64 KB on the item size. An item size is the sum of lengths of its attribute
names and values (binary and UTF-8 lengths).
Each attribute in an item is a name-value pair. An attribute can be single-
valued or multi-valued set. For example, a person can have *Name* and *Phones*
attributes. Each person has one name but can have several phone numbers.
The multi-valued attribute is a set; duplicate values are not allowed.
For example, consider storing a list of users in MagnetoDB.

You can create a table, *Users*, with the *Id* attribute as its primary key.::

    “Users” table

    {
        “Id”: 1001,
        “Login”: "admin",
        “Name”: “John Doe”,
        “OfficeNo”: 42
    }

    {
        “Id”: 1002,
        “Login”: “raj”,
        “Name”: “Rajesh Koothrappali“,
        “Phones”: [“555-1212121”, “555-1313131”],
        “DeptId”: 34
    }

In the example, the *Users* table contains two people with different sets of
attributes. Person #1002 has *Phones* - multi-valued attribute. The *Id*
is the only required attribute. Note that attribute values are shown using
JSON-like syntax for illustration purposes. MagnetoDB does not allow null or
empty string attribute values.

Primary Key
===========
When you create a table, in addition to the table name, you must specify
the primary key of the table. MagnetoDB supports the following two types
of primary keys:

* **Hash Type Primary Key** — In this case the primary key is made of one attribute,
  a hash attribute. MagnetoDB builds an unordered hash index on this primary
  key attribute. In the preceding example, the hash attribute for the Users table is Id.

* **Hash and Range Type Primary Key** — In this case, the primary key is made
  of two attributes. The first attribute is the hash attribute and the
  second one is the range attribute. MagnetoDB builds an unordered hash
  index on the hash primary key attribute and a sorted range index on the
  range primary key attribute.


For example, to model a discussion forum, you can create a table, *Threads*,
with the *Subject* attribute as a hash key and the *PostDateTime* as a range key.
We will use *Subject* to identify discussion thread and *PostDateTime* identifies
a message in the thread. Hence, pair of *Subject* and *PostDateTime* will uniquely
defines a message through the whole discussion forum.::

    “Threads” items

    {
        “Subject”: “Help needed”,
        “PostDateTime”: “2014-06-01 14:21:00”,
        “AuthorId”: 1002,
        “MessageBody”: “I need help with my PC”
    }

    {
        “Subject”: “Help needed”,
        “PostDateTime”: “2014-06-01 14:32:00”,
        “AuthorId”: 1001,
        “MessageBody”: “Bring it to my office”
        “RepliesTo”: “2014-06-01 14:21:00”
    }

When designing MagnetoDB tables you have to take into account the fact that
MagnetoDB does not support cross-table joins. In the example above, the *Threads*
table stores only *AuthorId*. If you need the author’s name, you can then parse
the *AuthorId* attribute and use it to query the Users table.

Secondary Indexes
=================
When you create a table with a hash-and-range key, you can optionally define one
or more secondary indexes on that table. A secondary index lets you query the data
in the table using an alternate key, in addition to queries against the primary key.

With the *Threads* table, you can query data items by *Subject* (hash) or by *Subject* and
*PostDateTime* (hash and range). If you had an attribute in the table — *AuthorId*, with
a secondary index on *AuthorId*, you could query the data by *Subject* (hash) and
*AuthorId* (range). Such a query would let you retrieve all the replies posted by a
particular user in a thread, with maximum efficiency and without having to access
any other items.

The kind of secondary index that MagnetoDB supports is a local secondary index —
an index that has the same hash key as the table, but a different range key.
Technically, you can define as many local secondary indexes per table as you need.
But note, that each index decreases performance of PutItem and UpdateItem operations.

MagnetoDB Data Types
====================
MagnetoDB supports the following data types:
* **Scalar data types** — Number, String, and Binary.
* **Multi-valued types** — String Set, Number Set, and Binary Set.

Note that primary key attributes can be any scalar types, but not multi-valued types.
The following are descriptions of each data type, along with examples.
Note that the examples use JSON syntax.

String
------
Strings are Unicode with UTF8 binary encoding. There is no upper limit to the string size
when you assign it to an attribute except when the attribute is part of the primary key.
The length of the attribute must be greater than zero. String value comparison is used when
returning ordered results in the Query and Scan APIs.
Comparison is based on ASCII character code values.
For example, "a" is greater that "A" , and "aa" is greater than "B".

Example::

    {"S": "John Doe"}

Number
------
Numbers are positive or negative exact-value decimals and integers. The
representation in MagnetoDB is of variable length. Leading and trailing
zeroes are trimmed.
Serialized numbers are sent to MagnetoDB as String types, which maximizes
compatibility across languages and libraries, however MagnetoDB handles
them as the Number type for mathematical operations.

Example::

    {"N": "42"}

Binary
------
Binary type attributes can store any binary data, for example, compressed
data, encrypted data, or images. MagnetoDB treats each byte of the binary
data as unsigned when it compares binary values, for example, when evaluating
query expressions. The length of the attribute must be greater than zero.
The following example is a binary attribute, using Base64-encoded text.

Example::

    {"B": "MjAxNC0wMy0yMw=="}

String, Number, and Binary Sets
-------------------------------
MagnetoDB also supports number sets, string sets and binary sets. Multi-valued
attributes such as Authors attribute in a book item and Color attribute of a
product item are examples of string set type attributes. Because it is a set,
the values in the set must be unique. Attribute sets are not ordered; the order
of the values returned in a set is not preserved. MagnetoDB does not support
empty sets.

Examples::

    {"SS": ["John Doe","Jane Smith"] }
    {"NS": ["42","3.14","2.71828", "-12"] }
    {"BS": ["MjAxNC0wMy0yMw==","MjAxNS0wMy0yNA==","MjAxNi0wNi0yNg=="] }

---------------------------------
Supported Operations in MagnetoDB
---------------------------------

To work with tables and items, MagnetoDB offers the following set of operations:

Table Operations
================
MagnetoDB provides operations to create and delete tables. MagnetoDB also
supports an operation to retrieve table information (the DescribeTable
operation) including the current status of the table, the primary key,
and when the table was created. The ListTables operation enables you to
get a list of tables.

Item Operations
===============
Item operations enable you to add, update and delete items from a table.
The UpdateItem operation allows you to update existing attribute values,
add new attributes, and delete existing attributes from an item. You can
also perform conditional updates. For example, if you are updating a price
value, you can set a condition so the update happens only if the current
price is $10.

MagnetoDB provides an operation to retrieve a single item (GetItem) or multiple
items (BatchGetItem). You can use the BatchGetItem operation to retrieve items
from multiple tables.

Query and Scan
==============
The Query operation enables you to query a table using the hash attribute and
an optional range filter. If the table has a secondary index, you can also
Query the index using its key. You can query only tables whose primary key is
of hash-and-range type; you can also query any secondary index on such tables.
Query is the most efficient way to retrieve items from a table or a secondary
index.

MagnetoDB also supports a Scan operation, which you can use on a query or a
secondary index. The Scan operation reads every item in the table or secondary
index. For large tables and secondary indexes, a Scan can consume a large amount
of resources; for this reason, we recommend that you design your applications
so that you can use the Query operation mostly, and use Scan only where
appropriate. You can use conditional expressions in both the Query and Scan
operations to control which items are returned.

-------------------
Accessing MagnetoDB
-------------------
MagnetoDB is a web service that uses HTTP and HTTPS as a transport and
JavaScript Object Notation (JSON) as a message serialization format. Your
application code can make requests directly to the MagnetoDB web service API.
Each request must contain a valid JSON payload and correct HTTP headers, including
a valid authentication token.
