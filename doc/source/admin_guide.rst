------------------
Installation guide
------------------

.. contents:: Content


Introduction
------------

This document describes how to install MagnetoDB on Ubuntu 12.04
using Apache Cassandra as database backend.

The following components will be installed also
 * Cassandra cluster with 3 nodes
 * JDK and JNA
 * Python 2.7

All packages will be installed in ``/opt/``


Requirements
------------

MagnetoDB uses authorization via Keystone.
Keystone should be configured beforehand on this or another host.
You should configure user space for MagnetoDB in Keystone.
This document does not describe configuring Keystone.

You should have root access or be logged in as any user
with rights to execute commands via sudo.


Creating a User
---------------

Creating a user ''magneto''::

    groupadd magneto
    useradd -g magneto -s /bin/bash -d /home/magneto -m magneto
    passwd magneto
    Enter new UNIX password:*****
    Retype new UNIX password:*****

Giving magneto user passwordless sudo privileges::

    grep -q "^#includedir.*/etc/sudoers.d" /etc/sudoers || echo "#includedir /etc/sudoers.d" >> /etc/sudoers
    ( umask 226 && echo "magneto ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/50_magneto )

    su magneto
    cd ~


Installing JDK and JNA
----------------------

Installing packages::

    sudo apt-get -y install openjdk-7-jdk libjna-java
    sudo update-alternatives --set java /usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java


Installing Python 2.7
---------------------

Ubuntu 12.04 already has python2.7


Installing Cassandra
--------------------
It is recommended to deploy Cassandra cluster on dedicated hardware.
However in order to try it, you can use one node installation as described below.

Please don't do it for production.

To install the Cassandra cluster on the same node,
we recommend using the CCM (Cassandra Cluster Manager)
https://github.com/pcmanus/ccm

ccm works on localhost only for now. So if you want to create more than one
node clusters the simplest way is to use multiple loopback aliases.
In this example we will build a cluster of three nodes.

Creating loopback aliases::

    sudo ip addr add 127.0.0.2/8 dev lo
    sudo ip addr add 127.0.0.3/8 dev lo

    # Checking results using this command:
    sudo ip addr show lo

Installing required packages::

    sudo apt-get update
    sudo apt-get -y install ant libyaml-0-2 libyaml-dev python-setuptools python-yaml libev4 libev-dev

Installing Cassandra Cluster Manager::

    sudo mkdir -p /opt/ccm
    sudo chown -R magneto:magneto /opt/ccm

    git clone https://github.com/pcmanus/ccm.git /opt/ccm
    cd /opt/ccm
    sudo ./setup.py install

Creating a cluster named ''Storage'' of three nodes of Cassandra 2.1.3::

    ccm create Storage -v 2.1.3
    ccm populate -n 3

Starting Cassandra Cluster::

    ccm start

    # Checking results using this commands:
    ccm status
    ccm node1 ring

Creating keyspaces in cassandra::

    # Replication factor is 3
    echo "CREATE KEYSPACE magnetodb WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };" > ~/.ccm/cql.txt
    echo "CREATE KEYSPACE user_default_tenant WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };" >> ~/.ccm/cql.txt
    echo 'CREATE TABLE magnetodb.table_info(tenant text, name text, id uuid, exists int, "schema" text, status text, internal_name text, last_update_date_time timestamp, creation_date_time timestamp, PRIMARY KEY(tenant, name));' >> ~/.ccm/cql.txt
    echo 'CREATE TABLE magnetodb.backup_info(tenant text, table_name text, id uuid, name text, status text, start_date_time timestamp, finish_date_time timestamp, location text, strategy map<text, text>, PRIMARY KEY((tenant, table_name), id));' >> ~/.ccm/cql.txt
    echo 'CREATE TABLE magnetodb.restore_info(tenant text, table_name text, id uuid, status text, backup_id uuid, start_date_time timestamp, finish_date_time timestamp, source text, PRIMARY KEY((tenant, table_name), id));' >> ~/.ccm/cql.txt
    echo 'CREATE TABLE magnetodb.dummy(id int PRIMARY KEY);' >> ~/.ccm/cql.txt
    ccm node1 cqlsh -f ~/.ccm/cql.txt


Installing MagnetoDB
--------------------

Installing required packages::

    sudo apt-get -y install build-essential python-dev
    sudo easy_install-2.7 pip

Installing MagnetoDB::

    sudo mkdir -p /opt/magnetodb
    sudo chown -R magneto:magneto /opt/magnetodb

    git clone https://github.com/stackforge/magnetodb.git /opt/magnetodb
    cd /opt/magnetodb
    sudo pip2.7 install -r requirements.txt -r test-requirements.txt

Creating directories and log files::

    sudo mkdir -p /var/log/magnetodb
    sudo touch /var/log/magnetodb/magnetodb.log
    sudo touch /var/log/magnetodb/magnetodb-streaming.log
    sudo touch /var/log/magnetodb/magnetodb-async-task-executor.log
    sudo chown -R magneto:magneto /var/log/magnetodb

Configuring MagnetoDB

Before starting magnetos must specify your own values for some variables in the configuration files:
``/opt/magnetodb/etc/api-paste.ini``, ``/opt/magnetodb/etc/streaming-api-paste.ini``,
``/opt/magnetodb/etc/magnetodb-api.conf``, ``/opt/magnetodb/etc/magnetodb-async-task-executor.conf``.
As a minimum, you must specify a value for the following variables
as example::

    auth_host = 127.0.0.1
    auth_port = 35357
    auth_protocol = http
    admin_tenant_name = service
    admin_user = magnetodb
    admin_password = magneto-password

    auth_uri = http://127.0.0.1:5000/v3

    rabbit_host = localhost
    rabbit_userid = userid
    rabbit_password = pass

Running MagnetoDB::

    python /opt/magnetodb/bin/magnetodb-api-server --config-file /opt/magnetodb/etc/magnetodb-api-server.conf
    python /opt/magnetodb/bin/magnetodb-streaming-api-server --config-file /opt/magnetodb/etc/magnetodb-streaming-api-server.conf
    python /opt/magnetodb/bin/magnetodb-async-task-executor --config-file /opt/magnetodb/etc/magnetodb-async-task-executor.conf

