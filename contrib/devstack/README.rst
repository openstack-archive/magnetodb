This directory contains the files necessary to integrate Magnetodb with devstack.

To install::

    $ DEVSTACK_DIR=.../path/to/devstack
    $ cp lib/magnetodb ${DEVSTACK_DIR}/lib
    $ cp extras.d/90-magnetodb.sh ${DEVSTACK_DIR}/extras.d

To configure devstack to run Magnetodb::

    $ cd ${DEVSTACK_DIR}
    $ echo "enable_service magnetodb" >> local.conf

Also for disabling Cassandra backend and install only Magnetodb::

    $ echo "MAGNETODB_BACKEND=none" >> local.conf

Run devstack as normal::

    $ ./stack.sh

Note::

    $ Make sure that your local.conf contains [[local|localrc]] in the first line
    $ See more about configuration of Devstack at http://devstack.org/configuration.html
    $ Also you can change other Magnetodb variables, see file lib/magnetodb
