This directory contains the files necessary to integrate Magnetodb with devstack.

To install::

    $ DEVSTACK_DIR=.../path/to/devstack
    $ cp lib/magnetodb ${DEVSTACK_DIR}/lib
    $ cp extras.d/70-magnetodb.sh ${DEVSTACK_DIR}/extras.d

To configure devstack to run Magnetodb::

    $ cd ${DEVSTACK_DIR}
    $ echo "enable_service magnetodb-api" >> local.conf

Run devstack as normal::

    $ ./stack.sh
