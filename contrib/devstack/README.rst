This directory contains the files necessary to integrate Magnetodb with devstack.

To install::

    $ DEVSTACK_DIR=.../path/to/devstack
    $ cp lib/magnetodb ${DEVSTACK_DIR}/lib
    $ cp extras.d/90-magnetodb.sh ${DEVSTACK_DIR}/extras.d

To configure devstack to run Magnetodb::

    $ cd ${DEVSTACK_DIR}
    $ echo "enable_service magnetodb" >> local.conf

To have an ability to run functional tests::

    $ echo "enable_service tempest" >> local.conf

Also for disabling Cassandra backend and install only Magnetodb::

    $ echo "MAGNETODB_BACKEND=none" >> local.conf

Run devstack as normal::

    $ ./stack.sh

Note::

    $ Make sure that your local.conf contains [[local|localrc]] in the first line
    $ See more about configuration of Devstack at http://devstack.org/configuration.html
    $ Also you can change other Magnetodb variables, see file lib/magnetodb


To install behind a proxy:

Add the following lines to files '/root/.bashrc', '/home/stack/.bashrc' and '/etc/profile.d/01-proxy.sh'::

    http_proxy=http://xxx.xxx.xxx.xxx:yyyy
    https_proxy=$http_proxy
    ftp_proxy=$http_proxy
    HTTP_PROXY=$http_proxy
    HTTPS_PROXY=$http_proxy
    FTP_PROXY=$http_proxy
    export http_proxy https_proxy ftp_proxy HTTP_PROXY HTTPS_PROXY FTP_PROXY
    alias curl="curl -x $http_proxy"

Note::

    User 'stack' is used as an example, you can use any other user which have rights for sudo.
    Optionally you can also add a variable for ANT:
        ANT_OPTS="-Dhttp.proxyHost=http://xxx.xxx.xxx.xxx -Dhttp.proxyPort=yyyy"
        export ANT_OPTS
    But this is not necessary.

Add the following lines to file '/etc/apt/apt.conf.d/proxy'::

    Acquire::http::Proxy "http://xxx.xxx.xxx.xxx:yyyy/";
    Acquire::https::Proxy "https://xxx.xxx.xxx.xxx:yyyy/";
    Acquire::ftp::Proxy "ftp://xxx.xxx.xxx.xxx:yyyy/";
    Acquire::::Proxy "true";

Configure git to use proxy::

    $ sudo git config --global http.proxy xxx.xxx.xxx.xxx:yyyy

Before starting the installation, you must relogin or open a new session.
It is necessary for initialize the variables of environment.

Note::

    Do not set 'OFFLINE=True' in 'local.conf' during the first installation.
    This option is used only if all the required packages are already installed.
