---------------------
Developer quick-start
---------------------

Setting Up a Development Environment
====================================

This page describes how to setup a working Python development environment
that can be used in developing MagnetoDB on Ubuntu. These instructions assume
you’re already familiar with git. Following these instructions will allow you
to run the MagnetoDB unit tests. If you want to be able to run MagnetoDB, you
will also need to install Cassandra and Devstack.

Virtual environments
--------------------

The easiest way to build a fully functional development environment is with
DevStack. Create a machine (such as a VM or Vagrant box) running a distribution
supported by DevStack and install DevStack there. For example, there is a
Vagrant script for DevStack here_. You can also use this documentation_.

.. _here: https://github.com/jogo/DevstackUp

.. _documentation: https://github.com/stackforge/magnetodb/blob/master/contrib/devstack/README.rst

**NOTE:** If you prefer not to use devstack, you can still check out source
code on your local machine and develop from there.

Linux Systems
-------------

**NOTE:** This section is tested for MagnetoDB on Ubuntu (12.04-64)
distribution. Feel free to add notes and change according to your experiences
or operating system.

Install the prerequisite packages::

$ sudo apt-get install python-dev python-pip git-core


Getting the code
----------------

Grab the code from GitHub::

$ git clone https://github.com/stackforge/magnetodb.git
$ cd magnetodb


Running unit tests
------------------

The unit tests will run by default inside a virtualenv in the `.venv` directory.
Run the unit tests by doing::

$ ./run_tests.sh

The first time you run them, you will be asked if you want to create a
virtual environment (hit “y”)::

 No virtual environment found...create one? (Y/n)

See `Unit Tests`_ for more details.


Manually installing and using the virtualenv
--------------------------------------------

You can manually install the virtual environment instead of having
`run_tests.sh` do it for you::

$ python tools/install_venv.py

This will install all of the Python packages listed in the requirements.txt
file and also those listed in the `test-requirements.txt` file into your
virtualenv. There will also be some additional packages (pip, setuptools,
greenlet) that are installed by the `tools/install_venv.py` file into the
virutalenv.

If all goes well, you should get a message something like this::

 MagnetoDB development environment setup is complete.

To activate the MagnetoDB virtualenv for the extent of your current shell
session you can run::

$ source .venv/bin/activate

Or, if you prefer, you can run commands in the virtualenv on a case by case
basis by running::

$ tools/with_venv.sh <your command>


Remote development
------------------

Some modern IDE such as PyCharm (commercial/open source) support remote
developing. Some useful links:

| `Configuring Remote Interpreters via SSH`_
| `How PyCharm helps you with remote development`_
| `Configuring to work on a VM`_


.. _Configuring Remote Interpreters via SSH:
   http://www.jetbrains.
   com/pycharm/webhelp/configuring-remote-interpreters-via-ssh.html

.. _How PyCharm helps you with remote development:
   http://blog.jetbrains.
   com/pycharm/2013/03/how-pycharm-helps-you-with-remote-development/

.. _Configuring to work on a VM:
   http://www.jetbrains.com/pycharm/quickstart/configuring_for_vm.html

Also, watch this video setting up dev environment for cases when MagnetoDB
installed on the separate machines with Devstack:

`MagnetoDB dev env configuration`_

.. _MagnetoDB dev env configuration:
   https://www.youtube.com/watch?v=HZzz1BrHD-A


Contributing Your Work
----------------------

Once your work is complete you may wish to contribute it to the project.
MagnetoDB uses the Gerrit code review system.


Unit Tests
==========

MagnetoDB contains a suite of unit tests, in the `/magnetodb/tests/unittests`
directory.

Any proposed code change will be automatically rejected by the
OpenStack Jenkins server if the change causes unit test failures.


Preferred way to run the tests
------------------------------
The preferred way to run the unit tests is using tox. See the
`unit testing section of the Testing wiki page`_ for more information.

.. _unit testing section of the Testing wiki page:
    https://wiki.openstack.org/wiki/Testing#Unit_Tests

To run the Python 2.7 tests::

$ tox -e py27

To run the style tests::

$ tox -e pep8

You can request multiple tests, separated by commas::

$ tox -e py27, pep8



Older way to run the tests
--------------------------
Using tox is preferred. It is also possible to run the unit tests using the
`run_tests.sh` script found at the top level of the project. The remainder
of this document is focused on `run_tests.sh`.

Run the unit tests by doing::

$ ./run_tests.sh

This script is a wrapper around the testr_ test runner and the flake8_ checker.

.. _testr: https://code.launchpad.net/testrepository

.. _flake8: https://pypi.python.org/pypi/flake8


Flags
-----

The `run_tests.sh` script supports several flags. You can view a list of
flags by doing::

$ ./run_tests.sh -h

This will show the following help information::

 Usage: ./run_tests.sh [OPTION]...
 Run MagnetoDB's test suite(s)

  -V, --virtual-env        Use virtualenv.  Install automatically if not present.
                           (Default is to run tests in local environment)
  -F, --force              Force a clean re-build of the virtual environment. Useful when dependencies have been added.
  -f, --func               Functional tests have been removed.
  -u, --unit               Run unit tests (default when nothing specified)
  -p, --pep8               Run pep8 tests
  --all                    Run pep8 and unit tests
  -c, --coverage           Generate coverage report
  -d, --debug              Run tests with testtools instead of testr. This allows you to use the debugger.
  -h, --help               Print this usage message

Because `run_tests.sh` is a wrapper around testrepository, it also accepts
the same flags as testr. See the `testr user manual`_ for details about these
additional flags.

.. _testr user manual:
   https://testrepository.readthedocs.org/en/latest/MANUAL.html


Running a subset of tests
-------------------------

Instead of running all tests, you can specify an individual directory, file,
class, or method that contains test code.

To run the tests in the `/magnetodb/tests/unittests/api/openstack/v1`
directory::

$ ./run_tests.sh v1

To run the tests in the
`/magnetodb/tests/unittests/api/openstack/v1/test_get_item.py` file::

$ ./run_tests.sh test_get_item

To run the tests in the GetItemTestCase class in
`/magnetodb/tests/unittests/api/openstack/v1/test_get_item.py`::

$ ./run_tests.sh test_get_item.GetItemTestCase

To run the GetItemTestCase.test_get_item test method in
`/magnetodb/tests/unittests/api/openstack/v1/test_get_item.py`::

$ ./run_tests.sh test_get_item.GetItemTestCase.test_get_item

Also note, that as all these tests (using `tox` or `run_tests.sh`) are run by
`testr` test runner, it is not possible to use `pdb` breakpoints in tests or
the code being tested. To be able to use debugger breakpoints you should
directly use `testtools` as in the following::

$ python -m testtools.run magnetodb.tests.unittests.test_get_item.GetItemTestCase.test_get_item


Virtualenv
----------

By default, the tests use the Python packages installed inside a `virtualenv`_.
(This is equivalent to using the `-V, --virtualenv` flag).

If you wish to recreate the virtualenv, call `run_tests.sh` with the flag::

 -f, --force

Recreating the virtualenv is useful if the package dependencies have changed
since the virtualenv was last created. If the `requirements.txt` or
`tools/install_venv.py` files have changed, it’s a good idea to recreate the
virtualenv.


Integration and functional tests
--------------------------------

MagnetoDB contains a suite of integration tests (in the
`/magnetodb/tests/storage` directory) and functional tests (in the
`/contrib/tempest` directory).

Any proposed code change will be automatically rejected by the
OpenStack Jenkins server if the change causes unit test failures.

Refer to `Tests on environment with devstack`_ for information, how to install
and set environment and how to run such kind of tests.

.. _Tests on environment with devstack:
   https://wiki.openstack.org/wiki/MagnetoDB/QA/Tests_on_env_with_devstack

--------------------
Source documentation
--------------------

magnetodb.common
================

.. automodule:: magnetodb.common.config
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: magnetodb.common.wsgi
   :members:
   :undoc-members:
   :show-inheritance:

magnetodb.common.cassandra
==========================

.. automodule:: magnetodb.common.cassandra.cluster_handler
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: magnetodb.common.cassandra.io.eventletreactor
   :members:
   :undoc-members:
   :show-inheritance:


magnetodb.common.middleware
===========================

.. automodule:: magnetodb.common.middleware.connection_handler
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: magnetodb.common.middleware.context
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: magnetodb.common.middleware.ec2token
   :members:
   :undoc-members:
   :show-inheritance:


.. automodule:: magnetodb.common.middleware.fault
   :members:
   :undoc-members:
   :show-inheritance:


magnetodb.storage
=================

.. automodule:: magnetodb.storage
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: magnetodb.storage.models
   :members:
   :undoc-members:
   :show-inheritance:



magnetodb.storage.driver
========================

.. automodule:: magnetodb.storage.driver
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: magnetodb.storage.driver.cassandra.cassandra_impl
   :members:
   :undoc-members:
   :show-inheritance:


magnetodb.storage.manager
=========================

.. automodule:: magnetodb.storage.manager
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: magnetodb.storage.manager.async_simple_impl
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: magnetodb.storage.manager.simple_impl
   :members:
   :undoc-members:
   :show-inheritance:


magnetodb.storage.table_info_repo
=================================

.. automodule:: magnetodb.storage.table_info_repo
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: magnetodb.storage.table_info_repo.cassandra_impl
   :members:
   :undoc-members:
   :show-inheritance:
