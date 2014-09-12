===============
Developer Guide
===============


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
Refer to `Improve MagnetoDB`_ for information. MagnetoDB uses the Gerrit code
review system. For information on how to submit your branch to Gerrit, see
`How to contribute to MagnetoDB`_.


Unit Tests
==========

MagnetoDB contains a suite of unit tests, in the `/magnetodb/tests/unittests`
directory.

Any proposed code change will be automatically rejected by the
`OpenStack Jenkins server`_ if the change causes unit test failures.

.. _OpenStack Jenkins server: `Continuous Integration with Jenkins`_


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
`/tempest` directory).

Any proposed code change will be automatically rejected by the
`OpenStack Jenkins server`_ if the change causes unit test failures.

Refer to `Tests on environment with devstack`_ for information, how to install
and set environment and how to run such kind of tests.

.. _Tests on environment with devstack:
   https://wiki.openstack.org/wiki/MagnetoDB/QA/Tests_on_env_with_devstack


Other Resources
===============

Project hosting with Launchpad
------------------------------

Launchpad_ hosts the MagnetoDB project. The MagnetoDB project homepage on
Launchpad is https://launchpad.net/magnetodb .

.. _Launchpad: https://launchpad.net

Launchpad credentials
`````````````````````
Creating a login on Launchpad is important even if you don’t use the
Launchpad site itself, since Launchpad credentials are used for logging
in on several OpenStack-related sites. These sites include:

 - Wiki_
 - Gerrit (see `Code Reviews with Gerrit`_)
 - Jenkins (see `Continuous Integration with Jenkins`_)

.. _Wiki: https://wiki.openstack.org

Mailing list
````````````
The mailing list email is `openstack-dev@lists.openstack.org`. This is a
common mailing list across the OpenStack projects. To participate in the
mailing list:

 - subscribe at http://lists.openstack.org/cgi-bin/mailman/listinfo/openstack-dev

The mailing list archives are at http://lists.openstack.org/pipermail/openstack-dev.

Bug tracking
````````````
Report MagnetoDB bugs at https://bugs.launchpad.net/magnetodb

Feature requests (Blueprints)
`````````````````````````````
MagnetoDB uses Launchpad Blueprints to track feature requests.
Blueprints are at https://blueprints.launchpad.net/magnetodb.

Technical support (Answers)
```````````````````````````
MagnetoDB uses `Ask OpenStack`_ (which are not hosted on Launchpad) to track
MagnetoDB technical support questions.

.. _Ask OpenStack: https://ask.openstack.org


Code Reviews with Gerrit
------------------------

MagnetoDB uses the `Gerrit`_ tool to review proposed code changes.
The review site is http://review.openstack.org.

.. _Gerrit: https://code.google.com/p/gerrit

Gerrit is a complete replacement for Github pull requests.
**All Github pull requests to the MagnetoDB repository will be ignored.**

See `How to Contribute`_ for information about how to get started using
Gerrit. See `Gerrit Workflow Quick Reference`_ and
`Gerrit, Jenkins and Github`_ for more detailed documentation on how to work
with Gerrit.

.. _Gerrit Workflow Quick Reference:
   https://wiki.openstack.org/wiki/GerritWorkflow

.. _Gerrit, Jenkins and Github:
   https://wiki.openstack.org/wiki/GerritJenkinsGithub

Also, look at `Code Review Guidelines`_.


Continuous Integration with Jenkins
-----------------------------------
MagnetoDB uses a `Jenkins`_ server to automate development tasks. The Jenkins
front-end is at http://jenkins.openstack.org. You must have an account on
`Launchpad`_ to be able to access the OpenStack Jenkins site.

.. _Jenkins: http://jenkins-ci.org

Jenkins performs tasks such as running static code analysis, running unit
tests, and running functional tests. For more details on the jobs being run
by Jenkins, see the code reviews on http://review.openstack.org. Tests are
run automatically and comments are put on the reviews automatically with the
results.

You can also get a view of the jobs that are currently running from the zuul
status dashboard, http://status.openstack.org/zuul/.



------------------------------
How to contribute to MagnetoDB
------------------------------

Improve MagnetoDB
=================

Where can I discuss & propose changes?
--------------------------------------

| Our IRC channel: IRC server `#magnetodb` on **irc.freenode.net**.

| Openstack mailing list: openstack-dev@lists.openstack.org (see subscription
  and usage instructions).

| `MagnetoDB team on Launchpad`_: Questions&Answers/Bugs/Blueprints.

.. _MagnetoDB team on Launchpad: https://launchpad.net/magnetodb


How can I start?
----------------

It is extremely simple to participate in different MagnetoDB development lines.
`MagnetoDB Launchpad page`_ contains a wide range of tasks perfectly suited for
you to start contributing to MagnetoDB. You can choose any unassigned `bug`_ or
`blueprint`_. As soon as you have chosen a bug, just assign it to you, and you
can start fixing it. If you would like chose a blueprint, please contact core
team at the `#magnetodb` IRC channel on **irc.freenode.net**.

.. _MagnetoDB Launchpad page: https://launchpad.net/magnetodb
.. _bug: https://bugs.launchpad.net/magnetodb
.. _blueprint: https://blueprints.launchpad.net/magnetodb


The most bugs and blueprints contain basic descriptions of what is to be done
there; in case you have questions or want to share your ideas, be sure to
contact us in IRC.

How to contribute?
------------------

1. First of all you need a `Launchpad`_ account. Make sure Launchpad has your SSH
key, Gerrit (the code review system) uses this.

2. Sign the Contributors License Agreement as outlined in section 3 of the
`How To Contribute`_ wiki page.

.. _How To Contribute:
   https://wiki.openstack.
   org/wiki/How_To_Contribute#Contributors_License_Agreement


3.\ Tell git your details::

 $ git config --global user.name "Firstname Lastname"
 $ git config --global user.email "your_email@youremail.com"

4.\ Install `git-review`. This tool takes a lot of the pain out of remembering
commands to push code up to Gerrit for review and to pull it back down to
edit it. It is installed using::

 $ pip install git-review

**NOTE:** Several Linux distributions (notably Fedora 16 and Ubuntu 12.04)
are also starting to include git-review in their repositories so it can also
be installed using the standard package manager.

5.\ Grab the MagnetoDB repository::

 $ git clone git@github.com:stackforge/magnetodb.git

6.\ Checkout a new branch to hack on::

 $ git checkout -b TOPIC-BRANCH

7. Start coding.

8.\ Run the test suite locally to make sure nothing broke, e.g.::

 $ ./run_tests.sh

or you can use `tox` test command line tool (install it with
`pip install tox`).

**NOTE:** If you extend MagnetoDB with new functionality, make sure you also
have provided unit tests for it.

9.\ Commit your work using::

 $ git commit -a

or you can use the following to edit a previous commit::

 $ git commit -a --amend

**NOTE:** Make sure you have supplied your commit with a neat commit message,
containing a link to the corresponding blueprint/bug, if appropriate.

10.\ Push the commit up for code review using::

 $ git review

That is the awesome tool you installed earlier that does a lot of hard work
for you.

11.\ Watch your email or `review site`_, it will automatically send your code
for a battery of tests on our Jenkins setup and the core team for the project
will review your code. If there is any changes that should be made they will
let you know.

12.\ When all is good the review site will automatically merge your code.

.. _review site: https://review.openstack.org

(This tutorial is based on:
http://www.linuxjedi.co.uk/2012/03/real-way-to-start-hacking-on-openstack.html)



Code Review Guidelines
======================

Motivation
----------

Any policy or guidelines form something intangible, often called "culture" or
"mentality". They effectively promote values accepted by a community or a
working group. I consider such stuff as very important for any long-term
project success. So I volunteered to help with this corner-stone document.

Of course this is a draft and can and must be discussed. Please come to the
project IRC channel #magnetodb on FreeNode server.

Values we ... hm ... value
--------------------------

Excellence, even small typo can change or even ruin a newcomer's perception.

Openness, we believe the more people contribute their best the better our
product will be.

Easiness, it should be easy to start, easy to excel, easy to be happy in this
big world :-)

We encourage (+1) these things
------------------------------
The change makes code/documents/design ...

Easier to understand

... to read (including typos)

... to maintain in the future

Better in terms of performance

... Scalability

... Simplicity

We discourage (-1) these things
-------------------------------

The changes make code/documents/design ...

... Opposite of values above :)

Especially ...

There is no description what inside and why the change should be accepted.

There is no reference to a bug or a blueprint to track discussions, decisions,
and history.

There are grammar or syntax errors.

The change is too big to be reviewed without extra efforts. It's recommended
to chop design into small chunks gathered into super-blueprint to provide a
big picture.

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

magnetodb.common.notifier
=========================

.. automodule:: magnetodb.common.notifier.decimal_encoder
   :members:
   :undoc-members:
   :show-inheritance:

.. automodule:: magnetodb.common.notifier.event
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
