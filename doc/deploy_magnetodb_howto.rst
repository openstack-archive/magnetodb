MagnetoDB deployment HOWTO
===========================


Overview
========

This  document describes how to install MagnetoDB on Mirantis OpenStack.

Minimal MagnetoDB installation contains of 5 VMs

- 2 MagnetoDB  instances installed on CentOS pre-build image
- 3 Cassandra nodes installed on CentOs 6.4 pre-build image (database backend)

Steps to deploy MagnetDB are:

- build MagnetoDB rpm package and all dependencies including python 2.7
- create repository for built packages and make it accessable via http. 
- build image with Cassandra and MagnetoDB included.
- deploy image built on previous step on Mirantis OpenStack with heat tool

You'll need CentOS 6.4 to build rpm packages. Mirantis OpenStack may be deployed over Ubuntu or CentOS.
Not tested on other OpenStack distributions. 

Building RPMs
=============

MagnetoDB runtime dependency list
---------------------------------

- python-cassandradriver >= 1.0.0b7
- python-eventlet >= 0.13
- python-iso8601 >= 0.1.4
- python-jsonschema >= 1.3.0, python-jsonschema <= 1.4.0
- python-kombu >= 2.4.8
- python-oslo-config >= 1.2.0
- python-pbr >= 0.5.21, python-pbr < 1.0
- python-keystoneclient >= 0.3.2
- python-babel >= 0.9.6
- python-paste-deploy = 1.5.0
- python-routes >= 1.12.3
- python-webob >= 1.2.3, python-webob < 1.3
- python27 = 2.7.1
- python27-tools = 2.7.1

All spec files for dependencies are included into MagnetoDB distribution (deployment/centos/dependency-specs folder) or can be installed from base or epel CentOS repositories.

Building RPMs for dependencies
------------------------------

To build dependencies you’ll need to do the following steps:

- Enable epel repo:
  
  - **wget http://dl.fedoraproject.org/pub/epel/6/i386/epel-release-6-8.noarch.rpm**
  
  - **rpm -ivh epel-release-6-8.noarch.rpm**
  
- Install rpmbuild tool

- Build rpm for dependencies:
  
  - download source code from https://pypi.python.org and put into SOURCE folder (Source code for python 2.7 is available at
    https://git.gitorious.org/pkg-python27/pkg-python27.git.
    Packages of python 2.7 do not replace python 2.6 so it is 100% safe
    for other python-based software and python 2.7 can be used simultaneously with python 2.6)
  
  - build rpm: rpmbuild -ba <spec.file>
  
  - if any unsatisfied requirements you’ll get notification - please install all packages  using yum tool and rum rpmbuild again.

Building RPM for MagnetoDB
--------------------------

- Go to magnetodb folder

- Run **PBR_VERSION=0.0.1 python.setup.py sdist** command to get tar.gz file with source code

- Copy **dist/magnetodb-0.0.1.tar.gz** to **SOURCE** folder of rpmbuild tree

- Copy **deployment/centos/magnetodb-specs/openstack-magnetodb-api** file to **SOURCE** folder of rpmbuild tree

- Run **rpmbuild -ba  deployment/centos/magnetodb-specs/openstack-magnetodb.spec** command

You can get more details about rpm packages from “How to create RPM” document:  https://fedoraproject.org/wiki/How_to_create_an_RPM_package?rd=PackageMaintainers/CreatingPackageHowTo


Creating repository
===================

- Install creatrepo tool

- Install and configure any http-server (apache, nginx etc)to make www-root accessible,
  e.g. /var/www/html/magnetodb-repo

- Copy all rpms for dependencies and magnetodb rpm in this folder

- Download cassandra 2.0.2 rpm: http://rpm.datastax.com/community/noarch/cassandra20-2.0.2-1.noarch.rpm  and copy to this folder

- Download Oracle Java 1.7 rpm and copy to this folder

- **cd /var/www/html/magnetodb-repo**

- **createrepo .**

- Create repository config file, e.g.:

::

  [magnetodb]
  name=magnetodb
  baseurl=http://192.168.0.1/magnetodb
  enabled=1
  gpgcheck=0

Please pay attention - baseurl depends on your http server configuration
Now you have repository with magnetodb and all requirements.
Repository is used during image build and must be accessable. 


Building image with MagnetoDB
=============================

For  deployment we use pre-created image with CentOS and MagnetoDB on it.
Below you can see steps how to create this image:

- Install JEOS toolkit: http://docs.openstack.org/developer/heat/getting_started/jeos_building.html
- Copy template **deployment/heat_templates/CentOS-6.4-x86_64-cfntools.tdl** to JEOS templates folder
- Please pay attention: you need to change in template the following lines:

  - path to repository file from http://192.168.1.1/magnetodb/magnetodb.repo to your repo path (configured on previous step)

  - you need to change passwords for “root” and “test” users.

- Build image: .**/heat-jeos.sh ../jeos/CentOS-6.4-x86_64-cfntools.tdl CentOS-6.4-x86_64-cfntool**
- Upload image to OpenStack environment and add it to glance as public image.


Deployment MagnetoDB on existing OpenStack
==========================================

For MagnetoDB we will use Mirantis OpenStack installed with Fuel.
On this step you have CentOs 6.4 image with installed but not configured cassandra and MagnetoDB.


Also you have oracle java installed as default java  and python 2.7.
Image built on previous must be accessible via glance.

To deploy MagnetoDB you need:
- Deploy at least 2 nodes with MagnetoDB API 
- Deploy at least 3 cassandra nodes
- Deploy Load Balancer.

First please check neutron, heat  and lbaas support on your OpenStack environment.
More details about lbaas plugin: https://wiki.openstack.org/wiki/Neutron/LBaaS/HowToRun

Please check do you have enough free resources on your compute nodes
To deploy simple MagnetoDB environment you need

- heat tool installed and configured
- Openstack credentials added to environment

Please edit template parameters before deploy:

- key_name: pre-created ssh key.  More details about key management: http://docs.openstack.org/user-guide/content/create_import_keys.html 
- flavor: flavor name.
  you need at least 1Gb of RAM for any instance
- image: Name of image in glance
- private_subnet_id, external_network_id, private_net  - network IDs for instances and LBaaS

::

 +--------------------------------------+-----------+--------------------------------------------------------+
 | id                                   | name        | subnets                                              |
 +--------------------------------------+-----------+--------------------------------------------------------+
 | 7c7e1cdc-70d0-4bc1-8fad-6510c1b2d7cb | net04     | ba1fb022-2f58-44ea-9b8e-0453de72a043 192.168.111.0/24  |
 | 863abccb-ad5f-4719-aeef-3da9f0c7f194 | net04_ext | 34c1f8a0-0bd2-4beb-9867-4e36590f06c5 172.18.169.128/25 |
 +--------------------------------------+-----------+--------------------------------------------------------+

In example above

- private_subnet_id - ba1fb022-2f58-44ea-9b8e-0453de72a043
- external_network_id - 863abccb-ad5f-4719-aeef-3da9f0c7f194
- private_net - 7c7e1cdc-70d0-4bc1-8fad-6510c1b2d7cb

External network is necessary for LBaaS
After you have template configured please deploy MagnetoDB stack with following command:

**heat stack-create  -f /path/to/magnetodb_and_loadbalancer.yaml test-stack**

**magnetodb_and_loadbalancer.yaml** is part of MagnetoDB distribution.

After deployment finished you can get details with

**heat  stack-show test-stack** command:

::


  
  {
  
  "output_value": "172.18.169.205",
  "description": "LB address",
  "output_key": "floating_ip_address" 
  }

On example above  MagnetoDB  is accessable via URL http://172.18.169.205:8080/
