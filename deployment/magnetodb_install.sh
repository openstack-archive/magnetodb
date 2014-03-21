#!/bin/bash
#
# This script was written by Alexei Vinogradov
# Mirantis Inc. 2014.
# Version 0.4 for Ubuntu 12.04
# Example: su stack; cd /home/stack; ./magnetodb_install.sh


JDKURL_RPM="http://download.oracle.com/otn-pub/java/jdk/7u45-b18/jdk-7u45-linux-x64.rpm"
JDKURL_DEB="http://download.oracle.com/otn-pub/java/jdk/7u51-b13/jdk-7u51-linux-x64.tar.gz"
JDKVersion="1.7.0_51"
JDKCookie="Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie"
JDKSpeedLimitRate="512k"
# CassandraPkgName_RPM="cassandra20-2.0.2"
# CassandraPkgName_DEB="cassandra=2.0.2"
CassandraVersion="2.0.2"
CassandraClusterName="test"
# By default CassandraClusterAmountNodes = 3
# If you need more, then you need to change the number of loopback network interface aliases below
CassandraClusterAmountNodes="3"
KEYSPACE_TenantName="default_tenant"
KEYSPACE_ReplicationFactor="3"
CCMURL="https://github.com/pcmanus/ccm"
MagnetodbURL="https://github.com/stackforge/magnetodb.git"
UserName=stack
UserGroup=stack

function myerror () {
  echo "Execution is terminated."; exit 1
}

Platform="Unknown"
sudo grep -l -i Ubuntu /etc/*release  > /dev/null; [[ $? -eq 0 ]] && Platform="Ubuntu"
sudo grep -l -i CentOS /etc/*release  > /dev/null; [[ $? -eq 0 ]] && Platform="CentOS"
if [[ $Platform = "Unknown" ]]; then echo "Unknown operation system."; exit 1; fi
echo "Operation system $Platform"


CurrentDir=`pwd`
DirName=/tmp/`date +%N`
mkdir $DirName
cd $DirName

if [[ $Platform = "Ubuntu" ]]; then

  ### Installing CCM (for Cassandra Cluster Manager ... or something)
  ### --------------------
  echo "===  Installing CCM  ==="
  echo "---  Installing requirements  ---"
  sudo apt-get update
  sudo apt-get -y install python-setuptools python-dev build-essential
  sudo easy_install pyYaml
  sudo apt-get -y install ant
  # Creating loopback network interface aliases
  sudo sh -c "echo '

# The loopback network interface alias
iface lo:0 inet static
address 127.0.0.2
netmask 255.0.0.0
auto lo:0

# The loopback network interface alias
iface lo:1 inet static
address 127.0.0.3
netmask 255.0.0.0
auto lo:1
' >> /etc/network/interfaces"

  sudo ifup lo:0
  sudo ifup lo:1

  echo "---  Downloading CCM  ---"
  cd ~
  git clone $CCMURL

  echo "---  Installing CCM  ---"
  cd ccm
  sudo ./setup.py install
  cd ~

  ### Installing JDK
  ### --------------------
  echo "===  Installing JDK  ==="
  # Download and install JDK
  echo "---  Downloading JDK  ---"
  wget --no-cookies --header "$JDKCookie" $JDKURL_DEB -O jdk-7-linux-x64.tar.gz --no-check-certificate --limit-rate=$JDKSpeedLimitRate

# cp -p ~/jdk-7-linux-x64.tar.gz $DirName/

  echo "---  Installing JDK  ---"
  sudo mkdir -p /usr/lib/jvm
  sudo tar zxvf jdk-7-linux-x64.tar.gz -C /usr/lib/jvm
  PkgName=`ls /usr/lib/jvm | grep jdk$JDKVersion`
  sudo update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/$PkgName/bin/java" 2000
  sudo update-alternatives --install "/usr/bin/java" "java" "/usr/lib/jvm/$PkgName/bin/java" 2000
  sudo update-alternatives --set java /usr/lib/jvm/$PkgName/bin/java

  echo "---  Installing JNA  ---"
  sudo apt-get -y install libjna-java

  # Cleanup
  echo "---  Cleanup  ---"
  sudo rm -rf $DirName/*
  echo

  ### Installing Cassandra Cluster
  ### --------------------
  echo "===  Installing Cassandra Cluster  ==="
  echo "---  Downloading and Compiling Cassandra $CassandraVersion  ---"
  cd ~
  ccm create $CassandraClusterName -v $CassandraVersion

  echo "---  Creating Cassandra Cassandra Cluster $CassandraClusterName ---"
  ccm populate -n $CassandraClusterAmountNodes


  ### Installing magnetodb
  ### --------------------
  echo "===  Installing MagnetoDB  ==="
  echo "---  Installing optional dev lib  ---"
  # for cassandra.io.libevwrapper extension.
  # The C extensions are not required for the driver to run, but they add support
  # for libev and token-aware routing with the Murmur3Partitioner.
  sudo apt-get -y install build-essential python-dev
  
  # libev Support
  sudo apt-get -y install libev4 libev-dev

#  echo "---  easy_install -U distribute  ---"
#  cd /usr/lib/python2.6/site-packages
#  sudo easy_install -U distribute

  echo "---  Downloading MagnetoDB  ---"
  cd ~
  git clone $MagnetodbURL
  echo "---  Installing MagnetoDB  ---"
  cd magnetodb
  sudo sudo pip install -r requirements.txt -r test-requirements.txt 'tox<1.7.0'

  sudo mkdir -p /var/log/magnetodb
  sudo chown $UserName.$UserGroup /var/log/magnetodb
  touch /var/log/magnetodb/magnetodb.log
  cd ~

  ### Starting Cassandra Clauster
  ### --------------------
  echo "===  Starting Cassandra Clauster  ==="
  ccm start

  ### Creating KeySpace
  ### --------------------
  echo "===  Creating KeySpace  ==="
  echo "CREATE KEYSPACE $KEYSPACE_TenantName WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':$KEYSPACE_ReplicationFactor};" > $DirName/KeySpace.txt
  #cqlsh -f $DirName/KeySpace.txt
  ~/.ccm/repository/$CassandraVersion/bin/cqlsh -f $DirName/KeySpace.txt

  # Cleanup
  echo "---  Cleanup  ---"
  cd $CurrentDir
  sudo rm -rf $DirName

fi

#====================================================

if [[ $Platform = "CentOS" ]]; then

  echo "Sorry. Under reconstruction"
  exit 0

  ### Installing JDK
  ### --------------------
  echo "===  Installing JDK  ==="
  # Download and install JDK
  echo "---  Downloading JDK  ---"

#+  wget --no-cookies --header "$JDKCookie" $JDKURL_RPM -O jdk-7u45-linux-x64.rpm --no-check-certificate --limit-rate=$JDKSpeedLimitRate
  echo "---  Installing JDK  ---"
#+  sudo yum -y install jdk-7u45-linux-x64.rpm

  # Cleanup
  echo "---  Cleanup  ---"
  sudo rm -rf $DirName/*
  echo

  ### Installing Cassandra
  ### --------------------
  echo "===  Installing Cassandra  ==="
  # Creating DataStax Repo
  echo "---  Creating DataStax Repo  ---"
  sudo sh -c "echo '[datastax]
name = DataStax Repo for Apache Cassandra
baseurl = http://rpm.datastax.com/community
enabled = 1
gpgcheck = 0' > /etc/yum.repos.d/datastax.repo"

  echo "---  Downloading Cassandra  ---"
  sudo yum -y install yum-downloadonly
  sudo sh -c "echo '[main]
enabled=1' > /etc/yum/pluginconf.d/downloadonly.conf"

#*  sudo yum -y install --downloadonly --downloaddir=./ $CassandraPkgName_RPM
#*  PkgName=`ls | grep $CassandraPkgName_RPM`
  echo "---  Installing Cassandra  ---"
#*  sudo rpm -Uvh $PkgName

  # Cleanup
  echo "---  Cleanup  ---"
  cd $CurrentDir
  sudo rm -rf $DirName

  ### Installing magnetodb
  ### --------------------
  echo "===  Installing MagnetoDB  ==="
  echo "---  easy_install -U distribute  ---"
  cd /usr/lib/python2.6/site-packages
  sudo easy_install -U distribute

  echo "---  Downloading MagnetoDB  ---"
  cd ~
  git clone $MagnetodbURL
  echo "---  Installing MagnetoDB  ---"
  cd magnetodb
  sudo sudo pip install -r requirements.txt -r test-requirements.txt 'tox<1.7.0'

fi

echo
echo
echo "To startup magnetodb use this example:"
echo "python /home/$UserName/magnetodb/bin/magnetodb-api-server --config-dir /home/$UserName/magnetodb/etc/"
echo
echo "All Done."
echo
