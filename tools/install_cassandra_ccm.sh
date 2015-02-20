#!/bin/bash -x

CASSANDRA_AMOUNT_NODES=1
MAGNETODB_DIR=$WORKSPACE
CCM_REPO=${CCM_REPO:-'https://github.com/pcmanus/ccm.git'}
CCM_BRANCH=${CCM_BRANCH:-master}
CCM_DIR=${CCM_DIR:-$HOME/ccm}
CASSANDRA_VER=${CASSANDRA_VER:-2.1.3}
CASSANDRA_CLUSTER_NAME=${CASSANDRA_CLUSTER_NAME:-test}
CASSANDRA_REPL_FACTOR=${CASSANDRA_REPL_FACTOR:-1}
GRADLE_VER=${GRADLE_VER:-2.2.1}
GRADLE_REPO=${GRADLE_REPO:-"https://services.gradle.org/distributions/gradle-$GRADLE_VER-bin.zip"}
GRADLE_DEST=`mktemp -d`

function fix_etc_hosts {
    # HPcloud stopped adding the hostname to /etc/hosts with their
    # precise images.

    HOSTNAME=`/bin/hostname`
    if ! egrep "[[:space:]]$HOSTNAME$" /etc/hosts > /dev/null; then
        echo "Need to add (or fix) hostname to /etc/hosts"
        if egrep "127.0.1.1[[:space:]]$HOSTNAME" /etc/hosts; then
            sudo sed -i "s/^127\.0\.1\.1.*$/127\.0\.1\.1 $HOSTNAME/" /etc/hosts
        else
            sudo bash -c 'echo "127.0.1.1 $HOSTNAME" >> /etc/hosts'
        fi
    fi
}

function apt_get {

    sudo DEBIAN_FRONTEND=noninteractive apt-get  \
        --option "Dpkg::Options::=--force-confold" --assume-yes "$@"
}

function configure_cassandra() {
    #allocate loopback interfaces 127.0.0.2 - its a first address
    #for second cassandra, the first node will be use 127.0.0.1
    n=1
    addr=2
    while [ $n -lt $CASSANDRA_AMOUNT_NODES ]; do
        echo "add secondary loopback 127.0.0.${addr}/8"
        #adding adresses only if doesnt exist
        sudo ip addr add 127.0.0.${addr}/8 dev lo || [ $? -eq 2 ] && true
        let addr=$addr+1
        let n=$n+1
    done

    ccm status $CASSANDRA_CLUSTER_NAME || ccm create $CASSANDRA_CLUSTER_NAME -v $CASSANDRA_VER

    # Build cassandra custom index
    wget -q $GRADLE_REPO -O $GRADLE_DEST/gradle.zip
    cd $GRADLE_DEST
    unzip gradle.zip
    PATH=$PATH:$GRADLE_DEST/gradle-$GRADLE_VER/bin
    cd $MAGNETODB_DIR/contrib/cassandra/magnetodb-cassandra-custom-indices
    gradle build
    CCIV=`grep '^version' build.gradle | cut -d"'" -f2`
    cp $MAGNETODB_DIR/contrib/cassandra/magnetodb-cassandra-custom-indices/build/libs/magnetodb-cassandra-custom-indices-$CCIV.jar ~/.ccm/repository/${CASSANDRA_VER}/lib/

    # Populate cassandra nodes
    ccm populate -n $CASSANDRA_AMOUNT_NODES || true
}

function create_keyspace_cassandra() {
    local k_name=$1
    echo "CREATE KEYSPACE $k_name WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : $CASSANDRA_REPL_FACTOR};"  >> ~/.ccm/cql.txt
}

fix_etc_hosts

# install requirements
apt_get -y install ant libyaml-dev libev4 libev-dev libxml2-dev libxslt-dev python-dev

# install java
apt_get -y install openjdk-7-jdk
sudo update-alternatives --set java /usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java

# install ccm
git clone $CCM_REPO $CCM_DIR -b $CCM_BRANCH
sudo pip install -e $CCM_DIR

# start cassandra
configure_cassandra
ccm start

# create keyspace and table
create_keyspace_cassandra magnetodb
create_keyspace_cassandra user_default_tenant
echo 'CREATE TABLE magnetodb.table_info(tenant text, name text, id uuid, exists int, "schema" text, status text, internal_name text, last_update_date_time timestamp, creation_date_time timestamp, PRIMARY KEY(tenant,name));' >> ~/.ccm/cql.txt
echo 'CREATE TABLE magnetodb.backup_info(tenant text, table_name text, id uuid, name text, status text, start_date_time timestamp, finish_date_time timestamp, location text, strategy map<text, text>, PRIMARY KEY((tenant, table_name), id));' >> ~/.ccm/cql.txt
echo 'CREATE TABLE magnetodb.restore_info(tenant text, table_name text, id uuid, status text, backup_id uuid, start_date_time timestamp, finish_date_time timestamp, source text, PRIMARY KEY((tenant, table_name), id));' >> ~/.ccm/cql.txt
echo 'CREATE TABLE magnetodb.dummy(id int PRIMARY KEY);' >> ~/.ccm/cql.txt

timeout 120 sh -c 'while ! nc -z 127.0.0.1 9160; do sleep 1; done' || echo 'Could not login at 127.0.0.1:9160'
ccm node1 cqlsh -f ~/.ccm/cql.txt
