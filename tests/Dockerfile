FROM phusion/baseimage
MAINTAINER James Kafader <jkafader@archive.org>

# see https://github.com/stuartpb/rethinkdb-dockerfiles/blob/master/trusty/2.1.3/Dockerfile
# and https://github.com/chali/hadoop-cdh-pseudo-docker/blob/master/Dockerfile

ENV LANG=C.UTF-8

RUN apt-get update && apt-get --auto-remove -y dist-upgrade

# Add the RethinkDB repository and public key
# "RethinkDB Packaging <packaging@rethinkdb.com>" http://download.rethinkdb.com/apt/pubkey.gpg
RUN apt-key adv --keyserver pgp.mit.edu --recv-keys 1614552E5765227AEC39EFCFA7E00EF33A8F2399 \
    && . /etc/lsb-release \
    && echo "deb http://download.rethinkdb.com/apt $DISTRIB_CODENAME main" \
       > /etc/apt/sources.list.d/rethinkdb.list \
    && apt-get update && apt-get -y install rethinkdb

RUN mkdir -vp /etc/service/rethinkdb \
    && echo "#!/bin/sh\nrethinkdb --bind 0.0.0.0 --directory /tmp/rethink-data --runuser rethinkdb --rungroup rethinkdb\n" > /etc/service/rethinkdb/run \
    && chmod a+x /etc/service/rethinkdb/run

RUN apt-get -y install python-virtualenv git
RUN apt-get -y install gcc
RUN apt-get -y install python-gdbm python3-gdbm libpython2.7-dev libpython3-dev libffi-dev libssl-dev

# Add the CDH 5 repository
RUN curl -s https://archive.cloudera.com/cdh5/ubuntu/xenial/amd64/cdh/archive.key | apt-key add - \
    && . /etc/lsb-release \
    && echo "deb [arch=amd64] http://archive.cloudera.com/cdh5/ubuntu/$DISTRIB_CODENAME/amd64/cdh $DISTRIB_CODENAME-cdh5 contrib" >> /etc/apt/sources.list.d/cloudera.list

RUN apt-get update
RUN apt-get install -y openjdk-8-jdk hadoop-conf-pseudo

RUN su hdfs -c 'hdfs namenode -format'

EXPOSE 8020 50070 50010 50020 50075

RUN mv -v /etc/hadoop/conf/hdfs-site.xml /etc/hadoop/conf/hdfs-site.xml.orig \
    && cat /etc/hadoop/conf/hdfs-site.xml.orig | sed 's,^</configuration>$,  <property>\n    <name>dfs.permissions.enabled</name>\n    <value>false</value>\n  </property>\n</configuration>,' > /etc/hadoop/conf/hdfs-site.xml

RUN echo '#!/bin/bash\nservice hadoop-hdfs-namenode start\nservice hadoop-hdfs-datanode start' > /etc/my_init.d/50_start_hdfs.sh \
    && chmod a+x /etc/my_init.d/50_start_hdfs.sh

