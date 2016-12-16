#!/bin/sh
# this file is here as a place to take notes about what the eventual ansible scripts should setup.

# this may come in handy. Get all the addresses of all of the consul nodes.
# curl http://consul.archive-it.org:8500/v1/catalog/nodes | jq -r '.[].Address' | paste -s -d, - # yields 10.0.1.22,10.12.100.100,...


################################
# CONSUL NODE SCRIPT
################################
sudo apt-get install docker
#sudo mkdir /etc/consul.d
# decision point: should we have a separate service name for the 'read' service vs 'write'?
# decision point: how should the port be configured? 
# echo '{"service": {"name": "{{ trough_service_name }}", "tags": [{{ dynamically configured via synchronizer }}], "port": {{read_port}} }}' /etc/consul.d/{{ trough_service_name }}
#echo '{"service": {"name": "trough", "tags": [], "port": 6008 } }' > /etc/consul.d/trough
# write a service script that sends a SIGHUP to consul?
# sudo service trough reload

# e.g. consul_hosts: "10.30.20.10:7300 10.30.20.11:7300 10.30.20.12:7300"
# consul agent -server -join {{ consul_hosts }} -data-dir=/var/tmp/consul -node={{ consul_server_one }} -bind={{ consul_server_one_ip }} -config-dir=/etc/consul.d
docker run -d consul agent -server -join 127.0.0.1:7300 127.0.0.2:7300 127.0.0.3:7300 -data-dir=/tmp/consul -node=consul-one -bind=172.20.20.10 -config-dir=/etc/consul.d -bootstrap-expect=3

################################
# READ NODE SCRIPT
################################
sudo apt-get install docker

# consul agent -join {{ consul_hosts }} -data-dir=/var/tmp/consul -node={{ read_server_one }} -bind={{ read_server_one_ip }} -config-dir=/etc/consul.d
docker run -d consul agent -join 127.0.0.1:7300 127.0.0.1:7300 127.0.0.3:7300 -data-dir=/var/tmp/consul -node=read-one -bind=172.20.20.18 -config-dir=/etc/consul.d
#    start trough read
docker run -d trough-read
#sudo service trough-read start
docker run -d trough-sync-local

################################
# WRITE NODE SCRIPT
################################
sudo apt-get install docker
#sudo mkdir /etc/consul.d

# consul agent -join {{ consul_hosts }} -data-dir=/var/tmp/consul -node={{ read_server_one }} -bind={{ read_server_one_ip }} -config-dir=/etc/consul.d
docker run -d consul agent -join 127.0.0.1:7300 127.0.0.1:7300 127.0.0.3:7300 -data-dir=/var/tmp/consul -node=write-one -bind=172.20.20.18 -config-dir=/etc/consul.d
# start trough write
docker run -d trough-write
# start trough synchronize (local mode)
docker run -d trough-sync-local


################################
# SYNC NODE SCRIPT
################################
sudo apt-get install docker

# consul agent -join {{ consul_hosts }} -data-dir=/var/tmp/consul -node={{ read_server_one }} -bind={{ read_server_one_ip }} -config-dir=/etc/consul.d
docker run -d consul agent -join 127.0.0.1:7300 127.0.0.1:7300 127.0.0.3:7300 -data-dir=/var/tmp/consul -node=sync-one -bind=172.20.20.18 -config-dir=/etc/consul.d
# start trough synchronize (local mode)
docker run -d trough-sync