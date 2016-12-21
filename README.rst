=======
Trough
=======

Big data, small databases.
==========================

The overall thrust of the design of trough is to separate a very large dataset into small SQLite databases that can be quickly and easily queried. The system focuses on simplicity and ease of administration and setup.


=============
The Processes
=============

Configuration/Ansible Deployment system
=======================================
    - configures hosts
    - deploys 4 types of worker nodes: a read worker, a write worker, a consul worker and a synchronizer worker.

Processes
=========

Reader
------
    - performs read requests on sqlite files
    - receives POST requests of SQL, throws error on non-SELECT queries
    - throws an error in the case that it is queried for a non-existant database
    - one reader per httpd thread


Writer
------
    - performs write requests to sqlite files
    - receives POST requests of SQL, throws error on non-INSERT/UPDATE queries

Consul Agent (Server mode)
--------------------------
    - Vanilla consul agent running in server mode
    - 3 dedicated server consul nodes
    - Every Read Node and Write Node runs consul agent (a gossip agent)
    - By way of DNS, HTTP POST SQL queries are sent directly to a Read Node
        for example:
        POST http://128100.trough.service.archive-it.org/
        -----
        SELECT count(id) FROM crawled_urls WHERE host = "www.example.com" GROUP BY status_code;
        -----
        Response returned as JSON
    - Health Checks:
        - make http 

Consul Agent (Local mode)
-------------------------
    - Vanilla consul agent running in local mode

Synchronizer (Server mode)
--------------------------
    - A small process that runs on at least two nodes for failover redundancy (dedicated or shared with Read workers)
    - Synchronizers elect a leader "Lead Synchronizer" via Consul
    - Local Synchronizer pushes tags to consul when segments are available
    - Local Synchronizer pushes a K/V for the current host and stored vs quota
    - Lead Synchronizer assigns segments to other nodes based on stored/quota ratio
    - Lead Synchronizer assigns one or more Read Nodes for each read-only segment (replication is configurable)
    - Lead Synchronizer assigns one Write Node for each writable segment
    - Lead Synchronizer discovers total segment pool from HDFS (file listing?)

Synchronizer (Local mode)
-------------------------
    - the local synchronizer should always be run niced so that we interfere as little as possible with query times.
    - checks if databases are removed from consul manifest, deletes them.
    - reads a consul manifest, pulls down sqlite files from HDFS, checks against checksum
    - sets up files to respond on /health/[ID], caches comparison of checksum of local files against HDFS, server returns 200 or 500 depending on comparison
    - periodically reports the databases that are *actually* available on the local node, in case of partial failure.



=========
The Nodes
=========

Read Node
=========
    - Runs a Reader process
    - Runs a Synchronizer process (Local mode)
    - Runs a Consul Agent (Local mode)

Write Node
==========
    - Runs a Writer process
    - Runs a Reader process
    - Runs a Synchronizer process (Local mode)
    - Runs a Consul Agent (Local mode)

Consul Node
===========
    - Runs a Consul Agent (Server mode)

Synchronizer Node
=================
    - Runs a Synchronizer process (Server mode)