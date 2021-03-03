====================
The Design of Trough
====================

Inspiration
===========

On the Archive-It team at Internet Archive, we run many crawlers in parallel, all executing crawl jobs.
Each of the crawlers log many events into their logs per second. At any given time, we expect to be able
to quickly serve analytics on any of the million+ crawls that we've run both historically, and in the last
few minutes.

We tried a number of analytics stores that would work at this scale, but none of them fit the bill. One
came close, a system called Druid that was designed for real-time analytics for advertisements. We found
operations with this system to be too challenging to maintain for a variety of reasons, mostly centered 
around tuning complexity and running java processes at scale in production (for those of you who have
never undertaking this challenge, it involves constant vigilance with reference to memory tuning, lest
the JVM should throw and OOME).

This led to a situation in which we were essentially sure about the features we _didn't want_ in our 
application -- a list of performance and maintenance challenges. Instead of re-tooling a system which
had many of these challenges baked-in, we decided instead to focus on building a new distributed 
database system, culling the tried-and-true technologies that we had found to be invaluable many times.

Our Query Pattern
-----------------

As we gained experience with previous systems, we found that most of our data was tied to a single sharding
key: the ID number of a given "crawl job" -- a single run of a crawl on a single machine.

Specifically, we thought that we could phrase the problem of analytics at scale as essentially a 
service discovery problem where small shards of analtics data could be segmented on some reasonable
key.

This led to an approach where we considered a number of service discovery systems and distributed configuration
stores -- including some such as `zookeeper`, `etcd` and `zetcd` to name a few. We again knew from broad
experience in our problem that this would be the bottleneck in any distributed design: _how many small 
databases could the system publish discovery data on in parallel?_

We found a match for our needs in a no-SQL database called RethinkDB: it now publishes discovery data for 
and receives TTL heartbeats on ~1.5m small databases. The key attribute of RethinkDB, however, is its
resiliency and ease of operation. The system can sustain writes with 1/3 of its servers missing; reads
with 2/3 of its servers missing. This particular feature of the system is particularly critical when
running servers with no battery backup as we do at Internet Archive.

Goals
=====

From the beginning, trough was designed to be a distributed system. An observation that guided its
development was that a distributed system multiplies, sometimes exponentially, the complexity of any 
given part. We took time to ensure that the algorithms and components involved were as reliable as possible.


Design
======

Topology
--------

RethinkDB
~~~~~~~~~

Sync Servers
~~~~~~~~~~~~

Workers
~~~~~~~

Interaction with HDFS
---------------------

Cold Storage
------------


The Shell
=========

Installing and using the shell
------------------------------

```
git clone https://github.com/internetarchive/trough.git

cd trough

virtualenv -p python3 venv

source venv/bin/activate

pip install -e .
```

SQLite SQL dialect
------------------

Multiple connections
--------------------

Aggregation Functions
---------------------


Maintenance and FAQs
====================


Known Issues
============

