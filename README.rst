.. image:: https://travis-ci.org/internetarchive/trough.svg?branch=master
    :target: https://travis-ci.org/internetarchive/trough

=======
Trough
=======

Big data, small databases.
==========================

Big data is really just lots and lots of little data. 

If you split a large dataset into lots of small SQL databases sharded on a well-chosen key, 
they can work in concert to create a database system that can query very large datasets.

Worst-case Performance is *important*
=====================================

A key insight when working with large datasets is that with monolithic big data tools' performance 
is largely tied to having a full dataset completely loaded and working in a 
production-quality cluster.

Trough is designed to have very predictable performance characteristics: simply determine your sharding key,
determine your largest shard, load it into a sqlite database locally, and you already know your worst-case
performance scenario.

Designed to leverage storage, not RAM
=====================================

Rather than having huge CPU and memory requirements to deliver performant queries over large datasets,
Trough relies on flat sqlite files, which are easily distributed to a cluster and queried against.

Reliable parts, reliable whole
==============================

Each piece of technology in the stack was carefully selected and load tested to ensure that your data stays
reliably up and reliably queryable. The code is small enough for one programmer to audit.

Ease of installation
====================

One of the worst parts of setting up a big data system generally is getting setting sensible defaults and
deploying it to staging and production environments. Trough has been designed to require as little 
configuration as possible.

An example ansible deployment specification has been removed from the trough
repo but can be found at https://github.com/internetarchive/trough/tree/cc32d3771a7/ansible.
It is designed for a cluster Ubuntu 16.04 Xenial nodes.

