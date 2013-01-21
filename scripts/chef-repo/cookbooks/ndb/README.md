Description
===========

Installs and configures a MySQL Cluster, including the management server(s), data nodes, and MySQL Server(s).

Requirements
============
Chef 0.10.10+.

Platform
--------
* Ubuntu, Debian


Tested on:

* Ubuntu 10.04-12.04


Attributes
==========

Usage
=====

On a node that provides both a Management Server and a MySQL Server, use both the mgmd and mysqld recipes:

    { "run_list": ["recipe[ndb::mgmd]", "recipe[ndb::mysqld]" }

This will install and start both a ndb_mgmd and a mysqld daemon on both nodes.

On a node that will provide a data node, run:
    { "run_list": ["recipe[ndb::ndbd]" }

This will install a data node on the host, that is, an ndbd process.

You can override attributes in your node or role.
For example, on an Ubuntu system:

    {
      "mysql": {
        "password": "secret"
      }
    }