
About
---------

cassandra-cql is node.js CQL driver for Apache Cassandra(http://cassandra.apache.org) version 0.8 and above.

Installation
--------------

Use npm to install::

  $ git clone https://github.com/yukim/node-cassandra-cql.git
  $ npm install

Usage
---------

Usage::

  var cql = require("cassandra-cql");
  var conn = new cql.Connection({host: 'localhost', keyspace: 'Keyspace1'}); // port is default to 9160
  conn.execute('SELECT * FROM SomeColumnFamily', function(err, res) {
    if (err) {
      // handle errors
      conn.close();
      return;
    }

    res.forEach(function(row) {
      console.log(row);
      row.columns.forEach(function(col) {
        console.log(col.name, col.value);
      });
    });

    conn.close();
  });


For more detailed example, see test/test.js.

TODOs
------------

* Validation and comparator class aware ResultSet
* Query Compression (Do we need this?)
* (Optionally) cqlsh equivalent shell

License
-----------

Apache 2.0 License
