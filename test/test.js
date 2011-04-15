/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var assert = require('assert'),
    cql = require('cassandra-cql');

var conn;

var schemaSetUp = false;
module.exports = require('nodeunit').testCase({
  'setUp': function(callback) {

    conn = new cql.Connection();
    if (!schemaSetUp) {
      console.log('dropping schema if exits...');
      conn.execute("DROP KEYSPACE CqlTest", function(err) {
        if (err) {
          // just log, ignore
          console.log(err);
        }
        console.log('create schema...');
        // one time schema set up
        conn.execute("CREATE KEYSPACE CqlTest WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1" , function(err) {
          if (err) {
            console.log(err);
            conn.close();
            return;
          }
          console.log('create cf...');
          conn.execute('USE CqlTest', function(err) {
            if (err) {
              console.log(err);
              conn.close();
              return;
            }
            conn.execute('CREATE COLUMNFAMILY Customer (KEY text PRIMARY KEY)', function(err) {
              if (err) {
                console.log(err);
                conn.close();
                return;
              }
              schemaSetUp = true;
              callback();
            });
          });
        });
      });
    } else {
      callback();
    }
  },
  'tearDown': function(callback) {
    conn.close();
    callback();
  },
  'test update': function(test) {
    conn.execute('USE CqlTest', function(err) {
      if (err) {
        console.log(err);
        test.fail();
      }
      conn.execute("UPDATE Customer SET firstname = 'John', lastname = 'Smith' WHERE KEY = 'jsmith'", function(err) {
        if (err) {
          console.log(err);
          test.fail();
        }
        test.ok(true);
        test.done();
      });
    });
  },
  'test select': function(test) {
    conn.execute('USE CqlTest', function(err) {
      if (err) {
        console.log(err);
        test.fail();
      }
      conn.execute('SELECT * FROM Customer', function(err, res) {
        if (err) {
          console.log(err);
          test.fail();
        }

        test.equal(res.length, 1);
        test.strictEqual(res[0].key, 'jsmith');
        test.equal(res[0].columns.length, 2);
        test.equal(res[0].columns[0].name, 'firstname');
        test.equal(res[0].columns[0].value, 'John');
        test.equal(res[0].columns[1].name, 'lastname');
        test.equal(res[0].columns[1].value, 'Smith');

        test.ok(true);
        test.done();
      });
    });
  }
});

