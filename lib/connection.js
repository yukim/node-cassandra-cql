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
var util = require('util'),
    thrift = require('thrift'),
    Cassandra = require('../gen-nodejs/Cassandra'),
    ttypes = require('../gen-nodejs/cassandra_types');

/**
 * @constructor
 */
var Connection = module.exports.Connection = function(options) {

  var defaults = {
    'host': 'localhost',
    'port': 9160,
    'keyspace': null,
    'credential': null
  };

  var _options = options || {};
  for (var prop in defaults) {
    this[prop] = _options[prop] || defaults[prop];
  }

  this.ready = false;

  this.queue = [];

  this.default_compression = ttypes.Compression.NONE;//GZIP;

  var self = this;

  this.connection = thrift.createConnection(this.host, this.port);
  this.connection.on('error', function(err) {
    this.emit('error', err);
  });
  this.connection.on('connect', function(err) {
    if (err) {
      return self.emit('error', err);
    }

    // if credential is given, then call login
    if (self.credential) {
      self.client.login(
        new ttype.AuthenticationRequest(self.credential), function(err) {
          if (err) {
            return self.emit('error', err);
          }

          // only when login is success, emit connected event
          self.ready = true;
          self.dispatch();
        });
    } else {
      self.ready = true;
      self.dispatch();
    }
  });

  // if keyspace is specified, use that ks
  if (this.keyspace) {
    this.execute('Use ' + this.keyspace, function(err) {
      if (err) {
        self.emit('error', err);
      }
    });
  };

  this.client = thrift.createClient(Cassandra, this.connection);
};
util.inherits(Connection, process.EventEmitter);

/**
 * Executes CQL query.
 *
 * @api {public}
 */
Connection.prototype.execute = function(query, callback) {
  var args = Array.prototype.slice.call(arguments);
  if (!this.ready) {
    // queue until ready
    this.queue.push([arguments.callee, args]);
    return;
  }

  // compression is a Compression object
  this.client.execute_cql_query(query, this.default_compression, function(err, res) {
      // err is either InvalidRequestException, UnavailableException,
      // TimedOutException, or SchemaDisagreementException
      if (err) {
        return callback(err);
      }
      
      // res is CqlResult object
      // CqlResult object has CqlResultType, list of CqlRow, and number of rows
      if (res.type === ttypes.CqlResultType.ROWS) {
        // rows
        callback(null, res.rows);
      } else if (res.type === ttypes.CqlResultType.INT) {
        // count
        callback(null, res.num);
      } else {
        // void
        callback(null, null);
      }
  });
};

/**
 * Close connection.
 *
 * @api [public}
 */
Connection.prototype.close = function() {
  this.connection.end();
};

/**
 * @api {private}
 */
Connection.prototype.dispatch = function() {
  if (this.ready) {
    if (this.queue.length > 0) {
      var next = this.queue.shift();
      next[0].apply(this, next[1]);
      this.dispatch();
    }
  }
};
