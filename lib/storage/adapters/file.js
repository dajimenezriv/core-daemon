'use strict';

var inherits = require('util').inherits;
var storj = require('storj-lib');
var kfs = require('kfs');
var path = require('path');
var assert = require('assert');
var fs = require('fs');
var stream = require('stream');
var mkdirp = require('mkdirp');

const StorageAdapter = storj.StorageAdapter;
const utils = storj.utils;

/**
 * Implements a file storage adapter interface
 * @extends {StorageAdapter}
 * @param {String} storageDirPath - Path to store the files
 * @constructor
 * @license AGPL-3.0
 */
function FileStorageAdapter(storageDirPath) {
  if (!(this instanceof FileStorageAdapter)) {
    return new FileStorageAdapter(storageDirPath);
  }

  this._validatePath(storageDirPath);
  this._fs = kfs(path.join(storageDirPath, 'sharddata.kfs'));
  
  this._path = path.join(storageDirPath, 'db');
  this._validatePath(this._path);

  this._isOpen = true;
}

inherits(FileStorageAdapter, StorageAdapter);

/**
 * Validates the storage path supplied
 * @private
 */
FileStorageAdapter.prototype._validatePath = function(storageDirPath) {
  if (!utils.existsSync(storageDirPath)) {
    mkdirp.sync(storageDirPath);
  }

  assert(utils.isDirectory(storageDirPath), 'Invalid directory path supplied');
};

/**
 * Implements the abstract {@link StorageAdapter#_get}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
FileStorageAdapter.prototype._get = function(key, callback) {
  var self = this;

  var filePath = path.join(this._path, key);

  fs.readFile(filePath, 'utf8', function(err, data) {
    if (err) {
      return callback(err);
    }

    var result = JSON.parse(data);
    var fskey = result.fskey || key;

    self._fs.exists(fskey, function(err, exists) {
      if (err) {
        return callback(err);
      }

      function _getShardStreamPointer(callback) {
        var getStream = exists ?
                        self._fs.createReadStream.bind(self._fs) :
                        self._fs.createWriteStream.bind(self._fs);
        if (!exists) {
          fskey = utils.ripemd160(key, 'hex');
          result.fskey = fskey;
        }
        getStream(fskey, function(err, stream) {
          if (err) {
            return callback(err);
          }

          result.shard = stream;

          callback(null, result);
        });
      }

      _getShardStreamPointer(callback);
    });
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_peek}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
FileStorageAdapter.prototype._peek = function(key, callback) {
  var filePath = path.join(this._path, key);

  fs.readFile(filePath, 'utf8', function(err, data) {
    if (err) {
      return callback(err);
    }

    callback(null, JSON.parse(data));
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_put}
 * @private
 * @param {String} key
 * @param {Object} item
 * @param {Function} callback
 */
FileStorageAdapter.prototype._put = function(key, item, callback) {
  item.shard = null; // NB: Don't store any shard data here

  item.fskey = utils.ripemd160(key, 'hex');

  var filePath = path.join(this._path, key);

  fs.writeFile(filePath, JSON.stringify(item), function(err) {
    if (err) {
      return callback(err);
    }

    callback(null);
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_del}
 * @private
 * @param {String} key
 * @param {Function} callback
 */
FileStorageAdapter.prototype._del = function(key, callback) {
  var self = this;
  var fskey = key;

  self._peek(key, function(err, item) {
    if (!err && item.fskey) {
      fskey = item.fskey;
    }

    // I've changed the order of self._fs.unlink and fs.unlink
    // from embedded.js since I assume it's better to have the
    // inconsistency in the file system that in the KFS
    self._fs.unlink(fskey, function(err) {
      if (err) {
        return callback(err);
      }

      const filePath = path.join(self._path, key);

      fs.unlink(filePath, function(err) {
        if (err) {
          return callback(err);
        }

        callback(null);
      });
    });
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_flush}
 * @private
 * @param {Function} callback
 */
FileStorageAdapter.prototype._flush = function(callback) {
  this._fs.flush(callback);
};

/**
 * Implements the abstract {@link StorageAdapter#_size}
 * @private
 * @param {String} [key]
 * @param {Function} callback
 */
FileStorageAdapter.prototype._size = function(key, callback) {
  var self = this;

  if (typeof key === 'function') {
    callback = key;
    key = null;
  }

  function handleStatResults(err, stats) {
    if (err) {
      return callback(err);
    }

    var kfsUsedSpace = stats.reduce(function(stat1, stat2) {
      return {
        sBucketStats: {
          size: stat1.sBucketStats.size + stat2.sBucketStats.size
        }
      };
    }, {
      sBucketStats: { size: 0 }
    }).sBucketStats.size;

    callback(null, kfsUsedSpace);
  }

  if (key) {
    self._fs.stat(utils.ripemd160(key, 'hex'), handleStatResults);
  } else {
    self._fs.stat(handleStatResults);
  }
};

/**
 * Implements the abstract {@link StorageAdapter#_keys}
 * @private
 * @returns {ReadableStream}
 */
FileStorageAdapter.prototype._keys = function() {
  var path = this._path;

  return new stream.Readable({
    objectMode: true,
    read() {
      var self = this;

      fs.readdir(path, function(err, files) {
        if (err) {
          self.destroy(err);
        } else {
          for (const file of files) {
            self.push(file);
          }
        }

        self.push(null);
      });
    }
  });
};

/**
 * Implements the abstract {@link StorageAdapter#_open}
 * @private
 * @param {Function} callback
 */
FileStorageAdapter.prototype._open = function(callback) {
  this._isOpen = true;
  callback(null);
};

/**
 * Implements the abstract {@link StorageAdapter#_close}
 * @private
 * @param {Function} callback
 */
FileStorageAdapter.prototype._close = function(callback) {
  this._isOpen = false;
  callback(null);
};

module.exports = FileStorageAdapter;
