'use strict';

var FileStorageAdapter = require('../../../lib/storage/adapters/file');
var storj = require('storj-lib');
var expect = require('chai').expect;
var sinon = require('sinon');
var os = require('os');
var fs = require('fs');
var rimraf = require('rimraf');
var path = require('path');
var TMP_DIR = path.join(os.tmpdir(), 'STORJ_FILE_ADAPTER_TEST');
var mkdirp = require('mkdirp');

const utils = storj.utils;
const AuditStream = storj.AuditStream;
const Contract = storj.Contract;

function tmpdir() {
  return path.join(TMP_DIR, 'test-' + Date.now());
}

var store = null;
var hash = utils.ripemd160('test');
var audit = new AuditStream(12);
var contract = new Contract();
var item = new storj.StorageItem({
  hash: hash,
  shard: new Buffer.from('test')
});

describe('FileStorageAdapter', function() {

  before(function() {
    if (utils.existsSync(TMP_DIR)) {
      rimraf.sync(TMP_DIR);
    }
    mkdirp.sync(TMP_DIR);
    audit.end(Buffer.from('test'));
    store = new FileStorageAdapter(tmpdir());
  });

  describe('@constructor', function() {

    it('should create instance without the new keyword', function() {
      expect(
        FileStorageAdapter(tmpdir())
      ).to.be.instanceOf(FileStorageAdapter);
    });

  });

  describe('#_validatePath', function() {

    it('should not make a directory that already exists', function() {
      expect(function() {
        var tmp = tmpdir();
        mkdirp.sync(tmp);
        FileStorageAdapter.prototype._validatePath(tmp);
      }).to.not.throw(Error);
    });

  });

  describe('#_put', function() {

    it('should store the item', function(done) {
      item.contracts[hash] = contract;
      item.challenges[hash] = audit.getPrivateRecord();
      item.trees[hash] = audit.getPublicRecord();
      store._put(hash, item, function(err) {
        expect(err).equal(null);
        done();
      });
    });

    it('should bubble error if the underlying db#put fails', function(done) {
      var _writeFile = sinon.stub(fs, 'writeFile').callsArgWith(
        2,
        new Error('Failed')
      );
      store._put(hash, item, function(err) {
        expect(err.message).equal('Failed');
        _writeFile.restore();
        done();
      });
    });

  });

  describe('#_get', function() {

    it('should return the stored item', function(done) {
      store._get(hash, function(err, item) {
        expect(err).to.equal(null);
        expect(item).to.be.instanceOf(Object);
        done();
      });
    });

    it('should return error if the data is not found', function(done) {
      var _readFile = sinon.stub(fs, 'readFile').callsArgWith(
        2,
        new Error('Not found')
      );
      store._get(hash, function(err) {
        expect(err.message).to.equal('Not found');
        _readFile.restore();
        done();
      });
    });

    it('should bubble error from Btable#exists', function(done) {
      var _exists = sinon.stub(store._fs, 'exists').callsArgWith(
        1,
        new Error('Failed')
      );
      store._get(hash, function(err) {
        _exists.restore();
        expect(err.message).to.equal('Failed');
        done();
      });
    });

    it('should bubble errors from Btable#getReadStream', function(done) {
      var _exists = sinon.stub(store._fs, 'exists').callsArgWith(
        1,
        null,
        true
      );
      var _createReadStream = sinon.stub(
        store._fs,
        'createReadStream'
      ).callsArgWith(
        1,
        new Error('Failed')
      );
      store._get(hash, function(err) {
        _exists.restore();
        _createReadStream.restore();
        expect(err.message).to.equal('Failed');
        done();
      });
    });

  });

  describe('#_flush', function() {

    it('should cass underlying flush method', function(done) {
      var _flush = sinon.stub(store._fs, 'flush').callsArg(0);
      store._flush(function() {
        _flush.restore();
        expect(_flush.called).to.equal(true);
        done();
      });
    });

  });

  describe('#_peek', function() {

    it('should return the stored item', function(done) {
      store._peek(hash, function(err, item) {
        expect(err).to.equal(null);
        expect(item).to.be.instanceOf(Object);
        done();
      });
    });

    it('should return error if the data is not found', function(done) {
      var _readFile = sinon.stub(fs, 'readFile').callsArgWith(
        2,
        new Error('Not found')
      );
      store._peek(hash, function(err) {
        expect(err.message).to.equal('Not found');
        _readFile.restore();
        done();
      });
    });

  });

  describe('#_keys', function() {

    it('should stream all of the keys', function(done) {
      var keyStream = store._keys();
      keyStream.on('data', function(key) {
        expect(key.toString()).to.equal(
          '5e52fee47e6b070565f74372468cdc699de89107'
        );
      }).on('end', done);
    });

    it('should bubble error if read from directory', function(done) {
      var _readdir = sinon.stub(fs, 'readdir').callsArgWith(
        1,
        new Error('Failed')
      );
      var keyStream = store._keys();
      keyStream.on('data', function() {})
      .on('error', function(err) {
        _readdir.restore();
        expect(err.message).to.equal('Failed');
        done();
      });
    });

  });

  describe('#_del', function() {

    it('should return error if del fails', function(done) {
      var _unlink = sinon.stub(fs, 'unlink').callsArgWith(
        1,
        new Error('Failed to delete')
      );
      store._del(hash, function(err) {
        expect(err.message).to.equal('Failed to delete');
        _unlink.restore();
        done();
      });
    });

    it('should return error if unlink fails', function(done) {
      var _unlink = sinon.stub(store._fs, 'unlink').callsArgWith(
        1,
        new Error('Failed to delete')
      );
      store._del(hash, function(err) {
        expect(err.message).to.equal('Failed to delete');
        _unlink.restore();
        done();
      });
    });

    it('should delete the shard if it exists', function(done) {
      store._del(hash, function(err) {
        expect(err).to.equal(null);
        done();
      });
    });

    it('should return error if shard does not exists', function(done) {
      store._del(hash, function(err) {
        expect(err.message).to.contain('no such file or directory');
        done();
      });
    });

  });

  describe('#_size', function() {

    it('should return the size of the store on disk', function(done) {
      var _stat = sinon.stub(store._fs, 'stat').callsArgWith(
        0,
        null,
        [{sBucketStats: {size: 2 * 1024}}, {sBucketStats: {size: 3 * 1024}}]
      );
      store._isUsingDefaultBackend = true;
      store._size(function(err, shardsize) {
        _stat.restore();
        expect(shardsize).to.equal(5 * 1024);
        done();
      });
    });

    it('should bubble errors from Btable#stat', function(done) {
      var _stat = sinon.stub(store._fs, 'stat').callsArgWith(
        0,
        new Error('Failed')
      );
      store._size(function(err) {
        _stat.restore();
        expect(err.message).to.equal('Failed');
        done();
      });
    });

  });

  describe('#_open', function() {

    it('should callback null if already open', function(done) {
      store._open(function(err) {
        expect(err).to.equal(null);
        done();
      });
    });

  });

  describe('#_close', function() {

    it('should callback null if already closed', function(done) {
      store._isOpen = false;
      store._close(function(err) {
        expect(err).to.equal(null);
        done();
      });
    });

  });

});

after(function() {
  rimraf.sync(TMP_DIR);
});
