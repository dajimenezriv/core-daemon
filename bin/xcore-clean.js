'use strict';

const config = require('../lib/config/daemon');
const utils = require('../lib/utils');
const storjshare_clean = require('commander');

storjshare_clean
  .description('clean the running node specified')
  .option('-i, --nodeid <nodeid>', 'id of the running node')
  .option('-r, --remote <hostname:port>',
    'hostname and optional port of the daemon')
  .parse(process.argv);

if (!storjshare_clean.nodeid) {
  console.error('\n  missing node id, try --help');
  process.exit(1);
}

let port = config.daemonRpcPort;
let address = null;
if (storjshare_clean.remote) {
  address = storjshare_clean.remote.split(':')[0];
  if (storjshare_clean.remote.split(':').length > 1) {
    port = parseInt(storjshare_clean.remote.split(':')[1], 10);
  }
}

utils.connectToDaemon(port, function(rpc, sock) {
  rpc.clean(storjshare_clean.nodeid, (err) => {
    if (err) {
      console.error(`\n  cannot clean node, reason: ${err.message}`);
      return sock.end();
    }
    console.info(
      `\n  * share ${storjshare_clean.nodeid} clean process started`
    );
    return sock.end();
  });
}, address);
