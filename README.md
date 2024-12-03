Core Daemon
==================
Daemon + CLI for farming data on the Internxt network, suitable for standalone
use / frameless environment or inclusion in other packages.

# Warning
This project is deprecated and Internxt does not develop here anymore. Only the TL;DR is kept updated. 

## TL;DR
### Installation

- Use Node 10.X.X and yarn for installing deps. 
- npm will fail to install the dependencies. 


#### MacOSX (using [Homebrew](https://brew.sh/))
```
brew install git python

wget -qO- https://raw.githubusercontent.com/creationix/nvm/v0.33.0/install.sh | bash

export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion

nvm install 10
npm i -g yarn

export STORJ_NETWORK=INXT

yarn --ignore-engines

```

#### Linux
```
sudo apt update
sudo apt install git python build-essential

wget -qO- https://raw.githubusercontent.com/creationix/nvm/v0.33.0/install.sh | bash

export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion

nvm install 10
npm i -g yarn

export STORJ_NETWORK=INXT

yarn --ignore-engines

```
## TL;DR SECTION ENDS












### Start core-daemon with old Core configuration

Make sure to close X Core before running xcore-daemon

```
xcore daemon
xcore start --config /home/user/.xcore/your_nodeid.json
```

### Create a new node

Parameters needed (examples):
* Wallet address (0x0000000000000000000000000000000000000000)
* Public IP (81.81.81.81)
* Public port (12345)
* Path of folder to share (/home/user/xcore)
* Size of storage to share (10GB)

```
xcore create --inxt 0x0000000000000000000000000000000000000000 --storage /home/user/xcore --size 10TB --rpcport 12345 --rpcaddress 81.81.81.81 --noedit
```

This command will only generate a new node configuration file on /home/user/.xcore/configs

Filename is [your node id].json

To start this new node, enter:

```
export STORJ_NETWORK=INXT
xcore daemon
xcore start --config /home/user/.xcore/configs/your_node_id.json
```

## Installation

Make sure you have the following prerequisites installed:

* Git
* Node.js LTS (8.x.x)
* NPM
* Python 2.7
* GCC/G++/Make

### Node.js + NPM

#### GNU+Linux & Mac OSX

```
wget -qO- https://raw.githubusercontent.com/creationix/nvm/v0.33.0/install.sh | bash
```

Close your shell and open an new one. Now that you can call the `nvm` program,
install Node.js (which comes with NPM):

```
nvm install --lts
```

#### Windows

Download [Node.js LTS](https://nodejs.org/en/download/) for Windows, launch the
installer and follow the setup instructions. Restart your PC, then test it from
the command prompt:

```
node --version
npm --version
```

### Build Dependencies

#### GNU+Linux

Debian based (like Ubuntu)
```
apt install git python build-essential
```

Red Hat / Centos
```
yum groupinstall 'Development Tools'
```
You might also find yourself lacking a C++11 compiler - [see this](https://hiltmon.com/blog/2015/08/09/c-plus-plus-11-on-centos-6-dot-6/)

#### Mac OSX

```
xcode-select --install
```

#### Windows

```
npm install --global windows-build-tools
```

---

### Install ###

Once build dependencies have been installed for your platform, install the
package globally using Node Package Manager:

```
npm install --global internxt/xcore-daemon
```

## Usage (CLI)

Once installed, you will have access to the `xcore` program, so start by
asking it for some help.

```
xcore --help

  Usage: xcore [options] [command]


  Commands:

    start       start a farming node
    stop        stop a farming node
    restart     restart a farming node
    status      check status of node(s)
    logs        tail the logs for a node
    create      create a new configuration
    destroy     kills the farming node
    killall     kills all shares and stops the daemon
    daemon      starts the daemon
    help [cmd]  display help for [cmd]

  Options:

    -h, --help     output usage information
    -V, --version  output the version number
```

You can also get more detailed help for a specific command.

```
xcore help create

  Usage: xcore-create [options]

  generates a new share configuration

  Options:

    -h, --help                 output usage information
    --inxt <addr>              specify the INXT address (required)
    --key <privkey>            specify the private key
    --storage <path>           specify the storage path
    --size <maxsize>           specify share size (ex: 10GB, 1TB)
    --rpcport <port>           specify the rpc port number
    --rpcaddress <addr>        specify the rpc address
    --maxtunnels <tunnels>     specify the max tunnels
    --tunnelportmin <port>     specify min gateway port
    --tunnelportmax <port>     specify max gateway port
    --manualforwarding         do not use nat traversal strategies
    --logdir <path>            specify the log directory
    --noedit                   do not open generated config in editor
    -o, --outfile <writepath>  write config to path
```

## Usage (Programmatic)

The X Core daemon uses a local [dnode](https://github.com/substack/dnode)
server to handle RPC message from the CLI and other applications. Assuming the
daemon is running, your program can communicate with it using this interface.
The example that follows is using Node.js, but dnode is implemented in many
[other languages](https://github.com/substack/dnode#dnode-in-other-languages).

```js
const dnode = require('dnode');
const daemon = dnode.connect(45015);

daemon.on('remote', (rpc) => {
  // rpc.start(configPath, callback);
  // rpc.stop(nodeId, callback);
  // rpc.restart(nodeId, callback);
  // rpc.status(callback);
  // rpc.destroy(nodeId, callback);
  // rpc.save(snapshotPath, callback);
  // rpc.load(snapshotPath, callback);
  // rpc.killall(callback);
});
```

You can also easily start the daemon from your program by creating a dnode
server and passing it an instance of the `RPC` class exposed from this package.

```js
const xcore = require('xcore-daemon');
const dnode = require('dnode');
const api = new xcore.RPC();

dnode(api.methods).listen(45015, '127.0.0.1');
```

## Configuring the Daemon

The X Core daemon loads configuration from anywhere the
[rc](https://www.npmjs.com/package/rc) package can read it. The first time you
run the daemon, it will create a directory in `$HOME/.xcore`, so
the simplest way to change the daemon's behavior is to create a file at
`$HOME/.xcore/config` containing the following:

```json
{
  "daemonRpcPort": 45015,
  "daemonRpcAddress": "127.0.0.1",
  "daemonLogFilePath": "",
  "daemonLogVerbosity": 3
}
```

Modify these parameters to your liking, see `example/daemon.config.json` for
detailed explanation of these properties.

## Debugging the Daemon

The daemon logs activity to the configured log file, which by default is
`$HOME/.xcore/logs/daemon.log`. However if you find yourself
needing to frequently restart the daemon and check the logs during
development, you can run the daemon as a foreground process for a tighter
feedback loop.

```
xcore killall
xcore daemon --foreground
```

## Connecting to a remote Daemon

**Note: Exposing your xcore-daemon to the Internet is a bad idea
as everybody could read your Private Key!**

To connect to a remote running daemon instance you will first need to
ensure this daemon is running on a different address than the default
`127.0.0.1`. This can be achieved by [configuring the Daemon](#configuring-the-daemon).

After your xcore-daemon is reachable (eg. within your home network)
you can use `-r` or `--remote` option (on supported commands) to use the
specified IP/hostname and port to connect to, instead of `127.0.0.1`.

**Note that this option does not support to start the xcore-daemon
on a different system, only connect to an already running one!**

Example to connect to remote daemon running on `192.168.0.10` on the default port (`45015`) and show the status:

```
xcore status --remote 192.168.0.10
```

If the port is changed, just append it like so:

```
xcore status --remote 192.168.0.10:51000
```

## License

X Core - Daemon + CLI for farming data on the Internxt network.  
Copyright (C) 2019 Internxt Universal Technologies Sociedad Limitada

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see http://www.gnu.org/licenses/.
