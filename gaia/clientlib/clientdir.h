/*
  
  Yesquel Storage Engine v0.1

  Copyright (c) Microsoft Corporation

  All rights reserved. 

  MIT License

  Permission is hereby granted, free of charge, to any person
  obtaining a copy of this software and associated documentation files
  (the ""Software""), to deal in the Software without restriction,
  including without limitation the rights to use, copy, modify, merge,
  publish, distribute, sublicense, and/or sell copies of the Software,
  and to permit persons to whom the Software is furnished to do so,
  subject to the following conditions:

  The above copyright notice and this permission notice shall be
  included in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
  BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
  ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
  CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.

*/

#ifndef _CLIENTDIR_H
#define _CLIENTDIR_H

#include "stdafx.h"

#include <set>
#include <signal.h>

#include "../gaiaoptions.h"
#include "../gaiatypes.h"
#include "../newconfig.h"

#if defined(GAIAUDP)
#include "../udp.h"
#include "../udpfrag.h"
#include "../grpc.h"
//#elif defined(TCPDATAGRAM_LIGHT)
#else
#include "../tcpdatagram-light.h"
#include "../grpctcp-light.h"
//#else
//#include "../tcpdatagram.h"
//#include "../grpctcp.h"
#endif

#include "../datastruct.h"

struct ServerInfo;

// maps object id's to servers
class ObjectDirectory {
private:
  ConfigState *Config;
  u8 Mysite;

public:
  // get server id of a given object id, for the local site
  IPPort GetServerId(COid& coid);

  Set<IPPort> *GetServerReplicasId(COid& coid);
  // returns server index of all replicas of a given object id.
  // returned value is dynamically allocated and should be freed by caller

  // get UDPDest of a given server id
  // void GetServerFromId(IPPort id, UDPDest *res);

  ObjectDirectory(ConfigState *cs, u8 mysite){ Config = cs; Mysite = mysite; }
};

// stores a storage configuration, indicating names of storage servers, etc
// It also includes the ObjectDirectory
class StorageConfig {
private:
  static void pingCallback(char *data, int len, void *callbackdata); // aux function: callback for shutdown rpc
  static void shutdownCallback(char *data, int len, void *callbackdata); // aux function: callback for shutdown rpc
  static void startsplitterServersCallback(char *data, int len, void *callbackdata);
  static void flushServersCallback(char *data, int len, void *callbackdata);
  static void loadServersCallback(char *data, int len, void *callbackdata);

public:
  ConfigState *CS;
  ObjectDirectory *Od;
  RPCClientLight Rpcc;

  // ping and wait for response once to each server (eg, to make sure they are all up)
  void pingServers(void);

  // the functions below invoke various RPCs on all servers to ask them to do various things
  void shutdownServers(void);       // shutdown
  void startsplitterServers(void);  // start splitter
  void flushServers(char *filename=0); // flush storage contents to a given filename or the default filename
  void loadServers(char *filename=0);  // load storage contents from a given filename or the default filename

   // read configuration from file
  StorageConfig(char *configfile, u8 mysite);
  ~StorageConfig(){
    if (CS){ delete CS; CS=0; }
    if (Od){ delete Od; Od=0; }
  }
};

#endif
