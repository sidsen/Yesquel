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

#ifndef _SPLITTER_CLIENT_H
#define _SPLITTER_CLIENT_H

#include "stdafx.h"
#include <set>
#include <signal.h>

#include "../options.h"

#include "../gaia/gaiatypes.h"
#include "../gaia/newconfig.h"
#ifndef GAIAUDP
#include "../gaia/tcpdatagram-light.h"
#include "../gaia/grpctcp-light.h"
#include "../gaia/tcpdatagram.h"
#include "../gaia/grpctcp.h"
#else
#include "../gaia/udp.h"
#include "../gaia/udpfrag.h"
#include "../gaia/grpc.h"
#endif
#include "splitterrpcaux.h"

#define SS_NULL_RPCNO 0
#define SS_SHUTDOWN_RPCNO 1
#ifndef STORAGESERVER_SPLITTER
#define SS_SPLITNODE_RPCNO 2
#define SS_GETROWID_RPCNO 3
#else
#define SS_SPLITNODE_RPCNO 14
#define SS_GETROWID_RPCNO 15
#endif

#ifndef GAIAUDP
int CallSplitterConnect(void); // connects to splitter servers (TCP implementation)
                               // should be called before CallSplitter()
#endif

void CallSplitter(COid &coid, void *specificparm); // invokes splitter on coid
i64 GetRowidFromServer(Cid cid, i64 hint); // get a fresh rowid for a given cid

#define SPLITTER_THROTTLE_NMETRICS 3
class SplitterThrottle {
private:
  RWLock lock; // lock protecting the fields below
  SplitterLoadStatus Load; // this variable is used for debugging only
  unsigned Nelements, Nbytes; // this variable is used for debugging only
  int Delays[SPLITTER_THROTTLE_NMETRICS];
  u64 Expirations[SPLITTER_THROTTLE_NMETRICS];

  // computes delay and expiration due to various metrics
  static int computeDelayFromQueue(SplitterLoadStatus &load, u64& expiration);
  static int computeDelayFromTimeRetrying(SplitterLoadStatus &load, u64& expiration);
  static int computeDelayFromNodesize(unsigned nelements, unsigned nbytes, u64& expiration);
  
public:
  SplitterThrottle();
  void ReportLoad(SplitterLoadStatus &newload);
  void ReportNodeSize(unsigned nelements, unsigned nbytes);
  int getCurrentDelay(void);
};


#define MAXSPLITTERS 1024

// maps container id's to splitter servers
class SplitterDirectory {
private:
  ConfigState *Config;
  u8 Mysite;
  SplitterThrottle *Throttle;

  int GetSplitterServerNo(const COid& coid, int leaf); // return splitter server number

public:
  SplitterDirectory(ConfigState *cs, u8 mysite);
  ~SplitterDirectory(){ if (Throttle) delete [] Throttle; }
  void GetSplitterServer(const COid &coid, IPPort *dest, int isleaf); // get server of a given object id, for the local site
  //void GetSplitterServerFromId(IPPort id, UDPDest *res); // get UDPDest of a given server id
  SplitterThrottle *GetThrottle(COid& coid){ return &Throttle[GetSplitterServerNo(coid,1)]; }

  // functions below allow direct access to the set of splitter servers
  int GetNGroups(void);
  int GetNServers(int servergroup);
  void GetServerDirect(int servergroup, int serverno, IPPort *dest);
  int getNHosts(void){ return Config->Hosts.nitems; }
  HostConfig *getFirstHost(){ return Config->Hosts.getFirst(); }
  HostConfig *getLastHost(){ return Config->Hosts.getLast(); }
  HostConfig *getNextHost(HostConfig *hc){ return Config->Hosts.getNext(hc); }

#ifndef GAIAUDP
  // connect to splitter servers
  int connectSplitterServers(RPCClientLight *rpcc);
#endif
};

extern SplitterDirectory *SD;

#endif
