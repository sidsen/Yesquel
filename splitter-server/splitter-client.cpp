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

#include "stdafx.h"
#include "../gaia/tmalloc.h"
#include "../options.h"
#include "../gaia/debug.h"
#include "splitter-client.h"
#include "../gaia/config.h"
#include "../gaia/gaiarpcaux.h"
#include "../gaia/clientlib/clientdir.h"
#include "splitter-server.h"
#include "splitterrpcaux.h"
#include "../dtreeaux.h"

extern SplitterDirectory *SD;
extern StorageConfig *SC;

// computes delay and expiration due to a node that is too large
int SplitterThrottle::computeDelayFromQueue(SplitterLoadStatus &load, u64& expiration){
  int duration=0;
  int delay=0;
  int m;
  //m = load.splitQueueSize/10;
  //if (m >= 1){ // kick in when queue size is 10 or more
  //  // at size 10 add 1ms, at size 100 add 1s, increasing exponentially between these
  //  if (m < 10) delay = 1<<m;
  //  else delay = 1024; // capped at 1024ms
  //  duration = (int) ceil(load.splitTimeAvg * load.splitQueueSize/2);
  //}
  //expiration = Time::now() + duration;
  //return delay;
  m = load.splitQueueSize/10;
  if (m >= 5){ // kick in when queue size is 50 or more
    m -= 4;
    // at size 50 add 1ms, at size 500 add 1s, increasing exponentially between these
    if (m < 10) delay = 1<<m;
    else delay = 1024; // capped at 1024ms
    duration = (int) ceil(load.splitTimeAvg * load.splitQueueSize/2);
  }
  expiration = Time::now() + duration;
  return delay;
}

int SplitterThrottle::computeDelayFromTimeRetrying(SplitterLoadStatus &load, u64& expiration){
  int m;
  int delay=0;
  int duration=0;
  //m = (int)(load.splitTimeRetryingMs/100);
  //if (m >= 1){ // kick in at 100ms
  //  m = m/100;  // at 100ms add 1ms, at 10000ms add 1s
  //  if (m < 10) delay = 1<<m;
  //  else delay = 1024;  // capped at 1024ms
  //  duration = delay*2;
  //  //if (duration < 1000) duration=1000;
  //}
  //expiration = Time::now() + duration;
  //return delay;
  m = (int)(load.splitTimeRetryingMs/100);
  if (m >= 5){ // kick in at 500ms
    m -= 4;
    m = m/100;  // at 500ms add 1ms, at 50000ms add 1s
    if (m < 10) delay = 1<<m;
    else delay = 1024;  // capped at 1024ms
    duration = delay*2;
    //if (duration < 1000) duration=1000;
  }
  expiration = Time::now() + duration;
  return delay;
}

// Currently, this is not enabled since no place in the code reports the node size.
// To report the node size, add this statement:
//     SD->GetThrottle(coid)->ReportNodeSize(node.Ncells(), node.CellsSize());
// The problem is that we should report a real node size at the storage server, not the node
// size within a transaction, as the latter can grow very quickly beyond the limit
int SplitterThrottle::computeDelayFromNodesize(unsigned nelements, unsigned nbytes, u64& expiration){
  int m;
  int delayelements=0;
  int delaybytes=0;
  int delay;
  int duration;
  m = nelements/DTREE_SPLIT_SIZE;
  if (m >= 2){ // kick in at twice the expected number of elements
    if (m < 12) delayelements = 1<<(m-2); // at 2x add 1ms, at 12x add 1s, increasing exponentially between these
    else delayelements = 1024; // capped at 1024ms
  }
  m = nbytes/DTREE_SPLIT_SIZE_BYTES;
  if (m >= 2){ // kick in at twice the expected size
    if (m < 12) delaybytes = 1<<(m-2); // at 2x add 1ms, at 12x add 1s, increasing exponentially between these
    else delaybytes = 1024; // capped at 1024ms
  }
  // pick max delay due to node size or node bytes
  if (delayelements > delaybytes) delay=delayelements;
  else delay = delaybytes;

  duration = delay*2; 
  //if (duration < 1000 && delay > 0) duration=1000; // ...but at least one second
  expiration = Time::now() + duration;
  return delay;
}

SplitterThrottle::SplitterThrottle(){
  int i;
  for (i=0; i < SPLITTER_THROTTLE_NMETRICS; ++i){
    Delays[i] = 0;
    Expirations[i] = 0;
  }
}

void SplitterThrottle::ReportLoad(SplitterLoadStatus &newload){
  int delay0, delay1;
  u64 expiration0, expiration1;
  Load = newload;
  delay0 = computeDelayFromQueue(newload, expiration0);
  delay1 = computeDelayFromTimeRetrying(newload, expiration1);
  lock.lock();
  Delays[0] = delay0;
  Delays[1] = delay1;
  Expirations[0] = expiration0;
  Expirations[1] = expiration1;
  lock.unlock();
  // debugging printfs
  //u64 now = Time::now();
  //printf("ReportLoad queuesize %d ave %f retrying %d\n", newload.splitQueueSize, newload.splitTimeAvg, newload.splitTimeRetryingMs);
  //printf("Delay %d %d Exp %d %d\n", Delays[0], Delays[1], Expirations[0]>now ? Expirations[0]-now : 0, Expirations[1]>now ? Expirations[1]-now : 0);
}

void SplitterThrottle::ReportNodeSize(unsigned nelements, unsigned nbytes){
  int delay;
  u64 expiration;
  Nelements = nelements;
  Nbytes = nbytes;
  delay = computeDelayFromNodesize(nelements, nbytes, expiration);
  lock.lock();
  if (Expirations[2] < Time::now() || Delays[2] < delay){
    Expirations[2] = expiration;
    Delays[2] = delay;
  }
  lock.unlock();
  // debugging printfs
  //u64 now = Time::now();
  //printf("ReportLoad Nelements %d Nbytes %d\n", nelements, nbytes);
  //printf("Delay %d Exp %d\n", Delays[2], Expirations[2]>now ? Expirations[2]-now : 0);
}

int SplitterThrottle::getCurrentDelay(void){
  int delay=0;
  u64 now;
  int i, iwin;
  now = Time::now();
  lock.lockRead();
  iwin=0;
  for (i=0; i < SPLITTER_THROTTLE_NMETRICS; ++i){
    if (Expirations[i] > now){
      if (delay < Delays[i]){ delay = Delays[i]; iwin=i; }
    }
  }
  lock.unlockRead();
  
  if (delay){ dprintf(1, "D%d,%d ", delay,iwin); }
  return delay;
}

SplitterDirectory::SplitterDirectory(ConfigState *cs, u8 mysite){
  Config = cs;
  Mysite = mysite;
  Throttle = new SplitterThrottle[MAXSPLITTERS]; // only one splitter at the moment
}

static inline unsigned GetServergroup(Cid cid, int ngroups, int method, int parm){
  switch(method){
  case 0:
    return ((u32)(cid >> parm))%ngroups;
  default: assert(0);
  }
  return 0;
}


#if !defined(ALT_MULTI_SPLITTER) && !defined(MULTI_SPLITTER)
static inline unsigned GetServerNumber(const COid &coid, int nservers, int method, int parm, int leaf){
  switch(method){
  case 0:
    //return ((u32)(oid >> parm))%nservers;
    return ((u32)(coid.cid))%nservers;
  default: assert(0);
  return 0;
}
#elif defined(MULTI_SPLITTER)
static inline unsigned GetServerNumber(const COid &coid, int nservers, int method, int parm, int leaf){
  if (!leaf){
    switch(method){
    case 0:
      //return ((u32)(oid >> parm))%nservers;
      return ((u32)(coid.cid))%nservers;
    default: assert(0);
    }
  } else { // leaf gets multiple splitters
    switch(method){
    case 0:
      //return ((u32)(oid >> parm))%nservers;
      return ((u32)(coid.cid ^ coid.oid))%nservers;
    default: assert(0);
    }
  }
  return 0;
}
#elif defined(ALT_MULTI_SPLITTER)
static inline unsigned GetServerNumber(const COid &coid, int nservers, int method, int parm, int leaf){
    switch(method){
    case 0:
      //return ((u32)(oid >> parm))%nservers;
      return ((u32)(coid.oid))%nservers;
    default: assert(0);
    }
  return 0;
}
#endif



int SplitterDirectory::GetSplitterServerNo(const COid& coid, int leaf){
  int servergroup;
  unsigned serverno;
  GroupConfig *gc;

  // get server group
  servergroup = GetServergroup(coid.cid, Config->Ngroups, Config->ContainerAssignMethod, Config->ContainerAssignParm);

  // get configuration for that server
  gc = Config->Groups[servergroup]; assert(gc);

  // now get server number
  serverno = GetServerNumber(coid, gc->nservers, gc->stripeMethod, gc->stripeParm, leaf);
  return serverno;
}

// get server of a given object id
void SplitterDirectory::GetSplitterServer(const COid &coid, IPPort *dest, int isleaf){
  int servergroup;
  unsigned serverno;
  GroupConfig *gc;
  IPPort ipport;

  // get server group
  servergroup = GetServergroup(coid.cid, Config->Ngroups, Config->ContainerAssignMethod, Config->ContainerAssignParm);

  // get configuration for that server
  gc = Config->Groups[servergroup]; assert(gc);

  // now get server number
  serverno = GetServerNumber(coid, gc->nservers, gc->stripeMethod, gc->stripeParm, isleaf);

  // add site number
  serverno |= (Mysite << 24);

  // finally, get server id
  //return gc->Servers[serverno];
  ipport = gc->Servers[serverno]->ipport;
  // create ipport with a port SPLITTER_PORT_OFFSET greater
  ipport.port = htons(ntohs(ipport.port) + SPLITTER_PORT_OFFSET);
  *dest = ipport;
}

//void SplitterDirectory::GetSplitterServerFromId(IPPort id, UDPDest *res){
//  u32 ip = id.ip;
//  res->destaddr.sin_family = AF_INET;
//  res->destaddr.sin_port = id.port;
//  res->sockaddr_len=16;
//  memcpy((void*)&res->destaddr.sin_addr, (void*)&ip, 4);
//  return;
//}

int SplitterDirectory::GetNGroups(void){ return Config->Ngroups; }
int SplitterDirectory::GetNServers(int servergroup){
  assert(0 <= servergroup && servergroup < Config->Ngroups);
  GroupConfig *gc = Config->Groups[servergroup]; assert(gc);
  // **!** TO DO: take site into consideration, if supporting multiple sites
  return gc->nservers;
}
void SplitterDirectory::GetServerDirect(int servergroup, int serverno, IPPort *dest){
  assert(0 <= servergroup && servergroup < Config->Ngroups);
  GroupConfig *gc = Config->Groups[servergroup]; assert(gc);
  assert(0 <= serverno && serverno < gc->nservers);
  IPPort ipport = gc->Servers[serverno]->ipport;
  // create ipport with a port SPLITTER_PORT_OFFSET greater
  ipport.port = htons(ntohs(ipport.port) + SPLITTER_PORT_OFFSET);
  *dest = ipport;
}

#ifndef GAIAUDP
int SplitterDirectory::connectSplitterServers(RPCClientLight *rpcc){
  int res;
  HostConfig *hc;
  UDPDest udpdest;
  IPPort ipportdest;
  int err;

  err=0;
  hc = Config->Hosts.getFirst(); // connect to first host
  assert(hc != Config->Hosts.getLast());
  u32 ip = hc->ipport.ip;
  udpdest.destaddr.sin_family = AF_INET;
  udpdest.destaddr.sin_port = htons(ntohs(hc->ipport.port) + SPLITTER_PORT_OFFSET);
  udpdest.sockaddr_len=16;
  memcpy((void*)&udpdest.destaddr.sin_addr, (void*)&ip, 4);
  ipportdest = udpdest.getIPPort();

  res=rpcc->clientconnect(ipportdest);
  if (res){
    err=1;
    printf("Cannot connect to server at %08x\n", hc->ipport.ip); 
  }
  return err;
}

int CallSplitterConnect(void){
  return SD->connectSplitterServers(&SC->Rpcc);
}
#endif

#if (DTREE_SPLIT_LOCATION==1)
void CallSplitter(COid &coid, void *specificparm){ assert(0); } // should not be called
#elif (DTREE_SPLIT_LOCATION==2)
extern RecentSplitsNew *RecentSplitsTracker;

void CallSplitterCallBack(char *data, int len, void *callbackdata){
  SplitnodeRPCRespData resp;
  assert(SD);
  if (data){
    resp.demarshall(data);
    if (resp.data->status==0){
      SD->GetThrottle(resp.data->coid)->ReportLoad(resp.data->load); // update splitter load information
      RecentSplitsTracker->reportduration(resp.data->coid, (int) (resp.data->load.splitQueueSize*resp.data->load.splitTimeAvg));
    }
  }
  return; // free buffer
}

void CallSplitter(COid &coid, void *specificparm){
  IPPort dest;
  SplitnodeRPCData *parm;
  int isleaf = (int) specificparm;

  if (!SC){
    printf("Error: need to split but splitter not started\n");
    return;
  }

  assert(SD);

  SD->GetSplitterServer(coid, &dest, isleaf);
  //printf("Invoking split at server %08x port %d\n", hc->ipport.ip, hc->ipport.port); fflush(stdout);
  parm = new SplitnodeRPCData;
  parm->data = new SplitnodeRPCParm;
  parm->data->getstatusonly = 0;
  parm->data->coid = coid;
  parm->data->wait = 0;
  parm->freedata = true;
  SC->Rpcc.AsyncRPC(dest, SS_SPLITNODE_RPCNO, FLAG_HID((u32)coid.oid), parm, CallSplitterCallBack, 0);
}
#elif (DTREE_SPLIT_LOCATION==3)
extern RecentSplitsNew *RecentSplitsTracker;

void CallSplitter(COid &coid, void *specificparm){
  UDPDest dest;
  SplitnodeRPCData *parm;
  char *resp;
  SplitnodeRPCRespData rpcresp;
  int isleaf = (int) specificparm;

  assert(SD);

  SD->GetSplitterServer(coid, &dest);
  //printf("Invoking split at server %08x port %d\n", hc->ipport.ip, hc->ipport.port); fflush(stdout);
  parm = new SplitnodeRPCData;
  parm->data = new SplitnodeRPCParm;
  parm->data->getstatusonly = 0;
  parm->data->coid = coid;
  parm->data->wait = 1;
  parm->freedata = true;
  resp = SC->Rpcc.SyncRPC(&dest, SS_SPLITNODE_RPCNO, parm, 0);

  if (!resp) return;
  rpcresp.demarshall(resp);
  if (rpcresp.data->status==0){
    SD->GetThrottle(rpcresp.data->coid)->ReportLoad(rpcresp.data->load); // update splitter load information
    RecentSplitsTracker->reportduration(rpcresp.data->coid, (int) (rpcresp.data->load.splitQueueSize*rpcresp.data->load.splitTimeAvg));
  }
}
#endif

i64 GetRowidLocal(Cid cid, i64 hint){
  static SkipList<U64,i64> KnownRowids;
  i64 *value, retval;
  U64 cidu64(cid);
  int res;

  if (hint){
    res = KnownRowids.lookupInsert(cidu64, value);
    if (!res) retval = ++(*value); // found
    else retval = *value = hint;   // not found, use hint and remember it
  } else {
    res = KnownRowids.lookup(cidu64, value);
    if (!res) retval = ++(*value); // found
    else retval = 0;
  }
  return retval;
}

i64 GetRowidFromServer(Cid cid, i64 hint){
  IPPort dest;
  GetRowidRPCData *parm;
  GetRowidRPCRespData rpcresp;
  char *resp;
  i64 rowid;
  COid fakecoid;

  assert(SD);

  if (cid >> 48 & EPHEMDB_CID_BIT) return GetRowidLocal(cid, hint);  // local container

  fakecoid.cid = cid;
  fakecoid.oid = 0;    // use oid 0 to determine what server will handle getrowid requests for a given cid

  SD->GetSplitterServer(fakecoid, &dest, 0);
  //printf("Invoking split at server %08x port %d\n", hc->ipport.ip, hc->ipport.port); fflush(stdout);
  parm = new GetRowidRPCData;
  parm->data = new GetRowidRPCParm;
  parm->data->cid = cid;
  parm->data->hint = hint;
  parm->freedata = true;
  resp = SC->Rpcc.SyncRPC(dest, SS_GETROWID_RPCNO, FLAG_HID((u32)cid),parm);

  if (!resp) return 0;
  rpcresp.demarshall(resp);
  rowid = rpcresp.data->rowid;

  free(resp);
  return rowid;
}

