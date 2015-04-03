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

#include "../tmalloc.h"
#include "../debug.h"
#include "clientdir.h"
#include "../config.h"
#include "../gaiarpcaux.h"

StorageConfig::StorageConfig(char *configfile, u8 mysite){
  CS = ConfigState::ParseConfig(configfile);
  if (!CS) exit(1); // cannot read config file

  u32 myip = IPMisc::getMyIP(parser_cs->PreferredPrefix16);
  UniqueId::init(myip);

#ifndef GAIAUDP
  CS->connectHosts(&Rpcc);
#endif

  Od = new ObjectDirectory(CS, mysite);
}

struct pingCallbackData {
  Semaphore *sem;
  RWLock *lock;
  list<HostConfig*> *servers;
  list<HostConfig*>::iterator it;
};

void StorageConfig::pingCallback(char *data, int len, void *callbackdata){
  NullRPCRespData resp;
  pingCallbackData *pcd = (pingCallbackData *) callbackdata;
  if (data){ // RPC got response
    dprintf(2, "Ping: got a response");
    resp.demarshall(data); // now resp->data has return results of RPC

    // remove it from list
    pcd->lock->lock();
    pcd->servers->erase(pcd->it);
    pcd->lock->unlock();
  }
  pcd->sem->signal();
  delete pcd;
  return; // free return results of RPC
}

// ping and wait for response once to each server (eg, to make sure they are all up)
void StorageConfig::pingServers(void){
  NullRPCData *parm;
  HostConfig *hc;
  Semaphore sem;
  int i, count;
  list<HostConfig*> servers;
  list<HostConfig*>::iterator it;
  RWLock serversLock;
  pingCallbackData *pcd;

  // add all servers to server list
  for (hc = CS->Hosts.getFirst(); hc != CS->Hosts.getLast(); hc = CS->Hosts.getNext(hc)){
    servers.push_back(hc);
  }

  while (!servers.empty()){
    count = (int)servers.size();
    serversLock.lock();
    for (it = servers.begin(); it != servers.end(); ++it){
      hc = *it;
      printf("Pinging server %08x port %d\n", hc->ipport.ip, hc->ipport.port);
      parm = new NullRPCData;
      parm->data = new NullRPCParm;
      parm->data->reserved = 0;
      parm->freedata = true;
      pcd = new pingCallbackData;
      pcd->it = it;
      pcd->lock = &serversLock;
      pcd->sem = &sem;
      pcd->servers = &servers;
      Rpcc.AsyncRPC(hc->ipport, NULL_RPCNO, 0, parm, pingCallback, (void *) pcd);
    }
    serversLock.unlock();
    printf("Waiting for responses\n");
    for (i=0; i < count; ++i){
      // wait for responses for all issued RPCs
      sem.wait(INFINITE);
    }
  }
}

void StorageConfig::shutdownCallback(char *data, int len, void *callbackdata){
  ShutdownRPCRespData resp;
  Semaphore *sem = (Semaphore*) callbackdata;
  dprintf(2, "Shutdown: got a response");
  resp.demarshall(data); // now resp.data has return results of RPC
  sem->signal();
  return; // free return results of RPC
}

void StorageConfig::shutdownServers(void){
  ShutdownRPCData *parm;
  HostConfig *hc;
  Semaphore sem;
  int i, count;
  
  count = 0;
  for (hc = CS->Hosts.getFirst(); hc != CS->Hosts.getLast(); hc = CS->Hosts.getNext(hc)){
    ++count;
    printf("Shutting down server %08x port %d\n", hc->ipport.ip, hc->ipport.port);
    parm = new ShutdownRPCData;
    parm->data = new ShutdownRPCParm;
    parm->data->reserved = 0;
    parm->freedata = true;
    Rpcc.AsyncRPC(hc->ipport, SHUTDOWN_RPCNO, 0, parm, shutdownCallback, (void *) &sem);
  }
  printf("Waiting for responses\n");
  for (i=0; i < count; ++i){
    // wait for responses for all issued RPCs
    sem.wait(INFINITE);
  }
}

//---------------------------------------------------------------------------------------------------
void StorageConfig::startsplitterServersCallback(char *data, int len, void *callbackdata){
  StartSplitterRPCRespData resp;
  Semaphore *sem = (Semaphore*) callbackdata;
  dprintf(2, "Start splitter: got a response");
  resp.demarshall(data); // now resp.data has return results of RPC
  sem->signal();
  return; // free return results of RPC
}

void StorageConfig::startsplitterServers(void){
  StartSplitterRPCData *parm;
  HostConfig *hc;
  Semaphore sem;
  int i, count;
  
  count = 0;
  for (hc = CS->Hosts.getFirst(); hc != CS->Hosts.getLast(); hc = CS->Hosts.getNext(hc)){
    ++count;
    printf("Starting splitter on server %08x port %d\n", hc->ipport.ip, hc->ipport.port);
    parm = new StartSplitterRPCData;
    parm->data = new StartSplitterRPCParm;
    parm->data->reserved = 0;
    parm->freedata = true;
    Rpcc.AsyncRPC(hc->ipport, STARTSPLITTER_RPCNO, 0, parm, startsplitterServersCallback, (void *) &sem);
  }
  printf("Waiting for responses\n");
  for (i=0; i < count; ++i){
    // wait for responses for all issued RPCs
    sem.wait(INFINITE);
  }
}

void StorageConfig::flushServersCallback(char *data, int len, void *callbackdata){
  FlushFileRPCRespData resp;
  Semaphore *sem = (Semaphore*) callbackdata;
  dprintf(2, "Save server: got a response");
  resp.demarshall(data); // now resp.data has return results of RPC
  if (resp.data->status != 0)
    printf("Got error from server\n");
  sem->signal();
  return; // free return results of RPC
}

void StorageConfig::flushServers(char *filename){
  FlushFileRPCData *parm;
  HostConfig *hc;
  Semaphore sem;
  int i, count, len;

  if (!filename) filename = "";
  
  count = 0;
  for (hc = CS->Hosts.getFirst(); hc != CS->Hosts.getLast(); hc = CS->Hosts.getNext(hc)){
    ++count;
    printf("Saving server %08x port %d\n", hc->ipport.ip, hc->ipport.port);
    parm = new FlushFileRPCData;
    parm->data = new FlushFileRPCParm;
    len = (int)strlen(filename);
    parm->data->filename = new char[len+1];
    strcpy(parm->data->filename, filename);
    parm->data->filenamelen = len+1;
    parm->freedata = true;
    parm->freefilenamebuf = parm->data->filename;

    Rpcc.AsyncRPC(hc->ipport, FLUSHFILE_RPCNO, 0, parm, flushServersCallback, (void *) &sem);
  }
  printf("Waiting for responses\n");
  for (i=0; i < count; ++i){
    // wait for responses for all issued RPCs
    sem.wait(INFINITE);
  }
}

void StorageConfig::loadServersCallback(char *data, int len, void *callbackdata){
  LoadFileRPCRespData resp;
  Semaphore *sem = (Semaphore*) callbackdata;
  dprintf(2, "Load server: got a response");
  resp.demarshall(data); // now resp.data has return results of RPC
  if (resp.data->status != 0)
    printf("Got error from server\n");
  sem->signal();
  return; // free return results of RPC
}

void StorageConfig::loadServers(char *filename){
  LoadFileRPCData *parm;
  HostConfig *hc;
  Semaphore sem;
  int i, count, len;

  if (!filename) filename = "";
  
  count = 0;
  for (hc = CS->Hosts.getFirst(); hc != CS->Hosts.getLast(); hc = CS->Hosts.getNext(hc)){
    ++count;
    printf("Loading server %08x port %d\n", hc->ipport.ip, hc->ipport.port);
    parm = new LoadFileRPCData;
    parm->data = new LoadFileRPCParm;
    len = (int)strlen(filename);
    parm->data->filename = new char[len+1];
    strcpy(parm->data->filename, filename);
    parm->data->filenamelen = len+1;
    parm->freedata = true;
    parm->freefilenamebuf = parm->data->filename;
    Rpcc.AsyncRPC(hc->ipport, LOADFILE_RPCNO, 0, parm, loadServersCallback, (void *) &sem);
  }
  dprintf(1, "Waiting for responses");
  for (i=0; i < count; ++i){
    // wait for responses for all issued RPCs
    sem.wait(INFINITE);
  }
}

//---------------------------------------------------------------------------------------------------

static inline unsigned GetServergroup(Cid cid, int ngroups, int method, int parm){
  switch(method){
  case 0:
    return ((u32)(cid >> parm))%ngroups;
  default: assert(0);
  }
  return 0;
}

static inline unsigned GetServerNumber(Oid oid, int nservers, int method, int parm){
  switch(method){
  case 0:
    return ((u32)(oid >> parm))%nservers;
  default: assert(0);
  }
  return 0;
}

IPPort ObjectDirectory::GetServerId(COid& coid){
  int servergroup;
  unsigned serverno;
  GroupConfig *gc;

  // get server group
  servergroup = GetServergroup(coid.cid, Config->Ngroups, Config->ContainerAssignMethod, Config->ContainerAssignParm);

  // get configuration for that server
  gc = Config->Groups[servergroup]; assert(gc);

  // now get server number
  serverno = GetServerNumber(coid.oid, gc->nservers, gc->stripeMethod, gc->stripeParm);

  // add site number
  serverno |= (Mysite << 24);

  // finally, get server id
  //return gc->Servers[serverno];
  return gc->Servers[serverno]->ipport;
}

Set<IPPort> *ObjectDirectory::GetServerReplicasId(COid& coid){
  int servergroup;
  unsigned serverno;
  GroupConfig *gc;
  u8 site;

  Set<IPPort> *retval;
  retval = new Set<IPPort>; assert(retval);

  // get server group
  servergroup = GetServergroup(coid.cid, Config->Ngroups, Config->ContainerAssignMethod, Config->ContainerAssignParm);

  // get configuration for that server
  gc = Config->Groups[servergroup]; assert(gc);

  // now get server number
  serverno = GetServerNumber(coid.oid, gc->nservers, gc->stripeMethod, gc->stripeParm);

  // for each site...
  for (site=0; site < gc->nsites; ++site){
    retval->insert(gc->Servers[serverno | (site << 24)]->ipport);
  }

  return retval;
}

//void ObjectDirectory::GetServerFromId(IPPort id, UDPDest *res){
//  u32 ip = id.ip;
//  res->destaddr.sin_family = AF_INET;
//  res->destaddr.sin_port = id.port;
//  res->sockaddr_len=16;
//  memcpy((void*)&res->destaddr.sin_addr, (void*)&ip, 4);
//  return;
//}
