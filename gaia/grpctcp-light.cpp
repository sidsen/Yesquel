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

// rpc.cpp

#include "stdafx.h"
#include "tmalloc.h"
#include "debug.h"
#include "grpctcp-light.h"
#include "ipmisc.h"
#include "scheduler.h"

void RPCClientLight::handleMsg(IPPort *dest, u32 req, u32 xid, u32 flags, char *data, int len){
  OutstandingRPCLight *orpc;

  orpc = RequestLookupAndDelete(xid);
  if (orpc && orpc->callback){
    orpc->callback(data, len, orpc->callbackdata);
  }
  if (orpc){
    // **!** added code after removing EfficientRetransmit
    delete orpc->dmsg.data;
    delete orpc;
  }
  // free buffer
  TCPDatagramCommunicationLight::freeIncomingBuf(data);

  return;
}

OutstandingRPCLight *RPCClientLight::RequestLookupAndDelete(u32 xid){
  U32 Xid(xid);
  int res;
  OutstandingRPCLight *it=0;
  res = OutstandingRequests.lookupRemove(Xid, 0, it);
  if (!res){ // found it
    it->done = true; // mark as done
  }
  return it;
}

RPCClientLight::RPCClientLight() :
  OutstandingRequests(OUTSTANDINGREQUESTS_HASHTABLE_SIZE),
  TCPDatagramCommunicationLight(0)
{
  CurrXid=0;
  launch(0);
}

void RPCClientLight::AsyncRPC(IPPort dest, int rpcno, u32 flags, Marshallable *data, RPCCallbackFunc callback, void *callbackdata){
  OutstandingRPCLight *orpc = new OutstandingRPCLight;
  orpc->dmsg.xid = AtomicInc32(&CurrXid);
  orpc->dmsg.flags = flags;
  orpc->dmsg.ipport = dest;
  orpc->dmsg.req = rpcno;
  orpc->dmsg.data = data;
  orpc->dmsg.freedata = 0;
  orpc->callback = callback;
  orpc->callbackdata = callbackdata;
  orpc->timestamp = (u64) Time::now();
  orpc->rpcc = this;
  orpc->done = 0;

  U32 Xid(orpc->dmsg.xid);
  OutstandingRequests.insert(Xid, orpc);

  sendMsg(&orpc->dmsg);
}

struct WaitCallbackData {
  EventSyncTwo *eventsync;
  char *retdata;
};

void RPCClientLight::waitCallBack(char *data, int len, void *callbackdata){
  WaitCallbackData *wcbd = (WaitCallbackData *) callbackdata;

  wcbd->retdata = (char*) malloc(len);
  memcpy(wcbd->retdata, data, len);
  wcbd->eventsync->set();
}

char *RPCClientLight::SyncRPC(IPPort dest, int rpcno, u32 flags, Marshallable *data){
  EventSyncTwo es;
  WaitCallbackData wcbd;

  wcbd.eventsync = &es;
  AsyncRPC(dest, rpcno, flags, data, &RPCClientLight::waitCallBack, (void*) &wcbd);
  es.wait();
  return wcbd.retdata;
}

/**************************************** SERVER ************************************************/

void RPCServerLight::handleMsg(IPPort *dest, u32 req, u32 xid, u32 flags, char *data, int len){
  Marshallable *result;
  MsgIdentifier msgid;
  int defer = 0; // default: free request message after remote procedure

  if (req >= NProcs){ return; } // no such procedure

  msgid.source = *dest;
  msgid.xid = xid;

  HandleDeferred *handle = new HandleDeferred(this, msgid);
  result = Procs[req](data, &defer, (void *) handle); // invoke procedure
  if (defer){ // defer the RPC
    // add dest, req, xid, flags, data to the set with deferred waiting RPCs
    RPCDeferred *rpcd = new RPCDeferred(dest, req, xid, flags, data, len);
    Deferred_lock.lock();
    DeferredWaiting.insert(msgid, rpcd);
    Deferred_lock.unlock();
    return; // keep the buffer
  }
  delete handle;

  if (result){ // send back answer
    DatagramMsg dmsg(result, *dest, req, xid, flags, true);  // or should we replace "flags" with 0 here (to clear flags in response)?
    sendMsg(&dmsg); 
  }
  return;
}


RPCServerLight::RPCServerLight(RPCProcLight *procs, int nprocs, int portno) :
  TCPDatagramCommunicationLight(portno)
{
  Procs = procs;
  NProcs = nprocs;
  ForceStop = 0;

  // launched deferred executor thread
  DeferredExecutor_thr = CreateThread(0, 0, RPCServerLight::DeferredExecutorThread, (void*) this, 0, 0);
  assert(DeferredExecutor_thr);
}

int RPCServerLight::SignalDeferred(void *handledeferred){
  int retval=0;
  HandleDeferred *h = (HandleDeferred*) handledeferred;
  RPCServerLight *S = h->server;
  RPCDeferred *rd, *rd2;
  int res;
  
  S->Deferred_lock.lock();
  res = S->DeferredWaiting.lookupDelete(h->msgid, 0, rd);
  if (res){
    S->Deferred_lock.unlock();
    retval = 1; // not found
  }
  else {
    // delete extra copies keeping the last one
    while (S->DeferredWaiting.lookupDelete(h->msgid, 0, rd2)==0){
      delete rd;
      rd = rd2;
    }
    S->DeferredReady.pushTail(rd); // put it in the ready queue
    S->Deferred_lock.unlock();
    S->DeferredReady_sem.signal();
    retval = 0;
  }
  delete h;
  return retval;
}

DWORD WINAPI RPCServerLight::DeferredExecutorThread(void *parm){
  RPCServerLight *S = (RPCServerLight *) parm;
  RPCDeferred *rd;

  while (!S->ForceStop){
    S->DeferredReady_sem.wait(INFINITE); // wait for work
    S->Deferred_lock.lock();
    if (!S->DeferredReady.empty()) rd = S->DeferredReady.popHead();
    else rd = 0;
    S->Deferred_lock.unlock();

    if (rd){
      S->handleMsg(&rd->dest, rd->req, rd->xid, rd->flags, rd->data, rd->len);
      free(rd->data);
      delete rd;
    }
  }

  return 0;
}
