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
#include "grpctcp.h"
#include "scheduler.h"

void RPCClient::handleMsg(IPPort *dest, u32 req, u32 xid, u32 flags, TaskMultiBuffer *tmb, char *data, int len){
  OutstandingRPC *orpc;

  orpc = RequestLookupAndDelete(xid);
  if (orpc && orpc->callback){
    orpc->callback(data, len, orpc->callbackdata);
  }
  if (orpc){
    // **!** added code after removing EfficientRetransmit
    delete orpc->dmsg.data;
    delete orpc;
  }

  freeMB(tmb);

  // note: orpc and orpc->data will be freed by the RetransmitRPC call below
  // so caller should not try to free them
  return;
}

OutstandingRPC *RPCClient::RequestLookupAndDelete(u32 xid){
  U32 Xid(xid);
  int res;
  OutstandingRPC *it=0;
  res = OutstandingRequests.lookupRemove(Xid, 0, it);
  if (!res){ // found it
    it->done = true; // mark as done
  }
  return it;
}

//// The efficient retransmission handler works as follows.
//// The queue always has a marker (a null pointer) that separates the entries
//// added before and after the last time this function was called.
//// When it executes, the function pops elements for retransmissions from the head
//// of the queue until the marker (the marker is also popped). The function then
//// adds a marker to the end of the queue.
//int RPCClient::EfficientRetransmitRPC(void*parm){
//  RPCClient *rpcc = (RPCClient*) parm;
//  OutstandingRPC *orpc;
//
//  // put new 0 marker at tail
//  rpcc->EfficientRetransmit_lock.lock();
//  rpcc->EfficientRetransmit.pushTail(0);
//  rpcc->EfficientRetransmit_lock.unlock();
//
//  // no need to acquire EfficientRetransmit_lock in this
//  // loop since we only remove elements from the front
//  // (while other threads may be adding elements to the back only)
//  orpc = rpcc->EfficientRetransmit.popHead();
//  while (orpc){  // continue until previous 0 marker is found (not the one we just added,
//                 // but the one we added in the previous invocation to this method)
//    // process orpc
//    assert(orpc->rpcc == rpcc);
//    //rpcc->OutstandingRequests_lock.lock();
//
//    if (!orpc->done){
//      //rpcc->OutstandingRequests_lock.unlock();
//      orpc->currtimeout = FIRST_RPC_REXMIT;
//      ++orpc->nretransmit;
//      assert(!orpc->dmsg.freedata);
//      rpcc->sendMsg(&orpc->dmsg); 
//      // schedule future retransmissions using event scheduler directly
//      //rpcc->SchedRetransmit.AddEvent(RetransmitRPC, (void*) orpc, 0, orpc->currtimeout);
//      TaskEventScheduler::AddEvent(tgetThreadNo(), RetransmitRPC, (void*) orpc, 0, orpc->currtimeout);
//    } else {
//      //rpcc->OutstandingRequests_lock.unlock();
//      delete orpc->dmsg.data;
//      delete orpc;
//    }
//
//    orpc = rpcc->EfficientRetransmit.popHead();
//  }
//
//  return 0;
//}

RPCClient::RPCClient() :
  OutstandingRequests(OUTSTANDINGREQUESTS_HASHTABLE_SIZE),
  TCPDatagramCommunication(0)
  //SchedRetransmit("RPCClient_Retransmit")
{
  CurrXid=0;
  //EfficientRetransmit.pushTail(0); // put marker there
  //SchedRetransmit.AddEvent(EfficientRetransmitRPC, this, 1, EFFICIENT_RPC_REXMIT_PERIOD);
  //SchedRetransmit.launch();
  launch(0);
}

// this function is called by the sendThread in TCPDatagramCommunication just before entering the event loop
void RPCClient::startupSendThread(){
  //int i, nthreads = gContext.getNThreads(TCLASS_SEND);
  //for (i=0; i < nthreads; ++i){
  //  TaskEventScheduler::AddEvent(gContext.getThread(TCLASS_SEND, i), EfficientRetransmitRPC, this, 1, EFFICIENT_RPC_REXMIT_PERIOD);
  //}
}

void RPCClient::AsyncRPC(IPPort dest, int rpcno, u32 flags, Marshallable *data, RPCCallbackFunc callback, void *callbackdata){
  OutstandingRPC *orpc = new OutstandingRPC;
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
  //orpc->currtimeout = 0;     // with the efficient retransmission optimization,
  //                           // first retransmit is determined by EFFICIENT_RPC_REXMIT_PERIOD,
  //                           // not by currtimeout. Currtimeout is used for subsequent
  //                           // retransmissions done by SchedRetransmit
  //orpc->nretransmit = 0;
  orpc->done = 0;

  U32 Xid(orpc->dmsg.xid);
  OutstandingRequests.insert(Xid, orpc);

  sendMsg(&orpc->dmsg);
  //EfficientRetransmit_lock.lock();
  //// Using SchedRetransmit like this
  ////      SchedRetransmit.AddEvent(RetransmitRPC, (void*) orpc, 0, orpc->currtimeout);
  //// is too slow, since that adds lots of events to the priority queue.
  //// Instead, we first add the retransmission to a queue, and there is a task that
  //// scans the queue every 1s and executes RetransmitRPC for the items that have been
  //// added before the previous scan. Subsequent retransmissions are done via
  //// SchedRetransmit directly. This optimization makes sense because most RPCs
  //// will not require a retransmission: when RetransmitRPC is called, the handler
  //// will simply realize that the response has arrived and therefore the item
  //// can be discarded
  //EfficientRetransmit.pushTail(orpc);
  //EfficientRetransmit_lock.unlock();
}

struct WaitCallbackData {
  EventSyncTwo *eventsync;
  char *retdata;
};

void RPCClient::waitCallBack(char *data, int len, void *callbackdata){
  WaitCallbackData *wcbd = (WaitCallbackData *) callbackdata;

  wcbd->retdata = (char*) malloc(len);
  memcpy(wcbd->retdata, data, len);
  wcbd->eventsync->set();
  return;
}

char *RPCClient::SyncRPC(IPPort dest, int rpcno, u32 flags, Marshallable *data){
  EventSyncTwo es;
  WaitCallbackData wcbd;

  wcbd.eventsync = &es;
  AsyncRPC(dest, rpcno, flags, data, &RPCClient::waitCallBack, (void*) &wcbd);
  es.wait();
  return wcbd.retdata;
}

//// auxilliary function to be used in RetransmitRPC to apply operations in an entry of OutstandingRequests while
//// holding a lock to its hashtable bin. Basically, the logic of RetransmitRPC is partly here.
//// Returns 1 if element was removed from OutstandingRequests because RPC call expired, 0 otherwise
//int auxRetransmitRPC(U32 &xid, OutstandingRPC **orpcptr, int status, SkipList<U32,OutstandingRPC*> *b, u64 parm){
//  OutstandingRPC *dummy, *orpc;
//  int res;
//  u64 now = parm;
//  if (status==0){
//    orpc = *orpcptr;
//    if (!orpc->done){
//      if (now > orpc->timestamp + RPCClientMsRetransmitExpire){ // RPC call expired
//        printf("RPC failed after too many retries!\n");
//        res = b->lookupDelete(xid, 0, dummy);
//        assert(res==0);
//        assert(dummy==orpc);
//        orpc->done = true;
//        return 1;
//      }
//    }
//    return 0;
//  }
//  else { // entry not found
//    return 0;
//  }
//}
//
//int RPCClient::RetransmitRPC(void*parm){
//  OutstandingRPC *orpc = (OutstandingRPC *) parm;
//  RPCClient *rpcc = orpc->rpcc;
//  int res;
//
//  if (orpc->done){
//    delete orpc->dmsg.data;
//    delete orpc;
//    return 0;
//  }
//
//  u64 now = Time::now();
//  U32 Xid(orpc->dmsg.xid);
//  res = rpcc->OutstandingRequests.lookupApply(Xid, auxRetransmitRPC, now);
//
//  if (res == 1){ // orpc->done was false and RPC expired
//    assert(orpc->done); // set to true by auxRetransmitRPC
//    orpc->callback(0, 0, orpc->callbackdata); // invoke callback with no data to indicate failure
//    delete orpc->dmsg.data;
//    delete orpc;
//  }
//  else if (!orpc->done){ // retransmit
//    orpc->currtimeout = (int)((double)orpc->currtimeout * MULT_RPC_REXMIT);
//    ++orpc->nretransmit;
//    assert(!orpc->dmsg.freedata);
//    rpcc->sendMsg(&orpc->dmsg); 
//    //rpcc->SchedRetransmit.AddEvent(RetransmitRPC, (void*) orpc, 0, orpc->currtimeout);
//    TaskEventScheduler::AddEvent(tgetThreadNo(), RetransmitRPC, (void*) orpc, 0, orpc->currtimeout);
//  } else {
//    delete orpc->dmsg.data;
//    delete orpc;
//  }
//
//  return 0;
//}

/**************************************** SERVER ************************************************/

struct TaskMsgDataToSend {
  TaskMsgDataToSend(SLinkList<DatagramMsgI> *ts, int tno){ tosend = ts; threadno = tno; }
  SLinkList<DatagramMsgI> *tosend; // tosend link list
  int threadno; // thread who allocated tosend
};

// sends a message to the SENDTOSEND immediate function at a sender thread
void RPCServer::SendToSend(SLinkList<DatagramMsgI> *ToSend, int destthreadno){
  TaskMsgDataToSend tmdts(ToSend, tgetThreadNo());
  sendIFMsg(destthreadno, IMMEDIATEFUNC_SENDTOSEND, (void*) &tmdts, sizeof(TaskMsgDataToSend));
}

void RPCServer::ImmediateFuncSendToSend(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread){
  TaskMsgDataToSend *tmds = (TaskMsgDataToSend*) &msgdata;
  SLinkList<DatagramMsgI> *ToSend = tmds->tosend;
  // extract shared space entry
  ThreadSharedSend *tss = (ThreadSharedSend*) tgetSharedSpace(THREADCONTEXT_SPACE_SENDTASK);
  DatagramMsgI *dmsgi;
  assert(tss);

  while (!ToSend->empty()){
    dmsgi = ToSend->popHead();
    tss->SendList.pushTail(*(DatagramMsg*)dmsgi); // add to list of messages to be sent
    delete dmsgi;
  }
  ts->wakeUpTask(tss->sendTaskInfo);
  delete ToSend;
}

void RPCServer::startupWorkerThread(){
  int i;
  TaskScheduler *ts = tgetTaskScheduler();
  RPCServerThreadInfo *rsti = new RPCServerThreadInfo;
  rsti->ToSend = new SLinkList<DatagramMsgI>*[SENDTHREADS];  // initialize ToSend list
  for (i=0; i < SENDTHREADS; ++i){
    rsti->ToSend[i] = new SLinkList<DatagramMsgI>;
  }
  tsetSharedSpace(THREADCONTEXT_SPACE_RPCSERVER_WORKER, rsti);
  //TaskEventScheduler::AddEvent(tgetThreadNo(), GCResultCacheHandler, (void*) this, 1, RPCServerMsGCPeriod);
  ts->createTask(PROGFlushToSend, 0);
  dprintf(1, "Server worker id %I64x", UniqueId::getUniqueId());
  fflush(stdout);
}

void RPCServer::finishWorkerThread(){
  RPCServerThreadInfo *rsti = (RPCServerThreadInfo*) tgetSharedSpace(THREADCONTEXT_SPACE_RPCSERVER_WORKER);
  delete rsti;
}

// called by TCPDatagramCommunication before entering event loop of sender thread
void RPCServer::startupSendThread(){
  TaskScheduler *ts = tgetTaskScheduler();
  ts->assignImmediateFunc(IMMEDIATEFUNC_SENDTOSEND, ImmediateFuncSendToSend);
}

// called by TCPDatagramCommunication after leaving event loop of sender thread
void RPCServer::finishSendThread(){
}

void RPCServer::duringPROGSend(TaskInfo *ti){
}

int RPCServer::RPCStart(RPCTaskInfo *rti){
  RPCServer *server = (RPCServer*) tgetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM);
  RPCServerThreadInfo *rsti = (RPCServerThreadInfo*) tgetSharedSpace(THREADCONTEXT_SPACE_RPCSERVER_WORKER);
  //RPCCachedResult *it;
  //bool idempotent;

  assert(rti->req < server->NProcs); // ensure rpcno is within range

  rti->msgid.source = rti->src;
  rti->msgid.xid = rti->xid;
  //idempotent = (rti->flags & MSG_FLAG_IDEMPOTENT) != 0;

  //it = 0;
  //if (!idempotent){
  //  it = rsti->ResultCache.lookup(&rti->msgid);
  //  if (it == 0){ // request not seen before
  //    rti->seen=0; 
  //    it = new RPCCachedResult;
  //    it->id = rti->msgid;
  //    it->result = 0;
  //    rsti->ResultCache.insert(it);
  //  }    
  //  else { // request seen before
  //    rti->seen = 1; // request seen. Response may or may not be ready
  //    rti->setResp(it->result);  // non-0 if answer available, 0 if not available (being computed by another thread)
  //  }
  //} else rti->seen=0;

  //if (!rti->seen){
    rti->setFunc((ProgFunc)server->Procs[rti->req]);
    return server->Procs[rti->req](rti); // invoke procedure
  //}
  //return SchedulerTaskStateRunning;
}

int RPCServer::RPCEnd(RPCTaskInfo *rti){
  //RPCCachedResult *it;
  RPCServer *server = (RPCServer*) tgetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM);
  RPCServerThreadInfo *rsti = (RPCServerThreadInfo*) tgetSharedSpace(THREADCONTEXT_SPACE_RPCSERVER_WORKER);
  DatagramMsgI *dmsgi;
  int serverthreadno;

  //bool idempotent = (rti->flags & MSG_FLAG_IDEMPOTENT) != 0;
  //if (!idempotent && !rti->seen){ // remember result
  //  it = rsti->ResultCache.lookup(&rti->msgid);
  //    if (it == 0){ // request not seen before
  //      it = new RPCCachedResult;
  //      it->id = rti->msgid;
  //      rsti->ResultCache.insert(it);
  //    }    
  //    it->result = rti->getResp();
  //    it->timeRequest = Time::now();
  //}

  dmsgi = new DatagramMsgI;
  dmsgi->data = rti->getResp();
  dmsgi->ipport = rti->src;
  dmsgi->req = rti->req;
  dmsgi->xid = rti->xid;
  dmsgi->flags = rti->flags; // or should we clear the flags?
  //dmsgi->freedata = idempotent ? true : false;  // if not idempotent then do not free result after sending since we are caching it
  //                                              // if idempotent then we can free result
  dmsgi->freedata = true;
  serverthreadno = gContext.hashThreadIndex(TCLASS_SEND, dmsgi->ipport.ip); // choose send thread based on client ip
  rsti->ToSend[serverthreadno]->pushTail(dmsgi);  // append response to list of messages for serverthreadno to send
  //server->sendMsg((DatagramMsg*) dmsgi);
   
  freeMB(rti->tmb); // free incoming RPC data
  return SchedulerTaskStateEnding;
}

// send message with ToSend to sender thread
int RPCServer::PROGFlushToSend(TaskInfo *ti){
  assert(sizeof(DatagramMsg) <= TASKSCHEDULER_TASKMSGDATA_SIZE);
  int senderthreadno; // to which sender thread to send
  int i;
  RPCServerThreadInfo *rsti = (RPCServerThreadInfo*) tgetSharedSpace(THREADCONTEXT_SPACE_RPCSERVER_WORKER);

  for (i=0; i < SENDTHREADS; ++i){
    if (!rsti->ToSend[i]->empty()){ // something to send to the i-th sender thread
      // send the ToSend to the sender thread
      senderthreadno = gContext.hashThread(TCLASS_SEND, i);
      SendToSend(rsti->ToSend[i], senderthreadno);

      rsti->ToSend[i] = new SLinkList<DatagramMsgI>; // reset to an empty list
      // note that the previous list will be freed by the sender thread
    }
  }

  return SchedulerTaskStateRunning;
}

// called by immediate function when worker thread gets a new RPC
void RPCServer::handleMsg(IPPort *dest, u32 req, u32 xid, u32 flags, TaskMultiBuffer *tmb, char *data, int len){
  TaskScheduler *ts = tgetTaskScheduler();
  TaskInfo *ti = new RPCTaskInfo((ProgFunc) RPCStart, 0, dest, req, xid, flags, tmb, data, len);
  ti->setEndFunc((ProgFunc) RPCEnd); // set ending function
  ts->createTask(ti); // creates task
}

//int RPCServer::GCResultCacheHandler(void *parm){
//  RPCServer *rs = (RPCServer*) tgetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM);
//  RPCServerThreadInfo *rsti = (RPCServerThreadInfo*) tgetSharedSpace(THREADCONTEXT_SPACE_RPCSERVER_WORKER);
//  HashTableBK<MsgIdentifier,RPCCachedResult> &ResultCache = rsti->ResultCache;
//  RPCCachedResult *it, *itnext;
//  u64 now;
//
//  dprintf(5, "GC Starts\n");
//  now = Time::now();
//
//  for (it = ResultCache.getFirst(); it != ResultCache.getLast(); it=itnext){
//    assert(it);
//    itnext=ResultCache.getNext(it);
//    if (it->timeRequest + RPCServerMsCacheExpire <= now){ // expired
//      ResultCache.remove(it); // remove from cache
//      delete it->result; // delete result itself
//      delete it; // delete RPCCachedResult object
//    }
//  }
//  dprintf(5, "GC ends: duration %I64d ms\n", Time::now()-now);
//
//  /* Faster version, but more complex */
//  //ResultCacheHashType::iterator firstdel, lastdel;
//  //for (it = ResultCache.begin(); it != ResultCache.end(); it=itnext){
//  //  rcr = it->second;
//  //  itnext=it;
//  //  ++itnext;
//  //  if (rcr && rcr->timeRequest + RPCServerMsCacheExpire <= now){ // expired
//  //    firstdel = it;
//  //    do {
//  //      delete rcr->result;
//  //      delete rcr;
//  //      if (itnext == ResultCache.end()) break;
//  //      rcr = itnext->second;
//  //      if (!rcr || rcr->timeRequest + RPCServerMsCacheExpire > now) break;
//  //      it = itnext;
//  //      ++itnext;
//  //    } while (1);
//  //    // delete from firstdel to it
//  //    ResultCache.erase(firstdel, it);
//  //  }
//  //}
//
//  /* version that copies non-expired entries to new cache */
//  /* Note: requires ResultCache to be a pointer in class, not the real object */
//  //ResultCacheHashType *oldResultCache = rs->ResultCache;
//  //ResultCacheHashType *newResultCache;
//  //newResultCache = new ResultCacheHashType;
//  //for (it = oldResultCache->begin(); it != oldResultCache->end(); ++it){
//  //  rcr = it->second;
//  //  if (rcr && rcr->timeRequest + RPCServerMsCacheExpire <= now){ // expired
//  //    delete rcr->result; // delete result itself
//  //    delete rcr; // delete RPCCachedResult object
//  //  }
//  //  else (*newResultCache)[it->first] = rcr; // add to new result cache
//  //}
//  //rs->ResultCache = newResultCache;
//  //rs->ResultCache_lock.unlock();
//  //delete oldResultCache;
//
//  return 0; // keep rescheduling forever
//}

RPCServer::RPCServer(RPCProc *procs, int nprocs, int portno) :
  TCPDatagramCommunication(portno)
{
  Procs = procs;
  NProcs = nprocs;
}
