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

//
// storageserver-splitter.cpp
// Splitter that lives in the storageserver and uses the efficient rpc. This also implements GetRowIDRPC.
//
// This implementation is independent of what is in splitter-server.cpp, which is intended for light RPCs
//
#include "stdafx.h"

#include "../gaia/tmalloc.h"
#include "../gaia/gaiaoptions.h"
#include "../gaia/debug.h"
#include "../gaia/task.h"
#include "../gaia/tcpdatagram.h"
#include "../gaia/grpctcp.h"

#include "../gaia/gaiarpcaux.h"
#include "../gaia/warning.h"
#include "../gaia/util-more.h"

#include "../options.h"
#include "../gaia/gaiatypes.h"
#include "../gaia/util.h"
#include "../gaia/datastruct.h"
#include "../gaia/newconfig.h"
#include "../gaia/clientlib/clientlib.h"
#include "../gaia/clientlib/clientdir.h"
#include "../dtreeaux.h"

#include "dtreesplit.h"
#include "storageserver-splitter.h"
#include "splitter-client.h"
#include "splitterrpcaux.h"

StorageConfig *SC=0;
SplitterDirectory *SD=0;

struct PendingSplitItem {
  int retry;  // 0=no retry requested, 1=retry requested
  PendingSplitItem() : retry(0) {}
};

struct SplitterStats {
  u32 splitQueueSize;      // how many elements are queued to be split
  u32 splitTimeRetryingMs; // how many ms we have been retrying current split (0 if current split is done)
  float splitTimeAvg;     // average time to split
  float splitTimeStddev;  // standard deviation time to split
  SplitterStats(){ splitQueueSize=0; splitTimeRetryingMs=0; splitTimeAvg=0.0; splitTimeStddev=0.0; }
};

struct COidListItem {
  COid coid;
  int threadno;
  COidListItem *prev, *next; // linklist stuff
  COidListItem() : threadno(-1) { coid.cid = (Cid)-1; coid.oid = (Oid)-1; }
  COidListItem(COid &c, int tno) : coid(c), threadno(tno){}
};

// maintains the rowid counters for each cid
class RowidCounters {
private:
  SkipList<COid,i64> rowidmap;
public:
  // lookup a cid. If found, increment rowid and return incremented value.
  // If not found, store hint and return it.
  i64 lookup(Cid cid, i64 hint){
    int res;
    i64 *rowidptr;
    i64 rowid;
    COid coid;

    coid.cid = cid;
    coid.oid = 0;

    res = rowidmap.lookupInsert(coid, rowidptr);
    if (res==0) rowid = ++(*rowidptr); // found it
    else rowid = *rowidptr = hint; // not found
    return rowid;
  }

  i64 lookupNohint(Cid cid){
    int res;
    i64 *rowidptr;
    i64 rowid;
    COid coid;

    coid.cid = cid;
    coid.oid = 0;
    res = rowidmap.lookup(coid, rowidptr);
    if (res==0) rowid = ++(*rowidptr); // found it
    else rowid = 0; // not found
    return rowid;
  }
};

class ServerSplitterState {
public:
  SkipList<COid,PendingSplitItem*> PendingSplits;
  LinkList<COidListItem> PendingResponses;
  SplitterStats Stats;
  RowidCounters RC;
  TaskInfo *tiProgSplitter;
  ServerSplitterState(){}
};

struct TaskMsgDataSplitterNewWork {
  COid coid; // coid to split
  int where; // if 0, put new work item at head (unusual), otherwise put it at tail (more common)
  //TaskMsgDataSplitterNewWork(COid &c, int w) : coid(c), where(w) {}
};

struct TaskMsgDataSplitterReply {
  SplitterStats stats; //
  COid coid; // coid that was just split, if stat.splitTimeRetryingMs == 0, otherwise invalid
};

void SendIFSplitterThreadNewWork(TaskScheduler *myts, COid &coid, int where);
Marshallable *SS_GETROWIDRPC(GetRowidRPCData *d);
int PROGSplitter(TaskInfo *ti);
  void ImmediateFuncSplitterHandleReportWork(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread);

int SS_SPLITNODERPC(RPCTaskInfo *rti){
  SplitnodeRPCData d;
  SplitnodeRPCRespData *resp;
  PendingSplitItem **psipp, *psip;
  TaskScheduler *ts;
  ServerSplitterState *SS = (ServerSplitterState*) tgetSharedSpace(THREADCONTEXT_SPACE_SPLITTER);
  int haspending;

  d.demarshall(rti->data);
  dprintf(1, "SPLIT  coid %I64x:%I64x wait %d getstatusonly %d", d.data->coid.cid, d.data->coid.oid, d.data->wait, d.data->getstatusonly);

  ts = tgetTaskScheduler();

  if (d.data->getstatusonly){
    haspending = SS->PendingSplits.nitems > 0;
  } else {
    haspending = 1;

    // check PendingSplits to see if a split is pending for d->data->coid
    if (!SS->PendingSplits.lookupInsert(d.data->coid, psipp)) {
      // found item already, so set retry=1, so that we attempt to split coid again when the current split is done
      //dprintf(1, "SPLIT found item, retry=%d, setting it to 1", (**psipp).retry);
      (**psipp).retry = 1;
    }
    else {
      // otherwise ask the worker to perform the split (send an immediatefunc message to it)
      //dprintf(1, "SPLIT did not find item, asking worker to split");
      psip = *psipp = new PendingSplitItem();
      psip->retry = 0;
      SendIFSplitterThreadNewWork(ts, d.data->coid, 1); // request retry to Splitter thread
    }
  }

  // return the current statistics
  resp = new SplitnodeRPCRespData;
  resp->data = new SplitnodeRPCResp;
  resp->freedata = true;
  resp->data->status = 0;
  resp->data->coid = d.data->coid;
  resp->data->load.splitQueueSize = SS->Stats.splitQueueSize;   // how many elements are queued to be split
  resp->data->load.splitTimeAvg = SS->Stats.splitTimeAvg;  // average time to split
  resp->data->load.splitTimeStddev = SS->Stats.splitTimeStddev;  // stddev time to split
  resp->data->load.splitTimeRetryingMs = SS->Stats.splitTimeRetryingMs;
  resp->data->haspending = haspending;
  
  rti->setResp(resp);
  dprintf(1, "SPLITR coid %I64x:%I64x queuesize %d timeavg %f timestddev %f retryingms %I64x haspending %d",
    resp->data->coid.cid, resp->data->coid.oid,
    resp->data->load.splitQueueSize, resp->data->load.splitTimeAvg, resp->data->load.splitTimeStddev, resp->data->load.splitTimeRetryingMs,
    resp->data->haspending);
  return SchedulerTaskStateEnding;
}


int SS_GETROWIDRPCstub(RPCTaskInfo *rti){
  GetRowidRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = SS_GETROWIDRPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}
Marshallable *SS_GETROWIDRPC(GetRowidRPCData *d){
  GetRowidRPCRespData *resp;
  i64 rowid;
  ServerSplitterState *SS = (ServerSplitterState*) tgetSharedSpace(THREADCONTEXT_SPACE_SPLITTER);

  if (d->data->hint) rowid = SS->RC.lookup(d->data->cid, d->data->hint);
  else rowid = SS->RC.lookupNohint(d->data->cid);

  //printf("GetRowidRPC cid %I64x hint %I64d rowid %I64d\n", d->data->cid, d->data->hint, rowid);

  resp = new GetRowidRPCRespData;
  resp->data = new GetRowidRPCResp;
  resp->freedata = true;
  resp->data->rowid = rowid;
  dprintf(1, "GETROWID oid %I64x hint %I64d resp %I64d", d->data->cid, d->data->hint, rowid);
  return resp;
}

// split item for thread to process
struct ThreadSplitItem {
  COid coid;
  int srcthread; // threadno that generated request
  u64 starttime;
  ThreadSplitItem *prev, *next; // linklist stuff
  ThreadSplitItem() : starttime(0) { coid.cid=(Cid)-1; coid.oid=(Oid)-1; }
  ThreadSplitItem(COid &c, int st) : coid(c), srcthread(st), starttime(0) { }
};


// this gets called at initialization of each of the RPC worker threads
void initServerTask(TaskScheduler *ts){ // **!** call it from each of the worker threads
  TaskInfo *ti;
  ServerSplitterState *SS = new ServerSplitterState;
  tsetSharedSpace(THREADCONTEXT_SPACE_SPLITTER, SS);

  ti = ts->createTask(PROGSplitter, 0); // creates task and assign it as a fixed task
  SS->tiProgSplitter = ti;
  ts->assignImmediateFunc(IMMEDIATEFUNC_SPLITTERTHREADREPORTWORK, ImmediateFuncSplitterHandleReportWork);
}

void SendIFSplitterThreadNewWork(TaskScheduler *myts, COid &coid, int where){
  TaskMsgDataSplitterNewWork tmdsnw;   assert(sizeof(TaskMsgDataSplitterNewWork) <= sizeof(TaskMsgData));
  tmdsnw.coid = coid;
  tmdsnw.where = where;
  sendIFMsg(gContext.getThread(TCLASS_SPLITTER, 0), IMMEDIATEFUNC_SPLITTERTHREADNEWWORK, &tmdsnw, sizeof(TaskMsgDataSplitterNewWork));
}

// handle reports of work from ServerSplitterThread
// registered as IMMEDIATEFUNC_SPLITTERTHREADREPORTWORK
void ImmediateFuncSplitterHandleReportWork(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread){
  TaskMsgDataSplitterReply *tmdsr = (TaskMsgDataSplitterReply*) &msgdata;
  ServerSplitterState *SS = (ServerSplitterState*) tgetSharedSpace(THREADCONTEXT_SPACE_SPLITTER);

  SS->Stats = tmdsr->stats; // record stats
  //dprintf(1, "SPLIT got split response for oid %I64x with retrying ms %d", tmdsr->coid.oid, tmdsr->stats.splitTimeRetryingMs);
  if (tmdsr->stats.splitTimeRetryingMs == 0){ // split just finished
    SS->PendingResponses.pushTail(new COidListItem(tmdsr->coid, srcthread));
    ts->wakeUpTask(SS->tiProgSplitter); // wake up PROGSplitter
  }
}

int PROGSplitter(TaskInfo *ti){
  int res;
  COidListItem *coidli;
  PendingSplitItem **psipp, *psip;
  TaskScheduler *ts = tgetTaskScheduler();
  ServerSplitterState *SS = (ServerSplitterState*) tgetSharedSpace(THREADCONTEXT_SPACE_SPLITTER);

  //dprintf(1, "PROGSplitter called");
  while (!SS->PendingResponses.empty()){
    coidli = SS->PendingResponses.popHead();
    res = SS->PendingSplits.lookup(coidli->coid, psipp);
    //dprintf(1, "PROGSplitter oid %I64x processing response, lookup res %d", coidli->coid.oid, res);
    if (!res){ // found
      psip = *psipp;
      if (psip->retry){
        psip->retry = 0;
        SendIFSplitterThreadNewWork(ts, coidli->coid, 1); // request retry to Splitter thread
        //dprintf(1, "PROGSplitter oid %I64x retry requested so requesting split, clearing retry", coidli->coid.oid);
      }
      else { // done with item
        //dprintf(1, "PROGSplitter oid %I64x no retry requested, done", coidli->coid.oid);
        SS->PendingSplits.lookupDelete(coidli->coid, 0, psip);
        delete psip;
      }
    }

    //else dprintf(1, "PROGSplitter oid %I64x not found in PendingSplits, weird", coidli->coid.oid);
    delete coidli;
  }
  //dprintf(1, "PROGSplitter going to sleep");
  return SchedulerTaskStateWaiting; // sleep until waken up again
}

// ----------------------------------------------- Splitter thread follows -------------------------------------------------------------

class SplitStats {
public:
  SplitStats() : average(SPLITTER_STAT_MOVING_AVE_WINDOW) { timeRetryingMs = 0; }
  MovingAverage average; // moving average time of successful splits
  u64 timeRetryingMs;     // time spent retrying split thus far (0 if no ongoing retries)
};

struct ServerSplitterThreadState {
  SplitStats Stats;
  LinkList<ThreadSplitItem> ThreadSplitQueue;
};

SplitStats *Stats=0;
ServerSplitterThreadState *TSS = 0;

DWORD WINAPI ServerSplitterThread(void *parm);

// Creates splitter thread. This gets called once only
void initServerSplitter(){
  int threadno;
  TSS = new ServerSplitterThreadState; assert(TSS);

  threadno = SLauncher->createThread("ServerSplitter", ServerSplitterThread, 0, false);
  gContext.setNThreads(TCLASS_SPLITTER, 1); 
  gContext.setThread(TCLASS_SPLITTER, 0, threadno);
}

#ifndef MULTI_SPLITTER
int ServerSplitterAnotherSplit(COid coid, int where, void *parm, int isleaf){
  ThreadSplitItem *tsi = new ThreadSplitItem(coid, -1); assert(tsi);
  if (where==0) TSS->ThreadSplitQueue.pushHead(tsi);
  else TSS->ThreadSplitQueue.pushTail(tsi);
  return 0;
}
#else
int ServerSplitterAnotherSplit(COid coid, int where, void *parm, int isleaf){
  CallSplitter(coid, (void*)isleaf);
  return 0;
}
#endif

void ImmediateFuncSplitterThreadNewWork(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread){
  TEvent e;
  TaskMsgDataSplitterNewWork *nw = (TaskMsgDataSplitterNewWork*) &msgdata;
  ThreadSplitItem *tsi = new ThreadSplitItem(nw->coid, srcthread); assert(tsi);
  if (nw->where==0) TSS->ThreadSplitQueue.pushHead(tsi);
  else TSS->ThreadSplitQueue.pushTail(tsi);
}

// remove repeated elements from split queue
void cleanupThreadSplitQueue(void){
  ThreadSplitItem *ptr, *next;
  Set<COid> existing;
  for (ptr = TSS->ThreadSplitQueue.getFirst(); ptr != TSS->ThreadSplitQueue.getLast(); ptr = next){
    next = TSS->ThreadSplitQueue.getNext(ptr);
    if (existing.insert(ptr->coid)){ // item already exists
      TSS->ThreadSplitQueue.remove(ptr);
    }
  }
}

void SendIFThreadReportWork(TaskScheduler *myts, COid &coid, int dstthread){
  TaskMsgDataSplitterReply tmdsr;   assert(sizeof(TaskMsgDataSplitterReply) <= sizeof(TaskMsgData));
  cleanupThreadSplitQueue();
  tmdsr.stats.splitQueueSize = TSS->ThreadSplitQueue.nitems;
  tmdsr.stats.splitTimeRetryingMs = (u32)TSS->Stats.timeRetryingMs;
  tmdsr.stats.splitTimeAvg = (float) TSS->Stats.average.getAvg();
  tmdsr.stats.splitTimeStddev = (float) TSS->Stats.average.getStdDev();
  tmdsr.coid = coid;
  sendIFMsg(dstthread, IMMEDIATEFUNC_SPLITTERTHREADREPORTWORK, (void*)&tmdsr, sizeof(TaskMsgDataSplitterReply));
}

DWORD WINAPI ServerSplitterThread(void *parm){
  TaskScheduler *ts;
  ThreadSplitItem *tsi=0;
  u64 endtime;
  int res;
  assert(TSS);
  static int scount=0, xcount=0, ocount=0;

  // SLauncher->initThreadContext("ServerSplitter",0);
  ts = tgetTaskScheduler();

  ts->assignImmediateFunc(IMMEDIATEFUNC_SPLITTERTHREADNEWWORK, ImmediateFuncSplitterThreadNewWork);

  while (1){
    ts->runOnce();

    if (!tsi){
      if (!TSS->ThreadSplitQueue.empty()){
        if (++scount % 100 == 0) putchar('S');
        tsi = TSS->ThreadSplitQueue.popHead();
        tsi->starttime = Time::now();
      } else { // no work to do
        SleepEx(2,1); // be nice
        continue;
      }
    }
    if (tsi){
      res = DtSplit(tsi->coid, true, ServerSplitterAnotherSplit, 0);
      endtime = Time::now();

      if (res){ // could not complete split
        TSS->Stats.timeRetryingMs = endtime - tsi->starttime;
        if (TSS->Stats.timeRetryingMs == 0) TSS->Stats.timeRetryingMs = 1; // avoid 0 since 0 indicates completion of split
        Sleep(1);
        if (++xcount % 100 == 0) putchar('X');

        // report stats to originator, if not us
        if (tsi->srcthread != -1)
          SendIFThreadReportWork(ts, tsi->coid, tsi->srcthread);
      }
      else { // finisher split
        if (++ocount % 100 == 0) putchar('O');
        TSS->Stats.average.put((double)(endtime - tsi->starttime));
        TSS->Stats.timeRetryingMs = 0;

        // report completion and stats to originator, if not us. Completion indicated by fact that timeRetryingMs==0
        if (tsi->srcthread != -1)
          SendIFThreadReportWork(ts, tsi->coid, tsi->srcthread);

        delete tsi;
        tsi = 0; // done with this item, process next one
      }
    }
    SleepEx(0,1);
  }
}
