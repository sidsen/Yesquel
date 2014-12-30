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
#include "../gaia/gaiaoptions.h"
#include "../gaia/debug.h"
#include "../gaia/task.h"
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
#include "splitter-server.h"
#include "splitter-client.h"
#include "splitterrpcaux.h"


#define SPLITTER_PORT_OFFSET 10  // port of splitter is port of storageserver plus SPLITTER_PORT_OFFSET
#define DEBUG_LEVEL_DEFAULT 0
#define DEBUG_LEVEL_WHEN_LOGFILE 1 // debug level when -g option (use logfile) is chosen

const u8 MYSITE=0;

#pragma comment(lib, "ws2_32.lib")

// forward definitions
class HostConfig;
//extern RPCServer *ServerPtr; // defined in storageserver-rpc.c

StorageConfig *SC=0;
SplitterDirectory *SD=0;

HANDLE SplitterThrId;
RWLock SplitterWork_l;
Semaphore SplitterSem;
#ifndef ALL_SPLITS_UNCONDITIONAL
RecentSplitsNew *Recent=0;
EventScheduler *ES=0;
#endif

// forward definition
int ServerDoSplit(COid coid, int where, void *parm);

// maintains the rowid counters for each cid
class RowidCounters {
private:
  SkipList<COid,i64> rowidmap;
  RWLock rowidmap_l;
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

    rowidmap_l.lock();
    res = rowidmap.lookupInsert(coid, rowidptr);
    if (res==0) rowid = ++*rowidptr; // found it
    else rowid = *rowidptr = hint; // not found
    rowidmap_l.unlock();
    return rowid;
  }

  i64 lookupNohint(Cid cid){
    int res;
    i64 *rowidptr;
    i64 rowid;
    COid coid;

    coid.cid = cid;
    coid.oid = 0;
    rowidmap_l.lock();
    res = rowidmap.lookup(coid, rowidptr);
    if (res==0) rowid = ++*rowidptr; // found it
    else rowid = 0; // not found
    rowidmap_l.unlock();
    return rowid;
  }
};

RowidCounters RC;

class SplitStats {
public:
  SplitStats() : average(SPLITTER_STAT_MOVING_AVE_WINDOW) { timeRetryingMs = 0; }
  RWLock lock; // lock protecting all variables in an instance of this class
  MovingAverage average; // moving average time of successful splits
  u64 timeRetryingMs;     // time spent retrying split thus far (0 if no ongoing retries)
};
SplitStats *Stats=0;

RPCProc RPCProcs[] = {  SS_NULLRPCstub,
                        SS_SHUTDOWNRPCstub,
                        SS_SPLITNODERPCstub,
                        SS_GETROWIDRPCstub
                     };


// State of a split job. A split job starts as a single split,
// but may grow if the split requires further splits
struct SplitterJobState {
  SplitterJobState() : splitstodo(0), splitsdone(0) { }
  int splitstodo; // number of splits that must still be done in job
  int splitsdone; // number of splits done so far
  Semaphore towake; // if non-zero, semaphore to post when job is done
};

struct SplitterWork {
  SplitterWork(){}
  SplitterWork(COid c) : coid(c), sjs(0), firstInJob(true) { when = Time::now(); }
  SplitterWork(COid c, SplitterJobState *s, bool fij) : coid(c), sjs(s), firstInJob(fij) { when = Time::now(); }
  u64 when;
  COid coid;
  SplitterJobState *sjs;
  bool firstInJob;
  SplitterWork *next, *prev;
};

LinkList<SplitterWork> SplitterWorkQueue;

void DoOneSplitUntilSuccess(COid coid, void *parm, void *specificparm){
  SplitterJobState *sjs = (SplitterJobState*) specificparm;
  int res;
  u64 timebeg, timeend;
  timebeg = Time::now();
  do { // this loop repeats until split is successful
    dputchar(1,'S');
    res = DtSplit(coid, true, ServerDoSplit, (void*) sjs);
    timeend = Time::now();
    if (res){
      Stats->lock.lock();
      Stats->timeRetryingMs = timeend-timebeg;
      Stats->lock.unlock();
      Sleep(10);
      dputchar(1, 'X');
    }
    else {
      dputchar(1, 'O');
      Stats->lock.lock();
      Stats->average.put((double)(timeend-timebeg));
      Stats->timeRetryingMs = 0;
      Stats->lock.unlock();
    }
  } while (res);
}


DWORD WINAPI SplitterThread(void *parm){
  SplitterWork *sw;
  COid coid;

#ifndef ALL_SPLITS_UNCONDITIONAL
  ES = new EventScheduler("SplitterThread");
  //Recent = new RecentSplits(DTREE_AVOID_DUPLICATE_INTERVAL, SplitterWorker, (void*) 0, &es);
  Recent = new RecentSplitsNew(DoOneSplitUntilSuccess, (void*) 0, ES);

  ES->launch();
#endif

  while (1){
    while (SplitterSem.wait(INFINITE));
    SplitterWork_l.lock();
    if (!SplitterWorkQueue.empty())
      sw = SplitterWorkQueue.popHead();
    else sw = 0;
    SplitterWork_l.unlock();
    if (sw){
      coid = sw->coid;
#ifdef ALL_SPLITS_UNCONDITIONAL
      DoOneSplitUntilSuccess(coid, 0, (void*) sw->sjs);
#else
      if (sw->firstInJob) Recent->submit(coid, (void*) sw->sjs); // if first split in job, do it conditionally
      else DoOneSplitUntilSuccess(coid, 0, (void*) sw->sjs); // otherwise, it is unconditional
#endif
      if (sw->sjs){
        ++sw->sjs->splitsdone; // one more split done
        if (sw->sjs->splitsdone == sw->sjs->splitstodo){ // if did all the splits
          sw->sjs->towake.signal(); // signal completion of split job
        }
      }
      delete sw;
    }
  }
}

void InitServerSplitter(){
  Stats = new SplitStats();
  char *configfile = getenv("GAIACONFIG");
  if (!configfile) configfile = GAIA_DEFAULT_CONFIG_FILENAME;
  SC = new StorageConfig(configfile, MYSITE);
  KVInterfaceInit();
  SplitterThrId = CreateThread(0, 0, SplitterThread, 0, 0, 0); assert(SplitterThrId);
}

void UninitServerSplitter(){
  if (SC) delete SC;
  SC = 0;
}

// Enqueues a request to split a node.
//   coid: coid to be split
//   where : indicates where to put the split request
//           0=put at head to be done first
//           1=put at tail to be done last
//   uncond: parameter passed in call to DtSplit. Currently, pointer is cast to
//           a boolean indicating if split is unconditional (always do it) or not
//           (do it dependent on a recency test)
//   towait: if non-null, semaphore to signal when split has been finished
//   counter: if non-null, pointer to 32-bit aligned counter to atomically increment if more splits are needed
// where=0 means put request at head (to be done first)
// where=1 means put request at tail (to be done last)
// Returns number of elements in queue after enqueuing
int ServerDoSplit0(COid coid, int where, void *parm, bool firstinjob){
  SplitterJobState *sjs = (SplitterJobState*) parm;
  DTreeNode node;
  int nelems;

  if (sjs) ++sjs->splitstodo; // mark one more split to do
  SplitterWork_l.lock();
  if (where==0) SplitterWorkQueue.pushHead(new SplitterWork(coid, sjs, firstinjob));
  else SplitterWorkQueue.pushTail(new SplitterWork(coid, sjs, firstinjob));
  nelems = SplitterWorkQueue.nitems;
  SplitterWork_l.unlock();
  SplitterSem.signal();
  return nelems;
}

int ServerDoSplit(COid coid, int where, void *parm)     { return ServerDoSplit0(coid, where, parm, false); }
int ServerDoSplitFirst(COid coid, int where, void *parm){ return ServerDoSplit0(coid, where, parm, true); }

int SS_NULLRPCstub(RPCTaskInfo *rti){
  NullRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = SS_NULLRPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}
Marshallable *SS_NULLRPC(NullRPCData *d){
  NullRPCRespData *resp;

  resp = new NullRPCRespData;
  resp->data = new NullRPCResp;
  resp->freedata = true;
  resp->data->reserved = 0;
  return resp;
}

// handler that will exit the server
int ss_ExitHandler(void *p){
  exit(0);
}

int SS_SHUTDOWNRPCstub(RPCTaskInfo *rti){
  ShutdownRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = SS_SHUTDOWNRPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}
Marshallable *SS_SHUTDOWNRPC(ShutdownRPCData *d){
  ShutdownRPCRespData *resp;

  // schedule exit to occur 2 seconds from now, to give time to answer request
  dprintf(1, "Shutting down server in 2 seconds...");
  TaskEventScheduler::AddEvent(tgetThreadNo(), ss_ExitHandler, 0, 0, 2000);
  resp = new ShutdownRPCRespData;
  resp->data = new ShutdownRPCResp;
  resp->freedata = true;
  resp->data->reserved = 0;
  return resp;
}

int SS_SPLITNODERPCstub(RPCTaskInfo *rti){
  SplitnodeRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = SS_SPLITNODERPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}
Marshallable *SS_SPLITNODERPC(SplitnodeRPCData *d){
  SplitnodeRPCRespData *resp;
  int queuesize;
  u64 now;
  SplitterJobState *sjs=0;

  if (!d->data->getstatusonly){
    if (d->data->wait) sjs = new SplitterJobState();
    queuesize = ServerDoSplitFirst(d->data->coid, 1, (void*) sjs);  // enqueue split request at the end
  }

  resp = new SplitnodeRPCRespData;
  resp->data = new SplitnodeRPCResp;
  resp->freedata = true;
  now = Time::now();
  resp->data->status = 0;
  resp->data->coid = d->data->coid;

  Stats->lock.lockRead();
  resp->data->load.splitQueueSize = queuesize;   // how many elements are queued to be split
  resp->data->load.splitTimeAvg = Stats->average.getAvg();  // average time to split
  resp->data->load.splitTimeStddev = Stats->average.getStdDev();  // stddev time to split
  resp->data->load.splitTimeRetryingMs = Stats->timeRetryingMs;
  resp->data->haspending = queuesize > 0;
  Stats->lock.unlockRead();

#ifndef ALL_SPLITS_UNCONDITIONAL
  if (!d->data->getstatusonly)
    Recent->reportduration(d->data->coid, (int) (queuesize * resp->data->load.splitTimeAvg));
#endif

  if (sjs){
    sjs->towake.wait(INFINITE);
    delete sjs;
    sjs = 0;
  }

  return resp;
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

  if (d->data->hint) rowid = RC.lookup(d->data->cid, d->data->hint);
  else rowid = RC.lookupNohint(d->data->cid);

  //printf("GetRowidRPC cid %I64x hint %I64d rowid %I64d\n", d->data->cid, d->data->hint, rowid);

  resp = new GetRowidRPCRespData;
  resp->data = new GetRowidRPCResp;
  resp->freedata = true;
  resp->data->rowid = rowid;
  return resp;
}


#ifdef NDEBUG
#define COMPILECONFIG "Release"
#else
#define COMPILECONFIG "Debug"
#endif

int _tmain(int argc, _TCHAR **av)
{
  int res;
  WSADATA wsadata;
  u32 myip;
  u16 myport;
  IPPort myipport;
  char **argv;
  char *argv0;
  int badargs, c;
  int useconsole=0;
  int loadfile=0;
  int uselogfile=0;
  int iamsplitter=0;

  setvbuf(stdout, 0, _IONBF, 0);
  setvbuf(stderr, 0, _IONBF, 0);

  res = WSAStartup(0x0202, &wsadata);
	if (res){
		fprintf(stderr, "WSAStartup failed: %d\n", res);
    exit(1);
	}

  tinitScheduler(0);

  argv = convertargv(argc, (void**) av);
  // remove path from argv[0]
  argv0 = strrchr(argv[0], '\\');
  if (!argv0) argv0 = argv[0];
  else ++argv0;

  badargs=0;
  while ((c = getopt(argc,argv, "g")) != -1){
    switch(c){
    case 'g':
      uselogfile = 1;
      break;
    default:
      ++badargs;
    }
  }
  if (badargs) exit(1); // bad arguments
  argc -= optind;

  // parse arguments
  switch(argc){
  case 0:
    myport = SERVER_DEFAULT_PORT; // use default port
    break;
  case 1:
	  myport = atoi(argv[optind]);
    break;
  default:
    fprintf(stderr, "usage: %s [-g] [portno]\n", argv0);
    fprintf(stderr, "  where portno refers to the port number of the corresponding storageserver, not the portno to use for the splitter server\n");
    exit(1);
  }

  InitServerSplitter(); // sets up SC
  ConfigState *CS = SC->CS;
  RPCClientLight *rpcc = &SC->Rpcc;
  myip = IPMisc::getMyIP(CS->PreferredPrefix16);
  myipport.set(myip, htons(myport));
  UniqueId::init(myip);

  // try to find ourselves in host list of configuration file
  HostConfig *hc;
  hc = CS->Hosts.lookup(&myipport);
  if (hc==0){
    fprintf(stderr, "Cannot find my ip %08x and port %d in config file\n",
      ntohl(myipport.ip), ntohs(myipport.port));
    exit(1);
  }
  printf("SPLITTER Compilation time %s %s configuration %s\n", __DATE__, __TIME__, COMPILECONFIG);
  printf("Host %s ip %d.%d.%d.%d port %d\n", hc->hostname, 
    myip & 0xff, (myip >> 8) & 0xff, (myip >> 16) & 0xff, (myip >> 24) & 0xff,
    hc->port+SPLITTER_PORT_OFFSET);

  if (IPPort::cmp(CS->Hosts.getFirst()->ipport, myipport)==0) iamsplitter=1;
  else iamsplitter=0;

  if (!iamsplitter){
    printf("Error: I am not the first node in config file, to be a splitter\n");
    exit(1);
  }

  // debugging information stuff
#if (!defined(NDEBUG) && defined(DEBUG) || defined(NDEBUG) && defined(DEBUGRELEASE))
  if (!uselogfile){
    SetDebugLevel(1);
    DebugInit(false);
  } else {
    SetDebugLevel(DEBUG_LEVEL_WHEN_LOGFILE);
    DebugInit(true);
  }
#endif

  assert(hc->port != 0);
  int myrealport = hc->port+SPLITTER_PORT_OFFSET;

#ifdef GAIAUDP
  RPCServer server(RPCProcs, sizeof(RPCProcs)/sizeof(RPCProc), myrealport, WORKERTHREADS);
#else
  RPCServer server(RPCProcs, sizeof(RPCProcs)/sizeof(RPCProc), myrealport);
#endif

  //ServerPtr = &server;

  WARNING_INIT();

  server.launch(0); 

#ifndef GAIAUDP
  server.waitServerEnd(); // should never return
#else
  Sleep(INFINITE);
#endif
    
  UninitServerSplitter();

#if (!defined(NDEBUG) && defined(DEBUG) || defined(NDEBUG) && defined(DEBUGRELEASE))
  DebugUninit();
#endif

  WSACleanup();
}
