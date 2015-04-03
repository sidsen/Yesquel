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

// callsplitter.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "../gaia/tmalloc.h"
#include "../gaia/gaiaoptions.h"
#include "../gaia/debug.h"
#include "../splitter-server/splitter-client.h"
#include "../gaia/clientlib/clientdir.h"


SplitterDirectory *SD=0;
StorageConfig *SC=0;

#pragma comment(lib, "ws2_32.lib")

#ifdef DEBUG
extern int DebugLevel;
#endif

void doSplit(COid tosplit){
  CallSplitter(tosplit, (void*) 1); // indicates node to split is a leaf. Does not matter much here since this is mostly
  // intended to deal with concurrency. This program is likely to be called only when there are no other splitting
  // activity in the system.
}

struct SplitNodeCallbackState {
  EventSync es;
  IPPort ipport;
  int status;
  SplitterLoadStatus load;
  int haspending;
};

void doReportStatusCallback(char *data, int len, void *callbackdata){
  SplitnodeRPCRespData resp;
  assert(SD);
  if (data){
    SplitNodeCallbackState *sncs = (SplitNodeCallbackState*) callbackdata;
    resp.demarshall(data);
    sncs->status = resp.data->status;
    sncs->load = resp.data->load;
    sncs->haspending = resp.data->haspending;
    sncs->es.set();
  }
  return; // free buffer
}

void doReportStatus(void){
  HostConfig *hc;
  SplitnodeRPCData *parm;
  COid coid;
  coid.cid = 0;
  coid.oid = 0;
  SplitNodeCallbackState *callbacks;
  int nhosts;
  int hostno;

  nhosts = SD->getNHosts();
  callbacks = new SplitNodeCallbackState[nhosts];

  for (hostno = 0, hc = SD->getFirstHost(); hc != SD->getLastHost(); ++hostno, hc = SD->getNextHost(hc)){
    assert(SD);
    assert(SC);

    //printf("Invoking split at server %08x port %d\n", hc->ipport.ip, hc->ipport.port); fflush(stdout);
    parm = new SplitnodeRPCData;
    parm->data = new SplitnodeRPCParm;
    parm->data->getstatusonly = 1;
    parm->data->coid = coid;
    parm->data->wait = 0;
    parm->freedata = true;
    callbacks[hostno].ipport = hc->ipport;
    SC->Rpcc.AsyncRPC(hc->ipport, SS_SPLITNODE_RPCNO, FLAG_HID((u32)coid.oid), parm, doReportStatusCallback, (void*) &callbacks[hostno]);
  }

  for (hostno = 0; hostno < nhosts; ++hostno){
    // wait for responses
    callbacks[hostno].es.wait();
  }

  for (hostno = 0; hostno < nhosts; ++hostno){
    printf("Splitter %I64x status %d haspending %d queuesize %d splittimeavg %f splittimestd %f splitretryingms %I64d\n", callbacks[hostno].ipport,
      callbacks[hostno].status, callbacks[hostno].haspending, callbacks[hostno].load.splitQueueSize, callbacks[hostno].load.splitTimeAvg,
      callbacks[hostno].load.splitTimeStddev, callbacks[hostno].load.splitTimeRetryingMs);
  }
  delete [] callbacks;
}

int _tmain(int argc, _TCHAR* av[])
{
  int res;
  char *Configfile=0;
  WSADATA wsadata;
  int mysite=0;  // default site is 0
  int badargs, c;
  int cmd=-1;
  int debuglevel=MININT;

  COid coid;
  int getstatusonly=0;

  setvbuf(stdout, 0, _IONBF, 0);
  setvbuf(stderr, 0, _IONBF, 0);

  char **argv = convertargv(argc, (void**) av);
  // remove path from argv[0]
  char *argv0 = strrchr(argv[0], '\\');
  if (argv0) ++argv0; else argv0 = argv[0];

  badargs=0;
  while ((c = getopt(argc,argv, "o:d:")) != -1){
    switch(c){
    case 'd':
      debuglevel = atoi(optarg);
      break;
    case 'o':
      Configfile = (char*) malloc(strlen(optarg)+1);
      strcpy(Configfile, optarg);
      break;
    default:
      ++badargs;
    }
  }
  if (badargs) exit(1); // bad arguments
  argc -= optind;

  // parse arguments
  switch(argc){
  case 0: // get status only
    getstatusonly = 1;
    break;
  case 2: // split given coid
    getstatusonly = 0;
    sscanf(argv[optind], "%I64x", &coid.cid);
    sscanf(argv[optind+1], "%I64x", &coid.oid);
    break;
  default:
    fprintf(stderr, "usage: %s [-o config] [-d debuglevel] [cid oid]\n", argv0);
    fprintf(stderr, "       without [cid oid], returns status from each splitter server\n");
    exit(1);
  }


  res = WSAStartup(0x0202, &wsadata);
	if (res){
		fprintf(stderr, "WSAStartup failed: %d\n", res);
		return 1;
	}

  UniqueId::init();

  SC = new StorageConfig(GAIA_DEFAULT_CONFIG_FILENAME, (u8)mysite);
  SD = new SplitterDirectory(SC->CS, mysite);

#if !defined(GAIAUDP) && !defined(STORAGESERVER_SPLITTER)
  CallSplitterConnect(); // connect to splitter server(s)
#endif

#ifdef DEBUG
  if (debuglevel != MININT) DebugLevel = debuglevel;
#endif

  if (getstatusonly) doReportStatus();
  else doSplit(coid);

  printf("Done\n");

  delete SD;
  delete SC;

  WSACleanup();
  exit(0);
}
