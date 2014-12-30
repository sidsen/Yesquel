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
#include "../../options.h"
#include "../gaiatypes.h"
#include "../util.h"
#include "../datastruct.h"
#include "../clientlib/clientlib.h"
#include "../clientlib/clientdir.h"
#include "../../dtreeaux.h"

#include "../../dtreesplit.cpp"

#define CONFIGFILENAME "config.txt"

const u8 MYSITE=0;

StorageConfig *SC=0;

struct SplitterWork {
  SplitterWork(){}
  SplitterWork(COid c) : coid(c), unconditional(false) { when = Time::now(); }
  SplitterWork(COid c, bool uncond) : coid(c), unconditional(uncond) { when = Time::now(); }
  u64 when;
  COid coid;
  bool unconditional; // if true, do not apply recency test before splitting
  SplitterWork *next, *prev;
};

RWLock SplitterWork_l;
LinkList<SplitterWork> SplitterWorkQueue;
Semaphore SplitterSem;
HANDLE SplitterThrId;

// forward definition
int ServerDoSplit(COid coid, int where, void *uncond);

//  RecentSplits(int periodms, void (*dowork)(COid, void*), void *parm, EventScheduler *es)

void SplitterWorker(COid coid, void *parm){
  int res;
  do { // this loop repeats until split is successful
    putchar('S'); fflush(stdout);
    res = DtSplit(coid, 0, false, true, ServerDoSplit, (void*) 1);
    if (res){ 
      Sleep(20); 
      putchar('X'); fflush(stdout); 
    }
    else { putchar('O'); fflush(stdout); }
  } while (res);
}

DWORD WINAPI SplitterThread(void *parm){
  SplitterWork *sw;
  COid coid;
  EventScheduler es("SplitterThread");
  RecentSplits recent(DTREE_AVOID_DUPLICATE_INTERVAL, SplitterWorker, (void*) 0, &es);

  es.launch();

  while (1){
    Sleep(20);
    while (SplitterSem.wait(INFINITE));
    SplitterWork_l.lock();
    if (!SplitterWorkQueue.Empty())
      sw = SplitterWorkQueue.PopHead();
    else sw = 0;
    SplitterWork_l.unlock();
    if (sw){
      coid = sw->coid;
      recent.submit(coid);
      delete sw;
    }
  }
}

extern void KVInterfaceInit(void);

void InitServerSplitter(){
  char *configfile = getenv("GAIACONFIG");
  if (!configfile) configfile = CONFIGFILENAME;
  SC = new StorageConfig(configfile, MYSITE);
  KVInterfaceInit();
  SplitterThrId = CreateThread(0, 0, SplitterThread, 0, 0, 0); assert(SplitterThrId);
}

void UninitServerSplitter(){
  if (SC) delete SC;
  SC = 0;
}

// Enqueues a request to split a node.
// where=0 means put request at head (to be done first)
// where=1 means put request at tail (to be done last)
int ServerDoSplit(COid coid, int where, void *uncond){
  DTreeNode node;
  bool unconditional = uncond != 0 ? true : false;
  SplitterWork_l.lock();
  if (where==0) SplitterWorkQueue.PushHead(new SplitterWork(coid, unconditional));
  else SplitterWorkQueue.PushTail(new SplitterWork(coid, unconditional));
  SplitterWork_l.unlock();
  SplitterSem.signal();
  return 0;
}
