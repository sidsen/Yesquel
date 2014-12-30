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
// disklog.cpp
//

#include "stdafx.h"
#include "../tmalloc.h"
#include "disklog-win.h"
#include "diskstorage.h"

#ifdef SKIPFFLUSH
// simple version without flushing to disk
void DiskLog::addWriteQueue(WriteQueueItem *wqi){}
DWORD WINAPI DiskLog::writeQueueWorker(void *data){}

void DiskLog::launch(void){}

void DiskLog::logAsync(LogEntry *le){
  int res;
#ifndef SKIPLOG
  res = fwrite((void*) le, 1, sizeof(LogEntry), f); assert(res == sizeof(LogEntry));
#endif
}

void DiskLog::logWritesAndYesVote(Tid tid, Timestamp ts, list<TxWriteItem *> *writeSet){
  MultiWriteLogEntry mwle;
  MultiWriteLogSubEntry mwlse;

#ifndef SKIPLOG
  int res;
  mwle.let = LEMultiWrite;
  mwle.tid = tid;
  mwle.ts = ts;
  mwle.nwrites = writeSet->size();
  res = fwrite((void*) &mwle, 1, sizeof(MultiWriteLogEntry), f); assert(res==sizeof(MultiWriteLogEntry));
  for (list<TxWriteItem *>::iterator it = writeSet->begin(); it != writeSet->end(); ++it){
    mwlse.coid = (*it)->coid;
    mwlse.off = (*it)->off;
    mwlse.len = (*it)->len;
    res = fwrite((void*) &mwlse, 1, sizeof(MultiWriteLogSubEntry), f); assert(res==sizeof(MultiWriteLogSubEntry));
    res = fwrite((void*) (*it)->buf, 1, mwlse.len, f); assert(res==mwlse.len);
  }
  // log a yes vote
  LogEntry le;
  le.let = LEVoteYes;
  le.tid = tid;
  le.ts.setIllegal();
  res = fwrite((void*)&le, 1, sizeof(LogEntry), f); assert(res==sizeof(LogEntry));

#endif
}

DiskLog::DiskLog(char *logname){
  char *str, *ptr, *lastptr;

  str = new char[strlen(logname)+1];
  strcpy(str, logname);

  // find the last separator in logname
  ptr = str;
  do {
    lastptr = ptr;
    ptr = DiskStorage::searchseparator(ptr);
  } while (*ptr);
  *lastptr = 0;
  
  // create path up to filename
  DiskStorage::Makepath(str);

  f = fopen(logname, "wbcS");
  if (!f){ perror(logname); exit(1); }
  delete [] str;
}
DiskLog::~DiskLog(){
  fclose(f);
}

#else // ifdef SKIPFFLUSH
// version that uses queues to flush to disk synchronously
void DiskLog::addWriteQueue(WriteQueueItem *wqi){
  WriteQueue_l.lock();
  WriteQueue.PushTail(wqi);
  WriteQueue_l.unlock();
  WriteQueue_cond.wakeOne();
}

DWORD WINAPI DiskLog::writeQueueWorker(void *data){
  int res;
  LinkList<WriteQueueItem> ToProcess;
  WriteQueueItem *wqi, *next;
  DiskLog *dl = (DiskLog*) data;
  while (1){
    dl->WriteQueue_l.lock();
    while (dl->WriteQueue.Empty()){
      // no work, so sleep on queue
      dl->WriteQueue_cond.unlockSleepLock(&dl->WriteQueue_l, INFINITE);
    }
    // transfer to another queue
    while (!dl->WriteQueue.Empty()){
      wqi = dl->WriteQueue.PopHead();
      ToProcess.PushTail(wqi);
    }
    // release lock to let other threads enqueue their requests
    dl->WriteQueue_l.unlock();

    // write to disk and flush
    for (wqi = ToProcess.GetFirst(); wqi != ToProcess.GetLast(); wqi = ToProcess.GetNext(wqi)){
      if (wqi->utype == 0){
        res = fwrite((void*)wqi->u.buf.buf, 1, wqi->u.buf.len, dl->f); assert(res==wqi->u.buf.len);
      } else { // wqi->utype == 1

        MultiWriteLogEntry mwle;
        MultiWriteLogSubEntry mwlse;
        list<TxWriteItem *> *writeSet = wqi->u.writes.writeSet;

        int res;
        mwle.let = LEMultiWrite;
        mwle.tid = wqi->u.writes.tid;
        mwle.ts = wqi->u.writes.ts;
        mwle.nwrites = writeSet->size();
        res = fwrite((void*) &mwle, 1, sizeof(MultiWriteLogEntry), dl->f); assert(res==sizeof(MultiWriteLogEntry));
        for (list<TxWriteItem *>::iterator it = writeSet->begin(); it != writeSet->end(); ++it){
          mwlse.coid = (*it)->coid;
          mwlse.off = (*it)->off;
          mwlse.len = (*it)->len;
          res = fwrite((void*) &mwlse, 1, sizeof(MultiWriteLogSubEntry), dl->f); assert(res==sizeof(MultiWriteLogSubEntry));
          res = fwrite((void*) (*it)->buf, 1, mwlse.len, dl->f); assert(res==mwlse.len);
        }
        // log a yes vote
        LogEntry le;
        le.let = LEVoteYes;
        le.tid = wqi->u.writes.tid;
        le.ts.setIllegal();
        res = fwrite((void*)&le, 1, sizeof(LogEntry), dl->f); assert(res==sizeof(LogEntry));
      }
    }
    fflush(dl->f);

    // signal requestor and free up entries
    wqi = ToProcess.GetFirst();
    while (wqi != ToProcess.GetLast()){
      next = ToProcess.GetNext(wqi);
      if (wqi->towake) wqi->towake->signal();
      ToProcess.Remove(wqi);
      if (wqi->utype==0 && wqi->u.buf.tofree)
        delete [] wqi->u.buf.buf;
      delete wqi;
      wqi = next;
    }
  }
}

void DiskLog::logAsync(LogEntry *le){
  int res;
  char *buf;
  int len;
  len = sizeof(LogEntry);
  buf = new char[len];
  memcpy((void*)buf, (void*)le, len);

  WriteQueueItem *wqi = new WriteQueueItem;
  wqi->utype = 0;
  wqi->u.buf.tofree = 1;
  wqi->u.buf.buf = buf;
  wqi->u.buf.len = len;
  wqi->towake = 0;
  addWriteQueue(wqi);
}


void DiskLog::logWritesAndYesVote(Tid tid, Timestamp ts, list<TxWriteItem *> *writeSet){
  Semaphore sem;
  WriteQueueItem *wqi = new WriteQueueItem();
  wqi->utype = 1;
  wqi->u.writes.tid = tid;
  wqi->u.writes.ts = ts;
  wqi->u.writes.writeSet = writeSet;
  wqi->towake = &sem;
  addWriteQueue(wqi);
  sem.wait(INFINITE);
}

DiskLog::DiskLog(char *logname){
  void *thread;
  char *str, *ptr, *lastptr;

  str = new char[strlen(logname)+1];
  strcpy(str, logname);

  // find the last separator in logname
  ptr = str;
  do {
    lastptr = ptr;
    ptr = DiskStorage::searchseparator(ptr);
  } while (*ptr);
  *lastptr = 0;
  
  // create path up to filename
  DiskStorage::Makepath(str);

  f = fopen(logname, "wbcS");
  if (!f){ perror(logname); exit(1); }
  thread = CreateThread(0, 0, writeQueueWorker, (void*) this, 0, 0); assert(thread);
  delete [] str;
}
DiskLog::~DiskLog(){
  fclose(f);
}

#endif





// ----------------------------------------------------------------------------


void DiskLog::logCommitAsync(Tid tid, Timestamp ts){
  LogEntry le;
  le.let = LECommit;
  le.tid = tid;
  le.ts = ts;
  logAsync(&le);
}

void DiskLog::logAbortAsync(Tid tid, Timestamp ts){
  LogEntry le;
  le.let = LEAbort;
  le.tid = tid;
  le.ts = ts;
  logAsync(&le);
}
