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
// disklog.h
//

#ifndef _DISKLOG_H
#define _DISKLOG_H

//#define SKIPLOG
#define SKIPFFLUSH

#include <list>
#include "../gaiatypes.h"
#include "pendingtx.h"

using namespace std;

enum LogEntryType { LEMultiWrite, LECommit, LEAbort, LEVoteYes };

struct LogEntry {
  LogEntryType let; // for LECommit, LEAbort, LEVoteYes
  Tid tid;
  Timestamp ts;
};

struct MultiWriteLogEntry {
  LogEntryType let; // for LEMultiWrite
  Tid tid;
  Timestamp ts;
  int nwrites;
};

struct MultiWriteLogSubEntry {
  COid coid;
  int off;
  int len;
};

class DiskLog {
private:
  FILE *f;
  void logAsync(LogEntry *le);
  struct WriteQueueItemBuf {
    int tofree; // whether to free buf afterwards
    char *buf;
    int len;
  };
  struct WriteQueueItemWrites {
    Tid tid;
    Timestamp ts;
    list<TxWriteItem *> *writeSet;
  };

  struct WriteQueueItem {
    int utype;
    union {
      WriteQueueItemBuf buf; // type 0
      WriteQueueItemWrites writes; // type 1
    } u;

    Semaphore *towake;
    // linklist stuff
    WriteQueueItem *next, *prev;
  };

  RWLock WriteQueue_l;
  LinkList<WriteQueueItem> WriteQueue;
  CondVar WriteQueue_cond;

  void addWriteQueue(WriteQueueItem *wqi);
  void addWriteQueueWrites(Tid tid, Timestamp ts, list<TxWriteItem *> *writeset);
  static DWORD WINAPI writeQueueWorker(void *data);

public:
  DiskLog(char *logname);
  ~DiskLog();
  void launch(void);
  void logWritesAndYesVote(Tid tid, Timestamp ts, list<TxWriteItem *> *writeSet);
  void logCommitAsync(Tid tid, Timestamp ts);
  void logAbortAsync(Tid tid, Timestamp ts);
};


#endif
