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
// disklog-win.h
//

#ifndef _DISKLOG_H
#define _DISKLOG_H

#include "../gaiaoptions.h"

#define ALIGNBUFSIZE 512    // must be power of 2

#define ALIGNLEN(x)  ((x) & ~(ALIGNBUFSIZE-1)) // clear low bits of x so it is aligned
#define ALIGNMOD(x)  ((x) & (ALIGNBUFSIZE-1))    // low bits of x

#include <list>
#include "../tmalloc.h"
#include "../gaiatypes.h"
#include "pendingtx.h"
//#include "../task.h"

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
  int ncoids;   // number of objects in this entry
};

struct WriteQueueItemBuf {
  int tofree; // whether to free buf afterwards
  char *buf;
  int len;
};

struct WriteQueueItemUpdates {
  Tid tid;
  Timestamp ts;
  Ptr<PendingTxInfo> pti;
};

struct WriteQueueItem {
  int utype;
  struct {
    WriteQueueItemBuf buf; // type 0
    WriteQueueItemUpdates updates; // type 1
  } u;

  void *notify;         // taskinfo to notif
  WriteQueueItem *next; // linklist
  WriteQueueItem(){ utype = -1; }
  ~WriteQueueItem(){
    if (utype==0 && u.buf.tofree && u.buf.buf) delete u.buf.buf;
  }
};

struct DiskLogThreadContext {
  WriteQueueItem *ToShipHead, *ToShipTail;
  DiskLogThreadContext(){
    ToShipHead = ToShipTail = new WriteQueueItem;
    memset(ToShipHead, 0, sizeof(WriteQueueItem));
    ToShipHead->next = 0;
  }
  ~DiskLogThreadContext(){
    WriteQueueItem *wqi, *next;
    wqi = ToShipHead;
    while (wqi){
      next = wqi->next;
      delete wqi;
      wqi = next;
    }
  }
};

class TaskInfo;

class DiskLog {
private:
  HANDLE f; // file handle

  char *RawWritebuf;     // unaligned buffer as returned by new()
  char *Writebuf;        // aligned buffer to be used
  unsigned WritebufSize; // length of aligned buffer
  int WritebufLeft;      // number of bytes left in buffer
  char *WritebufPtr;     // current location in buffer
  u64 FileOffset;        // current offset in file being written

  RWLock WriteQueue_l;                              // lock to protect write queue
  WriteQueueItem *WriteQueueHead, *WriteQueueTail;  // head and tail of write queue
  EventSync WriteQueueReadyEvent;                   // event to indicate when write queue gets new element

  RWLock NotifyQueue_l;                               // lock to protect notify queue
  WriteQueueItem *NotifyQueueHead, *NotifyQueueTail;  // head and tail of notify queue
  EventSync NotifyQueueReadyEvent;                    // event to indicate when notify queue gets new element

  void BufWrite(char *buf, int len);   // buffers a write to disk; calls AlignWrite
  void AlignWrite(char *buf, int len); // aligns a buffer, then calls auxwrite
  void auxwrite(char *buf, int len);   // writes an aligned buffer
  void BufFlush(void);                 // flushes write done

  void writeWqi(WriteQueueItem *wqi);  // writes a WriteQueueItem (calls BufWrite several times)

  static DWORD WINAPI writeQueueWorker(void *data);
  static int PROGShipDiskReqs(TaskInfo *ti);
  static DWORD WINAPI diskEnqueuerThread(void *parm);

public:
  DiskLog(char *logname);
  ~DiskLog();
  void launch(void);  // launches both writeQueueWorker and diskEnqueuerThread

  // --------- client functions, called to log various things -------------------

  // Logs updates and Yes Vote
  // Returns 0 if log has been done, non-zero if log being done in backgruond
  // Notification happens only if it returns non-zero
  static int logUpdatesAndYesVote(Tid tid, Timestamp ts, Ptr<PendingTxInfo> pti, void *notify); 

  // log a commit record
  static void logCommitAsync(Tid tid, Timestamp ts);
  // log an abort record
  static void logAbortAsync(Tid tid, Timestamp ts);
};

#endif
