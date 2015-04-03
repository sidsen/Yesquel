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
// disklog-win.cpp
//

#include "stdafx.h"
#include "../tmalloc.h"
#include "pendingtx.h"
#include "disklog-win.h"
#include "diskstorage.h"

#ifdef SKIPLOG
DiskLog::DiskLog(char *logname){
  RawWritebuf = Writebuf = 0;
  WritebufSize = WritebufLeft = 0;
  WritebufPtr = 0;
  FileOffset = 0;
  WriteQueueHead = WriteQueueTail = 0;
  NotifyQueueHead = NotifyQueueTail = 0;
}

DiskLog::~DiskLog(){}
void DiskLog::writeWqi(WriteQueueItem *wqi){}
void DiskLog::logCommitAsync(Tid tid, Timestamp ts){}
void DiskLog::logAbortAsync(Tid tid, Timestamp ts){}
int DiskLog::logUpdatesAndYesVote(Tid tid, Timestamp ts, Ptr<PendingTxInfo> pti, void *notify){
  return 0; // indicates no notification will happen
}
void DiskLog::launch(void){}

#else
#include "../task.h"

static void logAsync(LogEntry *le);  // auxilliary function called by logCommitAsync and logAbortAsync
static DWORD WINAPI writeQueueWorker(void *data);   // worker who will do the actual writing. Both writeQueueWorker and diskEnqueuerThread run in the same core,
                                                    // with writeQueueWorker having a higher thread priority
static DWORD WINAPI diskEnqueuerThread(void *parm); // creates and runs task PROGShipDiskReqs, which receives a write requests and enqueues it for worker to process
static void SendDiskLog(WriteQueueItem *wqi);  // sends a write request to the PROGShipDiskReqs task
static void ImmediateFuncEnqueueDiskReq(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread);  // immediate function to enqueue a disk request for the PROGShipDiskReqs task
static int PROGShipDiskReqs(TaskInfo *ti);


DiskLog::DiskLog(char *logname){
  char *str, *ptr, *lastptr;
  int res;

  str = new char[strlen(logname)+1];
  strcpy(str, logname);

  // find the last separator in logname
  ptr = str;
  do {
    lastptr = ptr;
    ptr = DiskStorage::searchseparator(ptr);
  } while (*ptr);
  *lastptr = 0;

#ifdef SKIPFFLUSH
  RawWritebuf = Writebuf = 0;
  WritebufSize = WritebufLeft = 0;
#else
  // allocate writebuf
  RawWritebuf = new char[WRITEBUFSIZE];
  if ((unsigned)RawWritebuf & (ALIGNBUFSIZE-1)){ // does not align
    Writebuf = RawWritebuf + (ALIGNBUFSIZE - ((unsigned)RawWritebuf & (ALIGNBUFSIZE-1)));
  } else Writebuf = RawWritebuf;
  assert(((unsigned)Writebuf & (ALIGNBUFSIZE-1))==0 && Writebuf >= RawWritebuf);
  WritebufSize = (int)(RawWritebuf + WRITEBUFSIZE - Writebuf);
  WritebufLeft = WritebufSize;
#endif

  WritebufPtr = Writebuf;
  FileOffset = 0;

  // allocate dummy nodes for WriteQueue and NotifyQueue
  WriteQueueHead = WriteQueueTail = new WriteQueueItem;
  memset(WriteQueueHead, 0, sizeof(WriteQueueItem));
  WriteQueueHead->next = 0;
  NotifyQueueHead = NotifyQueueTail = new WriteQueueItem;
  memset(NotifyQueueHead, 0, sizeof(WriteQueueItem));
  WriteQueueHead->next = 0;


  // create path up to filename
  DiskStorage::Makepath(str);

  //  convert logname to wstr
  wchar_t lognamewstr[1024];
  size_t ret;
  res = mbstowcs_s(&ret, lognamewstr, 1024, logname, _TRUNCATE); assert(res==0);

#ifndef SKIPFFLUSH
  f = CreateFile(lognamewstr, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | 
    FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH, 0);
#else
  f = CreateFile(lognamewstr, GENERIC_WRITE, 0, 0, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, 0);
#endif

  if (f == INVALID_HANDLE_VALUE){ 
    fprintf(stderr, "Cannot open logfile %s error %d", logname, GetLastError());
    exit(1);
  }
  delete [] str;
}

DiskLog::~DiskLog(){
  CloseHandle(f);
  if (RawWritebuf) delete [] RawWritebuf;
}

// auxilliary function for disklog write to log a WriteQueueItem

void DiskLog::writeWqi(WriteQueueItem *wqi){
  int type;
  int celltype;

  if (wqi->utype == 0){
    BufWrite(wqi->u.buf.buf, wqi->u.buf.len);
  } else { // wqi->utype == 1

    MultiWriteLogEntry mwle;
    //MultiWriteLogSubEntry mwlse;
    //list<TxWriteItem *> *writeSet = wqi->u.writes.writeSet;
    Ptr<PendingTxInfo> pti = wqi->u.updates.pti;

    // write header
    mwle.let = LEMultiWrite;
    mwle.tid = wqi->u.updates.tid;
    mwle.ts = wqi->u.updates.ts;
    mwle.ncoids = pti->coidinfo.nitems;
    BufWrite((char*) &mwle, sizeof(MultiWriteLogEntry));

    // iterator over all objects
    SkipListNode<COid, Ptr<TxInfoCoid> > *it;
    for (it = pti->coidinfo.getFirst(); it != pti->coidinfo.getLast(); it = pti->coidinfo.getNext(it)){
      Ptr<TxInfoCoid> ticoid = it->value;
      if (ticoid->Writevalue) type = 1;
      else if (ticoid->WriteSV) type = 2;
      else type = 0;
      BufWrite((char*) &type, sizeof(int));

      if (type == 0){ // write a delta record
        int len;
        BufWrite((char*)ticoid->SetAttrs, GAIA_MAX_ATTRS);
        BufWrite((char*)ticoid->Attrs, sizeof(u64)*GAIA_MAX_ATTRS);
        len = (int) ticoid->Litems.size(); // number of items
        BufWrite((char*)&len, sizeof(int));
        // for each item
        for (list<TxListItem*>::iterator it=ticoid->Litems.begin(); it != ticoid->Litems.end(); ++it){
          TxListItem *tli = *it;
          BufWrite((char*)&tli->type, sizeof(int));
          if (tli->type == 0){
            TxListAddItem *tlai = dynamic_cast<TxListAddItem*>(tli);
            // item
            BufWrite((char*)&tlai->item.nKey, sizeof(int));
            if (!tlai->item.pKey) celltype=0; // int key
            else celltype=1;
            BufWrite((char*)&celltype, sizeof(int));
            if (celltype) BufWrite((char*)tlai->item.pKey, (int) tlai->item.nKey);
            BufWrite((char*) &tlai->item.value, sizeof(u64));
          } else { // tli->type == 1
            TxListDelRangeItem *tldri = dynamic_cast<TxListDelRangeItem*>(tli);
            BufWrite((char*) &tldri->intervalType, 1);
            // itemstart
            BufWrite((char*) &tldri->itemstart.nKey, sizeof(int));
            if (!tldri->itemstart.pKey) celltype=0; // int key
            else celltype = 1;
            BufWrite((char*) &celltype, sizeof(int));
            if (celltype) BufWrite((char*)tldri->itemstart.pKey, (int)tldri->itemstart.nKey);
            BufWrite((char*) &tldri->itemstart.value, sizeof(u64));
            // itemend
            BufWrite((char*) &tldri->itemend.nKey, sizeof(int));
            if (!tldri->itemend.pKey) celltype=0; // int key
            else celltype = 1;
            BufWrite((char*) &celltype, sizeof(int));
            if (celltype) BufWrite((char*)tldri->itemend.pKey, (int)tldri->itemend.nKey);
            BufWrite((char*) &tldri->itemend.value, sizeof(u64));
          }
        }
      } else if (type == 1){ // write a value record
        TxWriteItem *twi = ticoid->Writevalue;
        BufWrite((char*) &twi->len, sizeof(int));
        BufWrite((char*)twi->buf, twi->len);
      } else { // type == 2
        // write a supervalue record
        TxWriteSVItem *twsvi = ticoid->WriteSV;
        BufWrite((char*)twsvi, offsetof(TxWriteSVItem, attrs)); // header
        BufWrite((char*)twsvi->attrs, sizeof(u64) * twsvi->nattrs);
        BufWrite((char*) &twsvi->cells.nitems, sizeof(int)); // number of cells
        // for each cell
        SkipListNodeBK<ListCellPlus,int> *ptr;
        for (ptr = twsvi->cells.getFirst(); ptr != twsvi->cells.getLast(); ptr = twsvi->cells.getNext(ptr)){
          ListCellPlus *lc = ptr->key;
          BufWrite((char*) &lc->nKey, sizeof(int));
          if (!lc->pKey) celltype=0; // int key
          else celltype = 1;
          BufWrite((char*) &celltype, sizeof(int));
          if (celltype) BufWrite((char*)lc->pKey, (int)lc->nKey);
          BufWrite((char*) &lc->value, sizeof(u64));
        }
      }
    }
    // log a yes vote
    LogEntry le;
    le.let = LEVoteYes;
    le.tid = wqi->u.updates.tid;
    le.ts.setIllegal();
    BufWrite((char*) &le, sizeof(LogEntry));
  }
}


DWORD WINAPI DiskLog::writeQueueWorker(void *data){
  WriteQueueItem *wqi, *lastwqi=0;
  WriteQueueItem *ToProcess = 0;
  DiskLog *dl = (DiskLog*) data;
  int res;

  res = SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_ABOVE_NORMAL);
  if (!res){ printf("Warning: writeQueueWorker cannot set thread priority: %d\n", GetLastError()); fflush(stdout); }

  pinThread(taskGetCore("DISKLOG"));

  while (1){
     dl->WriteQueueReadyEvent.wait();
     dl->WriteQueue_l.lock();
     // detach WriteQueue and set it to ToProcess. ToProcess does not have dummy head
     ToProcess = dl->WriteQueueHead->next;
     dl->WriteQueueTail = dl->WriteQueueHead;
     dl->WriteQueueHead->next = 0;

     dl->WriteQueueReadyEvent.reset();
     dl->WriteQueue_l.unlock();

     if (!ToProcess) continue; // nothing to process

    // write to disk and flush
    //bytesleft = WRITEBUFSIZE;
    for (wqi = ToProcess; wqi != 0; wqi = wqi->next){
      lastwqi = wqi;
      dl->writeWqi(wqi);
    }
    dl->BufFlush();

    dl->NotifyQueue_l.lock();
    dl->NotifyQueueTail->next = ToProcess;
    // append ToProcess to NotifyQueue
    assert(lastwqi && lastwqi->next==0);
    dl->NotifyQueueTail = lastwqi;
    dl->NotifyQueue_l.unlock();
    dl->NotifyQueueReadyEvent.set();
  }
}

void logAsync(LogEntry *le){
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
  wqi->notify = 0;
  SendDiskLog(wqi);
}

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

int DiskLog::logUpdatesAndYesVote(Tid tid, Timestamp ts, Ptr<PendingTxInfo> pti, void *notify){
  WriteQueueItem *wqi = new WriteQueueItem();
  wqi->utype = 1;
  wqi->u.updates.tid = tid;
  wqi->u.updates.ts = ts;
  wqi->u.updates.pti = pti;
  wqi->notify = notify;
  SendDiskLog(wqi);
  if (notify) return 1; // indicate that log will be done in the background, with notification happening subsequently
  else return 0; // indicate that no notification will happen
}

void ImmediateFuncEnqueueDiskReq(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread){
  DiskLogThreadContext *dltc = (DiskLogThreadContext*) tgetSharedSpace(THREADCONTEXT_SPACE_DISKLOG);
  WriteQueueItem *wqi = *(WriteQueueItem**) &msgdata;
  wqi->next = 0;

  // put disk request in tail of ToShip link list
  assert(dltc->ToShipTail->next == 0);
  dltc->ToShipTail->next = wqi;
  dltc->ToShipTail = wqi;
}

int DiskLog::PROGShipDiskReqs(TaskInfo *ti){
  DiskLogThreadContext *dltc = (DiskLogThreadContext*) tgetSharedSpace(THREADCONTEXT_SPACE_DISKLOG);
  DiskLog *dl;
  WriteQueueItem *ToNotify, *toshiphead, *toshiptail, *wqi, *next;
  int res;

  dl = (DiskLog*) ti->getTaskData();

  if (dltc->ToShipTail != dltc->ToShipHead){ // if ToShip not empty
    // detach ToShip link list
    toshiptail = dltc->ToShipTail;
    toshiphead = dltc->ToShipHead->next;
    dltc->ToShipTail = dltc->ToShipHead;
    dltc->ToShipHead->next = 0;

    dl->WriteQueue_l.lock();
    // append detached list to WriteQueue
    dl->WriteQueueTail->next = toshiphead;
    dl->WriteQueueTail = toshiptail;
    dl->WriteQueue_l.unlock();
    dl->WriteQueueReadyEvent.set();
  }

  res = dl->NotifyQueueReadyEvent.wait(0);
  if (res == 0){ // event was notified
    dl->NotifyQueue_l.lock();
    // detach NotifyQueue
    ToNotify = dl->NotifyQueueHead->next;
    dl->NotifyQueueTail = dl->NotifyQueueHead;
    dl->NotifyQueueHead->next = 0;

    dl->NotifyQueueReadyEvent.reset();
    dl->NotifyQueue_l.unlock();

    // send the notifications
    wqi = ToNotify;
    while (wqi){
      if (wqi->notify){
        // send a message to wqi->notify
        TaskMsg msg;
        msg.dest = (TaskInfo*) wqi->notify;
        msg.flags = 0;
        memset(&msg.data, 0, sizeof(TaskMsgData));
        msg.data.data[0] = 0xb0; // check byte only (message carries no relevant data; it is just a signal)
        tsendMessage(msg);
      }
      next = wqi->next;
      delete wqi;
      wqi = next;
    }
  }

  return SchedulerTaskStateRunning;
}

DWORD WINAPI DiskLog::diskEnqueuerThread(void *parm){
  DiskLog *dl = (DiskLog*) parm;
  TaskScheduler *ts = tgetTaskScheduler();
  TaskInfo *ti;
  DiskLogThreadContext *dltc;

  // assign immediate functions and tasks
  ts->assignImmediateFunc(IMMEDIATEFUNC_ENQUEUEDISKREQ, ImmediateFuncEnqueueDiskReq);
  dltc = new DiskLogThreadContext;
  tsetSharedSpace(THREADCONTEXT_SPACE_DISKLOG, dltc);
  ti = ts->createTask(PROGShipDiskReqs, dl);
  //ts->assignFixedTask(FIXED TASK NUMBER HERE, ti);

  ts->run();
  return -1;
}

void DiskLog::launch(void){
  void *thread;
  int threadno;
  thread = CreateThread(0, 0, writeQueueWorker, (void*) this, 0, 0); assert(thread);
  threadno = SLauncher->createThread("DISKLOG", diskEnqueuerThread, (void*) this, true);
  gContext.setNThreads(TCLASS_DISKLOG, 1); 
  gContext.setThread(TCLASS_DISKLOG, 0, threadno);
}

void SendDiskLog(WriteQueueItem *wqi){
  sendIFMsg(gContext.hashThread(TCLASS_DISKLOG, 0), IMMEDIATEFUNC_ENQUEUEDISKREQ, (void*) &wqi, sizeof(WriteQueueItem*));
}

#ifdef SKIPFFLUSH
// simple version without flushing to disk
void DiskLog::AlignWrite(char *buf, int len){}
void DiskLog::auxwrite(char *buf, int buflen){}
void DiskLog::BufFlush(){}

void DiskLog::BufWrite(char *buf, int len){
  int res;
  unsigned long written;

  while (len > 0){
    res = WriteFile(f, buf, len, &written, 0);
    if (res == 0){
      printf("WriteFile error %d\n", GetLastError());
      exit(1);
    }
    len -= written;
    buf += written;
  }
}

#else // ifdef SKIPFFLUSH
// version that uses queues to flush to disk synchronously

void DiskLog::AlignWrite(char *buf, int len){
  char *dumpbufraw, *dumpbuf;
  int buflen;
  buflen = WritebufSize - WritebufLeft;

  dumpbufraw = new char[buflen + len + ALIGNBUFSIZE-1]; // non-aligned
  if ((unsigned) dumpbufraw & (ALIGNBUFSIZE-1)){
    // align the buffer
    dumpbuf = dumpbufraw + (ALIGNBUFSIZE - ((unsigned) dumpbufraw & (ALIGNBUFSIZE-1)));
  }
  else dumpbuf = dumpbufraw;

  memcpy(dumpbuf, Writebuf, buflen);
  memcpy(dumpbuf + buflen, buf, len);
  auxwrite(dumpbuf, buflen + len);

  delete [] dumpbufraw;
}

// Writes buf to disk. Assumes buf is aligned and that it's
// legal to read it until buflen + the next align point.
// The last block written, if partial, is copied to the beginning of the
// Writebuf. WritebufLeft and WritebufPtr are set accordingly.
// Also sets FileOffset and seeks file handle so that they
// reflect the beginning of Writebuf.
void DiskLog::auxwrite(char *buf, int buflen){
  int res;
  unsigned long written;
  int writelen;

  // number of bytes to write, must be aligned, so move forward
  writelen = ALIGNLEN(buflen + (ALIGNBUFSIZE-1));

  // this loop writes writelen bytes starting at buf
  while (writelen > 0){
    res = WriteFile(f, buf, writelen, &written, 0);
    if (res == 0){
      printf("WriteFile error %d\n", GetLastError());
      exit(1);
    }
    writelen -= written;
    buf += written;
  }

  char *lastblock = Writebuf + ALIGNLEN(buflen); // beginning of last block written
  if (lastblock != Writebuf){
    // move last block to beginning to Writebuf
    memmove(Writebuf, lastblock, ALIGNMOD(buflen));
  }

  // adjust WritebufLeft and WritebufPtr
  WritebufPtr = Writebuf + ALIGNMOD(buflen);
  WritebufLeft = WritebufSize - ALIGNMOD(buflen);

  // adjust file offset
  FileOffset = FileOffset + ALIGNLEN(buflen); // offset at the beginning of buffer
  SetFilePointer(f, (u32)FileOffset, (long*)&FileOffset + 1, 0);
}

void DiskLog::BufFlush(void){
  auxwrite(Writebuf, WritebufSize - WritebufLeft);
}

void DiskLog::BufWrite(char *buf, int len){
  if (len < WritebufLeft){
    memcpy((void*) WritebufPtr, buf, len);
    WritebufPtr += len;
    WritebufLeft -= len;
  } else {
    BufFlush(); // flush buffer
    if (len < WritebufLeft){
      memcpy((void*) WritebufPtr, buf, len);
      WritebufPtr += len;
      WritebufLeft -= len;
    } else { // very big item, write it now
      AlignWrite(buf, len);
    }
  }
}

#endif // else SKIPFFLUSH
#endif // else SKIPLOG
