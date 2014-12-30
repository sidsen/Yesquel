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
#include "tmalloc.h"
#include "datastruct.h"
#include "tcpdatagram.h"
#include "util.h"
//#include "warning.h"

using namespace std;

//---------------------------------------------- RECEIVING ------------------------------------------------

// returns 0 if ok, non-zero if send queue is full
void TCPDatagramCommunication::sendHandleMsg(int threadno, TaskMultiBuffer *bufbase, char *buf, int len, IPPort ipport){
  TaskMsg msg;
  TaskMsgDataTCP *tmdt = (TaskMsgDataTCP *) &msg.data;
  TaskScheduler *ts;
  tmdt->bufbase = bufbase;
  tmdt->buf = buf;
  tmdt->len = len;
  tmdt->ipport = ipport;
  msg.dest = TASKID_CREATE(threadno, IMMEDIATEFUNC_HANDLEDISPATCHER);
  msg.flags = TMFLAG_IMMEDIATEFUNC | TMFLAG_FIXDEST;
  ts = SLauncher->getTaskScheduler(tgetThreadNo());
  ts->sendMessage(msg);
}

void TCPDatagramCommunication::updateState(ReceiveState &s, IPPort src, int len){
  DatagramMsgHeader *header;
  int totalsize;
  char *newbuf;
  TaskMultiBuffer *tmb;
  static int minsize=99999, maxsize=0;

  s.Ptr += len;
  s.Filled = (int)(s.Ptr - s.Buf);
  if (s.Filled < sizeof(DatagramMsgHeader)) return;
  header = (DatagramMsgHeader*) s.Buf;
  assert(header->cookie == REQ_HEADER_COOKIE);
  // totalsize is how much we are supposed to receive
  totalsize = sizeof(DatagramMsgHeader) + header->size;
  if (s.Filled < totalsize){ // didn't fill everything yet
    if (totalsize > s.Buflen){ // current buffer is too small; copy to new buffer
      newbuf = (char*) malloc(totalsize); assert(newbuf);
      memcpy(newbuf, s.Buf, s.Filled);
      free(s.Buf);
      s.Buf = newbuf;
      s.Buflen = totalsize;
      s.Ptr = newbuf+s.Filled;
    }
    return;
  }

  if (s.Filled == totalsize){   // we filled everything exactly
    // enqueue item, which will cause application handler to be called
    tmb =  new TaskMultiBuffer(s.Buf, 1); // TaskMultiBuffer is used to later free s.Buf
    // send message to Handler dispatcher on thread header->xid % NWorkerThreads
    sendHandleMsg(gContext.hashThread(TCLASS_WORKER, FLAG_GET_HID(header->flags)), tmb, s.Buf, header->size, src);

    s.Buf = (char*) malloc(TCP_RECLEN_DEFAULT); assert(s.Buf);
    s.Ptr = s.Buf;
    s.Buflen = TCP_RECLEN_DEFAULT;
    s.Filled = 0;
    return;
  }

  // Filled > totalsize
  unsigned extrasize, newlen;
  extrasize = s.Filled-totalsize;  // number of extra bytes we received
  char *extraptr = s.Buf + totalsize; // where the extra bytes are
  int sizenewreq;

#define MAXREQUESTSPERRECEIVE 10000
  char *bufs[MAXREQUESTSPERRECEIVE];
  int bufindex = 0;
  char *bufnewreq;

  bufs[bufindex] = s.Buf;
  ++bufindex; assert(bufindex < MAXREQUESTSPERRECEIVE);

  while (extrasize > sizeof(DatagramMsgHeader) && extrasize >= sizeof(DatagramMsgHeader) + ((DatagramMsgHeader*)extraptr)->size){
    // we have a full request to process
    sizenewreq = sizeof(DatagramMsgHeader) + ((DatagramMsgHeader*)extraptr)->size;

    assert(((DatagramMsgHeader*) extraptr)->cookie == REQ_HEADER_COOKIE);
    bufs[bufindex] = extraptr;
    ++bufindex; assert(bufindex < MAXREQUESTSPERRECEIVE);
    extrasize -= sizenewreq;
    extraptr += sizenewreq;
  }

  // now extraptr and extrasize still refers to an incomplete chunk at the end
  tmb = new TaskMultiBuffer(s.Buf, bufindex); // TaskMultiBuffer is used to later free s.Buf. It will expect bufindex requests before freeing s.Buf
  if (extrasize <= TCP_RECLEN_DEFAULT){
    newlen = TCP_RECLEN_DEFAULT;
    newbuf = (char*) malloc(newlen); assert(newbuf);
  }
  else { // we have at least the header, so we know the size of the next request
    newlen = sizeof(DatagramMsgHeader) + ((DatagramMsgHeader*)extraptr)->size;
    newbuf = (char*) malloc(newlen); assert(newbuf);
  }
  memcpy(newbuf, extraptr, extrasize);

  // state, for next iteration of the loop
  s.Buf = newbuf;
  s.Ptr = newbuf + extrasize;
  s.Buflen = newlen;
  s.Filled = (int)(s.Ptr-s.Buf);

  // call application handler for all complete requests
  for (int i=0; i < bufindex; ++i){
    bufnewreq = bufs[i];
    header = (DatagramMsgHeader*) bufnewreq;
    assert(header->cookie == REQ_HEADER_COOKIE);
    sizenewreq = sizeof(DatagramMsgHeader) + header->size;

    // enqueue item, which will cause application handler to be called
    // send message to Handler dispatcher on thread header->xid % NWorkerThreads
    header = (DatagramMsgHeader*) bufnewreq;
    sendHandleMsg(gContext.hashThread(TCLASS_WORKER, FLAG_GET_HID(header->flags)), tmb, bufnewreq, header->size, src);

    if ((int)header->size < minsize) minsize = header->size;
    if ((int)header->size > maxsize) maxsize = header->size;
  }
}

// returns 0 if ok, non-zero if there is a fatal problem
int TCPDatagramCommunication::handleCompletion(OVERLAPPED *ol, int okstatus, DWORD bytesxfer){
  OVERLAPPEDPLUS *olp = (OVERLAPPEDPLUS*) ol;
  TCPDatagramCommunication *tdc = olp->tdc;
  OVERLAPPEDPLUSReceive *olr;
  OVERLAPPEDPLUSSend *ols;
  OldOVERLAPPEDPLUSSend *ools;
  int res;

  switch (olp->OpCode)
  {
  case 0: // completion of receive
    olr = (OVERLAPPEDPLUSReceive*) olp;

    if (okstatus){
      // process request here...
      tdc->updateState(olr->state, olr->dest, bytesxfer);
      olr->wbuf.buf = olr->state.Ptr;
      olr->wbuf.len = olr->state.Buflen - olr->state.Filled;
    }

    // Repost the read
    ZeroMemory(&olr->ol, sizeof(OVERLAPPED));
#ifdef TCPDATAGRAM_IOCP
    res = WSARecv(olr->socketfd, &olr->wbuf, 1, &olr->dwBytes, &olr->dwFlags, (OVERLAPPED*) olr, 0);
#else
    res = WSARecv(olr->socketfd, &olr->wbuf, 1, &olr->dwBytes, &olr->dwFlags, (OVERLAPPED*) olr, completionFunction);
#endif

    if (res == SOCKET_ERROR){
      res = WSAGetLastError();
      if (res == WSANOTINITIALISED){ return -1; } // main thread must have finished
      if (res == WSAECONNRESET || res == WSAECONNABORTED){
        printf("%I64x Connection reset res %d\n", Time::now(), res);
        closesocket(olr->socketfd); 
        // *!* TODO: here, we can probably delete olr
        break; 
      }
      if (res != WSA_IO_PENDING){
        printf("%I64x WSAGetLastError: %d\n", Time::now(), res); fflush(stdout);
        assert(0);
      }
    }
    break;

  case 1: // completion of send.
    ols = (OVERLAPPEDPLUSSend*) olp;
    // if using completion port, this will cause a lot of messages to be sent to sender thread
    // best to send a single message asking sender thread to delete ols and ols->ipportinfoptr
#ifdef WSAFLATTEN
    if (ols->flattenbuf)
      free(ols->flattenbuf);
#endif
    if (ols->ipportinfoptr)
      delete ols->ipportinfoptr;
    delete ols;
    break;

  case 2: // new connection
    // set up parameters for calling WSARecv
    olr = (OVERLAPPEDPLUSReceive*) olp;
    olr->OpCode = 0;
    olr->ol.hEvent = 0;
    olr->wbuf.buf = (char*) malloc(TCP_RECLEN_DEFAULT); assert(olr->wbuf.buf);
    olr->wbuf.len = TCP_RECLEN_DEFAULT;
    olr->state.Buf = olr->wbuf.buf;
    olr->state.Buflen = olr->wbuf.len;
    olr->state.Ptr = olr->wbuf.buf;
    olr->state.Filled = 0;

#ifdef TCPDATAGRAM_IOCP
    CreateIoCompletionPort((HANDLE) olr->socketfd, tdc->HIocp, 0, 0);
    // post first receive buffer
    res = WSARecv(olr->socketfd, &olr->wbuf, 1, &olr->dwBytes, &olr->dwFlags, (OVERLAPPED*) olr, 0);
#else
    res = WSARecv(olr->socketfd, &olr->wbuf, 1, &olr->dwBytes, &olr->dwFlags, (OVERLAPPED*) olr, completionFunction);
#endif

    if (res == SOCKET_ERROR){
      res = WSAGetLastError();
      if (res != WSA_IO_PENDING){ printf("%I64x handleCompletion, new connection, error %d\n", res); }
    }
    break;

  case 3: // send with OldOVERLAPPEDPLUSSend
    ools = (OldOVERLAPPEDPLUSSend*) olp;
    if (ools->freedata) delete ools->data;
    delete ools;
    break;

#ifdef DIRECT_SEND
  case 4: // send with OVERLAPPEDPLUSSendDirect
    OVERLAPPEDPLUSSendDirect *olsd = (OVERLAPPEDPLUSSendDirect*) olp;
    if (olsd->rse.data.freedata) 
      delete olsd->rse.data.data;
    delete olsd;
    break;
#endif
  } // switch

  return 0;
}

void CALLBACK TCPDatagramCommunication::completionFunction(DWORD dwError, DWORD bytesxfer, OVERLAPPED *ol, DWORD dwFlags){
  int res;
  res = handleCompletion(ol, dwError==0, bytesxfer);
}

DWORD WINAPI TCPDatagramCommunication::receiveThread(void *parm){
  TCPDatagramCommunication *tdc = (TCPDatagramCommunication*) parm;
  TaskScheduler *ts;
#ifdef TCPDATAGRAM_IOCP
  ULONG_PTR *dummyperHandleKey;
  OVERLAPPED *ol;
  DWORD bytesxfer;
  int res;
  int okgetqueue;
#endif

  ts = tgetTaskScheduler();
  ts->assignImmediateFunc(IMMEDIATEFUNC_POSTFIRSTRECEIVEBUF, immediateFuncPostFirstReceiveBuf);
  tsetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM, tdc);

	while (!tdc->ForceEndThreads){
    ts->runOnce(); // run event schedule loop once
    if (ts->checkSendQueuesAlmostFull()){
      continue; // if internal send queues are almost full, do not receive TCP packets
    }
#ifdef TCPDATAGRAM_IOCP
    okgetqueue = GetQueuedCompletionStatus(tdc->HIocp, &bytesxfer, (PULONG_PTR)&dummyperHandleKey, &ol, INFINITE);
    if (!okgetqueue){ // failed
      res = GetLastError();
      if (res == ERROR_MORE_DATA){ 
        printf("Warning: UDP rec buffer has overflown\n");
      }
      //if (res){ printf("Error %d\n", res); }
      if (ol==0) continue; // nothing dequeued
      // otherwise, process the thing that was dequeued
    }

    res = handleCompletion(ol, okgetqueue, bytesxfer);
    if (res) return res;
#else
    SleepEx(0, true); // sleep to give opportunity for completion function to be called
#endif
  }  // while
  return 0;
}

struct TaskMsgDataUpdateIPPortInfo {
  TaskMsgDataUpdateIPPortInfo(IPPort i, i64 f){ ipport = i; fd = f; }
  IPPort ipport;
  i64 fd;
};

// associate completion port, post initial receive buffer, and start thread
// to handle receipt of data on a given connection. To be called after
// a client connect()s or a server accept()s.
void TCPDatagramCommunication::startReceiving(IPPort ipport, SOCKET fd){
  // ask sender thread to update IPPortInfo with new ipport+fd pair
  TaskMsgDataUpdateIPPortInfo updatemsg(ipport, fd);
  sendIFMsg(gContext.hashThread(TCLASS_SEND, ipport.ip), IMMEDIATEFUNC_UPDATEIPPORTINFO, (void*) &updatemsg, sizeof(TaskMsgDataUpdateIPPortInfo));

  // ask receive thread to post first receive buffer
  sendIFMsg(gContext.hashThread(TCLASS_RECEIVE, ipport.ip), IMMEDIATEFUNC_POSTFIRSTRECEIVEBUF, (void*) &updatemsg, sizeof(TaskMsgDataUpdateIPPortInfo));
}

void TCPDatagramCommunication::immediateFuncHandleDispatcher(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread){
  assert(sizeof(TaskMsgDataTCP) <= sizeof(TaskMsgData));
  TCPDatagramCommunication *tdc = (TCPDatagramCommunication*) tgetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM);
  TaskMsgDataTCP *tmdt = (TaskMsgDataTCP*) &msgdata;
  DatagramMsgHeader *header = (DatagramMsgHeader*) tmdt->buf;
  tdc->handleMsg(&tmdt->ipport, header->req, header->xid, header->flags, tmdt->bufbase, tmdt->buf+sizeof(DatagramMsgHeader), tmdt->len);
}

void TCPDatagramCommunication::immediateFuncPostFirstReceiveBuf(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread){
  TaskMsgDataUpdateIPPortInfo *updatemsg = (TaskMsgDataUpdateIPPortInfo*) &msgdata;
  TCPDatagramCommunication *tdc = (TCPDatagramCommunication*) tgetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM);
  OVERLAPPEDPLUSReceive  *olr;
  int res;

  // set up parameters for calling PostQueuedCompletionStatus
  olr = new OVERLAPPEDPLUSReceive; assert(olr);
  olr->OpCode = 2;
  olr->tdc = tdc;
  olr->socketfd = (SOCKET) updatemsg->fd;
  olr->ol.hEvent = 0;
  olr->wbuf.buf = 0;
  olr->wbuf.len = 0;
  olr->dest = updatemsg->ipport;
  olr->senderAddrLen = sizeof(sockaddr_in);
  UDPDest udpdest(updatemsg->ipport);
  memcpy(&olr->senderAddr, &udpdest.destaddr, olr->senderAddrLen);

#ifndef TCPDATAGRAM_IOCP
  res = tdc->handleCompletion((OVERLAPPED*) olr, 1, 0);
#else
  // post a completion to indicate that a new connection now exists
  res = PostQueuedCompletionStatus(HIocp, 0, 0, (OVERLAPPED*)olr); assert(res != 0);
#endif
}


void TCPDatagramCommunication::startupWorkerThread(){
}
void TCPDatagramCommunication::finishWorkerThread(){
}

//---------------------------------------------- WORKER ------------------------------------------------


DWORD WINAPI TCPDatagramCommunication::workerThread(void *parm){
  TCPDatagramCommunication *tdc = (TCPDatagramCommunication *) parm;
  TaskScheduler *ts = tgetTaskScheduler();
  ts->assignImmediateFunc(IMMEDIATEFUNC_HANDLEDISPATCHER, immediateFuncHandleDispatcher);
  tsetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM, tdc);

  tdc->startupWorkerThread(); // invokes startup code
  ts->run();
  tdc->finishWorkerThread(); // invokes cleaning up code
  return -1;
}

//---------------------------------------------- SENDING ------------------------------------------------

void TCPDatagramCommunication::startupSendThread(){
}
void TCPDatagramCommunication::finishSendThread(){
}


DWORD WINAPI TCPDatagramCommunication::sendThread(void *parm){
  TCPDatagramCommunication *tdc = (TCPDatagramCommunication *) parm;
  TaskScheduler *ts = tgetTaskScheduler();
  TaskInfo *ti;
  // assign immediate functions and tasks
  ts->assignImmediateFunc(IMMEDIATEFUNC_SEND, immediateFuncSend);
  ts->assignImmediateFunc(IMMEDIATEFUNC_UPDATEIPPORTINFO, immediateFuncUpdateIPPortInfo);
  tsetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM, tdc);
  ti = ts->createTask(PROGSend, (void*) tdc);
  ts->assignFixedTask(FIXEDTASK_SEND, ti);

  // create thread context space for the send task
  ThreadSharedSend *tss = new ThreadSharedSend(ti);
  tsetSharedSpace(THREADCONTEXT_SPACE_SENDTASK, tss);

  tdc->startupSendThread(); // invokes startup code
  ts->run();
  //while (!ts->getForceEnd()){
  //  ts->runOnce();
  //}
  tdc->finishSendThread();
  return -1;
}

// aux class whose sole purpose is to free a buffer upon deletion
class RawAuxFree : public Marshallable {
private:
  int marshall(LPWSABUF bufs, int maxbufs){ return 0; }
  void demarshall(char *buf){}
  char *buf;
public:
  RawAuxFree(char *b){ buf = b; }
  ~RawAuxFree(){ free(buf); }
};

void TCPDatagramCommunication::sendRaw(IPPort dest, u32 flags, char *buf, int len, bool freebuf){
  int nbufs, res;
  OldOVERLAPPEDPLUSSend *ols;
  DWORD sendBytes;
  IPPortInfo *ipinfo, **retipinfo;

  res = IPPortMap.lookup(dest, retipinfo);
  assert(res==0);
  ipinfo = *retipinfo;

  ols = new OldOVERLAPPEDPLUSSend(); assert(ols);
  ZeroMemory(ols, sizeof(OldOVERLAPPEDPLUSSend));
  ols->ol.hEvent = 0;
  ols->OpCode = 3;
  ols->tdc = this;
  ols->freedata = freebuf;
  if (freebuf) ols->data = new RawAuxFree(buf);

  // prepare msg
  ols->header.cookie = REQ_HEADER_COOKIE;
  ols->header.req = 0;
  ols->header.xid = 0;
  ols->header.flags = flags;

  ols->bufs[0].buf = buf;
  ols->bufs[0].len = len;
  nbufs = 1;

  UDPDest udpdest(dest);
  memcpy(&ols->destAddr, (void*) &udpdest.destaddr, sizeof(sockaddr_in));

#ifdef TCPDATAGRAM_IOCP
  res = WSASend(ipinfo->fd, ols->bufs, nbufs, &sendBytes, 0, (OVERLAPPED*) ols, 0);
#else
  res = WSASend(ipinfo->fd, ols->bufs, nbufs, &sendBytes, 0, (OVERLAPPED*) ols, completionFunction);
#endif
  if (res == 0) ;
  else {
    assert(res == SOCKET_ERROR);
    res = WSAGetLastError();
    if (res != WSA_IO_PENDING && res != WSAENOTSOCK) printf("res=%d\n", res);
    assert(res == WSA_IO_PENDING || res == WSAENOTSOCK || res == WSAECONNRESET || res == WSAECONNABORTED);
  }
}

#ifdef DIRECT_SEND
void TCPDatagramCommunication::sendMsg(DatagramMsg *dmsg){

  OVERLAPPEDPLUSSendDirect *olsd;
  WSABUF wsabufs[MAXWSABUFSERIALIZE];
  int nbufs;
  DWORD sendBytes;
  int res;
  IPPortInfo *ipinfo, **retipinfo;


  olsd = new OVERLAPPEDPLUSSendDirect; assert(olsd);
  memset(olsd, 0, sizeof(OVERLAPPEDPLUSSendDirect));
  olsd->ol.hEvent = 0; 
  olsd->OpCode = 4;
  olsd->tdc = 0;
  nbufs = marshallRPC(wsabufs, MAXWSABUFSERIALIZE, dmsg);

  res = IPPortMap.lookup(dmsg->ipport, retipinfo);
  assert(res==0);
  ipinfo = *retipinfo;

#ifdef TCPDATAGRAM_IOCP
  res = WSASend(ipinfo->fd, wsabufs, nbufs, &sendBytes, 0, (OVERLAPPED*) olsd, 0);
#else
  res = WSASend(ipinfo->fd, wsabufs, nbufs, &sendBytes, 0, (OVERLAPPED*) olsd, completionFunction);
#endif
  if (res == 0) ; // immediate send
  else {
    assert(res == SOCKET_ERROR);
    res = WSAGetLastError();
    if (res != WSA_IO_PENDING && res != WSAENOTSOCK) printf("res=%d\n", res);
    assert(res == WSA_IO_PENDING || res == WSAENOTSOCK || res == WSAECONNRESET || res == WSAECONNABORTED);
  }
}
#else
void TCPDatagramCommunication::sendMsg(DatagramMsg *dmsg){
  assert(sizeof(DatagramMsg) <= TASKSCHEDULER_TASKMSGDATA_SIZE);
  TaskMsg msg;
  int sendthread; // which of the sender threads will send

  *(DatagramMsg*)&msg.data = *dmsg; // copy dmsg

  sendthread = gContext.hashThread(TCLASS_SEND, dmsg->ipport.ip);
  msg.dest = TASKID_CREATE(sendthread, IMMEDIATEFUNC_SEND);
  //msg.dest = TASKID_CREATE(2, IMMEDIATEFUNC_NOP);
  msg.flags = TMFLAG_FIXDEST | TMFLAG_IMMEDIATEFUNC;
  tgetTaskScheduler()->sendMessage(msg);
}
#endif

int TCPDatagramCommunication::marshallRPC(WSABUF *wsabuf,  int bufsleft, RPCSendEntry *rse){
  DatagramMsgHeader *header;
  DatagramMsg *dmsg = &rse->dmsg;
  int nbufs;
  int size;
  
  // fill RPC header
  header = &rse->header;
  header->cookie = REQ_HEADER_COOKIE;
  // header->size is set below
  header->req = dmsg->req;
  header->xid = dmsg->xid;
  header->flags = dmsg->flags;
  // include it in buffer to send
  wsabuf[0].buf = (char*) &rse->header;
  wsabuf[0].len = sizeof(DatagramMsgHeader);
  nbufs = 1;

  // marshall RPC body
  nbufs += dmsg->data->marshall(wsabuf+1, bufsleft-1);

  // calculate length of marshalled data and store it in header
  size=0;
  for (int i=1; i < nbufs; ++i) size += wsabuf[i].len;
  header->size = size;

  return nbufs;
}

char *flattenwsabuf(WSABUF *buf, int nbufs, int &retlen){
  int len, i;
  char *retval, *ptr;
  len = 0;
  for (i=0; i < nbufs; ++i){
    len += buf[i].len;
  }
  retval = (char*) malloc(len);
  ptr = retval;
  for (i=0; i < nbufs; ++i){
    memcpy(ptr, buf[i].buf, buf[i].len);
    ptr += buf[i].len;
  }
  retlen = len;
  return retval;
}

void TCPDatagramCommunication::immediateFuncSend(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread){
  DatagramMsg *dmsg = (DatagramMsg*) &msgdata;
  // extract shared space entry
  ThreadSharedSend *tss = (ThreadSharedSend*) tgetSharedSpace(THREADCONTEXT_SPACE_SENDTASK);
  assert(tss);
  tss->SendList.pushTail(*dmsg);
  ts->wakeUpTask(tss->sendTaskInfo); 
}

void TCPDatagramCommunication::immediateFuncUpdateIPPortInfo(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread){
  TaskMsgDataUpdateIPPortInfo *updatemsg = (TaskMsgDataUpdateIPPortInfo*) &msgdata;
  TCPDatagramCommunication *tdc = (TCPDatagramCommunication*) tgetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM);

  IPPortInfo *info = new IPPortInfo;
  info->fd = (SOCKET) updatemsg->fd;
  tdc->IPPortMap.insert(updatemsg->ipport, info);
}

void TCPDatagramCommunication::sendIPPortInfoTCP(IPPort &dest, IPPortInfoTCP *ipinfotcp){
  int res;
  IPPortInfo *ipinfo, **retipinfo;
  OVERLAPPEDPLUSSend *ols;
  DWORD sendBytes;
  static int maxsendlog = 0;

  ols = new OVERLAPPEDPLUSSend; assert(ols);
  memset(ols, 0, sizeof(OVERLAPPEDPLUSSend));
  ols->ol.hEvent = (void*) 55; 
  ols->OpCode = 1;
  ols->tdc = this;
  ols->ipportinfoptr = ipinfotcp;

  res = IPPortMap.lookup(dest, retipinfo);
  assert(res==0);
  ipinfo = *retipinfo;

#ifdef WSAFLATTEN
  int newlen;
  WSABUF newbuf;
  newbuf.buf = flattenwsabuf(ipinfotcp->wsabufs, ipinfotcp->nextwsa, newlen);
  newbuf.len = newlen;
  ols->flattenbuf = newbuf.buf;

  if (newlen >= maxsendlog+1024){
    printf("%I64x Sending %d\n", Time::now(), newlen);
    maxsendlog = newlen;
  }

#ifdef TCPDATAGRAM_IOCP
  res = WSASend(ipinfo->fd, &newbuf, 1, &sendBytes, 0, (OVERLAPPED*) ols, 0);   
#else
  res = WSASend(ipinfo->fd, &newbuf, 1, &sendBytes, 0, (OVERLAPPED*) ols, completionFunction);   
#endif // TCPDATAGRAM_IOCP

#else // WSAFLATTEN
#ifdef TCPDATAGRAM_IOCP
  res = WSASend(ipinfo->fd, ipinfotcp->wsabufs, ipinfotcp->nextwsa, &sendBytes, 0, (OVERLAPPED*) ols, 0);
  //PostQueuedCompletionStatus(HIocp, 0, 0, (OVERLAPPED*) ols); // fake send (produce completion to free data)
#else
  res = WSASend(ipinfo->fd, ipinfotcp->wsabufs, ipinfotcp->nextwsa, &sendBytes, 0, (OVERLAPPED*) ols, completionFunction);
  //completionFunction(0, 0, (OVERLAPPED*)ols, 0);
#endif // TCPDATAGRAM_IOCP
#endif // WSAFLATTEN
  if (res == 0) ; // immediate send
  else {
    assert(res == SOCKET_ERROR);
    res = WSAGetLastError();
    if (res != WSA_IO_PENDING && res != WSAENOTSOCK) printf("res=%d\n", res);
    assert(res == WSA_IO_PENDING || res == WSAENOTSOCK || res == WSAECONNRESET || res == WSAECONNABORTED);
  }
}

// called during execution of PROGSend
void TCPDatagramCommunication::duringPROGSend(TaskInfo *ti){
}

//void TCPDatagramCommunication::auxdelIPPortInfoTCP(IPPortInfoTCP *ipinfo){
//  delete ipinfo;
//}

static int WSATotalLen(WSABUF *bufs, int nbufs){
  int len=0;
  for (int i=0; i < nbufs; ++i) len += bufs[i].len;
  return len;
} 

// send msg program
int TCPDatagramCommunication::PROGSend(TaskInfo *ti){
  int res;
  IPPort lastipport;
  IPPortInfoTCP *ipinfotcp, **ipinfotcpptr;
  DatagramMsg dmsg;
  RPCSendEntry *rse;
  TCPDatagramCommunication *tdc = (TCPDatagramCommunication *) ti->getTaskData();

  // extract shared space entry
  ThreadSharedSend *tss = (ThreadSharedSend*) tgetSharedSpace(THREADCONTEXT_SPACE_SENDTASK);
  assert(tss);

  //if (tss->SendList.nitems < 16) return SchedulerTaskStateWaiting;

  tdc->duringPROGSend(ti);

  ipinfotcp = 0;
  lastipport.invalidate();
  while (!tss->SendList.empty()){
    dmsg = tss->SendList.popHead();
    if (IPPort::cmp(lastipport,dmsg.ipport) != 0){ // check if it matches the last we used
      res = tss->ipportinfomap.lookupInsert(dmsg.ipport, ipinfotcpptr); // no, look it up in skiplist
      if (res) *ipinfotcpptr = new IPPortInfoTCP;
      ipinfotcp = *ipinfotcpptr;
      lastipport = dmsg.ipport;
    }

    rse = new RPCSendEntry;
    rse->dmsg = dmsg;
    // rse->header will be filled by marshallRPC below


    //if (ipinfotcp->nextwsa + MAXWSABUFSERIALIZE >= SEND_WSABUF_QUEUESIZE ||
    //    ipinfotcp->nbytes >= TCPDATAGRAM_MAX_SEND_SIZE){ // no more space to serialize
    if (ipinfotcp->nextwsa + MAXWSABUFSERIALIZE >= SEND_WSABUF_QUEUESIZE){ 
      tdc->sendIPPortInfoTCP(dmsg.ipport, ipinfotcp); // transmit WSABUFs
      ipinfotcp = new IPPortInfoTCP; // allocate a new one
      *ipinfotcpptr = ipinfotcp; // change entry in skiplist
    }
    ipinfotcp->gcQueue.pushTail(rse); // put entry in queue to garbage collect

    int nbufs = marshallRPC(ipinfotcp->wsabufs + ipinfotcp->nextwsa, SEND_WSABUF_QUEUESIZE - ipinfotcp->nextwsa, rse);
    ipinfotcp->nbytes += WSATotalLen(ipinfotcp->wsabufs + ipinfotcp->nextwsa, nbufs);
    ipinfotcp->nextwsa += nbufs;

    //static int c=0;
    //if (++c % 1003==0){ printf("wsa %d\n", ipinfotcp->nextwsa); fflush(stdout); }
    assert(ipinfotcp->nextwsa <= SEND_WSABUF_QUEUESIZE);
  }

  // loop over destinations
  SkipListNode<IPPort, IPPortInfoTCP*> *ptr, *ptrnext;
  //if (tss->ipportinfomap.getFirst() == tss->ipportinfomap.getLast()){ putchar('?'); fflush(stdout); }
  for (ptr = tss->ipportinfomap.getFirst(); ptr != tss->ipportinfomap.getLast(); ptr = ptrnext){
    ptrnext = tss->ipportinfomap.getNext(ptr);
    //if (ipinfotcp->nextwsa >= 500){ // no more space to serialize
    //  tdc->sendIPPortInfoTCP(ptr->key, ptr->value); // transmit WSABUFs
    //  ptr->value = new IPPortInfoTCP; // allocate a new one
    //}
    tdc->sendIPPortInfoTCP(ptr->key, ptr->value);
  }

  // delete entries in skiplist
  tss->ipportinfomap.clear(0, 0);

  return SchedulerTaskStateWaiting;
}

//---------------------------------------------- INIT + LISTENING ------------------------------------------------

TCPDatagramCommunication::TCPDatagramCommunication(int port){
  ServerPort = port;
  ServerThr = 0;
  ForceEndThreads = false; 

#ifdef TCPDATAGRAM_IOCP
  // create completion port
  HIocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, (ULONG_PTR)0, 0);
  assert(HIocp);
#endif
}

TCPDatagramCommunication::~TCPDatagramCommunication(){ 
  if (!ForceEndThreads) // exitThreads sets ForceEndThreads, so this tests ensures exitThreads is not called twice
    exitThreads(); 
}

DWORD WINAPI TCPDatagramCommunication::serverThread(void *parm){
  TCPDatagramCommunication *tdc = (TCPDatagramCommunication*) parm;
  SOCKET fdlisten, fdaccept;
  sockaddr_in sin_server;
  int res=0;
  TaskScheduler *ts;

  SLauncher->initThreadContext("SERVER", 0); // allows this thread to send messages to others
  ts = tgetTaskScheduler();
  tsetSharedSpace(THREADCONTEXT_SPACE_TCPDATAGRAM, tdc);

  res = SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_ABOVE_NORMAL);
  if (!res){ printf("Warning: serverThread cannot set thread priority: %d\n", GetLastError()); fflush(stdout); }


  //WARNING_INIT(); // initializes warning task

	fdlisten = (int) WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED); 
	if (fdlisten == INVALID_SOCKET) {
		printf("WSASocket(g_sdListen) failed: %d\n", WSAGetLastError());
		return -1;
  }

  memset(&sin_server, 0, sizeof(sockaddr_in));
  sin_server.sin_family = AF_INET;
  sin_server.sin_addr.s_addr = htonl(INADDR_ANY);
  sin_server.sin_port = htons(tdc->ServerPort);
	res = ::bind(fdlisten, (sockaddr*) &sin_server, sizeof(sin_server));
	if (res == SOCKET_ERROR) {
		printf("bind() failed: %d\n", WSAGetLastError());
		return -1;
	}

	res = listen(fdlisten, SOMAXCONN);
	if (res == SOCKET_ERROR) {
		printf("listen() failed: %d\n", WSAGetLastError());
		return -1;
	}

	// Disable send buffering on the socket. 
	//res = 0;
	//res = setsockopt(fdlisten, SOL_SOCKET, SO_SNDBUF, (char *)&res, sizeof(int));
	//if (res == SOCKET_ERROR) {
	//	printf("setsockopt(SNDBUF) failed: %d\n", WSAGetLastError());
	//	return -1;
	//}
  int value, len;
    
  value = 1;
  res = setsockopt(fdlisten, IPPROTO_TCP, TCP_NODELAY, (char*) &value, sizeof(int));
  if (res) printf("%016I64x setsockopt on TCP_NODELAY of listen socket: error %d\n", Time::now(), WSAGetLastError());

  //value=64*1024;
  value=0;
  res = setsockopt(fdlisten,  SOL_SOCKET, SO_SNDBUF, (char*) &value, sizeof(int));
  if (res) printf("%016I64xsetsockopt on SO_SNDBUF of listen socket: error %d\n", Time::now(), WSAGetLastError());
  len=sizeof(int);
  //res = getsockopt(fdlisten,  SOL_SOCKET, SO_SNDBUF, (char*) &value, &len);
  //if (res) printf("%016I64xgetsockopt on SO_SNDBUF of listen socket: error %d\n", Time::now(), WSAGetLastError());
  //else printf("%016I64xgetsockopt SO_SNDBUF of listen socket %d\n", Time::now(), value);

  //value=1*1024*1024;
  //res = setsockopt(fdlisten,  SOL_SOCKET, SO_RCVBUF, (char*) &value, sizeof(int));
  //if (res) printf("%016I64xsetsockopt on SO_RCVBUF of listen socket: error %d\n", Time::now(), WSAGetLastError());
  //len=sizeof(int);
  //res = getsockopt(fdlisten,  SOL_SOCKET, SO_RCVBUF, (char*) &value, &len);
  //if (res) printf("%016I64xgetsockopt on SO_RCVBUF of listen socket: error %d\n", Time::now(), WSAGetLastError());
  //else printf("%016I64xgetsockopt SO_RCVBUF of listen socket %d\n", Time::now(), value);

  UDPDest ud;
  while (!tdc->ForceEndThreads){
    ts->runOnce();
    ud.sockaddr_len = sizeof(sockaddr_in);
		fdaccept = WSAAccept(fdlisten, (sockaddr*) &ud.destaddr, &ud.sockaddr_len, NULL, 0);
    if (fdaccept == SOCKET_ERROR){
      int err = WSAGetLastError();
      if (err != WSAEINTR){  // WSAEINTR is a normal error we get when we end the program
        printf("%016I64x WSAAccept:socket_error %d from %08x\n", Time::now(), WSAGetLastError(), *(int*)&ud.destaddr.sin_addr);
      }
      continue;
    }
    //printf("WSAAccept:connection from %08x\n", *(int*)&ud.destaddr.sin_addr);

    value = 1;
    res = setsockopt(fdaccept, IPPROTO_TCP, TCP_NODELAY, (char*) &value, sizeof(int));
    if (res) printf("%016I64x setsockopt on TCP_NODELAY of accept socket: error %d\n", Time::now(), WSAGetLastError());

    value=0;
    //value=64*1024;
    res = setsockopt(fdaccept,  SOL_SOCKET, SO_SNDBUF, (char*) &value, sizeof(int));
    if (res) printf("%016I64x setsockopt on SO_SNDBUF accept socket: error %d\n", Time::now(), WSAGetLastError());
    //len=sizeof(int);
    //res = getsockopt(fdaccept,  SOL_SOCKET, SO_SNDBUF, (char*) &value, &len);
    //if (res) printf("%016I64x getsockopt on SO_SNDBUF of accept socket: error %d\n", Time::now(), WSAGetLastError());
    //else printf("%016I64x getsockopt SO_SNDBUF of accept socket %d\n", Time::now(), value);

    //value=1*1024*1024;
    //res = setsockopt(fdaccept,  SOL_SOCKET, SO_RCVBUF, (char*) &value, sizeof(int));
    //if (res) printf("%016I64x setsockopt on SO_RCVBUF of accept socket: error %d\n", Time::now(), WSAGetLastError());
    //len=sizeof(int);
    //res = getsockopt(fdaccept,  SOL_SOCKET, SO_RCVBUF, (char*) &value, &len);
    //if (res) printf("%016I64x getsockopt on SO_RCVBUF of accept socket: error %d\n", Time::now(), WSAGetLastError());
    //else printf("%016I64x getsockopt SO_RCVBUF of accept socket %d\n", Time::now(), value);

    tdc->startReceiving(ud.getIPPort(), fdaccept);
    SleepEx(0, true); // sleep to give opportunity for completion function to be called
  }
  return 0;
}

// should be called at the beginning by a single thread.
// This is because there are no locks protecting IPPortMap
int TCPDatagramCommunication::clientconnect(IPPort dest) {
  SOCKET fd;
  int res;
  int value;
  UDPDest udpdest(dest);

	fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if( fd == INVALID_SOCKET ) {
		printf("socket() failed: %d\n", WSAGetLastError());
		return -1;
	}
  do {
    res = connect(fd, (sockaddr*) &udpdest.destaddr, (int) udpdest.sockaddr_len);
	  if (res == SOCKET_ERROR) {
		  printf("connect failed: %d\n", WSAGetLastError());
      Sleep(1000);
	  }
  } while (res == SOCKET_ERROR);
  printf("connected\n");

  value = 1;
  res = setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*) &value, sizeof(int));
  if (res) printf("setsockopt on connect socket: error %d\n", WSAGetLastError());

  startReceiving(dest, fd);
	return 0;
}

TaskMultiBuffer::TaskMultiBuffer(char *b, int count){
  refcount = count;
  base = b;
  threadno = tgetThreadNo(); // get this from thread context
}
TaskMultiBuffer::~TaskMultiBuffer(){
  free(base);
}
void TaskMultiBuffer::decRef(){
  if (AtomicDec32(&refcount) <= 0) delete this;
}
void TaskMultiBuffer::incRef(){ AtomicInc32(&refcount); }
// sends message to free a TaskMultiBuffer

void TCPDatagramCommunication::freeMB(TaskMultiBuffer *bufbase){
  bufbase->decRef();
}

int TCPDatagramCommunication::launch(int wait){
  int i;
  int threadno;
  assert(TCP_MAX_RECEIVE_THREADS+WORKERTHREADS >= 2);
  assert(!ForceEndThreads);
  int nWorkerThreads, nReceiveThreads, nSendThreads, nClientThreads;

  if (!SLauncher){
    printf("Scheduler not initialized. Must call tinitScheduler()\n");
    exit(1);
  }

  if (ServerPort){ // we are a server
    // server has thread that listens for connections
    ServerThr = CreateThread(0, 0, serverThread, (void*) this, 0, 0); assert(ServerThr);
    nWorkerThreads = WORKERTHREADS;
    nReceiveThreads = TCP_MAX_RECEIVE_THREADS;
    nSendThreads = SENDTHREADS;
    nClientThreads = 0;
  } else { // we are client
    ServerThr = 0;
    nWorkerThreads = 1; // only 1 worker and receive threads for client
    nReceiveThreads = 1;
    nSendThreads = 1;
    nClientThreads = TCP_MAX_CLIENT_THREADS;
  }

  gContext.setNThreads(TCLASS_RECEIVE, nReceiveThreads);
  gContext.setNThreads(TCLASS_WORKER, nWorkerThreads);
  gContext.setNThreads(TCLASS_SEND, nSendThreads);
  gContext.setNThreads(TCLASS_CLIENT, nClientThreads);

  // launch scheduler threads
  assert(nReceiveThreads + nWorkerThreads + nSendThreads + nClientThreads <= TASKSCHEDULER_MAX_THREADS);
  for (i=0; i < nReceiveThreads; ++i){
    threadno = SLauncher->createThread("RECEIVE", receiveThread, (void*) this, true);
    gContext.setThread(TCLASS_RECEIVE, i, threadno);
  }

  for (i = 0; i < nWorkerThreads; ++i){
    threadno = SLauncher->createThread("WORKER", workerThread, (void*) this, true);
    gContext.setThread(TCLASS_WORKER, i, threadno);
  }

  for (i=0; i < nSendThreads; ++i){
    threadno = SLauncher->createThread("SEND", sendThread, (void*) this, true);
    gContext.setThread(TCLASS_SEND, i, threadno);
  }

  if (!wait) return 0;
  if (ServerPort){ // if server, wait for serverthread
    WaitForSingleObject(ServerThr, INFINITE);
    CloseHandle(ServerThr);
  }
  SLauncher->wait();

  return -1;
}

void TCPDatagramCommunication::exitThreads(){
  int i;
  int nthreads;
  int threadno;
  TaskMsg msg;
  // send message asking scheduler of receive threads to exit
  nthreads = gContext.getNThreads(TCLASS_RECEIVE);
  for (i=0; i < nthreads; ++i){
    threadno = gContext.getThread(TCLASS_RECEIVE, i);
    msg.dest = TASKID_CREATE(threadno, IMMEDIATEFUNC_EXIT);
    msg.flags = TMFLAG_FIXDEST | TMFLAG_IMMEDIATEFUNC;
    tgetTaskScheduler()->sendMessage(msg);
  }
  // send message asking scheduler of worker threads to exit
  nthreads = gContext.getNThreads(TCLASS_WORKER);
  for (i=0; i < nthreads; ++i){
    threadno = gContext.getThread(TCLASS_WORKER, i);
    msg.dest = TASKID_CREATE(threadno, IMMEDIATEFUNC_EXIT);
    msg.flags = TMFLAG_FIXDEST | TMFLAG_IMMEDIATEFUNC;
    tgetTaskScheduler()->sendMessage(msg);
  }
  // send message asking scheduler of worker threads to exit
  nthreads = gContext.getNThreads(TCLASS_SEND);
  for (i=0; i < nthreads; ++i){
    threadno = gContext.getThread(TCLASS_SEND, i);
    msg.dest = TASKID_CREATE(threadno, IMMEDIATEFUNC_EXIT);
    msg.flags = TMFLAG_FIXDEST | TMFLAG_IMMEDIATEFUNC;
    tgetTaskScheduler()->sendMessage(msg);
  }
  // send message asking scheduler of client threads to exit
  nthreads = gContext.getNThreads(TCLASS_CLIENT);
  for (i=0; i < nthreads; ++i){
    threadno = gContext.getThread(TCLASS_CLIENT, i);
    msg.dest = TASKID_CREATE(threadno, IMMEDIATEFUNC_EXIT);
    msg.flags = TMFLAG_FIXDEST | TMFLAG_IMMEDIATEFUNC;
    tgetTaskScheduler()->sendMessage(msg);
  }

  ForceEndThreads = true; // causes receive thread and thread that accepts connections to exit
}

// return IP address of a given name
u32 TCPDatagramCommunication::resolveName(char *name, unsigned preferredprefix16){
  char portstr[10];
	struct addrinfo ai, *result, *ptr;
  //struct hostent *he;
	int res;
  u32 thisip, ip192, ipprefer, ipother;

	ZeroMemory(&ai, sizeof(ai));
	ai.ai_family = AF_INET;
	ai.ai_socktype = SOCK_STREAM;
	ai.ai_protocol = IPPROTO_TCP;

	sprintf(portstr, "%d", 1);
  //other implementation
  res = getaddrinfo(name, portstr, &ai, &result);
  if (res) return 0;
  ptr = result;
  ip192 = ipother = ipprefer = 0;
  //printf("Resolve %s:\n", name);
  while (ptr){
    thisip = *(u32*) &ptr->ai_addr->sa_data[2];
    if ((thisip & 0xffff) == preferredprefix16) ipprefer = thisip;
    if ((thisip & 0xff) == 0xc0) ip192 = thisip;
    else 
      //if (!ipother)  // uncomment this line to choose first IP in list
      ipother = thisip; // pick last IP in list
    //printf("  %08x\n", thisip);
    ptr = ptr->ai_next;
  }

  //printf("resolve %s = %08x %08x\n", name, ipother, ip192);
  if (ipprefer){
    //printf("Using preferred IP %08x\n", ipprefer);
    return ipprefer;
  }
  else {
    if (ip192){
      //printf("Unsing non-preferred 192 IP %08x\n", ip192);
      return ip192;
    }
    else {
      //printf("Unsing non-preferred IP %08x\n", ipother);
      return ipother;
    }
  }

  // old implementation (doesn't work, returns IP address 0x00000002)
  //he = gethostbyname(name); assert(he);
  //assert(he->h_addrtype == AF_INET);
  //while (he->h_addr_list[i] != 0 && !ipother){
  //  thisip = * (u32*) he->h_addr_list[i];
  //  if ((thisip & 0xff) == 0xc0) ip192 = thisip;
  //  else ipother = thisip;
  //}
  //if (ip192) return ip192;
  //else return ipother;
}

// return my own ip address as a u32
u32 TCPDatagramCommunication::getMyIP(unsigned preferredprefix16){
  char localhostname[256];
  int res;
  //struct hostent *he;
  // int i;
  //u32 thisip, ip192, ipother;

  res = gethostname(localhostname, sizeof(localhostname)); assert(!res);
  return resolveName(localhostname, preferredprefix16);

  // old code
  //he = gethostbyname(localhostname); assert(he);
  //assert(he->h_addrtype == AF_INET);
  //i=0;
  //ip192 = ipother = 0;
  //printf("My IPs:\n");
  //while (he->h_addr_list[i] != 0 && !ipother){
  //  thisip = * (u32*) he->h_addr_list[i];
  //  printf("  %08x\n", thisip);
  //  if ((thisip & 0xff) == 0xc0) ip192 = thisip;
  //  else ipother = thisip;
  //}
  //printf("My ips %08x %08x\n", ipother, ip192);
  //if (ip192) return ip192;
  //else return ipother;
}
