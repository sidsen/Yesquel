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
#include "tcpdatagram-light.h"
#include "ipmisc.h"
#include "util.h"

using namespace std;

#define SKIPWORKERTHREAD     // receive thread directly invokes callback instead of going through worker thread
//#define MULTIPLECOMPLETIONS  // handles multiple completions at once using GetQueuedCompletionStatusEx (requires Windows Server 2008)

//FixedAllocator TCPDatagramCommunicationLight::OverlappedPlusSendAllocator(sizeof(OVERLAPPEDPLUSSend), 64, 64);
//
//void *TCPDatagramCommunicationLight::OVERLAPPEDPLUSSend::operator new(size_t size){
//  assert(size==sizeof(OVERLAPPEDPLUSSend));
//  return OverlappedPlusSendAllocator.myalloc();
//}
//
//void TCPDatagramCommunicationLight::OVERLAPPEDPLUSSend::operator delete(void *p){
//  OverlappedPlusSendAllocator.myfree(p);
//}

DWORD WINAPI TCPDatagramCommunicationLight::TCPWorkerThread(void *parm){
  TCPDatagramCommunicationLight *tdc = (TCPDatagramCommunicationLight *) parm;
  TCPWorkerItem *twi, *next;
  DatagramMsgHeader *header;

  while (!tdc->ForceEndThreads){
    tdc->WorkerEvent.wait();  // wait for event

    tdc->WorkerQueue_l.lock();
    tdc->WorkerEvent.reset(); 
    // detach the items from the linklist
    twi = tdc->WorkerQueueHead->next;
    tdc->WorkerQueueHead->next = 0;
    tdc->WorkerQueueTail = tdc->WorkerQueueHead;
    tdc->WorkerQueue_l.unlock();

    // process the detached items
    while (twi){
      header = (DatagramMsgHeader*) twi->buf;
      tdc->handleMsg(&twi->dest, header->req, header->xid, header->flags, twi->buf + sizeof(DatagramMsgHeader), twi->len); // call application handler
      
      next = twi->next;
      delete twi;
      twi = next;
    }
  }
  return 0;
}

void TCPDatagramCommunicationLight::updateState(ReceiveState &s, IPPort src, int len){
  DatagramMsgHeader *header;
  int totalsize;
  char *newbuf;
  TCPWorkerItem *tcpwi;
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

#ifdef SKIPWORKERTHREAD
    header = (DatagramMsgHeader*) s.Buf;
    handleMsg(&src, header->req, header->xid, header->flags, s.Buf + sizeof(DatagramMsgHeader), header->size); // call application handler
#else
    tcpwi = new TCPWorkerItem(src, s.Buf, header->size);
    tcpwi->next = 0;
    // add new item to linklist
    WorkerQueue_l.lock();
    assert(WorkerQueueTail->next == 0);
    WorkerQueueTail->next = tcpwi;
    WorkerQueueTail = tcpwi;
    WorkerQueue_l.unlock();
    WorkerEvent.set();
#endif

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
    bufnewreq = (char*) malloc(sizenewreq);
    memcpy(bufnewreq, extraptr, sizenewreq);

    assert(((DatagramMsgHeader*) bufnewreq)->cookie == REQ_HEADER_COOKIE);
    bufs[bufindex] = bufnewreq;
    ++bufindex; assert(bufindex < MAXREQUESTSPERRECEIVE);
    extrasize -= sizenewreq;
    extraptr += sizenewreq;
  }

  // now extraptr and extrasize still refers to an incomplete chunk at the end
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

  // build items to be added to WorkerQueue linklist
  if (bufindex >= 1){
    TCPWorkerItem head;
    head.next = 0;
    tcpwi = &head;
    for (int i=0; i < bufindex; ++i){
      bufnewreq = bufs[i];
      header = (DatagramMsgHeader*) bufnewreq;
      assert(header->cookie == REQ_HEADER_COOKIE);

#ifdef SKIPWORKERTHREAD
      handleMsg(&src, header->req, header->xid, header->flags, bufnewreq + sizeof(DatagramMsgHeader), header->size); // call application handler
#else
      //sizenewreq = sizeof(DatagramMsgHeader) + header->size;

      // enqueue item, which will cause application handler to be called
      tcpwi->next = new TCPWorkerItem(src, bufnewreq, header->size);
      tcpwi = tcpwi->next;
#endif
      if ((int)header->size < minsize) minsize = header->size;
      if ((int)header->size > maxsize) maxsize = header->size;
    }
    tcpwi->next = 0; // put null pointer at the end

#ifdef SKIPWORKERTHREAD
#else
    // attach new items to WorkerQueue linklist
    WorkerQueue_l.lock();
    assert(WorkerQueueTail->next == 0);
    WorkerQueueTail->next = head.next;
    WorkerQueueTail = tcpwi;
    WorkerQueue_l.unlock();
    WorkerEvent.set();
#endif
  }
}

#ifdef MULTIPLECOMPLETIONS
#define MAXCOMPLETIONS 64
DWORD WINAPI TCPDatagramCommunicationLight::receiveThread(void *parm){
  TCPDatagramCommunicationLight *tdc = (TCPDatagramCommunicationLight *) parm;
  ULONG_PTR perHandleKey;
  OVERLAPPED_ENTRY olvec[MAXCOMPLETIONS];
  unsigned long size, i;
  OVERLAPPED *olbase;
  OVERLAPPEDPLUS *olp;
  OVERLAPPEDPLUSReceive  *olr;
  OVERLAPPEDPLUSSend *ols;
  DWORD dwBytesXfered;
  int res;
  int okgetqueue;

  //pinThread(3);

	while (!tdc->ForceEndThreads){
    okgetqueue = GetQueuedCompletionStatusEx(tdc->HIocp, olvec, MAXCOMPLETIONS, &size, INFINITE, false);
    static int c=0;
    if (++c % 10000 == 0){ printf("size %d\n", size); fflush(stdout); }
    for (i=0; i < size; ++i){
      //dwBytesXfered, perHandleKey, olbase
      olbase = olvec[i].lpOverlapped;
      dwBytesXfered = olvec[i].dwNumberOfBytesTransferred;
      perHandleKey = olvec[i].lpCompletionKey;
      if (!okgetqueue){ // failed
        res = GetLastError();
        if (res == ERROR_MORE_DATA){ 
          printf("Warning: UDP rec buffer has overflown\n");
        }
        //if (res){ printf("Error %d\n", res); }
        if (olbase==0) continue; // nothing dequeued
        // otherwise, process the thing that was dequeued
      }

      olp = CONTAINING_RECORD(olbase, OVERLAPPEDPLUS, ol);
      switch (olp->OpCode)
      {
      case 0: // completion of receive
        olr = (OVERLAPPEDPLUSReceive*) olp;
        if (okgetqueue){

          // process request here...
          tdc->updateState(olr->state, olr->dest, dwBytesXfered);
          olr->wbuf.buf = olr->state.Ptr;
          olr->wbuf.len = olr->state.Buflen - olr->state.Filled;
        }

        // Repost the read
        ZeroMemory(&olr->ol, sizeof(OVERLAPPED));
        res = WSARecv(olr->socketfd, &olr->wbuf, 1, &olr->dwBytes, &olr->dwFlags,
             (OVERLAPPED*) olr, 0);

        if (res == SOCKET_ERROR){
          res = WSAGetLastError();
          if (res == WSANOTINITIALISED){ return 0; } // main thread must have finished
          if (res == WSAECONNRESET){
            printf("Connection reset\n");
            closesocket(olr->socketfd); 
            // *!* TODO: here, we can probably delete olr
            break; 
          }
          if (res != WSA_IO_PENDING){
            printf("WSAGetLastError: %d\n", res); fflush(stdout);
            assert(0);
          }
        }
        break;

      case 1: // completion of send
        //printf("WRITE\n"); fflush(stdout);
        ols = (OVERLAPPEDPLUSSend*) olp;
        if (ols->freedata) delete ols->data;
        delete ols;
        break;
      } // switch
    }
  }  // while
  return 0;
}
#else
DWORD WINAPI TCPDatagramCommunicationLight::receiveThread(void *parm){
  TCPDatagramCommunicationLight *tdc = (TCPDatagramCommunicationLight *) parm;
  ULONG_PTR perHandleKey;
  OVERLAPPED *olbase;
  OVERLAPPEDPLUS *olp;
  OVERLAPPEDPLUSReceive  *olr;
  OVERLAPPEDPLUSSend *ols;
  DWORD dwBytesXfered;
  int res;
  int okgetqueue;

  //pinThread(1);

	while (!tdc->ForceEndThreads){
    okgetqueue = GetQueuedCompletionStatus(tdc->HIocp, &dwBytesXfered, &perHandleKey, &olbase, INFINITE);
    if (!okgetqueue){ // failed
      res = GetLastError();
      if (res == ERROR_MORE_DATA){ 
        printf("Warning: UDP rec buffer has overflown\n");
      }
      //if (res){ printf("Error %d\n", res); }
      if (olbase==0) continue; // nothing dequeued
      // otherwise, process the thing that was dequeued
    }

    olp = CONTAINING_RECORD(olbase, OVERLAPPEDPLUS, ol);
    switch (olp->OpCode)
    {
    case 0: // completion of receive
      olr = (OVERLAPPEDPLUSReceive*) olp;
      if (okgetqueue){

        // process request here...
        tdc->updateState(olr->state, olr->dest, dwBytesXfered);
        olr->wbuf.buf = olr->state.Ptr;
        olr->wbuf.len = olr->state.Buflen - olr->state.Filled;
      }

      // Repost the read
      ZeroMemory(&olr->ol, sizeof(OVERLAPPED));
      res = WSARecv(olr->socketfd, &olr->wbuf, 1, &olr->dwBytes, &olr->dwFlags,
           (OVERLAPPED*) olr, 0);

      if (res == SOCKET_ERROR){
        res = WSAGetLastError();
        if (res == WSANOTINITIALISED){ return 0; } // main thread must have finished
        if (res == WSAECONNRESET){
          printf("Connection reset\n");
          closesocket(olr->socketfd); 
          // *!* TODO: here, we can probably delete olr
          break; 
        }
        if (res != WSA_IO_PENDING && res != WSAENOTSOCK){
          printf("WSAGetLastError: %d\n", res); fflush(stdout);
          assert(0);
        }
      }
      break;

    case 1: // completion of send
      //printf("WRITE\n"); fflush(stdout);
      ols = (OVERLAPPEDPLUSSend*) olp;
      if (ols->freedata) delete ols->data;
      delete ols;
      break;
    } // switch
  }  // while
  return 0;
}
#endif

// associate completion port, post initial receive buffer, and start thread
// to handle receipt of data on a given connection. To be called after
// a client connect()s or a server accept()s.
void TCPDatagramCommunicationLight::startReceiving(IPPort ipport, SOCKET fd){
  OVERLAPPEDPLUSReceive  *olr;
  int res;
  HANDLE h, dummyhandle;

  IPPortInfo *info = new IPPortInfo();
  info->fd = fd;
  IPPortMap.insert(ipport, info);

  // set up parameters for calling WSARecv
  olr = new OVERLAPPEDPLUSReceive; assert(olr);
  olr->OpCode = 0;
  olr->socketfd = fd;
  olr->ol.hEvent = 0;
  //olr->ol.hEvent = WSACreateEvent();  assert(olr->ol.hEvent);
  olr->wbuf.buf = (char*) malloc(TCP_RECLEN_DEFAULT); assert(olr->wbuf.buf);
  olr->wbuf.len = TCP_RECLEN_DEFAULT;
  olr->dest = ipport;
  olr->senderAddrLen = sizeof(sockaddr_in);
  UDPDest udpdest(ipport);
  memcpy(&olr->senderAddr, &udpdest.destaddr, olr->senderAddrLen);
  olr->state.Buf = olr->wbuf.buf;
  olr->state.Buflen = olr->wbuf.len;
  olr->state.Ptr = olr->wbuf.buf;
  olr->state.Filled = 0;

  dummyhandle = CreateIoCompletionPort((HANDLE) fd, HIocp, (ULONG_PTR)fd, 0);
  assert(dummyhandle);

  // post first receive buffer
  res = WSARecv(olr->socketfd, &olr->wbuf, 1, &olr->dwBytes, &olr->dwFlags, 
    (OVERLAPPED*) olr, 0);

  if (res == SOCKET_ERROR){
    res = WSAGetLastError();
    if (res != WSA_IO_PENDING){ printf("Error %d\n", res); fflush(stdout); assert(0); }
  }

  if (NReceiveThreads <= TCP_LIGHT_MAX_RECEIVE_THREADS){
    // create new thread to handle receipts
    AtomicInc32(&NReceiveThreads);
    h = CreateThread(0, 0, receiveThread, (void*) this, 0, 0); assert(h);
  }
}

DWORD WINAPI TCPDatagramCommunicationLight::serverThread(void *parm){
  TCPDatagramCommunicationLight *tdc = (TCPDatagramCommunicationLight*) parm;
  SOCKET fdlisten, fdaccept;
  sockaddr_in sin_server;
  int res=0;

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

	res = listen(fdlisten, 1024);
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

  int value=1;
  res = setsockopt(fdlisten, IPPROTO_TCP, TCP_NODELAY, (char*) &value, sizeof(int));
  if (res) printf("setsockopt on listen socket: error %d\n", WSAGetLastError());

  UDPDest ud;

  while (!tdc->ForceEndThreads){
    ud.sockaddr_len = sizeof(sockaddr_in);
		fdaccept = WSAAccept(fdlisten, (sockaddr*) &ud.destaddr, &ud.sockaddr_len, NULL, 0);
    if (fdaccept == SOCKET_ERROR) continue;
    printf("Accepted connection from %08x\n", *(int*)&ud.destaddr.sin_addr);

    res = setsockopt(fdaccept, IPPROTO_TCP, TCP_NODELAY, (char*) &value, sizeof(int));
    if (res) printf("setsockopt on accept socket: error %d\n", WSAGetLastError());

    tdc->startReceiving(ud.getIPPort(), fdaccept);
  }
  return 0;
}

TCPDatagramCommunicationLight::TCPDatagramCommunicationLight(int port){
  ServerPort = port;
  NReceiveThreads = 0;
  ServerThr = 0;
  WorkerThr = 0;
  NWorkerThr = 0;
  ForceEndThreads = false; 

  WorkerQueueHead = WorkerQueueTail = new TCPWorkerItem;
  memset(WorkerQueueHead, 0, sizeof(TCPWorkerItem));
  WorkerQueueHead->next = 0;

  nconnectsockets = 0;
  memset(connectsockets, 0, sizeof(connectsockets));

  // create completion port
  HIocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, (ULONG_PTR)0, 0);
  assert(HIocp);
}

TCPDatagramCommunicationLight::~TCPDatagramCommunicationLight(){ 
  if (WorkerThr) delete [] WorkerThr;
  clientdisconnectall();
}

// should be called at the beginning by a single thread.
// This is because there are no locks protecting IPPortMap
int TCPDatagramCommunicationLight::clientconnect(IPPort dest) {
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
    printf("Connecting to IP %08I64x...", dest.ip); fflush(stdout);
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

  value=0;
  res = setsockopt(fd,  SOL_SOCKET, SO_SNDBUF, (char*) &value, sizeof(int));
  if (res) printf("setsockopt on SO_SNDBUF of connect socket: error %d\n", WSAGetLastError());
  //len=sizeof(int);
  //res = getsockopt(fd,  SOL_SOCKET, SO_SNDBUF, (char*) &value, &len);
  //if (res) printf("getsockopt on SO_SNDBUF of connect socket: error %d\n", WSAGetLastError());
  //else printf("getsockopt SO_SNDBUF of connect socket %d\n", value);

  //value=64*1024;
  //res = setsockopt(fd,  SOL_SOCKET, SO_RCVBUF, (char*) &value, sizeof(int));
  //if (res) printf("setsockopt on SO_RCVBUF of connect socket: error %d\n", WSAGetLastError());
  //len=sizeof(int);
  //res = getsockopt(fd,  SOL_SOCKET, SO_RCVBUF, (char*) &value, &len);
  //if (res) printf("getsockopt on SO_RCVBUF of connect socket: error %d\n", WSAGetLastError());
  //else printf("getsockopt SO_RCVBUF of connect socket %d\n", value);

  if (nconnectsockets < TCPDATAGRAM_MAXCONNECTIONS){
    connectsockets[nconnectsockets] = fd;
    ++nconnectsockets;
  } else { printf("Exceeded TCPDATAGRAM_MAXCONNECTIONS=%d connections\n", TCPDATAGRAM_MAXCONNECTIONS); }

  startReceiving(dest, fd);
	return 0;
}

void TCPDatagramCommunicationLight::clientdisconnectall(void) {
  int i;
  for (i=0; i < nconnectsockets; ++i){
    closesocket(connectsockets[i]);
  }
  nconnectsockets=0;
}


int TCPDatagramCommunicationLight::launch(int wait){
  int i;
  if (ServerPort){ // we are a server
    ServerThr = CreateThread(0, 0, serverThread, (void*) this, 0, 0); assert(ServerThr);
    NWorkerThr = SERVERTHREADS;
    WorkerThr = new void*[SERVERTHREADS];
    for (i=0; i < SERVERTHREADS; ++i)
      WorkerThr[i] = CreateThread(0, 0, TCPWorkerThread, (void*) this, 0, 0); assert(WorkerThr[i]);
    if (!wait) return 0;
    WaitForSingleObject(ServerThr, INFINITE);
    CloseHandle(ServerThr);
    for (i=0; i < SERVERTHREADS; ++i){
      WaitForSingleObject(WorkerThr[i], INFINITE);
      CloseHandle(WorkerThr[i]);
    }
    return -1;
  } else { // we are a client
    ServerThr = 0;
    NWorkerThr = 1; // one worker thread is sufficient since clients should have a very simpler handler
    WorkerThr = new void*[1];
    WorkerThr[0] = CreateThread(0, 0, TCPWorkerThread, (void*) this, 0, 0); assert(WorkerThr[0]);
    if (!wait) return 0;
    WaitForSingleObject(WorkerThr[0], INFINITE);
    CloseHandle(WorkerThr[0]);
    return -1;
  }
  return 0;
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

void TCPDatagramCommunicationLight::sendRaw(IPPort dest, u32 flags, char *buf, int len, bool freebuf){
  int nbufs, res;
  OVERLAPPEDPLUSSend *ols;
  DWORD sendBytes;
  IPPortInfo *ipinfo, **retipinfo;

  res = IPPortMap.lookup(dest, retipinfo);
  assert(res==0);
  ipinfo = *retipinfo;

  ols = new OVERLAPPEDPLUSSend(); assert(ols);
  ZeroMemory(ols, sizeof(OVERLAPPEDPLUSSend));
  ols->ol.hEvent = 0;
  ols->OpCode = 1;
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

  res = WSASend(ipinfo->fd, ols->bufs, nbufs, &sendBytes, 0, (OVERLAPPED*) ols, 0);
  if (res == 0) ;
  else {
    assert(res == SOCKET_ERROR);
    res = WSAGetLastError();
    if (res != WSA_IO_PENDING && res != WSAENOTSOCK) printf("res=%d\n", res);
    assert(res == WSA_IO_PENDING || res == WSAENOTSOCK || res == WSAECONNRESET || res == WSAECONNABORTED);
  }
}


void TCPDatagramCommunicationLight::sendMsg(DatagramMsg *dmsg){
  int nbufs, res;
  OVERLAPPEDPLUSSend *ols;
  int i,size;
  DWORD sendBytes;
  IPPortInfo *ipinfo, **retipinfo;

  res = IPPortMap.lookup(dmsg->ipport, retipinfo);
  assert(res==0);
  ipinfo = *retipinfo;

  ols = new OVERLAPPEDPLUSSend(); assert(ols);
  ZeroMemory(ols, sizeof(OVERLAPPEDPLUSSend));
  ols->ol.hEvent = 0; // WSACreateEvent();
  ols->OpCode = 1;

  // prepare msg
  ols->header.cookie = REQ_HEADER_COOKIE;
  // ols->header.size is set below
  ols->header.req = dmsg->req;
  ols->header.xid = dmsg->xid;
  ols->header.flags = dmsg->flags;
  ols->bufs[0].buf = (char*) &ols->header;
  ols->bufs[0].len = sizeof(ols->header);
  nbufs = 1;
  ols->data = dmsg->data;
  nbufs += dmsg->data->marshall(ols->bufs+1, MAXWSABUFSERIALIZE-2);
  // calculate length of marshalled data
  size=0;
  for (i=1; i < nbufs; ++i) size += ols->bufs[i].len;
  ols->header.size = size;
  ols->freedata = dmsg->freedata;
  assert(nbufs < MAXWSABUFSERIALIZE);

  // **!** figure out which socket to send to:
  //  lookup table
  //  if found, done
  //  if address looks like a server
  //     socketfd = create connection
  //     add socketfd to table

  UDPDest udpdest(dmsg->ipport);
  memcpy(&ols->destAddr, (void*) &udpdest.destaddr, sizeof(sockaddr_in));
  //ipinfo->send_lock.lock();
  res = WSASend(ipinfo->fd, ols->bufs, nbufs, &sendBytes, 0, (OVERLAPPED*) ols, 0);
  //ipinfo->send_lock.unlock();
  if (res == 0){ 
    //printf("Immediate send\n"); fflush(stdout);
    //      WSACloseEvent(ols->ol.hEvent);
    //if (ols->freedata) delete ols->data;
    //delete ols;
  }
  else {
    assert(res == SOCKET_ERROR);
    res = WSAGetLastError();
    if (res != WSA_IO_PENDING && res != WSAENOTSOCK) printf("res=%d\n", res);
    assert(res == WSA_IO_PENDING || res == WSAENOTSOCK || res == WSAECONNRESET);
  }
}

void TCPDatagramCommunicationLight::freeIncomingBuf(char *buf){
  free(buf-sizeof(DatagramMsgHeader)); 
}

// return IP address of a given name
u32 TCPDatagramCommunicationLight::resolveName(char *name, unsigned preferredprefix16){
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
u32 TCPDatagramCommunicationLight::getMyIP(unsigned preferredprefix16){
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

// dummy tinitScheduler
//void tinitScheduler(int initthread){}
