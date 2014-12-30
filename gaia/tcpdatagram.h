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

#ifndef _TCPDATAGRAM_H
#define _TCPDATAGRAM_H

#include "tmalloc.h"
#include "gaiaoptions.h"
#include "gaiatypes.h"
#include "util.h"
#include "datastruct.h"
#include "ipmisc.h"
#include "tcpdatagram-common.h"
#include "task.h"



#define REQ_HEADER_COOKIE 0xbebe     // cookie added to beginning of datagram
#define MAXWSABUFSERIALIZE 32 // maximum number of WSABUFs that an RPC may produce
#define SEND_WSABUF_QUEUESIZE 16384 // length of wsabuf send queue
//#define TCPDATAGRAM_IOCP  // if defined, use I/O completion ports, otherwise use completion functions
#define WSAFLATTEN      // if defined, flatten out WSABUFs before sending
//#define DIRECTSEND      // if defined, send directly instead of sending via send thread

#ifdef CONCURMARKER
#include <cvmarkersobj.h>
using namespace Concurrency::diagnostic;
#define CMARKER(x) TcpMarkerSeries.write_flag(x)
#else
#define CMARKER(x)
#endif
class MsgBuffer;

// This is a buffer that tracks a buffer and a refcount for it.
// When the refcount reaches zero, the buffer is freed with free().
// The buffer being tracked should be allocated with malloc()
class TaskMultiBuffer {
private:
  Align4 int refcount;
public:
  u8 threadno;
  char *base;

  TaskMultiBuffer(char *b, int count=0); // allocate b with malloc()
  ~TaskMultiBuffer();
  void decRef();
  void incRef();
};

// Message data for RPCTask
struct TaskMsgDataTCP {
  TaskMultiBuffer *bufbase;
  char *buf;
  int len;
  IPPort ipport;
};

// TaskInfo for RPCTask
class TaskInfoTCP : public TaskInfo {
public:
  // input to the RPC task
  TaskMultiBuffer *bufbase;
  char *buf;
  IPPort ipport;

  // output set by the RPC Task. The EndFunc will be responsible
  // for sending out the result
  Marshallable *result;

  // construct an RPCTaskInfo object from an RPCTaskMsgData
  TaskInfoTCP(ProgFunc f) : TaskInfo(f,0) { bufbase = 0; buf = 0; }
  TaskInfoTCP(ProgFunc f, TaskMsgDataTCP *rtmd) :
    TaskInfo(f,0), bufbase(rtmd->bufbase), buf(rtmd->buf), ipport(rtmd->ipport) { }
};


class TCPDatagramCommunication {
  friend class MsgBuffer;
private:
#ifdef TCPDATAGRAM_IOCP
  HANDLE HIocp;    // handle for IO completion port
#endif
  SkipList<IPPort,IPPortInfo*> IPPortMap; // maps ip-port pairs to a file description to send data; should be accessed only by sender thread
  bool ForceEndThreads; // when set to true, threads will exit as soon as possible

#ifdef CONCURMARKER
  marker_series TcpMarkerSeries;
#endif

  struct ReceiveState {
    char *Buf;  // beginning of buffer being filled
    int Buflen; // total allocated size
    char *Ptr;  // current position being filled
    int Filled; // offset of current position being filled (==Ptr-Buf)
  };
  struct OVERLAPPEDPLUS {
    OVERLAPPED ol;
    int OpCode;
    TCPDatagramCommunication *tdc;
  };
  // Each OVERLAPPEDPLUSReceive is specialized for a given connection (fd),
  // and so it is uniquely associated with a destination (dest)
  struct OVERLAPPEDPLUSReceive : public OVERLAPPEDPLUS {
    SOCKET socketfd;
    WSABUF wbuf;
    DWORD dwBytes, dwFlags;
    IPPort dest;   
    int senderAddrLen;
    sockaddr_in senderAddr;
    ReceiveState state;
    OVERLAPPEDPLUSReceive(){ memset(this, 0, sizeof(OVERLAPPEDPLUSReceive)); }
  };
  // entry in linked list
  struct RPCSendEntry {
    DatagramMsg dmsg;
    DatagramMsgHeader header; // space for RPC header, which will be included in actual WSABUF when sending
    RPCSendEntry *next;
  };
  class IPPortInfoTCP {
  public:
    WSABUF wsabufs[SEND_WSABUF_QUEUESIZE];
    int nextwsa; // next available wsa buf so far
    u32 nbytes; // number of bytes so far
    SLinkList<RPCSendEntry> gcQueue;
    IPPortInfoTCP(){ nextwsa = 0;  nbytes = 0;}
    ~IPPortInfoTCP(){
      RPCSendEntry *rse;
      while (!gcQueue.empty()){
        rse = gcQueue.popHead();
        if (rse->dmsg.freedata) 
          delete rse->dmsg.data;
        delete rse;
      }
    }
  };
  class OldOVERLAPPEDPLUSSend : public OVERLAPPEDPLUS {
  public:
    WSABUF bufs[MAXWSABUFSERIALIZE];
    DatagramMsgHeader header;
    Marshallable *data;
    bool freedata;
    sockaddr_in destAddr;
  };
  class OVERLAPPEDPLUSSend : public OVERLAPPEDPLUS {
  public:
    IPPortInfoTCP *ipportinfoptr;
#ifdef WSAFLATTEN
    char *flattenbuf;
#endif
  };
  class OVERLAPPEDPLUSSendDirect : public OVERLAPPEDPLUS {
  public:
    RPCSendEntry rse;
  };

  // stuff for receiving
  void updateState(ReceiveState &s, IPPort src, int len);
  static DWORD WINAPI receiveThread(void *parm);
  static int handleCompletion(OVERLAPPED *olbase, int okstatus, DWORD bytesxfer);
  static void CALLBACK completionFunction(DWORD dwError, DWORD bytesxfer, OVERLAPPED *olbase, DWORD dwFlags);
  static void immediateFuncHandleDispatcher(TaskMsgData &data, TaskScheduler *ts, int srcthread);
  static void immediateFuncFreeTaskMultiBuffer(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread);
  static void immediateFuncFreeMBBatch(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread);

  static void immediateFuncPostFirstReceiveBuf(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread);
  void sendHandleMsg(int threadno, TaskMultiBuffer *bufbase, char *buf, int len, IPPort ipport);

  // worker thread
  static DWORD WINAPI workerThread(void *parm);
  virtual void startupWorkerThread();  // workerThread calls this method upon startup
  virtual void finishWorkerThread();   // workerThread calls this method when ending

  // stuff for sending
  static DWORD WINAPI sendThread(void *parm);
  static int marshallRPC(WSABUF *wsabuf, int bufsleft, RPCSendEntry *rse);
  static void immediateFuncSend(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread);
  static void immediateFuncUpdateIPPortInfo(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread);
  void sendIPPortInfoTCP(IPPort &dest, IPPortInfoTCP *ipinfotcp); // aux function used by PROGSend to send data
  virtual void duringPROGSend(TaskInfo *ti); // called during execution of PROGSend
  //static void auxdelIPPortInfoTCP(IPPortInfoTCP *ipinfo);
  static int PROGSend(TaskInfo *ti);
  virtual void startupSendThread(); // sendThread calls this method upon startup
  virtual void finishSendThread();  // sendThread calls this method when ending

  // called whenever a client connect()s or a server accept()s
  void startReceiving(IPPort ipport, SOCKET fd);

  // Server-specific stuff
  void *ServerThr; // thread for listening for new connections
  int ServerPort;
  static DWORD WINAPI serverThread(void *parm);


protected:
  // thread context for send threads
  class ThreadSharedSend {
  public:
    ThreadSharedSend(TaskInfo *sti){ sendTaskInfo = sti; }
    TaskInfo *sendTaskInfo;
    TCPDatagramCommunication *tdc;
    SimpleLinkList<DatagramMsg> SendList; // dequeue messages to be processed by PROGsend
    SkipList<IPPort, IPPortInfoTCP*> ipportinfomap; // maps destination to WSABUFs to send
  };

  // Specialize this function to provide handler of incoming UDP messages.
  // handleMsg is given the address of the sender (src), req and xid of the RPC,
  // a TaskMultiBuffer (used to free the message buffer),
  // and a pointer to a buffer containing the rest of the UDP message.
  // handleMsg should free the buffer by calling freeMB with the TaskMultiBuffer parameter.
  // It is not recommended the handleMsg holds on to the buffer for a long time, since the
  // buffer is shared with other requests received together, so holding on to the buffer will
  // keep more memory allocated than needed. Thus, if handleMsg needs to keep the data, it
  // should make a private copy and then free the buffer.
  virtual void handleMsg(IPPort *src, u32 req, u32 xid, u32 flags, TaskMultiBuffer *tmb, char *data, int len)=0;

  static int PROGBatchFreeMBBufs(TaskInfo *ti);
  static void freeMB(TaskMultiBuffer *bufbase); // add an entry to the batch of multibufs to free


public:
  // set port to non-zero for server, zero for client
  TCPDatagramCommunication(int port);
  ~TCPDatagramCommunication();

  // Client-specific stuff: connects to a server (must be called before the
  // client can send to it)
  int clientconnect(IPPort dest);

  // launches the receiver, sender, and worker threads.
  // If server (port is non-zero on constructor), also creates thread to listen and accept connections
  // wait=true means to never return
  int launch(int wait=1);
  void exitThreads(); // causes scheduler of threads to exit

  void sendMsg(DatagramMsg *dmsg); // Sends an RPC message. Copies the content of rpcmsg (but not rpcmsg->data).
  void sendRaw(IPPort dest, u32 flags, char *buf, int len, bool freebuf);

  // wait for server to end
  void waitServerEnd(void){ WaitForSingleObject(ServerThr, INFINITE); }

  static u32 resolveName(char *name, unsigned preferredprefix16=0); // return IP address of a given name
  static u32 getMyIP(unsigned preferredprefix16=0); // return my own ip address
};

#endif
