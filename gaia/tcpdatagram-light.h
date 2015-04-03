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

#ifndef _TCPDATAGRAM_LIGHT_H
#define _TCPDATAGRAM_LIGHT_H

#include "tmalloc.h"
#include "gaiaoptions.h"
#include "gaiatypes.h"
#include "util.h"
#include "datastruct.h"
#include "tcpdatagram-common.h"
#include "ipmisc.h"

#define REQ_HEADER_COOKIE 0xbebe // cookie added to beginning of datagram

#ifdef CONCURMARKER
#include <cvmarkersobj.h>
using namespace Concurrency::diagnostic;
#define CMARKER(x) TcpMarkerSeries.write_flag(x)
#else
#define CMARKER(x)
#endif

class MsgBuffer;

struct TCPWorkerItem {
  TCPWorkerItem(){ buf = 0; }
  TCPWorkerItem(IPPort d, char *b, int l) : dest(d), buf(b), len(l) { }
  IPPort dest;
  char *buf;
  int len;
  TCPWorkerItem *next;
};


#define MAXWSABUFSERIALIZE 32 // maximum number of buffers in WSABUF struct
#define TCPDATAGRAM_MAXCONNECTIONS   8192   // max number of connections a client can initiate
class TCPDatagramCommunicationLight {
private:
  HANDLE HIocp;    // handle for IO completion port
  SkipList<IPPort,IPPortInfo*> IPPortMap; // maps ip-port pairs to a file description + lock to send data
  EventSync WorkerEvent;
  RWLock WorkerQueue_l;
  TCPWorkerItem *WorkerQueueHead;
  TCPWorkerItem *WorkerQueueTail;
  bool ForceEndThreads; // when set to true, threads will exit as soon as possible

  Align4 int NReceiveThreads; // number of threads created so far for receiving TCP data

  struct OVERLAPPEDPLUS {
    OVERLAPPED ol;
    int OpCode;
  };

  struct ReceiveState {
    char *Buf;  // beginning of buffer being filled
    int Buflen; // total allocated size
    char *Ptr;  // current position being filled
    int Filled; // offset of current position being filled (==Ptr-Buf)
  };

  // Each OVERLAPPEDPLUSReceive is specialized for a given connection (fd),
  // and so it is uniquely associated with a destination (dest)
  struct OVERLAPPEDPLUSReceive {
    OVERLAPPED ol;
    int OpCode;
    SOCKET socketfd;
    WSABUF wbuf;
    DWORD dwBytes, dwFlags;
    IPPort dest;   
    int senderAddrLen;
    sockaddr_in senderAddr;
    ReceiveState state;
    OVERLAPPEDPLUSReceive(){ memset(this, 0, sizeof(OVERLAPPEDPLUSReceive)); }
  };

  class OVERLAPPEDPLUSSend {
  public:
    OVERLAPPED ol;
    int OpCode;
    WSABUF bufs[MAXWSABUFSERIALIZE];
    DatagramMsgHeader header;
    Marshallable *data;
    bool freedata;
    sockaddr_in destAddr;
    //void *operator new(size_t size);
    //void operator delete(void *p);
  };

  // stuff for receiving
  void updateState(ReceiveState &s, IPPort src, int len);
  static DWORD WINAPI receiveThread(void *parm);

  // worker thread that dequeues TCP datagrams and calls the user-supplied handler
  static DWORD WINAPI TCPWorkerThread(void *parm);
  int NWorkerThr; // number of worker threads
  void **WorkerThr; // array of worker thread handles

  // called whenever a client connect()s or a server accept()s
  void startReceiving(IPPort ipport, SOCKET fd);

  // Server-specific stuff
  void *ServerThr; // thread for listening for new connections
  int ServerPort;
  static DWORD WINAPI serverThread(void *parm);

  // client-specific stuff
  SOCKET connectsockets[TCPDATAGRAM_MAXCONNECTIONS];
  int nconnectsockets;

  friend class MsgBuffer;

protected:
  // Specialize this function to provide handler of incoming UDP messages.
  // handleMsg is given the address of the sender (src), req and xid of the RPC,
  // and a pointer to a buffer (data) containing the rest of the UDP message.
  // handleMsg should free data by calling TCPDatagramCommunication::freeIncomingBuf(data).
  virtual void handleMsg(IPPort *src, u32 req, u32 xid, u32 flags, char *data, int len)=0;

public:
  // set port to non-zero for server, zero for server
  TCPDatagramCommunicationLight(int port);
  ~TCPDatagramCommunicationLight();

  // Client-specific stuff: connects to a server (must be called before the
  // client can send to it)
  int clientconnect(IPPort dest);

  // closes all sockets previously connected by client
  void clientdisconnectall(void);

  // Server-specific: launches the process to listen and accept connections
  // wait=true means to never return
  int launch(int wait=1);

  void exitThreads(){ ForceEndThreads = true; WorkerEvent.set(); } // causes scheduler of threads to exit

  void sendRaw(IPPort dest, u32 flags, char *buf, int len, bool freebuf);
  void sendMsg(DatagramMsg *dmsg);

  // wait for server to end
  void waitServerEnd(void){ WaitForSingleObject(ServerThr, INFINITE); }

  // Free a buffer passed on to handleMsg. To be
  // called if handleMsg returns true (it wants
  // to keep data and free it later)
  static void freeIncomingBuf(char *buf);
  static u32 resolveName(char *name, unsigned preferredprefix16=0); // return IP address of a given name
  static u32 getMyIP(unsigned preferredprefix16=0); // return my own ip address
};

//void tinitScheduler(int initthread); // dummy function

#endif
