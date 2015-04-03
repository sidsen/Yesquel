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

// rpc.h
//
// Classes for remote procedure calls

#ifndef _GRPCTCP_H
#define _GRPCTCP_H

#include <hash_map>
#include <map>
using namespace stdext;
using namespace std;

#include "tmalloc.h"
#include "gaiaoptions.h"
#include "util.h"
#include "scheduler.h"
#include "datastruct.h"
#include "datastructmt.h"
#include "task.h"
#include "tcpdatagram.h"
#include "grpctcp-common.h"

class RPCClient;
struct OutstandingRPC;

// This is the callback function passed on to an asynchronous RPC call.
// The callback is invoked when the RPC response arrives. Data has
// the unmarshalled response with length len, and callbackdata is any data chosen
// by the entity which set up the callback function.
// The callback function should not free data, as it will be freed by the
// RPC library.
typedef void (*RPCCallbackFunc)(char *data, int len, void *callbackdata);

// outstanding RPCs of a client
struct OutstandingRPC
{
  DatagramMsg dmsg;          // message headers and data
  RPCCallbackFunc callback;  // callback for reply
  void *callbackdata;        // data to be passed to callback
  u64 timestamp;             // when RPC call was made (used for retrying)
  RPCClient *rpcc;
  //int currtimeout;           // current timeout value
  //int nretransmit;           // number of retransmits
  int done;                  // whether reply has arrived already or not
                             // Invariant: done=true iff xid is not in OutstandingRequests
  // HashTable stuff
  OutstandingRPC *next, *prev, *snext, *sprev;
  int GetKey(){ return dmsg.xid; }
  static unsigned HashKey(int i){ return (unsigned)i; }
  static int CompareKey(int i1, int i2){ if (i1<i2) return -1; else if (i1==i2) return 0; else return 1; }
};

//#define EFFICIENT_RPC_REXMIT_PERIOD 1000  // Period in ms to execute efficient retransmit handler.
//                                          // Retransmissions will occur between this number and
//                                          // twice it.
//#define FIRST_RPC_REXMIT 2000   // retransmit request after FIRST_RPC_REXMIT milliseconds
//#define MULT_RPC_REXMIT 1.3     // exponential backoff multiplier for retransmissions

//   unsigned hash()
// as well as comparison operators <=, <, and ==

class RPCClient : public TCPDatagramCommunication
{
private:
  HashTableMT<U32,OutstandingRPC*> OutstandingRequests; // outstanding RPC's. A map from xid to OutstandingRPC*
  Align4 u32 CurrXid;
  static void waitCallBack(char *data, int len, void *callbackdata); // internal callback for synchronous RPCs
  //SimpleLinkList<OutstandingRPC*> EfficientRetransmit; // list for first retransmission (efficient)
  //RWLock EfficientRetransmit_lock;
  //EventScheduler SchedRetransmit;

  // handles a message from the UDP layer and invokes any registered callbacks. For synchronous
  // RPCs, the callback is provided by the library and unblocks the RPC. For asynchronous
  // RPCs, the callback is provided by the user. See RPCCallbackFunc above for more
  // information on the callback.
  void handleMsg(IPPort *dest, u32 req, u32 xid, u32 flags, TaskMultiBuffer *tmb, char *data, int len);

  //// callback for retransmitting RPC's
  //static int EfficientRetransmitRPC(void*parm); // this handler dispatches elements queue in EfficientRetransmit
  //static int RetransmitRPC(void*parm);          // this handler is scheduled directly into the EventScheduler

  // stuff related to startup of client
  virtual void startupSendThread();

protected:
  OutstandingRPC *RequestLookupAndDelete(u32 xid);

public:
  RPCClient();
  ~RPCClient(){
    exitThreads();
    Sleep(1000);
  }

  // creates a thread that can make RPCs. Returns a local thread id (not a windows handle)
  int createThread(char *threadname, LPTHREAD_START_ROUTINE startroutine, void *threaddata, bool pinthread){
    return SLauncher->createThread(threadname, startroutine, threaddata, pinthread);
  }

  // wait for a thread to finish
  unsigned long waitThread(int threadno, unsigned long duration){ return SLauncher->waitThread(threadno, duration); }

  // callbackdata is data to be passed to the callback function
  // When RPC gets response, argument "data" will be deleted automatically, so caller should not delete it again
  void AsyncRPC(IPPort dest, int rpcno, u32 flags, Marshallable *data, RPCCallbackFunc callback, void *callbackdata);

  // Returns a buffer that caller must later free using free().
  // Comment about parameter "data" for AsyncRPC applies here too
  char *SyncRPC(IPPort dest, int rpcno, u32 flags, Marshallable *data);
};

// *********************************** SERVER STUFF ************************************

class RPCTaskInfo : public TaskInfo {
public:
  RPCTaskInfo(ProgFunc pf, void *taskdata, IPPort *s, u32 r, u32 x, u32 f, TaskMultiBuffer *t, char *d, int l) : TaskInfo(pf, taskdata) {
    src = *s;
    req = r;
    xid = x;
    flags = f;
    tmb = t;
    data = d;
    len = l;
    resp = 0;
    //seen = 0;
  }
  void setResp(Marshallable *r){ resp = r; }
  Marshallable *getResp(){ return resp; }

  // information coming from TCPDatagramCommunication
  IPPort src; 
  u32 req; 
  u32 xid; 
  u32 flags; 
  TaskMultiBuffer *tmb; 
  char *data;
  int len;

  // information used during the RPC processing
  MsgIdentifier msgid;
  bool seen; // whether the RPC was seen before or not

  // information to be returned
  Marshallable *resp;
};

//// stores the result of an RPC for the server cache of results
//struct RPCCachedResult {
//public:
//  MsgIdentifier id;
//  Marshallable *result;
//  u64           timeRequest;
//
//  // HashTable stuff
//  RPCCachedResult *prev, *next, *sprev, *snext;
//  MsgIdentifier *GetKeyPtr(){ return &id; }
//  static unsigned HashKey(MsgIdentifier *rpci){ return *((u32*)rpci) ^ *((u32*)rpci+1) ^ *((u32*)rpci+2); }
//  static int CompareKey(MsgIdentifier *i1, MsgIdentifier *i2){ return memcmp((void*)i1, (void*)i2, sizeof(MsgIdentifier)); }
//};

class RPCServer;

// item to be used in a linked list of DatagramMsg
struct DatagramMsgI : public DatagramMsg { // extended fields
  DatagramMsgI *next;
};

// thread-specific information for each worker thread at server
class RPCServerThreadInfo {
public:
  //HashTableBK<MsgIdentifier,RPCCachedResult> ResultCache; // cache for results, to handle retransmissions
  SLinkList<DatagramMsgI> **ToSend;  // Array of RPCs to send, with one entry for each sender thread

  RPCServerThreadInfo() 
  //  : ResultCache(SERVER_RESULTCACHE_HASHTABLE_SIZE)
  {
    ToSend = 0;
  }
};

typedef int (*RPCProc)(RPCTaskInfo *); // Parameter RPCTaskInfo includes all information about the RPC

class RPCServer : public TCPDatagramCommunication {
protected:
  RPCProc *Procs;
  unsigned NProcs; // number of registered procedures, which will range from 0 to NFuncs-1

  //// callback for garbage collecting result cache
  //static int GCResultCacheHandler(void *parm);

  void startupWorkerThread(); // called by TCPDatagramCommunication before entering event loop of worker thread
  void finishWorkerThread();  // called by TCPDatagramCommunication after leaving event loop of worker thread
  void startupSendThread();   // called by TCPDatagramCommunication before entering event loop of sender thread
  void finishSendThread();    // called by TCPDatagramCommunication after leaving event loop of sender thread

  void duringPROGSend(TaskInfo *ti); // called during execution of PROGSend

  static void SendToSend(SLinkList<DatagramMsgI> *ToSend, int destthreadno);
  static void ImmediateFuncSendToSend(TaskMsgData &msgdata, TaskScheduler *ts, int srcthread);

  static int RPCStart(RPCTaskInfo *rti);
  static int RPCEnd(RPCTaskInfo *rti);
  static int PROGFlushToSend(TaskInfo *ti);
  void handleMsg(IPPort *dest, u32 req, u32 xid, u32 flags, TaskMultiBuffer *tmb, char *data, int len);

public:
  RPCServer(RPCProc *procs, int nprocs, int portno);
};
#endif
