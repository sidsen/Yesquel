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

#ifndef _GRPCTCP_LIGHT_H
#define _GRPCTCP_LIGHT_H

#include <hash_map>
#include <map>
using namespace stdext;
using namespace std;

#include "gaiaoptions.h"
#include "util.h"
#include "scheduler.h"
#include "datastruct.h"
#include "datastructmt.h"
#include "ipmisc.h"
#include "tcpdatagram-light.h"
#include "grpctcp-common.h"

class RPCClientLight;

// outstanding RPCs of a client
struct OutstandingRPCLight
{
  DatagramMsg dmsg;          // message headers and data
  RPCCallbackFunc callback;  // callback for reply
  void *callbackdata;        // data to be passed to callback
  u64 timestamp;             // when RPC call was made (used for retrying)
  RPCClientLight *rpcc;
  int done;                  // whether reply has arrived already or not
                             // Invariant: done=true iff xid is not in OutstandingRequests
  // HashTable stuff
  OutstandingRPCLight *next, *prev, *snext, *sprev;
  int GetKey(){ return dmsg.xid; }
  static unsigned HashKey(int i){ return (unsigned)i; }
  static int CompareKey(int i1, int i2){ if (i1<i2) return -1; else if (i1==i2) return 0; else return 1; }
};


class RPCClientLight : public TCPDatagramCommunicationLight
{
private:
  HashTableMT<U32,OutstandingRPCLight*> OutstandingRequests; // outstanding RPC's. A map from xid to OutstandingRPCLight
  Align4 u32 CurrXid;
  static void waitCallBack(char *data, int len, void *callbackdata); // internal callback for synchronous RPCs

  // handles a message from the UDP layer and invokes any registered callbacks. For synchronous
  // RPCs, the callback is provided by the library and unblocks the RPC. For asynchronous
  // RPCs, the callback is provided by the user. See RPCCallbackFunc above for more
  // information on the callback.
  void handleMsg(IPPort *dest, u32 req, u32 xid, u32 flags, char *data, int len);

protected:
  OutstandingRPCLight *RequestLookupAndDelete(u32 xid);

public:
  RPCClientLight();
  ~RPCClientLight(){
    exitThreads();
    Sleep(1000);
  }

  // callbackdata is data to be passed to the callback function
  // When RPC gets response, argument "data" will be deleted automatically, so "data" must be
  // dynamically allocated by caller.
  void AsyncRPC(IPPort dest, int rpcno, u32 flags, Marshallable *data, RPCCallbackFunc callback, void *callbackdata);

  // Returns a buffer that caller must later free using MsgBuffer::Free().
  // Comment about parameter "data" for AsyncRPC applies here too
  char *SyncRPC(IPPort dest, int rpcno, u32 flags, Marshallable *data);
};

// *********************************** SERVER STUFF ************************************



class RPCServerLight;

// handle used to identify an RPC invocation
class HandleDeferred {
public:
  HandleDeferred(RPCServerLight *s, MsgIdentifier &m){ server=s; msgid=m; }
  RPCServerLight *server;
  MsgIdentifier msgid;
};

// information about a deferred RPC
class RPCDeferred {
public:
  RPCDeferred(){}
  RPCDeferred(IPPort *dst, u32 r, u32 x, u32 f, char *d, int l){ dest = *dst; req=r; xid=x; flags=f; data=d; len=l;}
  IPPort dest;
  u32 req;
  u32 xid;
  u32 flags;
  char *data;
  int len;
  // linklist stuff
  RPCDeferred *prev, *next;
};

typedef Marshallable *(*RPCProcLight)(char *, int *, void *);
// first parameter of RPCProcLight is pointer to packet data.
// Second parameter is a pointer to a flag.
// If the flag is set, the RPC layer will mark the RPC request as deferred,
// and will later call the remote procedure again, after someone invokes
//      RPCServer::SignalDeferred(handle) or
//      RPCServerLight::SignalDeferred(handle)
//      [depending on whether the server is running the regular or light version]
// where handle is the third parameter passed to the RPC Procedure, which serves
// to identify the RPC instance. The handle should be be used once only
// (i.e., do not call SignalDeferred() on the handle handle twice), since
// it will be freed by SignalDeferred.


class RPCServerLight : public TCPDatagramCommunicationLight {
private:
  // stuff for deferred execution of RPCs
  SkipList<MsgIdentifier, RPCDeferred*> DeferredWaiting; // deferred RPCs waiting for some condition
  LinkList<RPCDeferred> DeferredReady;                    // queue of deferred RPCs ready for execution
  RWLock Deferred_lock;  // lock for accessing both DeferredWaiting and DeferredReady
  Semaphore DeferredReady_sem;  // posted when something is inserted into DeferredReady
  int ForceStop;  // set to 1 will ask the DeferredExecutor thread to exit
  void *DeferredExecutor_thr; // stores handle for deferred executor thread

  RPCProcLight *Procs;
  unsigned NProcs; // number of registered procedures, which will range from 0 to NFuncs-1

  void handleMsg(IPPort *dest, u32 req, u32 xid, u32 flags, char *data, int len);

  // thread for executing deferred RPCs
  static DWORD WINAPI DeferredExecutorThread(void *parm);

public:
  RPCServerLight(RPCProcLight *procs, int nprocs, int portno);
  static int SignalDeferred(void *handledeferred); // returns 0 if handle found, 1 if not
};


#endif
