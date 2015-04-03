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

#include "../tmalloc.h"
#include "../debug.h"
#include "../gaiarpcaux.h"

#include "../task.h"

#include "storageserver.h"
#include "storageserverstate.h"
#include "storageserver-rpc.h"

#include "server-splitter.h"

//RPCServer *ServerPtr;

int NULLRPCstub(RPCTaskInfo *rti){
  NullRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = NULLRPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int WRITERPCstub(RPCTaskInfo *rti){
  WriteRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = WRITERPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int READRPCstub(RPCTaskInfo *rti){
  ReadRPCData d;
  Marshallable *resp;
  bool defer;
  defer = false;
  d.demarshall(rti->data);
  resp = READRPC(&d, (void*) rti, defer);
  if (defer) return SchedulerTaskStateWaiting;
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int FULLWRITERPCstub(RPCTaskInfo *rti){
  FullWriteRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = FULLWRITERPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int FULLREADRPCstub(RPCTaskInfo *rti){
  FullReadRPCData d;
  Marshallable *resp;
  bool defer;
  defer = false;
  d.demarshall(rti->data);
  resp = FULLREADRPC(&d, (void*) rti, defer);
  if (defer) return SchedulerTaskStateWaiting;
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int LISTADDRPCstub(RPCTaskInfo *rti){
  ListAddRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = LISTADDRPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int LISTDELRANGERPCstub(RPCTaskInfo *rti){
  ListDelRangeRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = LISTDELRANGERPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int ATTRSETRPCstub(RPCTaskInfo *rti){
  AttrSetRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = ATTRSETRPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int PREPARERPCstub(RPCTaskInfo *rti){
  PrepareRPCData d;
  Marshallable *resp;
  int res;
  d.demarshall(rti->data);


  if (rti->State == 0){
    resp = PREPARERPC(&d, rti->State, (void*) rti);
    if (!resp){ // no response yet
      assert(rti->State);
      return SchedulerTaskStateWaiting; // more work to do
    }
  } else {
    if (rti->hasMessage()){
      TaskMsgData msg;
      res = rti->getMessage(msg); assert(res == 0);
      assert(msg.data[0] == 0xb0); // this is just to check the response (which has no relevant data)
    }
    resp = PREPARERPC(&d, rti->State, (void*) rti);
    assert(resp);
    assert(rti->State == 0);
  }
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int COMMITRPCstub(RPCTaskInfo *rti){
  CommitRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = COMMITRPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int SHUTDOWNRPCstub(RPCTaskInfo *rti){
  ShutdownRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = SHUTDOWNRPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

int STARTSPLITTERRPCstub(RPCTaskInfo *rti){
  StartSplitterRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = STARTSPLITTERRPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}
int FLUSHFILERPCstub(RPCTaskInfo *rti){
  FlushFileRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = FLUSHFILERPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}
int LOADFILERPCstub(RPCTaskInfo *rti){
  LoadFileRPCData d;
  Marshallable *resp;
  d.demarshall(rti->data);
  resp = LOADFILERPC(&d);
  rti->setResp(resp);
  return SchedulerTaskStateEnding;
}

// Auxilliary function to be used by server implementation
// Wake up a task that was deferred, by sending a wake-up message to it
void ServerAuxWakeDeferred(void *handle){
  tsendWakeup((TaskInfo*)handle);
}

// handler that will exit the server
int ExitHandler(void *p){
  exit(0);
}

// a function that schedules an exit to occur after a while (2 seconds)
void ScheduleExit(){
  TaskEventScheduler::AddEvent(tgetThreadNo(), ExitHandler, 0, 0, 2000);
}
