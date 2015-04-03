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

// taskdefs.h
// global definitions for thread classes, immediate functions, fixed tasks, thread context spaces

#ifndef _TASKDEFS_H
#define _TASKDEFS_H

#include "gaiaoptions.h"

//------------------------------------ Limits -----------------------------------------
#define NFIXEDTASKS 32 // max # of fixed tasks
#define NIMMEDIATEFUNCS 32 // max # of immediate functions
#define THREADCONTEXT_SHARED_SPACE_SIZE 32 // max # of entries in space shared by tasks of the same thread

//--------------------------------- Thread classes -------------------------------------
// tcpdatagram.h
#define TCLASS_RECEIVE 1   // threads to receive TCP data
#define TCLASS_WORKER  2   // threads for processing incoming requests
#define TCLASS_SEND    3   // threads to send TCP data
#define TCLASS_CLIENT  4   // threads create TCP data to be sent
#define TCLASS_WARNING 5   // thread that issues warnings
#define TCLASS_DISKLOG 6   // thread that logs data to disk
#define TCLASS_SPLITTER 7  // thread that splits tree nodes (storageserver-splitter.h)

//------------------------------- Immediate functions ----------------------------------
// core
#define IMMEDIATEFUNC_NOP  0  // do nothing (intended for testing)
#define IMMEDIATEFUNC_EXIT 1  // cause scheduler to exit
#define IMMEDIATEFUNC_EVENTSCHEDULER_ADD 4  // add an event to the event scheduler
// tcpdatagram.h
#define IMMEDIATEFUNC_HANDLEDISPATCHER 11
#define IMMEDIATEFUNC_SEND 12
#define IMMEDIATEFUNC_UPDATEIPPORTINFO 13
#define IMMEDIATEFUNC_POSTFIRSTRECEIVEBUF 14
// grpctcp.h
#define IMMEDIATEFUNC_SENDTOSEND 21
// disklog-win.h
#define IMMEDIATEFUNC_ENQUEUEDISKREQ 22
// warning.h
#define IMMEDIATEFUNC_WARNING 25
// storageserver-splitter.h
#define IMMEDIATEFUNC_SPLITTERTHREADNEWWORK 26
#define IMMEDIATEFUNC_SPLITTERTHREADREPORTWORK 27

//---------------------------------- Fixed tasks ---------------------------------------
// core
#define FIXEDTASK_EVENTSCHEDULER 0  
// tcpdatagram.h
#define FIXEDTASK_SEND 10
#define FIXEDTASK_BATCHFREEMULTIBUFS 11

//---------------------------- Thread context spaces ------------------------------------
// core
#define THREADCONTEXT_SPACE_EVENTSCHEDULER 0
// tcpdatagram.h
#define THREADCONTEXT_SPACE_TCPDATAGRAM 10 // index for shared space for tcpdatagram pointer
#define THREADCONTEXT_SPACE_SENDTASK 11 // index for shared space of send task and thread
// grpctcp.h
#define THREADCONTEXT_SPACE_RPCSERVER_WORKER 13 // index for shared space of RPC worker thread at server
// disklog-win.h
#define THREADCONTEXT_SPACE_DISKLOG 14 // index for shared space of disklog thread
// storageserver-splitter.h
#define THREADCONTEXT_SPACE_SPLITTER 15 // index for shared space of disklog thread


int taskGetCore(char *threadname);

#ifdef _TASKC_ // this is defined by task.cpp, so it is compiled only when included from task.cpp
// This function determines how threads are assigned to cores.
// Given a threadname, it returns a core to be allocated to it.
// Returns -1 if there are no more cores left to be allocated
int taskGetCore(char *threadname){
#ifdef SKIPLOG
  static int nextUnallocatedCore = 1; // next core that is yet unallocated
#else
  static int nextUnallocatedCore = 2; // next core that is yet unallocated
#endif
  static int allocatedCore[] = {0,0}; // cores that will be allocated are still marked as not allocated
  int retval;
  int nprocessors = getNProcessors();
  //if (!strcmp(threadname, "RECEIVE")){
  //  allocatedCore[0]=1; // only one thread to be allocated to core 0
  //  return 0;
  //}
  //if (!strcmp(threadname, "SEND")){
  //  allocatedCore[0]=1; // only one thread to be allocated to core 0
  //  return 2;
  //}
  //if (!strcmp(threadname, "WORKER")){
  //  allocatedCore[0]=1; // only one thread to be allocated to core 0
  //  return 1;
  //}
  //if (!strcmp(threadname, "DISKLOG")){
  //  allocatedCore[1]=1;
  //  return 1;
  //}
  if (nextUnallocatedCore < nprocessors){
    retval = nextUnallocatedCore;
    ++nextUnallocatedCore;}
  else retval = -1;
  return retval;
}
#endif

#endif
