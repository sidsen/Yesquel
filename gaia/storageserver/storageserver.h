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

// storageserver.h
//
// This is an include file for invoking the functions of the storage server
// directly.

#ifndef _STORAGESERVER_H
#define _STORAGESERVER_H

#include "../gaiarpcaux.h"
#include "../newconfig.h"

// must call before invoking any of the functions below
void InitStorageServer(HostConfig *hc);

// remote procedures
Marshallable *NULLRPC(NullRPCData *d);
Marshallable *WRITERPC(WriteRPCData *d);
Marshallable *READRPC(ReadRPCData *d, void *handle, bool &defer);
Marshallable *FULLWRITERPC(FullWriteRPCData *d);
Marshallable *FULLREADRPC(FullReadRPCData *d, void *handle, bool &defer);
Marshallable *LISTADDRPC(ListAddRPCData *d);
Marshallable *LISTDELRANGERPC(ListDelRangeRPCData *d);
Marshallable *ATTRSETRPC(AttrSetRPCData *d);
Marshallable *PREPARERPC(PrepareRPCData *d, void *&state, void *rpctasknotify);
Marshallable *COMMITRPC(CommitRPCData *d);
Marshallable *SHUTDOWNRPC(ShutdownRPCData *d);
Marshallable *STARTSPLITTERRPC(StartSplitterRPCData *d);
Marshallable *FLUSHFILERPC(FlushFileRPCData *d);
Marshallable *LOADFILERPC(LoadFileRPCData *d);

// Auxilliary function to be used by server implementation
// Wake up a task that was deferred, by sending a wake-up message to it
void ServerAuxWakeDeferred(void *handle);

// a function that schedules an exit to occur after a while (2 seconds)
void ScheduleExit();


#endif
