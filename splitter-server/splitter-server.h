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

#ifndef _SPLITTER_SERVER_H
#define _SPLITTER_SERVER_H

#include "../gaia/gaiarpcaux.h"
#include "splitterrpcaux.h"

#ifndef STORAGESERVER_SPLITTER
#define SPLITTER_PORT_OFFSET 10  // port of splitter is port of storageserver plus SPLITTER_PORT_OFFSET
#else
#define SPLITTER_PORT_OFFSET 0  // port of splitter is same port of storageserver
#endif

#define SPLITTER_STAT_MOVING_AVE_WINDOW 30 // window size for moving average of split time

void InitServerSplitter();
void UninitServerSplitter();

int SS_NULLRPCstub(RPCTaskInfo *rti);
int SS_SHUTDOWNRPCstub(RPCTaskInfo *rti);
int SS_SPLITNODERPCstub(RPCTaskInfo *rti);
int SS_GETROWIDRPCstub(RPCTaskInfo *rti);

Marshallable *SS_NULLRPC(NullRPCData *d);
Marshallable *SS_SHUTDOWNRPC(ShutdownRPCData *d);
Marshallable *SS_SPLITNODERPC(SplitnodeRPCData *d);
Marshallable *SS_GETROWIDRPC(GetRowidRPCData *d);

#endif
