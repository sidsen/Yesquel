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

#ifndef _STORAGESERVER_RPC_H
#define _STORAGESERVER_RPC_H

#if defined(GAIAUDP)
#include "../udp.h"
#include "../udpfrag.h"
#include "../grpc.h"
#elif defined(TCPDATAGRAM_LIGHT)
#include "../tcpdatagram-light.h"
#include "../grpctcp-light.h"
#else
#include "../tcpdatagram.h"
#include "../grpctcp.h"
#endif
#include "../task.h"


// stubs
int NULLRPCstub(RPCTaskInfo *rti);
int WRITERPCstub(RPCTaskInfo *rti);
int READRPCstub(RPCTaskInfo *rti);
int FULLWRITERPCstub(RPCTaskInfo *rti);
int FULLREADRPCstub(RPCTaskInfo *rti);
int LISTADDRPCstub(RPCTaskInfo *rti);
int LISTDELRANGERPCstub(RPCTaskInfo *rti);
int ATTRSETRPCstub(RPCTaskInfo *rti);
int PREPARERPCstub(RPCTaskInfo *rti);
int COMMITRPCstub(RPCTaskInfo *rti);
int SHUTDOWNRPCstub(RPCTaskInfo *rti);
int STARTSPLITTERRPCstub(RPCTaskInfo *rti);
int FLUSHFILERPCstub(RPCTaskInfo *rti);
int LOADFILERPCstub(RPCTaskInfo *rti);
#endif
