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
#include "gaia/clientlib/clientdir.h"
#include "kvinterface.h"
#include "splitter-server/splitter-client.h"
#include "gaia/storageserver/storageserver.h"
#include "gaia/debug.h"

StorageConfig *SC=0;
SplitterDirectory *SD=0;
const u8 MYSITE=0;

extern StorageConfig *InitGaia(int mysite);
extern void UninitGaia(StorageConfig *SC);

extern void YesqlInitGlobals();

void InitYesql(void){
#ifndef NOGAIA
  SC = InitGaia(MYSITE);  // initialize gaia (including reading config file)
#else
  SC = 0;
#endif

#if (!defined(NDEBUG) && defined(DEBUG) || defined(NDEBUG) && defined(DEBUGRELEASE))
// initialize debug log
  DebugInit(true); // true=use log file, false=use stdout
  SetDebugLevel(0);
  //DebugInit(false); // true=use log file, false=use stdout
  //SetDebugLevel(2);
#endif

  KVInterfaceInit(); // initialize kvinterface
  InitStorageServer(0); // initialize local key-value system
  SD = new SplitterDirectory(SC->CS, MYSITE); // initialize splitter directory
#if !defined(GAIAUDP) && !defined(STORAGESERVER_SPLITTER)
  CallSplitterConnect(); // connect to splitter server(s)
#endif
  YesqlInitGlobals(); // initialize globals for yesql
}

void UninitYesql(void){
  UninitGaia(SC);
}
