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

#ifndef _GRPCTCP_COMMON_H
#define _GRPCTCP_COMMON_H

#include "tcpdatagram-common.h"

class RPCClient;

// This is the callback function passed on to an asynchronous RPC call.
// The callback is invoked when the RPC response arrives. Data has
// the unmarshalled response, and callbackdata is any data chosen
// by the entity which set up the callback function.
// The callback function should not free data, as it will be freed elsewhere.
// The place where it is freed depends on the return value. If the callback
// returns false, the RPC library frees data subsequently. If it returns
// true then it does not free the data and the application is supposed
// to do it by calling MsgBuffer::free().
typedef void (*RPCCallbackFunc)(char *data, int len, void *callbackdata);

#endif
