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

#ifndef _TCPDATAGRAM_COMMON_H
#define _TCPDATAGRAM_COMMON_H

// common definitions for tcpdatagram.h and tcpdatagram-light.h

#include "ipmisc.h"

// info about an RPC to be sent
struct DatagramMsg {
  Marshallable *data;
  IPPort ipport;
  u32 req;  // see wire format below for the meaning of req, xid, flags
  u32 xid;
  u32 flags;
  bool freedata;
  DatagramMsg(){}
  DatagramMsg(Marshallable *d, IPPort ip, u32 r, u32 x, u32 f, bool fd) : 
    data(d), ipport(ip), req(r), xid(x), flags(f), freedata(fd){}
};

#define FLAG_HID(hid) ((hid)<<16)       // given hid, returns corresponding bits in flag
#define FLAG_GET_HID(flag) ((flag)>>16) // extract hid bits from flag

// wire format for RPC header
struct DatagramMsgHeader {
  u32 cookie; // cookie to identify beginning of header
  u32 flags;  // The 16 high bits of flags are a hash id, which determines which
              // server thread will handle the request. Requests with the same
              // hashid are guaranteed to be handled by the same server thread.
              // The 16 low bits of flags are currently unused flags
  u32 size; // size of payload (does not include header or footer)
  u32 req;  // request number (rpc number)
  u32 xid;  // unique per-sender identifier for request
};

#endif
