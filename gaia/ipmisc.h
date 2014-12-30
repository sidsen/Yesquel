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

#ifndef _IPMISC_H
#define _IPMISC_H

// before including this file, include <winsock2.h>
#include "gaiatypes.h"
#include "util.h"
#include "inttypes.h"

struct UDPDest;

struct IPPort {
  u32 ip;
  u32 port;
  static int cmp(const IPPort &left, const IPPort &right){ return memcmp(&left, &right, sizeof(IPPort)); }
  bool operator <(const IPPort &right) const { return cmp(*this, right)<0; }
  void set(u32 i, u32 p){ ip=i; port=p; }
  void invalidate(){ ip=0; port=0; }
  void getUDPDest(UDPDest *udpdest);
};

// a destination for sending messages
struct UDPDest {
  sockaddr_in destaddr;
  int sockaddr_len;
  IPPort getIPPort(void); // gets a unique ID for UDP destination
  UDPDest(){}
  UDPDest(IPPort ipport){ ipport.getUDPDest(this); }
};

// information for a given IP-port pair
struct IPPortInfo {
  SOCKET fd;        // fd on which to send data
};

struct MsgIdentifier {
  IPPort source;
  u32 xid;
  static unsigned hash(const MsgIdentifier &m){ return *((u32*)&m) ^ *((u32*)&m+1) ^ *((u32*)&m+2); }
  static int cmp(const MsgIdentifier &l, const MsgIdentifier &r){ return memcmp(&l, &r, sizeof(MsgIdentifier)); }
};

class Marshallable {
public:
  virtual ~Marshallable(){}
  virtual int marshall(LPWSABUF bufs, int maxbufs) = 0;
  virtual void demarshall(char *buf) = 0;
};

#ifdef GAIAUDP
class MsgBuffer {
#define MSGBUFFERMAGIC "MSGBUFFER123456"
public:
  static void Free(char *ptr);
  static char *Alloc(int size);
};
#endif

// Flags for an RPC/UDPFRAG layers
#define MSG_FLAG_IDEMPOTENT 0x01 // whether RPC is idempotent
#define MSG_FLAG_FRAGMENTED 0x02 // whether UDP/RPC request was fragmented

class IPMisc {
public:
  static u32 resolveName(char *name, unsigned preferredprefix16=0); // return IP address of a given name
  static u32 getMyIP(unsigned preferredprefix16=0); // return my own ip address
};

#endif
