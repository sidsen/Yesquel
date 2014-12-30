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
#include "tmalloc.h"
#include "gaiaoptions.h"
#include "ipmisc.h"

#ifdef GAIAUDP
void MsgBuffer::Free(char *ptr){
  assert(ptr);
  char *header = ptr - sizeof(FixedAllocator::PadBefore)-sizeof(DatagramMsgHeader);
  if (memcmp(header, MSGBUFFERMAGIC, 16)==0) delete [] header; // our magic word
  else if (memcmp(header, PADBEFOREMAGIC, 12)==0) UDPCommunication::freeIncomingBuf(ptr); // from FixedAllocator
  else assert(0);
}

char *MsgBuffer::Alloc(int size){
  char *ptr;
  ptr = new char[size+sizeof(FixedAllocator::PadBefore)+sizeof(DatagramMsgHeader)];
  memcpy(ptr, MSGBUFFERMAGIC, 16);
  memset(ptr+16, 0, sizeof(DatagramMsgHeader));
  assert(sizeof(FixedAllocator::PadBefore)==16); // Paddings for FixedAllocator and MsgBuffer must have same size
  return ptr+sizeof(FixedAllocator::PadBefore)+sizeof(DatagramMsgHeader);
}
#endif

IPPort UDPDest::getIPPort(void){
  IPPort ipport;
  ipport.set(*(u32*)&destaddr.sin_addr, (u32)destaddr.sin_port);
  return ipport;
}

void IPPort::getUDPDest(UDPDest *udpdest){
  udpdest->destaddr.sin_family = AF_INET;
  udpdest->destaddr.sin_port = port;
  udpdest->sockaddr_len=16;
  memcpy((void*)&udpdest->destaddr.sin_addr, (void*)&ip, 4);
}

// return IP address of a given name
u32 IPMisc::resolveName(char *name, unsigned preferredprefix16){
  char portstr[10];
	struct addrinfo ai, *result, *ptr;
  //struct hostent *he;
	int res;
  u32 thisip, ip192, ipprefer, ipother;

	ZeroMemory(&ai, sizeof(ai));
	ai.ai_family = AF_INET;
	ai.ai_socktype = SOCK_DGRAM;
	ai.ai_protocol = IPPROTO_UDP;

	sprintf(portstr, "%d", 1);
  //other implementation
  res = getaddrinfo(name, portstr, &ai, &result);
  if (res) return 0;
  ptr = result;
  ip192 = ipother = ipprefer = 0;
  //printf("Resolve %s:\n", name);
  while (ptr){
    thisip = *(u32*) &ptr->ai_addr->sa_data[2];
    if ((thisip & 0xffff) == preferredprefix16) ipprefer = thisip;
    if ((thisip & 0xff) == 0xc0) ip192 = thisip;
    else 
      //if (!ipother)  // uncomment this line to choose first IP in list
      ipother = thisip; // pick last IP in list
    //printf("  %08x\n", thisip);
    ptr = ptr->ai_next;
  }

  //printf("resolve %s = %08x %08x\n", name, ipother, ip192);
  if (ipprefer){
    //printf("Using preferred IP %08x\n", ipprefer);
    return ipprefer;
  }
  else {
    if (ip192){
      //printf("Unsing non-preferred 192 IP %08x\n", ip192);
      return ip192;
    }
    else {
      //printf("Unsing non-preferred IP %08x\n", ipother);
      return ipother;
    }
  }

  // old implementation (doesn't work, returns IP address 0x00000002)
  //he = gethostbyname(name); assert(he);
  //assert(he->h_addrtype == AF_INET);
  //while (he->h_addr_list[i] != 0 && !ipother){
  //  thisip = * (u32*) he->h_addr_list[i];
  //  if ((thisip & 0xff) == 0xc0) ip192 = thisip;
  //  else ipother = thisip;
  //}
  //if (ip192) return ip192;
  //else return ipother;
}

// return my own ip address as a u32
u32 IPMisc::getMyIP(unsigned preferredprefix16){
  char localhostname[256];
  int res;
  //struct hostent *he;
  // int i;
  //u32 thisip, ip192, ipother;

  res = gethostname(localhostname, sizeof(localhostname)); assert(!res);
  return resolveName(localhostname, preferredprefix16);

  // old code
  //he = gethostbyname(localhostname); assert(he);
  //assert(he->h_addrtype == AF_INET);
  //i=0;
  //ip192 = ipother = 0;
  //printf("My IPs:\n");
  //while (he->h_addr_list[i] != 0 && !ipother){
  //  thisip = * (u32*) he->h_addr_list[i];
  //  printf("  %08x\n", thisip);
  //  if ((thisip & 0xff) == 0xc0) ip192 = thisip;
  //  else ipother = thisip;
  //}
  //printf("My ips %08x %08x\n", ipother, ip192);
  //if (ip192) return ip192;
  //else return ipother;
}
