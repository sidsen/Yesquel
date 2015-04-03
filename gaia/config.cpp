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

// config.cpp

#include "stdafx.h"
#include "tmalloc.h"
#include "config.h"
#include "util.h"

int ServerInfoConfig::SetAddr(void){
  int res;
  struct addrinfo ai, *result;
  char portstr[256];

	ZeroMemory(&ai, sizeof(ai));
	ai.ai_family = AF_INET;
	ai.ai_socktype = SOCK_DGRAM;
	ai.ai_protocol = IPPROTO_UDP;

	sprintf(portstr, "%d", serverport);
  res = getaddrinfo(servername, portstr, &ai, &result);
  if (res){ memset((void*) &addr, sizeof(UDPDest), 0); return -1; }

  memcpy(&addr.destaddr, (void*) result->ai_addr, sizeof(sockaddr_in));
  addr.sockaddr_len = result->ai_addrlen;

  return 0;
}

ServerInfoConfig SERVERS[NSERVERS] = {
  {0, "10.160.152.249", 12345},
//  {0, "157.57.50.62", 12345},
//  {1, "10.191.46.133", 12345},
//  {2, "157.58.56.54", 12345},
//  {3, "157.58.56.54", 12346}
//  {1, "2.3.4.5", 12345},
//  {2, "3.4.5.6", 12345},
//  {3, "4.5.6.7", 12345},
};

int ResolveSERVERS(void){
  int i;
  int retval=0, res;
  for (i=0; i < NSERVERS; ++i){
    res = SERVERS[i].SetAddr();
    if (res) retval=-1;
  }
  return retval;
}
