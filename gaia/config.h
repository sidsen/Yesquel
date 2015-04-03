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

#ifndef _CONFIG_H
#define _CONFIG_H

#include "stdafx.h"
#include "gaiaoptions.h"

#define NSERVERS 1

// simple replica designation
// NREPLICAS is the number of replicas
// REPLICAOFFSET is the quantity to be added to the server of lowest index
//   to obtain its replicas. For instance, if the lowest index server is 1
//   then all replicas will be 1, 1+REPLICAOFFSET, 1+2*REPLICAOFFSET, etc.
// This scheme is just a placeholder for a real configuration.
// It is currently used by clientdir.cpp in function 
//    ObjectDirectory::GetServerReplicasIndex
// to return the set of all replicas
#define NREPLICAS 1
#define REPLICAOFFSET 1

class ServerInfoConfig {
public:
  int index;
  char *servername;
  int serverport;
  UDPDest addr;
  int SetAddr(void);
};

extern ServerInfoConfig SERVERS[NSERVERS];
extern int ResolveSERVERS(void);

struct ServerInfo {
  int index;
  UDPDest addr;
};

#endif
