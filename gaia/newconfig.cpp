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
#include "debug.h"
#include "newconfig.h"
#include "util.h"
#ifndef GAIAUDP
#include "grpctcp.h"
#include "grpctcp-light.h"
#else
#include "grpc.h"
#endif

int GroupConfig::addSiteServer(int site, int server, char *hostname, int port, unsigned preferredprefix16){
	char portstr[10];
	struct addrinfo ai, *result, *ptr;
	int res;
  u32 ip192, ipother, ipprefer, thisip, chosenip;
  //SOCKET socketfd;
  IPPort ipport;

	ZeroMemory(&ai, sizeof(ai));
	ai.ai_family = AF_INET;
	ai.ai_socktype = SOCK_DGRAM;
	ai.ai_protocol = IPPROTO_UDP;

	sprintf(portstr, "%d", port);
  res = getaddrinfo(hostname, portstr, &ai, &result);
	if (res){
    fprintf(stderr, "warning: cannot resolve host %s\n", hostname);
  }

  res = getaddrinfo(hostname, portstr, &ai, &result);
	if (res){
    fprintf(stderr, "warning: cannot resolve host %s\n", hostname);
  }

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
  
  if (ipprefer) chosenip = ipprefer;
  else if (ip192) chosenip = ip192;
  else chosenip = ipother;
 

  //UDPDest dest;
  //if (result) memcpy(&dest.destaddr, (void*) result->ai_addr, sizeof(sockaddr_in));
  //else ZeroMemory(&dest.destaddr, sizeof(sockaddr_in));
  //printf("len %x family %x port %x\n", dest.sockaddr_len, dest.destaddr.sin_family, dest.destaddr.sin_port);
  //Servers[(site<<24)+server] = dest;

  //if (result) ipport = MakeIPPort(*(u32*)((char*)result->ai_addr+4), *(u16*)((char*)result->ai_addr+2));
  if (result) ipport.set(chosenip, htons(port));
  //Servers[(site<<24)+server] = ipport;
  Servers.insert(new ServerHT((site<<24)+server, ipport));
  return 0;
}

int GroupConfig::check(int groupid){
  int site, server;
  int retval=0;
  for (site=0; site < nsites; ++site){
    for (server=0; server < nservers; ++server){
 //     if (Servers.find((site<<24)+server) == Servers.end()){
      if (Servers.lookup((site<<24)+server) == 0){
          fprintf(stderr, "Missing configuration information for group %d site %d server %d\n", groupid, site, server);
        retval=1;
      }
    }
  }
  return retval;
}

void ConfigState::addGroup(GroupConfig *toadd)
{
  if (Groups.lookup(toadd->id)){
    // group already exists
    errRepeatedGroups.push_front(toadd->id);
    ++nerrors;
  }
  else Groups.insert(toadd);
}

void ConfigState::addHost(HostConfig *toadd)
{
  IPPort ipport;

  ipport.set(IPMisc::resolveName(toadd->hostname, PreferredPrefix16), htons(toadd->port));
  toadd->ipport = ipport;
  if (toadd->port == 0) toadd->port = DEFAULT_PORT;

  if (Hosts.lookup(&ipport) != 0){
    // repeated ip-port pair
    errRepeatedIPPort.push_front(pair<IPPort,char*>(ipport, toadd->hostname));
    ++nerrors;
  }
  else Hosts.insert(toadd);
}

void ConfigState::setPreferredIPPrefix(char *prefix){
  int byte1, byte2;
  sscanf(prefix, "%d.%d", &byte1, &byte2);
  PreferredPrefix16 = byte1 + (byte2 << 8);
}

int ConfigState::check(void){
  int retval;
  retval=0;

  if (Ngroups < 0){ fprintf(stderr, "Missing ngroups indication\n"); ++retval; }
  if (ContainerAssignMethod < 0){ fprintf(stderr, "Missing container_assign_method indication\n"); ++retval; }
  if (ContainerAssignParm < 0){ fprintf(stderr, "Missing container_assign_parm indication\n"); ++retval; }

  // check for repeated groups
  for (list<int>::iterator it = errRepeatedGroups.begin();
       it != errRepeatedGroups.end();
       ++it)
  {
    fprintf(stderr, "Repeated group %d\n", *it);
    ++retval;
  }

  // check for repeated host-port pairs
  for (list<pair<IPPort,char*>>::iterator it = errRepeatedIPPort.begin();
       it != errRepeatedIPPort.end();
       ++it)
  {
    fprintf(stderr, "Repeated host-port entry for host %s ip %x port %d\n", it->second, it->first.ip, it->first.port);
    ++retval;
  }

  GroupConfig *ptr;

  // check each group
  for (ptr=Groups.getFirst(); ptr != Groups.getLast(); ptr = Groups.getNext(ptr)){
    if (ptr->check(ptr->id)) ++retval;
  }

  // ensure all groups are there
  for (int i=0; i < Ngroups; ++i){
    if (Groups.lookup(i) == 0){
      fprintf(stderr, "Missing definition of group %d\n", i);
      ++retval;
    }
  }

  return retval;
}

ConfigState *ConfigState::ParseConfig(char *configfilename){
  extern FILE *yyin;
  extern int yyparse(void);
  FILE *f;
  int res;
  ConfigState *CS;

  CS = new ConfigState();

  // read and parse configuration
  f = fopen(configfilename, "r");
  if (!f){ 
    fprintf(stderr, "Cannot open configuration file %s\n", configfilename);
    return 0;
  }
  yyin = f;
  parser_cs = CS;
  res = yyparse();
  fclose(f);
  if (res || CS->check()){
    fprintf(stderr, "Problems reading configuration file %s\n", configfilename);
    return 0;
  }
  return CS;
}

#ifndef GAIAUDP
// call clientconnect() on all hosts
int ConfigState::connectHosts(RPCClientLight *rpcc){
  int res;
  HostConfig *hc;
  int err;

  err=0;
  //HashTableBK<IPPort,HostConfig> Hosts;
  for (hc = Hosts.getFirst(); hc != Hosts.getLast(); hc = Hosts.getNext(hc)){
    u32 ip = hc->ipport.ip;
    res=rpcc->clientconnect(hc->ipport);
    if (res){
      err=1;
      printf("Cannot connect to server at %08x\n", hc->ipport.ip); 
    }
  }
  return err;
}
#endif
