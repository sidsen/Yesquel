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

#ifndef _NEWCONFIG_H
#define _NEWCONFIG_H

#define DEFAULT_PORT 12121

#include <hash_map>
#include <list>
#include <WinSock2.h>
#include "inttypes.h"
#include "ipmisc.h"
#include "datastruct.h"

using namespace std;

struct ServerHT {
  int id;
  IPPort ipport;
  ServerHT(int i, IPPort ipp){ id=i; ipport=ipp; }
  ServerHT(){}
  // stuff for HashTable
  ServerHT *prev, *next, *sprev, *snext;
  int GetKey(){ return id; }
  static unsigned HashKey(int i){ return (unsigned) i; }
  static int CompareKey(int i1, int i2){ if (i1<i2) return -1; else if (i1==i2) return 0; else return 1; }
};

#define SERVER_HASHTABLE_SIZE 64
#define GROUPCONFIG_HASHTABLE_SIZE 64
#define HOSTSCONFIG_HASHTABLE_SIZE 64

class GroupConfig {
public:
  HashTable<int,ServerHT> Servers;
  //hash_map<int,IPPort> Servers;
  int id;           // group id
  int nsites;       // number of sites where group is stored
  int nservers;     // number of servers per site
  int stripeMethod; // method used for striping
  int stripeParm;   // parameter for method used for striping
  int addSiteServer(int site, int server, char *hostname, int port, unsigned preferredprefix16);
  int check(int groupid);  // check for problems (eg, servers missing, etc). Print errors on stderr.
                    // return 0 for ok, 1 for error
  GroupConfig() : Servers(SERVER_HASHTABLE_SIZE){};

  // HashTable stuff
  GroupConfig *next, *prev, *snext, *sprev;
  int GetKey(){ return id; }
  static unsigned HashKey(int i){ return (unsigned)i; }
  static int CompareKey(int i1, int i2){ if (i1<i2) return -1; else if (i1==i2) return 0; else return 1; }
};

class HostConfig {
public:
  IPPort ipport;   // IP and port of host
  char *hostname;  // name of host
  int port;        // port number
  char *logfile;   // name of log file
  char *storedir;  // name of directory where objects are stored. Should end with '/'

  // HashTable stuff
  HostConfig *next, *prev, *snext, *sprev;
  IPPort *GetKeyPtr(){ return &ipport; }
  static unsigned HashKey(IPPort *i){ return (unsigned)(*(u32*)i ^ *(u32*)i+1); }
  static int CompareKey(IPPort *i1, IPPort *i2){ return memcmp((void*)i1, (void*)i2, sizeof(IPPort)); }
};

class RPCClientLight;

class ConfigState {
private:
  int nerrors;
  list<int> errRepeatedGroups;
  list<pair<IPPort,char*>> errRepeatedIPPort;
  
public:
  HashTable<int,GroupConfig> Groups;
  HashTableBK<IPPort,HostConfig> Hosts;
  int Ngroups;
  int ContainerAssignMethod;
  int ContainerAssignParm;
  unsigned PreferredPrefix16;
  
  void addGroup(GroupConfig *toadd);
  void addHost(HostConfig *toadd);
  void setNgroups(int ngroups){ Ngroups = ngroups; }
  void setContainerAssignMethod(int cam){ ContainerAssignMethod = cam; }
  void setContainerAssignParm(int cap){ ContainerAssignParm = cap; }
  void setPreferredIPPrefix(char *prefix);
  int check(void);  // checks for configuration problems. Print errors on stderr. 
                    // Return 0 for ok, 1 for error
#ifndef GAIAUDP
  int connectHosts(RPCClientLight *rpcc); // call clientconnect(UDPDest *udpdest) on all hosts
#endif
  ConfigState() : Groups(GROUPCONFIG_HASHTABLE_SIZE), Hosts(HOSTSCONFIG_HASHTABLE_SIZE)
  {
    Ngroups = ContainerAssignMethod = ContainerAssignParm = -1;
    PreferredPrefix16 = 0;
    nerrors = 0;
  }

  static ConfigState *ParseConfig(char *configfilename);
}; 

extern ConfigState *parser_cs;

#endif
