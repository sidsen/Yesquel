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

// shutdown.cpp
//
// Shuts down the Gaia servers by calling the shutdown RPC on them
//

#include "stdafx.h"
#include "../tmalloc.h"
#include "../gaiaoptions.h"
#include "../util.h"
#include "../gaiatypes.h"
#include "../clientlib/clientdir.h"
#include "../clientlib/clientlib.h"

#pragma comment(lib, "ws2_32.lib")

#ifdef DEBUG
extern int DebugLevel;
#endif

struct {
  char *cmdname;
  int cmdnumber;
} Cmds[] = {
  {"load",0},
  {"save",1},
  {"ping",2},
  {"shutdown",3},
  {"splitter",4},
  {0,-1} // to indicate end
};

int _tmain(int argc, _TCHAR* av[])
{
  int res;
  char *Configfile=0;
  WSADATA wsadata;
  int mysite=0;  // default site is 0
  int badargs, c;
  char *command, *commandarg;
  int cmd=-1;
  int i;
  int debuglevel=MININT;

  setvbuf(stdout, 0, _IONBF, 0);
  setvbuf(stderr, 0, _IONBF, 0);

  char **argv = convertargv(argc, (void**) av);
  // remove path from argv[0]
  char *argv0 = strrchr(argv[0], '\\');
  if (argv0) ++argv0; else argv0 = argv[0];

  badargs=0;
  while ((c = getopt(argc,argv, "o:d:")) != -1){
    switch(c){
    case 'd':
      debuglevel = atoi(optarg);
      break;
    case 'o':
      Configfile = (char*) malloc(strlen(optarg)+1);
      strcpy(Configfile, optarg);
      break;
    default:
      ++badargs;
    }
  }
  if (badargs) exit(1); // bad arguments
  argc -= optind;

  // parse arguments
  switch(argc){
  case 1:
    command = argv[optind];
    commandarg = 0;
    break;
  case 2:
    command = argv[optind];
    commandarg = argv[optind+1];
    break;
  default:
    fprintf(stderr, "usage: %s [-o config] [-d debuglevel] command [parm]\n", argv0);
    fprintf(stderr, "existing commands:\n");
    fprintf(stderr, "  load [filename]\n");
    fprintf(stderr, "  save [filename]\n");
    fprintf(stderr, "  ping\n");
    fprintf(stderr, "  shutdown\n");
    fprintf(stderr, "  splitter\n");
    exit(1);
  }

  i=0;
  while (Cmds[i].cmdname){
    if (!strcmp(command, Cmds[i].cmdname)) break;
    ++i;
  }
  cmd = Cmds[i].cmdnumber;
  if (cmd == -1){
    printf("Invalid command %s\n", command);
    printf("Valid commands are the following:");
    i=0;
    while (Cmds[i].cmdname){
      printf(" %s", Cmds[i].cmdname);
      ++i;
    }
    putchar('\n');
    exit(1);
  }

  printf("Executing %s command", command);
  if (commandarg) printf(" with parameter %s", commandarg);
  putchar('\n');

  res = WSAStartup(0x0202, &wsadata);
	if (res){
		fprintf(stderr, "WSAStartup failed: %d\n", res);
		return 1;
	}

  UniqueId::init();

  StorageConfig sc(GAIA_DEFAULT_CONFIG_FILENAME, (u8)mysite);

#ifdef DEBUG
  if (debuglevel != MININT) DebugLevel = debuglevel;
#endif

  switch(cmd){
  case 0: // load
    sc.loadServers(commandarg);
    break;
  case 1: // save
    sc.flushServers(commandarg);
    break;
  case 2: // ping
    sc.pingServers();
    break;
  case 3: // shutdown
    sc.shutdownServers();
    break;
  case 4: // splitter
    sc.startsplitterServers();
    break;
  default: assert(0);
  }

  printf("Done\n");

  WSACleanup();
  exit(0);
}
