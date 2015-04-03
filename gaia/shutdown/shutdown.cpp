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

int _tmain(int argc, _TCHAR* av[])
{
  int res;
  char *config;
  WSADATA wsadata;
  int mysite;

  setvbuf(stdout, 0, _IONBF, 0);
  setvbuf(stderr, 0, _IONBF, 0);

  char **argv = convertargv(argc, (void**) av);
  // remove path from argv[0]
  char *argv0 = strrchr(argv[0], '\\');
  if (argv0) ++argv0; else argv0 = argv[0];

  if (argc > 2){
    fprintf(stderr, "usage: %s [config]\n", argv0);
    exit(1);
  }
  
  if (argc == 2){ config = argv[1]; }
  else config = GAIA_DEFAULT_CONFIG_FILENAME;

  mysite = 0; // default site is 0

  res = WSAStartup(0x0202, &wsadata);
	if (res){
		fprintf(stderr, "WSAStartup failed: %d\n", res);
		return 1;
	}

  UniqueId::init();

  StorageConfig sc(GAIA_DEFAULT_CONFIG_FILENAME, (u8)mysite);
  sc.shutdownServers();
  //sc.pingServers();
  WSACleanup();
  exit(0);
}

