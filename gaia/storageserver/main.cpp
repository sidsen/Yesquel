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
#include "../tmalloc.h"
#include "../gaiaoptions.h"
#include "../../options.h"
#include "../debug.h"

#if defined(GAIAUDP)
#include "../udp.h"
#include "../udpfrag.h"
#include "../grpc.h"
#elif defined(TCPDATAGRAM_LIGHT)
#include "../tcpdatagram-light.h"
#include "../grpctcp-light.h"
#else
#include "../tcpdatagram.h"
#include "../grpctcp.h"
#endif

#include "../task.h"

#include "../gaiarpcaux.h"
#include "../warning.h"

#include "storageserver.h"
#include "storageserver-rpc.h"
#include "storageserverstate.h"
#include "../../kvinterface.h"

#ifdef STORAGESERVER_SPLITTER
#include "../../splitter-server/storageserver-splitter.h"
#include "../../splitter-server/splitter-client.h"
#include "../clientlib/clientdir.h"
extern StorageConfig *SC;
#endif

#ifdef TCPDATAGRAM_LIGHT
#define RPCClient RPCClientLight
#define RPCServer RPCServerLight
#endif


#pragma comment(lib, "ws2_32.lib")

#define DEBUG_LEVEL_DEFAULT 0
#define DEBUG_LEVEL_WHEN_LOGFILE 2 // debug level when -g option (use logfile) is chosen
#define MYSITE 0                   // used only if STORAGESERVER_SPLITTER is defined

extern StorageServerState *S; // defined in storageserver.c
//extern RPCServer *ServerPtr; // defined in storageserver-rpc.c

char *Configfile=0;


RPCProc RPCProcs[] = {  NULLRPCstub,         // RPC 0
                        WRITERPCstub,        // RPC 1
                        READRPCstub,         // RPC 2
                        FULLWRITERPCstub,    // RPC 3
                        FULLREADRPCstub,     // RPC 4
                        LISTADDRPCstub,      // RPC 5
                        LISTDELRANGERPCstub, // RPC 6
                        ATTRSETRPCstub,      // RPC 7
                        PREPARERPCstub,      // RPC 8
                        COMMITRPCstub,       // RPC 9
                        SHUTDOWNRPCstub,     // RPC 10
                        STARTSPLITTERRPCstub,// RPC 11
                        FLUSHFILERPCstub,    // RPC 12
                        LOADFILERPCstub      // RPC 13

#ifdef STORAGESERVER_SPLITTER
                        ,
                        SS_SPLITNODERPC,     // RPC 14
                        SS_GETROWIDRPCstub   // RPC 15
#endif
                     };
  
struct ConsoleCmdMap {
  char *cmd;
  char *helpmsg;
  int  (*func)(char *parm, StorageServerState *sss);
};

int cmd_help(char *parm, StorageServerState *sss);
int cmd_flush(char *parm, StorageServerState *sss);
int cmd_load(char *parm, StorageServerState *sss);
int cmd_flushfile(char *parm, StorageServerState *sss);
int cmd_loadfile(char *parm, StorageServerState *sss);
int cmd_print(char *parm, StorageServerState *sss);
int cmd_print_detail(char *parm, StorageServerState *sss);
int cmd_splitter(char *parm, StorageServerState *sss);
int cmd_quit(char *parm, StorageServerState *sss);
int cmd_debug(char *parm, StorageServerState *sss);

ConsoleCmdMap ConsoleCmds[] = {
  {"save_individual", ": flush contents to disk", cmd_flush},
  {"save", " filename:   flush contents to file", cmd_flushfile}, 
  {"help", ":            show this message", cmd_help},
  {"load_individual", ": load contents from disk", cmd_load},
  {"load", " filename:   load contents from file", cmd_loadfile},
  {"print", ":           print contents of storage", cmd_print},
  {"printdetail", ":     print contents of storage in detail", cmd_print_detail},
  {"splitter", ":        start splitter", cmd_splitter},
  {"quit", ":            quit server", cmd_quit},
  {"debug", " n:         set debug level to n", cmd_debug},
};

#define NConsoleCmds (sizeof(ConsoleCmds)/sizeof(ConsoleCmdMap))

int cmd_help(char *parm, StorageServerState *S){
  int i;
  putchar('\n');
  for (i = 0; i < NConsoleCmds; ++i){
    printf("%s%s\n", ConsoleCmds[i].cmd, ConsoleCmds[i].helpmsg);
  }
  putchar('\n');
  return 0;
}

int cmd_load(char *parm, StorageServerState *S){
  printf("Loading from disk...");
  S->cLogInMemory.loadFromDisk();
  printf(" Done!\n");
  return 0;
}

int cmd_flush(char *parm, StorageServerState *S){
  Timestamp ts;
  ts.setNew();
  Sleep(1000); // wait for a second

  printf("Warning: using \"flush\" will make the contents visible automatically to future runs of storageserver\n");
  printf("This is not true for \"flush2\"\n");

  printf("Flushing to disk...");
  S->cLogInMemory.flushToDisk(ts);
  printf(" Done!\n");
  
  return 0;
}

int cmd_loadfile(char *parm, StorageServerState *S){
  int res;
  if (!parm || strlen(parm)==0){
    printf("Missing filename\n");
    return 0;
  }
  printf("Loading from file...");
  res = S->cLogInMemory.loadFromFile(parm);
  if (res) printf("Error. Probably file does not exist\n");
  else printf(" Done!\n");
  return 0;
}

int cmd_flushfile(char *parm, StorageServerState *S){
  if (!parm || strlen(parm)==0){
    printf("Missing filename\n");
    return 0;
  }
  Timestamp ts;
  int res;
  ts.setNew();
  Sleep(1000); // wait for a second

  printf("Flushing to file...");
  res = S->cLogInMemory.flushToFile(ts, parm);
  if (res) printf("Error. Cannot write file for some reason\n");
  else printf(" Done!\n");
  
  return 0;
}

int cmd_print(char *parm, StorageServerState *S){
  S->cLogInMemory.printAllLooim();
  return 0;
}

int cmd_print_detail(char *parm, StorageServerState *S){
  S->cLogInMemory.printAllLooimDetailed();
  return 0;
}

int cmd_splitter(char *parm, StorageServerState *S){
#ifdef STORAGESERVER_SPLITTER
  if (!SC){
    printf("Starting splitter...\n");
    SC = new StorageConfig(Configfile, MYSITE);
    printf("Done\n");
  } else printf("Splitter already running\n");
#else
  printf("This storageserver does not have a splitter\n");
#endif

  return 0;
}

int cmd_quit(char *parm, StorageServerState *S){
  return 1;
}

int cmd_debug(char *parm, StorageServerState *S){
  if (parm) SetDebugLevel(atoi(parm));
  else printf("Debug requires a numerical parameter\n");
  return 0;
}

// convert str to lower case
void strlower(char *str){
  while (*str){
    *str = tolower(*str);
    ++str;
  }
}

void console(void){
  char line[256];
  char *cmd;
  char *parm;
  int i;
  int done=0;
  while (!done && !feof(stdin)){
    //extern int nticoids; printf("nticoids %d\n", nticoids);
    fgets(line, sizeof(line), stdin);
    line[sizeof(line)-1]=0;
    cmd = strtok(line, " \t\n");
    if (!cmd || !*cmd) continue;
    strlower(cmd);
    
    parm = strtok(0, " \t\n");
    for (i = 0; i < NConsoleCmds; ++i){
      if (strcmp(cmd, ConsoleCmds[i].cmd)==0){
        done = ConsoleCmds[i].func(parm, S);
        break;
      }
    }
    if (i == NConsoleCmds) printf("Unrecognized command %s. Try \"help\".\n", cmd); 
  }
}

void printsizes(void){
  printf("LogOneObjectInMemory %d\n", (int)sizeof(LogOneObjectInMemory));
  printf("SingleLogEntryInMemory %d\n", (int)sizeof(SingleLogEntryInMemory));
  printf("LogInMemory %d\n", (int)sizeof(LogInMemory));
  printf("WriteRPCParm %d\n", (int)sizeof(WriteRPCParm));
  printf("WriteRPCData %d\n", (int)sizeof(WriteRPCData));
  printf("WriteRPCRespData %d\n", (int)sizeof(WriteRPCRespData));
  printf("ReadRPCParm %d\n", (int)sizeof(ReadRPCParm));
  printf("ReadRPCData %d\n", (int)sizeof(ReadRPCData));
  printf("ReadRPCRespData %d\n", (int)sizeof(ReadRPCRespData));
  printf("PrepareRPCParm %d\n", (int)sizeof(PrepareRPCParm));
  printf("PrepareRPCData %d\n", (int)sizeof(PrepareRPCData));
  printf("TxWriteItem %d\n", (int)sizeof(TxWriteItem));
  printf("TxReadItem %d\n", (int)sizeof(TxReadItem));
  printf("PendingTxInfo %d\n", (int)sizeof(PendingTxInfo));
  printf("PendingTx %d\n", (int)sizeof(PendingTx));
  printf("OutstandingRPC %d\n", (int)sizeof(OutstandingRPC));
  //printf("RPCCachedResult %d\n", (int)sizeof(RPCCachedResult));
}

#ifdef NDEBUG
#define COMPILECONFIG "Release"
#else
#define COMPILECONFIG "Debug"
#endif


class RPCServerGaia : public RPCServer {
protected:
  void startupWorkerThread(){
    RPCServer::startupWorkerThread();
#ifdef STORAGESERVER_SPLITTER
    initServerTask(tgetTaskScheduler());
#endif
  }
public:
  RPCServerGaia(RPCProc *procs, int nprocs, int portno) : RPCServer(procs, nprocs, portno) {}
};


int _tmain(int argc, _TCHAR **av)
{
  int res;
  WSADATA wsadata;
  u32 myip;
  u16 myport;
  IPPort myipport;
  char **argv;
  char *argv0;
  int badargs, c;
  int useconsole=0;
  int loadfile=0;
  int uselogfile=0;
  int setdebug=0;
  int skipsplitter=0;
  char *loadfilename=0;
  char *logfilename=0;

  int *i = (int*) malloc(4);

  setvbuf(stdout, 0, _IONBF, 0);
  setvbuf(stderr, 0, _IONBF, 0);

  res = WSAStartup(0x0202, &wsadata);
	if (res){
		fprintf(stderr, "WSAStartup failed: %d\n", res);
    exit(1);
	}

  argv = convertargv(argc, (void**) av);
  // remove path from argv[0]
  argv0 = strrchr(argv[0], '\\');
  if (!argv0) argv0 = argv[0];
  else ++argv0;

  badargs=0;
  while ((c = getopt(argc,argv, "cd:g:l:o:s")) != -1){
    switch(c){
    case 'c':
      useconsole = 1;
      break;
    case 'd':
      setdebug = 1;
      SetDebugLevel(atoi(optarg));
      break;
    case 'g':
      uselogfile = 1;
      logfilename = (char*) malloc(strlen(optarg)+1);
      strcpy(logfilename, optarg);
      break;
    case 'l':
      loadfile = 1;
      loadfilename = (char*) malloc(strlen(optarg)+1);
      strcpy(loadfilename, optarg);
      break;
    case 'o':
      Configfile = (char*) malloc(strlen(optarg)+1);
      strcpy(Configfile, optarg);
      break;
    case 's':
      skipsplitter = 1;
      break;
    default:
      ++badargs;
    }
  }
  if (badargs) exit(1); // bad arguments
  argc -= optind;

  tinitScheduler(0);

  // parse arguments
  switch(argc){
  case 0:
    myport = SERVER_DEFAULT_PORT; // use default port
    break;
  case 1:
	  myport = atoi(argv[optind]);
    break;
  default:
    fprintf(stderr, "usage: %s [-cgs] [-d debuglevel] [-l filename] [-o configfile] [-g logfile] [portno]\n", argv0);
    fprintf(stderr, "   -c  enable console\n");
    fprintf(stderr, "   -d  set debuglevel to given value\n");
    fprintf(stderr, "   -g  use log file\n");
    fprintf(stderr, "   -l  load state from given file\n");
    fprintf(stderr, "   -o  use given configuration file\n");
    fprintf(stderr, "   -s  do not start splitter (storageserver-splitter version)\n");
    fprintf(stderr, "       This is useful with more than one server, in which case it may be better\n");
    fprintf(stderr, "       to start the splitter remotely after all servers have started already,\n");
    fprintf(stderr, "       otherwise the servers that start first may not be able to start their\n");
    fprintf(stderr, "       splitters since they cannot communicate with the other servers\n");
    exit(1);
  }

  if (!Configfile){
    Configfile = getenv("GAIACONFIG");
    if (!Configfile) Configfile = GAIA_DEFAULT_CONFIG_FILENAME;
  }

  // What follows is code for handling the config file
  ConfigState *cs;

  cs = ConfigState::ParseConfig(Configfile);
  if (!cs) exit(1); // cannot read configuration file

  myip = IPMisc::getMyIP(parser_cs->PreferredPrefix16);
  myipport.set(myip, htons(myport));

  UniqueId::init(myip);

  // try to find ourselves in host list of configuration file
  HostConfig *hc;
  hc = cs->Hosts.lookup(&myipport);
  if (hc==0){
    fprintf(stderr, "Cannot find my ip %08x and port %d in config file\n",
      ntohl(myipport.ip), ntohs(myipport.port));
    exit(1);
  }
  printf("Compilation time %s %s configuration %s\n", __DATE__, __TIME__, COMPILECONFIG);
  printf("Configuration file %s debuglog %s\n", Configfile, uselogfile ? "yes" : "no");
  printf("Host %s ip %d.%d.%d.%d port %d log %s store %s\n", hc->hostname, 
    myip & 0xff, (myip >> 8) & 0xff, (myip >> 16) & 0xff, (myip >> 24) & 0xff,
    hc->port, hc->logfile, hc->storedir);
  printf("Workers %d Senders %d Receivers %d\n", WORKERTHREADS, SENDTHREADS, TCP_MAX_RECEIVE_THREADS);

  // debugging information stuff
#if (!defined(NDEBUG) && defined(DEBUG) || defined(NDEBUG) && defined(DEBUGRELEASE))
  if (!setdebug){ // if setdebug, override defaults
    if (!uselogfile)  SetDebugLevel(DEBUG_LEVEL_DEFAULT);
    else SetDebugLevel(DEBUG_LEVEL_WHEN_LOGFILE);
  }

  if (!uselogfile) DebugInit(false);
  else DebugInit(true, logfilename);

#endif

  InitStorageServer(hc);
  int myrealport = hc->port; assert(myrealport != 0);

#ifdef GAIAUDP
  RPCServerGaia server(RPCProcs, sizeof(RPCProcs)/sizeof(RPCProc), myrealport, WORKERTHREADS);
#else
  RPCServerGaia server(RPCProcs, sizeof(RPCProcs)/sizeof(RPCProc), myrealport);
#endif

  //ServerPtr = &server;

  if (loadfile){
    printf("Load state from file %s...", loadfilename); fflush(stdout);
    S->cLogInMemory.loadFromFile(loadfilename);
    putchar('\n'); fflush(stdout);
  }

  server.launch(0); 
  Sleep(1000);

#if defined(STORAGESERVER_SPLITTER) && DTREE_SPLIT_LOCATION >= 2
  if (!skipsplitter){
    SC = new StorageConfig(Configfile, MYSITE);
#ifdef MULTI_SPLITTER
    SD = new SplitterDirectory(SC->CS, 0);
    KVInterfaceInit();  // **!** TODO: probably should always call this even if MULTI_SPLITTER is not defined,
                        // but the question is whether it will work if SD has not been initialized
#endif
  }
#endif

  if (useconsole){
    console();
  } else {
#ifndef GAIAUDP
    server.waitServerEnd(); // should never return
#else
    Sleep(INFINITE);
#endif
  }

  server.exitThreads();
  Sleep(500);

#if (!defined(NDEBUG) && defined(DEBUG) || defined(NDEBUG) && defined(DEBUGRELEASE))
  DebugUninit();
#endif

  WSACleanup();
}


