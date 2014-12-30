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

//
// util.cpp

#include "stdafx.h"
#include "tmalloc.h"
#include "debug.h"
#include "util.h"


static void print16(unsigned char *ptr, int n)
{ int i; if (n > 16) n = 16; for (i = 0; i < n; ++i){
    printf("%02x", *ptr++); if (i % 8 == 7)  putchar('-');
    else putchar(' '); }
  ptr -= n; for (;i<16;i++) printf("   "); printf("  ");
  for (i = 0; i < n; i++) if (isprint((u8)*ptr)) putchar(*ptr++);
                          else { ++ptr; putchar('.'); }
  putchar('\n'); }

// Pretty printer for binary data
void DumpData(char *ptr, int i, int firstoff){
  while (i>0){ printf("%04x:  ", firstoff); print16((unsigned char*)ptr,
  i); ptr += 16; i -= 16; firstoff += 16; }}

void DumpDataShort(char *ptr, int n){
  if (!ptr) return;
  for (int i=0; i < n; ++i){
    if (isprint((u8)*ptr)) putchar(*ptr++);
    else if (!*ptr){ ++ptr; putchar('0'); }
    else { ++ptr; putchar('.'); }
  }
}

// convert from windows argv to character argv
char **convertargv(int argc, void **argv){
  char **ret;
  int i;
	char tmpstr[256];
  size_t dummy;

  assert(argc >= 1);
  ret = (char**) malloc(sizeof(char*) * argc);
  for (i=0; i<argc; ++i){
    wcstombs_s(&dummy, tmpstr, sizeof(tmpstr), (_TCHAR*)argv[i], _TRUNCATE);
    tmpstr[sizeof(tmpstr)-1]=0;
    ret[i] = (char*) malloc(strlen(tmpstr)+1);
    strcpy(ret[i], tmpstr);
  }
  return ret;
}

// Parse command-line options
int optind=1;  // current argv being processed
int opterr=1;  // whether to print error messages
int optopt;    // returned last processed option character
char *optarg;  // returned option argument
int getopt(int argc, char **argv, const char *options){
  static char *contopt=0; // point from where to continue processing options in the same argv
  const char *ptr;

  if (!contopt){
    if (optind >= argc || *argv[optind] != '-') return -1;
    if (argv[optind][1]=='-'){ ++optind; return -1; }
  }
  optopt = contopt ? *contopt : argv[optind][1];
  ptr=strchr(options,optopt);
  if (!ptr){
    if (opterr) fprintf(stderr, "Unrecognized option %c\n", optopt);
    // advance
    if (contopt && contopt[1]) ++contopt;
    else if (!contopt && argv[optind][2]) contopt=&argv[optind][2];
    else { contopt=0; ++optind; }
    return '?';
  }
  if (ptr[1]==':'){ // expecting argument
    if (contopt && contopt[1]){ optarg=contopt+1; contopt=0; ++optind; return optopt; }
    contopt=0;
    if (++optind == argc){
      optarg=0; 
      if (opterr) fprintf(stderr, "Missing argument for option %c\n", optopt);
      return ':';
    }
    optarg=argv[optind++];
  }
  else {
    // no argument
    if (contopt && contopt[1]) ++contopt;
    else if (!contopt && argv[optind][2]) contopt=&argv[optind][2];
    else { contopt=0; ++optind; }
  }
  return optopt;
}

int getNProcessors(){
  SYSTEM_INFO si;
  GetSystemInfo(&si);
  return (int)si.dwNumberOfProcessors;
}

void pinThread(int processor){ // pins a thread to a processor
  //SetThreadIdealProcessor(GetCurrentThread(), processor); // pin thread to processor
  SetThreadAffinityMask(GetCurrentThread(), 1<<processor); // stronger pin
}

__declspec(thread) u32 nowusdelta = 0;
__declspec(thread) u64 nowuslasttime = 0;

u64 Time::Offset=-1LL;
double Time::Frequency=0.0;

void Time::init(void){
  if (Offset!=-1LL) return; // already initialized
  LARGE_INTEGER pc, pf;
  FILETIME ft;
  u64 now;
  int res;

  res = QueryPerformanceFrequency(&pf); assert(res);
  if (res==0){
    fprintf(stderr, "High-resolution clock not available\n");
    exit(1);
  }
  Frequency = (double) pf.QuadPart / 1e6; // frequency in us

  GetSystemTimeAsFileTime(&ft);
	now=(((u64) ft.dwHighDateTime << 32) | ft.dwLowDateTime)/10; // time now in us
  QueryPerformanceCounter(&pc);
  Offset = now - (u64)(pc.QuadPart / Frequency);
}
