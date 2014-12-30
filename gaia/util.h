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

// util.h
//
// General utility classes
//  - Mutexes
//  - fixed size memory allocator

#ifndef _UTIL_H
#define _UTIL_H

#include "prng.h"
#include "utilsync.h"


// Pretty printer for binary data
void DumpData(char *ptr, int i, int firstoff=0);

// prints a buffer in a short format
void DumpDataShort(char *ptr, int n);

// command-line argument processing
char **convertargv(int argc, void **argv); // convert argv from windows
extern int optind;  // current argv being processed
extern int opterr;  // whether to print error messages
extern int optopt;    // returned last processed option character
extern char *optarg;  // returned option argument
int getopt(int argc, char **argv, const char *options);

int getNProcessors(); // returns number of processors
void pinThread(int processor); // pins current thread to a processor

__declspec(thread) extern u32 nowusdelta;
__declspec(thread) extern u64 nowuslasttime;

class OldTime {
public:
  // returns the time now in milliseconds
  static u64 now(void){
    FILETIME ft;
    GetSystemTimeAsFileTime(&ft);
		return (((u64) ft.dwHighDateTime << 32) | ft.dwLowDateTime)/10000;
  }

  // return the time now in microseconds, added with a small
  // counter in case previous call returned same value (due to
  // granularity of internal clock)
  // Thread safe
  static u64 nowusunique(void){
    u64 now;
    FILETIME ft;
    //nowuslock.lock();
    GetSystemTimeAsFileTime(&ft);
		now=(((u64) ft.dwHighDateTime << 32) | ft.dwLowDateTime)/10;
    if (now==nowuslasttime){
      now += ++nowusdelta;
    }
    else {
      nowusdelta=0;
      nowuslasttime=now;
    }
    //nowuslock.unlock();
    return now;
  }
};

class Time {
private:
  static u64 Offset;
  static double Frequency;
public:
  static void init(void);

  // return time in microseconds
  static u64 nowus(void){
    LARGE_INTEGER pc;
    if (Offset==-1LL) init();
    QueryPerformanceCounter(&pc);
    return (u64)(pc.QuadPart / Frequency) + Offset;
  }

  // return time in milliseconds
  static u64 now(void){
    return nowus()/1000;
  }
};

#endif
