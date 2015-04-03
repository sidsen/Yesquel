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
// debug.cpp
//

#include "stdafx.h"
#include "debug.h"

int DebugLevel = 0;
void SetDebugLevel(int dl){ DebugLevel = dl; }

#if (!defined(NDEBUG) && defined(DEBUG) || defined(NDEBUG) && defined(DEBUGRELEASE))
HANDLE DebugHandle=0;

void DebugInit(bool uselogfile, char *logfile){
  int res;

  if (uselogfile){
    wchar_t lognamewstr[1024];
    size_t ret;

    if (!logfile){
      logfile = getenv(GAIADEBUG_ENV_VAR);
      if (!logfile) logfile = GAIADEBUG_DEFAULT_FILE;
    }

    // convert logname to wstr    
    res = mbstowcs_s(&ret, lognamewstr, 1024, logfile, _TRUNCATE); assert(res==0);

    DebugHandle = CreateFile(lognamewstr, GENERIC_WRITE, 0x7, 0, CREATE_ALWAYS,
                             FILE_ATTRIBUTE_NORMAL, 0);
  } else DebugHandle = 0;
}

void DebugUninit(){
  if (DebugHandle) CloseHandle(DebugHandle);
}

void DebugPrintf(int level, char *format, ...){
  char msg[1024];
  int res;
  DWORD written;
  va_list vl;
  va_start(vl, format); 

  vsnprintf(msg, sizeof(msg)-2, format, vl);
  msg[sizeof(msg)-2] = 0;
  if (DebugHandle)
    strcat(msg, "\r\n");
  else strcat(msg, "\n");
  if (DebugHandle)
    res = WriteFile(DebugHandle, msg, (int)strlen(msg), &written, 0);
  else 
    fputs(msg, stdout);
  va_end(vl);
}
#endif
