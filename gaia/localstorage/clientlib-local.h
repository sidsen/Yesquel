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

// clientlib-local.h: Client library for Gaia

#ifndef _CLIENTLIBLOCAL_H
#define _CLIENTLIBLOCAL_H

#include "../debug.h"
#include "../util.h"

#include "../gaiarpcaux.h"
#include "../datastruct.h"
#include "../clientlib/supervalue.h"
#include <set>
#include <list>

#define CACHE_HASHTABLE_SIZE 64
//#define CACHE_HASHTABLE_SIZE 300000

// cache only the first MAX_READS_TO_CACHE
#define MAX_READS_TO_CACHE 1000


// Transaction running locally at a client
class LocalTransaction
{
private:
  int State;          // 0=valid, -1=aborted, -2=aborted due to I/O error
  Timestamp StartTs;
  Tid Id;
  int readsCached;
  bool hasWrites;

  struct TxCacheEntry {
  public:
    COid coid;
    ~TxCacheEntry(){}
    Ptr<Valbuf> vbuf;
    COid *GetKeyPtr(){ return &coid; }
    static unsigned HashKey(COid *i){ return COid::hash(*i); }
    static int CompareKey(COid *i1, COid *i2){ return COid::cmp(*i1, *i2); }
    static void delEntry(TxCacheEntry *tce){ delete tce; }
  };

  SkipList<COid,TxCacheEntry*> Cache;

  void ClearCache(void);
  void UpdateCache(COid *coid, Ptr<Valbuf> &buf);
  int tryLocalRead(COid coid, Ptr<Valbuf> &buf);
  void Wsamemcpy(char *dest, LPWSABUF wsabuf, int nbufs);
  int auxprepare(bool remote2pc, Timestamp &chosents);
  int auxcommit(int outcome, bool remote2pc, Timestamp committs);


public:
  LocalTransaction();
  ~LocalTransaction();

  // the methods below are as in class Transaction
  int start();
  int startDeferredTs(void);
  int write(COid coid, char *buf, int len);
  int writeWsabuf(COid coid, int nbufs, LPWSABUF bufs);
  int put(COid coid, char *buf, int len){ return write(coid, buf, len); }
  int put2(COid coid, char *data1, int len1, char *data2, int len2);
  int put3(COid coid, char *data1, int len1, char *data2, int len2, char *data3, int len3);
  int vget(COid coid, Ptr<Valbuf> &buf);
  int vsuperget(COid coid, Ptr<Valbuf> &buf);
  static void readFreeBuf(char *buf);
  static char *allocReadBuf(int len);
  int addset(COid coid, COid toadd);
  int remset(COid coid, COid toremove);
  int writeSuperValue(COid coid, SuperValue *svret);
  int ListAdd(COid coid, ListCell *cell, GKeyInfo *ki);
  int ListDelRange(COid coid, u8 intervalType, ListCell *cell1, ListCell *cell2, GKeyInfo *ki);
  int AttrSet(COid coid, u32 attrid, u64 attrvalue);
  int tryCommit(bool remote2pc, Timestamp *retcommitts=0);
  int tryCommitSync(void);
  int abort(void);
};

#endif
