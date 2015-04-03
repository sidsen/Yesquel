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

// clientlib.h: Client library for Gaia

#ifndef _CLIENTLIB_H
#define _CLIENTLIB_H

#include <set>
#include <list>
#include <signal.h>

#include "../debug.h"
#include "../util.h"

#include "../ipmisc.h"

//#if defined(GAIAUDP)
//#include "../udp.h"
//#include "../udpfrag.h"
//#include "../grpc.h"
//#elif defined(TCPDATAGRAM_LIGHT)
//#include "../tcpdatagram-light.h"
//#include "../grpctcp-light.h"
//#else
//#include "../tcpdatagram.h"
//#include "../grpctcp.h"
//#endif

#include "../config.h"
#include "clientdir.h"
#include "../gaiarpcaux.h"
#include "../datastruct.h"
#include "supervalue.h"

#define STARTTS_MAX_STALE 50  // maximum staleness for start timestamp in ms
//#define STARTTS_NO_READMYWRITES // comment in to start timestamp in the past by STARTTS_MAX_STALE even if we committed, so
                                  // a thread will not see its own previous transactions. If commented out,
                                  // the start timestamp will be set to max(now-STARTTS_MAX_STALE,lastcommitts), so that
                                  // the thread will see its own committed transactions.

#define CACHE_HASHTABLE_SIZE 64
//#define CACHE_HASHTABLE_SIZE 50000

// cache only the first MAX_READS_TO_CACHE
#define MAX_READS_TO_CACHE 1000

// largest time in the past for choosing a deferred start timestamp.
// With deferred timestamps, the start timestamp is chosen to be
// the timestamp of the first item read by the transaction. But if that
// timestamp is older than MAX_DEFERRED_START_TS, then the start timestamp
// will be now minus MAX_DEFERRED_START_TS
#define MAX_DEFERRED_START_TS 1000


#define TID_TO_RPCHASHID(tid) ((u32)tid.d1)  // rpc hashid to use for a given tid. Currently, returns d1, which currently is the client's IP + PID.
                                        // Thus, all requests of the client are assigned the same rpc hashid, so they are all handled by the
                                        // same server thread.

// Initializes and uninitializes Gaia
StorageConfig *InitGaia(int mysite);
void UninitGaia(StorageConfig *SC);

class Transaction
{
private:
  int State;          // 0=valid, -1=aborted, -2=aborted due to I/O error
  StorageConfig *Sc;
  Timestamp StartTs;
  Tid Id;
  Set<IPPort> Servers;
  Set<IPPort> ServersAndReplicas;
  int readsCached;
  bool hasWrites;
#ifdef GAIA_OCC
  Set<COid> ReadSet;
#endif

  struct TxCacheEntry {
  public:
    COid coid;
    ~TxCacheEntry(){}
    Ptr<Valbuf> vbuf;
    TxCacheEntry *next, *prev, *snext, *sprev;
    COid *GetKeyPtr(){ return &coid; }
    static unsigned HashKey(COid *i){ return COid::hash(*i); }
    static int CompareKey(COid *i1, COid *i2){ return memcmp((void*)i1, (void*)i2, sizeof(COid)); }
    static void delEntry(TxCacheEntry *tce){ delete tce; }
  };

  SkipList<COid,TxCacheEntry*> Cache;

  void ClearCache(void);
  void UpdateCache(COid *coid, Ptr<Valbuf> &buf);

  // Try to read data locally using Cache from transaction.
  // If there is data to be read, data is placed in *buf if *buf!=0;
  //   if *buf=0 then it gets allocated and should be freed later with
  //   readFreeBuf. Note that buffer is allocated only if there is
  //   data to be read.
  //
  // Returns: 0 = nothing read, 
  //          1 = partial data read
  //          2 = all data read
  int tryLocalRead(COid coid, Ptr<Valbuf> &buf);

  // ------------------------------ Prepare RPC -----------------------------------

  struct PrepareCallbackData {
    Semaphore sem; // to wait for response
    int vote;
    Timestamp mincommitts;
    PrepareCallbackData *next, *prev;  // linklist stuff
  };

  void Wsamemcpy(char *dest, LPWSABUF wsabuf, int nbufs);

  // Prepare part of two-phase commit
  // remote2pc = true means that all remote replicas are contacted as
  //             part of two-phase commit. Otherwise, just the local
  //             servers are contacted
  // sets chosents to the timestamp chosen for transaction
  // Sets hascommitted to indicate if the transaction was also committed using one-phase commit.
  // This is possible when the transaction spans only one server.
  int auxprepare(bool remote2pc, Timestamp &chosents, int &hascommitted);
  static void auxpreparecallback(char *data, int len, void *callbackdata);

  // ------------------------------ Commit RPC -----------------------------------


  struct CommitCallbackData {
    Semaphore sem; // to wait for response
    int status;
    Timestamp waitingts;
    CommitCallbackData *prev, *next; // linklist stuff
  };

  static void auxcommitcallback(char *data, int len, void *callbackdata);

  // Commit part of two-phase commit
  // remote2pc = true means that all remote replicas are contacted as
  //             part of two-phase commit. Otherwise, just the local
  //             servers are contacted
  // Returns 0 if ok, -1 if cannot contact some server
  int auxcommit(int outcome, bool remote2pc, Timestamp committs);


public:
  Transaction(StorageConfig *sc);
  ~Transaction();

  // start a new transaction. There is no need to call this for a
  // newly created Transaction object. This is intended for recycling
  // the same object to execute a new transaction.
  // 
  // **!** TO DO: REMOVE The parameter allows the start timestamp to be offset by offsetms.
  // Negative values offset into the past, while positive into the future.
  //int start(int offsetms=0); **!**
  int start();

  // start a transaction with a start timestamp that will be set when the
  // transaction first read, to be the timestamp of the latest available version to read.
  // Right now, these transactions must read something before committing. Later,
  // we should be able to extend this so that transactions can commit without
  // having read.
  int startDeferredTs(void);

  // write an object in the context of a transaction.
  // Returns status:
  //   0=no error
  //   -9=transaction is aborted
  //  -10=cannot contact server
  int write(COid coid, char *buf, int len);
  int writeWsabuf(COid coid, int nbufs, LPWSABUF bufs);

  // returns a status as in write() above
  int put(COid coid, char *buf, int len){ return write(coid, buf, len); }

  // put with 2 or 3 user buffers
  // returns a status as in write() above
  int put2(COid coid, char *data1, int len1, char *data2, int len2);
  int put3(COid coid, char *data1, int len1, char *data2, int len2, char *data3, int len3);

  // read a value into a Valbuf
  int vget(COid coid, Ptr<Valbuf> &buf);

  // read a supervalue into a Valbuf
  int vsuperget(COid coid, Ptr<Valbuf> &buf);

  static void readFreeBuf(char *buf); // frees a buffer returned by readNewBuf() or get()
  static char *allocReadBuf(int len); // allocates a buffer that can be freed by readFreeBuf.
                            // Normally, such a buffer comes from the RPC layer, but
                            // when reading cached data, we need to return a pointer to
                            // such a type of buffer so caller can free it in the same way

  // add an object to a set in the context of a transaction
  // Returns 0 if ok, <0 if error (eg, -9 if transaction is aborted)
  int addset(COid coid, COid toadd);

  // remove an object from a set in the context of a transaction
  // Returns 0 if ok, <0 if error (eg, -9 if transaction is aborted)
  int remset(COid coid, COid toremove);

  // reads a super value. Returns the super value in *svret, as a newly
  // allocated SuperValue object.
  // Returns 0 if ok, non-0 if I/O error. In case of error,
  // *svret is not touched.
  //int readSuperValue(COid coid, SuperValue **svret);

  // writes a super value. Returns 0 if ok, non-0 if I/O error.
  // If super value has non-int cells, must provide svret->pki != 0.
  int writeSuperValue(COid coid, SuperValue *svret);

  // adds a cell to a supervalue.
  int ListAdd(COid coid, ListCell *cell, GKeyInfo *ki);

  // deletes a range of cells
  // intervalType indicates how the interval is to be treated. The possible values are
  //     0=(cell1,cell2)   1=(cell1,cell2]   2=(cell1,inf)
  //     3=[cell1,cell2)   4=[cell1,cell2]   5=[cell1,inf)
  //     6=(-inf,cell2)    7=(-inf,cell2]    8=(-inf,inf)
  // where inf is infinity
  int ListDelRange(COid coid, u8 intervalType, ListCell *cell1, ListCell *cell2, GKeyInfo *ki);
  
  // sets an attribute
  int AttrSet(COid coid, u32 attrid, u64 attrvalue);

  // try to commit
  // remote2pc = true means that all remote replicas are contacted as
  //             part of two-phase commit. Otherwise, just the local
  //             servers are contacted
  // Return 0 if committed, non-0 if aborted
  int tryCommit(bool remote2pc, Timestamp *retcommitts=0);

  int tryCommitSync(void);

  // try to abort
  int abort(void);
};

#endif
