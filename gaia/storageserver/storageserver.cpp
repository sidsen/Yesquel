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

// storageserver.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "../tmalloc.h"
#include "../gaiaoptions.h"
#include "../../options.h"
#include "../debug.h"
#include "../gaiarpcaux.h"

#include "storageserver.h"
#include "storageserverstate.h"

#include "server-splitter.h"

#ifdef STORAGESERVER_SPLITTER
#include "../../splitter-server/storageserver-splitter.h"
#include "../../splitter-server/splitter-client.h"
#include "../clientlib/clientdir.h"
#include "../../kvinterface.h"
#endif

#define WRITEBUF_COPY_THRES 8192 /* threshold below or at which data is copied to private buffer to free the large UDP buffer */

#define MYSITE 0                   // used only if STORAGESERVER_SPLITTER is defined

StorageServerState *S=0;

void InitStorageServer(HostConfig *hc){
  S = new StorageServerState(hc);
#ifdef STORAGESERVER_SPLITTER
  initServerSplitter();
#endif
}

Marshallable *NULLRPC(NullRPCData *d){
  NullRPCRespData *resp;
  assert(S); // if this assert fails, forgot to call InitStorageServer()

  dprintf(2, "Got ping");
  dshowchar('_');

  resp = new NullRPCRespData;
  resp->data = new NullRPCResp;
  resp->freedata = true;
  resp->data->reserved = 0;
  return resp;
}

#define nNoMansLandSize 4
typedef struct _CrtMemBlockHeader
{
  struct _CrtMemBlockHeader * pBlockHeaderNext;
  struct _CrtMemBlockHeader * pBlockHeaderPrev;
  char *                      szFileName;
  int                         nLine;
#ifdef _WIN64
  /* These items are reversed on Win64 to eliminate gaps in the struct
  * and ensure that sizeof(struct)%16 == 0, so 16-byte alignment is
  * maintained in the debug heap.
  */
  int                         nBlockUse;
  size_t                      nDataSize;
#else  /* _WIN64 */
  size_t                      nDataSize;
  int                         nBlockUse;
#endif  /* _WIN64 */
  long                        lRequest;
  unsigned char               gap[nNoMansLandSize];
  /* followed by:
  *  unsigned char           data[nDataSize];
  *  unsigned char           anotherGap[nNoMansLandSize];
  */
} _CrtMemBlockHeader;


static void checknoman(char *buf){
  _CrtMemBlockHeader *pHead = (_CrtMemBlockHeader*) buf - 1;
  assert(*(u32*)(buf+pHead->nDataSize) == 0xfdfdfdfd);
}

Marshallable *WRITERPC(WriteRPCData *d){
  Ptr<PendingTxInfo> pti;
  WriteRPCRespData *resp;
  Ptr<TxInfoCoid> *pticoidptr;
  Ptr<TxInfoCoid> pticoid;
  TxWriteItem *twi;
  COid coid;
  int res;
  char *buf=0;
  int status=0;

  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('w');

  dprintf(1, "WRITE    tid %I64x:%I64x coid %I64x:%I64x len %d", 
    d->data->tid.d1, d->data->tid.d2, d->data->cid, d->data->oid, d->data->len);
  //printf("WRITERPC len %d", d->data->len);
  // write causes the written data to be stored in memory for the transaction

  S->cPendingTx.getInfo(d->data->tid, pti);

  twi = new TxWriteItem;
  buf = (char*) malloc(d->data->len);
  //buf = new char[d->data->len]; // make private copy of data
  memcpy(buf, d->data->buf, d->data->len);
  twi->alloctype=1; // if set, will free twi->buf latter but not twi->rpcrequest

  coid.cid = d->data->cid;
  coid.oid = d->data->oid;

  res = pti->coidinfo.lookupInsert(coid, pticoidptr);
  if (res){ 
    *pticoidptr = new TxInfoCoid;
    pticoid = *pticoidptr;
  } else {
    pticoid = *pticoidptr;
    pticoid->ClearUpdates(); // write overwrites any previous updates done by transaction on coid
  }

  // create and add new item to write set
  twi->coid.cid = d->data->cid;
  twi->coid.oid = d->data->oid;
  twi->len = d->data->len;
  twi->buf = buf;
  twi->rpcrequest = (char*) d;

  pticoid->Writevalue = twi; // save write item

  //pti->unlock();
  resp = new WriteRPCRespData;
  resp->data = new WriteRPCResp;
  resp->data->status = status;
  resp->freedata = true;

  return resp;
}

Marshallable *READRPC(ReadRPCData *d, void *handle, bool &defer){
  ReadRPCRespData *resp;
  int res;
  COid coid;
  Timestamp readts;
  Ptr<TxInfoCoid> ticoid;

  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('r');

  coid.cid=d->data->cid;
  coid.oid=d->data->oid;

  res = S->cLogInMemory.readCOid(coid, d->data->ts, ticoid, &readts, handle);

  if (res == GAIAERR_DEFER_RPC){ // defer the RPC
    defer = true; // defer RPC (go to sleep instead of finishing task)
    return 0;
  }
  if (!res && ticoid->WriteSV) res = GAIAERR_WRONG_TYPE; // wrong type
  resp = new ReadRPCRespData;
  resp->data = new ReadRPCResp;
  if (res<0){
    resp->data->status = res;
    resp->data->buf = 0;
    resp->data->len = 0;
    resp->freedata = false;
  }
  else { // no error
    // create and add new item to read set
    //Ptr<PendingTxInfo> pti;
    //S->cPendingTx.getInfo(d->data->tid, pti);
    //tri = new TxReadItem;
    //tri->coid.cid = d->data->cid;
    //tri->coid.oid = d->data->oid;

    //pti->unlock();
    TxWriteItem *twi=ticoid->Writevalue;
    assert(twi);
    char *buf;
    int len = twi->len;

    buf = twi->buf;
    resp->freedatabuf = 0; // nothing to delete later

    resp->data->status = 0;
    resp->data->readts = readts;
    resp->data->buf = buf;
    resp->data->len = len;
    resp->freedata = true;
    resp->ticoid = ticoid;
  }
  dprintf(1, "READ     tid %I64x:%I64x coid %I64x:%I64x ts %016I64x:%016I64x len %d [len %d status %d readts %016I64x:%016I64x]", 
    d->data->tid.d1, d->data->tid.d2, d->data->cid, d->data->oid, d->data->ts.getd1(), d->data->ts.getd2(), d->data->len, resp->data->len, resp->data->status,
    resp->data->readts.getd1(), resp->data->readts.getd2());

  defer = false;
  return resp;
}

Marshallable *FULLWRITERPC(FullWriteRPCData *d){
  Ptr<PendingTxInfo> pti;
  FullWriteRPCRespData *resp;
  Ptr<TxInfoCoid> *pticoidptr;
  Ptr<TxInfoCoid> pticoid;
  TxWriteSVItem *twsvi;
  COid coid;
  int res;
  FullWriteRPCParm *buf=0;
  int status=0;

  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('W');
  dprintf(1, "WRITESV  tid %I64x:%I64x coid %I64x:%I64x nattrs %d ncells %d lencells %d", 
    d->data->tid.d1, d->data->tid.d2, d->data->cid, d->data->oid, d->data->nattrs, d->data->ncelloids, d->data->lencelloids);
  //printf("WRITERPC len %d", d->data->len);
  // write causes the written data to be stored in memory for the transaction

  S->cPendingTx.getInfo(d->data->tid, pti);

  twsvi = FullWriteRPCParmToTxWriteSVItem(d->data);

  coid.cid = d->data->cid;
  coid.oid = d->data->oid;

  res = pti->coidinfo.lookupInsert(coid, pticoidptr);
  if (res){
    *pticoidptr = new TxInfoCoid;
    pticoid = *pticoidptr;
  } else {
    pticoid = *pticoidptr;
    pticoid->ClearUpdates(); // write overwrites any previous updates done by transaction on coid
  }

  // create and add new item to write set
  pticoid->WriteSV = twsvi; // save write item

  //pti->unlock();
  resp = new FullWriteRPCRespData;
  resp->data = new FullWriteRPCResp;
  resp->data->status = status;
  resp->freedata = true;

  return resp;
}

Marshallable *FULLREADRPC(FullReadRPCData *d, void *handle, bool &defer){
  FullReadRPCRespData *resp;
  int res;
  COid coid;
  Timestamp readts;
  Ptr<TxInfoCoid> ticoid;

  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('R');
  coid.cid=d->data->cid;
  coid.oid=d->data->oid;

  res = S->cLogInMemory.readCOid(coid, d->data->ts, ticoid, &readts, handle);

  if (res == GAIAERR_DEFER_RPC){ // defer the RPC
    defer = true; // special value to mark RPC as deferred
    return 0;
  }
  if (!res && ticoid->Writevalue) res = GAIAERR_WRONG_TYPE; // wrong type
  resp = new FullReadRPCRespData;
  resp->data = new FullReadRPCResp;
  if (res<0){
    resp->data->status = res;
    resp->data->readts.setIllegal();
    resp->data->nattrs = 0;
    resp->data->celltype = 0;
    resp->data->ncelloids = 0;
    resp->data->lencelloids = 0;
    resp->data->attrs = 0;
    resp->data->celloids = 0;
    resp->data->pki = 0;
    resp->freedata = 1;
    resp->freedatapki = 0;
    resp->deletecelloids = 0;
    resp->twsvi = 0;
  }
  else { // no error
    // create and add new item to read set
    //Ptr<PendingTxInfo> pti;
    //S->cPendingTx.getInfo(d->data->tid, pti);
    //tri = new TxReadItem;
    //tri->coid.cid = d->data->cid;
    //tri->coid.oid = d->data->oid;

    //pti->unlock();
    TxWriteSVItem *twsvi = ticoid->WriteSV;
    assert(twsvi);

    int ncelloids, lencelloids;
    char *buf = twsvi->GetCelloids(ncelloids, lencelloids);

    resp->data->status = 0;
    resp->data->readts = readts;
    resp->data->nattrs = twsvi->nattrs;
    resp->data->celltype = twsvi->celltype;
    resp->data->ncelloids = twsvi->cells.nitems;
    resp->data->lencelloids = lencelloids;
    resp->data->attrs = twsvi->attrs;
    resp->data->celloids = buf;
    resp->data->pki = CloneGKeyInfo(twsvi->pki);

    resp->freedata = 1;
    resp->deletecelloids = 0; // do not free celloids since it belows to twsvi
    resp->freedatapki = resp->data->pki;
    resp->twsvi = 0;
    resp->ticoid = ticoid;
  }

  dprintf(1, "READSV   tid %I64x:%I64x coid %I64x:%I64x ts %016I64x:%016I64x [celltype %d ncells %d status %d readts %016I64x:%016I64x]", 
    d->data->tid.d1, d->data->tid.d2, d->data->cid, d->data->oid, d->data->ts.getd1(), d->data->ts.getd2(), 
    resp->data->celltype, resp->data->ncelloids, resp->data->status, resp->data->readts.getd1(), resp->data->readts.getd2());
  defer = false;
  return resp;
}

Marshallable *LISTADDRPC(ListAddRPCData *d){
  Ptr<PendingTxInfo> pti;
  ListAddRPCRespData *resp;
  Ptr<TxInfoCoid> *pticoidptr;
  Ptr<TxInfoCoid> pticoid;
  COid coid;
  int res;
  int status=0;

  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('+');
  dprintf(1, "LISTADD  tid %I64x:%I64x coid %I64x:%I64x nkey %I64d", 
    d->data->tid.d1, d->data->tid.d2, d->data->cid, d->data->oid, d->data->cell.nKey);

  coid.cid = d->data->cid;
  coid.oid = d->data->oid;

  S->cPendingTx.getInfo(d->data->tid, pti);

  res = pti->coidinfo.lookupInsert(coid, pticoidptr);
  if (res){ *pticoidptr = new TxInfoCoid; }
  pticoid = *pticoidptr;

  if (pticoid->WriteSV){ // if transaction already has a supervalue, change it
    TxWriteSVItem *twsvi = pticoid->WriteSV;
    if (d->data->pKeyInfo) twsvi->SetPkiSticky(d->data->pKeyInfo); // provide pki to TxWriteSVItem if it doesn't have it already
    twsvi->cells.insertOrReplace(new ListCellPlus(d->data->cell, &twsvi->pki), 0, ListCellPlus::del, 0); // copy item since twsvi->cells will own it
  }
  else { // otherwise add a listadd item to transaction
    assert(!pticoid->Writevalue);
    TxListAddItem *tlai = new TxListAddItem(coid, d->data->pKeyInfo, d->data->cell);
    pticoid->Litems.push_back(tlai);
  }

  //pti->unlock();
  resp = new ListAddRPCRespData;
  resp->data = new ListAddRPCResp;
  resp->data->status = status;
  resp->freedata = true;

  return resp;
}

Marshallable *LISTDELRANGERPC(ListDelRangeRPCData *d){
  Ptr<PendingTxInfo> pti;
  ListDelRangeRPCRespData *resp;
  Ptr<TxInfoCoid> *pticoidptr;
  Ptr<TxInfoCoid> pticoid;
  COid coid;
  int res;
  int status=0;

  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('-');
  dprintf(1, "LISTDELR tid %I64x:%I64x coid %I64x:%I64x nkey1 %I64d nkey2 %I64d type %d", 
    d->data->tid.d1, d->data->tid.d2, d->data->cid, d->data->oid, d->data->cell1.nKey, d->data->cell2.nKey, (int)d->data->intervalType);

  coid.cid = d->data->cid;
  coid.oid = d->data->oid;

  S->cPendingTx.getInfo(d->data->tid, pti);

  res = pti->coidinfo.lookupInsert(coid, pticoidptr);
  if (res){ *pticoidptr = new TxInfoCoid; }
  pticoid = *pticoidptr;

  if (pticoid->WriteSV){ // if transaction already has a supervalue, change it
    TxWriteSVItem *twsvi = pticoid->WriteSV;
    int type1, type2;
    if (d->data->pKeyInfo) twsvi->SetPkiSticky(d->data->pKeyInfo); // provide pki to TxWriteSVItem if it doesn't have it already
    ListCellPlus rangestart(d->data->cell1, &twsvi->pki);
    ListCellPlus rangeend(d->data->cell2, &twsvi->pki);
    TxWriteSVItem::convertOneIntervalTypeToTwoIntervalType(d->data->intervalType, type1, type2);
    twsvi->cells.delRange(&rangestart, type1, &rangeend, type2, ListCellPlus::del, 0);
  } else { // otherwise add a delrange item to transaction
    TxListDelRangeItem *tldri = new TxListDelRangeItem(coid, d->data->pKeyInfo, d->data->intervalType, d->data->cell1, d->data->cell2);
    pticoid->Litems.push_back(tldri);
  }

  //pti->unlock();
  resp = new ListDelRangeRPCRespData;
  resp->data = new ListDelRangeRPCResp;
  resp->data->status = status;
  resp->freedata = true;

  return resp;
}

Marshallable *ATTRSETRPC(AttrSetRPCData *d){
  Ptr<PendingTxInfo> pti;
  AttrSetRPCRespData *resp;
  Ptr<TxInfoCoid> *pticoidptr;
  Ptr<TxInfoCoid> pticoid;
  COid coid;
  int res;
  int status=0;

  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('a');
  dprintf(1, "ATTRSET  tid %I64x:%I64x coid %I64x:%I64x attrid %x attrvalue %I64x", 
    d->data->tid.d1, d->data->tid.d2, d->data->cid, d->data->oid, d->data->attrid, d->data->attrvalue);

  coid.cid = d->data->cid;
  coid.oid = d->data->oid;

  S->cPendingTx.getInfo(d->data->tid, pti);

  res = pti->coidinfo.lookupInsert(coid, pticoidptr);
  if (res){ *pticoidptr = new TxInfoCoid; }
  pticoid = *pticoidptr;
  assert(d->data->attrid < GAIA_MAX_ATTRS);
  if (pticoid->WriteSV) // if transaction already has a supervalue, change it
    pticoid->WriteSV->attrs[d->data->attrid] = d->data->attrvalue;
  else {
    pticoid->SetAttrs[d->data->attrid]=1; // mark attribute as set
    pticoid->Attrs[d->data->attrid] = d->data->attrvalue; // record attribute value
  }

  //pti->unlock();
  resp = new AttrSetRPCRespData;
  resp->data = new AttrSetRPCResp;
  resp->data->status = status;
  resp->freedata = true;

  return resp;
}

int doCommitWork(CommitRPCParm *parm, Ptr<PendingTxInfo> pti, Timestamp &waitingts); // forward definition

struct PREPARERPCState {
  Timestamp proposecommitts;
  int status;
  int vote;
  Ptr<PendingTxInfo> pti;
  PREPARERPCState(Timestamp &pts, int s, int v, Ptr<PendingTxInfo> pt) : proposecommitts(pts), status(s), vote(v) { pti = pt; }
};

// PREPARERPC is called with state=0 for the first time.
// If it returns 0, it has issued a request to log to disk, and it wants to be
//   called again with state=1. It will then return the non-null result.
// If it returns non-null, it is done.
// rpctasknotify is a parameter that is passed on to the disk logging function
//  so that it knows what to notify when the logging is finished. In the local version
//  this parameter is 0.

Marshallable *PREPARERPC(PrepareRPCData *d, void *&state, void *rpctasknotify){
  PrepareRPCRespData *resp;
  Ptr<PendingTxInfo> pti;
  LogOneObjectInMemory *looim;
  int vote;
  Timestamp startts, proposecommitts;
  Timestamp dummywaitingts;
  SkipListNode<COid,Ptr<TxInfoCoid> > *ptr;
  LinkList<LogOneObjectInMemory> looim_list;
  int status=0;
  int res;
  int waitforlog;
  PREPARERPCState *pstate = (PREPARERPCState*) state;
  int immediatetransition=0;

  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('p');
  //committs = d->data->committs;
  proposecommitts = d->data->startts; // begin with the startts, the minimum possible proposed commit ts
  startts = d->data->startts;

  if (pstate == 0){
#ifdef GAIA_OCC
    S->cPendingTx.getInfo(d->data->tid, pti);
    res = 0;
#else
    res = S->cPendingTx.getInfoNoCreate(d->data->tid, pti);
#endif
    if (res){ // tid not found
      CRASH;
      vote=1; goto end;
    } 
    if (pti->status == PTISTATUS_VOTEDYES){ vote=0; goto end; }
    if (pti->status == PTISTATUS_VOTEDNO){ vote=1; goto end; }
    if (pti->status == PTISTATUS_CLEAREDABORT){ CRASH; vote=1; goto end; }

    vote = 0;  // vote yes, unless we decide otherwise below

#ifdef GAIA_OCC
    // check readset to see if it has changed
    // This OCC implementation is simplistic, because it does not do two things
    // that it should be done:
    //   1. One needs to lock the entire readset and writeset before checking, rather
    //      than locking, checking, and unlocking one at a time. And to avoid deadlocks,
    //      one needs to lock in coid order.
    //   2. One needs to keep the readset in the pendingupdates until commit time
    // Without these two things, the implementation will be faster but incorrect.
    // This is ok for our purposes since we want to get an upper bound on performance
    // Another thing which can be improved: the client currently sends the entire
    // readset to all participants, but it suffices to send only those objects
    // that each participant is responsible for. In the loop below, we
    // check all other objects (those that the participant is not responsible
    // for will not be found), which could be optimized if the client had not
    // sent everything.
    int pos;
    COid coid;
    //myprintf("Checking readset of len %d\n", d->data->readset_len);

    for (pos = 0; pos < d->data->readset_len; ++pos){
      coid = d->data->readset[pos];
      //myprintf("  Element %d (%016I64x:%016I64x): ", pos, coid.cid, coid.oid);
      res = S->cLogInMemory.coidInLog(coid);
      if (res){ // found coid, this is one that we are responsible for
        looim = S->cLogInMemory.getAndLock(coid, true, false); assert(looim);
        // check for pending updates
        if (!looim->pendingentries.empty()){ 
          //myprintf("some pending update (abort)\n");
          vote = 1; 
        }
        else {
          // check for updates in logentries; suffices to look at last entry in log
          SingleLogEntryInMemory *sleim = looim->logentries.rGetFirst();
          if (sleim != looim->logentries.rGetLast()){
            if (Timestamp::cmp(sleim->ts, startts) >= 0){
              //myprintf("conflict (abort)\n");
              vote = 1; // something changed
            } else {
            //myprintf("no conflict (ok)\n");
            }
          } else {
            //myprintf("log exists but empty (ok)\n");
          }
        }
        looim->unlock();
      } else {
        //myprintf("not in log (ok)\n");
      }
      if (vote) goto done_checking_votes; // already found conflict, no need to keep checking for more
    }
#endif

    // for each oid in transaction's write set, in order
    for (ptr = pti->coidinfo.getFirst(); ptr != pti->coidinfo.getLast(); ptr = pti->coidinfo.getNext(ptr)){
      // get the looims for a given coid and acquire write lock on that object
      // (note we are getting locks in oid order in this loop, which avoids deadlocks)
      looim = S->cLogInMemory.getAndLock(ptr->key, true, false);
      looim_list.pushTail(looim); // looims that we locked

      // check last-read timestamp
      if (Timestamp::cmp(proposecommitts, looim->LastRead) < 0) 
        proposecommitts = looim->LastRead; // track largest read timestamp seen

      // check for conflicts with other transactions in log
      LinkList<SingleLogEntryInMemory> *entries = &looim->logentries; 
      if (!entries->empty()){
        SingleLogEntryInMemory *sleim;
        // for each update in the looim's log
        for (sleim = entries->rGetFirst(); sleim != entries->rGetLast(); sleim = entries->rGetNext(sleim)){
          if (sleim->flags & SLEIM_FLAG_SNAPSHOT) continue;  // ignore this entry since it was artificially inserted for efficiency
          if (Timestamp::cmp(sleim->ts, startts) <= 0) break; // all done
          // startts < sleim->ts so we need to check for conflicts with this entry
          if (sleim->ticoid->hasConflicts(ptr->value, sleim)){ // conflict, must abort
            vote = 1;
            break;
          }
        }
      }

      // now check for conflicts with pending transactions
      entries = &looim->pendingentries; 
      if (!entries->empty()){
        SingleLogEntryInMemory *sleim;
        // for each update in the looim's log
        for (sleim = entries->getFirst(); sleim != entries->getLast(); sleim = entries->getNext(sleim)){
          if (sleim->ticoid->hasConflicts(ptr->value, sleim)){ // conflict, must abort
            vote = 1;
            break;
          }
        }
      }
    }

#ifdef GAIA_OCC
    done_checking_votes:
#endif

    if (vote){ // if aborting, then release locks immediately
      pti->status = PTISTATUS_VOTEDNO;
      for (looim = looim_list.getFirst(); looim != looim_list.getLast(); looim = looim_list.getNext(looim)){
        looim->unlock();
      }
      waitforlog = 0;
    }
    else { // vote is to commit
      pti->status = PTISTATUS_VOTEDYES;
      // (4) add entry to in-memory pendingentries
      ptr = pti->coidinfo.getFirst();
      looim = looim_list.getFirst();
      // iterate over coidinfo and looim_list in sync.
      // Note that items were added to looim_list in COid order
      while (ptr != pti->coidinfo.getLast()){
        SingleLogEntryInMemory *sleim = S->cLogInMemory.auxAddSleimToPendingentries(looim, proposecommitts, true, ptr->value);
        assert(!ptr->value->pendingentriesSleim); // if ptr->value->pendingentriesSleim is set, then a tx is adding
                                                  // multiple sleims for one object. This should not be the case since all
                                                  // updates to an object are accumulated in a single ticoid, which is then
                                                  // added as a single entry. If the tx adds multiple sleims for an object,
                                                  // the logic in LogInMemory::removeOrMovePendingToLogentries needs to be revised
                                                  // to move all of those entries (currently, it only moves one)
        ptr->value->pendingentriesSleim = sleim; // remember sleim so that we can quickly find it at commit time
        looim->unlock(); // ok to release lock even before we log, since
                                     // the transaction is still marked as pending and
                                     // we have not yet returned the vote (and the
                                     // transaction will not commit before we return the vote, meaning that
                                     // others will not be able to read it before we return the vote)
        ptr = pti->coidinfo.getNext(ptr);
        looim = looim_list.getNext(looim);
      }

      // log the writes and vote
      // Note that we are writing the proposecommitts not the real committs, which is determined
      // only later (as the max of all the proposecommitts)
      waitforlog = S->cDiskLog.logUpdatesAndYesVote(d->data->tid, proposecommitts, pti, rpctasknotify);
      if (!rpctasknotify) assert(waitforlog == 0);
    }

    if (waitforlog){
      assert(rpctasknotify);
      // save variables, and restore then later below after we return with state != 0
      assert(pstate == 0);
      pstate = new PREPARERPCState(proposecommitts, status, vote, pti);
      state = (void*) pstate;
      return 0;
    }
    else {
      immediatetransition = 1;
    }
  }

  if (immediatetransition || pstate){ // we transitioned to the new state
    if (pstate){ // restore local variables
      proposecommitts = pstate->proposecommitts;
      status = pstate->status;
      vote = pstate->vote;
      pti = pstate->pti;
      delete pstate;
      state = 0;
    }

    // note: variables used below should be restored from pstate above
    if (d->data->onephasecommit){
      CommitRPCParm parm;
      parm.commit = vote;
      parm.committs = proposecommitts;
      parm.committs.addEpsilon();
      parm.tid = d->data->tid;
      status = doCommitWork(&parm, pti, dummywaitingts); // we use dummywaitingts (and discard this value) since waitingts is
                                                         // not useful in 1PC where there is no delay between prepare and commit
    }
  }

  end:
  //pti->unlock();

  // note: variables used below should be restored from pstate above
  resp = new PrepareRPCRespData;
  resp->data = new PrepareRPCResp;
  resp->data->vote = vote;
  resp->data->status = status;
  resp->data->mincommitts = proposecommitts;
  resp->freedata = true;

  dprintf(1, "PREPARE  tid %I64x:%I64x startts %016I64x:%016I64x mincommitts %016I64x:%016I64x vote %s onephase %d", d->data->tid.d1, d->data->tid.d2, d->data->startts.getd1(), d->data->startts.getd2(), proposecommitts.getd1(), proposecommitts.getd2(), vote ? "no" : "yes", d->data->onephasecommit);

  return resp;
}

// does the actual work in COMMITRPC
// Assumes lock is held in pti.
int doCommitWork(CommitRPCParm *parm, Ptr<PendingTxInfo> pti, Timestamp &waitingts){
  SkipListNode<COid,Ptr<TxInfoCoid> > *ptr;
  SingleLogEntryInMemory *pendingsleim;
  int status=0;
  waitingts.setIllegal();

  if (parm->commit == 0){ // commit
    for (ptr = pti->coidinfo.getFirst(); ptr != pti->coidinfo.getLast(); ptr = pti->coidinfo.getNext(ptr)){
      pendingsleim = ptr->value->pendingentriesSleim;
      if (pendingsleim){
        if (Timestamp::cmp(waitingts, pendingsleim->waitingts) < 0) waitingts = pendingsleim->waitingts;
        S->cLogInMemory.removeOrMovePendingToLogentries(ptr->key, pendingsleim, parm->committs, true);
        ptr->value->pendingentriesSleim = 0;
      }
    }
    S->cDiskLog.logCommitAsync(parm->tid, parm->committs);
  } else {
    // note: abort due to application (parm->commit == 2) does not
    // require removing writes from cLogInMemory or logging an abort,
    // since the prepare phase was never done
    if (parm->commit == 1 || parm->commit == 3){
      for (ptr = pti->coidinfo.getFirst(); ptr != pti->coidinfo.getLast(); ptr = pti->coidinfo.getNext(ptr)){
        pendingsleim = ptr->value->pendingentriesSleim;
        if (pendingsleim){
          if (Timestamp::cmp(waitingts, pendingsleim->waitingts) < 0) waitingts = pendingsleim->waitingts;
          S->cLogInMemory.removeOrMovePendingToLogentries(ptr->key, pendingsleim, parm->committs, false);
        ptr->value->pendingentriesSleim = 0;
        }
      }
      S->cDiskLog.logAbortAsync(parm->tid, parm->committs);
    }
    pti->clear();   // delete update items in transaction
    // question: will this free the ticoids within the pti? I believe so, because
    // upon deleting the skiplist nodes, the nodes' destructor will call the destructor
    // of Ptr<TxInfoCoid>. This needs to be checked, otherwise a memory leak will occur.
    // Also check that removing the pti later (when tx commits) will not deallocate
    // the ticoid since there is a sleim that smart-points to the ticoid in the object's logentries.
  }

  // remove information about tid, freeing up memory
  S->cPendingTx.removeInfo(parm->tid);
  return status;
}

Marshallable *COMMITRPC(CommitRPCData *d){
  CommitRPCRespData *resp;
  Ptr<PendingTxInfo> pti;

  int res=0;

  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('c');
  dprintf(1, "COMMIT   tid %I64x:%I64x ts %016I64x:%016I64x commit %d", d->data->tid.d1, d->data->tid.d2, 
    d->data->committs.getd1(), d->data->committs.getd2(), d->data->commit);

  resp = new CommitRPCRespData;
  resp->data = new CommitRPCResp;

  res = S->cPendingTx.getInfoNoCreate(d->data->tid, pti);
  if (res) 
    CRASH; // did not find tid
  else {
    if (pti->status == PTISTATUS_CLEAREDABORT) 
      CRASH;
    else res = doCommitWork(d->data, pti, resp->data->waitingts);
    //pti->unlock();
  }

  resp->data->status = res;
  resp->freedata = true;

  return resp;
}

Marshallable *SHUTDOWNRPC(ShutdownRPCData *d){
  ShutdownRPCRespData *resp;

  // schedule exit to occur 2 seconds from now, to give time to answer request
  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('s');
  printf("Shutting down server in 2 seconds...");
  ScheduleExit(); // schedule calling exint after 2 seconds

  resp = new ShutdownRPCRespData;
  resp->data = new ShutdownRPCResp;
  resp->freedata = true;
  resp->data->reserved = 0;
  return resp;
}

Marshallable *STARTSPLITTERRPC(StartSplitterRPCData *d){
  StartSplitterRPCRespData *resp;

  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('S');

#if defined(STORAGESERVER_SPLITTER)
  extern StorageConfig *SC;
  extern char *Configfile;
  dprintf(1, "Starting splitter based on config %s", Configfile);
  if (!SC){
    SC = new StorageConfig(Configfile, MYSITE);
    dprintf(1, "Splitter started");
#ifdef MULTI_SPLITTER
    SD = new SplitterDirectory(SC->CS, 0);
    KVInterfaceInit();  // **!** TODO: probably should always call this even if MULTI_SPLITTER is not defined,
                        // but the question is whether it will work if SD has not been initialized
#endif
  }
  else { dprintf(1, "Splitter already running"); }
#endif

  resp = new StartSplitterRPCRespData;
  resp->data = new StartSplitterRPCResp;
  resp->freedata = true;
  resp->data->reserved = 0;
  return resp;
}

Marshallable *FLUSHFILERPC(FlushFileRPCData *d){
  FlushFileRPCRespData *resp;
  char *filename;
  Timestamp ts;
  int res;
  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('f');

  ts.setNew();
  Sleep(1000); // wait for a second

  if (!d->data->filename || strlen(d->data->filename)==0) filename = FLUSH_FILENAME; // no filename provided
  else filename = d->data->filename;
  dprintf(1, "Flushing to file %s", filename);
  res = S->cLogInMemory.flushToFile(ts, filename);
  dprintf(1, "Flushing done, result %d", res);

  resp = new FlushFileRPCRespData;
  resp->data = new FlushFileRPCResp;
  resp->freedata = true;
  resp->data->status = res;
  resp->data->reserved = 0;
  return resp;
}

Marshallable *LOADFILERPC(LoadFileRPCData *d){
  LoadFileRPCRespData *resp;
  char *filename;
  int res;
  assert(S); // if this assert fails, forgot to call InitStorageServer()
  dshowchar('l');

  if (!d->data->filename || strlen(d->data->filename)==0) filename = FLUSH_FILENAME; // no filename provided
  else filename = d->data->filename;
  dprintf(1, "Loading from file %s", filename);
  res = S->cLogInMemory.loadFromFile(filename);
  dprintf(1, "Loading done, result %d", res);

  resp = new LoadFileRPCRespData;
  resp->data = new LoadFileRPCResp;
  resp->freedata = true;
  resp->data->status = res;
  resp->data->reserved = 0;
  return resp;
}
