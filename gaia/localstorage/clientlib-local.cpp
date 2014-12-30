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
#include "../debug.h"
#include "../util.h"

#include "../gaiarpcaux.h"
#include "../newconfig.h"
#include "../storageserver/storageserver.h"
#include "../storageserver/storageserverstate.h"
#include "../clientlib/supervalue.h"
#include "clientlib-local.h"
#include "../record.h"
#include "../clientlib/clientlib.h"
#include "../task.h"

LocalTransaction::LocalTransaction() :
  Cache()
{
  readsCached = 0;
  start();
}

LocalTransaction::~LocalTransaction(){
  ClearCache();
}

void LocalTransaction::ClearCache(void){
  Cache.clear(0, TxCacheEntry::delEntry);
}

// start a new transaction
int LocalTransaction::start(){
  // obtain a new timestamp from local clock (assumes synchronized clocks)
  StartTs.setNew();
  Id.setNew();
  ClearCache();
  State = 0;  // valid
  hasWrites = false;
  return 0;
}

// start a new transaction
int LocalTransaction::startDeferredTs(){ return start(); }

void LocalTransaction::Wsamemcpy(char *dest, LPWSABUF wsabuf, int nbufs){
  for (int i=0; i < nbufs; ++i){
    memcpy((void*) dest, (void*) wsabuf[i].buf, wsabuf[i].len);
    dest += wsabuf[i].len;
  }
}

// write an object in the context of a transaction
int LocalTransaction::writeWsabuf(COid coid, int nbufs, LPWSABUF bufs){
  WriteRPCData *rpcdata;
  WriteRPCRespData *rpcresp;
  int respstatus;
  int totlen;
  char *buf;

  if (State != 0) return GAIAERR_TX_ENDED;

  hasWrites = true;

  rpcdata = new WriteRPCData;
  rpcdata->data = new WriteRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->nbufs = 0;
  rpcdata->wsabuf = 0;
  totlen=0;
  for (int i=0; i < nbufs; ++i) totlen += bufs[i].len;
  rpcdata->data->len = totlen;  // total length
  buf = LocalTransaction::allocReadBuf(totlen); // we use allocReadBuf because we will place
                       // this buffer in the cache below, inside a Valbuf (which is
                       // destroyed by calling LocalTransaction::freeReadBuf())
  rpcdata->data->buf = buf;
  rpcdata->freedatabuf = 0; 
  Wsamemcpy(buf, bufs, nbufs);

  rpcresp = (WriteRPCRespData*) WRITERPC(rpcdata);
  //delete rpcdata;

  if (!rpcresp){ State=-2; return -10; } // error contacting server

  // record written data
  // create a private copy of the data
  Valbuf *vb = new Valbuf;
  vb->type = 0; // regular value
  vb->coid = coid;
  vb->immutable = false;
  vb->commitTs.setIllegal();
  vb->readTs.setIllegal();
  vb->len = totlen;
  vb->u.buf = buf;
  //vb->u.buf = LocalTransaction::allocReadBuf(totlen);
  //memcpy(vb->u.buf, buf, totlen);
  //Wsamemcpy(vb->u.buf, bufs, nbufs); 
  delete rpcdata; // will not delete buf

  Ptr<Valbuf> vbuf = vb;
  UpdateCache(&coid, vbuf);

  respstatus = rpcresp->data->status;

  if (respstatus) State=-2; // mark transaction as aborted due to I/O error
  delete rpcresp;

  return respstatus;
}

// write an object in the context of a transaction
int LocalTransaction::write(COid coid, char *buf, int len){
  WSABUF wsabuf;
  wsabuf.len = len;
  wsabuf.buf = buf;
  return writeWsabuf(coid, 1, &wsabuf);
}

int LocalTransaction::put2(COid coid, char *data1, int len1, char *data2, int len2){
  WSABUF wsabuf[2];
  wsabuf[0].len = len1;
  wsabuf[0].buf = data1;
  wsabuf[1].len = len2;
  wsabuf[1].buf = data2;
  return writeWsabuf(coid, 2, wsabuf);
}

int LocalTransaction::put3(COid coid, char *data1, int len1, char *data2, int len2, char *data3, int len3){
  WSABUF wsabuf[3];
  wsabuf[0].len = len1;
  wsabuf[0].buf = data1;
  wsabuf[1].len = len2;
  wsabuf[1].buf = data2;
  wsabuf[2].len = len3;
  wsabuf[2].buf = data3;
  return writeWsabuf(coid, 3, wsabuf);
}

// Try to read data locally using Cache from transaction.
// Returns: 0 = nothing read, 
//          1 = partial data read
//          2 = all data read
//          GAIAERR_TX_ENDED = cannot read because transaction is aborted
//
int LocalTransaction::tryLocalRead(COid coid, Ptr<Valbuf> &buf){
  TxCacheEntry **cepp;
  int res;

  if (State) return GAIAERR_TX_ENDED;

  res = Cache.lookup(coid, cepp);
  if (!res){ // found
    buf = (*cepp)->vbuf;
    return 1;
  }
  else return 0;
}

void LocalTransaction::UpdateCache(COid *coid, Ptr<Valbuf> &buf){
  TxCacheEntry **cepp, *cep;
  int res;
  // remove any previous entry for coid
  res = Cache.lookupInsert(*coid, cepp);
  if (!res){ // element already exists; remove old entry
    assert(*cepp);
    delete *cepp;
  }
  cep = *cepp = new TxCacheEntry;
  cep->coid = *coid;
  cep->vbuf = buf;
}

int LocalTransaction::vget(COid coid, Ptr<Valbuf> &buf){
  int reslocalread;
  ReadRPCData *rpcdata;
  ReadRPCRespData *rpcresp;
  int respstatus;
  bool defer;

  if (State != 0){ buf = 0; return GAIAERR_TX_ENDED; }

  reslocalread = tryLocalRead(coid, buf);
  if (reslocalread == 1 && buf->type == 0) return 0; // read completed already
  if (reslocalread < 0) return reslocalread;

  // add server index to set of servers participating in transaction
  // Not for read items
  // Servers.insert(server); 

  rpcdata = new ReadRPCData;
  rpcdata->data = new ReadRPCParm;
  rpcdata->freedata = true; 

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->ts = StartTs;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->len = -1;  // requested max bytes to read

  rpcresp = (ReadRPCRespData*) READRPC(rpcdata, 0, defer); assert(!defer);
  delete rpcdata;

  if (!rpcresp){ // error contacting server
    State=-2; // mark transaction as aborted due to I/O error
    buf = 0;
    return -10;
  }

  respstatus = rpcresp->data->status;
  if (respstatus){ buf = 0; goto end; }

  // fill out buf (returned value to user) with reply from RPC
  Valbuf *vbuf = new Valbuf;
  vbuf->type = 0;
  vbuf->coid = coid;
  vbuf->immutable = true;
  vbuf->commitTs = rpcresp->data->readts;
  vbuf->readTs = StartTs;
  vbuf->len = rpcresp->data->len;
  vbuf->u.buf = LocalTransaction::allocReadBuf(rpcresp->data->len);
  memcpy(vbuf->u.buf, rpcresp->data->buf, rpcresp->data->len);
  buf = vbuf;

  end:
  delete rpcresp;
  return respstatus;
}

int LocalTransaction::vsuperget(COid coid, Ptr<Valbuf> &buf){
  int reslocalread;
  FullReadRPCData *rpcdata;
  FullReadRPCRespData *rpcresp;
  int respstatus;
  bool defer;

  if (State != 0){ buf = 0; return GAIAERR_TX_ENDED; }

  reslocalread = tryLocalRead(coid, buf);
  if (reslocalread == 1 && buf->type==1) return 0; // read completed already
  if (reslocalread < 0) return reslocalread;

  // add server index to set of servers participating in transaction
  // Not for read items
  // Servers.insert(server); 

  rpcdata = new FullReadRPCData;
  rpcdata->data = new FullReadRPCParm;
  rpcdata->freedata = true; 

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->ts = StartTs;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;

  rpcresp = (FullReadRPCRespData *) FULLREADRPC(rpcdata, 0, defer); assert(!defer);
  delete rpcdata;

  if (!rpcresp){ // error contacting server
    State=-2; // mark transaction as aborted due to I/O error
    buf = 0;
    return -10;
  }

  respstatus = rpcresp->data->status;
  if (respstatus){ buf = 0; goto end; }

  FullReadRPCResp *r = rpcresp->data; // for convenience

  Valbuf *vbuf = new Valbuf;
  vbuf->type = 1;
  vbuf->coid = coid;
  vbuf->immutable = true;
  vbuf->commitTs = r->readts;
  vbuf->readTs = StartTs;
  vbuf->len = 0; // not applicable for supervalue
  SuperValue *sv = new SuperValue;
  vbuf->u.raw = sv;

  sv->Nattrs = r->nattrs;
  sv->CellType = r->celltype;
  sv->Ncells = r->ncelloids;
  sv->CellsSize = r->lencelloids;
  sv->Attrs = new u64[sv->Nattrs]; assert(sv->Attrs);
  memcpy(sv->Attrs, r->attrs, sizeof(u64) * sv->Nattrs);
  sv->Cells = new ListCell[sv->Ncells];
  // fill out cells
  char *ptr = r->celloids;
  for (int i=0; i < sv->Ncells; ++i){
    // extract nkey
    u64 nkey;
    ptr += myGetVarint((unsigned char*) ptr, &nkey);
    sv->Cells[i].nKey = nkey;
    if (r->celltype == 0) sv->Cells[i].pKey = 0; // integer cell, set pKey=0
    else { // non-integer key, so extract pKey (nkey has its length)
      sv->Cells[i].pKey = new char[(int)nkey];
      memcpy(sv->Cells[i].pKey, ptr, (int)nkey);
      ptr += (int)nkey;
    }
    // extract childOid
    sv->Cells[i].value = *(Oid*)ptr;
    ptr += sizeof(u64); // space for 64-bit value in cell
  }
  sv->pki = CloneGKeyInfo(r->pki);
  buf = vbuf;
  if (readsCached < MAX_READS_TO_CACHE){
    ++readsCached;
    UpdateCache(&coid, buf);
  }
 end:
  delete rpcresp;
  return respstatus;
}

// free a buffer returned by Transaction::read
void LocalTransaction::readFreeBuf(char *buf){
  assert(buf);
  //delete [] buf;
  return Transaction::readFreeBuf(buf);
}

char *LocalTransaction::allocReadBuf(int len){
  //return new char[len];
  return Transaction::allocReadBuf(len);
}


// add an object to a set in the context of a transaction
int LocalTransaction::addset(COid coid, COid toadd){
  if (State != 0) return GAIAERR_TX_ENDED;
  return -1; // not implemented
}

// remove an object from a set in the context of a transaction
int LocalTransaction::remset(COid coid, COid toremove){
  if (State != 0) return GAIAERR_TX_ENDED;
  return -1; // not implemented
}


// ------------------------------ Prepare RPC -----------------------------------

// Prepare part of two-phase commit
// remote2pc = true means that all remote replicas are contacted as
//             part of two-phase commit. Otherwise, just the local
//             servers are contacted
// sets *chosents to the timestamp chosen for transaction
// Return 0 if all voted to commit, 1 if some voted to abort, 3 if error getting vote
int LocalTransaction::auxprepare(bool remote2pc, Timestamp &chosents){
  PrepareRPCData *rpcdata;
  PrepareRPCRespData *rpcresp;
  int decision;
  int vote;
  void *state=0;

  //committs.setNew();
  //*chosents = committs;

  rpcdata = new PrepareRPCData;
  rpcdata->data = new PrepareRPCParm;
  rpcdata->deletedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->startts = StartTs;
  //rpcdata->data->committs = committs;
  rpcdata->data->onephasecommit = 0; // no need to use 1pc with local transactions

  rpcresp = (PrepareRPCRespData*) PREPARERPC(rpcdata, state, 0);
  if (!rpcresp){ 
    // function wants us to call again with state after logging is finished, 
    // but there is no logging in the local version, so we call back immidiately
    rpcresp = (PrepareRPCRespData*) PREPARERPC(rpcdata, state, 0);
    assert(state==0);
  }
  delete rpcdata;
  if (rpcresp) vote = rpcresp->data->vote;
  else vote = -1; // I/O error

  // determine decision
  if (vote<0) decision = 3;
  else if (vote>0) decision = 1;
  else decision = 0;

  if (decision == 0){
    chosents = rpcresp->data->mincommitts;
    chosents.addEpsilon();
    Timestamp::catchup(chosents);
  }
  delete rpcresp;

  return decision;
}


// ------------------------------ Commit RPC -----------------------------------

// Commit part of two-phase commit
// outcome is 0 to commit, 1 to abort due to no vote, 3 to abort due to error preparing
// remote2pc = true means that all remote replicas are contacted as
//             part of two-phase commit. Otherwise, just the local
//             servers are contacted
int LocalTransaction::auxcommit(int outcome, bool remote2pc, Timestamp committs){
  CommitRPCData *rpcdata;
  CommitRPCRespData *rpcresp;
  int res;

  rpcdata = new CommitRPCData;
  rpcdata->data = new CommitRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->committs = committs;
  rpcdata->data->commit = outcome;

  rpcresp = (CommitRPCRespData *) COMMITRPC(rpcdata);
  delete rpcdata;
  res = 0;
  if (!rpcresp) res = GAIAERR_SERVER_TIMEOUT; // I/O error
  else {
    res = rpcresp->data->status;
    delete rpcresp;
  }
  return res;
}


//---------------------------- TryCommit -----------------------------

// try to commit
// remote2pc = true means that all remote replicas are contacted as
//             part of two-phase commit. Otherwise, just the local
//             servers are contacted
// Returns
//    0 if committed,
//    1 if aborted due to no vote, 
//    3 if aborted due to prepare failure,
//   <0 if commit error or transaction has ended

int LocalTransaction::tryCommit(bool remote2pc, Timestamp *retcommitts){
  int outcome;
  Timestamp committs;
  int res;

  if (State) return GAIAERR_TX_ENDED;

  if (!hasWrites) return 0; // nothing to commit

  // Prepare phase
  outcome = auxprepare(remote2pc, committs);

  // Commit phase
  res = auxcommit(outcome, remote2pc, committs);

  if (outcome == 0){
    if (retcommitts) *retcommitts = committs; // if requested, return commit timestamp
  }

  State=-1;  // transaction now invalid

  // clear cache
  ClearCache();
  if (res < 0) return res;
  else return outcome;
}

int LocalTransaction::tryCommitSync(void){
  // **!** TBD
  if (State != 0) return GAIAERR_TX_ENDED;
  // run two-phase commit across data centers. Basically want to get vote from
  // remote replicas as well.
  return -1; // not implemented
}

// Abort transaction. Leaves it in valid state.
int LocalTransaction::abort(void){
  Timestamp dummyts;
  int res;

  if (State) return GAIAERR_TX_ENDED;
  if (!hasWrites) return 0; // nothing to commit

  // tell servers to throw away any outstanding writes they had
  dummyts.setIllegal();
  res = auxcommit(2, false, dummyts); // timestamp not really used for aborting txs

  // clear cache
  ClearCache();

  State=-1;  // transaction now invalid
  if (res) return res;
  return 0;
}

//// Read an object in the context of a transaction. Returns
//// a ptr that must be serialized by calling
//// rpcresp.demarshall(ptr).
//int LocalTransaction::readSuperValue(COid coid, SuperValue **svret){
//  FullReadRPCData *rpcdata;
//  FullReadRPCRespData *rpcresp;
//  int respstatus;
//  SuperValue *sv;
//
//  if (State != 0) return 0;
//
//  // here, try to locally read from write set
//  // **!**TO DO
//
//  rpcdata = new FullReadRPCData;
//  rpcdata->data = new FullReadRPCParm;
//  rpcdata->freedata = true; 
//
//  // fill out RPC parameters
//  rpcdata->data->tid = Id;
//  rpcdata->data->ts = StartTs;
//  rpcdata->data->cid = coid.cid;
//  rpcdata->data->oid = coid.oid;
//
//  // do the RPC
//  rpcresp = (FullReadRPCRespData *) FULLREADRPC(rpcdata);
//  delete rpcdata;
//
//  if (!rpcresp){ State=-2; return -10; } // error contacting server
//
//  respstatus = rpcresp->data->status;
//  if (respstatus != 0){
//    delete rpcresp;
//    return respstatus;
//  }
//
//  // set local attributes
//  FullReadRPCResp *r = rpcresp->data; // for convenience
//  sv = new SuperValue;
//  sv->Nattrs = r->nattrs;
//  sv->Ncells = r->ncelloids;
//  sv->Attrs = new u64[r->nattrs]; assert(sv->Attrs);
//  memcpy(sv->Attrs, r->attrs, r->nattrs * sizeof(u64));
//  sv->Cells = new ListCell[sv->Ncells];
//  // fill out cells
//  char *ptr = r->celloids;
//  for (int i=0; i < sv->Ncells; ++i){
//    // extract nkey
//    int nkey = *(int*)ptr;
//    ptr += sizeof(int);
//    sv->Cells[i].nKey = nkey;
//    if (r->celltype == 0) sv->Cells[i].pKey = 0; // integer cell, set pKey=0
//    else { // non-integer key, so extract pKey (nkey has its length)
//      sv->Cells[i].pKey = new char[nkey];
//      memcpy(sv->Cells[i].pKey, ptr, nkey);
//      ptr += nkey;
//    }
//    // extract childOid
//    sv->Cells[i].value = *(Oid*)ptr;
//    ptr += sizeof(u64);
//  }
//  delete rpcresp;
//  *svret = sv;
//  return 0;
//}

int LocalTransaction::writeSuperValue(COid coid, SuperValue *sv){
  FullWriteRPCData *rpcdata;
  FullWriteRPCRespData *rpcresp;
  char *cells, *ptr;
  int respstatus, len, i;

  if (State != 0) return GAIAERR_TX_ENDED;

  hasWrites = true;

  // here, add write to the local write set
  Valbuf *vb = new Valbuf;
  vb->type = 1; // supervalue
  vb->coid = coid;
  vb->immutable = false;
  vb->commitTs.setIllegal();
  vb->readTs.setIllegal();
  vb->len = 0; // not applicable for supervalue
  vb->u.raw = new SuperValue(*sv);

  Ptr<Valbuf> buf = vb;
  UpdateCache(&coid, buf);

  rpcdata = new FullWriteRPCData;
  rpcdata->data = new FullWriteRPCParm;
  rpcdata->freedata = true; 

  FullWriteRPCParm *fp = rpcdata->data; // for convenience

  // fill out RPC parameters
  fp->tid = Id;
  fp->cid = coid.cid;
  fp->oid = coid.oid;
  fp->nattrs = sv->Nattrs;
  fp->celltype = sv->CellType;
  fp->ncelloids = sv->Ncells;
  fp->pKeyInfo = sv->pki;

  // calculate space needed for cells
  len = 0;
  for (i=0; i < sv->Ncells; ++i){
	  if (sv->CellType == 0){ // int key
      len += myVarintLen(sv->Cells[i].nKey);
		  assert(sv->Cells[i].pKey==0);
	  }
	  else len += myVarintLen(sv->Cells[i].nKey) + (int) sv->Cells[i].nKey; // non-int key
	  len += sizeof(u64); // space for 64-bit value in cell
  }
  // fill celloids
  fp->lencelloids = len;
  fp->attrs = (u64*) sv->Attrs;
  cells = fp->celloids = new char[len];
  rpcdata->deletecelloids = cells; // free celloids when rpcdata is destroyed
  ptr = cells;
  for (i=0; i < sv->Ncells; ++i){
    ptr += myPutVarint((unsigned char*)ptr, sv->Cells[i].nKey);
	  if (sv->CellType == 0) ; // int key, do nothing
    else { // non-int key, copy content in pKey
      memcpy(ptr, sv->Cells[i].pKey, (int) sv->Cells[i].nKey);
      ptr += sv->Cells[i].nKey;
    }
    // copy childOid
    memcpy(ptr, &sv->Cells[i].value, sizeof(u64));
    ptr += sizeof(u64);
  }

  // do the RPC
  rpcresp = (FullWriteRPCRespData *) FULLWRITERPC(rpcdata);
  delete rpcdata;

  if (!rpcresp){ State=-2; return -10; } // error contacting server

  respstatus = rpcresp->data->status;
  if (respstatus) State = -2;
  delete rpcresp;
  return respstatus;
}

/* compares a cell key against intKey2/pIdxKey2. Use intKey2 if pIdxKey2==0 otherwise use pIdxKey2 */
/* inline */
static int compareNpKeyWithKey(i64 nKey1, char *pKey1, i64 nKey2, UnpackedRecord *pIdxKey2) {
  if (pIdxKey2) return myVdbeRecordCompare((int) nKey1, pKey1, pIdxKey2);
  else if (nKey1==nKey2) return 0;
  else return (nKey1 < nKey2) ? -1 : +1;
}

// Searches the cells of a node for a given key, using binary search.
// Returns the child pointer that needs to be followed for that key.
// If biasRight!=0 then optimize for the case the key is larger than any entries in node.
// Assumes that the path at the given level has some node (real or approx).
// Guaranteed to return an index between 0 and N where N is the number of cells
// in that node (N+1 is the number of pointers).
// Returns *matches!=0 if found key, *matches==0 if did not find key.
static int CellSearchUnpacked(Ptr<Valbuf> &vbuf, UnpackedRecord *pIdxKey,
                              i64 nkey, int biasRight, int *matches=0){
  int cmp;
  int bottom, top, mid;
  ListCell *cell;
  assert(vbuf->type==1); // must be supernode

  bottom=0;
  top=vbuf->u.raw->Ncells-1; /* number of keys on node minus 1 */
  if (top<0){ matches=0; return 0; } // there are no keys in node, so return index of only pointer there (index 0)
  do {
    if (biasRight){ mid = top; biasRight=0; } /* bias first search only */
    else mid=(bottom+top)/2;
    cell = &vbuf->u.raw->Cells[mid];
    cmp = compareNpKeyWithKey(cell->nKey, cell->pKey, nkey, pIdxKey);

    if (cmp==0) break; /* found key */
    if (cmp < 0) bottom=mid+1; /* mid < target */
    else top=mid-1; /* mid > target */
  } while (bottom <= top);
  // if key was found, then mid points to its index and cmp==0
  // if key was not found, then mid points to entry immediately before key (cmp<0) or after key (cmp>0)

  if (cmp<0) ++mid; // now mid points to entry immediately after key or to one past the last entry
                    // if key is greater than all entries
  // note: if key matches a cell (cmp==0), we follow the pointer to the left of the cell, which
  //       has the same index as the cell

  if (matches) *matches = cmp == 0 ? 1 : 0;
  assert(0 <= mid && mid <= vbuf->u.raw->Ncells);
  return mid;
}
static int CellSearchNode(Ptr<Valbuf> &vbuf, i64 nkey, void *pkey, int biasRight, 
                          GKeyInfo *ki, int *matches=0){
  UnpackedRecord *pIdxKey;   /* Unpacked index key */
  char aSpace[150];          /* Temp space for pIdxKey - to avoid a malloc */
  int res;

  if (pkey){
    pIdxKey = myVdbeRecordUnpack(ki, (int) nkey, pkey, aSpace, sizeof(aSpace));
    if (pIdxKey == 0) return SQLITE_NOMEM;
  } else pIdxKey = 0;
  res = CellSearchUnpacked(vbuf, pIdxKey, nkey, biasRight, matches);
  if (pkey) myVdbeDeleteUnpackedRecord(pIdxKey);
  return res;
}

int LocalTransaction::ListAdd(COid coid, ListCell *cell, GKeyInfo *ki){
  ListAddRPCData *rpcdata;
  ListAddRPCRespData *rpcresp;
  int respstatus;
  Ptr<Valbuf> origvalue;
  int res;

  if (State != 0) return GAIAERR_TX_ENDED;

  res = vsuperget(coid, origvalue); if (res) return res;
  assert(origvalue->type==1); // must be supervalue

  hasWrites = true;

  rpcdata = new ListAddRPCData;
  rpcdata->data = new ListAddRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->pKeyInfo = ki;
  rpcdata->data->cell = *cell;

  // this is the buf information really used by the marshaller

  rpcresp = (ListAddRPCRespData *) LISTADDRPC(rpcdata);
  delete rpcdata;

  if (!rpcresp){ // error contacting server
    State=-2; // mark transaction as aborted due to I/O error
    return -10;
  }

  respstatus = rpcresp->data->status;

  if (respstatus) State=-2; // mark transaction as aborted due to I/O error
  else {
    // insert into a cell and store it in transaction cache
    int index, matches=0;
    if (origvalue->u.raw->Ncells >= 1)
      index = CellSearchNode(origvalue, cell->nKey, cell->pKey, 1, ki, &matches);
    else index = 0; // no cells, so insert at position 0
    assert(0 <= index && index <= origvalue->u.raw->Ncells);
    ListCell *newcell = new ListCell(*cell);
    if (!matches) origvalue->u.raw->InsertCell(index); // item not yet in list
    else {
      origvalue->u.raw->CellsSize -= origvalue->u.raw->Cells[index].size();
      origvalue->u.raw->Cells[index].Free(); // free old item to be replaced
    }
    new(&origvalue->u.raw->Cells[index]) ListCell(*cell); // placement constructor
    origvalue->u.raw->CellsSize += cell->size();
    UpdateCache(&coid, origvalue);
  }
  delete rpcresp;
  return respstatus;
}

// deletes a range of cells from a supervalue
int LocalTransaction::ListDelRange(COid coid, u8 intervalType, ListCell *cell1, ListCell *cell2, GKeyInfo *ki){
  ListDelRangeRPCData *rpcdata;
  ListDelRangeRPCRespData *rpcresp;
  int respstatus;
  Ptr<Valbuf> origvalue;
  int res;

  if (State != 0) return GAIAERR_TX_ENDED;

  res = vsuperget(coid, origvalue); if (res) return res;
  assert(origvalue->type==1); // must be supervalue

  hasWrites = true;

  rpcdata = new ListDelRangeRPCData;
  rpcdata->data = new ListDelRangeRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->pKeyInfo = ki;
  rpcdata->data->intervalType = intervalType;
  rpcdata->data->cell1 = *cell1;
  rpcdata->data->cell2 = *cell2;

  // this is the buf information really used by the marshaller

  rpcresp = (ListDelRangeRPCRespData *) LISTDELRANGERPC(rpcdata);
  delete rpcdata;

  if (!rpcresp){ // error contacting server
    State=-2; // mark transaction as aborted due to I/O error
    return -10;
  }

  respstatus = rpcresp->data->status;

  if (respstatus) State=-2; // mark transaction as aborted due to I/O error
  else {
    // delete chosen cells and store node Valbuf in transaction cache
    int index1, index2;
    int matches1, matches2;
    index1 = CellSearchNode(origvalue, cell1->nKey, cell1->pKey, 0, ki, &matches1);
    // must find value in cell
    assert(0 <= index1 && index1 <= origvalue->u.raw->Ncells);
    if ((intervalType & 2) == 0) ++index1;  // bit 2 is clear so do not include left
    if (index1 < origvalue->u.raw->Ncells){
      index2 = CellSearchNode(origvalue, cell2->nKey, cell2->pKey, 0, ki, &matches2);
      // must find value in cell
      assert(0 <= index2 && index2 <= origvalue->u.raw->Ncells);
      if ((intervalType & 1) == 0) --index2;  // bit 1 is clear so do not include right
      if (index2 == origvalue->u.raw->Ncells) index2--;
    
      if (index1 <= index2){
        origvalue->u.raw->DeleteCellRange(index1, index2+1);
        UpdateCache(&coid, origvalue);
      }
    }
  }

  delete rpcresp;
  return respstatus;
}

// adds a cell to a supervalue
int LocalTransaction::AttrSet(COid coid, u32 attrid, u64 attrvalue){
  AttrSetRPCData *rpcdata;
  AttrSetRPCRespData *rpcresp;
  int respstatus;
  Ptr<Valbuf> origvalue;
  int res;

  if (State != 0) return GAIAERR_TX_ENDED;

  hasWrites = true;

  res = vsuperget(coid, origvalue); if (res) return res;
  assert(origvalue->type==1); // must be supervalue
  assert(origvalue->u.raw->Nattrs > (int)attrid);

  rpcdata = new AttrSetRPCData;
  rpcdata->data = new AttrSetRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->attrid = attrid;  
  rpcdata->data->attrvalue = attrvalue; 

  rpcresp = (AttrSetRPCRespData *) ATTRSETRPC(rpcdata);
  delete rpcdata;

  if (!rpcresp){ // error contacting server
    State=-2; // mark transaction as aborted due to I/O error
    return -10;
  }

  respstatus = rpcresp->data->status;

  if (respstatus) State=-2; // mark transaction as aborted due to I/O error
  else {
    // modify chosen attribute and store node Valbuf in transaction cache
    origvalue->u.raw->Attrs[attrid] = attrvalue;
    UpdateCache(&coid, origvalue);
  }

  delete rpcresp;
  return respstatus;
}

#ifdef TESTMAIN

int _tmain(int argc, _TCHAR* argv[])
{
  WSADATA wsadata;
  int res;
  u8 mysite;
  COid coid;
  char *buf;
  int len;

  res = WSAStartup(0x0202, &wsadata);
	if (res){
		fprintf(stderr, "WSAStartup failed: %d\n", res);
		return 1;
	}

  UniqueId::init();

  mysite=0;
	LocalTransaction t;

  coid.cid=0;
  coid.oid=0;

  t.put(coid, "hi", 3);
  t.tryCommit(0);

  t.start();
  t.put(coid, "me", 3);
  t.get(coid, &buf, &len);
  printf("Got len %d buf %s\n", len, buf);
  t.abort();
  t.tryCommit(0);

  t.start();
  t.get(coid, &buf, &len);
  printf("Got len %d buf %s\n", len, buf);
  WSACleanup();
}
#endif



#ifdef TESTMAIN2

int _tmain(int argc, _TCHAR **av)
{
  char buf[1024];
  int outcome;
  WSADATA wsadata;
  int res, c;
  u64 t1, t2, t3, t4;
  PreciseClock pc;
  bool truncate;
  char data[256];
  char *getbuf;
  int mysite;
  int badargs;

  int operation, off, len;
  int readlen;
  COid coid;

  char **argv = convertargv(argc, (void**) av);
  // remove path from argv[0]
  char *argv0 = strrchr(argv[0], '\\');
  if (argv0) ++argv0; else argv0 = argv[0];


  mysite=0; // site 0 is the default
  badargs=0;
  while ((c = getopt(argc,argv, "s:")) != -1){
    switch(c){
    case 's':
      mysite=atoi(optarg);
      if (mysite > 255){
        printf("Site %d out of bounds (max is 255)\n", mysite); 
        ++badargs; 
      }
      break;
    default:
      ++badargs;
    }
  }
  if (badargs) exit(1); // bad arguments

  argc -= optind;

  if (argc != 3 && argc != 4 && argc != 5 && argc != 7){
    fprintf(stderr, "usage: %s r cid oid off len\n", argv0);
    fprintf(stderr, "usage: %s w cid oid off len truncate data\n", argv0);
    fprintf(stderr, "       %s g   cid oid\n", argv0);
    fprintf(stderr, "       %s p   cid oid data\n", argv0);
    exit(1);
  }

  // parse argument 1
  operation=tolower(*argv[optind]);
  if (!strchr("rwgp", operation)){
    fprintf(stderr, "valid operations are r,w,g,p\n", argv0);
    exit(1);
  }

  switch(operation){
  case 'r':
    if (argc != 5){ fprintf(stderr, "%s: r operation requires 4 arguments\n", argv0); exit(1); }
    coid.cid = (u64) atoi((char*) argv[optind+1]);
    coid.oid = (u64) atoi((char*) argv[optind+2]);
    off = atoi((char*) argv[optind+3]);
    len = atoi((char*) argv[optind+4]);
    *data = 0;
    break;
  case 'w':
    if (argc != 7){ fprintf(stderr, "%s: w operation requires 6 arguments\n", argv0); exit(1); }
    coid.cid = (u64) atoi((char*) argv[optind+1]);
    coid.oid = (u64) atoi((char*) argv[optind+2]);
    off = atoi((char*) argv[optind+3]);
    len = atoi((char*) argv[optind+4]);
    truncate = (bool) atoi((char*) argv[optind+5]);
    strncpy(data, argv[optind+6], sizeof(data));
    data[sizeof(data)-1]=0;
    break;
  case 'g':
    if (argc != 3){ fprintf(stderr, "%s: g operation requires 2 arguments\n", argv0); exit(1); }
    coid.cid = (u64) atoi((char*) argv[optind+1]);
    coid.oid = (u64) atoi((char*) argv[optind+2]);
    break;
  case 'p':
    if (argc != 4){ fprintf(stderr, "%s: p operation requires 3 arguments\n", argv0); exit(1); }
    coid.cid = (u64) atoi((char*) argv[optind+1]);
    coid.oid = (u64) atoi((char*) argv[optind+2]);
    strncpy(data, argv[optind+3], sizeof(data));
    data[sizeof(data)-1]=0;
    break;
  }

  res = WSAStartup(0x0202, &wsadata);
	if (res){
		fprintf(stderr, "WSAStartup failed: %d\n", res);
		return 1;
	}

  UniqueId::init();

	LocalTransaction t;

  t.start();
  switch(operation){
  case 'r':
    t1 = pc.readus();
    res = t.read(coid, buf, off, len, &readlen);
    t2 = pc.readus();

    t3 = pc.readus();
    outcome = t.tryCommit(false);
    t4 = pc.readus();

    printf("read  res %d\n", res);
    printf("read data %s\n", buf);
    printf("read time %d\n", (int)(t2-t1));
    printf("comm time %d\n", (int)(t4-t3));
    printf("comm res  %d\n", outcome);
    break;
  case 'w':
    t1 = pc.readus();
    printf("Writing cid %I64x oid %I64x off %d len %d trunc %d data %s\n", coid.cid, coid.oid, off, len, truncate, data);
    res = t.write(coid, data, off, len, truncate);
    t2 = pc.readus();

    t3 = pc.readus();
    outcome = t.tryCommit(false);
    t4 = pc.readus();

    printf("write  res %d\n", res);
    printf("write time %d\n", (int)(t2-t1));
    printf("comm  time %d\n", (int)(t4-t3));
    printf("comm res  %d\n", outcome);
    break;
  case 'p':
    res = t.put(coid, data, strlen(data)+1);
    printf("put    res %d\n", res);
    outcome = t.tryCommit(false);
    break;
  case 'g':
    res = t.get(coid, &getbuf, &len);
    printf("get    res %d\n", res);
    printf("get    len %d\n", len);
    printf("get    buf %s\n", getbuf);
    t.readFreeBuf(buf);
    break;
  }
  WSACleanup();
}
#endif


#ifdef TESTMAIN3

#pragma comment(lib, "ws2_32.lib")

#include <stdlib.h>
#include <stdio.h>
#include <time.h>

int _tmain(int argc, _TCHAR **av)
{
  //char buf[1024];
  //int outcome;
  WSADATA wsadata;
  //int , c;
  //u64 t1, t2, t3, t4;
  //PreciseClock pc;
  //bool truncate;
  //char data[256];
  //char *getbuf;
  int mysite=0;
  int i, res, count, v1, v2, len1, len2;
  int cid;
  //int badargs;

  //int operation, off, len;
  //int readlen;
  COid coid1, coid2;

#ifdef DEBUGMEMORY
  _CrtSetDbgFlag ( _CRTDBG_ALLOC_MEM_DF | _CRTDBG_LEAK_CHECK_DF );
  _CrtSetReportMode( _CRT_WARN, _CRTDBG_MODE_FILE );
  _CrtSetReportFile( _CRT_WARN, _CRTDBG_FILE_STDOUT );
  _CrtSetReportMode( _CRT_ERROR, _CRTDBG_MODE_FILE );
  _CrtSetReportFile( _CRT_ERROR, _CRTDBG_FILE_STDOUT );
  _CrtSetReportMode( _CRT_ASSERT, _CRTDBG_MODE_FILE );
  _CrtSetReportFile( _CRT_ASSERT, _CRTDBG_FILE_STDOUT );
#endif


  char **argv = convertargv(argc, (void**) av);
  if (argc == 2) cid=atoi(argv[1]);
  else cid=0;

  setvbuf(stdout, 0, _IONBF, 0);

  srand((unsigned)time(0)); // seed PRNG

  res = WSAStartup(0x0202, &wsadata);
	if (res){
		fprintf(stderr, "WSAStartup failed: %d\n", res);
		return 1;
	}

  UniqueId::init();

	LocalTransaction t;
  S = new StorageServerState(0);

  coid1.cid = cid;
  coid1.oid = 0;
  coid2.cid = cid;
  coid2.oid = 256;
  res = 0;
  count = 0;

  for (i=0; i<1000; ++i){
    if (rand()%2==0){
      t.read(coid1, (char*) &v1, 0, sizeof(int), &len1);
      if (len1==0) v1=0;
      ++v1;
      t.write(coid1, (char*) &v1, 0, sizeof(int), true);

      t.read(coid2, (char*) &v2, 0, sizeof(int), &len2);
      if (len2==0) v2=0;
      ++v2;
      t.write(coid2, (char*) &v2, 0, sizeof(int), true);
    } else {
      t.read(coid2, (char*) &v2, 0, sizeof(int), &len2);
      if (len2==0) v2=0;
      ++v2;
      t.write(coid2, (char*) &v2, 0, sizeof(int), true);

      t.read(coid1, (char*) &v1, 0, sizeof(int), &len1);
      if (len1==0) v1=0;
      ++v1;
      t.write(coid1, (char*) &v1, 0, sizeof(int), true);
    }
    res = t.tryCommit(false);
    count += res ? 1 : 0;
    printf("res %d v1 %d v2 %d\n", res, v1, v2);
    t.start();
  }
  printf("sum errors %d\n", count);
  delete S;

  WSACleanup();
}
#endif
