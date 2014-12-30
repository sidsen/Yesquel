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

// clientlib.cpp: Client library for Gaia
#include "stdafx.h"

#define DISABLE_REMOTE2PC  // does not include remote2pc functionality

#include "../tmalloc.h"
#include "../gaiaoptions.h"
#include "../debug.h"

#include <set>
#include <map>
#include "../util.h"
#include "clientlib.h"

#include "../config.h"
#include "clientdir.h"
#include "../gaiarpcaux.h"
#include "../newconfig.h"
#include "supervalue.h"
#include "../record.h"

#define CONFIGFILENAME "config.txt"

//#define TESTMAIN3

#if defined(TESTMAIN) || defined(TESTMAIN2) || defined(TESTMAIN3)
#include "../preciseclock.h"
static const char *CONFIG_FILE="config.txt";
#pragma comment(lib, "ws2_32.lib")
#endif

//__declspec(thread) u64 lastcommitts=0;

const u8 MYSITE=0;

int hasinitwsa = 0;

StorageConfig *InitGaia(int mysite){
  int res;
  WSADATA wsadata;
  StorageConfig *SC=0;

#ifdef USELOG
  setvbuf(stdout, 0, _IONBF, 0);
  setvbuf(stderr, 0, _IONBF, 0);
#endif

  if (!hasinitwsa){
    hasinitwsa=1;
    res = WSAStartup(0x0202, &wsadata);
	  if (res){
		  fprintf(stderr, "WSAStartup failed: %d\n", res);
		  exit(1);
	  }
  }
  UniqueId::init();

  char *configfile = getenv("GAIACONFIG");
  if (!configfile) configfile = GAIA_DEFAULT_CONFIG_FILENAME;
  SC = new StorageConfig(configfile, mysite);
  return SC;
}

void UninitGaia(StorageConfig *SC){
  if (SC) delete SC;
}

Transaction::Transaction(StorageConfig *sc) :
  Cache()
{
  readsCached = 0;
  Sc = sc;
  start();
}

Transaction::~Transaction(){
  ClearCache();
}

void Transaction::ClearCache(void){
  Cache.clear(0, TxCacheEntry::delEntry);
}

// start a new transaction
//int Transaction::start(int offsetms){
int Transaction::start(){
  // obtain a new timestamp from local clock (assumes synchronized clocks)
  //StartTs.setNew(offsetms);
  StartTs.setNew();
  Id.setNew();
  ClearCache();
  State = 0;  // valid
  hasWrites = false;
  return 0;
}

// start a transaction with a start timestamp that will be set when the
// transaction first read, to be the timestamp of the latest available version to read.
// Right now, these transactions must read something before committing. Later,
// we should be able to extend this so that transactions can commit without
// having read.
int Transaction::startDeferredTs(void){
  StartTs.setIllegal();
  Id.setNew();
  ClearCache();
  State = 0;  // valid
  hasWrites = false;
  return 0;

}

void Transaction::Wsamemcpy(char *dest, LPWSABUF wsabuf, int nbufs){
  for (int i=0; i < nbufs; ++i){
    memcpy((void*) dest, (void*) wsabuf[i].buf, wsabuf[i].len);
    dest += wsabuf[i].len;
  }
}

// write an object in the context of a transaction
int Transaction::writeWsabuf(COid coid, int nbufs, LPWSABUF bufs){
  IPPort server = Sc->Od->GetServerId(coid);
  WriteRPCData *rpcdata;
  WriteRPCRespData rpcresp;
  char *resp;
  int respstatus;
  int totlen;

  if (State) return GAIAERR_TX_ENDED;

  // add server index to set of servers participating in transaction
  hasWrites = true;
  Servers.insert(server);

#ifndef DISABLE_REMOTE2PC
  Set<IPPort> *replicas = Sc->Od->GetServerReplicasId(coid);
  for (SetNode<IPPort> *it = replicas->getFirst(); it != replicas->getLast(); it = replicas->getNext(it)){
    ServersAndReplicas.insert(it->key);
  }
  delete replicas;
#endif

  rpcdata = new WriteRPCData;
  rpcdata->data = new WriteRPCParm;
  rpcdata->freedata = true;
  rpcdata->freedatabuf = 0; // buffer comes from caller, so do not free it

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->buf = 0;  // data->buf is not used by client

  // this is the buf information really used by the marshaller
  rpcdata->nbufs=nbufs;
  rpcdata->wsabuf = new WSABUF[nbufs];
  totlen=0;
  for (int i=0; i < nbufs; ++i){
    rpcdata->wsabuf[i] = bufs[i]; 
    totlen += bufs[i].len;
  }
  rpcdata->data->len = totlen;  // total length

  resp = Sc->Rpcc.SyncRPC(server, WRITE_RPCNO, FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ // error contacting server
    //State=-2; // mark transaction as aborted due to I/O error
    return GAIAERR_SERVER_TIMEOUT;
  }

  // record written data
  // create a private copy of the data
  Valbuf *vb = new Valbuf;
  vb->type = 0; // regular value
  vb->coid = coid;
  vb->immutable = false;
  vb->commitTs.setIllegal();
  vb->readTs.setIllegal();
  vb->len = totlen;
  vb->u.buf = Transaction::allocReadBuf(totlen);
  Wsamemcpy(vb->u.buf, bufs, nbufs); 

  Ptr<Valbuf> buf = vb;
  UpdateCache(&coid, buf);

  rpcresp.demarshall(resp);
  respstatus = rpcresp.data->status;
  free(resp);

  //if (respstatus) State=-2; // mark transaction as aborted due to I/O error

  return respstatus;
}

// write an object in the context of a transaction
int Transaction::write(COid coid, char *buf, int len){
  WSABUF wsabuf;
  wsabuf.len = len;
  wsabuf.buf = buf;
  return writeWsabuf(coid, 1, &wsabuf);
}

int Transaction::put2(COid coid, char *data1, int len1, char *data2, int len2){
  WSABUF wsabuf[2];
  wsabuf[0].len = len1;
  wsabuf[0].buf = data1;
  wsabuf[1].len = len2;
  wsabuf[1].buf = data2;
  return writeWsabuf(coid, 2, wsabuf);
}

int Transaction::put3(COid coid, char *data1, int len1, char *data2, int len2, char *data3, int len3){
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
// Returns:       0 = nothing read, 
//                1 = all data read
// GAIAERR_TX_ENDED = cannot read because transaction is aborted
//
int Transaction::tryLocalRead(COid coid, Ptr<Valbuf> &buf){
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

void Transaction::UpdateCache(COid *coid, Ptr<Valbuf> &buf){
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

int Transaction::vget(COid coid, Ptr<Valbuf> &buf){
  IPPort server = Sc->Od->GetServerId(coid);
  int reslocalread;

  ReadRPCData *rpcdata;
  ReadRPCRespData rpcresp;
  char *resp;
  int respstatus;

  if (State){ buf = 0; return GAIAERR_TX_ENDED; }

  reslocalread = tryLocalRead(coid, buf);
  if (reslocalread == 1) return 0; // read completed already
  if (reslocalread < 0) return reslocalread;

#ifdef GAIA_OCC
  // add server index to set of servers participating in transaction
  Servers.insert(server); 
  ReadSet.insert(coid);
#endif

  rpcdata = new ReadRPCData;
  rpcdata->data = new ReadRPCParm;
  rpcdata->freedata = true; 

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->ts = StartTs;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->len = -1;  // requested max bytes to read

  resp = Sc->Rpcc.SyncRPC(server, READ_RPCNO, FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ // error contacting server
    //State=-2; // mark transaction as aborted due to I/O error
    buf = 0;
    return GAIAERR_SERVER_TIMEOUT;
  }

  rpcresp.demarshall(resp);
  respstatus = rpcresp.data->status;
  if (respstatus){ free(resp); buf = 0; return respstatus; }

  if (StartTs.isIllegal()){ // if tx had no start timestamp, set it
    u64 readtsage = rpcresp.data->readts.age();
    if (readtsage > MAX_DEFERRED_START_TS){
      //dprintf(1,"Deferred: beyond max deferred age by %I64d ms\n", readtsage);
      StartTs.setOld(MAX_DEFERRED_START_TS);
    }
    else StartTs = rpcresp.data->readts;
  }

  // fill out buf (returned value to user) with reply from RPC
  Valbuf *vbuf = new Valbuf;
  vbuf->type = 0;
  vbuf->coid = coid;
  vbuf->immutable = true;
  vbuf->commitTs = rpcresp.data->readts;
  vbuf->readTs = StartTs;
  vbuf->len = rpcresp.data->len;
  vbuf->u.buf = rpcresp.data->buf;
  buf = vbuf;

  // add to transaction cache
  if (readsCached < MAX_READS_TO_CACHE){
    ++readsCached;
    UpdateCache(&coid, buf);
  }

  return respstatus;
}

int Transaction::vsuperget(COid coid, Ptr<Valbuf> &buf){
  IPPort server = Sc->Od->GetServerId(coid);
  int reslocalread;

  FullReadRPCData *rpcdata;
  FullReadRPCRespData rpcresp;
  char *resp;
  int respstatus;

  if (State){ buf = 0; return GAIAERR_TX_ENDED; }

  reslocalread = tryLocalRead(coid, buf);
  if (reslocalread == 1 && buf->type==1) return 0; // read completed already
  if (reslocalread < 0) return reslocalread;

#ifdef GAIA_OCC
  // add server index to set of servers participating in transaction
  Servers.insert(server); 
  ReadSet.insert(coid);
#endif

  rpcdata = new FullReadRPCData;
  rpcdata->data = new FullReadRPCParm;
  rpcdata->freedata = true; 

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->ts = StartTs;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;

  resp = Sc->Rpcc.SyncRPC(server, FULLREAD_RPCNO, FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ // error contacting server
    //State=-2; // mark transaction as aborted due to I/O error
    buf = 0;
    return GAIAERR_SERVER_TIMEOUT;
  }

  rpcresp.demarshall(resp);
  respstatus = rpcresp.data->status;
  if (respstatus){ free(resp); buf = 0; return respstatus; }

  FullReadRPCResp *r = rpcresp.data; // for convenience

  if (StartTs.isIllegal()){ // if tx had no start timestamp, set it
    i64 readtsage = rpcresp.data->readts.age();
    if (readtsage > MAX_DEFERRED_START_TS){
      //printf("\nDeferred: beyond max deferred age by %I64d ms ", readtsage);
      StartTs.setOld(MAX_DEFERRED_START_TS);
    }
    else StartTs = rpcresp.data->readts;
  }

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
      sv->Cells[i].pKey = new char[(unsigned)nkey];
      memcpy(sv->Cells[i].pKey, ptr, (unsigned)nkey);
      ptr += nkey;
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
  free(resp); // free buffer from udp/tcp layer
  return respstatus;
}

// free a buffer returned by Transaction::read
void Transaction::readFreeBuf(char *buf){
  assert(buf);
  ReadRPCRespData::clientFreeUDPReceiveBuffer(buf);
}

char *Transaction::allocReadBuf(int len){
  return ReadRPCRespData::clientAllocUDPReceiveBuffer(len);
}

// add an object to a set in the context of a transaction
int Transaction::addset(COid coid, COid toadd){
  if (State) return GAIAERR_TX_ENDED;
  return GAIAERR_NOT_IMPL; // not implemented
}

// remove an object from a set in the context of a transaction
int Transaction::remset(COid coid, COid toremove){
  if (State) return GAIAERR_TX_ENDED;
  return GAIAERR_NOT_IMPL; // not implemented
}


// ------------------------------ Prepare RPC -----------------------------------

// static method
void Transaction::auxpreparecallback(char *data, int len, void *callbackdata){
  PrepareCallbackData *pcd = (PrepareCallbackData*) callbackdata;
  PrepareRPCRespData rpcresp;
  if (data){
    rpcresp.demarshall(data);
    pcd->vote = rpcresp.data->vote;
    pcd->mincommitts = rpcresp.data->mincommitts;
  } else pcd->vote = -1;   // indicates an error (no vote)
  pcd->sem.signal();
  return; // free buffer
}

// Prepare part of two-phase commit
// remote2pc = true means that all remote replicas are contacted as
//             part of two-phase commit. Otherwise, just the local
//             servers are contacted
// sets chosents to the timestamp chosen for transaction
// Return 0 if all voted to commit, 1 if some voted to abort, 3 if error in getting some vote.
// Sets hascommitted to indicate if the transaction was also committed using one-phase commit.
// This is possible when the transaction spans only one server.
int Transaction::auxprepare(bool remote2pc, Timestamp &chosents, int &hascommitted){
  IPPort server;
  SetNode<IPPort> *it;
  PrepareRPCData *rpcdata;
  PrepareCallbackData *pcd;
  LinkList<PrepareCallbackData> pcdlist(true);
  int decision;
  Set<IPPort> *serverset;
  Timestamp committs;

  if (remote2pc) serverset = &ServersAndReplicas;
  else serverset = &Servers;

  //committs.setNew();
  //if (Timestamp::cmp(committs, StartTs) < 0) committs = StartTs; // this could happen if (a) clock is not monotonic or (b) startts is deferred
  //chosents = committs;
  committs = StartTs;

#ifndef DISABLE_ONE_PHASE_COMMIT
  hascommitted = serverset->getnitems()==1 ? 1 : 0;
#else
  hascommitted = 0;
#endif

  for (it = serverset->getFirst(); it != serverset->getLast(); it = serverset->getNext(it)){
    server = it->key;

    rpcdata = new PrepareRPCData;
    rpcdata->data = new PrepareRPCParm;
    rpcdata->deletedata = true;

    // fill out parameters
    rpcdata->data->tid = Id;
    rpcdata->data->startts = StartTs;
    //rpcdata->data->committs = committs;
    rpcdata->data->onephasecommit = hascommitted;

#ifdef GAIA_OCC
    // fill up the readset parameter
    rpcdata->deletereadset = true;
    int sizereadset = ReadSet.getnitems();
    rpcdata->data->readset_len = sizereadset;
    rpcdata->data->readset = new COid[sizereadset];
    SetNode<COid> *itreadset;
    int pos;
    // first, count the number of entries
    for (pos = 0, itreadset = ReadSet.getFirst(); itreadset != ReadSet.getLast(); ++pos, itreadset = ReadSet.getNext(itreadset)){
      rpcdata->data->readset[pos] = itreadset->key;
    }
#else
    rpcdata->deletedata = false; // nothing to delete
    rpcdata->data->readset_len = 0;
    rpcdata->data->readset = 0;
#endif

    pcd = new PrepareCallbackData;
    pcdlist.pushTail(pcd);

    Sc->Rpcc.AsyncRPC(server, PREPARE_RPCNO, FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata, auxpreparecallback, pcd); 
  }

  decision = 0; // commit decision
  for (pcd = pcdlist.getFirst(); pcd != pcdlist.getLast(); pcd = pcdlist.getNext(pcd)){
    pcd->sem.wait(INFINITE);
    if (pcd->vote){   // error or abort vote
      if (decision < 3){
        if (pcd->vote < 0) decision = 3; // error getting some vote
        else decision = 1; // someone voted to abort
      }
    }
    if (decision==0){ // still want to commit
      if (Timestamp::cmp(pcd->mincommitts, committs) > 0) committs = pcd->mincommitts;  // keep track of largest seen timestamp in committs
    }
  }
  if (decision==0){ // commit decision
    committs.addEpsilon(); // next higher timestamp
    Timestamp::catchup(committs);
    chosents = committs;
  } else chosents.setIllegal();

  return decision;
}


// ------------------------------ Commit RPC -----------------------------------

// static method
void Transaction::auxcommitcallback(char *data, int len, void *callbackdata){
  CommitCallbackData *ccd = (CommitCallbackData*) callbackdata;
  CommitRPCRespData rpcresp;
  if (data){
    rpcresp.demarshall(data);
    // record information from response
    ccd->status = rpcresp.data->status;
    ccd->waitingts = rpcresp.data->waitingts;
  } else {
    ccd->status = -1;  // mark error
    ccd->waitingts.setIllegal();
  }
  ccd->sem.signal();
  return; // free buffer
}

// Commit part of two-phase commit
// outcome is 0 to commit, 1 to abort due to prepare no vote, 2 to abort (user-initiated),
// 3 to abort due to prepare failure
// remote2pc = true means that all remote replicas are contacted as
//             part of two-phase commit. Otherwise, just the local
//             servers are contacted
int Transaction::auxcommit(int outcome, bool remote2pc, Timestamp committs){
  IPPort server;
  CommitRPCData *rpcdata;
  CommitCallbackData *ccd;
  LinkList<CommitCallbackData> ccdlist(true);
  Set<IPPort> *serverset;
  SetNode<IPPort> *it;
  int res;

  if (remote2pc) serverset = &ServersAndReplicas;
  else serverset = &Servers;

  res = 0;
  for (it = serverset->getFirst(); it != serverset->getLast(); it = serverset->getNext(it)){
    server = it->key;

    rpcdata = new CommitRPCData;
    rpcdata->data = new CommitRPCParm;
    rpcdata->freedata = true;

    // fill out parameters
    rpcdata->data->tid = Id;
    rpcdata->data->committs = committs;
    rpcdata->data->commit = outcome;

    ccd = new CommitCallbackData;
    ccdlist.pushTail(ccd);

    Sc->Rpcc.AsyncRPC(server, COMMIT_RPCNO, FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata, auxcommitcallback, ccd);
  }

  for (ccd = ccdlist.getFirst(); ccd != ccdlist.getLast(); ccd = ccdlist.getNext(ccd)){
    ccd->sem.wait(INFINITE);
    if (ccd->status < 0) res = GAIAERR_SERVER_TIMEOUT; // error contacting server
    else {
      if (!ccd->waitingts.isIllegal()){
        Timestamp::catchup(ccd->waitingts);
      }
    }
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

int Transaction::tryCommit(bool remote2pc, Timestamp *retcommitts){
  int outcome;
  Timestamp committs;
  int hascommitted;
  int res;

  if (State) return GAIAERR_TX_ENDED;

#ifdef GAIA_OCC
  //if (!hasWrites && ReadSet.getnitems() <= 1) return 0; // nothing to commit
#else
  if (!hasWrites) return 0; // nothing to commit
#endif

  // Prepare phase
  outcome = auxprepare(remote2pc, committs, hascommitted);

  if (!hascommitted){
    // Commit phase
    res = auxcommit(outcome, remote2pc, committs);
  } else res = 0;

  if (outcome==0){
    if (retcommitts) *retcommitts = committs; // if requested, return commit timestamp
    // update lastcommitts
    //if (lastcommitts < committs.getd1()) lastcommitts = committs.getd1();
  }

  State=-1;  // transaction now invalid

  // clear Cache
  ClearCache();
  if (res < 0) return res;
  else return outcome;
}

int Transaction::tryCommitSync(void){
  if (State) return GAIAERR_TX_ENDED;
  // run two-phase commit across data centers. Basically want to get vote from
  // remote replicas as well.
  return GAIAERR_NOT_IMPL; // not implemented
}

// Abort transaction. Leaves it in valid state.
int Transaction::abort(void){
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
//int Transaction::readSuperValue(COid coid, SuperValue **svret){
//  IPPort server = Sc->Od->GetServerId(coid);
//  FullReadRPCData *rpcdata;
//  FullReadRPCRespData rpcresp;
//  char *resp;
//  int respstatus;
//  SuperValue *sv;
//
//  if (State) return GAIAERR_TX_ENDED;
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
//  resp = Sc->Rpcc.SyncRPC(server, FULLREAD_RPCNO, FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);
//
//  if (!resp){ /*State=-2;*/ return GAIAERR_SERVER_TIMEOUT; } // error contacting server
//  rpcresp.demarshall(resp);
//  respstatus = rpcresp.data->status;
//  if (respstatus != 0){
//    free(resp);
//    return respstatus;
//  }
//
//  // set local attributes
//  FullReadRPCResp *r = rpcresp.data; // for convenience
//  sv = new SuperValue;
//  sv->Nattrs = r->nattrs;
//  sv->CellType = r->celltype;
//  sv->Ncells = r->ncelloids;
//  sv->CellsSize = r->lencelloids;
//  sv->Attrs = new u64[r->nattrs]; assert(sv->Attrs);
//  memcpy(sv->Attrs, r->attrs, r->nattrs * sizeof(u64));
//  sv->Cells = new ListCell[sv->Ncells];
//  // fill out cells
//  char *ptr = r->celloids;
//  for (int i=0; i < sv->Ncells; ++i){
//    // extract nkey
//    u64 nkey;
//    ptr += myGetVarint((unsigned char*) ptr, &nkey);
//    sv->Cells[i].nKey = nkey;
//    if (r->celltype == 0) sv->Cells[i].pKey = 0; // integer cell, set pKey=0
//    else { // non-integer key, so extract pKey (nkey has its length)
//      sv->Cells[i].pKey = new char[nkey];
//      memcpy(sv->Cells[i].pKey, ptr, nkey);
//      ptr += nkey;
//    }
//    // extract childOid
//    sv->Cells[i].value = *(Oid*)ptr;
//    ptr += sizeof(u64); // space for 64-bit value in cell
//  }
//  free(resp);
//  *svret = sv;
//  return 0;
//}

int Transaction::writeSuperValue(COid coid, SuperValue *sv){
  IPPort server = Sc->Od->GetServerId(coid);
  FullWriteRPCData *rpcdata;
  FullWriteRPCRespData rpcresp;
  char *cells, *ptr;
  char *resp;
  int respstatus, len, i;

  if (State) return GAIAERR_TX_ENDED;

  // add server index to set of servers participating in transaction
  hasWrites = true;
  Servers.insert(server);

#ifndef DISABLE_REMOTE2PC
  Set<IPPort> *replicas = Sc->Od->GetServerReplicasId(coid);
  for (SetNode<IPPort> *it = replicas->getFirst(); it != replicas->getLast(); it = replicas->getNext(it)){
    ServersAndReplicas.insert(it->key);
  }
  delete replicas;
#endif

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
      ptr += (int) sv->Cells[i].nKey;
    }
    // copy childOid
    memcpy(ptr, &sv->Cells[i].value, sizeof(u64));
    ptr += sizeof(u64);
  }

  // do the RPC
  resp = Sc->Rpcc.SyncRPC(server, FULLWRITE_RPCNO, FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ /* State=-2; */ return GAIAERR_SERVER_TIMEOUT; } // error contacting server

  rpcresp.demarshall(resp);
  respstatus = rpcresp.data->status;
  free(resp);
  // if (respstatus) State = -2;
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
    if (pIdxKey == 0) return GAIAERR_NO_MEMORY;
  } else pIdxKey = 0;
  res = CellSearchUnpacked(vbuf, pIdxKey, nkey, biasRight, matches);
  if (pkey) myVdbeDeleteUnpackedRecord(pIdxKey);
  return res;
}

int Transaction::ListAdd(COid coid, ListCell *cell, GKeyInfo *ki){
  IPPort server = Sc->Od->GetServerId(coid);
  ListAddRPCData *rpcdata;
  ListAddRPCRespData rpcresp;
  char *resp;
  int respstatus;
  Ptr<Valbuf> origvalue;
  int res;

  if (State) return GAIAERR_TX_ENDED;

  res = vsuperget(coid, origvalue); if (res) return res; 
  assert(origvalue->type==1); // must be supervalue

  // add server index to set of servers participating in transaction
  hasWrites = true;
  Servers.insert(server);

#ifndef DISABLE_REMOTE2PC
  Set<IPPort> *replicas = Sc->Od->GetServerReplicasId(coid);
  for (SetNode<IPPort> *it = replicas->getFirst(); it != replicas->getLast(); it = replicas->getNext(it)){
    ServersAndReplicas.insert(it->key);
  }
  delete replicas;
#endif

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

  resp = Sc->Rpcc.SyncRPC(server, LISTADD_RPCNO, FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ // error contacting server
    // State=-2; // mark transaction as aborted due to I/O error
    return GAIAERR_SERVER_TIMEOUT;
  }

  rpcresp.demarshall(resp);
  respstatus = rpcresp.data->status;
  free(resp);

  if (respstatus) ; // State=-2; // mark transaction as aborted due to I/O error
  else {
    // insert into a cell and store it in transaction cache
    int index, matches=0;
    if (origvalue->u.raw->Ncells >= 1){
      index = CellSearchNode(origvalue, cell->nKey, cell->pKey, 1, ki, &matches);
      if (index < 0) return index;
    }
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
  return respstatus;
}

// deletes a range of cells from a supervalue
// intervalType indicates how the interval is to be treated. The possible values are
//     0=(cell1,cell2)   1=(cell1,cell2]   2=(cell1,inf)
//     3=[cell1,cell2)   4=[cell1,cell2]   5=[cell1,inf)
//     6=(-inf,cell2)    7=(-inf,cell2]    8=(-inf,inf)
// where inf is infinity
int Transaction::ListDelRange(COid coid, u8 intervalType, ListCell *cell1, ListCell *cell2, GKeyInfo *ki){
  IPPort server = Sc->Od->GetServerId(coid);
  ListDelRangeRPCData *rpcdata;
  ListDelRangeRPCRespData rpcresp;
  char *resp;
  int respstatus;
  Ptr<Valbuf> origvalue;
  int res;

  if (State) return GAIAERR_TX_ENDED;

  res = vsuperget(coid, origvalue); if (res) return res;
  assert(origvalue->type==1); // must be supervalue

  // add server index to set of servers participating in transaction
  hasWrites = true;
  Servers.insert(server);

#ifndef DISABLE_REMOTE2PC
  Set<IPPort> *replicas = Sc->Od->GetServerReplicasId(coid);
  for (SetNode<IPPort> *it = replicas->getFirst(); it != replicas->getLast(); it = replicas->getNext(it)){
    ServersAndReplicas.insert(it->key);
  }
  delete replicas;
#endif

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

  resp = Sc->Rpcc.SyncRPC(server, LISTDELRANGE_RPCNO, FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ // error contacting server
    // State=-2; // mark transaction as aborted due to I/O error
    return GAIAERR_SERVER_TIMEOUT;
  }

  rpcresp.demarshall(resp);
  respstatus = rpcresp.data->status;
  free(resp);

  if (respstatus) ; // State=-2; // mark transaction as aborted due to I/O error
  else {
    // delete chosen cells and store node Valbuf in transaction cache
    int index1, index2;
    int matches1, matches2;
    index1 = CellSearchNode(origvalue, cell1->nKey, cell1->pKey, 0, ki, &matches1);
    if (index1 < 0) return index1;
    // must find value in cell
    assert(0 <= index1 && index1 <= origvalue->u.raw->Ncells);
    if ((intervalType & 2) == 0) ++index1;  // bit 2 is clear so do not include left
    if (index1 < origvalue->u.raw->Ncells){
      index2 = CellSearchNode(origvalue, cell2->nKey, cell2->pKey, 0, ki, &matches2);
      if (index2 < 0) return index2;
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

  return respstatus;
}
  
// adds a cell to a supervalue
int Transaction::AttrSet(COid coid, u32 attrid, u64 attrvalue){
  IPPort server = Sc->Od->GetServerId(coid);
  AttrSetRPCData *rpcdata;
  AttrSetRPCRespData rpcresp;
  char *resp;
  int respstatus;
  Ptr<Valbuf> origvalue;
  int res;

  if (State) return GAIAERR_TX_ENDED;

  res = vsuperget(coid, origvalue); if (res) return res;
  assert(origvalue->type==1); // must be supervalue
  assert((unsigned)origvalue->u.raw->Nattrs > attrid);

  // add server index to set of servers participating in transaction
  hasWrites = true;
  Servers.insert(server);

#ifndef DISABLE_REMOTE2PC
  Set<IPPort> *replicas = Sc->Od->GetServerReplicasId(coid);
  for (SetNode<IPPort> *it = replicas->getFirst(); it != replicas->getLast(); it = replicas->getNext(it)){
    ServersAndReplicas.insert(it->key);
  }
  delete replicas;
#endif

  rpcdata = new AttrSetRPCData;
  rpcdata->data = new AttrSetRPCParm;
  rpcdata->freedata = true;

  // fill out parameters
  rpcdata->data->tid = Id;
  rpcdata->data->cid = coid.cid;
  rpcdata->data->oid = coid.oid;
  rpcdata->data->attrid = attrid;  
  rpcdata->data->attrvalue = attrvalue; 

  resp = Sc->Rpcc.SyncRPC(server, ATTRSET_RPCNO, FLAG_HID(TID_TO_RPCHASHID(Id)), rpcdata);

  if (!resp){ // error contacting server
    // State=-2; // mark transaction as aborted due to I/O error
    return GAIAERR_SERVER_TIMEOUT;
  }

  rpcresp.demarshall(resp);
  respstatus = rpcresp.data->status;
  free(resp);

  if (respstatus) ; // State=-2; // mark transaction as aborted due to I/O error
  else {
    // modify chosen attribute and store node Valbuf in transaction cache
    origvalue->u.raw->Attrs[attrid] = attrvalue;
    UpdateCache(&coid, origvalue);
  }

  return respstatus;
}

#ifdef TESTMAIN

int _tmain(int argc, _TCHAR* argv[])
{
  WSADATA wsadata;
  int res;
  u8 mysite;
  COid coid;
  Ptr<Valbuf> buf;

  res = WSAStartup(0x0202, &wsadata);
	if (res){
		fprintf(stderr, "WSAStartup failed: %d\n", res);
		return 1;
	}

  tinitScheduler(0);
  UniqueId::init();

  mysite=0;
  StorageConfig sc(CONFIGFILENAME, (u8)mysite);
	Transaction t(&sc);

  coid.cid=0;
  coid.oid=0;

  t.put(coid, "hi", 3);
  t.tryCommit(0);

  t.start();
  t.put(coid, "me", 3);
  t.vget(coid, buf);
  printf("Got len %d buf %s\n", buf->len, buf->u.buf);
  t.abort();
  t.tryCommit(0);

  t.start();
  t.vget(coid, buf);
  printf("Got len %d buf %s\n", buf->len, buf->u.buf);
  WSACleanup();
}
#endif



#ifdef TESTMAIN2

int _tmain(int argc, _TCHAR **av)
{
  int outcome;
  WSADATA wsadata;
  int res, c;
  PreciseClock pc;
  char data[256];
  int mysite;
  int badargs;
  int operation;
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

  if (argc != 3 && argc != 4){
    fprintf(stderr, "usage: %s g   cid oid\n", argv0);
    fprintf(stderr, "       %s p   cid oid data\n", argv0);
    exit(1);
  }

  // parse argument 1
  operation=tolower(*argv[optind]);
  if (!strchr("gp", operation)){
    fprintf(stderr, "valid operations are g,p\n", argv0);
    exit(1);
  }

  switch(operation){
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

  tinitScheduler(0);
  UniqueId::init();

  StorageConfig sc(CONFIGFILENAME, (u8)mysite);
	Transaction t(&sc);

  t.start();
  switch(operation){
  case 'p':
    res = t.put(coid, data, strlen(data)+1);
    printf("put    res %d\n", res);
    outcome = t.tryCommit(false);
    break;
  case 'g':
    Ptr<Valbuf> buf;
    res = t.vget(coid, buf);
    printf("get    res %d\n", res);
    printf("get    len %d\n", buf->len);
    if (buf->len)
      printf("get    buf %s\n", buf->u.buf);
    break;
  }
  fflush(stdout);
  WSACleanup();
}
#endif


#ifdef TESTMAIN3

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
  int i, res, count, v1, v2;
  int cid;
  Ptr<Valbuf> vbuf1, vbuf2;
  //int badargs;

  //int operation, off, len;
  //int readlen;
  COid coid1, coid2;

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

  tinitScheduler(0);
  UniqueId::init();

  StorageConfig sc(CONFIGFILENAME, (u8)mysite);
	Transaction t(&sc);

  coid1.cid = cid;
  coid1.oid = 0;
  coid2.cid = cid;
  coid2.oid = 256;
  res = 0;
  count = 0;

  for (i=0; i<100000; ++i){
    if (rand()%2==0){
      t.vget(coid1, vbuf1);
      if (vbuf1->len==0) v1=0;
      else v1 = *(int*)vbuf1->u.buf;
      ++v1;
      t.write(coid1, (char*) &v1, sizeof(int));

      t.vget(coid2, vbuf2);
      if (vbuf2->len==0) v2=0;
      else v2 = *(int*)vbuf2->u.buf;
      ++v2;
      t.write(coid2, (char*) &v2, sizeof(int));
    } else {
      t.vget(coid2, vbuf2);
      if (vbuf2->len==0) v2=0;
      else v2 = *(int*)vbuf2->u.buf;
      ++v2;
      t.write(coid2, (char*) &v2, sizeof(int));

      t.vget(coid1, vbuf1);
      if (vbuf1->len==0) v1=0;
      else v1 = *(int*)vbuf1->u.buf;
      ++v1;
      t.write(coid1, (char*) &v1, sizeof(int));
    }
    res = t.tryCommit(false);
    count += res ? 1 : 0;
    printf("res %d v1 %d v2 %d\n", res, v1, v2);
    t.start();
  }
  printf("sum errors %d\n", count);

  WSACleanup();
}
#endif
