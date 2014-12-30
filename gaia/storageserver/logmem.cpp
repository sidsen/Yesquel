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
// logmem.cpp
//
// In-memory cache of log

#include "stdafx.h"
#include "../tmalloc.h"
#include "../gaiaoptions.h"
#include "../debug.h"
#include "logmem.h"
#include "storageserver.h"

using namespace std;

void LogOneObjectInMemory::print(COid &coid){
  SingleLogEntryInMemory *sleim;

  lock();
  printf(" LastRead %I64x\n", LastRead.getd1());
  for (sleim = logentries.getFirst(); sleim != logentries.getLast(); sleim = logentries.getNext(sleim)){
    printf(" cid %016I64x oid %016I64x ts %I64x dirty %d\n  ", 
      coid.cid, coid.oid, sleim->ts.getd1(), sleim->flags & SLEIM_FLAG_DIRTY);
    sleim->ticoid->print();
  }
  printf("  ------ pending entries follow\n  ");
  for (sleim = pendingentries.getFirst(); sleim != pendingentries.getLast(); sleim = pendingentries.getNext(sleim)){
    printf(" cid %016I64x oid %016I64x ts %I64x dirty %d\n  ", 
      coid.cid, coid.oid, sleim->ts.getd1(), sleim->flags & SLEIM_FLAG_DIRTY);
    sleim->ticoid->print();
  }
  unlock();
}

void LogOneObjectInMemory::printdetail(COid &coid){
  SingleLogEntryInMemory *sleim;

  lock();
  printf(" LastRead %I64x\n", LastRead.getd1());
  if (logentries.empty()){ printf(" logentries empty\n"); }
  else {
    printf(" logentries-------------\n");
    for (sleim = logentries.getFirst(); sleim != logentries.getLast(); sleim = logentries.getNext(sleim)){
      printf("  ");
      sleim->printShort(coid);
      putchar('\n');
    }
  }
  if (pendingentries.empty()){ printf(" pendingentries empty\n"); }
  else {
    printf(" pendingingentries---------");

    for (sleim = pendingentries.getFirst(); sleim != pendingentries.getLast(); sleim = pendingentries.getNext(sleim)){
      printf("  ");
      sleim->printShort(coid);
      putchar('\n');
    }
  }
  unlock();
}

void LogInMemory::printAllLooim(){
  int nbuckets, i;
  SkipList<COid, LogOneObjectInMemory*> *bucket;
  SkipListNode<COid, LogOneObjectInMemory*> *ptr;
  Timestamp ts;
  Ptr<TxInfoCoid> ticoid;
  int size;
  int nitems=0;

  ts.setNew();

  nbuckets = COidMap.GetNbuckets();
  for (i=0; i < nbuckets; ++i){
    bucket = COidMap.GetBucket(i);
    for (ptr = bucket->getFirst(); ptr != bucket->getLast(); ptr = bucket->getNext(ptr)){
      ++nitems;
      // read entire oid
      size = readCOid(ptr->key, ts, ticoid, 0, 0);

      if (size >= 0){
        printf("COid %016I64x:%016I64x ", ptr->key.cid, ptr->key.oid);
        ticoid->printdetail(ptr->key);
        putchar('\n');
      } else if (size == GAIAERR_TOO_OLD_VERSION)
        printf("COid %016I64x:%016I64x *nodata*\n", ptr->key.cid, ptr->key.oid);
      else printf("COid %016I64x:%016I64x error %d\n", ptr->key.cid, ptr->key.oid, size);
    }
  }
  printf("Total objects %d\n", nitems);
}

void LogInMemory::printAllLooimDetailed(){
  int nbuckets, i;
  SkipList<COid, LogOneObjectInMemory*> *bucket;
  SkipListNode<COid, LogOneObjectInMemory*> *ptr;
  Timestamp ts;
  Ptr<TxInfoCoid> ticoid;
  int size;
  int nitems=0;

  ts.setNew();

  nbuckets = COidMap.GetNbuckets();
  for (i=0; i < nbuckets; ++i){
    bucket = COidMap.GetBucket(i);
    for (ptr = bucket->getFirst(); ptr != bucket->getLast(); ptr = bucket->getNext(ptr)){
      ++nitems;
      printf("COid %016I64x:%016I64x-----------------------------------------------------\n", ptr->key.cid, ptr->key.oid);
      ptr->value->printdetail(ptr->key);
      printf(" Contents ");
      
      // read entire oid
      size = readCOid(ptr->key, ts, ticoid, 0, 0);
      if (size >= 0){
        ticoid->printdetail(ptr->key);
        putchar('\n');
      } else if (size == GAIAERR_TOO_OLD_VERSION)
        printf("COid %016I64x:%016I64x *nodata*\n", ptr->key.cid, ptr->key.oid);
      else printf("COid %016I64x:%016I64x error %d\n", ptr->key.cid, ptr->key.oid, size);
    }
  }
  printf("Total objects %d\n", nitems);
}

#ifndef _LOGMEM_LOCAL
LogInMemory::LogInMemory(DiskStorage *ds) : COidMap(COID_CACHE_HASHTABLE_SIZE)   
#else
LogInMemory::LogInMemory(DiskStorage *ds) : COidMap(COID_CACHE_HASHTABLE_SIZE_LOCAL)   
#endif
{ DS = ds; SingleVersion = false; }

void LogInMemory::getAndLockaux(int res, LogOneObjectInMemory **looimptr){
  if (res) *looimptr = new LogOneObjectInMemory; // not found, so create object
}

// Return entry for an object and locks it for reading or writing.
// If entry does not exist, create it, reading object from disk
// to set the sole entry in the log.
//   coid is the coid of the object
//   writelock=true if locking for write, false if locking for read
// If createfirstlog is true, then create first log entry for object if object is not found
LogOneObjectInMemory *LogInMemory::getAndLock(COid& coid, bool writelock, bool createfirstlog){
  LogOneObjectInMemory *looim, **looimptr;
  int size, res;
  SingleLogEntryInMemory *sleim;

  res = COidMap.lookupInsert(coid, looimptr, getAndLockaux);
  looim = *looimptr;
  if (res==0){ // object found
    if (writelock) looim->lock();
    else looim->lockRead();
    return looim;
  }

  // object not found
  looim->lock();
  
  if (looim->logentries.nitems != 0){ // someone else created item concurrently
    if (!writelock) { looim->unlock(); looim->lockRead(); } // change lock mode, as requested
    return looim;
  }
  
  // try to read object from disk
  size = DS->getCOidSize(coid);
  sleim = 0;

  if (size >= 0){ // oid on disk already, so read it
    sleim = new SingleLogEntryInMemory;
    sleim->ts.setIllegal(); // unknown yet, will be filled by DS->ReadOid below
    sleim->flags = 0; 

    // read it from disk
    Ptr<TxInfoCoid> ticoid;
    res = DS->readCOid(coid, size, ticoid, sleim->ts); assert(res==0);
    // exactly one of twi or twsvi must be non-zero
    assert(ticoid->Writevalue && !ticoid->WriteSV || !ticoid->Writevalue && ticoid->WriteSV);
    assert(ticoid->Litems.size() == 0);

    sleim->ticoid = ticoid;

    // insert one item into list
    looim->logentries.pushTail(sleim);
  } else {
    if (createfirstlog){  // create a first log entry
      sleim = new SingleLogEntryInMemory;
      sleim->ts.setLowest(); // empty entry has lowest timestamp
      sleim->flags = 0; 
      TxWriteItem *twi = new TxWriteItem;
      twi->coid = coid;
      twi->len = 0;
      twi->buf = (char*) malloc(1);
      twi->rpcrequest = 0;
      twi->alloctype = 1;  // allocated via malloc
      sleim->ticoid = new TxInfoCoid(twi);
      sleim->ts.setLowest(); 
      looim->logentries.pushTail(sleim);
    }
  }
#if (SYNC_TYPE != 3)
  // for SYNC_TYPE 3, lock() and lockRead() are identical
  if (!writelock) { looim->unlock(); looim->lockRead(); } // change lock mode, as requested
#endif
  return looim;
}


/****!**** DEBUG CODE */
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
#ifndef NDEBUG
  _CrtMemBlockHeader *pHead = (_CrtMemBlockHeader*) buf - 1;
  assert(*(u32*)(buf+pHead->nDataSize) == 0xfdfdfdfd);
#endif
}

#ifndef NDEBUG
static int checkTicoid(Ptr<TxInfoCoid> ticoid){
  int i;
  int someset=0;
  for (i=0; i < GAIA_MAX_ATTRS; ++i){
    assert((ticoid->SetAttrs[i] & 1) == ticoid->SetAttrs[i]); // only lowest bit should be set
    if (ticoid->SetAttrs[i]) someset=1;
  }
  if (!someset && ticoid->Litems.empty())
    assert(!ticoid->Writevalue && ticoid->WriteSV || ticoid->Writevalue && !ticoid->WriteSV);
  //if (ticoid->SLAddItems.nitems > 0) assert(ticoid->SLpopulated);
  return 1;
}

static int checklog(LinkList<SingleLogEntryInMemory> &logentries){
  SingleLogEntryInMemory *sleim;
  Timestamp ts;

  if (logentries.nitems == 0) return 1;
  ts.setLowest();
  bool hasckp = false;
  for (sleim = logentries.getFirst(); sleim != logentries.getLast(); sleim = logentries.getNext(sleim)){
    assert(Timestamp::cmp(ts, sleim->ts) <= 0);
    assert((sleim->flags & (SLEIM_FLAG_LAST-1)) == sleim->flags);
    assert(checkTicoid(sleim->ticoid));
    if (sleim->ticoid->Writevalue || sleim->ticoid->WriteSV) hasckp = true;
    ts = sleim->ts;
  }
  assert(hasckp);
  return 1;
}

static int checkpending(LinkList<SingleLogEntryInMemory> &pendingentries){
  SingleLogEntryInMemory *sleim;
  Timestamp ts;

  if (pendingentries.nitems == 0) return 1;
  ts.setLowest();
  for (sleim = pendingentries.getFirst(); sleim != pendingentries.getLast(); sleim = pendingentries.getNext(sleim)){
    assert(Timestamp::cmp(ts, sleim->ts) <= 0);
    assert((sleim->flags & (SLEIM_FLAG_LAST-1)) == sleim->flags);
    assert(checkTicoid(sleim->ticoid));
    ts = sleim->ts;
  }
  return 1;
}

#endif

  // Eliminates old entries from log. The eliminated entries are the ones that
  // are subsumed by a newer entry and that are older than LOG_STALE_GC_MS relative to the given ts.
  // Assumes looim->object_lock is held in write mode.
  // Returns number of entries that were removed.

int LogInMemory::gClog(LogOneObjectInMemory *looim, Timestamp ts){
  SingleLogEntryInMemory *sleim, *sleimchkpoint=0;
  int ndeleted=0;

  ts.addMs(-LOG_STALE_GC_MS);

  // find the highest checkpoint with a timestamp <= ts
  for (sleim = looim->logentries.getFirst(); sleim != looim->logentries.getLast(); sleim = looim->logentries.getNext(sleim)){
    if (Timestamp::cmp(sleim->ts, ts) >= 0) break;
    if (sleim->ticoid->Writevalue || sleim->ticoid->WriteSV) sleimchkpoint = sleim;
  }
  if (!sleimchkpoint) return 0; // nothing to GC
  sleim = looim->logentries.getFirst();
  while (sleim != sleimchkpoint){
    ++ndeleted;
    looim->logentries.popHead();
    delete sleim;
    sleim = looim->logentries.getFirst();
  }
  return ndeleted;
}

// read data of a given oid and version, starting from offset off
// with length len. If len==-1 then read until the end.
// Put the result in retticoid. Returns
//          0 if ok
//         -1 if oid does not exist,
//         -2 if oid exists but version does not exist
//         -3 if oid exists, data to be read is pending, and read cannot be deferred (deferredhandle==0)
//         -4 if oid is corrupted (e.g., attrset followed by a regular value)
//         -5 if oid exists, data to be read is pending, and read was deferred
// *!* TODO: check for -4 return value from caller
// Also returns the timestamp of the data that is actually read in *readts, if readts != 0.
// If function returns an error and *buf=0 when it is called, it is possible
// that *buf gets changed to an allocated buffer, so caller should free *buf
//
// int LogInMemory::readCOid(COid& coid, Timestamp ts, int len, char **destbuf, Timestamp *readts, int nolock){

int LogInMemory::readCOid(COid& coid, Timestamp ts, Ptr<TxInfoCoid> &retticoid,
                          Timestamp *readts, void *deferredhandle){
  LogOneObjectInMemory *looim;
  SingleLogEntryInMemory *sleim;
  Ptr<TxInfoCoid> ticoid;
  int retval = 0;
  int type = -1;
  int moveback=0, moveforward=0, moveforwardadd=0, moveforwarddel=0;

  // try to find COid in memory
  looim = getAndLock(coid, true, true); assert(looim);

  //assert(checklog(looim->logentries));
  //assert(checkpending(looim->pendingentries));
  sleim = 0;
  if (ts.isIllegal()){ // read first available timestamp
    Timestamp pendingts;

    // get earliest pending entry
    sleim = looim->pendingentries.getFirst();
    if (sleim != looim->pendingentries.getLast()) pendingts = sleim->ts;
    else pendingts.setHighest(); // no pending entries

    // first latest logentry that is <= than earliest pending entry
    // (= is fine since tx will commit with a ts strictly bigger than pending entries)
    sleim = looim->logentries.rGetFirst(); // start from end
    while (sleim != looim->logentries.rGetLast()){
      if (Timestamp::cmp(sleim->ts, pendingts) <= 0) break; // found entry <= pending entry
      sleim = looim->logentries.rGetNext(sleim);
    }
    if (sleim == looim->logentries.rGetLast()){ retval = GAIAERR_TOO_OLD_VERSION; goto end; }

    ts = sleim->ts; // this is the timestamp we will read
    goto proceed_with_read;
  }

  // check to see if there are any pending reads with higher timestamp, in which case we need to defer or fail the read
  SingleLogEntryInMemory *pendingsleim = looim->pendingentries.getFirst();
  if (pendingsleim != looim->pendingentries.getLast()){ // pendingentries not empty
    // enough to check first entry since pendingentries is sorted by timestamp
    if (Timestamp::cmp(pendingsleim->ts, ts) <= 0){ //some pendingentry has smaller timestamp, defer or fail
      if (deferredhandle){
        // TODO: some way to garbage collect those pending sleims. Right now,
        // it will remain pending until the transaction commits or aborts. But if
        // the client died, it remains pending forever. Need a way to
        // cancel the pending data after a while. Problem is that the transaction
        // may have committed (and server doesn't yet know about it), so we cannot just
        // throw away the pending data. We need to determine transaction status and
        // if aborted, throw away, otherwise mark it as non-pending.
        WaitingListItem *vli = new WaitingListItem(deferredhandle, pendingsleim->waitOnPending.next, ts);
        pendingsleim->waitOnPending.next = vli; // add item to waitOnPending linklist (at beginning)

        // remember highest waiting timestamp. This is to report to the client doing the pending
        // transaction, when it commits
        if (Timestamp::cmp(ts, pendingsleim->waitingts) > 0) pendingsleim->waitingts = ts;
        retval = GAIAERR_DEFER_RPC;
      } else {
        retval = GAIAERR_PENDING_DATA; // trying to read pending data
      }
      goto end;
    }
  }

  // move backwards in log looking for correct timestamp to read from (first timestamp <= ts)
  for (sleim = looim->logentries.rGetFirst(); sleim != looim->logentries.rGetLast(); sleim = looim->logentries.rGetNext(sleim)){
    if (Timestamp::cmp(sleim->ts, ts) <= 0) break; // found it
    ++moveback;
  }
  if (sleim == looim->logentries.rGetLast()){ // version is too old for what we have in log
    retval =  GAIAERR_TOO_OLD_VERSION;
    goto end;
  }
  //printf("%07I64d ", ts.getd1()-sleim->ts.getd1());

  proceed_with_read:

  if (readts) *readts = sleim->ts; // record the timestamp

  // now keep moving backwards in log until we find a checkpoint (a full write for a value or supervalue)
  for (; sleim != looim->logentries.rGetLast(); sleim = looim->logentries.rGetNext(sleim)){
    if (sleim->ticoid->Writevalue){ type=0; break; }  // found checkpoint of regular value
    if (sleim->ticoid->WriteSV){ type=1; break; }  // found checkpoint of supervalue
    ++moveback;
  }

  if (sleim == looim->logentries.rGetLast()){
    assert(type==-1);
    retval = GAIAERR_TOO_OLD_VERSION; // missing checkpoint of object, so cannot read it
    goto end;
  }

  ticoid = sleim->ticoid;
  assert(ticoid->Writevalue && !ticoid->WriteSV || !ticoid->Writevalue && ticoid->WriteSV);
  assert(ticoid->Litems.size()==0);
#ifdef DEBUG
  for (int i=0; i < GAIA_MAX_ATTRS; ++i) assert(!ticoid->SetAttrs[i]);
#endif

  if (type == 0){ // regular value
    sleim = looim->logentries.getNext(sleim); // look at next log entry
    if (sleim != looim->logentries.getLast() && Timestamp::cmp(sleim->ts, ts) <= 0){
      retval = GAIAERR_CORRUPTED_LOG; // corruption: write of value cannot be followed by further updates
      goto end;
    }
  } else { // super value
    TxWriteSVItem *twsvi = 0; // set if we need to create a new twsvi
    Timestamp lastts;
    assert(type==1);
    assert(ticoid->WriteSV);
    sleim = looim->logentries.getNext(sleim);
    //twsvi = new TxWriteSVItem(*twsvi); // make a copy of what is in log since we will be applying updates to it

    // update deltas in current and next log entries (sleims)
    while (sleim != looim->logentries.getLast() && Timestamp::cmp(sleim->ts, ts) <= 0){
      ++moveforward;
      lastts = sleim->ts;
      // assert(!(sleim->flags & SLEIM_FLAG_PENDING)); // we already checked above that it is not pending
      // Here update twsvi with the updates in the sleim
      //ticoid = sleim->ticoid;
      // apply any updates to attributes
      for (int i=0; i < GAIA_MAX_ATTRS; ++i){
        if (sleim->ticoid->SetAttrs[i]){ // setting attribute
          if (!twsvi)
            twsvi = new TxWriteSVItem(*ticoid->WriteSV); // copy WriteSV in checkpoint
          assert(twsvi->nattrs >= i);
          twsvi->attrs[i] = sleim->ticoid->Attrs[i]; // apply the change
        }
      }
      for (list<TxListItem*>::iterator it=sleim->ticoid->Litems.begin(); it != sleim->ticoid->Litems.end(); ++it){
        if (!twsvi) 
          twsvi = new TxWriteSVItem(*ticoid->WriteSV); // copy WriteSV in checkpoint

        TxListItem *tli = *it;
        if (tli->type==0){ // add item
          ++moveforwardadd;
          TxListAddItem *tlai = dynamic_cast<TxListAddItem*>(tli);
          if (tlai->item.ppki.ki) twsvi->SetPkiSticky(*tlai->item.ppki.ki); // provide pki to TxWriteSVItem if it doesn't have it already
          twsvi->cells.insertOrReplace(new ListCellPlus(tlai->item, &twsvi->pki), 0, ListCellPlus::del, 0); // copy item since twsvi->cells will own it
                                                                            // Use pki of enclosing TxWriteSVItem.
        }
        else { // delrange item
          assert(tli->type==1);
          int type1, type2; // indicates type of left and right intervals
          ++moveforwarddel;
          TxListDelRangeItem *tldri = dynamic_cast<TxListDelRangeItem*>(tli);
          TxWriteSVItem::convertOneIntervalTypeToTwoIntervalType(tldri->intervalType, type1, type2);
          if (tldri->itemstart.ppki.ki)twsvi->SetPkiSticky(*tldri->itemstart.ppki.ki); // provide pki to TxWriteSVItem if it doesn't have it already
          twsvi->cells.delRange(&tldri->itemstart, type1, &tldri->itemend, type2, ListCellPlus::del, 0);
        }
      }
      sleim = looim->logentries.getNext(sleim);
    }
    if (twsvi){ // create ticoid with checkpoint and perhaps insert it to in-memory log
      ticoid = new TxInfoCoid(twsvi);
      if (moveforwardadd >= LOG_CHECKPOINT_MIN_ADDITEMS ||
          moveforwarddel >= LOG_CHECKPOINT_MIN_DELRANGEITEMS ||
          moveforward > LOG_CHECKPOINT_MIN_ITEMS){ // only store checkpoint in log if some of these conditions are met
        SingleLogEntryInMemory *toadd = new SingleLogEntryInMemory;
        toadd->ts = lastts;
        toadd->flags = SLEIM_FLAG_SNAPSHOT;
        //toadd->dirty = false; // no need to flush this to disk
        //toadd->pending = false;
        toadd->ticoid = ticoid;
        looim->logentries.addBefore(toadd, sleim);
        //assert(checklog(looim->logentries));
      }
    }
  }

  if (Timestamp::cmp(looim->LastRead, ts) < 0) looim->LastRead = ts;
  retticoid = ticoid;
  gClog(looim, ts);
  dprintf(2, "readCOid: moveback %d moveforward %d", moveback, moveforward);
     
end:
  looim->unlock();
  return retval;
}

// after writing, buf will be owned by LogInMemory. Caller should have allocated it and should not free it.
int LogInMemory::writeCOid(COid& coid, Timestamp ts, Ptr<TxInfoCoid> ticoid){
  LogOneObjectInMemory *looim;

  assert(ticoid->Writevalue&&!ticoid->WriteSV || !ticoid->Writevalue&&ticoid->WriteSV);
  assert(ticoid->Litems.size()==0);
#ifdef DEBUG
  for (int i=0; i < GAIA_MAX_ATTRS; ++i) assert(!ticoid->SetAttrs[i]);
#endif

//  printf("writeCOid %016I64x %016I64x ts %I64x truncate %d off %d len %d buf %c%c%c pending %d\n",
//    coid.cid, coid.oid, ts.getd1(), truncate, off, len, buf[0], buf[1], buf[2], pending);

  looim = getAndLock(coid, true, false);
  auxAddSleimToLogentries(looim, ts, true,ticoid);
  looim->unlock();

  return 0;
}

// Wakes up deferred RPCs in the waiting list of a sleim or move them to the sleim of another pending entry,
// based on the ts of the waiting list item. If that ts is < than all pending entries (though it suffices to check the first
// since the entries are ordered by their timestamp), then wake up, otherwise move.
// Motivation: if the ts is < than all pending entries, then deferred RPC will not block. Otherwise,
// there is no point in waking it up.
// The waiting list of the given sleim ends up empty.
void LogInMemory::wakeOrMoveWaitingList(LogOneObjectInMemory *looim, SingleLogEntryInMemory *sleim){
  LinkList<SingleLogEntryInMemory> *pendingentries = &looim->pendingentries;
  SingleLogEntryInMemory *firstsleim;
  if (pendingentries->empty()) firstsleim = 0; // nothing
  else firstsleim = pendingentries->getFirst();

  // For each deferred RPC indicated in the sleim, check if its timestamp < smallest timestamp in
  // pendingentries. If so, wake up RPC. Otherwise, move RPC to entry with smallest timestamp in pendingentries
  WaitingListItem *next;
  for (WaitingListItem *vli = sleim->waitOnPending.next; vli; vli = next){
    next = vli->next;
    if (!firstsleim || Timestamp::cmp(vli->ts, firstsleim->ts) < 0){ // RPC ready to be woken up
      ServerAuxWakeDeferred(vli->ptr);
      delete vli;
    }
    else { // Other sleims are blocking the item. Move it to one such sleim (eg, first one)
      // insert item into waitOnPending linklist of firstsleim
      vli->next = firstsleim->waitOnPending.next;
      firstsleim->waitOnPending.next = vli;
    }
  }
  sleim->waitOnPending.next = 0; // clear list since we either deleted or moved each item

}

// Remove entry from pendingentries.
// If parameter move is true, add removed entry to logentries, setting its final timestamp to the given parameter.
// Wake up any deferred RPCs indicated in the sleim if no more pendingentries will block it. If some
// pendingentries may block it, move deferred RPCs to one of them (say, the earliest one).
// The GC logentries.
// Each tx should have a single pending entry per object, so this
// function is effecting all the updates of a tx to a given object.
void LogInMemory::removeOrMovePendingToLogentries(COid& coid, SingleLogEntryInMemory *pendingentriesSleim, Timestamp ts, bool move){
  assert(pendingentriesSleim);
  LogOneObjectInMemory *looim;
  int res;

  res = COidMap.lookup(coid, looim);
  if (res) return; // not found

  looim->lock();
#ifdef DEBUG
  //assert(checklog(looim->logentries));
  //assert(checkpending(looim->pendingentries));
#endif

  LinkList<SingleLogEntryInMemory> *pendingentries = &looim->pendingentries;
  // remove from pendingentries
  pendingentries->remove(pendingentriesSleim);

  if (move){
    // Add a new sleim to logentries
    // We could have recycled the sleim, but we do not so that we can reuse auxAddSleimToLogentries.
    auxAddSleimToLogentries(looim, ts, true, pendingentriesSleim->ticoid);
  }

  // wake up deferred RPCs or move them to another sleim
  wakeOrMoveWaitingList(looim, pendingentriesSleim);
  delete pendingentriesSleim;

  looim->unlock();
  return;
}

// flushes all entries in memory to disk.
// This function is not safe to be called when there is other activity in the storagenode.
void LogInMemory::flushToDisk(Timestamp &ts){
  int res;
  Ptr<TxInfoCoid> ticoid;
  int nbuckets, i;
  SkipList<COid, LogOneObjectInMemory*> *bucket;
  SkipListNode<COid, LogOneObjectInMemory*> *ptr;

  // iterate over all oids in memory
  nbuckets = COidMap.GetNbuckets();
  for (i=0; i < nbuckets; ++i){
    bucket = COidMap.GetBucket(i);
    for (ptr = bucket->getFirst(); ptr != bucket->getLast(); ptr = bucket->getNext(ptr)){
      // read entire oid
      res = readCOid(ptr->key, ts, ticoid, 0, 0);
      if (res >= 0){
        // write it to disk
       DS->writeCOid(ptr->key, ticoid, ts);
      }
    }
  }
}


// flushes all entries in memory to file.
// Returns 0 if ok, non-zero if error.
int LogInMemory::flushToFile(Timestamp &ts, char *flushfilename){
  char *buf;
  int res, size;
  char *filename;
  FILE *f=0;
  int retval=0;
  Ptr<TxInfoCoid> ticoid;
  int type=-1;
  int nbuckets, i;
  SkipList<COid, LogOneObjectInMemory*> *bucket;
  SkipListNode<COid, LogOneObjectInMemory*> *ptr;

  //char *dir = DS->getDiskStoragePath();
  //filename = new char[strlen(dir)+2+strlen(flushfilename)];
  //strcpy(filename, dir);
  //if (dir[strlen(dir)-1] != '/') strcat(filename, "/");
  //strcat(filename, flushfilename);
  filename = flushfilename;
  f = fopen(filename, "wbS");
  if (!f){
    dprintf(1, "flushToFile: error opening file %s for writing", filename);
    goto error;
  }
  //delete [] filename;

  // iterate over all oids in memory
  nbuckets = COidMap.GetNbuckets();
  for (i=0; i < nbuckets; ++i){
    bucket = COidMap.GetBucket(i);
    for (ptr = bucket->getFirst(); ptr != bucket->getLast(); ptr = bucket->getNext(ptr)){
      // read entire oid
      buf = 0;
      size = readCOid(ptr->key, ts, ticoid, 0, 0);
      if (size >= 0){
        // write coid
        res = (int) fwrite((void*)&ptr->key, 1, sizeof(COid), f); if (res != sizeof(COid)) goto error;
  
        res = DS->writeCOidToFile(f, ticoid);
        if (res) goto error;
      }
    }
  }

 end:
  if (f) fclose(f);
  return retval;

 error:
  retval = -1;
  goto end;
}


// load contents of disk into memory cache
void LogInMemory::loadFromDisk(void){
  void *handle;
  WIN32_FIND_DATA ffd;
  char *dir;
  wchar_t *dir2;
  size_t outlen, size;
  COid coid;
  char filename[256];
  char *dirname;
  Timestamp ts;
  char *buf;
  int res;
  Ptr<TxInfoCoid> ticoid;

  ts.setNew();

  // convert directory from char to wchar
  dir = DS->getDiskStoragePath();
  size = strlen(dir);
  dirname = new char[size+3];
  strcpy(dirname, dir);
  // remove trailing \ or /
  if (dirname[size-1] == '/' || dirname[size-1] == '\\') dirname[size-1]=0; 
  strcat(dirname, "\\*"); // append \* to end of dirname
  size += 2;
  dir2 = new wchar_t[size+1];
  mbstowcs_s(&outlen, dir2, size+1, dirname, _TRUNCATE);
  delete [] dirname;
 
  // iterate over all files in storage path
  handle = FindFirstFile(dir2, &ffd);
  delete [] dir2;

  if (handle == INVALID_HANDLE_VALUE) return;
  do
  {
    if (!(ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)){
      wcstombs(filename, ffd.cFileName, sizeof(filename));
      filename[sizeof(filename)-1]=0;
      if (!strcmp(filename, FLUSH_FILENAME)) continue; // this file is not an object...
      // convert to a coid
      coid = DiskStorage::FilenameToCOid(filename);
      buf = 0;
      // the call to readCOid will cause the object to be read from disk since it is not in memory
      res = readCOid(coid, ts, ticoid, 0, 0); assert(res >= 0);
    }
  } while (FindNextFile(handle, &ffd) != 0);
  FindClose(handle);
}

// load contents of disk into memory cache
int LogInMemory::loadFromFile(char *flushfilename){
  int res;
  COid coid;
  char *filename;
  Timestamp ts;
  FILE *f=0;
  int retval = 0;
  Ptr<TxInfoCoid> ticoid;

  //char *dir = DS->getDiskStoragePath();
  //filename = new char[strlen(dir)+2+strlen(flushfilename)];
  //strcpy(filename, dir);
  //if (dir[strlen(dir)-1] != '/') strcat(filename, "/");
  //strcat(filename, flushfilename);
  filename = flushfilename;

  f = fopen(filename, "rbS");
  if (!f){
    dprintf(1, "loadFromFile: error opening file %s for reading", filename);
    goto error;
  }
  //delete [] filename;

  ts.setNew();

  while (!feof(f)){
    // read one object
    res = (int)fread((void*)&coid, 1, sizeof(COid), f); // coid
  	if (res == 0){ 
      if (!feof(f)) goto error;
      continue;
    }
    if (res != sizeof(COid)) goto error;
    res = DS->readCOidFromFile(f, coid, ticoid); if (res) goto error;
    res = writeCOid(coid, ts, ticoid); if (res) goto error;
  }
 end:
  if (f) fclose(f);
  return retval;
 error:
  retval = -1;
  goto end;
}
