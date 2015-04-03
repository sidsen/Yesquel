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
#include "dtreeaux.h"
#include "gaia/clientlib/supervalue.h"
#include "gaia/util.h"

// initializes a supervalue used to used a DTreeNode
void DTreeNode::InitSuperValue(SuperValue *sv, u8 celltype){
  sv->Nattrs = DTREENODE_NATTRIBS;
  sv->CellType = celltype;
  sv->Ncells = 0;
  sv->Attrs = new u64[sv->Nattrs];
  sv->Cells = 0;
  sv->CellsSize = 0;
  memset(sv->Attrs, 0, sizeof(u64)*sv->Nattrs);
}

RecentSplitsNew::RecentSplitsNew(void (*dowork)(COid, void*, void*), void *parm, EventScheduler *es){ 
  DoWorkFunc = dowork; 
  Parm = parm; 
  ES = es;
}

// submits a request
int RecentSplitsNew::submit(COid &coid, void *specificparm){
  RecentSplitNewItemData **rsid;
  bool doit;
  int res;

  RecentRequests_l.lock();
  res = RecentRequests.lookupInsert(coid, rsid);
  doit = res != 0; // item was not previously in batch, 

  if (doit) *rsid = new RecentSplitNewItemData(this, coid); // new item

  // Currently we are turning off secondrequest. That means that
  // if further requests are submitted during the duration (as informed by
  // reportduration), those requests will be ignored without generating
  // another request at the end of the duration interval.
  // COMMENTED OUT CODE:
  // else (*rsid)->secondrequest = true; // mark as having a second request

  RecentRequests_l.unlock();

  if (doit){
    Work_l.lock();
    DoWorkFunc(coid, Parm, specificparm);  // Parm is a fixed parameter per instance of RecentSplitsNew, 
                                           // whereas specificparm is specific to this submit call
    Work_l.unlock();
  }
  return 0;
}

// Inform duration of request on coid. Schedule the function SchedulingEvent
// to be called after the given duration. That function will check if more requests
// have been submitted for coid; if so, it will call DoWorkFunc() again.
// The overall effect is that DoWorkFunc() gets called once for coid and, if
// multiple requests are submitted for the same coid during the duration period,
// it gets called only one more time at the end of the duration period.
void RecentSplitsNew::reportduration(COid &coid, int durationMs){
  RecentSplitNewItemData **rsid;
  int res;

  RecentRequests_l.lockRead();
  res = RecentRequests.lookup(coid, rsid);

  if (res == 0){ // found item, update finish time
    if ((*rsid)->finishtime == 0){ // finish time not set yet
      (*rsid)->finishtime = durationMs;
      //printf("DurationMs %d\n", durationMs);
      ES->AddEvent(SchedulingEvent, (*rsid), 0, (*rsid)->finishtime);
    }
  }
  RecentRequests_l.unlockRead();
}

int RecentSplitsNew::SchedulingEvent(void *p){
  RecentSplitNewItemData *rsnid = (RecentSplitNewItemData*) p;
  RecentSplitNewItemData *dummy;
  RecentSplitsNew *rsn = rsnid->rsn;
  if (rsnid->secondrequest){ // submit another request
    rsnid->finishtime = 0; // unknown finish time for next request
    rsnid->secondrequest = false;
    rsn->Work_l.lock();
    rsn->DoWorkFunc(rsnid->coid, rsn->Parm, 0);
    rsn->Work_l.unlock();
  } else { // remove item from recent requests
    rsn->RecentRequests_l.lock();
    rsn->RecentRequests.lookupDelete(rsnid->coid, 0, dummy);
    rsn->RecentRequests_l.unlock();
  }
  return 0;
}

// Reads a real node given its oid, obtaining a private copy to the transaction.
// Real is defined by the cursor transaction's start timestamp.
// First try nodes stored in the TxWriteCache (the transaction write set)
// and in TxReadCache.
// We can return data from either cache without making a copy, since that
// data is private to this transaction.
// Then try nodes in the NodeCache, but only if it would bring real data (NOT IMPLEMENTED).
// If reading from NodeCache, make a private copy to return.
// If neither worked, finally read from the TKVS.
// Puts a copy in NodeCache if read node is not leaf and it is more recent than the one there.
// Puts the read node (not copy) in TxReadCache if it is not in TxWriteCache.
// Returns a status: 0 if ok, != 0 if problem.
// The read node is returned in variable outptr.
int auxReadReal(KVTransaction *tx, COid coid, DTreeNode &outptr){
  DTreeNode dtn;
  int res;

  res = KVreadSuperValue(tx, coid, dtn.raw);
  if (res) return res;

  if (dtn.raw->type == 1 && dtn.isInner()){ // inner supernode
    // try to refresh cache if newer
    GCache.refresh(dtn.raw);
  }

  outptr = dtn;
  return 0;
}

// Read a node from the global cache. The data will be immutable (caller should not modify it).
// Returns a status: 0 if found, non-zero if not found
// The read node is returned in variable outptr.
int auxReadCache(COid coid, DTreeNode &outptr){
  return GCache.lookup(coid, outptr.raw);
}

void auxRemoveCache(COid coid){
  GCache.remove(coid);
}

// read data from cache or, if it is not there, from the TKVS.
// If the returned node is a leaf node, it will be real (cache should not have it).
// Returns three things:
//    real=1 if value came from TKVS, real=0 if value came from cache
//    outptr has the read node
//    return value is 0 iff there were no errors
// If the data returned is from the cache (non-real), caller is not allowed to change it
//    since the value may be shared with other threads.
// Otherwise, the data is private and the caller can change it.
int auxReadCacheOrReal(KVTransaction *tx, COid coid, DTreeNode &outptr, int &real){
  int res;
  res = auxReadCache(coid, outptr);
  if (res==0){ 
    assert(outptr.isInner()); // cannot read leaf nodes from cache
    real=0; 
    return 0; 
  } // found in cache
  res = auxReadReal(tx, coid, outptr);
  if (res) return res; // error
  real = 1;
  return 0;
}

