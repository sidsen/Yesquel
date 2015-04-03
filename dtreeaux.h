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

#ifndef _DTREEAUX_H_
#define _DTREEAUX_H_

#include "options.h"
#include "gaia/inttypes.h"
#include "cellbuf.h"
#include "kvinterface.h"
#include "gaia/util.h"
#include "gaia/clientlib/supervalue.h"
#include "gaia/datastruct.h"
#include "gaia/scheduler.h"
#include "gaia/debug.h"

#ifdef USELOG
#define DTREELOG(format,...) dprintf(2, "%I64x:%s:%d:" format, Time::now(), __FUNCTION__, __LINE__, ##__VA_ARGS__)
#else
#define DTREELOG(format,...)
#endif

#define BSKIPLIST_CID_BIT  0x0000800000000000LL /* bit that defines a b-skiplist container id */
#define DATA_CID(cid) (cid & ~BSKIPLIST_CID_BIT)

#define DTREENODE_FLAG_INTKEY    0x0001 // node holds integer keys only
#define DTREENODE_FLAG_LEAF      0x0002 // node is a leaf

#define DTREENODE_NATTRIBS           5  // number of attribs in each node
// attribute numbers
#define DTREENODE_ATTRIB_FLAGS         0 // flags
#define DTREENODE_ATTRIB_HEIGHT        1 // height
#define DTREENODE_ATTRIB_LASTPTR       2 // last pointer in node
#define DTREENODE_ATTRIB_LEFTPTR       3 // pointer to left neighbor (0 if none)
#define DTREENODE_ATTRIB_RIGHTPTR      4 // pointer to right neighbor (0 if none)

// used to store a node of the DTree
class DTreeNode {
public:
  Ptr<Valbuf> raw;    // has flags, height, ncells, cells, etc
  Oid &NodeOid(){ return raw->coid.oid; }
  u64 &Flags(){ return raw->u.raw->Attrs[DTREENODE_ATTRIB_FLAGS]; }
  u64 &Height(){ return raw->u.raw->Attrs[DTREENODE_ATTRIB_HEIGHT]; }
  Oid &LastPtr(){ return raw->u.raw->Attrs[DTREENODE_ATTRIB_LASTPTR]; }
  Oid &LeftPtr(){ return raw->u.raw->Attrs[DTREENODE_ATTRIB_LEFTPTR]; }
  Oid &RightPtr(){ return raw->u.raw->Attrs[DTREENODE_ATTRIB_RIGHTPTR]; }
  int &Ncells(){ return raw->u.raw->Ncells; }
  int &CellsSize(){ return raw->u.raw->CellsSize; }
  ListCell *Cells(){ return raw->u.raw->Cells; }
  u8 &CellType(){ return raw->u.raw->CellType; }
  GKeyInfo *Pki(){ return raw->u.raw->pki; }
  Oid &GetPtr(int index){
    if (index==raw->u.raw->Ncells) return LastPtr();
    else return raw->u.raw->Cells[index].value;
  }

  DTreeNode(){ raw = 0; }
  //DTreeNode(const DTreeNode& c);

   
  bool isRoot(){ return raw->coid.oid==0; } // root is oid 0
  bool isLeaf(){ return (Flags() & DTREENODE_FLAG_LEAF) != 0; }
  bool isInner(){ return !isLeaf(); }
  bool isIntKey(){ 
    assert(((Flags() & DTREENODE_FLAG_INTKEY) != 0) == (raw->u.raw->CellType==0));
    return (Flags() & DTREENODE_FLAG_INTKEY) != 0;
  }

  //void newEmpty(COid coid, bool intKey);   // create new empty node
  //                                       // intKey==true iff node stores integers
  //int read(KVTransaction *tx, COid coid); // read node from KV store
  //int write(KVTransaction *tx, COid coid); // write node to KV store

  static void InitSuperValue(SuperValue *sv, u8 celltype);
};

class RecentSplitsNew;

class RecentSplitNewItemData {
public:
  RecentSplitsNew *rsn;
  COid coid;
  bool secondrequest;  // whether a second request was made
  int  finishtime;       // duration if known or 0 if not known
  RecentSplitNewItemData(){}
  RecentSplitNewItemData(RecentSplitsNew *r, COid &c){ rsn=r; coid=c; secondrequest=false; finishtime=0; }
  static void del(RecentSplitNewItemData *rsid){ delete rsid; }
};

class RecentSplitsNew {
private:
  SkipList<COid,RecentSplitNewItemData*> RecentRequests;
  RWLock RecentRequests_l; // protects Batch
  RWLock Work_l;  // serializes calls to DoWorkFunc
  EventScheduler *ES;
  void (*DoWorkFunc)(COid coid, void *parm, void *specificparm);  // function to call to do work
  void *Parm;     // parameter to pass to DoWorkFunc
 
  static int SchedulingEvent(void *p);

public:
  RecentSplitsNew(void (*dowork)(COid, void*, void*), void *parm, EventScheduler *es);
  int submit(COid &coid, void *specificparm); // submit a request to DoWorkFunc

  // Inform duration of request on coid. Schedule the function SchedulingEvent
  // to be called after the given duration. That function will check if more requests
  // have been submitted for coid; if so, it will call DoWorkFunc() again.
  // The overall effect is that DoWorkFunc() gets called once for coid and, if
  // multiple requests are submitted for the same coid during the duration period,
  // it gets called only one more time at the end of the duration period.
  void reportduration(COid &coid, int durationMs);
};


// prototype definitions
int auxReadReal(KVTransaction *tx, COid coid, DTreeNode &outptr);
int auxReadCache(COid coid, DTreeNode &outptr);
void auxRemoveCache(COid coid);
int auxReadCacheOrReal(KVTransaction *tx, COid coid, DTreeNode &outptr, int &real);

#endif
