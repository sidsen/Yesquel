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

#include "cellbuf.h"


// struct modified from SQLite
struct Btree {
  sqlite3 *db;
  BtShared *pBt;
  u8 inTrans;
  u8 sharable;
  u8 locked;
  int wantToLock;
  int nBackup;
  Btree *pNext;
  Btree *pPrev;
#ifndef SQLITE_OMIT_SHARED_CACHE
  BtLock lock;       /* Object used to lock page 1 */
#endif
  KVTransaction *tx; // MKA
};

// struct modified from SQLite
struct BtShared {
  Pager *pPager;
  sqlite3 *db;
  BtCursor *pCursor;
  DbRootInfo *pPage1;
  u8 readOnly;
  u8 pageSizeFixed;
  u8 secureDelete;
  u8 initiallyEmpty;
  u8 openFlags;
#ifndef SQLITE_OMIT_AUTOVACUUM
  u8 autoVacuum;
  u8 incrVacuum;
#endif
  u8 inTransaction;
  u8 doNotUseWAL;
  u16 maxLocal;
  u16 minLocal;
  u16 maxLeaf;
  u16 minLeaf;
  u32 pageSize;
  u32 usableSize;
  int nTransaction;
  u32 nPage;
  void *pSchema;
  void (*xFreeSchema)(void*);
  sqlite3_mutex *mutex;
  Bitvec *pHasContent;
#ifndef SQLITE_OMIT_SHARED_CACHE
  int nRef;
  BtShared *pNext;
//  BtLock *pLock;
//  Btree *pWriter;
//  u8 isExclusive;
//  u8 isPending;
#endif
//  u8 *pTmpSpace;
    u16 KVdbid;
};

typedef struct CellInfo CellInfo;

// struct modified from SQLite
struct CellInfo {
  u16 valid;
  u8 *pCell;
  u64 nKey;
  // u32 nData;
  // u32 nPayload;
  u16 nHeader;
  u16 nSize;
};


// struct modified from SQLite
struct BtCursor {
  /* note: *A* = filled on initialization
           *B* = filled by bskipParseCells() or btreeParseCells()
           *C* = filled by bskipChangeNode() or btreeChangeNode()
  */
  Btree *pBtree;               /* *A* The Btree to which this cursor belongs */
  BtShared *pBt;               /* *A* The BtShared this cursor points to */
  BtCursor *pNext, *pPrev;     /* *A* Forms a linked list of all cursors */
  struct KeyInfo *pKeyInfo;    /* *A* Argument passed to comparison function */
  u64 rootCid;                 /* *A* cid of root */
  //Pgno pgnoRoot;             /* The root page of this tree. MKA: replaced with rootId */
  sqlite3_int64 cachedRowid;   /* Next rowid cache.  0 means not valid */
#if (INDEX_STRUCTURE == IS_SKIPLIST)
  CellInfo info;               /* Parse of the cell we are pointing at */
#endif
  i64 savenKey;                /* Size of pKey, or last integer key */
  void *savepKey;              /* Saved key at cursor's last known position */
  int skipNext;                /* If negative, make Prev() a no-op. If positive, make Next() a no-op */
  int cursorFaultError;        /* error code when eState==CURSOR_FAULT; undefined otherwise */
  u8 intKey;                   /* *C* whether key is integer */
  u8 wrFlag;                   /* *A* True if writable */
#if (INDEX_STRUCTURE == IS_SKIPLIST)
  u8 atLast;                   /* Cursor pointing to the last entry */
#endif
/*  u8 validNKey;              /* True if info.nKey is valid */
  u8 eState;                   /* One of the CURSOR_XXX constants (see below) */
/*#ifndef SQLITE_OMIT_INCRBLOB*/
/*  Pgno *aOverflow;           /* Cache of overflow page locations */
/*  u8 isIncrblobHandle;       /* True if this cursor is an incr. io handle */
/*#endif */
  /************* offsets from here below are not zeroed by sqlite3BtreeCursorZero */
#if (INDEX_STRUCTURE == IS_SKIPLIST)
  u64 currOid;                 /* *C* oid of current node */
  u32 currIndex;               /* *C* index within current node */
  BSkiplistNode Node;          /* *C* buffer with content of node */
  u8 cellDirect[9];            /* reserved cell for direct seek optimization */
  Oid prevPtrs[MAX_POINTERS];  /* previous pointers to current node, set by MovetoUnpacked */
  u16 prevPtrsValid;           /* 0 = prevPtrs invalid (due to next() or prev() that changed node)
                                  1 = prevPtrs valid from node's height and above (normal case)
                                  2 = prevPtrs valid at all heights (set by movetounpacked with special flag) */
#elif (INDEX_STRUCTURE == IS_BTREE)
  u64 currOid;                 /* *C* oid of current node */
  u32 currIndex;               /* *C* index within current node */
  BtreeNode Node;              /* *C* buffer with content of node */
  u8 cellDirect[9];            /* reserved cell for direct seek optimization */
#elif (INDEX_STRUCTURE == IS_DTREE)
  int            levelLeaf;                   // level of leaf node; set after we traverse until leaf
  DTreeNode      node[DTREE_MAX_LEVELS];      // current path information; set as we traverse the tree
  u8             nodetype[DTREE_MAX_LEVELS];  // type of each node. 0=approx (from cache), 1=real (from TKVS)
  i32            nodeIndex[DTREE_MAX_LEVELS]; // index within cells of node at each level
  i64            directIntKey;                // when eState = CURSOR_DIRECT, integer key of direct cell
#endif
  Ptr<Valbuf> data;            /* data part of cursor:
                                   if !intKey: always NULL
                                   if intKey: NULL if not loaded yet, otherwise pointer to buffer */
};

