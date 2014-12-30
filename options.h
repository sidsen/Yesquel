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

#ifndef _OPTIONS_H
#define _OPTIONS_H

#define DTREE_SPLIT_LOCATION 2 // 1=client, 2=split server, 3=split server synchronous
#define DTREE_SPLIT_CLIENT_MAX_RETRIES 100 // max # of times for client to retry split before giving up

//#define MULTI_SPLITTER         // if defined, lets many splitters split the same tree. Currently
                               // leaf nodes get split by different splitters, but higher levels
                               // always get split by the same splitter, to avoid contention

#define ALT_MULTI_SPLITTER  // alternative multisplitter where client hashes based on oid not cid


//#define NOGAIA

//#define DEBUGMEMORY

//#define USEKVLOG
//#define USELOG
//#define USESQLITELOG
//#define NODIRECTSEEK
//#define NOLOOKAHEAD
//#define NOSPLITCELL // relevant for distributed skiplist only

#define COUNTKVOPS

// defining constants for the various data structures
#define IS_SKIPLIST 1   // skiplist
#define IS_BTREE 2      // btree
#define IS_DTREE 3      // dtree
#define INDEX_STRUCTURE IS_DTREE // data structure to use for indexes

// definitions for btreeaux.h
#define BTREE_DEFAULT_MAX_CELLS_PER_NODE 128 // default max cells per b-skiplist node 
#define BTREE_TABLE1_MAX_CELLS_PER_NODE 128        // max cells per bskiplist-node for a database's table1
#define MAX_BTREE_LEVELS 15 // max # of levels in btree. This is currently used in BtCursor.prevPtrs only
                     // to record the parent nodes while traversing the Btree. This information is
                     // not used for anything yet, so if necessary it's possible to remove
                     // an upper bound on the number of levels.

// definitions for dtreeaux.h
#define DTREE_MAX_LEVELS 14 // max # of levels in Dtree
#define DTREE_ROOT_OID   0 // oid of root node
// NORMALNODE
//#define DTREE_SPLIT_SIZE 100 // # of cells above which to split a node (must be at least 2 since we cannot split a node with 2 cells)
//#define DTREE_SPLIT_SIZE_BYTES 15100 // size above which to split a node
// BIGNODE
//#define DTREE_SPLIT_SIZE 200 // # of cells above which to split a node (must be at least 2 since we cannot split a node with 2 cells)
//#define DTREE_SPLIT_SIZE_BYTES 64000 // size above which to split a node
// SMALLNODE
#define DTREE_SPLIT_SIZE 50 // # of cells above which to split a node (must be at least 2 since we cannot split a node with 2 cells)
#define DTREE_SPLIT_SIZE_BYTES 8000 // size above which to split a node
//#define DTREE_SPLIT_DEFER_TS  // if set, defers start timestamp of split. Probably not needed using new commit protocol.
//#define DTREE_SPLIT_TSOFFSET 100 // splitter will have a start timestamp this much into the past (so its reads will not abort others)
                                 // probably subsumed by defer timestamp technique, which is subsumed by new commit protocol
#define DTREE_AVOID_DUPLICATE_INTERVAL 1000 // avoids splitting the same item within this time interval, in ms
//#define DTREE_NOFIRSTNODE // for new dtrees, do not include a dummy first node

// definitions for skipaux.h
#define BSKIP_DEFAULT_MAX_CELLS_PER_NODE 128 // default max cells per b-skiplist node 
#define BSKIP_TABLE1_MAX_CELLS_PER_NODE 128        // max cells per bskiplist-node for a database's table1
#define MAX_POINTERS 32  // max # of pointers in b-skiplist 

// definitions for splitter-server.h
//#define ALL_SPLITS_UNCONDITIONAL // if set, splitter server always tries to split a node, even if a recent identical request was made

// definitions for sqlite
#define SQLITE_ENABLE_COLUMN_METADATA
//#define SQLITE_OMIT_SHARED_CACHE

#ifdef NDEBUG
#define NODEBUG
#endif

#ifdef DEBUGMEMORY
#ifndef _DEBUG
#error Must define _DEBUG
#endif

#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>

#define DEBUG_NEW new(_NORMAL_BLOCK, __FILE__, __LINE__)
#define new DEBUG_NEW
#endif

#ifndef GLOBALRWVAR
#ifdef MIDORI
//#define GLOBALRWVAR __declspec(thread)
#define GLOBALRWVAR
#define GLOBALROVAR
#else
#define GLOBALRWVAR
#define GLOBALROVAR
#endif
#endif

#endif
