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
// diskstorage.cpp
//

#include "stdafx.h"
#include "../tmalloc.h"
#include "../debug.h"
#include "../storageserver/diskstorage.h"
#include "../storageserver/pendingtx.h"

DiskStorage::DiskStorage(char *diskstoragepath){}
char *DiskStorage::getFilename(COid& coid){ return 0; }
char *DiskStorage::searchseparator(char *name){ return 0; }
int DiskStorage::Makepath(char *dirname){return 0; }
COid DiskStorage::FilenameToCOid(char *filename){ COid d; d.cid=d.oid=0; return d; }
int DiskStorage::readCOidFromFile(FILE *f, COid coid, Ptr<TxInfoCoid> &ticoid){ return 0; }
int DiskStorage::writeCOidToFile(FILE *f, Ptr<TxInfoCoid> ticoid){ return 0; }
int DiskStorage::readCOid(COid& coid, int len, Ptr<TxInfoCoid> &ticoid, Timestamp& version){ return -1; }
int DiskStorage::writeCOid(COid& coid, Ptr<TxInfoCoid> ticoid, Timestamp version){ return 0; }
int DiskStorage::getCOidSize(COid& coid){ return -1; }
