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
#include "diskstorage.h"
#include "../gaiarpcauxfunc.h"

DiskStorage::DiskStorage(char *diskstoragepath){
  DiskStoragePathLen = (int) strlen(diskstoragepath);
  DiskStoragePath = new char[DiskStoragePathLen+1];
  strcpy(DiskStoragePath, diskstoragepath);
  if (Makepath(DiskStoragePath))
    printf("Warning: directory %s does not exist and cannot be created. Files will not be written\n", DiskStoragePath);
}

char *DiskStorage::getFilename(COid& coid){
  char *retval;

  retval = new char[DiskStoragePathLen+32+3];  // 16 chars for cid, 16 chars for oid, 1 for ., 1 for /, 1 for null
  sprintf(retval, "%s/%I64x.%I64x", DiskStoragePath, coid.cid, coid.oid);
  return retval;
}

char *DiskStorage::searchseparator(char *name){
  if (name[0] == 0) return name;
  if (name[1] == 0) return name+1;
  ++name;
  while (1){
    if (name[0]==0) break;
    if (name[0] == '/' && name[-1] != ':') break;
    if (name[0] == '\\' && name[-1] != ':'){
      if (name[1] != '\\') break;
      ++name; // skip over double \ in name
    }
    ++name;
  }
  return name;
}

// creates directories in path if they don't exit
int DiskStorage::Makepath(char *dirname){
  char *str, *ptr;
  int len;
  char save;
  struct __stat64 buf;
  int res;
  size_t outlen;
  int retval=0;
  wchar_t *wname;

  len = (int) strlen(dirname);
  str = new char[len+1];
  strcpy(str, dirname);
  ptr = str;
  do {
    ptr = searchseparator(ptr);
    save = *ptr;
    *ptr = 0; // make a path prefix

    // check if file exists
    res = _stat64(str, &buf);
    if (res != 0){
      len = (int)strlen(str);
      wname = new wchar_t[len+1];
      mbstowcs_s(&outlen, wname, len+1, str, _TRUNCATE);
      res = CreateDirectory(wname, 0);
      delete [] wname;

      if (res == 0){ retval=1; goto exit; }
    }

    // put back separator
    *ptr = save;
    ++ptr;
  } while (save);

  exit:
  delete [] str;
  return retval;
}

COid DiskStorage::FilenameToCOid(char *filename){
  char *ptr;
  COid coid;
  ptr = strtok(filename, ".");
  sscanf(ptr, "%I64x", &coid.cid);
  ptr = strtok(0, ".");
  sscanf(ptr, "%I64x", &coid.oid);
  return coid;
}

// read coid from current position of file f
int DiskStorage::readCOidFromFile(FILE *f, COid coid, Ptr<TxInfoCoid> &ticoid){
  int res, type, len, lenkeyinfo;
  TxInfoCoid *ticoidptr=0;
  char *keyinfobuf, *ptr;

  // read type
  res = (int)fread((void*) &type, 1, sizeof(int), f);
  if (res != sizeof(int)) goto error;

  if (type==0){ // regular value
    // read length
    res = (int)fread((void*) &len, 1, sizeof(int), f); if (res != sizeof(int)) goto error; 
    // read data
    TxWriteItem *twi = new TxWriteItem;
    twi->coid = coid;
    twi->len = len;
    twi->buf = (char*) malloc(len);
    twi->rpcrequest = 0;
    twi->alloctype = 1; // allocated via malloc
    res = (int)fread((void*) twi->buf, 1, len, f); if (res != len) goto error;
    ticoidptr = new TxInfoCoid(twi);
  } else { // super value
    TxWriteSVItem *twsvi = new TxWriteSVItem;
    // read top part of TxWriteSVItem
    len = offsetof(TxWriteSVItem, attrs);
    res = (int)fread((void*) twsvi, 1, len, f); if (res < len) goto error;
    // read attrs
    twsvi->attrs = new u64[twsvi->nattrs];
    len = twsvi->nattrs * sizeof(u64);
    res = (int)fread(twsvi->attrs, 1, len, f); if (res < len) goto error;
    // read Keyinfo len
    res = (int)fread((void*) &lenkeyinfo, 1, sizeof(int), f); if (res != sizeof(int)) goto error; 
    // read KeyInfo
    keyinfobuf = new char[lenkeyinfo]; assert(keyinfobuf);
    res = (int)fread((void*) keyinfobuf, 1, lenkeyinfo, f); if (res != lenkeyinfo) goto error; 
    ptr = keyinfobuf;
    twsvi->pki = demarshall_keyinfo(&ptr);
    assert(ptr-keyinfobuf == lenkeyinfo);
    delete [] keyinfobuf;

    // read length of all celloids
    int lencelloids;
    res = (int)fread(&lencelloids, 1, sizeof(int), f); if (res < sizeof(int)) goto error;
    // read number of celloids
    int ncelloids;
    res = (int)fread(&ncelloids, 1, sizeof(int), f); if (res < sizeof(int)) goto error;
    // read celloids
    char *celloids = new char[lencelloids];
    res = (int)fread(celloids, 1, lencelloids, f); if (res < ncelloids){ delete [] celloids;  goto error; }
    // deserialize cells

    CelloidsToListCells(celloids, ncelloids, twsvi->celltype, twsvi->cells, &twsvi->pki);
    delete [] celloids;
    ticoidptr = new TxInfoCoid(twsvi);
  }

  ticoid = ticoidptr;
  return 0;
 error:
  return -1;
}

// write coid at current position of file f
int DiskStorage::writeCOidToFile(FILE *f, Ptr<TxInfoCoid> ticoid){
  int res, len, type;
  char *keyinfo;
  TxWriteItem *twi = ticoid->Writevalue;
  TxWriteSVItem *twsvi = ticoid->WriteSV;

  assert(twi&&!twsvi || !twi&&twsvi);
  assert(ticoid->Litems.size()==0);
  if (twi) type = 0;
  else type = 1;

  // write type
  res = (int)fwrite((void*) &type, 1, sizeof(int), f); 
  if(res!=sizeof(int)) goto error;

  if (type==0){ // regular value
    // write length
    res = (int)fwrite((void*) &twi->len, 1, sizeof(int), f); if (res != sizeof(int)) goto error;
    // write data
    res = (int)fwrite((void*) twi->buf, 1, twi->len, f); if (res != twi->len) goto error;
  } else { // super value
    // write top part of TxWriteSVItem
    len = offsetof(TxWriteSVItem, attrs);
    res = (int)fwrite((void*) twsvi, 1, len, f); if (res != len) goto error;
    // write attrs
    len = twsvi->nattrs * sizeof(u64);
    res = (int)fwrite((void*) twsvi->attrs, 1, len, f); if (res != len) goto error;
    // write KeyInfo len
    keyinfo = marshall_keyinfo_onebuf(twsvi->pki, len);
    res = (int)fwrite((void*) &len, 1, sizeof(int), f); if (res != sizeof(int)) goto error;
    // write Keyinfo
    res = (int)fwrite((void*) keyinfo, 1, len, f); if (res != len) goto error;
    // write length of celloids
    int ncelloids;
    char *celloids = twsvi->GetCelloids(ncelloids, len); assert(len >= 0);
    res = (int)fwrite((void*) &len, 1, sizeof(int), f); if (res != sizeof(int)) goto error;
    // write number of celloids
    res = (int)fwrite((void*) &ncelloids, 1, sizeof(int), f); if (res != sizeof(int)) goto error;
    // write celloids
    res = (int)fwrite((void*) celloids, 1, len, f); if (res != len) goto error;
  }
  return 0;
 error:
  return -1;
}

int DiskStorage::readCOid(COid& coid, int len, Ptr<TxInfoCoid> &ticoid, Timestamp& version){
  int res, retval=0;
  FILE *f=0;

  char *name = getFilename(coid);

  f = fopen(name, "rb");
  if (f == NULL) goto error; // set to -1 to return error if file does not exist

  res = (int)fread((void*) &version, 1, sizeof(Timestamp), f);
  if (res != sizeof(Timestamp)) goto error;

  res = readCOidFromFile(f, coid, ticoid);

 end:
  if (f) fclose(f);

  delete [] name;

  return retval;
 error:
  retval = -1;
  goto end;
}

// Write an object id to disk.
int DiskStorage::writeCOid(COid& coid, Ptr<TxInfoCoid> ticoid, Timestamp version){
  int retval, res;
  FILE *f=0;
  int type;
  TxWriteItem *twi=ticoid->Writevalue;
  TxWriteSVItem *twsvi=ticoid->WriteSV;
  assert(ticoid->Litems.size() == 0);
  retval = 0;

  assert(twi&&!twsvi || !twi&&twsvi); // exactly one must be non-zero
  if (twi) type=0;
  else type=1;

  char *name = getFilename(coid);

  f = fopen(name, "r+b");
  if (f == NULL){
    f = fopen(name, "wb");
    if (f == NULL){ retval = -1; goto end; }
  }

  res = (int)fwrite((void*) &version, 1, sizeof(Timestamp), f); 
  if (res != sizeof(Timestamp)){ retval = -1; goto end; }
  res = writeCOidToFile(f, ticoid); if (res) retval = -1;

  end:
  if (f) fclose(f);
  delete [] name;
  return retval;
}

int DiskStorage::getCOidSize(COid& coid){
  char *name = getFilename(coid);
  struct __stat64 buf;
  int res;
  int retval;

  res = _stat64(name, &buf);
  if (res != 0) retval = -1;
  else {
    retval = (int)buf.st_size;
    assert(retval >= sizeof(Timestamp));
    retval -= sizeof(Timestamp);
  }

  delete [] name;
  return retval;
}
