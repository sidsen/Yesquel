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

/*
 * configaction.c
 *
 * Actions called by bison parser to build configuration data
 */

#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

#include "configaction.h"
#include "util.h"

int ConfigAction::setStripeSize(int size) 
{
	LOCALCONTIGUOUSBYTES = size;
	return 0;
}

int ConfigAction::setNumberMemnodes(int nmemnodes){
  if (config->numberMemnodes != 0){
    fprintf(stderr, "Number of memory nodes set twice\n");
    err = 1;
    return -1;
  }
  if (nmemnodes <= 0){
    fprintf(stderr, "Number of memory nodes cannot be nonpositive number %d\n",
	    nmemnodes);
    err = 1;
    return -1;
  }
  
  config->numberMemnodes = nmemnodes;
  config->memnodes = new MemnodeConfig[nmemnodes];
  config->backupnodes = new BackupnodeConfig[nmemnodes];
  return 0;
}

int ConfigAction::setMemnodeSize(int memnodesize){
  if (config->memnodeSize != 0){
    fprintf(stderr, "Memory node size set twice\n");
    err = 1;
    return -1;
  }
  if (memnodesize <= 0){
    fprintf(stderr, "Memory node size cannot be nonpositive number %d\n",
	    memnodesize);
    err = 1;
    return -1;
  }
  
  config->memnodeSize = memnodesize * 1024;
  return 0;
}

int ConfigAction::setAsyncTransactions(bool val){
  config->asyncTransactions = val;
  return 0;
}

int ConfigAction::setHasBackup(bool val){
  config->hasBackup = val;
  return 0;
}

int ConfigAction::setHasDiskLog(bool val){
  config->hasDiskLog = val;
  return 0;
}

int ConfigAction::setHasDiskImage(bool val){
  config->hasDiskImage = val;
  return 0;
}

int ConfigAction::setPidFile(char *file){
  config->pidFile = strdup(file);
  return 0;
}

int ConfigAction::setFastOneNodeTransactions(bool val){
  config->fastOneNodeTransactions = val;  
  return 0;
}

int ConfigAction::setReadOnlyTransactions(bool val){
  config->readOnlyTransactions = val;  
  return 0;
}

int ConfigAction::setSlowTransactions(int val){
  config->slowTransactions = val;  
  return 0;
}

int ConfigAction::addMemnode(int memnode, MemnodeConfig *d){
  int retval = 0;
  if (config->memnodes == 0){
    fprintf(stderr, "Must set number of memory nodes before defining them\n");
    err = 1;
    retval = -1;
  }
  else if (memnode >= (int)config->numberMemnodes){
    fprintf(stderr, "Memory node %d out of range\n", memnode);
    err = 1;
    retval = -1;
  }
  else { // all ok
    config->memnodes[memnode] = *d;
    config->memnodes[memnode].memnode = memnode;
    config->memnodes[memnode].ipaddress = resolvename(d->host);
    if (config->memnodes[memnode].ipaddress == 0){
      fprintf(stderr, "Cannot resolve host name %s\n", d->host);
      err = 1;
      retval = -1;
    }
  }
  
  delete d;
  return retval;
}

int ConfigAction::addBackupnode(int memnode, BackupnodeConfig *d){
  int retval = 0;
  if (config->memnodes == 0){
    fprintf(stderr, "Must set number of memory nodes before defining backups\n");
    err = 1;
    retval = -1;
  }
  else if (memnode >= (int)config->numberMemnodes){
    fprintf(stderr, "Backup node %d out of range\n", memnode);
    err = 1;
    retval = -1;
  } else {
    config->backupnodes[memnode] = *d;
    config->backupnodes[memnode].memnode = memnode;
    config->backupnodes[memnode].ipaddress = resolvename(d->host);
    if (config->backupnodes[memnode].ipaddress == 0){
      fprintf(stderr, "Cannot resolve host name %s\n", d->host);
      err = 1;
      retval = -1;
    }
  }

  delete d;
  return retval;
}



int ConfigAction::start(const char *filename){
  extern FILE *yyin;
  extern int yyparse(void);
  FILE *f;
  int res;
  int i;

  f = fopen(filename, "r");
  if (!f){ err = 1; return err; }
  err = 0;
  
  yyin = f;
  parser_ca = this;
  res = yyparse();
  if (res) goto error;
  if (config->numberMemnodes == 0){
    fprintf(stderr, "Must specify number of memory nodes\n");
    goto error;
  }
  if (config->memnodeSize == 0){
    fprintf(stderr, "Must specify memory node size\n");
    goto error;
  }
  for (i=0; i < (int)config->numberMemnodes; ++i){
    if (config->memnodes[i].memnode == -1){
      fprintf(stderr, "Must specify memory node%d\n", i);
      goto error;
    }
    assert(config->memnodes[i].ipaddress != 0);
    assert(config->memnodes[i].filename != 0);
  }
  fclose(f);
  return 0;
 error:
  err = 1;
  fclose(f);
  return err;
}

extern class ConfigAction *ca;
  
