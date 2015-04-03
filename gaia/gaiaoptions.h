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

// gaiaoptions.h

#ifndef _GAIAOPTIONS_H
#define _GAIAOPTIONS_H

//#define GAIA_OCC            // if defined, do optimistic concurrency control (check readsets and writesets on commit and
                            // abort if any object changed)
//#define GAIA_NONCOMMUTATIVE // if set, updates to the same object always conflict (no operations are commutative)


// debug.h
//#define DEBUGRELEASE   // define this to enable debug printfs in release mode
#ifndef NDEBUG
#define DEBUG          // define this to enable debug printfs in debug mode
#endif
//#define DEBUGMEMORY    // define this to enable memory debugging
#define GAIADEBUG_ENV_VAR      "GAIADEBUGLOGFILE" // environment variable with path to gaia debug log file
#define GAIADEBUG_DEFAULT_FILE "debuglog.txt" // default debug log file if environment variable is not defined

// storage server
#define GAIA_DEFAULT_CONFIG_FILENAME "config.txt"
#define SERVER_DEFAULT_PORT 11223

#define SKIP_LOOIM_LOCKS    // do not lock looim. Should be used only if WORKERTHREADS is 1

//#define TCPDATAGRAM_LIGHT // define to use the non-polling RPC implementation using TCP
#define SENDTHREADS 1
#ifndef NDEBUG
#define WORKERTHREADS 1     // number of worker threads for storage server
#else
#define WORKERTHREADS 1     // number of worker threads for storage server
#endif
#define SERVERTHREADS 8     // number of server threads for tcpdatagram-light
//#define GAIA_DESTROY_MARK   // whether to write a mark on certain objects being destroyed
#define PENDINGTX_HASHTABLE_SIZE 101 // size of hash table for pending transactions. Each hash table bucket consists of
                                     // a skiplist. The hash table is mostly useful for improving concurrency since
                                     // at times an entire bucket is locked.
//#define DISABLE_ONE_PHASE_COMMIT     // if set, avoids using one-phase commit for transactions that affects only one server
//#define DISABLE_DELRANGE_DELRANGE_CONFLICTS // if set, delranges never conflict with delranges, otherwise they always do
                                            // I believe conflicting is needed for correctness of Dtree algorithm (TO DO: check this)
#define LOG_CHECKPOINT_MIN_ITEMS         15  // store checkpoint in log if find at least this many items
#define LOG_CHECKPOINT_MIN_ADDITEMS      10  // store checkpoint if log if find at least this many add items
#define LOG_CHECKPOINT_MIN_DELRANGEITEMS 1   // store checkpoint in log if find at least this many delrange items

// grpctcp.h
#define RPCServerMsGCPeriod 2000  // period in ms for GCing result cache
#define RPCServerMsCacheExpire 120000  // when to expire result cache entry in ms
//#define RPCServerMsCacheExpire 4000  // when to expire result cache entry in ms
// When to give up on retransmission in ms
// It should be much smaller than RPCServerMsCacheExpire, otherwise
// server may execute request multiple times, as client keeps
// retransmitting while server has thrown 
// away cached result from previous execution). Also, if it is not
// much smaller than server may start resending response
// reacting to client retransmission while the buffer
// gets GCed, which would cause a crash. 20s smaller should be enough
#define RPCClientMsRetransmitExpire 100000
//#define RPCClientMsRetransmitExpire 1500
#define OUTSTANDINGREQUESTS_HASHTABLE_SIZE 101 // hash table for outstanding requests
//#define SERVER_RESULTCACHE_HASHTABLE_SIZE 65003 // hash table for server result cache
#define SERVER_RESULTCACHE_HASHTABLE_SIZE 16381 // hash table for server result cache

// udpfrag.h
// maximum payload size to send in a UDPFRAG message. Actual payload
// in UDP packet will be slightly larger due to headers in the UDP layer
// (req, xid, flags) and UDPFRAG headers
#define UDPFRAG_SENDSIZE 20
#define FRAG_HASHTABLE_SIZE 1009 // size of hashtable that holds pending fragments
#define UDPFRAG_EXPIRE_FRAGMENT_MS 4000 // expiration time for message fragments in ms
#define UDPFRAG_GC_PERIOD_MS 2000 // period of garbage collector for message fragments

// tcpdatagram.h
//#define TCP_RECLEN_DEFAULT 200   // size of buffers to receive data
//#define TCP_RECLEN_DEFAULT 15300   // size of buffers to receive data
#define TCP_RECLEN_DEFAULT 64000   // size of buffers to receive data
//#define TCP_RECLEN_DEFAULT 65536   // size of buffers to receive data
#define TCP_MAX_RECEIVE_THREADS 1 // max # of threads to receive data 
//#define TCP_MAX_RECEIVE_THREADS 2 // # of threads to receive data 
#define TCP_MAX_CLIENT_THREADS 8
#define TCPDATAGRAM_MAX_SEND_SIZE 49152 // [NOTE: the code that enforces this option is commented out in tcpdatagram.cpp]
                                        // max # of bytes to send at a time. Actually, a send bigger than
                                        // this is possible if (a) previously fewer bytes than TCPDATAGRAM_MAX_SEND_SIZE
                                        // were buffered, and (b) the next deserialized RPC/reply goes beyond TCPDATAGRAM_MAX_SEND_SIZE.
                                        // So this is not a hard limit. However, it will ensure that at most one RPC/reply
                                        // goes past the TCPDATAGRAM_MAX_SEND_SIZE limit

// tcpdatagram-light.h (in addition to parameters for tcpdatagram.h)
#define TCP_LIGHT_MAX_RECEIVE_THREADS 4

// grpc.h
#define RPC_UDP_CLIENT_RECEIVE_THREADS 1 // number of threads to receive RPC responses (using UDP)

// logmem.h
#define LOG_STALE_GC_MS 3000  // entries older than this value, in ms, will be deleted from the in-memory log
#define COID_CACHE_HASHTABLE_SIZE 10009 // hashtable size for coid log
//#define COID_CACHE_HASHTABLE_SIZE 4000000 // hashtable size for coid log
#define COID_CACHE_HASHTABLE_SIZE_LOCAL 4001 // hashtable size for local TKVS
#define FLUSH_FILENAME "kv.dat" // name of file where to "save" storage state

// disklog-win.h
//#define SKIPFFLUSH // whether to skip synchronous flushing to disk
#define SKIPLOG // whether to skip logging to disk altogether
// The write buffer is used to gather writes when the worker thread
// needs to flush data to disk. This buffer is used because there is no
// gather write in Windows.
#define WRITEBUFSIZE 64*1024*1024

#if defined(SKIP_LOOIM_LOCKS) && WORKERTHREADS != 1
#error SKIP_LOOIM_LOCKS should be used only when WORKERTHREADS is 1
#endif

#endif
