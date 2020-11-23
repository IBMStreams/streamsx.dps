/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2020
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
/*
=====================================================================
Here is the copyright statement for our use of the hiredis APIs:

Hiredis was written by Salvatore Sanfilippo (antirez at gmail) and
Pieter Noordhuis (pcnoordhuis at gmail) and is released under the
BSD license.

Copyright (c) 2009-2011, Salvatore Sanfilippo <antirez at gmail dot com>
Copyright (c) 2010-2011, Pieter Noordhuis <pcnoordhuis at gmail dot com>

redis-plus-plus include files provide wrappers on top of the hiredis library.
This wrapper allows us to use the familiar hiredis APIs in the context of
a Redis cluster (Version 6 and higher) to provide HA facility for automatic
fail-over when a Redis instance or the entire machine crashes.
In addition, it provides the TLS/SSL support for the redis-cluster.
Please note that this redis-plus-plus wrapper supercedes the older 
hiredis-cluster wrapper that we also use in the DPS toolkit. If the Redis server
version is v5 and lower, one may continue to use the older hiredis-cluster in DPS.
If the Redis server version is v6 and higher, it is recommended to use the 
redis-cluster-plus-plus in order to work with both non-TLS and TLS.

The redis-plus-plus wrapper carries the Apache 2.0 copyright as shown in the following line.

A permissive license whose main conditions require preservation of copyright and license notices. 
Contributors provide an express grant of patent rights. Licensed works, modifications, and larger 
works may be distributed under different terms and without source code.
=====================================================================
*/

/*
==================================================================================================================
This CPP file contains the source code for the distributed process store (dps) back-end activities such as
insert, update, read, remove, etc. This dps implementation runs on top of the popular redis-cluster in-memory store.
Redis-Cluster is a simple, but a great open source effort that carries a BSD license.
Thanks to Salvatore Sanfilippo, who created Redis, when he was 30 years old in 2009. What an amazing raw talent!!!
In a (2011) interview, he nicely describes about the reason to start that marvelous project.

http://www.thestartup.eu/2011/01/an-interview-with-salvatore-sanfilippo-creator-of-redis-cluster-working-out-of-sicily/

Redis-Cluster (Version 3 and higher) is a full fledged NoSQL data store with support for complex types such as list,
set, and hash. It also has APIs to perform store commands within a transaction block. Its replication, persistence,
and cluster features are far superior considering that its first release was done only in April/2015. 
As of Nov/2020, Redis is at version 6 with many more enhancements including TLS/SSL. To work with Redis v6 and higher
in cluster mode, DPS toolkit introduces a new redis-cluster-plus-plus option which is implemented in this file using
the popular redis-plus-plus C++ library. (DPS toolkit also supports two other options: "redis" for non-cluster
configuration and "redis-cluster" for cluster mode configuration for Redis v5 and lower versions.)

Any dpsXXXXX native function call made from a SPL composite will go through a serialization layer and then hit
this CPP file. Here, the purpose of such SPL native function calls will be fulfilled using the redis-plus-plus APIs.
After that, the results will be sent to a deserialization layer. From there, results will be transformed using the
correct SPL types and delivered back to the SPL composite. In general, our distributed process store provides
a "global + distributed" in-memory cache for different processes (multiple PEs from one or more Streams applications).
We provide a set of free for all native function APIs to create/read/update/delete data items on one or more stores.
In the worst case, there could be multiple writers and multiple readers for the same store.
It is important to note that a Streams application designer/developer should carefully address how different parts
of his/her application will access the store simultaneously i.e. who puts what, who gets what and at
what frequency from where etc.

There are simple and advanced examples included in the DPS toolkit to test all the features described in the previous
paragraph.
========================================================================================================
*/

#include "RedisClusterPlusPlusDBLayer.h"
#include "DpsConstants.h"

#include <iostream>
#include <unistd.h>
#include <sys/utsname.h>
#include <sstream>
#include <chrono>
#include <cassert>
#include <stdio.h>
#include <time.h>
#include <string>
#include <vector>
#include <streams_boost/lexical_cast.hpp>
#include <streams_boost/algorithm/string.hpp>
#include <streams_boost/algorithm/string/erase.hpp>
#include <streams_boost/algorithm/string/split.hpp>
#include <streams_boost/algorithm/string/classification.hpp>
#include <streams_boost/archive/iterators/base64_from_binary.hpp>
#include <streams_boost/archive/iterators/binary_from_base64.hpp>
#include <streams_boost/archive/iterators/transform_width.hpp>
#include <streams_boost/archive/iterators/insert_linebreaks.hpp>
#include <streams_boost/archive/iterators/remove_whitespace.hpp>
#include <streams_boost/foreach.hpp>
#include <streams_boost/tokenizer.hpp>
// On the IBM Streams application Linux machines, it is a must to install opensssl and
// and openssl-devel RPM packages. The TLS connection logic in the code below will 
// require openssl to be available.
// In particular, it will use the /lib64/libssl.so and /lib64/libcrypto.so libraries 
// that are part of openssl.

using namespace std;
using namespace SPL;
using namespace streams_boost::archive::iterators;

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  RedisClusterPlusPlusDBLayer::RedisClusterPlusPlusDBLayer()
  {
     redis_cluster = NULL;
  } // End of constructor.

  RedisClusterPlusPlusDBLayer::~RedisClusterPlusPlusDBLayer()
  {
     // Delete the cluster connection object.
     delete redis_cluster;
  } // End of destructor.
        
  void RedisClusterPlusPlusDBLayer::connectToDatabase(std::set<std::string> const & dbServers, PersistenceError & dbError)
  {
     SPLAPPTRC(L_DEBUG, "Inside connectToDatabase", "RedisClusterPlusPlusDBLayer");

     // Get the name, OS version and CPU type of this machine.
     struct utsname machineDetails;

     if(uname(&machineDetails) < 0) {
        dbError.set("Unable to get the machine/os/cpu details.", DPS_INITIALIZE_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to get the machine/os/cpu details. " << DPS_INITIALIZE_ERROR, "RedisClusterPlusPlusDBLayer");
        return;
     } else {
        nameOfThisMachine = string(machineDetails.nodename);
        osVersionOfThisMachine = string(machineDetails.sysname) + string(" ") + string(machineDetails.release);
        cpuTypeOfThisMachine = string(machineDetails.machine);
     }

     string redisClusterConnectionErrorMsg = "Unable to initialize the redis-cluster-plus-plus connection context.";
     // Senthil added this block of code on May/02/2017.
     // As part of the Redis configuration in the DPS config file, we now allow the user to specify
     // an optional Redis authentication password as shown below.
     // server:port:RedisPassword:ConnectionTimeoutValue:use_tls
     string targetServerPassword = "";
     string targetServerName = "";
     int targetServerPort = 0;
     int connectionTimeout = 0;
     int useTls = -1;
     string redisClusterCACertFileName = "";
     int connectionAttemptCnt = 0;

     // Get the current thread id that is trying to make a connection to redis cluster.
     int threadId = (int)gettid();

     // We need only one server in the Redis cluster to do the cluster based Redis HA operations.
     for (std::set<std::string>::iterator it=dbServers.begin(); it!=dbServers.end(); ++it) {
        std::string serverName = *it;
        // If the user has configured to use the unix domain socket, take care of that as well.
        if (serverName == "unixsocket") {
           // We will consider supporting this at a later time.
           redisClusterConnectionErrorMsg += " UnixSocket is not supported when DPS is configured with redis-cluster-plus-plus.";
           dbError.set(redisClusterConnectionErrorMsg, DPS_INITIALIZE_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error '" <<
              redisClusterConnectionErrorMsg << "'. " << DPS_INITIALIZE_ERROR, "RedisClusterPlusPlusDBLayer");
              return;
        } else {
           // Redis server name can have port number, password, connection timeout value and 
           // use_tls=1 specified along with it --> MyHost:2345:xyz:5:use_tls
           int32_t tokenCnt = 0;
           // This technique to get the empty tokens while tokenizing a string is described in this URL:
           // https://stackoverflow.com/questions/22331648/boosttokenizer-point-separated-but-also-keeping-empty-fields
           typedef streams_boost::tokenizer<streams_boost::char_separator<char> > tokenizer;
           streams_boost::char_separator<char> sep(
              ":", // dropped delimiters
              "",  // kept delimiters
              streams_boost::keep_empty_tokens); // empty token policy

           STREAMS_BOOST_FOREACH(std::string token, tokenizer(serverName, sep)) {
              tokenCnt++;

              if (tokenCnt == 1) {
                 // This must be our first token.
                 if (token != "") {
                    targetServerName = token;
                 }
              } else if (tokenCnt == 2){
                 // This must be our second token.
                 if (token != "") {
                    targetServerPort = atoi(token.c_str());
                 }
                          
                 if (targetServerPort <= 0) {
                     targetServerPort = REDIS_SERVER_PORT;
                 }
              } else if (tokenCnt == 3) {
                 // This must be our third token.
                 if (token != "") {
                    targetServerPassword = token;
                 }
              } else if (tokenCnt == 4) {
                 // This must be our fourth token.
                 if (token != "") {
                    connectionTimeout = atoi(token.c_str());
                 }

                 if (connectionTimeout <= 0) {
                    // Set it to a default of 3 seconds.
                    connectionTimeout = 3;
                 }
              } else if (tokenCnt == 5) {
                 // This must be our fifth token.
                 if (token != "") {
                    useTls = atoi(token.c_str());
                 }

                 if (useTls < 0) {
                    // Set it to a default of 0 i.e. no TLS needed.
                    useTls = 0;
                 }

                 if (useTls > 0) {
                    // Set it to 1 i.e. TLS needed.
                    useTls = 1;
                 }
              } else if (tokenCnt == 6) {
                 // This must be our sixth token.
                 if (token != "") {
                    // This is a fully qualified Redis cluster CA cert filename.
                    redisClusterCACertFileName = token;
                 } 
              } // End of the long if-else block.
           } // End of the Boost FOREACH loop.

           if (targetServerName == "") {
              // User only specified the server name and no port.
              // (This is the case of server name followed by a : character with a missing port number)
              targetServerName = serverName;
              // In this case, use the default Redis server port.
              targetServerPort = REDIS_SERVER_PORT;
           }

           if (targetServerPort <= 0) {
              // User didn't give a Redis server port.
              // Only a server name was given not followed by a : character.
              // Use the default Redis server port.
              targetServerPort = REDIS_SERVER_PORT;
           }

           // Password is already set to empty string at the time of variable declaration.
           // Because of that, we are good even if the redis configuration string has this field as blank.

           if (connectionTimeout <= 0) {
              // User didn't configure a connectionTimeout field at all. So, set it to 3 seconds.
              connectionTimeout = 3;
           }

           if (useTls < 0) {
              // User didn't configure a useTls field at all, So, set it to 0 i.e. no TLS needed.
              useTls = 0;
           }

           // Use this line to test out the parsed tokens from the Redis configuration string.
           connectionAttemptCnt++;
           string clusterPasswordUsage = "a";

           if (targetServerPassword.length() <= 0) {
              clusterPasswordUsage = "no";
           }

           cout << connectionAttemptCnt << ") ThreadId=" << threadId << ". Attempting to connect to the Redis cluster node " << targetServerName << " on port " << targetServerPort << " with " << clusterPasswordUsage << " password. " << "connectionTimeout=" << connectionTimeout << ", use_tls=" << useTls << ", redisClusterCACertFileName=" << redisClusterCACertFileName  << "." << endl;
                          
           // Current redis cluster version as of Oct/29/2020 is 6.0.9
           // If the redis cluster node is inactive, the following statement may throw an exception.
           // We will catch it, log it and proceed to connect with the next available redis cluster node.
           try {
              // Initialize the connection options with a master node and its port along with
              // other options such as password authentication, TLS/SSL etc.
              ConnectionOptions connection_options;
              connection_options.host = targetServerName;
              connection_options.port = targetServerPort;
              
              if(targetServerPassword.length() > 0) {
                 connection_options.password = targetServerPassword;
              }

              // Set the TCP socket timeout in msec.
              connection_options.socket_timeout = std::chrono::milliseconds(connectionTimeout * 1000);
              // Set the connection timeout in msec.
              connection_options.connect_timeout = std::chrono::milliseconds(connectionTimeout * 1000);

              // Set the desired TLS/SSL option.
              if(useTls == 1) {
                 connection_options.tls.enabled = true;
                 connection_options.tls.cacert = redisClusterCACertFileName;
              }
              
              // Get a redis-plus-plus cluster object for the given connection options.
              // Automatically get other nodes' information. Then, make a (single) connection to
              // every given master node. A single connection here means, we don't want a pool of
              // connections to every master node. 
              redis_cluster = new RedisCluster(connection_options);
           } catch (Error &ex) {
              cout << "Caught an exception connecting to a redis cluster node at " << targetServerName << ":" << targetServerPort << " (" << ex.what() << "). Skipping it and moving on to a next available redis cluster node." << endl;
              // Continue with the next iteration in the for loop.
              continue;
           }
        } // End of if (serverName == "unixsocket")

        if (redis_cluster == NULL) {
           redisClusterConnectionErrorMsg += " Connection error in the createCluster API.";
           cout << "ThreadId=" << threadId << ". Unable to connect to the Redis cluster node " << targetServerName << " on port " << targetServerPort << endl;
        } else {
           // Reset the error string.
           redisClusterConnectionErrorMsg = "";
           string passwordUsage = "a";

           if (targetServerPassword.length() <= 0) {
              passwordUsage = "no";
           }

           cout << "ThreadId=" << threadId << ". Successfully connected with " << passwordUsage << " password to the Redis cluster node " << targetServerName << " on port " << targetServerPort << endl;
           // Break out of the for loop.
           break;
        }
     } // End of the for loop.

     // Check if there was any connection error.
     if (redisClusterConnectionErrorMsg != "") {
        dbError.set(redisClusterConnectionErrorMsg, DPS_INITIALIZE_ERROR);
        SPLAPPTRC(L_ERROR, "Inside connectToDatabase, it failed with an error '" <<
           redisClusterConnectionErrorMsg << "'. " << DPS_INITIALIZE_ERROR, "RedisClusterPlusPlusDBLayer");
        return;
     }

     // We have now made connection to one server in a redis-cluster.
     // Let us check if the global storeId key:value pair is already there in the cache.
     string keyString = string(DPS_AND_DL_GUID_KEY);
     long long exists_result_value = 0;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        exists_result_value = redis_cluster->exists(keyString);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_EXISTS_CMD: ") + exceptionString, DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside connectToDatabase, it failed with a Redis connection error for REDIS_EXISTS_CMD. Exception: " << exceptionString << " " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to check the existence of the dps GUID key. Error=" + exceptionString, DPS_KEY_EXISTENCE_CHECK_ERROR);
        SPLAPPTRC(L_ERROR, "Inside connectToDatabase, it failed. Error=" <<
           exceptionString << ". rc=" << DPS_KEY_EXISTENCE_CHECK_ERROR, "RedisClusterPlusPlusDBLayer");
        return;
     }

     if (exists_result_value == 0) {
        // It could be that our global store id is not there now.
        // Let us create one with an initial value of 0.
        // Redis setnx is an atomic operation. It will succeed only for the very first operator that
        // attempts to do this setting after a redis-cluster server is started fresh. If some other operator
        // already raced us ahead and created this guid_key, then our attempt below will be safely rejected.
        exceptionString = "";
        exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

        try {
           redis_cluster->setnx(keyString, string("0"));
        } catch (const ReplyError &ex) {
           // WRONGTYPE Operation against a key holding the wrong kind of value
           exceptionString = ex.what();
           // Command execution error.
           exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
        } catch (const TimeoutError &ex) {
           // Reading or writing timeout
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
        } catch (const ClosedError &ex) {
           // Connection has been closed.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const IoError &ex) {
           // I/O error on the connection.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const Error &ex) {
           // Other errors
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
        }

        if(exceptionType != REDIS_PLUS_PLUS_NO_ERROR) {
           dbError.set(string("Unable to connect to the redis-cluster server(s). ") + 
              string("Error in REDIS_SETNX_CMD. Exception=") + exceptionString, DPS_CONNECTION_ERROR);
           SPLAPPTRC(L_ERROR, "Inside connectToDatabase, it failed with an error for REDIS_SETNX_CMD. Exception=" << exceptionString << ". rc=" << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
           return;
        }
     } // End of if (exists_result_value == 0)

     SPLAPPTRC(L_DEBUG, "Inside connectToDatabase done", "RedisClusterPlusPlusDBLayer");
  } // End of connectToDatabase.

  uint64_t RedisClusterPlusPlusDBLayer::createStore(std::string const & name,
     std::string const & keySplTypeName, std::string const & valueSplTypeName,
     PersistenceError & dbError)
  {
     SPLAPPTRC(L_DEBUG, "Inside createStore for store " << name, "RedisClusterPlusPlusDBLayer");

     string base64_encoded_name;
     base64_encode(name, base64_encoded_name);

     // Get a general purpose lock so that only one thread can
     // enter inside of this method at any given time with the same store name.
     if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
        // Unable to acquire the general purpose lock.
        dbError.set("Unable to get a generic lock for creating a store with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for an yet to be created store with its name as " <<
        name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "RedisClusterPlusPlusDBLayer");
        // User has to retry again to create this store.
        return 0;
     }

     // Let us first see if a store with the given name already exists.
     //
     // Inside Redis, all our store names will have a mapping type indicator of
     // "0" at the beginning followed by the actual store name.  "0" + 'store name'
     // (See the store layout description documented in the next page.)
     // Additionally, in Redis, store names can have space characters in them.
     string keyString = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
     long long exists_result_value = 0;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        exists_result_value = redis_cluster->exists(keyString);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_EXISTS_CMD: ") + exceptionString + 
           string(" Application code may call the DPS reconnect API and then retry the failed operation. "), 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside createStore, it failed for store " << name << " with a Redis connection error for REDIS_EXISTS_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to check the existence of a store with a name" + name + 
           ". Error=" + exceptionString, DPS_KEY_EXISTENCE_CHECK_ERROR);
        SPLAPPTRC(L_ERROR, "Inside createStore, it failed to check for a store existence. Error=" <<
           exceptionString << ". rc=" << DPS_KEY_EXISTENCE_CHECK_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     if (exists_result_value == 1) {
        // This store already exists in our cache.
        // We can't create another one with the same name now.
        dbError.set("A store named " + name + " already exists", DPS_STORE_EXISTS);
        SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << " that already exists. " << DPS_STORE_EXISTS, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     // If we reach here that means, the store doesn't exist at this time.
     // We can go ahead and try to create that new store.
     // Create a new store.
     // At first, let us increment our global dps_guid to reserve a new store id.
     keyString = string(DPS_AND_DL_GUID_KEY);
     long long incr_result_value = 0;
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     uint64_t storeId = 0;

     try {
        incr_result_value = redis_cluster->incr(keyString);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_INCR_CMD: ") + exceptionString + 
           string(" Application code may call the DPS reconnect API and then retry the failed operation. "), 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside createStore, it failed for store " << name << " with a Redis connection error for REDIS_INCR_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to get a unique store id for a store with a name" + name + 
           ". Error=" + exceptionString, DPS_GUID_CREATION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside createStore, it failed to create a unique store id for a store with a name " << name << ". Error=" << exceptionString << ". rc=" << DPS_GUID_CREATION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     storeId = incr_result_value;

     /*
     We secured a guid. We can now create this store. Layout for a distributed process store (dps) looks like this.
     1) Create a root entry called "Store Name":  '0' + 'store name' => 'store id'                                                                                
     2) Create "Store Contents Hash": '1' + 'store id' => 'Redis Hash'                                                                                            
        This hash will always have the following three metadata entries:                                                                                       
        * a) dps_name_of_this_store ==> 'store name'                                                                                                     		   
        * b) dps_spl_type_name_of_key ==> 'spl type name for this store's key'                                                                                      
        * c) dps_spl_type_name_of_value ==> 'spl type name for this store's value'                                                                                  
     3) In addition, we will also create and delete custom locks for modifying store contents in (2) above: '4' + 'store id' + 'dps_lock' => 1                   

     4) Create a root entry called "Lock Name":  '5' + 'lock name' ==> 'lock id'    [This lock is used for performing store commands in a transaction block.]    

     5) Create "Lock Info":  '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'lock name' 

     6) In addition, we will also create and delete user-defined locks: '7' + 'lock id' + 'dl_lock' => 1                                                          
     7) We will also allow general purpose locks to be created by any entity for sundry use:  '501' + 'entity name' + 'generic_lock' => 1                         
     */

     //
     // 1) Create the Store Name
     //    '0' + 'store name' => 'store id'
     std::ostringstream value;
     value << storeId;
     keyString = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        redis_cluster->set(keyString, value.str());
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_SET_CMD: ") + exceptionString + 
           string(" Application code may call the DPS reconnect API and then retry the failed operation. "), 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside createStore, it failed for store " << name << " with a Redis connection error for REDIS_SET_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to set a K/V pair in a store with a name" + name + 
           ". Error=" + exceptionString, DPS_STORE_NAME_CREATION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside createStore, it failed to set a K/V pair in a store with a name " << name << ". Error=" << exceptionString << ". rc=" << DPS_STORE_NAME_CREATION_ERROR, "RedisClusterPlusPlusDBLayer");
        // We are simply leaving an incremented value for the dps_guid key in the cache that will never get used.
        // Since it is harmless, there is no need to reduce this number by 1. It is okay that this guid number will remain unassigned to any store.
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     // 2) Create the Store Contents Hash
     // '1' + 'store id' => 'Redis Hash'
     // Every store contents hash will always have these three metadata entries:
     // dps_name_of_this_store ==> 'store name'
     // dps_spl_type_name_of_key ==> 'spl type name for this store's key'
     // dps_spl_type_name_of_value ==> 'spl type name for this store's value'
     //
     // Every store contents hash will have at least three elements in it carrying the actual store name, key spl type name and value spl type name.
     // In addition, inside this store contents hash, we will house the
     // actual key:value data items that user wants to keep in this store.
     // Such a store contents hash is very useful for data item read, write, deletion, enumeration etc.
     // Redis hash has the operational efficiency of O(1) i.e. constant time execution for get, put, and del with any hash size.
     // 2a) Let us create a new store contents hash with a mandatory element that will carry the name of this store.
     // (This mandatory entry will help us to do the reverse mapping from store id to store name.)
     // StoreId becomes the new key now.
     keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + value.str();     
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        redis_cluster->hset(keyString, 
           string(REDIS_STORE_ID_TO_STORE_NAME_KEY),
           base64_encoded_name);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HSET_CMD: ") + exceptionString + 
           string(" Application code may call the DPS reconnect API and then retry the failed operation. "), 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside createStore, it failed for store " << name << " with a Redis connection error for REDIS_HSET_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to create 'Store Contents Hash' in a store with a name" + name + 
           ". Error=" + exceptionString, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside createStore, it failed to create 'Store Contents Hash' in a store with a name " << name << ". Error=" << exceptionString << ". rc=" << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "RedisClusterPlusPlusDBLayer");

        // Delete the previous store name root entry we made.
        keyString = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;

        try {
           redis_cluster->del(keyString);
        } catch (const Error &ex) {
          // It is not good if this one fails. We can't do much about that.
        }

        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }
     
     // 2b) We are now going to save the SPL type names of the key as part of this
     // store's metadata. That will help us in the Java dps API "findStore" to cache the
     // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
     // Add the key spl type name metadata.
     string base64_encoded_keySplTypeName;
     base64_encode(keySplTypeName, base64_encoded_keySplTypeName);  
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        redis_cluster->hset(keyString, 
           string(REDIS_SPL_TYPE_NAME_OF_KEY),
           base64_encoded_keySplTypeName);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HSET_CMD: ") + exceptionString + 
           string(" Application code may call the DPS reconnect API and then retry the failed operation. "), 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside createStore, it failed for store " << name << " with a Redis connection error for REDIS_HSET_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to create 'Store Contents Hash' in a store with a name" + name + 
           ". Error=" + exceptionString, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside createStore, it failed to create 'Store Contents Hash' in a store with a name " << name << ". Error=" << exceptionString << ". rc=" << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "RedisClusterPlusPlusDBLayer");

        // Delete the store contents hash with partial entries we created above.
        try {
           redis_cluster->del(keyString);
        } catch (const Error &ex) {
          // It is not good if this one fails. We can't do much about that.
        }

        // We will also delete the store name root entry we made above.
        keyString = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;

        try {
           redis_cluster->del(keyString);
        } catch (const Error &ex) {
          // It is not good if this one fails. We can't do much about that.
        }
       
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     // 2c) We are now going to save the SPL type names of the value as part of this
     // store's metadata. That will help us in the Java dps API "findStore" to cache the
     // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
     // Add the value spl type name metadata.
     string base64_encoded_valueSplTypeName;
     base64_encode(valueSplTypeName, base64_encoded_valueSplTypeName);
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        redis_cluster->hset(keyString, 
           string(REDIS_SPL_TYPE_NAME_OF_VALUE),
           base64_encoded_valueSplTypeName);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HSET_CMD: ") + exceptionString + 
           string(" Application code may call the DPS reconnect API and then retry the failed operation. "), 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside createStore, it failed for store " << name << " with a Redis connection error for REDIS_HSET_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to create 'Store Contents Hash' in a store with a name" + name + 
           ". Error=" + exceptionString, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside createStore, it failed to create 'Store Contents Hash' in a store with a name " << name << ". Error=" << exceptionString << ". rc=" << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "RedisClusterPlusPlusDBLayer");

        // Delete the store contents hash with partial entries we created above.
        try {
           redis_cluster->del(keyString);
        } catch (const Error &ex) {
          // It is not good if this one fails. We can't do much about that.
        }

        // We will also delete the store name root entry we made above.
        keyString = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;

        try {
           redis_cluster->del(keyString);
        } catch (const Error &ex) {
          // It is not good if this one fails. We can't do much about that.
        }
       
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     // A new store is created now.
     releaseGeneralPurposeLock(base64_encoded_name);
     return 0;
  } // End of createStore.

  uint64_t RedisClusterPlusPlusDBLayer::createOrGetStore(std::string const & name,
     std::string const & keySplTypeName, std::string const & valueSplTypeName,
     PersistenceError & dbError)
  {
     // We will rely on a method above this and another method below this to accomplish what is needed here.
     SPLAPPTRC(L_DEBUG, "Inside createOrGetStore for store " << name, "RedisClusterPlusPlusDBLayer");

     uint64_t storeId = createStore(name, keySplTypeName, valueSplTypeName, dbError);

     if (storeId > 0) {
        // It must be a new store that just got created in the method call we made above.
        return(storeId);
     }

     // Check if any error code is set from the create store method call we made above..
     if ((dbError.hasError() == true) && (dbError.getErrorCode() != DPS_STORE_EXISTS)) {
        // There was an error in creating the store.
        return(0);
     }

     // In all other cases, we are dealing with an existing store in our cache.
     // We can get the storeId by calling the method below and return the result to the caller.
     dbError.reset();
     return(findStore(name, dbError));
  } // End of createOrGetStore.
                
  uint64_t RedisClusterPlusPlusDBLayer::findStore(std::string const & name, PersistenceError & dbError)
  {
     SPLAPPTRC(L_DEBUG, "Inside findStore for store " << name, "RedisClusterPlusPlusDBLayer");

     string base64_encoded_name;
     base64_encode(name, base64_encoded_name);

     // Let us first see if this store already exists.
     // Inside Redis, all our store names will have a mapping type indicator of
     // "0" at the beginning followed by the actual store name.  "0" + 'store name'
     std::string storeNameKey = DPS_STORE_NAME_TYPE + base64_encoded_name;
     long long exists_result_value = 0;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        exists_result_value = redis_cluster->exists(storeNameKey);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_EXISTS_CMD: ") + exceptionString + 
           string(" Application code may call the DPS reconnect API and then retry the failed operation. "), 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside findStore, it failed for store "  << name << " with a Redis connection error for REDIS_EXISTS_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return 0;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to check the existence of a store with a name" + name + 
           ". Error=" + exceptionString, DPS_STORE_EXISTENCE_CHECK_ERROR);
        SPLAPPTRC(L_ERROR, "Inside findStore, it failed to check for a store existence. Error=" <<
           exceptionString << ". rc=" << DPS_STORE_EXISTENCE_CHECK_ERROR, "RedisClusterPlusPlusDBLayer");
        return 0;
     }

     if (exists_result_value == 0) {
        // This store is not there in our cache.
        dbError.set("Store named " + name + " not found.", DPS_STORE_DOES_NOT_EXIST);
        SPLAPPTRC(L_DEBUG, "Inside findStore, it couldn't find a store named " << name << ". " << DPS_STORE_DOES_NOT_EXIST, "RedisClusterPlusPlusDBLayer");
        return 0;
     }

     // It is an existing store.
     // We can get the storeId and return it to the caller.
     string get_result_value = "0";
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        auto val = redis_cluster->get(storeNameKey);
        
        if(val) {
           get_result_value = *val;
        }
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_GET_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside findStore, unable to get the storeId for the storeName "  << name << " with a Redis connection error for REDIS_GET_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return 0;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to get the storeId for the storeName "  + name + 
           ". Error=" + exceptionString, DPS_GET_STORE_ID_ERROR);
        SPLAPPTRC(L_ERROR, "Inside findStore, unable to get the storeId for the storeName "  << name << ". Error=" << exceptionString << ". rc=" << DPS_GET_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        return 0;
     }

     if (get_result_value.length() <= 0) {
        // Requested data item is not there in the cache.
        dbError.set("Unable to get The store id for the store " + name + ".", DPS_DATA_ITEM_READ_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside findStore, it couldn't get the store id for store " << name << ". " << DPS_DATA_ITEM_READ_ERROR, "RedisClusterPlusPlusDBLayer");
        return(0);
     }

     uint64_t storeId = streams_boost::lexical_cast<uint64_t>(get_result_value);
     return(storeId);
  } // End of findStore.
        
  bool RedisClusterPlusPlusDBLayer::removeStore(uint64_t store, PersistenceError & dbError)
  {
     SPLAPPTRC(L_DEBUG, "Inside removeStore for store id " << store, "RedisClusterPlusPlusDBLayer");

     ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     // Ensure that a store exists for the given store id.
     if (storeIdExistsOrNot(storeIdString, dbError) == false) {
        if (dbError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return(false);
     }

     // Lock the store first.
     if (acquireStoreLock(storeIdString) == false) {
        // Unable to acquire the store lock.
        dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "RedisClusterPlusPlusDBLayer");
        // User has to retry again to remove the store.
        return(false);
     }

     // Get rid of these two entries that are holding the store contents hash and the store name root entry.
     // 1) Store Contents Hash
     // 2) Store Name root entry
     //
     uint32_t dataItemCnt = 0;
     string storeName = "";
     string keySplTypeName = "";
     string valueSplTypeName = "";

     if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
        SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        // This is alarming. This will put this store in a bad state. Poor user has to deal with it.
        return(false);
     }

     // Let us delete the Store Contents Hash that contains all the active data items in this store.
     // '1' + 'store id' => 'Redis Hash'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
     string storeContentsHashKey = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        redis_cluster->del(storeContentsHashKey);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_DEL_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside removeStore, it failed for store id " << storeIdString << " with a store name " << storeName << " with a Redis connection error for REDIS_DEL_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to delete a store with an id " + storeIdString + 
           " with a store name " + storeName + 
           ". Error=" + exceptionString, DPS_STORE_REMOVAL_ERROR);
        SPLAPPTRC(L_ERROR, "Inside removeStore, it failed to delete a store with an id " << storeIdString << " with a store name " << storeName << ". Error=" << exceptionString << ". rc=" << DPS_STORE_REMOVAL_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return(false);
     }

     // Finally, delete the StoreName key now.
     string storeNameKey = string(DPS_STORE_NAME_TYPE) + storeName;
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        redis_cluster->del(storeNameKey);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_DEL_CMD2: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside removeStore, it failed for store id " << storeIdString << " with a store name " << storeName << " with a Redis connection error for REDIS_DEL_CMD2. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to delete a store name with an id " + storeIdString + 
           " with a store name " + storeName + 
           ". Error=" + exceptionString, DPS_STORE_REMOVAL_ERROR);
        SPLAPPTRC(L_ERROR, "Inside removeStore, it failed to delete a store name with an id " << storeIdString << " with a store name " << storeName << ". Error=" << exceptionString << ". rc=" << DPS_STORE_REMOVAL_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return(false);
     }

     // Life of this store ended completely with no trace left behind.
     releaseStoreLock(storeIdString);
     return(true);
  } // End of removeStore.

  // This is a lean and mean put operation into a store.
  // It doesn't do any safety checks before putting a data item into a store.
  // If you want to go through that rigor, please use the putSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool RedisClusterPlusPlusDBLayer::put(uint64_t store, char const * keyData, uint32_t keySize,
     unsigned char const * valueData, uint32_t valueSize,
     PersistenceError & dbError)
  {
     SPLAPPTRC(L_DEBUG, "Inside put for store id " << store, "RedisClusterPlusPlusDBLayer");

     // Let us try to store this item irrespective of whether it is
     // new or it is an existing item in the cache.
     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

    // CAUTION: Regular and a faster version of dpsPut will simply create an incorrect store structure when an actual store doesn't exist.
    // Because, faster version of dpsPut doesn't do any safety checks to validate the store existence.
    // If users call dpsPut (faster version) on a non-existing store, that will surely cause all kinds of issues in the
    // back-end data store by creating invalid store structures thereby producing dangling stores. Users should take proper care
    // and call the faster version of the dpsPut API only on existing stores. If they ignore this rule, then the back-end data store
    // will be in a big mess.
    // Ideally, at this point here in this API, we can check for whether the store exists or not. But, that will slow down this
    // faster put API. Hence, we are going to trust the users to call this faster API only on existing stores.

    // In our Redis dps implementation, data item keys can have space characters.
    string data_item_key = string(keyData, keySize);

    // We are ready to either store a new data item or update an existing data item.
    // This action is performed on the Store Contents Hash that takes the following format.
    // '1' + 'store id' => 'Redis Hash'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
    // To support space characters in the data item key, let us base64 encode it.
    string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
    string base64_encoded_data_item_key;
    base64_encode(data_item_key, base64_encoded_data_item_key);
    string exceptionString = "";
    int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

    try {
       redis_cluster->hset(keyString, 
          base64_encoded_data_item_key,
          string((const char*)valueData, (size_t)valueSize));
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HSET_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside put, it failed for store id " << storeIdString << " with a Redis connection error for REDIS_HSET_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to store a data item in the store id " + storeIdString + 
           ". Error=" + exceptionString, DPS_DATA_ITEM_WRITE_ERROR);
        SPLAPPTRC(L_ERROR, "Inside put, it failed for store id " << storeIdString << ". Error=" << exceptionString << ". rc=" << DPS_DATA_ITEM_WRITE_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     return(true);
  } // End of put.

  // This is a special bullet proof version that does several safety checks before putting a data item into a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular put method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool RedisClusterPlusPlusDBLayer::putSafe(uint64_t store, char const * keyData, uint32_t keySize,
     unsigned char const * valueData, uint32_t valueSize,
     PersistenceError & dbError)
  {
     SPLAPPTRC(L_DEBUG, "Inside putSafe for store id " << store, "RedisClusterPlusPlusDBLayer");

     // Let us try to store this item irrespective of whether it is
     // new or it is an existing item in the cache.
     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     // Ensure that a store exists for the given store id.
     if (storeIdExistsOrNot(storeIdString, dbError) == false) {
        if (dbError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return(false);
     }

     // In our Redis dps implementation, data item keys can have space characters.
     string data_item_key = string(keyData, keySize);

     // Lock the store first.
     if (acquireStoreLock(storeIdString) == false) {
        // Unable to acquire the store lock.
        dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "RedisClusterPlusPlusDBLayer");
        // User has to retry again to put the data item in the store.
        return(false);
     }

     // We are ready to either store a new data item or update an existing data item.
     // This action is performed on the Store Contents Hash that takes the following format.
     // '1' + 'store id' => 'Redis Hash'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
     // To support space characters in the data item key, let us base64 encode it.
     string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
     string base64_encoded_data_item_key;
     base64_encode(data_item_key, base64_encoded_data_item_key);

     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        redis_cluster->hset(keyString, 
           base64_encoded_data_item_key,
           string((const char*)valueData, (size_t)valueSize));
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HSET_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside putSafe, it failed for store id " << storeIdString << " with a Redis connection error for REDIS_HSET_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to store a data item in the store id " + storeIdString + 
           ". Error=" + exceptionString, DPS_DATA_ITEM_WRITE_ERROR);
        SPLAPPTRC(L_ERROR, "Inside put, it failed for store id " << storeIdString << ". Error=" << exceptionString << ". rc=" << DPS_DATA_ITEM_WRITE_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return(false);
     }

     releaseStoreLock(storeIdString);
     return(true);
  } // End of putSafe.

  // Put a data item with a TTL (Time To Live in seconds) value into the global area of the Redis DB.
  bool RedisClusterPlusPlusDBLayer::putTTL(char const * keyData, uint32_t keySize,
     unsigned char const * valueData, uint32_t valueSize,
     uint32_t ttl, PersistenceError & dbError, bool encodeKey, bool encodeValue)
  {
     SPLAPPTRC(L_DEBUG, "Inside putTTL.", "RedisClusterPlusPlusDBLayer");

     // In our Redis dps implementation, data item keys can have space characters.
     string data_item_key;

     if (encodeKey == true) {
        base64_encode(string(keyData, keySize), data_item_key);
     } else {
        // Since the key data sent here will always be in the network byte buffer format (NBF), 
        // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
        // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
        // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
        if ((uint8_t)keyData[0] < 0x80) {
           // Skip the first length byte. 
           data_item_key = string(&keyData[1], keySize-1);  
        } else {
           // Skip the five bytes at the beginning that represent the length of the key data.
           data_item_key = string(&keyData[5], keySize-5);
        }
     }

     string data_item_value = "";

     if (encodeValue == false) {
        // Caller wants to store the value as plain string. Do the same thing we did above for the key.
        if ((uint8_t)valueData[0] < 0x80) {
           data_item_value = string((char const *) &valueData[1], valueSize-1);
        } else {
           data_item_value = string((char const *) &valueData[5], valueSize-5); 
        }
     } else {
        data_item_value = string((char const *)valueData, (size_t)valueSize); 
     }

     // We are ready to either store a new data item or update an existing data item with a TTL value specified in seconds.
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        if(ttl > 0) {
           // User wants to do a put with a TTL value.
           redis_cluster->setex(data_item_key,
              streams_boost::lexical_cast<uint64_t>(ttl),
              data_item_value);
        } else {
           // User doesn't want do a put with a TTL value.
           // In this case, we will do a regular Redis set that will keep an unexpired K/V pair.
           redis_cluster->set(data_item_key, data_item_value);
        }
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_PUTTTL_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside putTTL, it failed with a Redis connection error for REDIS_PUTTTL_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to store a data item with TTL. Error=" + exceptionString, DPS_DATA_ITEM_WRITE_ERROR);
        SPLAPPTRC(L_ERROR, "Inside putTTL, it failed to store a K/V pair" << ". Error=" << exceptionString << ". rc=" << DPS_DATA_ITEM_WRITE_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     return(true);
  } // End of putTTL.

  // This is a lean and mean get operation from a store.
  // It doesn't do any safety checks before getting a data item from a store.
  // If you want to go through that rigor, please use the getSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool RedisClusterPlusPlusDBLayer::get(uint64_t store, char const * keyData, uint32_t keySize,
     unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
     SPLAPPTRC(L_DEBUG, "Inside get for store id " << store, "RedisClusterPlusPlusDBLayer");

     // Let us get this data item from the cache as it is.
     // Since there could be multiple data writers, we are going to get whatever is there now.
     // It is always possible that the value for the requested item can change right after
     // you read it due to the data write made by some other thread. Such is life in a global distributed in-memory store.
     // Every data item's key must be prefixed with its store id before being used for CRUD operation in the Redis store.
     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     // In our Redis dps implementation, data item keys can have space characters.
     string base64_encoded_data_item_key;
     base64_encode(string(keyData, keySize), base64_encoded_data_item_key);

     bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
        false, true, valueData, valueSize, dbError);

     if ((result == false) || (dbError.hasError() == true)) {
        // Some error has occurred in reading the data item value.
        SPLAPPTRC(L_DEBUG, "Inside get, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
     }

     return(result);
  } // End of get.

  // This is a special bullet proof version that does several safety checks before getting a data item from a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular get method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool RedisClusterPlusPlusDBLayer::getSafe(uint64_t store, char const * keyData, uint32_t keySize,
     unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
     SPLAPPTRC(L_DEBUG, "Inside getSafe for store id " << store, "RedisClusterPlusPlusDBLayer");

     // Let us get this data item from the cache as it is.
     // Since there could be multiple data writers, we are going to get whatever is there now.
     // It is always possible that the value for the requested item can change right after
     // you read it due to the data write made by some other thread. Such is life in a global distributed in-memory store.
     // Every data item's key must be prefixed with its store id before being used for CRUD operation in the Redis store.
     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     // Ensure that a store exists for the given store id.
     if (storeIdExistsOrNot(storeIdString, dbError) == false) {
        if (dbError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return(false);
     }

     // In our Redis dps implementation, data item keys can have space characters.
     string base64_encoded_data_item_key;
     base64_encode(string(keyData, keySize), base64_encoded_data_item_key);

     bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
        false, false, valueData, valueSize, dbError);

     if ((result == false) || (dbError.hasError() == true)) {
        // Some error has occurred in reading the data item value.
        SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
     }

     return(result);
  }  // End of getSafe.

  // Get a TTL based data item that is stored in the global area of the Redis DB.
  bool RedisClusterPlusPlusDBLayer::getTTL(char const * keyData, uint32_t keySize,
     unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError, bool encodeKey)
  {
     SPLAPPTRC(L_DEBUG, "Inside getTTL.", "RedisClusterPlusPlusDBLayer");

     // Let us get this data item from the cache as it is.
     // Since there could be multiple data writers, we are going to get whatever is there now.
     // It is always possible that the value for the requested item can change right after
     // you read it due to the data write made by some other thread. Such is life in a global distributed in-memory store.

     // In our Redis dps implementation, data item keys can have space characters.
     string data_item_key;

     if (encodeKey == true) {
        base64_encode(string(keyData, keySize), data_item_key);
     } else {
        // Since the key data sent here will always be in the network byte buffer format (NBF), 
        // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
        // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
        // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
        if ((uint8_t)keyData[0] < 0x80) {
           // Skip the first length byte. 
           data_item_key = string(&keyData[1], keySize-1);  
        } else {
           // Skip the five bytes at the beginning that represent the length of the key data.
           data_item_key = string(&keyData[5], keySize-5);
        }
     }

     // Since this is a data item with TTL, it is stored in the global area of Redis and not inside a user created store (i.e. a Redis hash).
     // Hence, we can't use the Redis hash get command. Rather, we will use the plain Redis get command to read this data item.
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     string str_value = "";

     try {
        auto value = redis_cluster->get(data_item_key);

        if(value) {
           str_value = *value;
        }
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_GETTTL_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside getTTL, it failed with a Redis connection error for REDIS_GETTTL_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to get the requested data item with TTL value. Error=" + exceptionString, DPS_DATA_ITEM_READ_ERROR);
        SPLAPPTRC(L_ERROR, "Inside getTTL, it failed to get the data item with TTL. It was either never there to begin with or it probably expired due to its TTL value. Error=" << exceptionString << ". rc=" << DPS_DATA_ITEM_READ_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     if (str_value.length() == 0) {
        // User stored empty data item value in the cache.
        valueData = (unsigned char *)"";
        valueSize = 0;
     } else {
        // We can allocate memory for the exact length of the data item value.
        valueSize = str_value.length();
        valueData = (unsigned char *) malloc(valueSize);

        if (valueData == NULL) {
           // Unable to allocate memory to transfer the data item value.
           dbError.setTTL("Unable to allocate memory to copy the data item value with TTL.", DPS_GET_DATA_ITEM_MALLOC_ERROR);
           valueSize = 0;
           return(false);
        }

        // We expect the caller of this method to free the valueData pointer.
        memcpy(valueData, str_value.c_str(), valueSize);
     }

     return(true);
  } // End of getTTL.

  bool RedisClusterPlusPlusDBLayer::remove(uint64_t store, char const * keyData, uint32_t keySize,
     PersistenceError & dbError) 
  {
     SPLAPPTRC(L_DEBUG, "Inside remove for store id " << store, "RedisClusterPlusPlusDBLayer");

     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     // Ensure that a store exists for the given store id.
     if (storeIdExistsOrNot(storeIdString, dbError) == false) {
        if (dbError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside remove, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return(false);
     }

     // In our Redis dps implementation, data item keys can have space characters.
     string data_item_key = string(keyData, keySize);

     // Lock the store first.
     if (acquireStoreLock(storeIdString) == false) {
        // Unable to acquire the store lock.
        dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "RedisClusterPlusPlusDBLayer");
        // User has to retry again to remove the data item from the store.
        return(false);
     }

     // This action is performed on the Store Contents Hash that takes the following format.
     // '1' + 'store id' => 'Redis Hash'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
     string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
     string base64_encoded_data_item_key;
     base64_encode(data_item_key, base64_encoded_data_item_key);

     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     long long hdel_result_value = 0;

    try {
       hdel_result_value = redis_cluster->hdel(keyString, base64_encoded_data_item_key);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HDEL_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside remove, it failed with a Redis connection error for REDIS_HDEL_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to remove the requested data item from the  store id " + storeIdString + ". Error=" + exceptionString, DPS_DATA_ITEM_DELETE_ERROR);
        SPLAPPTRC(L_ERROR, "Inside remove, it failed while removing the requested data item from the store id " << storeIdString << ". Error=" << exceptionString << ". rc=" << DPS_DATA_ITEM_DELETE_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return(false);
     }

     // Let us ensure that it really removed the requested data item.
     if (hdel_result_value == 0) {
        // Something is not correct here. It didn't remove the data item. Raise an error.
        dbError.set("Unable to remove the requested data item from the store id " + storeIdString + ". REDIS_HDEL returned 0 to indicate the absence of the given field.", DPS_DATA_ITEM_DELETE_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside remove, it failed to remove the requested data item from the store id " << storeIdString << ". REDIS_HDEL returned 0 to indicate the absence of the given field. rc=" << DPS_DATA_ITEM_DELETE_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return(false);
     }

     // All done. An existing data item in the given store has been removed.
     releaseStoreLock(storeIdString);
     return(true);
  } // End of remove.

  // Remove a TTL based data item that is stored in the global area of the Redis DB.
  bool RedisClusterPlusPlusDBLayer::removeTTL(char const * keyData, uint32_t keySize,
     PersistenceError & dbError, bool encodeKey)
  {
     SPLAPPTRC(L_DEBUG, "Inside removeTTL.", "RedisClusterPlusPlusDBLayer");

     // In our Redis dps implementation, data item keys can have space characters.
     string data_item_key;

     if (encodeKey == true) {
        base64_encode(string(keyData, keySize), data_item_key);
     } else {
        // Since the key data sent here will always be in the network byte buffer format (NBF), 
        // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
        // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
        // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
        if ((uint8_t)keyData[0] < 0x80) {
           // Skip the first length byte. 
           data_item_key = string(&keyData[1], keySize-1);  
        } else {
           // Skip the five bytes at the beginning that represent the length of the key data.
           data_item_key = string(&keyData[5], keySize-5);
        }
     }

     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     long long del_result_value = 0;

    try {
       del_result_value = redis_cluster->del(data_item_key);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_DEL_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside removeTTL, it failed with a Redis connection error for REDIS_DEL_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to remove the requested TTL data item from the global store. Error=" + exceptionString, DPS_DATA_ITEM_DELETE_ERROR);
        SPLAPPTRC(L_ERROR, "Inside removeTTL, it failed while removing the requested TTL data item from the global  store. Error=" << exceptionString << ". rc=" << DPS_DATA_ITEM_DELETE_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Let us ensure that it really removed the requested data item.
     if (del_result_value == 0) {
        // Something is not correct here. It didn't remove the data item. Raise an error.
        dbError.set("Unable to remove the requested TTL data item from the global store. REDIS_DEL returned 0 to indicate the absence of the given field.", DPS_DATA_ITEM_DELETE_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside removeTTL, it failed to remove the requested TTL data item from the global store. REDIS_DEL returned 0 to indicate the absence of the given field. rc=" << DPS_DATA_ITEM_DELETE_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // All done. An existing data item with TTL in the global area has been removed.
     return(true);
  } // End of removeTTL.

  bool RedisClusterPlusPlusDBLayer::has(uint64_t store, char const * keyData, uint32_t keySize,
     PersistenceError & dbError) 
  {
     SPLAPPTRC(L_DEBUG, "Inside has for store id " << store, "RedisClusterPlusPlusDBLayer");

     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     // Ensure that a store exists for the given store id.
     if (storeIdExistsOrNot(storeIdString, dbError) == false) {
        if (dbError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside has, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return(false);
     }

     // In our Redis dps implementation, data item keys can have space characters.
     string base64_encoded_data_item_key;
     base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
     unsigned char *dummyValueData;
     uint32_t dummyValueSize;

     // Let us see if we already have this data item in our cache.
     // Check only for the data item existence and don't fetch the data item value.
     bool dataItemAlreadyInCache = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
        true, false, dummyValueData, dummyValueSize, dbError);

     if (dbError.getErrorCode() != 0) {
        SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
     }

     return(dataItemAlreadyInCache);
  } // End of has.

  // Check for the existence of a TTL based data item that is stored in the global area of the Redis DB.
  bool RedisClusterPlusPlusDBLayer::hasTTL(char const * keyData, uint32_t keySize,
     PersistenceError & dbError, bool encodeKey)
  {
     SPLAPPTRC(L_DEBUG, "Inside hasTTL.", "RedisClusterPlusPlusDBLayer");

     // In our Redis dps implementation, data item keys can have space characters.
     string data_item_key;

     if (encodeKey == true) {
        base64_encode(string(keyData, keySize), data_item_key);
     } else {
        // Since the key data sent here will always be in the network byte buffer format (NBF), 
        // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
        // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
        // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
        if ((uint8_t)keyData[0] < 0x80) {
           // Skip the first length byte. 
           data_item_key = string(&keyData[1], keySize-1);  
        } else {
           // Skip the five bytes at the beginning that represent the length of the key data.
           data_item_key = string(&keyData[5], keySize-5);
        }
     }

     long long exists_result_value = 0;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        exists_result_value = redis_cluster->exists(data_item_key);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_EXISTS_CMD: ") + exceptionString, DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside hasTTL, it failed with a Redis connection error for REDIS_EXISTS_CMD. Exception: " << exceptionString << " " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to check the existence of a TTL based data item. Error=" + exceptionString, DPS_KEY_EXISTENCE_CHECK_ERROR);
        SPLAPPTRC(L_ERROR, "Inside hasTTL, it failed to check the existence of a TTL based data item. Error=" <<
           exceptionString << ". rc=" << DPS_KEY_EXISTENCE_CHECK_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     if (exists_result_value == 0) {
        // TTL data item doesn't exist.
        return(false);
     } else {
        // TTL data item exists.
        return(true);
     }
  } // End of hasTTL.

  void RedisClusterPlusPlusDBLayer::clear(uint64_t store, PersistenceError & dbError) {
     SPLAPPTRC(L_DEBUG, "Inside clear for store id " << store, "RedisClusterPlusPlusDBLayer");

     ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     // Ensure that a store exists for the given store id.
     if (storeIdExistsOrNot(storeIdString, dbError) == false) {
        if (dbError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside clear, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return;
     }

     // Lock the store first.
     if (acquireStoreLock(storeIdString) == false) {
        // Unable to acquire the store lock.
        dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "RedisClusterPlusPlusDBLayer");
        // User has to retry again to remove the store.
        return;
     }

     // Get the store name.
     uint32_t dataItemCnt = 0;
     string storeName = "";
     string keySplTypeName = "";
     string valueSplTypeName = "";

     if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
        SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        // This is alarming. This will put this store in a bad state. Poor user has to deal with it.
        return;
     }

     // A very fast and quick thing to do is to simply delete the Store Contents Hash and
     // recreate it rather than removing one element at a time.
     // This action is performed on the Store Contents Hash that takes the following format.
     // '1' + 'store id' => 'Redis Hash'  [It will always have three metadata entries carrying the value of the actual store name, key spl type name, and value spl type name.]
     // Delete the entire store contents hash.
     string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;

     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        redis_cluster->del(keyString);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_DEL_CMD: ") + exceptionString, DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside clear, it failed with a Redis connection error for REDIS_DEL_CMD. Exception: " << exceptionString << " " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to remove the requested data item from the store for the store id " + storeIdString + ". Error=" + exceptionString, DPS_STORE_CLEARING_ERROR);
        SPLAPPTRC(L_ERROR, "Inside clear, it failed to remove the requested data item from the store for the store id " << storeIdString << ". Error=" <<
           exceptionString << ". rc=" << DPS_STORE_CLEARING_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return;
     }

     // Let us now recreate a new Store Contents Hash for this store with three meta data entries (store name, key spl type name, value spl type name).
     // Then we are done.
     // 1) Store name.
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        redis_cluster->hset(keyString, 
           string(REDIS_STORE_ID_TO_STORE_NAME_KEY), storeName);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HSET_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside clear, it failed for store id " << storeIdString << " with a Redis connection error for REDIS_HSET_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Fatal error in clear method: Unable to recreate 'Store Contents Hash' metadata1 in the store id " + storeIdString + "Unable to create 'Store Contents Hash' in a store with a name" + storeName + ". Error=" + exceptionString, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
        SPLAPPTRC(L_ERROR, "Fatal error: Inside clear, it failed for store id " << storeIdString << ". Error=" << exceptionString << ". rc=" << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return;
     }

     // 2) Key spl type name.
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        redis_cluster->hset(keyString, 
           string(REDIS_SPL_TYPE_NAME_OF_KEY), keySplTypeName);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HSET_CMD2: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside clear, it failed for store id " << storeIdString << " with a Redis connection error for REDIS_HSET_CMD2. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Fatal error in clear method: Unable to recreate 'Store Contents Hash' metadata2 in the store id " + storeIdString + "Unable to create 'Store Contents Hash' in a store with a name" + storeName + ". Error=" + exceptionString, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
        SPLAPPTRC(L_ERROR, "Fatal error: Inside clear, it failed for store id " << storeIdString << ". Error=" << exceptionString << ". rc=" << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return;
     }

     // 3) Value spl type name.
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        redis_cluster->hset(keyString, 
           string(REDIS_SPL_TYPE_NAME_OF_VALUE), valueSplTypeName);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HSET_CMD3: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside clear, it failed for store id " << storeIdString << " with a Redis connection error for REDIS_HSET_CMD3. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Fatal error in clear method: Unable to recreate 'Store Contents Hash' metadata3 in the store id " + storeIdString + "Unable to create 'Store Contents Hash' in a store with a name" + storeName + ". Error=" + exceptionString, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
        SPLAPPTRC(L_ERROR, "Fatal error: Inside clear, it failed for store id " << storeIdString << ". Error=" << exceptionString << ". rc=" << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseStoreLock(storeIdString);
        return;
     }

     // If there was an error in the store contents hash recreation, then this store is going to be in a weird state.
     // It is a fatal error. If that happened, something terribly had gone wrong in clearing the contents.
     // User should look at the dbError code and decide about a corrective action.
     releaseStoreLock(storeIdString);
  } // End of clear.
        
  uint64_t RedisClusterPlusPlusDBLayer::size(uint64_t store, PersistenceError & dbError)
  {
     SPLAPPTRC(L_DEBUG, "Inside size for store id " << store, "RedisClusterPlusPlusDBLayer");

     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     // Ensure that a store exists for the given store id.
     if (storeIdExistsOrNot(storeIdString, dbError) == false) {
        if (dbError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside size, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside size, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return(false);
     }

     // Store size information is maintained as part of the store information.
     uint32_t dataItemCnt = 0;
     string storeName = "";
     string keySplTypeName = "";
     string valueSplTypeName = "";

     if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
        SPLAPPTRC(L_DEBUG, "Inside size, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        return(0);
     }

     return((uint64_t)dataItemCnt);
  } // End of size.

  // We allow space characters in the data item keys.
  // Hence, it is required to based64 encode them before storing the
  // key:value data item in Redis.
  // (Use boost functions to do this.)
  void RedisClusterPlusPlusDBLayer::base64_encode(std::string const & str, std::string & base64) {
     // Insert line breaks for every 64KB characters.
     typedef insert_linebreaks<base64_from_binary<transform_width<string::const_iterator,6,8> >, 64*1024 > it_base64_t;

     unsigned int writePaddChars = (3-str.length()%3)%3;
     base64 = string(it_base64_t(str.begin()),it_base64_t(str.end()));
     base64.append(writePaddChars,'=');
  } // End of base64_encode.

  // As explained above, we based64 encoded the data item keys before adding them to the store.
  // If we need to get back the original key name, this function will help us in
  // decoding the base64 encoded key.
  // (Use boost functions to do this.)
  void RedisClusterPlusPlusDBLayer::base64_decode(std::string & base64, std::string & result) {
     // IMPORTANT:
     // For performance reasons, we are not passing a const string to this method.
     // Instead, we are passing a directly modifiable reference. Caller should be aware that
     // the string they passed to this method gets altered during the base64 decoding logic below.
     // After this method returns back to the caller, it is not advisable to use that modified string.
     typedef transform_width< binary_from_base64<remove_whitespace<string::const_iterator> >, 8, 6 > it_binary_t;

     unsigned int paddChars = count(base64.begin(), base64.end(), '=');
     std::replace(base64.begin(),base64.end(),'=','A'); // replace '=' by base64 encoding of '\0'
     result = string(it_binary_t(base64.begin()), it_binary_t(base64.end())); // decode
     result.erase(result.end()-paddChars,result.end());  // erase padding '\0' characters
  } // End of base64_decode.

  // This method will check if a store exists for a given store id.
  bool RedisClusterPlusPlusDBLayer::storeIdExistsOrNot(string storeIdString, PersistenceError & dbError) {
     string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
     long long exists_result_value = 0;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        exists_result_value = redis_cluster->exists(keyString);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_EXISTS_CMD: ") + exceptionString, DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside storeIdExistsOrNot, it failed with a Redis connection error for REDIS_EXISTS_CMD. Exception: " << exceptionString << " " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("StoreIdExistsOrNot: Unable to get StoreContentsHash from the StoreId " + storeIdString + ". Error=" + exceptionString, DPS_GET_STORE_CONTENTS_HASH_ERROR);
        SPLAPPTRC(L_ERROR, "Inside storeIdExistsOrNot, it failed to get StoreContentsHash from the StoreId " << storeIdString <<  ". Error=" << exceptionString << ". rc=" << DPS_GET_STORE_CONTENTS_HASH_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     if (exists_result_value == 0) {
        // Store doesn't exist.
        return(false);
     } else {
        // Store exists.
        return(true);
     }
  } // End of storeIdExistsOrNot.

  // This method will acquire a lock for a given store.
  bool RedisClusterPlusPlusDBLayer::acquireStoreLock(string const & storeIdString) {
     int32_t retryCnt = 0;

     // Try to get a lock for this store.
     while (1) {
        // '4' + 'store id' + 'dps_lock' => 1
        std::string storeLockKey = string(DPS_STORE_LOCK_TYPE) + storeIdString + DPS_LOCK_TOKEN;
        // This is an atomic activity.
        // If multiple threads attempt to do it at the same time, only one will succeed.
        // Winner will hold the lock until they release it voluntarily or
        // until the Redis back-end removes this lock entry after the DPS_AND_DL_GET_LOCK_TTL times out.
        string exceptionString = "";
        int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
        bool setnx_result_value = false;

        try {
           setnx_result_value = redis_cluster->setnx(storeLockKey, string("1"));
        } catch (const ReplyError &ex) {
           // WRONGTYPE Operation against a key holding the wrong kind of value
           exceptionString = ex.what();
           // Command execution error.
           exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
        } catch (const TimeoutError &ex) {
           // Reading or writing timeout
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
        } catch (const ClosedError &ex) {
           // Connection has been closed.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const IoError &ex) {
           // I/O error on the connection.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const Error &ex) {
           // Other errors
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
        }

        // Did we encounter an exception?
        if (exceptionType != REDIS_PLUS_PLUS_NO_ERROR) {
           // Problem in atomic creation of the store lock.
           return(false);
        }

        if(setnx_result_value == true) {
           // We got the lock.
           // Set the expiration time for this lock key.
           exceptionString = "";
           exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

           try {
              redis_cluster->expire(storeLockKey, 1);
           } catch (const ReplyError &ex) {
              // WRONGTYPE Operation against a key holding the wrong kind of value
              exceptionString = ex.what();
              // Command execution error.
              exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
           } catch (const TimeoutError &ex) {
              // Reading or writing timeout
              exceptionString = ex.what();
              // Connectivity related error.
              exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
           } catch (const ClosedError &ex) {
              // Connection has been closed.
              exceptionString = ex.what();
              // Connectivity related error.
              exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
           } catch (const IoError &ex) {
              // I/O error on the connection.
              exceptionString = ex.what();
              // Connectivity related error.
              exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
           } catch (const Error &ex) {
              // Other errors
              exceptionString = ex.what();
              // Connectivity related error.
              exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
           }

           // Did we encounter an exception?
           if (exceptionType != REDIS_PLUS_PLUS_NO_ERROR) {
              // Problem in setting the lock expiry time.
              SPLAPPTRC(L_ERROR, "b) Inside acquireStoreLock, it failed  with an exception. Error=" << exceptionString, "RedisClusterPlusPlusDBLayer");
              // We already got an exception. There is not much use in the code block below.
              // In any case, we will give it a try.
              // Delete the erroneous lock data item we created.
              exceptionString = "";
              exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

              try {
                 redis_cluster->del(storeLockKey);
              } catch (const ReplyError &ex) {
                 // WRONGTYPE Operation against a key holding the wrong kind of value
                 exceptionString = ex.what();
                 // Command execution error.
                 exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
              } catch (const TimeoutError &ex) {
                 // Reading or writing timeout
                 exceptionString = ex.what();
                 // Connectivity related error.
                 exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
              } catch (const ClosedError &ex) {
                 // Connection has been closed.
                 exceptionString = ex.what();
                 // Connectivity related error.
                 exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
              } catch (const IoError &ex) {
                 // I/O error on the connection.
                 exceptionString = ex.what();
                 // Connectivity related error.
                 exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
              } catch (const Error &ex) {
                 // Other errors
                 exceptionString = ex.what();
                 // Connectivity related error.
                 exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
              }

              // We couldn't get a lock.
              return(false);
           } 

           // We got the lock.
           return(true);
        } // End of if(setnx_result_value == true)

        // Someone else is holding on to the lock of this store. Wait for a while before trying again.
        retryCnt++;

        if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
           return(false);
        }

        // Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
        usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
           (retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
     } // End of the while loop.

     return(false);
  } // End of acquireStoreLock.

  void RedisClusterPlusPlusDBLayer::releaseStoreLock(string const & storeIdString) {
     // '4' + 'store id' + 'dps_lock' => 1
     std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;

     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        redis_cluster->del(storeLockKey);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter an exception?
     if (exceptionType != REDIS_PLUS_PLUS_NO_ERROR) {
        // Problem in atomic creation of the store lock.
        SPLAPPTRC(L_ERROR, "Inside releaseStoreLock, it failed with an exception. Error=" << exceptionString, "RedisClusterPlusPlusDBLayer");
     }
  } // End of releaseStoreLock.

  bool RedisClusterPlusPlusDBLayer::readStoreInformation(std::string const & storeIdString,
     PersistenceError & dbError, uint32_t & dataItemCnt, std::string & storeName, 
     std::string & keySplTypeName, std::string & valueSplTypeName) {
     // Read the store name, this store's key and value SPL type names,  and get the store size.
     storeName = "";
     keySplTypeName = "";
     valueSplTypeName = "";
     dataItemCnt = 0;

     // This action is performed on the Store Contents Hash that takes the following format.
     // '1' + 'store id' => 'Redis Hash'
     // It will always have the following three metadata entries:
     // dps_name_of_this_store ==> 'store name'
     // dps_spl_type_name_of_key ==> 'spl type name for this store's key'
     // dps_spl_type_name_of_value ==> 'spl type name for this store's value'
     // 1) Get the store name.
     string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        auto value = redis_cluster->hget(keyString, string(REDIS_STORE_ID_TO_STORE_NAME_KEY));

        if(value) {
           storeName = *value;
        }
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HGET_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "a) Inside readStoreInformation, it failed with a Redis connection error for REDIS_HGET_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to get StoreContentsHash metadata1 from the StoreId " + storeIdString + ". Error=" + exceptionString, DPS_GET_STORE_CONTENTS_HASH_ERROR);
        SPLAPPTRC(L_ERROR, "Inside readStoreInformation, it failed to get StoreContentsHash metadata1 from the StoreId " << storeIdString << ". Error=" << exceptionString << ". rc=" << DPS_GET_STORE_CONTENTS_HASH_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     if (storeName == "") {
        // Unable to get the name of this store.
        dbError.set("Unable to get the store name for StoreId " + storeIdString + ".", DPS_GET_STORE_NAME_ERROR);
        return(false);
     }

     // 2) Let us get the spl type name for this store's key.
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        auto value = redis_cluster->hget(keyString, string(REDIS_SPL_TYPE_NAME_OF_KEY));

        if(value) {
           keySplTypeName = *value;
        }
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HGET_CMD2: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "b) Inside readStoreInformation, it failed with a Redis connection error for REDIS_HGET_CMD2. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to get StoreContentsHash metadata2 from the StoreId " + storeIdString + ". Error=" + exceptionString, DPS_GET_STORE_CONTENTS_HASH_ERROR);
        SPLAPPTRC(L_ERROR, "Inside readStoreInformation, it failed to get StoreContentsHash metadata2 from the StoreId " << storeIdString << ". Error=" << exceptionString << ". rc=" << DPS_GET_STORE_CONTENTS_HASH_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     if (keySplTypeName == "") {
        // Unable to get the name of this store.
        dbError.set("Unable to get the key spl type name for StoreId " + storeIdString + ".", DPS_GET_STORE_NAME_ERROR);
        return(false);
     }

     // 3) Let us get the spl type name for this store's value.
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        auto value = redis_cluster->hget(keyString, string(REDIS_SPL_TYPE_NAME_OF_VALUE));

        if(value) {
           valueSplTypeName = *value;
        }
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HGET_CMD3: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "c) Inside readStoreInformation, it failed with a Redis connection error for REDIS_HGET_CMD3. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to get StoreContentsHash metadata3 from the StoreId " + storeIdString + ". Error=" + exceptionString, DPS_GET_STORE_CONTENTS_HASH_ERROR);
        SPLAPPTRC(L_ERROR, "Inside readStoreInformation, it failed to get StoreContentsHash metadata3 from the StoreId " << storeIdString << ". Error=" << exceptionString << ". rc=" << DPS_GET_STORE_CONTENTS_HASH_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     if (valueSplTypeName == "") {
        // Unable to get the name of this store.
        dbError.set("Unable to get the value spl type name for StoreId " + storeIdString + ".", DPS_GET_STORE_NAME_ERROR);
        return(false);
     }

     // 4) Let us get the size of the store contents hash now.
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     long long hlen_result_value = 0;

     try {
        hlen_result_value = redis_cluster->hlen(keyString);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HLEN_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "d) Inside readStoreInformation, it failed with a Redis connection error for REDIS_HLEN_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to get StoreContentsHash size from the StoreId " + storeIdString + ". Error=" + exceptionString, DPS_GET_STORE_SIZE_ERROR);
        SPLAPPTRC(L_ERROR, "Inside readStoreInformation, it failed to get StoreContentsHash size from the StoreId " << storeIdString << ". Error=" << exceptionString << ". rc=" << DPS_GET_STORE_SIZE_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Our Store Contents Hash for every store will have a mandatory reserved internal elements (store name, key spl type name, and value spl type name).
     // Let us not count those three elements in the actual store contents hash size that the caller wants now.
     if (hlen_result_value <= 0) {
        // This is not correct. We must have a minimum hash size of 3 because of the reserved elements.
        dbError.set("Wrong value (zero) observed as the store size for StoreId " + storeIdString + ".", DPS_GET_STORE_SIZE_ERROR);
        return(false);
     }

     dataItemCnt = hlen_result_value - 3;
     return(true);
  } // End of readStoreInformation.

  string RedisClusterPlusPlusDBLayer::getStoreName(uint64_t store, PersistenceError & dbError) {
     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     // Ensure that a store exists for the given store id.
     if (storeIdExistsOrNot(storeIdString, dbError) == false) {
        if (dbError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return("");
     }

     uint32_t dataItemCnt = 0;
     string storeName = "";
     string keySplTypeName = "";
     string valueSplTypeName = "";

     if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
        SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        return("");
     }

     string base64_decoded_storeName;
     base64_decode(storeName, base64_decoded_storeName);
     return(base64_decoded_storeName);
  } // End of getStoreName.

  string RedisClusterPlusPlusDBLayer::getSplTypeNameForKey(uint64_t store, PersistenceError & dbError) {
     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     // Ensure that a store exists for the given store id.
     if (storeIdExistsOrNot(storeIdString, dbError) == false) {
        if (dbError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return("");
     }

     uint32_t dataItemCnt = 0;
     string storeName = "";
     string keySplTypeName = "";
     string valueSplTypeName = "";

     if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
        SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        return("");
     }

     string base64_decoded_keySplTypeName;
     base64_decode(keySplTypeName, base64_decoded_keySplTypeName);
     return(base64_decoded_keySplTypeName);
  } // End of getSplTypeNameForKey.

  string RedisClusterPlusPlusDBLayer::getSplTypeNameForValue(uint64_t store, PersistenceError & dbError) {
     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     // Ensure that a store exists for the given store id.
     if (storeIdExistsOrNot(storeIdString, dbError) == false) {
        if (dbError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return("");
     }

     uint32_t dataItemCnt = 0;
     string storeName = "";
     string keySplTypeName = "";
     string valueSplTypeName = "";

     if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
        SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        return("");
     }

     string base64_decoded_valueSplTypeName;
     base64_decode(valueSplTypeName, base64_decoded_valueSplTypeName);
     return(base64_decoded_valueSplTypeName);
  } // End of getSplTypeNameForValue.

  std::string RedisClusterPlusPlusDBLayer::getNoSqlDbProductName(void) {
     return(string(REDIS_CLUSTER_PLUS_PLUS_NO_SQL_DB_NAME));
  } // End of getNoSqlDbProductName.

  void RedisClusterPlusPlusDBLayer::getDetailsAboutThisMachine(std::string & machineName, std::string & osVersion, std::string & cpuArchitecture) {
     machineName = nameOfThisMachine;
     osVersion = osVersionOfThisMachine;
     cpuArchitecture = cpuTypeOfThisMachine;
  } // End of getDetailsAboutThisMachine.

  bool RedisClusterPlusPlusDBLayer::runDataStoreCommand(std::string const & cmd, PersistenceError & dbError)   {
     // Redis commands in this case will be one way commands that don't return any data back after
     // running that command. e-g: "set foo bar", "del foo".
     // We will get this done using the other comprehensive overloaded method below.
     typedef streams_boost::tokenizer<streams_boost::char_separator<char> > tokenizer;
     streams_boost::char_separator<char> sep(
        " ", // dropped delimiters
        "",  // kept delimiters
        streams_boost::keep_empty_tokens); // empty token policy
     std::vector<string> myVector;

     // Collect all the space delimited tokens in the given Redis command.
     STREAMS_BOOST_FOREACH(std::string token, tokenizer(cmd, sep)) {
        myVector.push_back(token);
     } // End of the Boost FOREACH loop.

     string result = "";
     return(runDataStoreCommand(myVector, result, dbError));
  } // End of runDataStoreCommand. (Fire and Forget style).

  bool RedisClusterPlusPlusDBLayer::runDataStoreCommand(uint32_t const & cmdType, std::string const & httpVerb,
     std::string const & baseUrl, std::string const & apiEndpoint, std::string const & queryParams,
     std::string const & jsonRequest, std::string & jsonResponse, PersistenceError & dbError) {
     // This API can only be supported in NoSQL data stores such as Cloudant, HBase etc.
     // Redis doesn't have a way to do this.
     dbError.set("From Redis data store: This API to run native data store commands is not supported in Redis.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
     SPLAPPTRC(L_DEBUG, "From Redis data store: This API to run native data store commands is not supported in Redis. rc=" << DPS_RUN_DATA_STORE_COMMAND_ERROR, "RedisClusterPlusPlusDBLayer");
     return(false);
  } // End of runDataStoreCommand (Http style)

  /// If users want to send any valid Redis command to the Redis server made up as individual parts,
  /// this API can be used. This will work only with Redis. Users simply have to split their
  /// valid Redis command into individual parts that appear between spaces and pass them in 
  /// exacly in that order via a list<rstring>. DPS back-end code will put them together 
  /// correctly before executing the command on a configured Redis server. This API will also
  /// return the resulting value from executing any given Redis command as a string. It is upto
  /// the caller to interpret the Redis returned value and make sense out of it.
  /// In essence, it is a two way Redis command which is very diffferent from the other plain
  /// API that is explained above. [NOTE: If you have to deal with storing or fetching 
  /// non-string complex Streams data types, you can't use this API. Instead, use the other
  /// DPS put/get/remove/has DPS APIs.]
  bool RedisClusterPlusPlusDBLayer::runDataStoreCommand(std::vector<std::string> const & cmdList, std::string & resultValue, PersistenceError & dbError) {
     resultValue = "";

     if (cmdList.size() <= 0) {
        resultValue = "Error: Empty Redis command list was given by the caller.";
        dbError.set(resultValue, DPS_RUN_DATA_STORE_COMMAND_ERROR);
        return(false);
     }

     // Unlike the hiredis and cpp-hiredis-cluster libraries, redis-plus-plus doesn't support the 
     // "format string" to combine the command arguments into a command string i.e. RedisCommandArgv.
     // Instead, it has its own way of doing a generic command interface.
     //
     // Please note that there are several commands that will allow multiple keys in a single
     // command execution. In Redis Cluster, that will cause a CROSSSLOT error due to keys not
     // getting partitioned to the same Redis cluster node. It requires special things such as
     // using a hash tag by the client code calling this method. Please search for CROSSSLOT
     // in this file and in the advanced/04_XXXXXX example splmm file.
     //
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     resultValue = "OK";
     // Following logic looks for the redis commands to be in upper case.
     string cmd = streams_boost::to_upper_copy(cmdList.at(0));

     try {
        // Redis plus-plus has a few overloaded APIs for the generic command execution.
        // We will use one of those variations here for our needs.
        //
        // NOTE1: In Redis, there are a few commands that take no arguments.
        // Such commands can't be executed via the generic command API.
        //
        // NOTE2: Certain Redis command support the use of  multiple keys.
        // Because of that, there is a potential for CROSSSLOT error.
        // This code block can handle multiple keys in a single command.
        // However, it will give an exception as shown below if there are no 
        // hash tags used as part of the keys.
        // "CROSSSLOT Keys in request don't hash to the same slot"
        // https://stackoverflow.com/questions/38042629/redis-cross-slot-error
        // https://redis.io/topics/cluster-spec#keys-hash-tags
        //
        if(cmdList.size() > 1) {
           // Execute it using the generic command API.
           auto r = redis_cluster->command(cmdList.begin(), cmdList.end());
           
           /*
           Redis can return one of these replies after executing a command:

           Status Reply: Also known as Simple String Reply. It's a non-binary string reply.
           Bulk String Reply: Binary safe string reply.
           Integer Reply: Signed integer reply. Large enough to hold long long.
           Array Reply: (Nested) Array reply.
           Error Reply: Non-binary string reply that gives error info.
           */
           if(reply::is_error(*r) ==  true) {
              resultValue = reply::parse<string>(*r);
           } else if(reply::is_nil(*r) ==  true) {
              resultValue = "nil";
           } else if(reply::is_string(*r) ==  true) {
              resultValue = reply::parse<string>(*r);
           } else if(reply::is_status(*r) ==  true) {
              resultValue = reply::parse<string>(*r);
           } else if(reply::is_integer(*r) ==  true) {
              auto num = reply::parse<long long>(*r);
              resultValue = streams_boost::lexical_cast<string>(num);
           } else if(reply::is_array(*r) ==  true) {
              // In an array based result, there may be Redis nil result items if the query didn't yield any valid result.
              // So, it is safe to use the OptionalString instead of std::string which can lead to an exception while trying to convert nil to std::string.
              std::vector<OptionalString> result = reply::parse<std::vector<OptionalString>>(*r);
              resultValue = "";

              // We will flatten the result entries indide the array to a single std::string.
              for(uint32_t i = 0; i < result.size(); i++) {
                 OptionalString s = result.at(i);

                 if(resultValue.length() > 0) {
                    // We will append a new line character except for the very fist result.
                    resultValue.append("\n");
                 }

                 if(s) {
                    // Consider this string result item only if the OptionalString really has a string value.
                    resultValue.append(*s);
                 } else {
                    // This Redis result probably is Nil. We will make it an empty string.
                    resultValue.append("");
                 }
              } // End of for loop.
           }
        // Unprocessed Functions.
        } else {
          resultValue = "Your Redis command '" + cmd +  
             "' either has an incorrect syntax or it is not supported at this time in the redis-plus-plus K/V store.";
          dbError.set("Redis_Cluster_Reply_Error while executing the user given Redis command. Error=" + resultValue, DPS_RUN_DATA_STORE_COMMAND_ERROR);
          SPLAPPTRC(L_DEBUG, "Redis_Cluster_Reply_Error. Inside runDataStoreCommand using Redis cmdList, it failed to execute the user given Redis command list. Error=" << resultValue << ". " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "RedisClusterPlusPlusDBLayer");
          return(false);
        }
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     if(exceptionType != REDIS_PLUS_PLUS_NO_ERROR) {
        // Error in executing the user given Redis command.
        string exceptionTypeString = streams_boost::lexical_cast<string>(exceptionType);
        resultValue = "[" + exceptionTypeString + "] " + exceptionString;
        dbError.set("Redis_Cluster_Reply_Error while executing the user given Redis command. Error=" + resultValue, DPS_RUN_DATA_STORE_COMMAND_ERROR);
        SPLAPPTRC(L_DEBUG, "Redis_Cluster_Reply_Error. Inside runDataStoreCommand using Redis cmdList, it failed to execute the user given Redis command list. Error=" << resultValue << ". " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     } else {
        return(true);
     }
  }  // End of runDataStoreCommand. (Generic command style).

  // This method will get the data item from the store for a given key.
  // Caller of this method can also ask us just to find if a data item
  // exists in the store without the extra work of fetching and returning the data item value.
  bool RedisClusterPlusPlusDBLayer::getDataItemFromStore(std::string const & storeIdString,
     std::string const & keyDataString, bool const & checkOnlyForDataItemExistence,
     bool const & skipDataItemExistenceCheck, unsigned char * & valueData,
     uint32_t & valueSize, PersistenceError & dbError) {
     // Let us get this data item from the cache as it is.
     // Since there could be multiple data writers, we are going to get whatever is there now.
     // It is always possible that the value for the requested item can change right after
     // you read it due to the data write made by some other thread. Such is life in a global distributed in-memory store.
     string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     // If the caller doesn't want to perform the data existence check to save time, honor that wish here.
     if (skipDataItemExistenceCheck == false) {
        // This action is performed on the Store Contents Hash that takes the following format.
        // '1' + 'store id' => 'Redis Hash'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
        bool hexists_result_value = false;
     
        try {
           hexists_result_value = redis_cluster->hexists(keyString, keyDataString);
        } catch (const ReplyError &ex) {
           // WRONGTYPE Operation against a key holding the wrong kind of value
           exceptionString = ex.what();
           // Command execution error.
           exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
        } catch (const TimeoutError &ex) {
           // Reading or writing timeout
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
        } catch (const ClosedError &ex) {
           // Connection has been closed.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const IoError &ex) {
           // I/O error on the connection.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const Error &ex) {
           // Other errors
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
        }

        // Did we encounter a redis-cluster server connection error?
        if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
           // This is how we can detect that a wrong redis-cluster server name is configured by the user or
           // not even a single redis-cluster server daemon being up and running.
           // This is a serious error.
           dbError.set(string("Unable to connect to the redis-cluster server(s).") +
              string(" Got an exception for REDIS_HEXISTS_CMD: ") + exceptionString, DPS_CONNECTION_ERROR);
           SPLAPPTRC(L_ERROR, "a) Inside getDataItemFromStore, it failed with a Redis connection error for REDIS_HEXISTS_CMD. Exception: " << exceptionString << " " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
           return(false);
        }

        // Did we encounter a redis reply error?
        if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
           exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
           dbError.set("getDataItemFromStore: Unable to check for the existence of the data item in the StoreId " + storeIdString + ". Error=" + exceptionString, DPS_KEY_EXISTENCE_CHECK_ERROR);
           SPLAPPTRC(L_ERROR, "a) Inside getDataItemFromStore, unable to check for the existence of the data item in the StoreId " << storeIdString <<  ". Error=" << exceptionString << ". rc=" << DPS_KEY_EXISTENCE_CHECK_ERROR, "RedisClusterPlusPlusDBLayer");
           return(false);
        }

        // If the caller only wanted us to check for the data item existence, we can exit now.
        if (checkOnlyForDataItemExistence == true) {
           return(hexists_result_value);
        }

        // Caller wants us to fetch and return the data item value.
        // If the data item is not there, we can't do much at this point.
        if (hexists_result_value == false) {
           // This data item doesn't exist. Let us raise an error.
           // Requested data item is not there in the cache.
           dbError.set("The requested data item doesn't exist in the StoreId " + storeIdString +
              ".", DPS_DATA_ITEM_READ_ERROR);
           return(false);
        }
     } // End of if (skipDataItemExistenceCheck == false)

     // Fetch the data item now.
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     string data_item_value = "";

     try {
        auto value = redis_cluster->hget(keyString, keyDataString);

        if(value) {
           data_item_value = *value;
        }
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        dbError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_HGET_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DPS_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "b) Inside getDataItemFromStore, it failed with a Redis connection error for REDIS_HGET_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        dbError.set("Unable to get a data item from the StoreId " + storeIdString + ". Error=" + exceptionString, DPS_DATA_ITEM_READ_ERROR);
        SPLAPPTRC(L_ERROR, "Inside getDataItemFromStore, it failed to get a data item from the StoreId " << storeIdString << ". Error=" << exceptionString << ". rc=" << DPS_DATA_ITEM_READ_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Data item value read from the store will be in this format: 'value'
     if (data_item_value.length() == 0) {
        // User stored empty data item value in the cache.
        valueData = (unsigned char *)"";
        valueSize = 0;
     } else {
        // We can allocate memory for the exact length of the data item value.
        valueSize = data_item_value.length();
        valueData = (unsigned char *) malloc(valueSize);

        if (valueData == NULL) {
           // Unable to allocate memory to transfer the data item value.
           dbError.set("Unable to allocate memory to copy the data item value for the StoreId " +
              storeIdString + ".", DPS_GET_DATA_ITEM_MALLOC_ERROR);
           valueSize = 0;
           return(false);
        }

        // We expect the caller of this method to free the valueData pointer.
        memcpy(valueData, data_item_value.c_str(), valueSize);
     }

     return(true);
  } // End of getDataItemFromStore.

  RedisClusterPlusPlusDBLayerIterator * RedisClusterPlusPlusDBLayer::newIterator(uint64_t store, PersistenceError & dbError) {
     SPLAPPTRC(L_DEBUG, "Inside newIterator for store id " << store, "RedisClusterPlusPlusDBLayer");

     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     // Ensure that a store exists for the given store id.
     if (storeIdExistsOrNot(storeIdString, dbError) == false) {
        if (dbError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return(NULL);
     }

     // Get the general information about this store.
     uint32_t dataItemCnt = 0;
     string storeName = "";
     string keySplTypeName = "";
     string valueSplTypeName = "";

     if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
        SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        return(NULL);
     }

     // It is a valid store. Create a new iterator and return it to the caller.
     RedisClusterPlusPlusDBLayerIterator *iter = new RedisClusterPlusPlusDBLayerIterator();
     iter->store = store;
     base64_decode(storeName, iter->storeName);
     iter->hasData = true;
     // Give this iterator access to our RedisClusterPlusPlusDBLayer object.
     iter->redisClusterPlusPlusDBLayerPtr = this;
     iter->sizeOfDataItemKeysVector = 0;
     iter->currentIndex = 0;

     SPLAPPTRC(L_DEBUG, "Inside newIterator: store=" << iter->store << ", storeName=" << iter->storeName << ", hasData=" << iter->hasData << ", sizeOfDataItemKeysVector=" << iter->sizeOfDataItemKeysVector << ", currentIndex=" << iter->currentIndex, "RedisClusterPlusPlusDBLayer");

     return(iter);
  } // End of newIterator.

  void RedisClusterPlusPlusDBLayer::deleteIterator(uint64_t store, Iterator * iter, PersistenceError & dbError) {
     SPLAPPTRC(L_DEBUG, "Inside deleteIterator for store id " << store, "RedisClusterPlusPlusDBLayer");

     if (iter == NULL) {
        return;
     }

     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();

     RedisClusterPlusPlusDBLayerIterator *myIter = static_cast<RedisClusterPlusPlusDBLayerIterator *>(iter);

     // Let us ensure that the user wants to delete an iterator that really belongs to the store passed to us.
     // This will handle user's coding errors where a wrong combination of store id and iterator is passed to us for deletion.
     if (myIter->store != store) {
        // User sent us a wrong combination of a store and an iterator.
        dbError.set("A wrong iterator has been sent for deletion. This iterator doesn't belong to the StoreId " +
           storeIdString + ".", DPS_STORE_ITERATION_DELETION_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside deleteIterator, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_DELETION_ERROR, "RedisClusterPlusPlusDBLayer");
        return;
     } else {
        delete iter;
     }
  } // End of deleteIterator.

  // This method will acquire a lock for any given generic/arbitrary identifier passed as a string..
  // This is typically used inside the createStore, createOrGetStore, createOrGetLock methods to
  // provide thread safety. There are other lock acquisition/release methods once someone has a valid store id or lock id.
  bool RedisClusterPlusPlusDBLayer::acquireGeneralPurposeLock(string const & entityName) {
     int32_t retryCnt = 0;

     //Try to get a lock for this generic entity.
     while (1) {
        // '501' + 'entity name' + 'generic_lock' => 1
        std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
        // This is an atomic activity.
        // If multiple threads attempt to do it at the same time, only one will succeed.
        // Winner will hold the lock until they release it voluntarily or
        // until the Redis back-end removes this lock entry after the DPS_AND_DL_GET_LOCK_TTL times out.
        string exceptionString = "";
        int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
        bool setnx_result_value = false;

        try {
           setnx_result_value = redis_cluster->setnx(genericLockKey, string("1"));
        } catch (const ReplyError &ex) {
           // WRONGTYPE Operation against a key holding the wrong kind of value
           exceptionString = ex.what();
           // Command execution error.
           exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
        } catch (const TimeoutError &ex) {
           // Reading or writing timeout
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
        } catch (const ClosedError &ex) {
           // Connection has been closed.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const IoError &ex) {
           // I/O error on the connection.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const Error &ex) {
           // Other errors
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
        }

        // Did we encounter a redis-cluster server connection error?
        if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
           SPLAPPTRC(L_ERROR, "a) Inside acquireGeneralPurposeLock, it failed with a Redis connection error for REDIS_SETNX_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
           return(false);
        }

        // Did we encounter a redis reply error?
        if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
           exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
           // Problem in atomic creation of the general purpose lock.
           return(false);
        }

        if(setnx_result_value == true) {
           // We got the lock.
           // Set the expiration time for this lock key.
           exceptionString = "";
           exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

           try {
              redis_cluster->expire(genericLockKey, DPS_AND_DL_GET_LOCK_TTL);
           } catch (const ReplyError &ex) {
              // WRONGTYPE Operation against a key holding the wrong kind of value
              exceptionString = ex.what();
              // Command execution error.
              exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
           } catch (const TimeoutError &ex) {
              // Reading or writing timeout
              exceptionString = ex.what();
              // Connectivity related error.
              exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
           } catch (const ClosedError &ex) {
              // Connection has been closed.
              exceptionString = ex.what();
              // Connectivity related error.
              exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
           } catch (const IoError &ex) {
              // I/O error on the connection.
              exceptionString = ex.what();
              // Connectivity related error.
              exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
           } catch (const Error &ex) {
              // Other errors
              exceptionString = ex.what();
              // Connectivity related error.
              exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
           }

           // Did we encounter an exception?
           if (exceptionType != REDIS_PLUS_PLUS_NO_ERROR) {
              // Problem in setting the lock expiry time.
              SPLAPPTRC(L_ERROR, "b) Inside acquireGeneralPurposeLock, it failed with an exception for REDIS_EXPIRE_CMD. Exception: " << exceptionString << ".", "RedisClusterPlusPlusDBLayer");

              // We already got an exception. There is not much use in the code block below.
              // In any case, we will give it a try.
              // Delete the erroneous lock data item we created.
              try {
                 redis_cluster->del(genericLockKey);
              } catch (const Error &ex) {
                 // It is not good if this one fails. We can't do much about that.
                 SPLAPPTRC(L_ERROR, "c) Inside acquireGeneralPurposeLock, it failed with an exception for REDIS_DEL_CMD. Exception: " << ex.what() << ".", "RedisClusterPlusPlusDBLayer");
              }

              return(false);
           }

           // We got the lock with proper expiry time set.
           return(true);
        } // End of if(setnx_result_value == true).

        // Someone else is holding on to the lock of this entity. Wait for a while before trying again.
        retryCnt++;

        if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
           return(false);
        }

        // Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
        usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
           (retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
     } // End of while loop.

     return(false);
  } // End of acquireGeneralPurposeLock.

  void RedisClusterPlusPlusDBLayer::releaseGeneralPurposeLock(string const & entityName) {
     // '501' + 'entity name' + 'generic_lock' => 1
     std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;

     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        redis_cluster->del(genericLockKey);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we get into an exception?
     if (exceptionType != REDIS_PLUS_PLUS_NO_ERROR) {
        SPLAPPTRC(L_ERROR, "Inside releaseGeneralPurposeLock, it failed to delete a lock. Error=" << exceptionString << ". rc=" << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
     }
  } // End of releaseGeneralPurposeLock.

  RedisClusterPlusPlusDBLayerIterator::RedisClusterPlusPlusDBLayerIterator() {

  }

  RedisClusterPlusPlusDBLayerIterator::~RedisClusterPlusPlusDBLayerIterator() {

  }

  bool RedisClusterPlusPlusDBLayerIterator::getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
     unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError) {
     SPLAPPTRC(L_DEBUG, "Inside getNext for store id " << store, "RedisClusterPlusPlusDBLayerIterator");

     // If the iteration already ended, do a quick return back to the caller.
     // Another possibility we want to detect is whether the caller really passed the
     // correct store id that belongs to this iterator object. If either of them
     // is not in our favor, bail out right away.
     if ((this->hasData == false) || (store != this->store)) {
        return(false);
     }

     std::ostringstream storeId;
     storeId << store;
     string storeIdString = storeId.str();
     string data_item_key = "";

     // Ensure that a store exists for the given store id.
     if (this->redisClusterPlusPlusDBLayerPtr->storeIdExistsOrNot(storeIdString, dbError) == false) {
        if (dbError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayerIterator");
        } else {
           dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisClusterPlusPlusDBLayerIterator");
        }

        return(false);
     }

     // Ensure that this store is not empty at this time.
     if (this->redisClusterPlusPlusDBLayerPtr->size(store, dbError) <= 0) {
        dbError.set("Store is empty for the StoreId " + storeIdString + ".", DPS_STORE_EMPTY_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_STORE_EMPTY_ERROR, "RedisClusterPlusPlusDBLayerIterator");
        return(false);
     }

     if (this->sizeOfDataItemKeysVector <= 0) {
        // This is the first time we are coming inside getNext for store iteration.
        // Let us get the available data item keys from this store.
        this->dataItemKeys.clear();
        string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
        std::vector<std::string> keys;
        string exceptionString = "";
        int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

        try {
           this->redisClusterPlusPlusDBLayerPtr->redis_cluster->hkeys(keyString, std::back_inserter(keys));
        } catch (const ReplyError &ex) {
           // WRONGTYPE Operation against a key holding the wrong kind of value
           exceptionString = ex.what();
           // Command execution error.
           exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
        } catch (const TimeoutError &ex) {
           // Reading or writing timeout
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
        } catch (const ClosedError &ex) {
           // Connection has been closed.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const IoError &ex) {
           // I/O error on the connection.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const Error &ex) {
           // Other errors
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
        }

        // Did we encounter a redis-cluster server connection error?
        if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
           // This is how we can detect that a wrong redis-cluster server name is configured by the user or
           // not even a single redis-cluster server daemon being up and running.
           // This is a serious error.
           dbError.set(string("getNext: Unable to connect to the redis-cluster server(s).") +
              string(" Got an exception for REDIS_HKEYS_CMD: ") + exceptionString + 
              " Application code may call the DPS reconnect API and then retry the failed operation. ", 
              DPS_CONNECTION_ERROR);
           SPLAPPTRC(L_ERROR, "Inside getNext, it failed with a Redis connection error for REDIS_HKEYS_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
           this->hasData = false;
           return(false);
        }

        // Did we encounter a redis reply error?
        if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
           exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
           // Unable to get data item keys from the store.
           dbError.set("Unable to get data item keys for the StoreId " + storeIdString + ". Error=" + exceptionString, DPS_GET_STORE_DATA_ITEM_KEYS_ERROR);
           SPLAPPTRC(L_ERROR, "Inside getNext, it failed to get data item keys from the StoreId " << storeIdString << ". Error=" << exceptionString << ". rc=" << DPS_GET_STORE_DATA_ITEM_KEYS_ERROR, "RedisClusterPlusPlusDBLayer");
           this->hasData = false;
           return(false);
        }

        // We have the data item keys returned in array now.
        // Let us insert them into the iterator object's member variable that will hold the data item keys for this store.
        for(unsigned int i=0; i < keys.size(); i++) {
           data_item_key = keys[i];

           // Every dps store will have three mandatory reserved data item keys for internal use.
           // Let us not add them to the iteration object's member variable.
           if (data_item_key.compare(REDIS_STORE_ID_TO_STORE_NAME_KEY) == 0) {
              continue; // Skip this one.
           } else if (data_item_key.compare(REDIS_SPL_TYPE_NAME_OF_KEY) == 0) {
              continue; // Skip this one.
           } else if (data_item_key.compare(REDIS_SPL_TYPE_NAME_OF_VALUE) == 0) {
              continue; // Skip this one.
           }

           this->dataItemKeys.push_back(data_item_key);
        } // End of for loop.

        this->sizeOfDataItemKeysVector = this->dataItemKeys.size();
        this->currentIndex = 0;

        if (this->sizeOfDataItemKeysVector == 0) {
           // This is an empty store at this time.
           // Let us exit now.
           this->hasData = false;
           return(false);
        }

        SPLAPPTRC(L_DEBUG, "Inside getNext: store=" << this->store << ", storeName=" << this->storeName << ", hasData=" << this->hasData << ", sizeOfDataItemKeysVector=" << this->sizeOfDataItemKeysVector << ", currentIndex=" << this->currentIndex, "RedisClusterPlusPlusDBLayer");
     } // End of if (this->sizeOfDataItemKeysVector <= 0)

     // We have data item keys.
     // Let us get the next available data.
        SPLAPPTRC(L_DEBUG, "Inside getNext: Just about to get the data item key at index " << this->currentIndex << " from store " << this->storeName << ". Total number of keys=" << this->sizeOfDataItemKeysVector, "RedisClusterPlusPlusDBLayer");

     data_item_key = this->dataItemKeys.at(this->currentIndex);
     // Advance the data item key vector index by 1 for it to be ready for the next iteration.
     this->currentIndex += 1;

     if (this->currentIndex >= this->sizeOfDataItemKeysVector) {
        // We have served all the available data to the caller who is iterating this store.
        // There is no more data to deliver for subsequent iteration requests from the caller.
        this->dataItemKeys.clear();
        this->currentIndex = 0;
        this->sizeOfDataItemKeysVector = 0;
        this->hasData = false;
     }

     // Get this data item's value data and value size.
     // data_item_key was obtained straight from the store contents hash, where it is
     // already in the base64 encoded format.
     bool result = this->redisClusterPlusPlusDBLayerPtr->getDataItemFromStore(storeIdString, data_item_key,
        false, false, valueData, valueSize, dbError);

     if (result == false) {
        // Some error has occurred in reading the data item value.
        SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisClusterPlusPlusDBLayerIterator");
        // We will disable any future action for this store using the current iterator.
        this->hasData = false;
        return(false);
     }

     // We are almost done once we take care of arranging to return the key name and key size.
     // In order to support spaces in data item keys, we base64 encoded them before storing it in Redis.
     // Let us base64 decode it now to get the original data item key.
     string base64_decoded_data_item_key;
     this->redisClusterPlusPlusDBLayerPtr->base64_decode(data_item_key, base64_decoded_data_item_key);
     data_item_key = base64_decoded_data_item_key;
     keySize = data_item_key.length();
     // Allocate memory for this key and copy it to that buffer.
     keyData = (unsigned char *) malloc(keySize);

     if (keyData == NULL) {
        // This error will occur very rarely.
        // If it happens, we will handle it.
        // We will not return any useful data to the caller.
        if (valueSize > 0) {
           delete [] valueData;
           valueData = NULL;
        }

        // We will disable any future action for this store using the current iterator.
        this->hasData = false;
        keySize = 0;
        dbError.set("Unable to allocate memory for the keyData while doing the next data item iteration for the StoreId " + storeIdString + ".", DPS_STORE_ITERATION_MALLOC_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_MALLOC_ERROR, "RedisClusterPlusPlusDBLayerIterator");
        return(false);
     }

     // Copy the raw key data into the allocated buffer.
     memcpy(keyData, data_item_key.data(), keySize);
     // We are done. We expect the caller to free the keyData and valueData buffers.
     return(true);
  } // End of getNext.

// =======================================================================================================
// Beyond this point, we have code that deals with the distributed locks that a SPL developer can
// create, remove,acquire, and release.
// =======================================================================================================
  uint64_t RedisClusterPlusPlusDBLayer::createOrGetLock(std::string const & name, PersistenceError & lkError) {
     SPLAPPTRC(L_DEBUG, "Inside createOrGetLock with a name " << name, "RedisClusterPlusPlusDBLayer");

     string base64_encoded_name;
     base64_encode(name, base64_encoded_name);

     // Get a general purpose lock so that only one thread can
     // enter inside of this method at any given time with the same lock name.
     if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
        // Unable to acquire the general purpose lock.
        lkError.set("Unable to get a generic lock for creating a lock with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for an yet to be created lock with its name as " <<
           name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "RedisClusterPlusPlusDBLayer");
        // User has to retry again to create this distributed lock.
        return 0;
     }

     // Let us first see if a lock with the given name already exists.
     // In our Redis dps implementation, data item keys can have space characters.
     // Inside Redis, all our lock names will have a mapping type indicator of
     // "5" at the beginning followed by the actual lock name.
     // '5' + 'lock name' ==> 'lock id'
     std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
     long long exists_result_value = 0;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        exists_result_value = redis_cluster->exists(lockNameKey);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        lkError.set(string("Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_EXISTS_CMD: ") + exceptionString, DL_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "a) Inside createOrGetLock, it failed with a Redis connection error for REDIS_EXISTS_CMD. Exception: " << exceptionString << " " << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return(0);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        lkError.set("Unable to check the existence of the lock. Error=" + exceptionString, DPS_KEY_EXISTENCE_CHECK_ERROR);
        SPLAPPTRC(L_ERROR, "b) Inside createOrGetLock, it failed to check for the existence of the lock. Error=" << exceptionString << ". rc=" << DPS_KEY_EXISTENCE_CHECK_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return(0);
     }

     if (exists_result_value == 1) {
        // This lock already exists in our cache.
        // We can get the lockId and return it to the caller.
        string get_result_value = "";
        exceptionString = "";
        exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

        try {
           auto val = redis_cluster->get(lockNameKey);

           if(val) {
              get_result_value = *val;
           }
        } catch (const ReplyError &ex) {
           // WRONGTYPE Operation against a key holding the wrong kind of value
           exceptionString = ex.what();
           // Command execution error.
           exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
        } catch (const TimeoutError &ex) {
           // Reading or writing timeout
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
        } catch (const ClosedError &ex) {
           // Connection has been closed.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const IoError &ex) {
           // I/O error on the connection.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const Error &ex) {
           // Other errors
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
        }

        // Did we encounter a redis-cluster server connection error?
        if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
           // This is how we can detect that a wrong redis-cluster server name is configured by the user or
           // not even a single redis-cluster server daemon being up and running.
           // This is a serious error.
           lkError.set(string("createOrGetLock: Unable to connect to the redis-cluster server(s).") +
              string(" Got an exception for REDIS_GET_CMD: ") + exceptionString + 
              " Application code may call the DPS reconnect API and then retry the failed operation. ", 
              DL_CONNECTION_ERROR);
           SPLAPPTRC(L_ERROR, "c) Inside createOrGetLock, it failed for the lock named " << name << " with a Redis connection error for REDIS_GET_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
           releaseGeneralPurposeLock(base64_encoded_name);
           return(0);
        }

        // Did we encounter a redis reply error?
        if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
           exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
           lkError.set("Unable to get the lockId for the lockName " + name +  
              ". Error=" + exceptionString, DL_GET_LOCK_ID_ERROR);
           SPLAPPTRC(L_ERROR, "d) Inside createOrGetLock, unable to get the lockId for the lockName "  << name << ". Error=" << exceptionString << ". rc=" << DL_GET_LOCK_ID_ERROR, "RedisClusterPlusPlusDBLayer");
           releaseGeneralPurposeLock(base64_encoded_name);
           return(0);
        }

        if (get_result_value.length() <= 0) {
           // Unable to get the lock information. It is an abnormal error. Convey this to the caller.
           lkError.set("Redis returned an empty lockId for the lockName " + name + ".", DL_GET_LOCK_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "e) Inside createOrGetLock, it failed with an empty lockId for the lockName " <<
              name << ". " << DL_GET_LOCK_ID_ERROR, "RedisClusterPlusPlusDBLayer");
           releaseGeneralPurposeLock(base64_encoded_name);
           return(0);
        } else {
	   uint64_t lockId = 0;
           lockId = streams_boost::lexical_cast<uint64_t>(get_result_value);
           releaseGeneralPurposeLock(base64_encoded_name);
           return(lockId);
        }
     } // End of if (exists_result_value == 1)

     // There is no existing lock.
     // Create a new lock.
     // At first, let us increment our global dps_and_dl_guid to reserve a new lock id.
     uint64_t lockId = 0;
     std::string guid_key = DPS_AND_DL_GUID_KEY;     
     long long incr_result_value = 0;
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        incr_result_value = redis_cluster->incr(guid_key);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        lkError.set(string("createOrGetLock: Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_INCR_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DL_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "f) Inside createOrGetLock, it failed for the lock named " << name << " with a Redis connection error for REDIS_INCR_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return 0;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        lkError.set("Unable to get a unique lock id for a lock named " + name + 
           ". Error=" + exceptionString, DL_GUID_CREATION_ERROR);
        SPLAPPTRC(L_ERROR, "g) Inside createOrGetLock, unable to get a unique lock id for a lock named " << name << ". Error=" << exceptionString << ". rc=" << DL_GUID_CREATION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return(0);
     }

     // Get the newly created lock id.
     lockId = incr_result_value;
     // We secured a guid. We can now create this lock.
     //
     // 1) Create the Lock Name
     //    '5' + 'lock name' ==> 'lock id'
     std::ostringstream value;
     value << lockId;
     std::string value_string = value.str();
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        redis_cluster->set(lockNameKey, value_string);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        lkError.set(string("createOrGetLock: Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_SET_CMD: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DL_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "h) Inside createOrGetLock, it failed for the lock named " << name << " with a Redis connection error for REDIS_SET_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return(0);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        lkError.set("Unable to create 'LockName:LockId' in the cache for a lock named " + name +  
           ". Error=" + exceptionString, DL_LOCK_NAME_CREATION_ERROR);
        SPLAPPTRC(L_ERROR, "i) Inside createOrGetLock, it failed to create 'LockName:LockId' in the cache for a lock named " << name << ". Error=" << exceptionString << ". rc=" << DL_LOCK_NAME_CREATION_ERROR, "RedisClusterPlusPlusDBLayer");
        // We are simply leaving an incremented value for the dps_and_dl_guid key in the cache that will never get used.
        // Since it is harmless, there is no need to reduce this number by 1. It is okay that this guid number will remain unassigned to any store.
        releaseGeneralPurposeLock(base64_encoded_name);
        return(0);
     }

     // 2) Create the Lock Info
     //    '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
     std::string lockInfoKey = DL_LOCK_INFO_TYPE + value_string;  // LockId becomes the new key now.
     value_string = string("0_0_0_") + base64_encoded_name;
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        redis_cluster->set(lockInfoKey, value_string);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        // This is how we can detect that a wrong redis-cluster server name is configured by the user or
        // not even a single redis-cluster server daemon being up and running.
        // This is a serious error.
        lkError.set(string("createOrGetLock: Unable to connect to the redis-cluster server(s).") +
           string(" Got an exception for REDIS_SET_CMD2: ") + exceptionString + 
           " Application code may call the DPS reconnect API and then retry the failed operation. ", 
           DL_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "j) Inside createOrGetLock, it failed for the lock named " << name << " with a Redis connection error for REDIS_SET_CMD2. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        releaseGeneralPurposeLock(base64_encoded_name);
        return(0);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        lkError.set("Unable to create 'LockId:LockInfo' in the cache for a lock named " + name +  
           ". Error=" + exceptionString, DL_LOCK_NAME_CREATION_ERROR);
        SPLAPPTRC(L_ERROR, "k) Inside createOrGetLock, it failed to create 'LockId:LockInfo' in the cache for a lock named " << name << ". Error=" << exceptionString << ". rc=" << DL_LOCK_NAME_CREATION_ERROR, "RedisClusterPlusPlusDBLayer");
        // We are simply leaving an incremented value for the dps_and_dl_guid key in the cache that will never get used.
        // Since it is harmless, there is no need to reduce this number by 1. It is okay that this guid number will remain unassigned to any store.

        // Delete the previous entry we made.
        try {
           redis_cluster->del(lockNameKey);
        } catch (const Error &ex) {
          // It is not good if this one fails. We can't do much about that.
        }

        releaseGeneralPurposeLock(base64_encoded_name);
        return(0);
     }

     // We created the lock.
     SPLAPPTRC(L_DEBUG, "Inside createOrGetLock done for a lock named " << name, "RedisClusterPlusPlusDBLayer");
     releaseGeneralPurposeLock(base64_encoded_name);
     return (lockId);
  } // End of createOrGetLock.

  bool RedisClusterPlusPlusDBLayer::removeLock(uint64_t lock, PersistenceError & lkError) {
     SPLAPPTRC(L_DEBUG, "Inside removeLock for lock id " << lock, "RedisClusterPlusPlusDBLayer");

     ostringstream lockId;
     lockId << lock;
     string lockIdString = lockId.str();

     // If the lock doesn't exist, there is nothing to remove. Don't allow this caller inside this method.
     if(lockIdExistsOrNot(lockIdString, lkError) == false) {
        if (lkError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return(false);
     }

     // Before removing the lock entirely, ensure that the lock is not currently being used by anyone else.
     if (acquireLock(lock, 5, 3, lkError) == false) {
        // Unable to acquire the distributed lock.
        lkError.set("Unable to get a distributed lock for the LockId " + lockIdString + ".", DL_GET_DISTRIBUTED_LOCK_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for the lock id " << lockIdString << ". " << DL_GET_DISTRIBUTED_LOCK_ERROR, "RedisClusterPlusPlusDBLayer");
        // User has to retry again to remove the lock.
        return(false);
     }

     // We ensured that this lock is not being used by anyone at this time.
     // We are safe to remove this distributed lock entirely.
     // Let us first get the lock name for this lock id.
     uint32_t lockUsageCnt = 0;
     int32_t lockExpirationTime = 0;
     std::string lockName = "";
     pid_t lockOwningPid = 0;

     if (readLockInformation(lockIdString, lkError, lockUsageCnt, lockExpirationTime, lockOwningPid, lockName) == false) {
        SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        releaseLock(lock, lkError);
        // This is alarming. This will put this lock in a bad state. Poor user has to deal with it.
        return(false);
     }

     // Let us first remove the lock info for this distributed lock.
     // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
     std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        redis_cluster->del(lockInfoKey);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Do we have an exception?     
     if(exceptionType != REDIS_PLUS_PLUS_NO_ERROR) {
        SPLAPPTRC(L_ERROR, "a) Inside removeLock, it failed with an exception. Error=" << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
     }

     // We can now delete the lock name root entry.
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     string lockNameKey = DL_LOCK_NAME_TYPE + lockName;
     
     try {
        redis_cluster->del(lockNameKey);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Do we have an exception?     
     if(exceptionType != REDIS_PLUS_PLUS_NO_ERROR) {
        SPLAPPTRC(L_ERROR, "b) Inside removeLock, it failed with an exception. Error=" << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
     }

     // We can delete the lock item itself now.
     releaseLock(lock, lkError);
     // Inside the release lock function we called in the previous statement, it makes a
     // call to update the lock info. That will obviously fail since we removed here everything about
     // this lock. Hence, let us not propagate that error and cause the user to panic.
     // Reset any error that may have happened in the previous operation of releasing the lock.
     lkError.reset();
     // Life of this lock ended completely with no trace left behind.
     return(true);
  } // End of removeLock.

  bool RedisClusterPlusPlusDBLayer::acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError) {
     SPLAPPTRC(L_DEBUG, "Inside acquireLock for lock id " << lock, "RedisClusterPlusPlusDBLayer");

     ostringstream lockId;
     lockId << lock;
     string lockIdString = lockId.str();
     int32_t retryCnt = 0;

     // If the lock doesn't exist, there is nothing to acquire. Don't allow this caller inside this method.
     if(lockIdExistsOrNot(lockIdString, lkError) == false) {
        if (lkError.hasError() == true) {
           SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        } else {
           lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for lock id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        }

        return(false);
     }

     // We will first check if we can get this lock.
     // '7' + 'lock id' + 'dl_lock' => 1
     std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
     time_t startTime, timeNow;
     // Get the start time for our lock acquisition attempts.
     time(&startTime);

     // Try to get a distributed lock.
     while(1) {
        // This is an atomic activity.
        // If multiple threads attempt to do it at the same time, only one will succeed.
        // Winner will hold the lock until they release it voluntarily or
        // until the Redis back-end removes this lock entry after the lease time ends.
        // We will add the lease time to the current timestamp i.e. seconds elapsed since the epoch.
        time_t new_lock_expiry_time = time(0) + (time_t)leaseTime;
        string exceptionString = "";
        int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
        bool setnx_result_value = false;

        try {
           setnx_result_value = redis_cluster->setnx(distributedLockKey, string("1"));
        } catch (const ReplyError &ex) {
           // WRONGTYPE Operation against a key holding the wrong kind of value
           exceptionString = ex.what();
           // Command execution error.
           exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
        } catch (const TimeoutError &ex) {
           // Reading or writing timeout
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
        } catch (const ClosedError &ex) {
           // Connection has been closed.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const IoError &ex) {
           // I/O error on the connection.
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
        } catch (const Error &ex) {
           // Other errors
           exceptionString = ex.what();
           // Connectivity related error.
           exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
        }

        // Did we encounter a redis-cluster server connection error?
        if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
           SPLAPPTRC(L_ERROR, "a) Inside acquireLock, it failed with a Redis connection error for REDIS_SETNX_CMD. Exception: " << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
           return(false);
        }

        // Did we encounter a redis reply error?
        if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
           exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
           // Problem in atomic creation of the distributed lock.
            SPLAPPTRC(L_ERROR, "b) Inside acquireLock, it failed with an error for REDIS_SETNX_CMD. Exception: " << exceptionString << ".", "RedisClusterPlusPlusDBLayer");
           return(false);
        }

        if(setnx_result_value == true) {
           // We got the lock.
           // Set the expiration time for this lock key.
           ostringstream expiryTimeInMillis;
           expiryTimeInMillis << (leaseTime*1000.00);
           long long ttlInMillis = streams_boost::lexical_cast<uint64_t>(expiryTimeInMillis.str());
           exceptionString = "";
           exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

           try {
              redis_cluster->psetex(distributedLockKey, ttlInMillis, "2");
           } catch (const ReplyError &ex) {
              // WRONGTYPE Operation against a key holding the wrong kind of value
              exceptionString = ex.what();
              // Command execution error.
              exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
           } catch (const TimeoutError &ex) {
              // Reading or writing timeout
              exceptionString = ex.what();
              // Connectivity related error.
              exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
           } catch (const ClosedError &ex) {
              // Connection has been closed.
              exceptionString = ex.what();
              // Connectivity related error.
              exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
           } catch (const IoError &ex) {
              // I/O error on the connection.
              exceptionString = ex.what();
              // Connectivity related error.
              exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
           } catch (const Error &ex) {
              // Other errors
              exceptionString = ex.what();
              // Connectivity related error.
              exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
           }

           // Is there an exception?
           if(exceptionType != REDIS_PLUS_PLUS_NO_ERROR) {
              SPLAPPTRC(L_ERROR, "c) Inside acquireLock, it failed with an exception for REDIS_PSETEX_CMD. Exception: " << exceptionString << ".", "RedisClusterPlusPlusDBLayer");
              // In any case, let us try the following code block which may also fail.
              // Delete the erroneous lock data item we created.
              try {
                 redis_cluster->del(distributedLockKey);
              } catch (const Error &ex) {
                 // It is not good if this one fails. We can't do much about that.
              }

              return(false);
           }

           // We got the lock.
           // Let us update the lock information now.
           if(updateLockInformation(lockIdString, lkError, 1, new_lock_expiry_time, getpid()) == true) {
              return(true);
           } else {
              // Some error occurred while updating the lock information.
              // It will be in an inconsistent state. Let us release the lock.
              // After than, we will continue in the while loop.
              releaseLock(lock, lkError);
           }
        } else {
           // We didn't get the lock.
           // Let us check if the previous owner of this lock simply forgot to release it.
           // In that case, we will release this expired lock.
           // Read the time at which this lock is expected to expire.
           uint32_t _lockUsageCnt = 0;
           int32_t _lockExpirationTime = 0;
           std::string _lockName = "";
           pid_t _lockOwningPid = 0;

           if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
              SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
           } else {
              // Is current time greater than the lock expiration time?
              if ((_lockExpirationTime > 0) && (time(0) > (time_t)_lockExpirationTime)) {
                 // Time has passed beyond the lease of this lock.
                 // Lease expired for this lock. Original owner forgot to release the lock and simply left it hanging there without a valid lease.
                 releaseLock(lock, lkError);
              }
           }
        } // End of if(setnx_result_value == true)

        // Someone else is holding on to this distributed lock. Wait for a while before trying again.
        retryCnt++;

        if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
           lkError.set("Unable to acquire the lock named " + lockIdString + ".", DL_GET_LOCK_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for a lock named " << lockIdString << ". " << DL_GET_LOCK_ERROR, "RedisClusterPlusPlusDBLayer");
           // Our caller can check the error code and try to acquire the lock again.
           return(false);
        }

        // Check if we have gone past the maximum wait time the caller was willing to wait in order to acquire this lock.
        time(&timeNow);
        if (difftime(startTime, timeNow) > maxWaitTimeToAcquireLock) {
           lkError.set("Unable to acquire the lock named " + lockIdString + " within the caller specified wait time.", DL_GET_LOCK_TIMEOUT_ERROR);
           SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire the lock named " << lockIdString <<
              " within the caller specified wait time." << DL_GET_LOCK_TIMEOUT_ERROR, "RedisClusterPlusPlusDBLayer");
           // Our caller can check the error code and try to acquire the lock again.
           return(false);
        }

        // Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
        usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
           (retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
     } // End of while(1)
  } // End of acquireLock.

  void RedisClusterPlusPlusDBLayer::releaseLock(uint64_t lock, PersistenceError & lkError) {
     SPLAPPTRC(L_DEBUG, "Inside releaseLock for lock id " << lock, "RedisClusterPlusPlusDBLayer");

     ostringstream lockId;
     lockId << lock;
     string lockIdString = lockId.str();

     // '7' + 'lock id' + 'dl_lock' => 1
     std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        redis_cluster->del(distributedLockKey);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        lkError.set("Unable to release the distributed lock id " + lockIdString + ". Possible connection error. Error=" + exceptionString, DL_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside releaseLock, it failed with an exception. Error=" << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return;
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        lkError.set("Unable to release the distributed lock id " + lockIdString + ". Error=" + exceptionString, DL_LOCK_RELEASE_ERROR);

        SPLAPPTRC(L_ERROR, "Inside releaseLock, it failed to release a lock using REDIS_DEL_CMD. Error=" << exceptionString << ". rc=" << DL_LOCK_RELEASE_ERROR, "RedisClusterPlusPlusDBLayer");
        return;
     }

     updateLockInformation(lockIdString, lkError, 0, 0, 0);
  } // End of releaseLock.

  bool RedisClusterPlusPlusDBLayer::updateLockInformation(std::string const & lockIdString,
     PersistenceError & lkError, uint32_t const & lockUsageCnt, int32_t const & lockExpirationTime, pid_t const & lockOwningPid) {
     // Get the lock name for this lock.
     uint32_t _lockUsageCnt = 0;
     int32_t _lockExpirationTime = 0;
     std::string _lockName = "";
     pid_t _lockOwningPid = 0;

     if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
        SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Let us update the lock information.
     // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
     std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
     ostringstream lockInfoValue;
     lockInfoValue << lockUsageCnt << "_" << lockExpirationTime << "_" << lockOwningPid << "_" << _lockName;
     string lockInfoValueString = lockInfoValue.str();
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        redis_cluster->set(lockInfoKey, lockInfoValueString);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        lkError.set("updateLockInformation: Unable to update 'LockId:LockInfo' for " + _lockName + ". Error=" + exceptionString + ". Possible connection error. Application code may call the DPS reconnect API and then retry the failed operation.", DL_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside updateLockInformation, it failed with an exception. Error="  << exceptionString << ". rc=" << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        // Problem in updating the "LockId:LockInfo" entry in the cache.
        lkError.set("Unable to update 'LockId:LockInfo' in the cache for a lock named " + _lockName +
        ". Error=" + exceptionString, DL_LOCK_INFO_UPDATE_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed for a lock named " << _lockName <<
           ". Error=" << exceptionString << ". rc=" <<
           DL_LOCK_INFO_UPDATE_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     return(true);
  } // End of updateLockInformation.

  bool RedisClusterPlusPlusDBLayer::readLockInformation(std::string const & lockIdString, PersistenceError & lkError, uint32_t & lockUsageCnt, int32_t & lockExpirationTime, pid_t & lockOwningPid, std::string & lockName) {
     // Read the contents of the lock information.
     lockName = "";

     // Lock Info contains meta data information about a given lock.
     // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
     string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;

     string get_result_value = "";
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        auto val = redis_cluster->get(lockInfoKey);

        if(val) {
           get_result_value = *val;
        }
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        lkError.set("readLockInformation: Unable to get LockInfo for " + lockIdString + ". Error=" + exceptionString + ". Possible connection error. Application code may call the DPS reconnect API and then retry the failed operation.", DL_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside readLockInformation, it failed with an exception. Error=" << exceptionString << "rc=" << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        // Unable to get the LockInfo from our cache.
        lkError.set("Unable to get LockInfo using the LockId " + lockIdString +
           ". Exception=" + exceptionString, DL_GET_LOCK_INFO_ERROR);
        return(false);
     }

     std::string lockInfo = get_result_value;
     // As shown in the comment line above, lock information is a string that has multiple pieces of
     // information each separated by an underscore character. We are interested in all the three tokens (lock usage count, lock expiration time, lock name).
     // Let us parse it now.
     std::vector<std::string> words;
     streams_boost::split(words, lockInfo, streams_boost::is_any_of("_"), streams_boost::token_compress_on);
     int32_t tokenCnt = 0;
     lockUsageCnt = 0;

     for (std::vector<std::string>::iterator it = words.begin(); it != words.end(); ++it) {
         string tmpString = *it;

         switch(++tokenCnt) {
            case 1:
               if (tmpString.empty() == false) {
                  lockUsageCnt = streams_boost::lexical_cast<uint32_t>(tmpString.c_str());
               }

               break;

            case 2:
               if (tmpString.empty() == false) {
                  lockExpirationTime = streams_boost::lexical_cast<int32_t>(tmpString.c_str());
               }

               break;

            case 3:
               if (tmpString.empty() == false) {
                  lockOwningPid = streams_boost::lexical_cast<int32_t>(tmpString.c_str());
               }

               break;

            case 4:
               lockName = *it;
               break;

            default:
               // If we keep getting more than 3 tokens, then it means that the lock name has
               // underscore character(s) in it. e-g: Super_Duper_Lock.
               lockName += "_" + *it;
         } // End of switch
      } // End of for loop.

      if (lockName == "") {
         // Unable to get the name of this lock.
         lkError.set("Unable to get the lock name for lockId " + lockIdString + ".", DL_GET_LOCK_NAME_ERROR);
         return(false);
      }

      return(true);
  } // End of readLockInformation.

  // This method will check if a lock exists for a given lock id.
  bool RedisClusterPlusPlusDBLayer::lockIdExistsOrNot(string lockIdString, PersistenceError & lkError) {
     string keyString = string(DL_LOCK_INFO_TYPE) + lockIdString;
     long long exists_result_value = 0;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        exists_result_value = redis_cluster->exists(keyString);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        lkError.set("lockIdExistsOrNot: LockIdExistsOrNot: Unable to connect to the redis-cluster server(s). Application code may call the DPS reconnect API and then retry the failed operation.", DL_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "Inside lockIdExistsOrNot, it failed with an exception. Error=" << exceptionString <<  ". Application code may call the DPS reconnect API and then retry the failed operation. " << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(false);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        // Unable to get the lock info for the given lock id.
        lkError.set("LockIdExistsOrNot: Unable to get LockInfo for the lockId " + lockIdString +
           ". Error=" + exceptionString, DL_GET_LOCK_INFO_ERROR);
        return(false);
     }

     if(exists_result_value == 1) {
        return(true);
     } else {
        return(false);
     }
  } // End of lockIdExistsOrNot.

  // This method will return the process id that currently owns the given lock.
  uint32_t RedisClusterPlusPlusDBLayer::getPidForLock(string const & name, PersistenceError & lkError) {
     SPLAPPTRC(L_DEBUG, "Inside getPidForLock with a name " << name, "RedisClusterPlusPlusDBLayer");

     string base64_encoded_name;
     base64_encode(name, base64_encoded_name);
     uint64_t lock = 0;

     // Let us first see if a lock with the given name already exists.
     // In our Redis dps implementation, data item keys can have space characters.
     // Inside Redis, all our lock names will have a mapping type indicator of
     // "5" at the beginning followed by the actual lock name.
     // '5' + 'lock name' ==> 'lock id'
     std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
     long long exists_result_value = 0;
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;
     
     try {
        exists_result_value = redis_cluster->exists(lockNameKey);
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        lkError.set("a) getPidForLock: Unable to connect to the redis-cluster server(s). Error=" + exceptionString + ". Application code may call the DPS reconnect API and then retry the failed operation.", DL_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "a) Inside getPidForLock, it failed for the lock named " << name << " with an exception. Error=" << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(0);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        // Unable to get the lock info for the given lock id.
        lkError.set("b) getPidForLock: It failed for the lock name " + name +
           " in the REDIS_EXISTS_CMD. Error=" + exceptionString, DL_GET_LOCK_INFO_ERROR);
        return(false);
     }

     if(exists_result_value == 0) {
        // Lock with the given name doesn't exist.
        lkError.set("d) Unable to find a lockName " + name + ".", DL_LOCK_NOT_FOUND_ERROR);
        SPLAPPTRC(L_DEBUG, "d) Inside getPidForLock, unable to find the lockName " << name << ". " << DL_LOCK_NOT_FOUND_ERROR, "RedisClusterPlusPlusDBLayer");
        return(0);
     }

     // This lock already exists in our cache.
     // We can get the lockId and return it to the caller.
     string get_result_value = "";
     exceptionString = "";
     exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        auto val = redis_cluster->get(lockNameKey);

        if(val) {
           get_result_value = *val;
        }
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        lkError.set("e) getPidForLock: Unable to connect to the redis-cluster server(s). Error=" + exceptionString + ". Application code may call the DPS reconnect API and then retry the failed operation.", DL_CONNECTION_ERROR);
        SPLAPPTRC(L_ERROR, "e) Inside getPidForLock, it failed for the lock named " << name << " with an exception. Error=" << exceptionString << ". Application code may call the DPS reconnect API and then retry the failed operation. " << DL_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        return(0);
     }

     // Did we encounter a redis reply error?
     if (exceptionType == REDIS_PLUS_PLUS_REPLY_ERROR || 
        exceptionType == REDIS_PLUS_PLUS_OTHER_ERROR) {
        // Unable to get an existing lock id from the cache.
        lkError.set("f) Unable to get the lockId for the lockName " + name + ". Error=" +
           exceptionString, DL_GET_LOCK_ID_ERROR);
        SPLAPPTRC(L_DEBUG, "f) Inside getPidForLock, it failed for the lockName " << name <<
           ". Error=" << exceptionString << ". rc=" <<
           DL_GET_LOCK_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        return(0);
     }

     if(get_result_value.length() <= 0) {
        // Unable to get the lock information. It is an abnormal error. Convey this to the caller.
        lkError.set("Redis returned an empty lockId for the lockName " + name + ".", DL_GET_LOCK_ID_ERROR);
        SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed with an empty lockId for the lockName " << name << ". " << DL_GET_LOCK_ID_ERROR, "RedisClusterPlusPlusDBLayer");
        return(0);
     }

     lock = streams_boost::lexical_cast<uint64_t>(get_result_value);
     // Read the lock information.
     ostringstream lockId;
     lockId << lock;
     string lockIdString = lockId.str();

     uint32_t _lockUsageCnt = 0;
     int32_t _lockExpirationTime = 0;
     std::string _lockName = "";
     pid_t _lockOwningPid = 0;

     if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
        SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "RedisClusterPlusPlusDBLayer");
        return(0);
     } else {
        return(_lockOwningPid);
     }
  } // End of getPidForLock.

  void RedisClusterPlusPlusDBLayer::persist(PersistenceError & dbError){
     // As of Nov/2020, there is no support for Wait command in the redis-plus-plus library for redis_cluster.
     // So, Wwe will use the generic command to execute this.
     std::vector<string> vec1;
     vec1.push_back(string("WAIT"));
     vec1.push_back("1");
     vec1.push_back("0");
     string result = "";
     runDataStoreCommand(vec1, result, dbError);

     if (result.compare(string("1")) != 0) {
        // Didn't write to at least one slave
        dbError.set("dpsPersist: return value should be at least 1.", DPS_MAKE_DURABLE_ERROR);
        SPLAPPTRC(L_ERROR, "dpsPersist: WAIT return value should be 1. But it is not.", "RedisClusterPlusPlusDBLayer");
     } else{
        SPLAPPTRC(L_DEBUG, "dpsPersist, WAIT returned successfully, wrote to " << result << " replica.", "RedisClusterPlusPlusDBLayer");
     }
  } // End of dpsPersist.

  // This method will return the status of the connection to the back-end data store.
  bool RedisClusterPlusPlusDBLayer::isConnected() {
     if (redis_cluster == NULL) {
        // There is no active connection.
        return(false);
     }

     // We will simply do a read API for a dummy key.
     // If it results in a connection error, that will tell us the status of the connection.
     string get_result_value = "";
     string exceptionString = "";
     int exceptionType = REDIS_PLUS_PLUS_NO_ERROR;

     try {
        auto val = redis_cluster->get("my_dummy_key");

        if(val) {
           get_result_value = *val;
        }
     } catch (const ReplyError &ex) {
        // WRONGTYPE Operation against a key holding the wrong kind of value
        exceptionString = ex.what();
        // Command execution error.
        exceptionType = REDIS_PLUS_PLUS_REPLY_ERROR;
     } catch (const TimeoutError &ex) {
        // Reading or writing timeout
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;        
     } catch (const ClosedError &ex) {
        // Connection has been closed.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const IoError &ex) {
        // I/O error on the connection.
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_CONNECTION_ERROR;
     } catch (const Error &ex) {
        // Other errors
        exceptionString = ex.what();
        // Connectivity related error.
        exceptionType = REDIS_PLUS_PLUS_OTHER_ERROR;
     }

     // Did we encounter a redis-cluster server connection error?
     if (exceptionType == REDIS_PLUS_PLUS_CONNECTION_ERROR) {
        SPLAPPTRC(L_ERROR, "Inside isConnected: Unable to connect to the redis-cluster server(s). Failed with an exception. Error=" << exceptionString << ". rc=" << DPS_CONNECTION_ERROR, "RedisClusterPlusPlusDBLayer");
        // Connection error.
        return(false);
     } else {
        // Connection is active.
        return(true);
     }
  } // End of isConnected.

  // This method will reestablish the status of the connection to the back-end data store.
  bool RedisClusterPlusPlusDBLayer::reconnect(std::set<std::string> & dbServers, PersistenceError & dbError) {
     if (redis_cluster != NULL) {
        // Delete the existing cluster connection object.
        delete redis_cluster;
        redis_cluster = NULL;
     }

     connectToDatabase(dbServers, dbError);

     if(dbError.hasError()) {
        // Connection didn't happen.
        // Caller can query the error code and error string using two other DPS APIs meant for that purpose.
        return(false);
     } else {
        // All good.
        return(true);
     }          
  } // End of reconnect.

} } } } }

using namespace com::ibm::streamsx::store;
using namespace com::ibm::streamsx::store::distributed;
extern "C" {
	DBLayer * create(){
	   return new RedisClusterPlusPlusDBLayer();
	}
}
