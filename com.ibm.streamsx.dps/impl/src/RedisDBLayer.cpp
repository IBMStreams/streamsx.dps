/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
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

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of Redis nor the names of its contributors may be used
  to endorse or promote products derived from this software without specific
  prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
=====================================================================
*/

/*
==================================================================================================================
This CPP file contains the source code for the distributed process store (dps) back-end activities such as
insert, update, read, remove, etc. This dps implementation runs on top of the popular redis in-memory store.
Redis is a simple, but a great open source effort that carries a BSD license.
Thanks to Salvatore Sanfilippo, who created Redis, when he was 30 years old in 2009. What an amazing raw talent!!!
In a (2011) interview, he nicely describes about the reason to start that marvelous project.

http://www.thestartup.eu/2011/01/an-interview-with-salvatore-sanfilippo-creator-of-redis-working-out-of-sicily/

Redis is a full fledged NoSQL data store with support for complex types such as list, set, and hash. It also has
APIs to perform store commands within a transaction block. Its replication, persistence, and cluster features are
far superior considering that its first release was done only in 2009. In our Redis store implementation for
Streams, we are using APIs from the popular hiredis C library.

Any dpsXXXXX native function call made from a SPL composite will go through a serialization layer and then hit
this CPP file. Here, the purpose of such SPL native function calls will be fulfilled using the redis APIs.
After that, the results will be sent to a deserialization layer. From there, results will be transformed using the
correct SPL types and delivered back to the SPL composite. In general, our distributed process store provides
a "global + distributed" in-memory cache for different processes (multiple PEs from one or more Streams applications).
We provide a set of free for all native function APIs to create/read/update/delete data items on one or more stores.
In the worst case, there could be multiple writers and multiple readers for the same store.
It is important to note that a Streams application designer/developer should carefully address how different parts
of his/her application will access the store simultaneously i.e. who puts what, who gets what and at
what frequency from where etc.

This C++ project has a companion SPL project (058_data_sharing_between_non_fused_spl_custom_and_cpp_primitive_operators).
Please refer to the commentary in that SPL project file for learning about the procedure to do an
end-to-end test run involving the SPL code, serialization/deserialization code,
redis interface code (this file), and your redis infrastructure.

As a first step, you should run the ./mk script from the C++ project directory (DistributedProcessStoreLib).
That will take care of building the .so file for the dps and copy it to the SPL project's impl/lib directory.
==================================================================================================================
*/

#include "RedisDBLayer.h"
#include "DpsConstants.h"

#include <SPL/Runtime/Common/RuntimeDebug.h>
#include <SPL/Runtime/Type/SPLType.h>
#include <SPL/Runtime/Function/TimeFunctions.h>
#include <SPL/Runtime/Function/UtilFunctions.h>

#include <iostream>
#include <unistd.h>
#include <sys/utsname.h>
#include <sstream>
#include <cassert>
#include <stdio.h>
#include <time.h>
#include <string>
#include <vector>
#include <streams_boost/lexical_cast.hpp>
#include <streams_boost/algorithm/string/erase.hpp>
#include <streams_boost/algorithm/string/split.hpp>
#include <streams_boost/algorithm/string/classification.hpp>
#include <streams_boost/archive/iterators/base64_from_binary.hpp>
#include <streams_boost/archive/iterators/binary_from_base64.hpp>
#include <streams_boost/archive/iterators/transform_width.hpp>
#include <streams_boost/archive/iterators/insert_linebreaks.hpp>
#include <streams_boost/archive/iterators/remove_whitespace.hpp>

using namespace std;
using namespace SPL;
using namespace streams_boost::archive::iterators;

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  RedisDBLayer::RedisDBLayer()
  {

  }

  RedisDBLayer::~RedisDBLayer()
  {
	if (redisPartitionCnt == 0) {
		// We are not using the client side Redis partitioning.
		// Clear the single redis connection we opened.
		// In this case of just a single Redis server instance being used,
		// its context address is stored in the very first element of the redis partition array.
		if (redisPartitions[0].rdsc != NULL) {
			redisFree(redisPartitions[0].rdsc);
			redisPartitions[0].rdsc = NULL;
		}
	} else {
		// We are using the client side Redis partitioning.
		// Let us clear all the connections we made.
		for(int32_t cnt=0; cnt < redisPartitionCnt; cnt++) {
			if (redisPartitions[cnt].rdsc != NULL) {
				redisFree(redisPartitions[cnt].rdsc);
				redisPartitions[cnt].rdsc = NULL;
			}
		}
	}
  }
        
  void RedisDBLayer::connectToDatabase(std::set<std::string> const & dbServers, PersistenceError & dbError)
  {
	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase", "RedisDBLayer");

	  // Get the name, OS version and CPU type of this machine.
	  struct utsname machineDetails;

	  if(uname(&machineDetails) < 0) {
		  dbError.set("Unable to get the machine/os/cpu details.", DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to get the machine/os/cpu details. " << DPS_INITIALIZE_ERROR, "RedisDBLayer");
		  return;
	  } else {
		  nameOfThisMachine = string(machineDetails.nodename);
		  osVersionOfThisMachine = string(machineDetails.sysname) + string(" ") + string(machineDetails.release);
		  cpuTypeOfThisMachine = string(machineDetails.machine);
	  }

	  string redisConnectionErrorMsg = "Unable to initialize the redis connection context.";
          // Senthil added this block of code on May/02/2017.
          // As part of the Redis configuration in the DPS config file, we now allow the user to specify
          // an optional Redis authentication password as shown below.
          // server:port:RedisPassword
          string targetServerPassword = "";

	  // When the Redis cluster releases with support for the hiredis client, then change this logic to
	  // take advantage of the Redis cluster features.
	  //
	  // If the user configured only one redis server, connect to it using unixsocket or TCP.
	  // If the user configured multiple redis servers, then we are going to do the client side
	  // partitioning. In that case, we will connect to all of them and get a separate handle
	  // and store them in an array of structures.
	  //
	  if (dbServers.size() == 1) {
		  // This means, no client side Redis partitioning.
		  redisPartitionCnt = 0;
		  // We have only one Redis server configured by the user.
		  for (std::set<std::string>::iterator it=dbServers.begin(); it!=dbServers.end(); ++it) {
			  std::string serverName = *it;
			  // If the user has configured to use the unix domain socket, take care of that as well.
			  if (serverName == "unixsocket") {
				  redisPartitions[0].rdsc = redisConnectUnix((char *)"/tmp/redis.sock");
			  } else {
				  struct timeval timeout = { 1, 500000 }; // 1.5 seconds {tv_sec, tv_microsecs}
	              // Redis server name can have port number specified along with it --> MyHost:2345
	              string targetServerName = "";
	              int targetServerPort = 0;
	              char serverNameBuf[300];
	              strcpy(serverNameBuf, serverName.c_str());
	              char *ptr = strtok(serverNameBuf, ":");

	              while(ptr) {
	                if (targetServerName == "") {
	                  // This must be our first token.
	                  targetServerName = string(ptr);
	                  ptr = strtok(NULL, ":");
	                } else if (targetServerPort == 0){
	                  // This must be our second token.
	                  targetServerPort = atoi(ptr);
                          
                          if (targetServerPort == 0) {
                             targetServerPort = REDIS_SERVER_PORT;
                          }

	                  ptr = strtok(NULL, ":");
	                } else if (targetServerPassword == "") {
                          // This must be our third token.
                          targetServerPassword = string(ptr);
                          // We are done.
                          break;
                        }
	              }

	              if (targetServerName == "") {
	                // User only specified the server name and no port.
	            	// (This is the case of server name followed by a : character with a missing port number)
	                targetServerName = serverName;
	                // In this case, use the default Redis server port.
	                targetServerPort = REDIS_SERVER_PORT;
	              }

	              if (targetServerPort == 0) {
	                // User didn't give a Redis server port.
	            	// Only a server name was given not followed by a : character.
	                // Use the default Redis server port.
	                targetServerPort = REDIS_SERVER_PORT;
	              }

	              char msg[128];
	              sprintf(msg, "%d", targetServerPort);
	              SPLAPPTRC(L_ERROR, "Connecting to the Redis server " << targetServerName << " on port " << string(msg), "RedisDBLayer");
	              redisPartitions[0].rdsc = redisConnectWithTimeout(targetServerName.c_str(), targetServerPort, timeout);
			  }

			  if (redisPartitions[0].rdsc == NULL || redisPartitions[0].rdsc->err) {
				  if (redisPartitions[0].rdsc) {
					  redisConnectionErrorMsg += " Connection error: " + string(redisPartitions[0].rdsc->errstr);
				  }
			  } else {
				  // We connected to at least one redis server. That is enough for our needs.
                                  // If the user configured it with a Redis auth password, then we must authenticate now.
                                  // If the authentication is successful, all good. If any error, Redis will send one of the
                                  // following two errors:
                                  // ERR invalid password  (OR) ERR Client sent AUTH, but no password is set
                                  if (targetServerPassword.length() > 0) {
	                             std::string cmd = string(REDIS_AUTH_CMD) + targetServerPassword;
	                             redis_reply = (redisReply*)redisCommand(redisPartitions[0].rdsc, cmd.c_str());

	                             // If we get a NULL reply, then it indicates a redis server connection error.
	                             if (redis_reply == NULL) {
		                        // When this error occurs, we can't reuse that redis context for further server commands. This is a serious error.
		                        dbError.set("Unable to authenticate to the redis server(s). Possible connection breakage. " + std::string(redisPartitions[0].rdsc->errstr), DPS_CONNECTION_ERROR);
		                        SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed during authentication with an error " << string("Possible connection breakage. ") << DPS_CONNECTION_ERROR, "RedisDBLayer");
		                        return;
	                             }

	                             if (redis_reply->type == REDIS_REPLY_ERROR) {
		                        dbError.set("Unable to authenticate to the Redis server. Error msg=" + std::string(redis_reply->str), DPS_AUTHENTICATION_ERROR);
		                        SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed during authentication. error=" << redis_reply->str << ", rc=" << DPS_AUTHENTICATION_ERROR, "RedisDBLayer");
		                        freeReplyObject(redis_reply);
		                        return; 
	                             }                                 

                                     freeReplyObject(redis_reply);
                                  } // End of Redis authentication.

				  // Reset the error string.
				  redisConnectionErrorMsg = "";
				  break;
			  }
		  }

		  // Check if there was any connection error.
		  if (redisConnectionErrorMsg != "") {
			  if (redisPartitions[0].rdsc != NULL) {
				  redisFree(redisPartitions[0].rdsc);
				  redisPartitions[0].rdsc = NULL;
			  }

			  dbError.set(redisConnectionErrorMsg, DPS_INITIALIZE_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error '" << redisConnectionErrorMsg << "'. " << DPS_INITIALIZE_ERROR, "RedisDBLayer");
			  return;
		  }
	  } else {
		  // We have more than one Redis server configured by the user.
		  // In our dps toolkit, we allow only upto 50 servers. (It is just our own limit).
		  if (dbServers.size() > 50) {
			  redisConnectionErrorMsg += " Too many Redis servers configured. DPS toolkit supports only a maximum of 50 Redis servers.";
			  dbError.set(redisConnectionErrorMsg, DPS_TOO_MANY_REDIS_SERVERS_CONFIGURED);
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error '" << redisConnectionErrorMsg << "'. " << DPS_TOO_MANY_REDIS_SERVERS_CONFIGURED, "RedisDBLayer");
			  return;
		  }

		  redisPartitionCnt = dbServers.size();
		  int32_t idx = -1;
		  // Now stay in a loop and connect to each of them.
		  for (std::set<std::string>::iterator it=dbServers.begin(); it!=dbServers.end(); ++it) {
			  std::string serverName = *it;
			  struct timeval timeout = { 1, 500000 }; // 1.5 seconds {tv_sec, tv_microsecs}
			  // In the case of client side Redis server partitioning, we expect the user to configure the ports of
			  // their Redis servers starting from our REDIS_SERVER_PORT (base port number 6379) + 2 and go up by one for each new Redis server.
			  idx++;

              // Redis server name can have port number and password specified along with it --> MyHost:2345:MyPassword
              targetServerPassword = "";
              string targetServerName = "";
              int targetServerPort = 0;
              char serverNameBuf[300];
              strcpy(serverNameBuf, serverName.c_str());
              char *ptr = strtok(serverNameBuf, ":");

              while(ptr) {
	         if (targetServerName == "") {
	            // This must be our first token.
	            targetServerName = string(ptr);
	            ptr = strtok(NULL, ":");
	         } else if (targetServerPort == 0){
	            // This must be our second token.
	            targetServerPort = atoi(ptr);
                          
                    if (targetServerPort == 0) {
                       targetServerPort = REDIS_SERVER_PORT;
                    }

	            ptr = strtok(NULL, ":");
	         } else if (targetServerPassword == "") {
                    // This must be our third token.
                    targetServerPassword = string(ptr);
                    // We are done.
                    break;
                 }
              }

              if (targetServerName == "") {
                // User only specified the server name and no port.
            	// (This is the case of server name followed by a : character with a missing port number)
                targetServerName = serverName;
                // In this case, use the default Redis server port.
                targetServerPort = REDIS_SERVER_PORT;
              }

              if (targetServerPort == 0) {
                // User didn't give a Redis server port.
            	// Only a server name was given not followed by a : character.
                // Use the default Redis server port.
                targetServerPort = REDIS_SERVER_PORT;
              }

              char msg[128];
              sprintf(msg, "%d", targetServerPort);
              SPLAPPTRC(L_ERROR, "Connecting to the Redis server " << targetServerName << " on port " << string(msg), "RedisDBLayer");
			  redisPartitions[idx].rdsc = redisConnectWithTimeout(targetServerName.c_str(), targetServerPort, timeout);

			  if (redisPartitions[idx].rdsc == NULL || redisPartitions[idx].rdsc->err) {
				  if (redisPartitions[idx].rdsc) {
					  char serverNumber[50];
					  sprintf(serverNumber, "%d", idx+1);
					  redisConnectionErrorMsg += " Connection error for Redis server " + serverName + ". Error="  + string(redisPartitions[idx].rdsc->errstr);
				  }

				  // Since we got a connection error on one of the servers, let us disconnect from the servers that we successfully connected to so far.
				  // Loop backwards.
				  for(int32_t cnt=idx; cnt >=0; cnt--) {
					  if (redisPartitions[cnt].rdsc != NULL) {
						  redisFree(redisPartitions[cnt].rdsc);
						  redisPartitions[cnt].rdsc = NULL;
					  }
				  }

				  dbError.set(redisConnectionErrorMsg, DPS_INITIALIZE_ERROR);
				  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error '" << redisConnectionErrorMsg << "'. " << DPS_INITIALIZE_ERROR, "RedisDBLayer");
				  return;
			  }

                          // If the user configured it with a Redis auth password, then we must authenticate now.
                          if (targetServerPassword.length() > 0) {
	                     std::string cmd = string(REDIS_AUTH_CMD) + targetServerPassword;
	                     redis_reply = (redisReply*)redisCommand(redisPartitions[idx].rdsc, cmd.c_str());

	                     // If we get a NULL reply, then it indicates a redis server connection error.
	                     if (redis_reply == NULL) {
		                // When this error occurs, we can't reuse that redis context for further server commands. This is a serious error.
		                dbError.set("Unable to authenticate to the redis server(s). Possible connection breakage. " + std::string(redisPartitions[idx].rdsc->errstr), DPS_CONNECTION_ERROR);
		                SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed during authentication with an error " << string("Possible connection breakage. ") << DPS_CONNECTION_ERROR, "RedisDBLayer");

				// Since we got a connection error on one of the servers, let us disconnect from the servers that we successfully connected to so far.
				// Loop backwards.
				for(int32_t cnt=idx; cnt >=0; cnt--) {
			           if (redisPartitions[cnt].rdsc != NULL) {
				      redisFree(redisPartitions[cnt].rdsc);
				      redisPartitions[cnt].rdsc = NULL;
				   }
				}

		                return;
	                     }

	                     if (redis_reply->type == REDIS_REPLY_ERROR) {
		                dbError.set("Unable to authenticate to the Redis server. Error msg=" + std::string(redis_reply->str), DPS_AUTHENTICATION_ERROR);
		                SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed during authentication. error=" << redis_reply->str << ", rc=" << DPS_AUTHENTICATION_ERROR, "RedisDBLayer");

				// Since we got an authentication error on one of the servers, let us disconnect from the servers that we successfully connected to so far.
				// Loop backwards.
				for(int32_t cnt=idx; cnt >=0; cnt--) {
			           if (redisPartitions[cnt].rdsc != NULL) {
				      redisFree(redisPartitions[cnt].rdsc);
				      redisPartitions[cnt].rdsc = NULL;
				   }
				}


		                freeReplyObject(redis_reply);
		                return; 
	                     }                                 

                             freeReplyObject(redis_reply);
                          } // End of Redis authentication.

		  } // End of for loop.
	  }

	  // We have now made connection to one or more servers in a redis cluster.
	  // Let us check if the global storeId key:value pair is already there in the cache.
	  string keyString = string(DPS_AND_DL_GUID_KEY);
	  int32_t partitionIdx = getRedisServerPartitionIndex(keyString);
	  std::string cmd = string(REDIS_EXISTS_CMD) + keyString;
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	  // If we get a NULL reply, then it indicates a redis server connection error.
	  if (redis_reply == NULL) {
		// This is how we can detect that a wrong redis server name is configured by the user or
		// not even a single redis server daemon being up and running.
		// On such errors, redis context will carry an error string.
		// When this error occurs, we can't reuse that redis context for further server commands. This is a serious error.
		dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error " << DPS_CONNECTION_ERROR, "RedisDBLayer");
		return;
	  }

	  if (redis_reply->type == REDIS_REPLY_ERROR) {
		  dbError.set("Unable to check the existence of the dps GUID key. Error=" + string(redis_reply->str), DPS_KEY_EXISTENCE_CHECK_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed. Error=" << string(redis_reply->str) << ", rc=" << DPS_KEY_EXISTENCE_CHECK_ERROR, "RedisDBLayer");
		  freeReplyObject(redis_reply);
		  return;
	  }

	  if (redis_reply->integer == (int)0) {
		  // It could be that our global store id is not there now.
		  // Let us create one with an initial value of 0.
		  // Redis setnx is an atomic operation. It will succeed only for the very first operator that
		  // attempts to do this setting after a redis server is started fresh. If some other operator
		  // already raced us ahead and created this guid_key, then our attempt below will be safely rejected.
		  freeReplyObject(redis_reply);
		  cmd = string(REDIS_SETNX_CMD) + keyString + string(" ") + string("0");
		  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
	  }

	  freeReplyObject(redis_reply);
	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase done", "RedisDBLayer");
  }

  uint64_t RedisDBLayer::createStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside createStore for store " << name, "RedisDBLayer");

	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);

 	// Get a general purpose lock so that only one thread can
	// enter inside of this method at any given time with the same store name.
 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
 		// Unable to acquire the general purpose lock.
 		dbError.set("Unable to get a generic lock for creating a store with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for an yet to be created store with its name as " <<
 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "RedisDBLayer");
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
 	int32_t partitionIdx = getRedisServerPartitionIndex(keyString);
	std::string cmd = string(REDIS_EXISTS_CMD) + keyString;
	redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	if (redis_reply == NULL) {
		dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_CONNECTION_ERROR, "RedisDBLayer");
		releaseGeneralPurposeLock(base64_encoded_name);
		return 0;
	}

	if (redis_reply->type == REDIS_REPLY_ERROR) {
		dbError.set("Unable to check the existence of a store with a name" + name + ".", DPS_KEY_EXISTENCE_CHECK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to check for a store existence. rc=" << DPS_KEY_EXISTENCE_CHECK_ERROR, "RedisDBLayer");
		freeReplyObject(redis_reply);
		releaseGeneralPurposeLock(base64_encoded_name);
		return 0;
	  }

	if (redis_reply->integer == (int)1) {
		// This store already exists in our cache.
		// We can't create another one with the same name now.
		dbError.set("A store named " + name + " already exists", DPS_STORE_EXISTS);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_STORE_EXISTS, "RedisDBLayer");
		freeReplyObject(redis_reply);
		releaseGeneralPurposeLock(base64_encoded_name);
		return 0;
	}

	if (redis_reply->integer == (int)0) {
		// Create a new store.
		// At first, let us increment our global dps_guid to reserve a new store id.
		freeReplyObject(redis_reply);
		uint64_t storeId = 0;
		keyString = string(DPS_AND_DL_GUID_KEY);
		partitionIdx = getRedisServerPartitionIndex(keyString);
		cmd = string(REDIS_INCR_CMD) + keyString;
		redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

		if (redis_reply->type == REDIS_REPLY_ERROR) {
			dbError.set("Unable to get a unique store id for a store named " + name + ". " + std::string(redis_reply->str), DPS_GUID_CREATION_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_GUID_CREATION_ERROR, "RedisDBLayer");
			freeReplyObject(redis_reply);
			releaseGeneralPurposeLock(base64_encoded_name);
			return 0;
		}

		if (redis_reply->type == REDIS_REPLY_INTEGER) {
			storeId = redis_reply->integer;
			freeReplyObject(redis_reply);

			/*
			We secured a guid. We can now create this store. Layout for a distributed process store (dps) looks like this.
			****************************************************************************************************************************************************************
			* 1) Create a root entry called "Store Name":  '0' + 'store name' => 'store id'                                                                                *
			* 2) Create "Store Contents Hash": '1' + 'store id' => 'Redis Hash'                                                                                            *
			*       This hash will always have the following three metadata entries:                                                                                       *
			*       dps_name_of_this_store ==> 'store name'                                                                                                     		   *
			*       dps_spl_type_name_of_key ==> 'spl type name for this store's key'                                                                                      *
			*       dps_spl_type_name_of_value ==> 'spl type name for this store's value'                                                                                  *
			* 3) In addition, we will also create and delete custom locks for modifying store contents in  (2) above: '4' + 'store id' + 'dps_lock' => 1                   *
			*                                                                                                                                                              *
			*                                                                                                                                                              *
			* 4) Create a root entry called "Lock Name":  '5' + 'lock name' ==> 'lock id'    [This lock is used for performing store commands in a transaction block.]     *
			* 5) Create "Lock Info":  '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'lock name' *
			* 6) In addition, we will also create and delete user-defined locks: '7' + 'lock id' + 'dl_lock' => 1                                                          *
			*                                                                                                                                                              *
			* 7) We will also allow general purpose locks to be created by any entity for sundry use:  '501' + 'entity name' + 'generic_lock' => 1                         *
			****************************************************************************************************************************************************************
			*/
			//
			// 1) Create the Store Name
			//    '0' + 'store name' => 'store id'
			std::ostringstream value;
			value << storeId;
			keyString = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
			partitionIdx = getRedisServerPartitionIndex(keyString);
			cmd = string(REDIS_SET_CMD) + keyString + " " + value.str();
			redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

			if (redis_reply->type == REDIS_REPLY_ERROR) {
				// Problem in creating the "Store Name" entry in the cache.
				dbError.set("Unable to create 'StoreName:StoreId' in the cache for a store named " + name + ". " + std::string(redis_reply->str), DPS_STORE_NAME_CREATION_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_STORE_NAME_CREATION_ERROR, "RedisDBLayer");
				// We are simply leaving an incremented value for the dps_guid key in the cache that will never get used.
				// Since it is harmless, there is no need to reduce this number by 1. It is okay that this guid number will remain unassigned to any store.
				freeReplyObject(redis_reply);
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
			// Let us create a new store contents hash with a mandatory element that will carry the name of this store.
			// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
			freeReplyObject(redis_reply);
			// StoreId becomes the new key now.
			keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + value.str();
			partitionIdx = getRedisServerPartitionIndex(keyString);
			cmd = string(REDIS_HSET_CMD) + keyString + " " +
				string(REDIS_STORE_ID_TO_STORE_NAME_KEY) + " " + base64_encoded_name;
			redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

			if (redis_reply->type == REDIS_REPLY_ERROR) {
				// Problem in creating the "Store Content Hash" metadata1 entry in the cache.
				dbError.set("Unable to create 'Store Contents Hash' in the cache for the store named " + name + ". " + std::string(redis_reply->str), DPS_STORE_HASH_METADATA1_CREATION_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "RedisDBLayer");
				freeReplyObject(redis_reply);
				// Delete the previous store name root entry we made.
				keyString = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
				partitionIdx = getRedisServerPartitionIndex(keyString);
				cmd = string(REDIS_DEL_CMD) + keyString;
				redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
				freeReplyObject(redis_reply);
				releaseGeneralPurposeLock(base64_encoded_name);
				return 0;
			}

			freeReplyObject(redis_reply);
		    // We are now going to save the SPL type names of the key and value as part of this
		    // store's metadata. That will help us in the Java dps API "findStore" to cache the
		    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
			// Add the key spl type name metadata.
			string base64_encoded_keySplTypeName;
			base64_encode(keySplTypeName, base64_encoded_keySplTypeName);

			cmd = string(REDIS_HSET_CMD) + keyString + " " +
				string(REDIS_SPL_TYPE_NAME_OF_KEY) + " " + base64_encoded_keySplTypeName;
			redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

			if (redis_reply->type == REDIS_REPLY_ERROR) {
				// Problem in creating the "Store Content Hash" metadata2 entry in the cache.
				dbError.set("Unable to create 'Store Contents Hash' in the cache for the store named " + name + ". " + std::string(redis_reply->str), DPS_STORE_HASH_METADATA2_CREATION_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "RedisDBLayer");
				freeReplyObject(redis_reply);
				// Delete the store contents hash we created above.
				cmd = string(REDIS_DEL_CMD) + keyString;
				redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
				freeReplyObject(redis_reply);
				// Delete the previous store name root entry we made.
				keyString = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
				partitionIdx = getRedisServerPartitionIndex(keyString);
				cmd = string(REDIS_DEL_CMD) + keyString;
				redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
				freeReplyObject(redis_reply);
				releaseGeneralPurposeLock(base64_encoded_name);
				return 0;
			}

			freeReplyObject(redis_reply);
			string base64_encoded_valueSplTypeName;
			base64_encode(valueSplTypeName, base64_encoded_valueSplTypeName);

			// Add the value spl type name metadata.
			cmd = string(REDIS_HSET_CMD) + keyString + " " +
				string(REDIS_SPL_TYPE_NAME_OF_VALUE) + " " + base64_encoded_valueSplTypeName;
			redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

			if (redis_reply->type == REDIS_REPLY_ERROR) {
				// Problem in creating the "Store Content Hash" metadata3 entry in the cache.
				dbError.set("Unable to create 'Store Contents Hash' in the cache for the store named " + name + ". " + std::string(redis_reply->str), DPS_STORE_HASH_METADATA3_CREATION_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "RedisDBLayer");
				freeReplyObject(redis_reply);
				// Delete the store contents hash we created above.
				cmd = string(REDIS_DEL_CMD) + keyString;
				redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
				freeReplyObject(redis_reply);
				// Delete the previous store name root entry we made.
				keyString = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
				partitionIdx = getRedisServerPartitionIndex(keyString);
				cmd = string(REDIS_DEL_CMD) + keyString;
				redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
				freeReplyObject(redis_reply);
				releaseGeneralPurposeLock(base64_encoded_name);
				return 0;
			}

			freeReplyObject(redis_reply);
			releaseGeneralPurposeLock(base64_encoded_name);
			return(storeId);
		}
	}

	freeReplyObject(redis_reply);
	releaseGeneralPurposeLock(base64_encoded_name);
    return 0;
  }

  uint64_t RedisDBLayer::createOrGetStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	// We will rely on a method above this and another method below this to accomplish what is needed here.
	SPLAPPTRC(L_DEBUG, "Inside createOrGetStore for store " << name, "RedisDBLayer");

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
  }
                
  uint64_t RedisDBLayer::findStore(std::string const & name, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside findStore for store " << name, "RedisDBLayer");

	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);

	// Let us first see if this store already exists.
	// Inside Redis, all our store names will have a mapping type indicator of
	// "0" at the beginning followed by the actual store name.  "0" + 'store name'
	std::string storeNameKey = DPS_STORE_NAME_TYPE + base64_encoded_name;
	int32_t partitionIdx = getRedisServerPartitionIndex(storeNameKey);
	string cmd = string(REDIS_EXISTS_CMD) + storeNameKey;
	redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	if (redis_reply == NULL) {
		dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed for store " << name << ". " << DPS_CONNECTION_ERROR, "RedisDBLayer");
		return 0;
	}

	if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Problem in finding the existence of a store.
		dbError.set("Unable to find the existence of a store named " + name + ". " + std::string(redis_reply->str), DPS_STORE_EXISTENCE_CHECK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed for store " << name << ". " << DPS_STORE_EXISTENCE_CHECK_ERROR, "RedisDBLayer");
		freeReplyObject(redis_reply);
		return(0);
	}

	if (redis_reply->integer == (int)0) {
		// This store is not there in our cache.
		dbError.set("Store named " + name + " not found.", DPS_STORE_DOES_NOT_EXIST);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed for store " << name << ". " << DPS_STORE_DOES_NOT_EXIST, "RedisDBLayer");
		freeReplyObject(redis_reply);
		return 0;
	}

	// It is an existing store.
	// We can get the storeId and return it to the caller.
	freeReplyObject(redis_reply);
	cmd = string(REDIS_GET_CMD) + storeNameKey;
	redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Unable to get an existing store id from the cache.
		dbError.set("Unable to get the storeId for the storeName " + name + ". " + std::string(redis_reply->str), DPS_GET_STORE_ID_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed for store " << name << ". " << DPS_GET_STORE_ID_ERROR, "RedisDBLayer");
		freeReplyObject(redis_reply);
		return(0);
	} else if ((redis_reply->type == REDIS_REPLY_NIL) || (redis_reply->len <= 0)) {
		// Requested data item is not there in the cache.
		dbError.set("The requested store " + name + " doesn't exist.", DPS_DATA_ITEM_READ_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed for store " << name << ". " << DPS_DATA_ITEM_READ_ERROR, "RedisDBLayer");
		freeReplyObject(redis_reply);
		return(0);
	} else {
		uint64_t storeId = streams_boost::lexical_cast<uint64_t>(redis_reply->str);
		freeReplyObject(redis_reply);
		return(storeId);
	}

	return 0;
  }
        
  bool RedisDBLayer::removeStore(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside removeStore for store id " << store, "RedisDBLayer");

	ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "RedisDBLayer");
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
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		releaseStoreLock(storeIdString);
		// This is alarming. This will put this store in a bad state. Poor user has to deal with it.
		return(false);
	}

	string cmd = "";

	// Let us delete the Store Contents Hash that contains all the active data items in this store.
	// '1' + 'store id' => 'Redis Hash'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeContentsHashKey = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
	int32_t partitionIdx = getRedisServerPartitionIndex(storeContentsHashKey);
	cmd = string(REDIS_DEL_CMD) + storeContentsHashKey;
	redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
	freeReplyObject(redis_reply);

	// Finally, delete the StoreName key now.
	string storeNameKey = string(DPS_STORE_NAME_TYPE) + storeName;
	partitionIdx = getRedisServerPartitionIndex(storeNameKey);
	cmd = string(REDIS_DEL_CMD) + storeNameKey;
	redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
	freeReplyObject(redis_reply);

	// Life of this store ended completely with no trace left behind.
	releaseStoreLock(storeIdString);
    return(true);
  }

  // This is a lean and mean put operation into a store.
  // It doesn't do any safety checks before putting a data item into a store.
  // If you want to go through that rigor, please use the putSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool RedisDBLayer::put(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside put for store id " << store, "RedisDBLayer");

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
	int32_t partitionIdx = getRedisServerPartitionIndex(keyString);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);

	string cmd = string(REDIS_HSET_CMD) + keyString + " " +
			base64_encoded_data_item_key + " " +  "%b";
	// We want to pass the exact binary data item value as given to us by the caller of this method.
	redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str(), (const char*)valueData, (size_t)valueSize);

	if (redis_reply == NULL) {
		dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed for store " << storeIdString << ". " << DPS_CONNECTION_ERROR, "RedisDBLayer");
		return(false);
	}

	if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Problem in storing a data item in the cache.
		dbError.set("Unable to store a data item in the store id " + storeIdString + ". " + std::string(redis_reply->str), DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed for store id " << storeIdString << ". " << DPS_DATA_ITEM_WRITE_ERROR, "RedisDBLayer");
		freeReplyObject(redis_reply);
		return(false);
	}

	freeReplyObject(redis_reply);
	return(true);
  }

  // This is a special bullet proof version that does several safety checks before putting a data item into a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular put method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool RedisDBLayer::putSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside putSafe for store id " << store, "RedisDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisDBLayer");
		}

		return(false);
	}

	// In our Redis dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "RedisDBLayer");
		// User has to retry again to put the data item in the store.
		return(false);
	}

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Contents Hash that takes the following format.
	// '1' + 'store id' => 'Redis Hash'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	// To support space characters in the data item key, let us base64 encode it.
	string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
	int32_t partitionIdx = getRedisServerPartitionIndex(keyString);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);

	string cmd = string(REDIS_HSET_CMD) + keyString + " " +
		base64_encoded_data_item_key + " " +  "%b";
	// We want to pass the exact binary data item value as given to us by the caller of this method.
	redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str(), (const char*)valueData, (size_t)valueSize);

	if (redis_reply == NULL) {
		dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store " << storeIdString << ". " << DPS_CONNECTION_ERROR, "RedisDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Problem in storing a data item in the cache.
		dbError.set("Unable to store a data item in the store id " + storeIdString + ". " + std::string(redis_reply->str), DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << DPS_DATA_ITEM_WRITE_ERROR, "RedisDBLayer");
		freeReplyObject(redis_reply);
		releaseStoreLock(storeIdString);
		return(false);
	}

	freeReplyObject(redis_reply);
	releaseStoreLock(storeIdString);
	return(true);
  }

  // Put a data item with a TTL (Time To Live in seconds) value into the global area of the Redis DB.
  bool RedisDBLayer::putTTL(char const * keyData, uint32_t keySize,
		  	  	  	  	    unsigned char const * valueData, uint32_t valueSize,
							uint32_t ttl, PersistenceError & dbError, bool encodeKey, bool encodeValue)
  {
	  SPLAPPTRC(L_DEBUG, "Inside putTTL.", "RedisDBLayer");

	  std::ostringstream ttlValue;
	  ttlValue << ttl;

	  // In our Redis dps implementation, data item keys can have space characters.
	  string base64_encoded_data_item_key;

          if (encodeKey == true) {
	     base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
          } else {
            // Since the key data sent here will always be in the network byte buffer format (NBF), 
            // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
            // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
            // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
            if ((uint8_t)keyData[0] < 0x80) {
               // Skip the first length byte. 
               base64_encoded_data_item_key = string(&keyData[1], keySize-1);  
            } else {
               // Skip the five bytes at the beginning that represent the length of the key data.
               base64_encoded_data_item_key = string(&keyData[5], keySize-5);
            }
          }

          string value_as_plain_string = "";
          int32_t valueIdx = 0;
          string argvStyleRedisCommand = "";

          if (encodeValue == false) {
             // Caller wants to store the value as plain string. Do the same thing we did above for the key.
             if ((uint8_t)valueData[0] < 0x80) {
                value_as_plain_string = string((char const *) &valueData[1], valueSize-1);
                valueIdx = 1;
             } else {
                value_as_plain_string = string((char const *) &valueData[5], valueSize-5); 
                valueIdx = 5;
             }
          }

	  int32_t partitionIdx = getRedisServerPartitionIndex(base64_encoded_data_item_key);
	  // We are ready to either store a new data item or update an existing data item with a TTL value specified in seconds.
	  // To support space characters in the data item key, let us base64 encode it.
	  string cmd = "";

	  if (ttl > 0) {
		  cmd = string(REDIS_SETX_CMD) + base64_encoded_data_item_key + " " + ttlValue.str() + " " + "%b";
                  argvStyleRedisCommand = string(REDIS_SETX_CMD);
                  // Strip the space at the end of the command that should not be there for the argv style Redis command.
                  argvStyleRedisCommand = argvStyleRedisCommand.substr(0, argvStyleRedisCommand.size()-1);            
	  } else {
		  // TTL value specified by the user is 0.
		  // User wants to use the dpsXXXXTTL APIs instead of the other store based APIs for the sake of simplicity.
		  // In that case, we will let the user store in the global area for their K/V pair to remain forever or until it is deleted by the user.
		  // No TTL effect needed here.
		  cmd = string(REDIS_SET_CMD) + base64_encoded_data_item_key + " " + "%b";
                  argvStyleRedisCommand = string(REDIS_SET_CMD);
                  // Strip the space at the end of the command that should not be there for the argv style Redis command.
                  argvStyleRedisCommand = argvStyleRedisCommand.substr(0, argvStyleRedisCommand.size()-1);              
	  }

          if (encodeKey == true || encodeValue == true) {
	     // We want to pass the exact binary data item value as given to us by the caller of this method.
	     redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str(), (const char*)(&valueData[valueIdx]), (size_t)(valueSize-valueIdx));
          } else {
             // This is the case where the caller doesn't want to encode the key as well as the value.
             // encodeKey == false and encodeValue == false.
             // Generally applicable when the caller wants to store both the key and value as strings in clear with no encoding.
             vector<const char *> argv;
             vector<size_t> argvlen;

             // Using the argv style Redis command will allow us to have spaces and quotes in the keys and values.
             argv.push_back(argvStyleRedisCommand.c_str());
             argvlen.push_back(argvStyleRedisCommand.size());

             argv.push_back(base64_encoded_data_item_key.c_str());
             argvlen.push_back(base64_encoded_data_item_key.size());

             argv.push_back(ttlValue.str().c_str());
             argvlen.push_back(ttlValue.str().size());

             argv.push_back(value_as_plain_string.c_str());
             argvlen.push_back(value_as_plain_string.size());
             redis_reply = (redisReply*) redisCommandArgv(redisPartitions[partitionIdx].rdsc, argv.size(), &(argv[0]), &(argvlen[0]));
          }

	  if (redis_reply == NULL) {
		  dbError.setTTL("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside putTTL, it failed for executing the set command. Error=" << std::string(redisPartitions[partitionIdx].rdsc->errstr) << ". " << DPS_CONNECTION_ERROR, "RedisDBLayer");
		  return(false);
	  }

	  if (redis_reply->type == REDIS_REPLY_ERROR) {
		  // Problem in storing a data item in the cache with TTL.
		  dbError.setTTL("Unable to store a data item with TTL. " + std::string(redis_reply->str), DPS_DATA_ITEM_WRITE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside putTTL, it failed to store a data item with TTL. Error=" + std::string(redis_reply->str) << ". " << DPS_DATA_ITEM_WRITE_ERROR, "RedisDBLayer");
		  freeReplyObject(redis_reply);
		  return(false);
	  }

	  freeReplyObject(redis_reply);
	  return(true);
  }

  // This is a lean and mean get operation from a store.
  // It doesn't do any safety checks before getting a data item from a store.
  // If you want to go through that rigor, please use the getSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool RedisDBLayer::get(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside get for store id " << store, "RedisDBLayer");

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
		SPLAPPTRC(L_DEBUG, "Inside get, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
	}

    return(result);
  }

  // This is a special bullet proof version that does several safety checks before getting a data item from a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular get method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool RedisDBLayer::getSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside getSafe for store id " << store, "RedisDBLayer");

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
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisDBLayer");
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
		SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
	}

    return(result);
  }

  // Get a TTL based data item that is stored in the global area of the Redis DB.
   bool RedisDBLayer::getTTL(char const * keyData, uint32_t keySize,
                              unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError, bool encodeKey)
   {
		SPLAPPTRC(L_DEBUG, "Inside getTTL.", "RedisDBLayer");

		// Let us get this data item from the cache as it is.
		// Since there could be multiple data writers, we are going to get whatever is there now.
		// It is always possible that the value for the requested item can change right after
		// you read it due to the data write made by some other thread. Such is life in a global distributed in-memory store.

		// In our Redis dps implementation, data item keys can have space characters.
		string base64_encoded_data_item_key;

                if (encodeKey == true) {
	           base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
                } else {
                   // Since the key data sent here will always be in the network byte buffer format (NBF), 
                   // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
                   // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
                   // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
                   if ((uint8_t)keyData[0] < 0x80) {
                      // Skip the first length byte. 
                      base64_encoded_data_item_key = string(&keyData[1], keySize-1);  
                   } else {
                      // Skip the five bytes at the beginning that represent the length of the key data.
                      base64_encoded_data_item_key = string(&keyData[5], keySize-5);
                   }
                }

		int32_t partitionIdx = getRedisServerPartitionIndex(base64_encoded_data_item_key);
		// Since this is a data item with TTL, it is stored in the global area of Redis and not inside a user created store (i.e. a Redis hash).
		// Hence, we can't use the Redis hash get command. Rather, we will use the plain Redis get command to read this data item.
                vector<const char *> argv;
                vector<size_t> argvlen;

                string argvStyleRedisCommand = string(REDIS_GET_CMD);
                // Strip the space at the end of the command that should not be there for the argv style Redis command.
                // Using the argv style Redis command will allow us to have spaces and quotes in the key.
                argvStyleRedisCommand = argvStyleRedisCommand.substr(0, argvStyleRedisCommand.size()-1);              

                argv.push_back(argvStyleRedisCommand.c_str());
                argvlen.push_back(argvStyleRedisCommand.size());

                argv.push_back(base64_encoded_data_item_key.c_str());
                argvlen.push_back(base64_encoded_data_item_key.size());

                redis_reply = (redisReply*) redisCommandArgv(redisPartitions[partitionIdx].rdsc, argv.size(), &(argv[0]), &(argvlen[0]));

		if (redis_reply == NULL) {
			dbError.setTTL("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getTTL, it failed for executing the setx command. Error=" << std::string(redisPartitions[partitionIdx].rdsc->errstr) << ". " << DPS_CONNECTION_ERROR, "RedisDBLayer");
			return(false);
		}

		if (redis_reply->type == REDIS_REPLY_ERROR) {
			// Unable to get an existing data item with TTL from the cache.
			dbError.setTTL("Unable to get the requested data item with TTL value. Error=" + std::string(redis_reply->str), DPS_DATA_ITEM_READ_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getTTL, it failed to get the requested data item with TTL value. Error=" << std::string(redis_reply->str) << ". " << DPS_DATA_ITEM_READ_ERROR, "RedisDBLayer");
			freeReplyObject(redis_reply);
			return(false);
		} else if (redis_reply->type == REDIS_REPLY_NIL) {
			// Requested data item is not there in the cache. It was never inserted there to begin with or it probably expired due to its TTL value.
			dbError.setTTL("The requested data item with TTL doesn't exist.", DPS_DATA_ITEM_READ_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getTTL, it failed to get the data item with TTL. It was either never there to begin with or it probably expired due to its TTL value. " << DPS_DATA_ITEM_READ_ERROR, "RedisDBLayer");
			freeReplyObject(redis_reply);
			return(false);
		}

		// Data item value read from the store will be in this format: 'value'
		if ((unsigned)redis_reply->len == 0) {
			// User stored empty data item value in the cache.
			valueData = (unsigned char *)"";
			valueSize = 0;
		} else {
			// We can allocate memory for the exact length of the data item value.
			valueSize = redis_reply->len;
			valueData = (unsigned char *) malloc(valueSize);

			if (valueData == NULL) {
				// Unable to allocate memory to transfer the data item value.
				dbError.setTTL("Unable to allocate memory to copy the data item value with TTL.", DPS_GET_DATA_ITEM_MALLOC_ERROR);
				// Free the response memory pointer handed to us.
				freeReplyObject(redis_reply);
				valueSize = 0;
				return(false);
			}

			// We expect the caller of this method to free the valueData pointer.
			memcpy(valueData, redis_reply->str, valueSize);
		}

		freeReplyObject(redis_reply);
		return(true);
   }

  bool RedisDBLayer::remove(uint64_t store, char const * keyData, uint32_t keySize,
                                PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside remove for store id " << store, "RedisDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisDBLayer");
		}

		return(false);
	}

	// In our Redis dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "RedisDBLayer");
		// User has to retry again to remove the data item from the store.
		return(false);
	}

	// This action is performed on the Store Contents Hash that takes the following format.
	// '1' + 'store id' => 'Redis Hash'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
	int32_t partitionIdx = getRedisServerPartitionIndex(keyString);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);
	string cmd = string(REDIS_HDEL_CMD) + keyString + " " + base64_encoded_data_item_key;
	redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	if (redis_reply == NULL) {
		dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_CONNECTION_ERROR, "RedisDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Problem in deleting the requested data item from the store.
		dbError.set("Redis reply error while removing the requested data item from the store id " + storeIdString + ". " + std::string(redis_reply->str), DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed with Redis reply error for store id " << storeIdString << ". " << DPS_DATA_ITEM_DELETE_ERROR, "RedisDBLayer");
		freeReplyObject(redis_reply);
		releaseStoreLock(storeIdString);
		return(false);
	}

	// Let us ensure that it really removed the requested data item.
	if ((redis_reply->type == REDIS_REPLY_INTEGER) && (redis_reply->integer == (int)0)) {
		// Something is not correct here. It didn't remove the data item. Raise an error.
		dbError.set("Unable to remove the requested data item from the store id " + storeIdString + ".", DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed to remove the requested data item from the store id " << storeIdString << ". " << DPS_DATA_ITEM_DELETE_ERROR, "RedisDBLayer");
		freeReplyObject(redis_reply);
		releaseStoreLock(storeIdString);
		return(false);
	}

	// All done. An existing data item in the given store has been removed.
	freeReplyObject(redis_reply);
	releaseStoreLock(storeIdString);
    return(true);
  }

  // Remove a TTL based data item that is stored in the global area of the Redis DB.
  bool RedisDBLayer::removeTTL(char const * keyData, uint32_t keySize,
                                PersistenceError & dbError, bool encodeKey)
  {
		SPLAPPTRC(L_DEBUG, "Inside removeTTL.", "RedisDBLayer");

		// In our Redis dps implementation, data item keys can have space characters.
		string base64_encoded_data_item_key;

                if (encodeKey == true) {
	           base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
                } else {
                   // Since the key data sent here will always be in the network byte buffer format (NBF), 
                   // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
                   // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
                   // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
                   if ((uint8_t)keyData[0] < 0x80) {
                      // Skip the first length byte. 
                      base64_encoded_data_item_key = string(&keyData[1], keySize-1);  
                   } else {
                      // Skip the five bytes at the beginning that represent the length of the key data.
                      base64_encoded_data_item_key = string(&keyData[5], keySize-5);
                   }
                }

		int32_t partitionIdx = getRedisServerPartitionIndex(base64_encoded_data_item_key);
		// Since this data item has a TTL value, it is not stored in the Redis hash (i.e. user created store).
		// Instead, it will be in the global area of the Redis DB. Hence, use the regular del command instead of the hash del command.
		string cmd = string(REDIS_DEL_CMD) + base64_encoded_data_item_key;
		redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

		if (redis_reply == NULL) {
			dbError.setTTL("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeTTL, it failed to remove a data item with TTL. Error=" << std::string(redisPartitions[partitionIdx].rdsc->errstr) << ". " << DPS_CONNECTION_ERROR, "RedisDBLayer");
			return(false);
		}

		if (redis_reply->type == REDIS_REPLY_ERROR) {
			// Problem in deleting the requested data item from the store.
			dbError.setTTL("Redis reply error while removing the requested data item with TTL. " + std::string(redis_reply->str), DPS_DATA_ITEM_DELETE_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeTTL, it failed with Redis reply error. Error=" << std::string(redis_reply->str) << ". " << DPS_DATA_ITEM_DELETE_ERROR, "RedisDBLayer");
			freeReplyObject(redis_reply);
			return(false);
		}

		// Let us ensure that it really removed the requested data item.
		if ((redis_reply->type == REDIS_REPLY_INTEGER) && (redis_reply->integer == (int)0)) {
			// Something is not correct here. It didn't remove the data item. Raise an error.
			dbError.setTTL("Unable to remove the requested data item with TTL.", DPS_DATA_ITEM_DELETE_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeTTL, it failed to remove the requested data item with TTL. " << DPS_DATA_ITEM_DELETE_ERROR, "RedisDBLayer");
			freeReplyObject(redis_reply);
			return(false);
		}

		// All done. An existing data item with TTL in the global area has been removed.
		freeReplyObject(redis_reply);
		return(true);
  }

  bool RedisDBLayer::has(uint64_t store, char const * keyData, uint32_t keySize,
                             PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside has for store id " << store, "RedisDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisDBLayer");
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
		SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
	}

	return(dataItemAlreadyInCache);
  }

  // Check for the existence of a TTL based data item that is stored in the global area of the Redis DB.
  bool RedisDBLayer::hasTTL(char const * keyData, uint32_t keySize,
                             PersistenceError & dbError, bool encodeKey)
  {
		SPLAPPTRC(L_DEBUG, "Inside hasTTL.", "RedisDBLayer");

		// In our Redis dps implementation, data item keys can have space characters.
		string base64_encoded_data_item_key;

                if (encodeKey == true) {
	           base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
                } else {
                   // Since the key data sent here will always be in the network byte buffer format (NBF), 
                   // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
                   // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
                   // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
                   if ((uint8_t)keyData[0] < 0x80) {
                      // Skip the first length byte. 
                      base64_encoded_data_item_key = string(&keyData[1], keySize-1);  
                   } else {
                      // Skip the five bytes at the beginning that represent the length of the key data.
                      base64_encoded_data_item_key = string(&keyData[5], keySize-5);
                   }
                }

		int32_t partitionIdx = getRedisServerPartitionIndex(base64_encoded_data_item_key);
		string cmd = string(REDIS_EXISTS_CMD) + base64_encoded_data_item_key;
		redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

		if (redis_reply == NULL) {
			dbError.setTTL("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
			return(false);
		}

		if (redis_reply->type == REDIS_REPLY_ERROR) {
			// Unable to check for the existence of the data item.
			dbError.setTTL("Unable to check for the existence of the data item with TTL. Error=" + std::string(redis_reply->str), DPS_KEY_EXISTENCE_CHECK_ERROR);
			freeReplyObject(redis_reply);
			return(false);
		}

		bool ttlKeyExists = false;

		if (redis_reply->integer == (int)1) {
			// TTL based key exists;
			ttlKeyExists = true;
		}

		freeReplyObject(redis_reply);
		return(ttlKeyExists);
  }

  void RedisDBLayer::clear(uint64_t store, PersistenceError & dbError) {
     SPLAPPTRC(L_DEBUG, "Inside clear for store id " << store, "RedisDBLayer");

 	ostringstream storeId;
 	storeId << store;
 	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisDBLayer");
		}

		return;
	}

 	// Lock the store first.
 	if (acquireStoreLock(storeIdString) == false) {
 		// Unable to acquire the store lock.
 		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "RedisDBLayer");
 		// User has to retry again to remove the store.
 		return;
 	}

 	// Get the store name.
 	uint32_t dataItemCnt = 0;
 	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
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
	int32_t partitionIdx = getRedisServerPartitionIndex(keyString);
	string cmd = string(REDIS_DEL_CMD) + keyString;
	redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	if (redis_reply == NULL) {
		dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << DPS_CONNECTION_ERROR, "RedisDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

	if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Problem in deleting the store contents hash.
		dbError.set("Unable to remove the requested data item from the store for the store id " + storeIdString + ". " + std::string(redis_reply->str), DPS_STORE_CLEARING_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << DPS_STORE_CLEARING_ERROR, "RedisDBLayer");
		freeReplyObject(redis_reply);
		releaseStoreLock(storeIdString);
		return;
	}

	freeReplyObject(redis_reply);

	// Let us now recreate a new Store Contents Hash for this store with three meta data entries (store name, key spl type name, value spl type name).
	// Then we are done.
	// 1) Store name.
	cmd = string(REDIS_HSET_CMD) + keyString + " " +
		string(REDIS_STORE_ID_TO_STORE_NAME_KEY) + " " + storeName;
	redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Problem in creating the "Store Content Hash" metadata1 entry in the cache.
		dbError.set("Fatal error in clear method: Unable to recreate 'Store Contents Hash' metadata1 in the store id " +
			storeIdString + ". " + std::string(redis_reply->str), DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Fatal error: Inside clear, it failed for store id " << storeIdString << ". " << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "RedisDBLayer");
		freeReplyObject(redis_reply);
		releaseStoreLock(storeIdString);
		return;
	}

	freeReplyObject(redis_reply);

	// 2) Key spl type name.
	cmd = string(REDIS_HSET_CMD) + keyString + " " +
		string(REDIS_SPL_TYPE_NAME_OF_KEY) + " " + keySplTypeName;
	redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Problem in creating the "Store Content Hash" metadata2 entry in the cache.
		dbError.set("Fatal error in clear method: Unable to recreate 'Store Contents Hash' metadata2  in the store id " +
			storeIdString + ". " + std::string(redis_reply->str), DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Fatal error: Inside clear, it failed for store id " << storeIdString << ". " << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "RedisDBLayer");
		freeReplyObject(redis_reply);
		releaseStoreLock(storeIdString);
		return;
	}

	freeReplyObject(redis_reply);

	// 3) Value spl type name.
	cmd = string(REDIS_HSET_CMD) + keyString + " " +
		string(REDIS_SPL_TYPE_NAME_OF_VALUE) + " " + valueSplTypeName;
	redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Problem in creating the "Store Content Hash" metadata3 entry in the cache.
		dbError.set("Fatal error in clear method: Unable to recreate 'Store Contents Hash' metadata3  in the store id " +
			storeIdString + ". " + std::string(redis_reply->str), DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Fatal error: Inside clear, it failed for store id " << storeIdString << ". " << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "RedisDBLayer");
		freeReplyObject(redis_reply);
		releaseStoreLock(storeIdString);
		return;
	}

	freeReplyObject(redis_reply);

 	// If there was an error in the store contents hash recreation, then this store is going to be in a weird state.
	// It is a fatal error. If that happened, something terribly had gone wrong in clearing the contents.
	// User should look at the dbError code and decide about a corrective action.
 	releaseStoreLock(storeIdString);
  }
        
  uint64_t RedisDBLayer::size(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside size for store id " << store, "RedisDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside size, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside size, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisDBLayer");
		}

		return(false);
	}

	// Store size information is maintained as part of the store information.
	uint32_t dataItemCnt = 0;
	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside size, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		return(0);
	}

	return((uint64_t)dataItemCnt);
  }

  // We allow space characters in the data item keys.
  // Hence, it is required to based64 encode them before storing the
  // key:value data item in Redis.
  // (Use boost functions to do this.)
  void RedisDBLayer::base64_encode(std::string const & str, std::string & base64) {
	  // Insert line breaks for every 64KB characters.
	  typedef insert_linebreaks<base64_from_binary<transform_width<string::const_iterator,6,8> >, 64*1024 > it_base64_t;

	  unsigned int writePaddChars = (3-str.length()%3)%3;
	  base64 = string(it_base64_t(str.begin()),it_base64_t(str.end()));
	  base64.append(writePaddChars,'=');
  }

  // As explained above, we based64 encoded the data item keys before adding them to the store.
  // If we need to get back the original key name, this function will help us in
  // decoding the base64 encoded key.
  // (Use boost functions to do this.)
  void RedisDBLayer::base64_decode(std::string & base64, std::string & result) {
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
  }

  // This method will check if a store exists for a given store id.
  bool RedisDBLayer::storeIdExistsOrNot(string storeIdString, PersistenceError & dbError) {
	  string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
	  int32_t partitionIdx = getRedisServerPartitionIndex(keyString);
	  string cmd = string(REDIS_EXISTS_CMD) + keyString;
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	  if (redis_reply == NULL) {
		dbError.set("StoreIdExistsOrNot: Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		return(false);
	  }

	  if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Unable to get the store contents hash for the given store id.
		dbError.set("StoreIdExistsOrNot: Unable to get StoreContentsHash from the StoreId " + storeIdString +
			". " + std::string(redis_reply->str), DPS_GET_STORE_CONTENTS_HASH_ERROR);
		freeReplyObject(redis_reply);
		return(false);
	  }

	  bool storeIdExists = true;

	  if (redis_reply->integer == (int)0) {
		  storeIdExists = false;
	  }

	  freeReplyObject(redis_reply);
	  return(storeIdExists);
  }

  // This method will acquire a lock for a given store.
  bool RedisDBLayer::acquireStoreLock(string const & storeIdString) {
	  int32_t retryCnt = 0;
	  string cmd = "";

	  //Try to get a lock for this store.
	  while (1) {
		// '4' + 'store id' + 'dps_lock' => 1
		std::string storeLockKey = string(DPS_STORE_LOCK_TYPE) + storeIdString + DPS_LOCK_TOKEN;
		int32_t partitionIdx = getRedisServerPartitionIndex(storeLockKey);
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or
		// until the Redis back-end removes this lock entry after the DPS_AND_DL_GET_LOCK_TTL times out.
		cmd = string(REDIS_SETNX_CMD) + storeLockKey + " " + "1";
		redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

		if (redis_reply == NULL) {
			return(false);
		}

		if (redis_reply->type == REDIS_REPLY_ERROR) {
			// Problem in atomic creation of the store lock.
			freeReplyObject(redis_reply);
			return(false);
		}

		if (redis_reply->integer == (int)1) {
			// We got the lock.
			// Set the expiration time for this lock key.
			freeReplyObject(redis_reply);
			std::ostringstream cmd_stream;
			cmd_stream << string(REDIS_EXPIRE_CMD) << storeLockKey << " " << DPS_AND_DL_GET_LOCK_TTL;
			cmd = cmd_stream.str();
			redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

			if (redis_reply == NULL) {
				// Delete the erroneous lock data item we created.
				cmd = string(REDIS_DEL_CMD) + " " + storeLockKey;
				redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
				freeReplyObject(redis_reply);
				return(false);
			}

			if (redis_reply->type == REDIS_REPLY_ERROR) {
				// Problem in atomic creation of the store lock.
				freeReplyObject(redis_reply);
				// Delete the erroneous lock data item we created.
				cmd = string(REDIS_DEL_CMD) + " " + storeLockKey;
				redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
				freeReplyObject(redis_reply);
				return(false);
			}

			freeReplyObject(redis_reply);
			return(true);
		}

		freeReplyObject(redis_reply);
		// Someone else is holding on to the lock of this store. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {

			return(false);
		}

		// Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
		usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
			  (retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
	  }

	  return(false);
  }

  void RedisDBLayer::releaseStoreLock(string const & storeIdString) {
	  // '4' + 'store id' + 'dps_lock' => 1
	  std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;
	  int32_t partitionIdx = getRedisServerPartitionIndex(storeLockKey);
	  string cmd = string(REDIS_DEL_CMD) + storeLockKey;
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
	  freeReplyObject(redis_reply);
  }

  bool RedisDBLayer::readStoreInformation(std::string const & storeIdString, PersistenceError & dbError,
		  uint32_t & dataItemCnt, std::string & storeName, std::string & keySplTypeName, std::string & valueSplTypeName) {
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
	  int32_t partitionIdx = getRedisServerPartitionIndex(keyString);
	  string cmd = string(REDIS_HGET_CMD) + keyString +
			  " " + string(REDIS_STORE_ID_TO_STORE_NAME_KEY);
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	  if (redis_reply == NULL) {
		dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		return(false);
	  }

	  if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Unable to get the metadata1 from the store contents hash for the given store id.
		dbError.set("Unable to get StoreContentsHash metadata1 from the StoreId " + storeIdString +
			". " + std::string(redis_reply->str), DPS_GET_STORE_CONTENTS_HASH_ERROR);
		freeReplyObject(redis_reply);
		return(false);
	  }

	  if (redis_reply->str == NULL) {
			// Null pointer returned in place of the store name.
			dbError.set("Redis returned a NULL pointer. Unable to get the store name for the StoreId " + storeIdString,
				DPS_GET_STORE_CONTENTS_HASH_ERROR);
			freeReplyObject(redis_reply);
			return(false);
	  }

	  storeName = string(redis_reply->str);
	  freeReplyObject(redis_reply);

	  if (storeName == "") {
		  // Unable to get the name of this store.
		  dbError.set("Unable to get the store name for StoreId " + storeIdString + ".", DPS_GET_STORE_NAME_ERROR);
		  return(false);
	  }

	  // 2) Let us get the spl type name for this store's key.
	  cmd = string(REDIS_HGET_CMD) + keyString +
			  " " + string(REDIS_SPL_TYPE_NAME_OF_KEY);
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	  if (redis_reply == NULL) {
		dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		return(false);
	  }

	  if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Unable to get the metadata2 from the store contents hash for the given store id.
		dbError.set("Unable to get StoreContentsHash metadata2 from the StoreId " + storeIdString +
			". " + std::string(redis_reply->str), DPS_GET_STORE_CONTENTS_HASH_ERROR);
		freeReplyObject(redis_reply);
		return(false);
	  }

	  if (redis_reply->str == NULL) {
			// Null pointer returned in place of the SPL type name for the key.
			dbError.set("Redis returned a NULL pointer. Unable to get the SPL type name of the key for the StoreId " + storeIdString,
				DPS_GET_STORE_CONTENTS_HASH_ERROR);
			freeReplyObject(redis_reply);
			return(false);
	  }

	  keySplTypeName = string(redis_reply->str);
	  freeReplyObject(redis_reply);

	  if (keySplTypeName == "") {
		  // Unable to get the spl type name for this store's key.
		  dbError.set("Unable to get the key spl type name for StoreId " + storeIdString + ".", DPS_GET_KEY_SPL_TYPE_NAME_ERROR);
		  return(false);
	  }

	  // 3) Let us get the spl type name for this store's value.
	  cmd = string(REDIS_HGET_CMD) + keyString +
			  " " + string(REDIS_SPL_TYPE_NAME_OF_VALUE);
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	  if (redis_reply == NULL) {
		dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		return(false);
	  }

	  if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Unable to get the metadata3 from the store contents hash for the given store id.
		dbError.set("Unable to get StoreContentsHash metadata3 from the StoreId " + storeIdString +
			". " + std::string(redis_reply->str), DPS_GET_STORE_CONTENTS_HASH_ERROR);
		freeReplyObject(redis_reply);
		return(false);
	  }

	  if (redis_reply->str == NULL) {
			// Null pointer returned in place of the SPL type name for the value.
			dbError.set("Redis returned a NULL pointer. Unable to get the SPL type name of the value for the StoreId " + storeIdString,
				DPS_GET_STORE_CONTENTS_HASH_ERROR);
			freeReplyObject(redis_reply);
			return(false);
	  }

	  valueSplTypeName = string(redis_reply->str);
	  freeReplyObject(redis_reply);

	  if (valueSplTypeName == "") {
		  // Unable to get the spl type name for this store's value.
		  dbError.set("Unable to get the value spl type name for StoreId " + storeIdString + ".", DPS_GET_VALUE_SPL_TYPE_NAME_ERROR);
		  return(false);
	  }

	  // 4) Let us get the size of the store contents hash now.
	  cmd = string(REDIS_HLEN_CMD) + keyString;
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	  if (redis_reply == NULL) {
		  dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		  return(false);
	  }

	  if (redis_reply->type == REDIS_REPLY_ERROR) {
		  // Unable to get the store contents hash for the given store id.
		  dbError.set("Unable to get StoreContentsHash size from the StoreId " + storeIdString +
				  ". " + std::string(redis_reply->str), DPS_GET_STORE_SIZE_ERROR);
		  freeReplyObject(redis_reply);
		  return(false);
	  }

	  // Our Store Contents Hash for every store will have a mandatory reserved internal elements (store name, key spl type name, and value spl type name).
	  // Let us not count those three elements in the actual store contents hash size that the caller wants now.
	  if (redis_reply->integer <= 0) {
		  // This is not correct. We must have a minimum hash size of 3 because of the reserved elements.
		  dbError.set("Wrong value (zero) observed as the store size for StoreId " + storeIdString + ".", DPS_GET_STORE_SIZE_ERROR);
		  freeReplyObject(redis_reply);
		  return(false);
	  }

	  dataItemCnt = redis_reply->integer - 3;
	  freeReplyObject(redis_reply);
	  return(true);
  }

  string RedisDBLayer::getStoreName(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		  return("");
	  }

	  string base64_decoded_storeName;
	  base64_decode(storeName, base64_decoded_storeName);
	  return(base64_decoded_storeName);
  }

  string RedisDBLayer::getSplTypeNameForKey(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		  return("");
	  }

	  string base64_decoded_keySplTypeName;
	  base64_decode(keySplTypeName, base64_decoded_keySplTypeName);
	  return(base64_decoded_keySplTypeName);
  }

  string RedisDBLayer::getSplTypeNameForValue(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		  return("");
	  }

	  string base64_decoded_valueSplTypeName;
	  base64_decode(valueSplTypeName, base64_decoded_valueSplTypeName);
	  return(base64_decoded_valueSplTypeName);
  }

  std::string RedisDBLayer::getNoSqlDbProductName(void) {
	  return(string(REDIS_NO_SQL_DB_NAME));
  }

  void RedisDBLayer::getDetailsAboutThisMachine(std::string & machineName, std::string & osVersion, std::string & cpuArchitecture) {
	  machineName = nameOfThisMachine;
	  osVersion = osVersionOfThisMachine;
	  cpuArchitecture = cpuTypeOfThisMachine;
  }

  bool RedisDBLayer::runDataStoreCommand(std::string const & cmd, PersistenceError & dbError) {
	  // If users want to execute simple arbitrary back-end data store (fire and forget)
	  // native commands, this API can be used. This covers any Redis or Cassandra(CQL)
	  // native commands that don't have to fetch and return K/V pairs or return size of the db etc.
	  // (Insert and Delete are the more suitable ones here. However, key and value can only have string types.)
	  // User must ensure that his/her command string is syntactically correct according to the
	  // rules of the back-end data store you configured. DPS logic will not do the syntax checking.
	  //
	  // (You can't do get command using this technique. Similarly, no complex type keys or values.
	  //  In that case, please use the regular dps APIs.)
	  //
	  // We will simply take your command string and run it. So, be sure of what
	  // command you are sending here.
	  redis_reply = (redisReply*)redisCommand(redisPartitions[0].rdsc, cmd.c_str());

	  if (redis_reply == NULL) {
		  dbError.set("From Redis data store: Unable to connect to the redis server(s). " + std::string(redisPartitions[0].rdsc->errstr), DPS_CONNECTION_ERROR);
		  SPLAPPTRC(L_DEBUG, "From Redis data store: Inside runDataStoreCommand, it failed to run this command: '" <<
				  cmd << "'. Error=" << std::string(redisPartitions[0].rdsc->errstr) << ". " << DPS_CONNECTION_ERROR, "RedisDBLayer");
		  return(false);
	  }

	  if (redis_reply->type == REDIS_REPLY_ERROR) {
		  // Problem in running an arbitrary data store command.
		  dbError.set("From Redis data store: Unable to run this command: '" + cmd + "'. Error=" + std::string(redis_reply->str), DPS_RUN_DATA_STORE_COMMAND_ERROR);
		  SPLAPPTRC(L_DEBUG, "From Redis data store: Inside runDataStoreCommand, it failed to run this command: '" << cmd <<
				  "'. Error=" + std::string(redis_reply->str) << ". " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "RedisDBLayer");
		  freeReplyObject(redis_reply);
		  return(false);
	  }

	  freeReplyObject(redis_reply);
	  return(true);
  }

  bool RedisDBLayer::runDataStoreCommand(uint32_t const & cmdType, std::string const & httpVerb,
		std::string const & baseUrl, std::string const & apiEndpoint, std::string const & queryParams,
		std::string const & jsonRequest, std::string & jsonResponse, PersistenceError & dbError) {
		// This API can only be supported in NoSQL data stores such as Cloudant, HBase etc.
		// Redis doesn't have a way to do this.
		dbError.set("From Redis data store: This API to run native data store commands is not supported in Redis.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Redis data store: This API to run native data store commands is not supported in Redis. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "RedisDBLayer");
		return(false);
  }

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
  bool RedisDBLayer::runDataStoreCommand(std::vector<std::string> const & cmdList, std::string & resultValue, PersistenceError & dbError) {
     resultValue = "";

     if (cmdList.size() == 0) {
        resultValue = "Error: Empty Redis command list was given by the caller.";
        dbError.set(resultValue, DPS_RUN_DATA_STORE_COMMAND_ERROR);
        return(false);
     }

     // We are going to use the RedisCommandArgv to push different parts of the Redis command as passed by the caller.
     vector<const char *> argv;
     vector<size_t> argvlen;
     
     // Iterate over the caller provided items in the cmdList and add them to the argv array.
     for (std::vector<std::string>::const_iterator it = cmdList.begin() ; it != cmdList.end(); ++it) {
        argv.push_back((*it).c_str());
        argvlen.push_back((*it).size());
     }

     redis_reply = (redisReply*) redisCommandArgv(redisPartitions[0].rdsc, argv.size(), &(argv[0]), &(argvlen[0]));

     if (redis_reply == NULL) {
        dbError.set("Redis_Reply_Null error. Unable to connect to the redis server(s). " + std::string(redisPartitions[0].rdsc->errstr), DPS_CONNECTION_ERROR);
	SPLAPPTRC(L_DEBUG, "Redis_Reply_Null error. Inside runDataStoreCommand using Redis cmdList, it failed for executing the user given Redis command list. Error=" << std::string(redisPartitions[0].rdsc->errstr) << ". " << DPS_CONNECTION_ERROR, "RedisDBLayer");
	return(false);
     }

     if (redis_reply->type == REDIS_REPLY_ERROR) {
        // Error in executing the user given Redis command.
        resultValue = std::string(redis_reply->str);
        dbError.set("Redis_Reply_Error while executing the user given Redis command. Error=" + resultValue, DPS_RUN_DATA_STORE_COMMAND_ERROR);
	SPLAPPTRC(L_DEBUG, "Redis_Reply_Error. Inside runDataStoreCommand using Redis cmdList, it failed to execute the user given Redis command list. Error=" << std::string(redis_reply->str) << ". " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "RedisDBLayer");
        freeReplyObject(redis_reply);
        return(false);
     } else if (redis_reply->type == REDIS_REPLY_NIL) {
        // Redis returned NIL response.
        resultValue = "nil";
        dbError.set("Redis_Reply_Nil error while executing user given Redis command list. Possibly missing or invalid tokens in the Redis command.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
        SPLAPPTRC(L_DEBUG, "Redis_Reply_Nil error. Inside runDataStoreCommand using Redis cmdList, it failed to execute the user given Redis command list. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "RedisDBLayer");
	freeReplyObject(redis_reply);
	return(false);
     } else if (redis_reply->type == REDIS_REPLY_STRING) {
        resultValue = string(redis_reply->str, redis_reply->len);
     } else if (redis_reply->type == REDIS_REPLY_ARRAY) {
        for (uint32_t j = 0; j < redis_reply->elements; j++) {
           resultValue +=  string(redis_reply->element[j]->str, redis_reply->element[j]->len);
           if (j != redis_reply->elements-1) {
              // Add a new line for every element except for the very last element.
              resultValue += "\n";
           }
        }
     } else if (redis_reply->type == REDIS_REPLY_INTEGER) {
        char msg[260];
        sprintf(msg, "%d", (int)redis_reply->integer);
        resultValue = string(msg);
     } else if (redis_reply->type == REDIS_REPLY_STATUS) {
        resultValue = string(redis_reply->str, redis_reply->len);
     }

     freeReplyObject(redis_reply);
     return(true);
  }

  // This method will get the data item from the store for a given key.
  // Caller of this method can also ask us just to find if a data item
  // exists in the store without the extra work of fetching and returning the data item value.
  bool RedisDBLayer::getDataItemFromStore(std::string const & storeIdString,
		  std::string const & keyDataString, bool const & checkOnlyForDataItemExistence,
		  bool const & skipDataItemExistenceCheck, unsigned char * & valueData,
		  uint32_t & valueSize, PersistenceError & dbError) {
		// Let us get this data item from the cache as it is.
		// Since there could be multiple data writers, we are going to get whatever is there now.
		// It is always possible that the value for the requested item can change right after
		// you read it due to the data write made by some other thread. Such is life in a global distributed in-memory store.
	    string cmd = "";
    	string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
    	int32_t partitionIdx = getRedisServerPartitionIndex(keyString);

	  	// If the caller doesn't want to perform the data existence check to save time, honor that wish here.
	    if (skipDataItemExistenceCheck == false) {
			// This action is performed on the Store Contents Hash that takes the following format.
			// '1' + 'store id' => 'Redis Hash'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
			cmd = string(REDIS_HEXISTS_CMD) + keyString + " " + keyDataString;
			redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

			if (redis_reply == NULL) {
				dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
				return(false);
			}

			if (redis_reply->type == REDIS_REPLY_ERROR) {
				// Unable to check for the existence of the data item.
				dbError.set("Unable to check for the existence of the data item in the StoreId " + storeIdString +
					". " + std::string(redis_reply->str), DPS_KEY_EXISTENCE_CHECK_ERROR);
				freeReplyObject(redis_reply);
				return(false);
			}

			bool dataItemExists = true;

			if (redis_reply->integer == int(0)) {
				dataItemExists = false;
			}

			freeReplyObject(redis_reply);

			// If the caller only wanted us to check for the data item existence, we can exit now.
			if (checkOnlyForDataItemExistence == true) {
				return(dataItemExists);
			}

			// Caller wants us to fetch and return the data item value.
			// If the data item is not there, we can't do much at this point.
			if (dataItemExists == false) {
				// This data item doesn't exist. Let us raise an error.
				// Requested data item is not there in the cache.
				dbError.set("The requested data item doesn't exist in the StoreId " + storeIdString +
					".", DPS_DATA_ITEM_READ_ERROR);
				return(false);
			}
	    } // End of if (skipDataItemExistenceCheck == false)

	    // Fetch the data item now.
		cmd = string(REDIS_HGET_CMD) + keyString + " " + keyDataString;
		redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

		if (redis_reply == NULL) {
			dbError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
			return(false);
		}

		// If SUCCESS, this result can come as an empty string.
		if (redis_reply->type == REDIS_REPLY_ERROR) {
			// Unable to get the requested data item from the cache.
			dbError.set("Unable to get the requested data item from the store with the StoreId " + storeIdString +
				". " + std::string(redis_reply->str), DPS_DATA_ITEM_READ_ERROR);
			freeReplyObject(redis_reply);
			return(false);
		}

		if (redis_reply->type == REDIS_REPLY_NIL) {
			// Requested data item is not there in the cache.
			dbError.set("The requested data item doesn't exist in the StoreId " + storeIdString +
				".", DPS_DATA_ITEM_READ_ERROR);
			freeReplyObject(redis_reply);
			return(false);
		}

		// Data item value read from the store will be in this format: 'value'
		if ((unsigned)redis_reply->len == 0) {
			// User stored empty data item value in the cache.
			valueData = (unsigned char *)"";
			valueSize = 0;
		} else {
			// We can allocate memory for the exact length of the data item value.
			valueSize = redis_reply->len;
			valueData = (unsigned char *) malloc(valueSize);

			if (valueData == NULL) {
				// Unable to allocate memory to transfer the data item value.
				dbError.set("Unable to allocate memory to copy the data item value for the StoreId " +
					storeIdString + ".", DPS_GET_DATA_ITEM_MALLOC_ERROR);
				// Free the response memory pointer handed to us.
				freeReplyObject(redis_reply);
				valueSize = 0;
				return(false);
			}

			// We expect the caller of this method to free the valueData pointer.
			memcpy(valueData, redis_reply->str, valueSize);
		}

		freeReplyObject(redis_reply);
	    return(true);
  }

  RedisDBLayerIterator * RedisDBLayer::newIterator(uint64_t store, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside newIterator for store id " << store, "RedisDBLayer");

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  // Ensure that a store exists for the given store id.
	  if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisDBLayer");
		  }

		  return(NULL);
	  }

	  // Get the general information about this store.
	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayer");
		  return(NULL);
	  }

	  // It is a valid store. Create a new iterator and return it to the caller.
	  string keyString = string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
	  int32_t partitionIdx = getRedisServerPartitionIndex(keyString);
	  RedisDBLayerIterator *iter = new RedisDBLayerIterator();
	  iter->store = store;
	  base64_decode(storeName, iter->storeName);
	  // Give this iterator access to our redis connection handle.
	  iter->rdsc = redisPartitions[partitionIdx].rdsc;
	  iter->hasData = true;
	  // Give this iterator access to our RedisDBLayer object.
	  iter->redisDBLayerPtr = this;
	  iter->sizeOfDataItemKeysVector = 0;
	  iter->currentIndex = 0;
	  return(iter);
  }

  void RedisDBLayer::deleteIterator(uint64_t store, Iterator * iter, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside deleteIterator for store id " << store, "RedisDBLayer");

	  if (iter == NULL) {
		  return;
	  }

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  RedisDBLayerIterator *myIter = static_cast<RedisDBLayerIterator *>(iter);

	  // Let us ensure that the user wants to delete an iterator that really belongs to the store passed to us.
	  // This will handle user's coding errors where a wrong combination of store id and iterator is passed to us for deletion.
	  if (myIter->store != store) {
		  // User sent us a wrong combination of a store and an iterator.
		  dbError.set("A wrong iterator has been sent for deletion. This iterator doesn't belong to the StoreId " +
		  				storeIdString + ".", DPS_STORE_ITERATION_DELETION_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside deleteIterator, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_DELETION_ERROR, "RedisDBLayer");
		  return;
	  } else {
		  delete iter;
	  }
  }

  // This method will acquire a lock for any given generic/arbitrary identifier passed as a string..
  // This is typically used inside the createStore, createOrGetStore, createOrGetLock methods to
  // provide thread safety. There are other lock acquisition/release methods once someone has a valid store id or lock id.
  bool RedisDBLayer::acquireGeneralPurposeLock(string const & entityName) {
	  int32_t retryCnt = 0;
	  string cmd = "";

	  //Try to get a lock for this generic entity.
	  while (1) {
		// '501' + 'entity name' + 'generic_lock' => 1
		std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
		int32_t partitionIdx = getRedisServerPartitionIndex(genericLockKey);
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or
		// until the Redis back-end removes this lock entry after the DPS_AND_DL_GET_LOCK_TTL times out.
		cmd = string(REDIS_SETNX_CMD) + genericLockKey + " " + "1";
		redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

		if (redis_reply == NULL) {
			return(false);
		}

		if (redis_reply->type == REDIS_REPLY_ERROR) {
			// Problem in atomic creation of the general purpose lock.
			freeReplyObject(redis_reply);
			return(false);
		}

		if (redis_reply->integer == (int)1) {
			// We got the lock.
			// Set the expiration time for this lock key.
			freeReplyObject(redis_reply);
			std::ostringstream cmd_stream;
			cmd_stream << string(REDIS_EXPIRE_CMD) << genericLockKey << " " << DPS_AND_DL_GET_LOCK_TTL;
			cmd = cmd_stream.str();
			redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

			if (redis_reply == NULL) {
				// Delete the erroneous lock data item we created.
				cmd = string(REDIS_DEL_CMD) + " " + genericLockKey;
				redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
				freeReplyObject(redis_reply);
				return(false);
			}

			if (redis_reply->type == REDIS_REPLY_ERROR) {
				// Problem in atomic creation of the general purpose lock.
				freeReplyObject(redis_reply);
				// Delete the erroneous lock data item we created.
				cmd = string(REDIS_DEL_CMD) + " " + genericLockKey;
				redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
				freeReplyObject(redis_reply);
				return(false);
			}

			freeReplyObject(redis_reply);
			return(true);
		}

		freeReplyObject(redis_reply);
		// Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {

			return(false);
		}

		// Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
		usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
			  (retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
	  }

	  return(false);
  }

  void RedisDBLayer::releaseGeneralPurposeLock(string const & entityName) {
	  // '501' + 'entity name' + 'generic_lock' => 1
	  std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
	  int32_t partitionIdx = getRedisServerPartitionIndex(genericLockKey);
	  string cmd = string(REDIS_DEL_CMD) + genericLockKey;
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
	  freeReplyObject(redis_reply);
  }

  RedisDBLayerIterator::RedisDBLayerIterator() {

  }

  RedisDBLayerIterator::~RedisDBLayerIterator() {

  }

  bool RedisDBLayerIterator::getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
  	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside getNext for store id " << store, "RedisDBLayerIterator");

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
	  string cmd = "";
	  string data_item_key = "";

	  // Ensure that a store exists for the given store id.
	  if (this->redisDBLayerPtr->storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayerIterator");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "RedisDBLayerIterator");
		  }

		  return(false);
	  }

	  // Ensure that this store is not empty at this time.
	  if (this->redisDBLayerPtr->size(store, dbError) <= 0) {
		  dbError.set("Store is empty for the StoreId " + storeIdString + ".", DPS_STORE_EMPTY_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_STORE_EMPTY_ERROR, "RedisDBLayerIterator");
		  return(false);
	  }

	  if (this->sizeOfDataItemKeysVector <= 0) {
		  // This is the first time we are coming inside getNext for store iteration.
		  // Let us get the available data item keys from this store.
		  this->dataItemKeys.clear();

		  string cmd = string(REDIS_HKEYS_CMD) + string(DPS_STORE_CONTENTS_HASH_TYPE) + storeIdString;
		  this->redis_reply = (redisReply*)redisCommand(this->rdsc, cmd.c_str());

			if (this->redis_reply == NULL) {
				dbError.set("Unable to connect to the redis server(s). " + std::string(this->rdsc->errstr), DPS_CONNECTION_ERROR);
				this->hasData = false;
				return(false);
			}

			if (this->redis_reply->type == REDIS_REPLY_ERROR) {
				// Unable to get data item keys from the store.
				dbError.set("Unable to get data item keys for the StoreId " + storeIdString +
					". " + std::string(this->redis_reply->str), DPS_GET_STORE_DATA_ITEM_KEYS_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_DATA_ITEM_KEYS_ERROR, "RedisDBLayerIterator");
				freeReplyObject(this->redis_reply);
				this->hasData = false;
				return(false);
			}

			if (this->redis_reply->type != REDIS_REPLY_ARRAY) {
				// Unable to get data item keys from the store in an array format.
				dbError.set("Unable to get data item keys in an array format for the StoreId " + storeIdString +
					". " + std::string(this->redis_reply->str), DPS_GET_STORE_DATA_ITEM_KEYS_AS_AN_ARRAY_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_DATA_ITEM_KEYS_AS_AN_ARRAY_ERROR, "RedisDBLayerIterator");
				freeReplyObject(this->redis_reply);
				this->hasData = false;
				return(false);
			}

			// We have the data item keys returned in array now.
			// Let us insert them into the iterator object's member variable that will hold the data item keys for this store.
	        for (unsigned int j = 0; j < this->redis_reply->elements; j++) {
	        	data_item_key = string(this->redis_reply->element[j]->str);

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
	        }

	        freeReplyObject(this->redis_reply);
	        this->sizeOfDataItemKeysVector = this->dataItemKeys.size();
	        this->currentIndex = 0;

	        if (this->sizeOfDataItemKeysVector == 0) {
	        	// This is an empty store at this time.
	        	// Let us exit now.
	        	this->hasData = false;
	        	return(false);
	        }
	  }

	  // We have data item keys.
	  // Let us get the next available data.
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
	  bool result = this->redisDBLayerPtr->getDataItemFromStore(storeIdString, data_item_key,
		false, false, valueData, valueSize, dbError);

	  if (result == false) {
		  // Some error has occurred in reading the data item value.
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "RedisDBLayerIterator");
		  // We will disable any future action for this store using the current iterator.
		  this->hasData = false;
		  return(false);
	  }

	  // We are almost done once we take care of arranging to return the key name and key size.
	  // In order to support spaces in data item keys, we base64 encoded them before storing it in Redis.
	  // Let us base64 decode it now to get the original data item key.
	  string base64_decoded_data_item_key;
	  this->redisDBLayerPtr->base64_decode(data_item_key, base64_decoded_data_item_key);
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
		  dbError.set("Unable to allocate memory for the keyData while doing the next data item iteration for the StoreId " +
		  				storeIdString + ".", DPS_STORE_ITERATION_MALLOC_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_MALLOC_ERROR, "RedisDBLayerIterator");
		  return(false);
	  }

	  // Copy the raw key data into the allocated buffer.
	  memcpy(keyData, data_item_key.data(), keySize);
	  // We are done. We expect the caller to free the keyData and valueData buffers.
	  return(true);
  }

// =======================================================================================================
// Beyond this point, we have code that deals with the distributed locks that a SPL developer can
// create, remove,acquire, and release.
// =======================================================================================================
  uint64_t RedisDBLayer::createOrGetLock(std::string const & name, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock with a name " << name, "RedisDBLayer");

		string base64_encoded_name;
		base64_encode(name, base64_encoded_name);

	 	// Get a general purpose lock so that only one thread can
		// enter inside of this method at any given time with the same lock name.
	 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
	 		// Unable to acquire the general purpose lock.
	 		lkError.set("Unable to get a generic lock for creating a lock with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
	 		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for an yet to be created lock with its name as " <<
	 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "RedisDBLayer");
	 		// User has to retry again to create this distributed lock.
	 		return 0;
	 	}

		// Let us first see if a lock with the given name already exists.
	 	// In our Redis dps implementation, data item keys can have space characters.
		// Inside Redis, all our lock names will have a mapping type indicator of
		// "5" at the beginning followed by the actual lock name.
		// '5' + 'lock name' ==> 'lock id'
		std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
		int32_t partitionIdx = getRedisServerPartitionIndex(lockNameKey);
		std::string cmd = string(REDIS_EXISTS_CMD) + lockNameKey;
		redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

		// If we get a NULL reply, then it indicates a redis server connection error.
		if (redis_reply == NULL) {
			lkError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DL_CONNECTION_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for the lock named " << name << ". " << DL_CONNECTION_ERROR, "RedisDBLayer");
			return(0);
		}

		if (redis_reply->integer == (int)1) {
			// This lock already exists in our cache.
			// We can get the lockId and return it to the caller.
			freeReplyObject(redis_reply);
			cmd = string(REDIS_GET_CMD) + lockNameKey;
			redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

			if (redis_reply->type == REDIS_REPLY_ERROR) {
				// Unable to get an existing lock id from the cache.
				lkError.set("Unable to get the lockId for the lockName " + name + ". " + std::string(redis_reply->str), DL_GET_LOCK_ID_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for the lockName " << name << ". " << DL_GET_LOCK_ID_ERROR, "RedisDBLayer");
				freeReplyObject(redis_reply);
				releaseGeneralPurposeLock(base64_encoded_name);
				return(0);
			} else {
				uint64_t lockId = 0;

				if (redis_reply->len > 0) {
					lockId = streams_boost::lexical_cast<uint64_t>(redis_reply->str);
				} else {
					// Unable to get the lock information. It is an abnormal error. Convey this to the caller.
					lkError.set("Redis returned an empty lockId for the lockName " + name + ".", DL_GET_LOCK_ID_ERROR);
					SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed with an empty lockId for the lockName " << name << ". " << DL_GET_LOCK_ID_ERROR, "RedisDBLayer");
					freeReplyObject(redis_reply);
					releaseGeneralPurposeLock(base64_encoded_name);
					return(0);
				}

				freeReplyObject(redis_reply);
				releaseGeneralPurposeLock(base64_encoded_name);
				return(lockId);
			}
		}

		if (redis_reply->integer == (int)0) {
			// Create a new lock.
			// At first, let us increment our global dps_and_dl_guid to reserve a new lock id.
			freeReplyObject(redis_reply);
			uint64_t lockId = 0;
			std::string guid_key = DPS_AND_DL_GUID_KEY;
			partitionIdx = getRedisServerPartitionIndex(guid_key);
			cmd = string(REDIS_INCR_CMD) + guid_key;
			redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

			if (redis_reply->type == REDIS_REPLY_ERROR) {
				lkError.set("Unable to get a unique lock id for a lock named " + name + ". " + std::string(redis_reply->str), DL_GUID_CREATION_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for a lock named " << name << ". " << DL_GUID_CREATION_ERROR, "RedisDBLayer");
				freeReplyObject(redis_reply);
				releaseGeneralPurposeLock(base64_encoded_name);
				return 0;
			}

			if (redis_reply->type == REDIS_REPLY_INTEGER) {
				// Get the newly created lock id.
				lockId = redis_reply->integer;
				freeReplyObject(redis_reply);

				// We secured a guid. We can now create this lock.
				//
				// 1) Create the Lock Name
				//    '5' + 'lock name' ==> 'lock id'
				std::ostringstream value;
				value << lockId;
				std::string value_string = value.str();
				partitionIdx = getRedisServerPartitionIndex(lockNameKey);
				cmd = string(REDIS_SET_CMD) + lockNameKey + " " + value_string;
				redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

				if (redis_reply->type == REDIS_REPLY_ERROR) {
					// Problem in creating the "Lock Name" entry in the cache.
					lkError.set("Unable to create 'LockName:LockId' in the cache for a lock named " + name + ". " + std::string(redis_reply->str), DL_LOCK_NAME_CREATION_ERROR);
					SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for a lock named " << name << ". " << DL_LOCK_NAME_CREATION_ERROR, "RedisDBLayer");
					// We are simply leaving an incremented value for the dps_and_dl_guid key in the cache that will never get used.
					// Since it is harmless, there is no need to reduce this number by 1. It is okay that this guid number will remain unassigned to any store or a lock.
					freeReplyObject(redis_reply);
					releaseGeneralPurposeLock(base64_encoded_name);
					return 0;
				}

				// 2) Create the Lock Info
				//    '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
				freeReplyObject(redis_reply);
				std::string lockInfoKey = DL_LOCK_INFO_TYPE + value_string;  // LockId becomes the new key now.
				value_string = string("0_0_0_") + base64_encoded_name;
				partitionIdx = getRedisServerPartitionIndex(lockInfoKey);
				cmd = string(REDIS_SET_CMD) + lockInfoKey + " " + value_string;
				redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

				if (redis_reply->type == REDIS_REPLY_ERROR) {
					// Problem in creating the "LockId:LockInfo" entry in the cache.
					lkError.set("Unable to create 'LockId:LockInfo' in the cache for a lock named " + name + ". " + std::string(redis_reply->str), DL_LOCK_INFO_CREATION_ERROR);
					SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for a lock named " << name << ". " << DL_LOCK_INFO_CREATION_ERROR, "RedisDBLayer");
					// Delete the previous entry we made.
					freeReplyObject(redis_reply);
					partitionIdx = getRedisServerPartitionIndex(lockNameKey);
					cmd = string(REDIS_DEL_CMD) + lockNameKey;
					redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
					freeReplyObject(redis_reply);
					releaseGeneralPurposeLock(base64_encoded_name);
					return 0;
				}

				// We created the lock.
				freeReplyObject(redis_reply);
				SPLAPPTRC(L_DEBUG, "Inside createOrGetLock done for a lock named " << name, "RedisDBLayer");
				releaseGeneralPurposeLock(base64_encoded_name);
				return (lockId);
			}
		}

		freeReplyObject(redis_reply);
		releaseGeneralPurposeLock(base64_encoded_name);
		return 0;
  }

  bool RedisDBLayer::removeLock(uint64_t lock, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside removeLock for lock id " << lock, "RedisDBLayer");

		ostringstream lockId;
		lockId << lock;
		string lockIdString = lockId.str();

		// If the lock doesn't exist, there is nothing to remove. Don't allow this caller inside this method.
		if(lockIdExistsOrNot(lockIdString, lkError) == false) {
			if (lkError.hasError() == true) {
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "RedisDBLayer");
			} else {
				lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "RedisDBLayer");
			}

			return(false);
		}

		// Before removing the lock entirely, ensure that the lock is not currently being used by anyone else.
		if (acquireLock(lock, 5, 3, lkError) == false) {
			// Unable to acquire the distributed lock.
			lkError.set("Unable to get a distributed lock for the LockId " + lockIdString + ".", DL_GET_DISTRIBUTED_LOCK_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for the lock id " << lockIdString << ". " << DL_GET_DISTRIBUTED_LOCK_ERROR, "RedisDBLayer");
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
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "RedisDBLayer");
			releaseLock(lock, lkError);
			// This is alarming. This will put this lock in a bad state. Poor user has to deal with it.
			return(false);
		}

		// Let us first remove the lock info for this distributed lock.
		// '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
		int32_t partitionIdx = getRedisServerPartitionIndex(lockInfoKey);
		string cmd = string(REDIS_DEL_CMD) + lockInfoKey;
		redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
		freeReplyObject(redis_reply);

		// We can now delete the lock name root entry.
		string lockNameKey = DL_LOCK_NAME_TYPE + lockName;
		partitionIdx = getRedisServerPartitionIndex(lockNameKey);
		cmd = string(REDIS_DEL_CMD) + lockNameKey;
		redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
		freeReplyObject(redis_reply);

		// We can delete the lock item itself now.
		releaseLock(lock, lkError);
		// Inside the release lock function we called in the previous statement, it makes a
		// call to update the lock info. That will obviously fail since we removed here everything about
		// this lock. Hence, let us not propagate that error and cause the user to panic.
		// Reset any error that may have happened in the previous operation of releasing the lock.
		lkError.reset();
		// Life of this lock ended completely with no trace left behind.
	    return(true);
  }

  bool RedisDBLayer::acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside acquireLock for lock id " << lock, "RedisDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();
	  int32_t retryCnt = 0;
	  string cmd = "";

	  // If the lock doesn't exist, there is nothing to acquire. Don't allow this caller inside this method.
	  if(lockIdExistsOrNot(lockIdString, lkError) == false) {
		  if (lkError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "RedisDBLayer");
		  } else {
			  lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for lock id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "RedisDBLayer");
		  }

		  return(false);
	  }

	  // We will first check if we can get this lock.
	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  int32_t partitionIdx = getRedisServerPartitionIndex(distributedLockKey);
	  time_t startTime, timeNow;
	  // Get the start time for our lock acquisition attempts.
	  time(&startTime);

	  //Try to get a distributed lock.
	  while(1) {
		  // This is an atomic activity.
		  // If multiple threads attempt to do it at the same time, only one will succeed.
		  // Winner will hold the lock until they release it voluntarily or
		  // until the Redis back-end removes this lock entry after the lease time ends.
		  // We will add the lease time to the current timestamp i.e. seconds elapsed since the epoch.
		  time_t new_lock_expiry_time = time(0) + (time_t)leaseTime;
		  cmd = string(REDIS_SETNX_CMD) + distributedLockKey + " " + "1";
		  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

			if (redis_reply == NULL) {
				return(false);
			}

			if (redis_reply->type == REDIS_REPLY_ERROR) {
				// Problem in atomic creation of the distributed lock.
				freeReplyObject(redis_reply);
				return(false);
			}

			if (redis_reply->integer == (int)1) {
				// We got the lock.
				// Set the expiration time for this lock key.
				freeReplyObject(redis_reply);
				ostringstream expiryTimeInMillis;
				expiryTimeInMillis << (leaseTime*1000.00);
				cmd = string(REDIS_PSETEX_CMD) + distributedLockKey + " " + expiryTimeInMillis.str() + " " + "2";
				redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

				if (redis_reply == NULL) {
					// Delete the erroneous lock data item we created.
					cmd = string(REDIS_DEL_CMD) + " " + distributedLockKey;
					redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
					freeReplyObject(redis_reply);
					return(false);
				}

				if (redis_reply->type == REDIS_REPLY_ERROR) {
					// Problem in atomic creation of the general purpose lock.
					freeReplyObject(redis_reply);
					// Delete the erroneous lock data item we created.
					cmd = string(REDIS_DEL_CMD) + " " + distributedLockKey;
					redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());
					freeReplyObject(redis_reply);
					return(false);
				}

				freeReplyObject(redis_reply);

				// We got the lock.
				// Let us update the lock information now.
				if(updateLockInformation(lockIdString, lkError, 1, new_lock_expiry_time, getpid()) == true) {
					return(true);
				} else {
					// Some error occurred while updating the lock information.
					// It will be in an inconsistent state. Let us release the lock.
					releaseLock(lock, lkError);
				}
			} else {
				// We didn't get the lock.
				// Let us check if the previous owner of this lock simply forgot to release it.
				// In that case, we will release this expired lock.
				// Read the time at which this lock is expected to expire.
				freeReplyObject(redis_reply);
				uint32_t _lockUsageCnt = 0;
				int32_t _lockExpirationTime = 0;
				std::string _lockName = "";
				pid_t _lockOwningPid = 0;

				if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
					SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "RedisDBLayer");
				} else {
					// Is current time greater than the lock expiration time?
					if ((_lockExpirationTime > 0) && (time(0) > (time_t)_lockExpirationTime)) {
						// Time has passed beyond the lease of this lock.
						// Lease expired for this lock. Original owner forgot to release the lock and simply left it hanging there without a valid lease.
						releaseLock(lock, lkError);
					}
				}
			}

			// Someone else is holding on to this distributed lock. Wait for a while before trying again.
			retryCnt++;

			if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
				lkError.set("Unable to acquire the lock named " + lockIdString + ".", DL_GET_LOCK_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for a lock named " << lockIdString << ". " << DL_GET_LOCK_ERROR, "RedisDBLayer");
				// Our caller can check the error code and try to acquire the lock again.
				return(false);
			}

			// Check if we have gone past the maximum wait time the caller was willing to wait in order to acquire this lock.
			time(&timeNow);
			if (difftime(startTime, timeNow) > maxWaitTimeToAcquireLock) {
				lkError.set("Unable to acquire the lock named " + lockIdString + " within the caller specified wait time.", DL_GET_LOCK_TIMEOUT_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire the lock named " << lockIdString <<
					" within the caller specified wait time." << DL_GET_LOCK_TIMEOUT_ERROR, "RedisDBLayer");
				// Our caller can check the error code and try to acquire the lock again.
				return(false);
			}

			// Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
			usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
				(retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
	  } // End of while(1)
  }

  void RedisDBLayer::releaseLock(uint64_t lock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside releaseLock for lock id " << lock, "RedisDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();

	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  int32_t partitionIdx = getRedisServerPartitionIndex(distributedLockKey);
	  string cmd = string(REDIS_DEL_CMD) + distributedLockKey;
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	  if (redis_reply->type == REDIS_REPLY_ERROR) {
		  lkError.set("Unable to release the distributed lock id " + lockIdString + ". " + std::string(redis_reply->str), DL_LOCK_RELEASE_ERROR);
		  freeReplyObject(redis_reply);
		  return;
	  }

	  freeReplyObject(redis_reply);
	  updateLockInformation(lockIdString, lkError, 0, 0, 0);
  }

  bool RedisDBLayer::updateLockInformation(std::string const & lockIdString,
	PersistenceError & lkError, uint32_t const & lockUsageCnt, int32_t const & lockExpirationTime, pid_t const & lockOwningPid) {
	  // Get the lock name for this lock.
	  uint32_t _lockUsageCnt = 0;
	  int32_t _lockExpirationTime = 0;
	  std::string _lockName = "";
	  string cmd = "";
	  pid_t _lockOwningPid = 0;


	  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "RedisDBLayer");
		  return(false);
	  }

	  // Let us update the lock information.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  int32_t partitionIdx = getRedisServerPartitionIndex(lockInfoKey);
	  ostringstream lockInfoValue;
	  lockInfoValue << lockUsageCnt << "_" << lockExpirationTime << "_" << lockOwningPid << "_" << _lockName;
	  string lockInfoValueString = lockInfoValue.str();
	  cmd = string(REDIS_SET_CMD) + lockInfoKey + " " + lockInfoValueString;
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	  if (redis_reply->type == REDIS_REPLY_ERROR) {
		  // Problem in updating the "LockId:LockInfo" entry in the cache.
		  lkError.set("Unable to update 'LockId:LockInfo' in the cache for a lock named " + _lockName + ". " + std::string(redis_reply->str), DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed for a lock named " << _lockName << ". " << DL_LOCK_INFO_UPDATE_ERROR, "RedisDBLayer");
		  freeReplyObject(redis_reply);
		  return(false);
	  }

	  freeReplyObject(redis_reply);
	  return(true);
  }

  bool RedisDBLayer::readLockInformation(std::string const & lockIdString, PersistenceError & lkError, uint32_t & lockUsageCnt,
		  int32_t & lockExpirationTime, pid_t & lockOwningPid, std::string & lockName) {
	  // Read the contents of the lock information.
	  lockName = "";
	  string cmd = "";

	  // Lock Info contains meta data information about a given lock.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  int32_t partitionIdx = getRedisServerPartitionIndex(lockInfoKey);
	  cmd = string(REDIS_GET_CMD) + lockInfoKey;
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	  if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Unable to get the LockInfo from our cache.
		lkError.set("Unable to get LockInfo using the LockId " + lockIdString +
			". " + std::string(redis_reply->str), DL_GET_LOCK_INFO_ERROR);
		freeReplyObject(redis_reply);
		return(false);
	  }

	  std::string lockInfo = std::string(redis_reply->str, redis_reply->len);
	  freeReplyObject(redis_reply);

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
		  }
	  }

	  if (lockName == "") {
		  // Unable to get the name of this lock.
		  lkError.set("Unable to get the lock name for lockId " + lockIdString + ".", DL_GET_LOCK_NAME_ERROR);
		  return(false);
	  }

	  return(true);
  }

  // This method will check if a lock exists for a given lock id.
  bool RedisDBLayer::lockIdExistsOrNot(string lockIdString, PersistenceError & lkError) {
	  string keyString = string(DL_LOCK_INFO_TYPE) + lockIdString;
	  int32_t partitionIdx = getRedisServerPartitionIndex(keyString);
	  string cmd = string(REDIS_EXISTS_CMD) + keyString;
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	  if (redis_reply == NULL) {
		lkError.set("LockIdExistsOrNot: Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DPS_CONNECTION_ERROR);
		return(false);
	  }

	  if (redis_reply->type == REDIS_REPLY_ERROR) {
		// Unable to get the lock info for the given lock id.
		lkError.set("LockIdExistsOrNot: Unable to get LockInfo for the lockId " + lockIdString +
			". " + std::string(redis_reply->str), DL_GET_LOCK_INFO_ERROR);
		freeReplyObject(redis_reply);
		return(false);
	  }

	  bool lockIdExists = true;

	  if (redis_reply->integer == (int)0) {
		  lockIdExists = false;
	  }

	  freeReplyObject(redis_reply);
	  return(lockIdExists);
  }

  // This method will return the process id that currently owns the given lock.
  uint32_t RedisDBLayer::getPidForLock(string const & name, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside getPidForLock with a name " << name, "RedisDBLayer");

	  string base64_encoded_name;
	  base64_encode(name, base64_encoded_name);
	  uint64_t lock = 0;

	  // Let us first see if a lock with the given name already exists.
	  // In our Redis dps implementation, data item keys can have space characters.
	  // Inside Redis, all our lock names will have a mapping type indicator of
	  // "5" at the beginning followed by the actual lock name.
	  // '5' + 'lock name' ==> 'lock id'
	  std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
	  int32_t partitionIdx = getRedisServerPartitionIndex(lockNameKey);
	  std::string cmd = string(REDIS_EXISTS_CMD) + lockNameKey;
	  redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

	  // If we get a NULL reply, then it indicates a redis server connection error.
	  if (redis_reply == NULL) {
		lkError.set("Unable to connect to the redis server(s). " + std::string(redisPartitions[partitionIdx].rdsc->errstr), DL_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for the lock named " << name << ". " << DL_CONNECTION_ERROR, "RedisDBLayer");
		return(0);
	  }

	  if (redis_reply->integer == (int)1) {
		// This lock already exists in our cache.
		// We can get the lockId and return it to the caller.
		freeReplyObject(redis_reply);
		cmd = string(REDIS_GET_CMD) + lockNameKey;
		redis_reply = (redisReply*)redisCommand(redisPartitions[partitionIdx].rdsc, cmd.c_str());

		if (redis_reply->type == REDIS_REPLY_ERROR) {
			// Unable to get an existing lock id from the cache.
			lkError.set("Unable to get the lockId for the lockName " + name + ". " + std::string(redis_reply->str), DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for the lockName " << name << ". " << DL_GET_LOCK_ID_ERROR, "RedisDBLayer");
			freeReplyObject(redis_reply);
			return(0);
		} else {
			if (redis_reply->len > 0) {
				lock = streams_boost::lexical_cast<uint64_t>(redis_reply->str);
			} else {
				// Unable to get the lock information. It is an abnormal error. Convey this to the caller.
				lkError.set("Redis returned an empty lockId for the lockName " + name + ".", DL_GET_LOCK_ID_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed with an empty lockId for the lockName " << name << ". " << DL_GET_LOCK_ID_ERROR, "RedisDBLayer");
				freeReplyObject(redis_reply);
				return(0);
			}

			freeReplyObject(redis_reply);
		}
	  }

	  if (lock == 0) {
		  // Lock with the given name doesn't exist.
		  lkError.set("Unable to find a lockName " + name + ".", DL_LOCK_NOT_FOUND_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getPidForLock, unable to find the lockName " << name << ". " << DL_LOCK_NOT_FOUND_ERROR, "RedisDBLayer");
		  freeReplyObject(redis_reply);
		  return(0);
	  }

	  // Read the lock information.
	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();

	  uint32_t _lockUsageCnt = 0;
	  int32_t _lockExpirationTime = 0;
	  std::string _lockName = "";
	  pid_t _lockOwningPid = 0;

	  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "RedisDBLayer");
		  return(0);
	  } else {
		  return(_lockOwningPid);
	  }
  }

  // This method will return the redis server partition index for a given key string.
  inline int32_t RedisDBLayer::getRedisServerPartitionIndex(std::string const & key) {
	  if (redisPartitionCnt == 0) {
		  // We only have a single redis server.
		  return(0);
	  } else {
		  // We have multiple Redis servers and that means we are doing client side partitioning.
		  // Hence, pick the correct Redis server partition for the given key.
		  uint64_t hashValue = SPL::Functions::Utility::hashCode(key);
		  // Take modulo based on the available number of Redis servers.
		  return((int32_t)(hashValue % redisPartitionCnt));
	  }
  }

  // This method will return the status of the connection to the back-end data store.
  bool RedisDBLayer::isConnected() {
         if (redisPartitions[0].rdsc == NULL) {
            // There is no active connection.
            return(false);
         }

         // We will simply do a read API for a dummy key.
         // If it results in a connection error, that will tell us the status of the connection.
	 string cmd = string(REDIS_GET_CMD) + string("my_dummy_key");
	 redis_reply = (redisReply*)redisCommand(redisPartitions[0].rdsc, cmd.c_str());

	 if (redis_reply == NULL) {
            // Connection error.
	    return(false);
	 } else {
            // Connection is active.
            freeReplyObject(redis_reply);
            return(true);
         }
  }

  // This method will reestablish the status of the connection to the back-end data store.
  bool RedisDBLayer::reconnect(std::set<std::string> & dbServers, PersistenceError & dbError) {
         // We have to first free the existing redis context.
	 if (redisPartitionCnt == 0) {
		// We are not using the client side Redis partitioning.
		// Clear the single redis connection we opened.
		// In this case of just a single Redis server instance being used,
		// its context address is stored in the very first element of the redis partition array.
		if (redisPartitions[0].rdsc != NULL) {
			redisFree(redisPartitions[0].rdsc);
			redisPartitions[0].rdsc = NULL;
		}
	 } else {
		// We are using the client side Redis partitioning.
		// Let us clear all the connections we made.
		for(int32_t cnt=0; cnt < redisPartitionCnt; cnt++) {
			if (redisPartitions[cnt].rdsc != NULL) {
				redisFree(redisPartitions[cnt].rdsc);
				redisPartitions[cnt].rdsc = NULL;
			}
		}
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
  }

} } } } }

using namespace com::ibm::streamsx::store;
using namespace com::ibm::streamsx::store::distributed;
extern "C" {
	DBLayer * create(){
	   return new RedisDBLayer();
	}
}
