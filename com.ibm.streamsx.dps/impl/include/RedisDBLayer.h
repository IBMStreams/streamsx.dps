/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2022
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef REDIS_DB_LAYER_H_
#define REDIS_DB_LAYER_H_
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
#include "DBLayer.h"

#include "hiredis/hiredis.h"
#include "hiredis/hiredis_ssl.h"
#include <tr1/memory>
#include <set>
#include <vector>

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  class RedisDBLayer;

  /// Class that implements the Iterator for Redis
  class RedisDBLayerIterator : public DBLayer::Iterator
  {
  	  public:
	  	  uint64_t store;
	  	  std::string storeName;
  	  	  std::vector<std::string> dataItemKeys;
  	  	  uint32_t sizeOfDataItemKeysVector;
  	  	  uint32_t currentIndex;
  	  	  bool hasData;
  	  	  redisContext *rdsc;
  	  	  redisReply *redis_reply;
  	  	  RedisDBLayer *redisDBLayerPtr;

  	  	  RedisDBLayerIterator();
	  	  ~RedisDBLayerIterator();
	  	  bool getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError);
  };

  /// Class that implements the DBLayer for Redis
  class RedisDBLayer : public DBLayer
  {
  private:
	  redisReply *redis_reply;
	  // We will store one or more redis server handles in this structure.
	  typedef struct {
		  redisContext *rdsc;
	  } redisPartition;

	  redisPartition redisPartitions[50];

	  bool readStoreInformation(std::string const & storeIdString, PersistenceError & dbError,
	  		  uint32_t & dataItemCnt, std::string & storeName,
	  		  std::string & keySplTypeName, std::string & valueSplTypeName);
	  bool acquireStoreLock(std::string const &  storeIdString);
	  void releaseStoreLock(std::string const & storeIdString);
	  bool readLockInformation(std::string const & storeIdString, PersistenceError & dbError, uint32_t & lockUsageCnt,
		int32_t & lockExpirationTime, pid_t & lockOwningPid, std::string & lockName);
	  bool updateLockInformation(std::string const & lockIdString, PersistenceError & lkError,
		uint32_t const & lockUsageCnt, int32_t const & lockExpirationTime, pid_t const & lockOwningPid);
	  bool lockIdExistsOrNot(std::string lockIdString, PersistenceError & lkError);
	  bool acquireGeneralPurposeLock(std::string const & entityName);
	  void releaseGeneralPurposeLock(std::string const & entityName);
	  int32_t getRedisServerPartitionIndex(std::string const & key);

  public:
    int32_t redisPartitionCnt;

    /// Constructor
    RedisDBLayer();

    /// Destructor            
    ~RedisDBLayer();

    // These are inherited from DBLayer, see DBLayer for descriptions
    void connectToDatabase(std::set<std::string> const & dbServers, PersistenceError & dbError);
            
    uint64_t createStore(std::string const & name,
                         std::string const & keySplTypeName,
                         std::string const & valueSplTypeName,
                         PersistenceError & dbError);
    uint64_t createOrGetStore(std::string const & name,
                              std::string const & keySplTypeName,
                              std::string const & valueSplTypeName,
                              PersistenceError & dbError);
    uint64_t findStore(std::string const & name, 
                       PersistenceError & dbError);
    bool removeStore(uint64_t store, PersistenceError & dbError);

    bool put(uint64_t store, char const * keyData, uint32_t keySize,
             unsigned char const * valueData, uint32_t valueSize, PersistenceError & dbError);
    bool putSafe(uint64_t store, char const * keyData, uint32_t keySize,
             unsigned char const * valueData, uint32_t valueSize, PersistenceError & dbError);
    bool putTTL(char const * keyData, uint32_t keySize,
             unsigned char const * valueData, uint32_t valueSize, uint32_t ttl, PersistenceError & dbError, bool encodeKey=true, bool encodeValue=true);
    bool get(uint64_t store, char const * keyData, uint32_t keySize,
             unsigned char * & valueData, uint32_t & valueSize,
             PersistenceError & dbError);
    bool getSafe(uint64_t store, char const * keyData, uint32_t keySize,
             unsigned char * & valueData, uint32_t & valueSize,
             PersistenceError & dbError);
    bool getTTL(char const * keyData, uint32_t keySize,
             unsigned char * & valueData, uint32_t & valueSize,
             PersistenceError & dbError, bool encodeKey=true);
    bool remove(uint64_t store, char const * keyData, uint32_t keySize, PersistenceError & dbError);
    bool removeTTL(char const * keyData, uint32_t keySize, PersistenceError & dbError, bool encodeKey=true);
    bool has(uint64_t store, char const * keyData, uint32_t keySize, PersistenceError & dbError);
    bool hasTTL(char const * keyData, uint32_t keySize, PersistenceError & dbError, bool encodeKey=true);
    void clear(uint64_t store, PersistenceError & dbError);
    uint64_t size(uint64_t store, PersistenceError & dbError);
    void base64_encode(std::string const & str, std::string & base64);
    void base64_decode(std::string & base64, std::string & result);
    bool isConnected();
    bool reconnect(std::set<std::string> & dbServers, PersistenceError & dbError);

    RedisDBLayerIterator * newIterator(uint64_t store, PersistenceError & dbError);
    void deleteIterator(uint64_t store, Iterator * iter, PersistenceError & dbError);
    bool storeIdExistsOrNot(std::string storeIdString, PersistenceError & dbError);
	bool getDataItemFromStore(std::string const & storeIdString,
		std::string const & keyDataString, bool const & checkOnlyForDataItemExistence,
		bool const & skipDataItemExistenceCheck, unsigned char * & valueData,
		uint32_t & valueSize, PersistenceError & dbError);
	std::string getStoreName(uint64_t store, PersistenceError & dbError);
	std::string getSplTypeNameForKey(uint64_t store, PersistenceError & dbError);
	std::string getSplTypeNameForValue(uint64_t store, PersistenceError & dbError);
	std::string getNoSqlDbProductName(void);
	void getDetailsAboutThisMachine(std::string & machineName, std::string & osVersion, std::string & cpuArchitecture);
	bool runDataStoreCommand(std::string const & cmd, PersistenceError & dbError);
	bool runDataStoreCommand(uint32_t const & cmdType, std::string const & httpVerb,
		std::string const & baseUrl, std::string const & apiEndpoint, std::string const & queryParams,
		std::string const & jsonRequest, std::string & jsonResponse, PersistenceError & dbError);
        bool runDataStoreCommand(std::vector<std::string> const & cmdList, std::string & resultValue, PersistenceError & dbError);
    void getAllKeys(uint64_t store, std::vector<unsigned char *> & keysBuffer, std::vector<uint32_t> & keysSize, PersistenceError & dbError);

	// Lock related methods.
    uint64_t createOrGetLock(std::string const & name, PersistenceError & lkError);
    void releaseLock(uint64_t lock, PersistenceError & lkError);
    bool acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError);
    bool removeLock(uint64_t lock, PersistenceError & lkError);
    uint32_t getPidForLock(std::string const & name, PersistenceError & lkError);
  };
} } } } }
#endif /* REDIS_DB_LAYER_H_ */
