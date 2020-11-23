/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2020
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef REDIS_CLUSTER_PLUS_PLUS_DB_LAYER_H_
#define REDIS_CLUSTER_PLUS_PLUS_DB_LAYER_H_
/*
=====================================================================
Here is the copyright statement for our use of the hiredis APIs:

Hiredis was written by Salvatore Sanfilippo (antirez at gmail) and
Pieter Noordhuis (pcnoordhuis at gmail) and is released under the
BSD license.

Copyright (c) 2009-2011, Salvatore Sanfilippo <antirez at gmail dot com>
Copyright (c) 2010-2011, Pieter Noordhuis <pcnoordhuis at gmail dot com>

hiredis-cluster-plus-plus include files provide wrappers on top of the hiredis library.
This wrapper allows us to use the familiar hiredis APIs in the context of
a Redis cluster (Version 6 and higher) to provide HA facility for automatic
fail-over when a Redis instance or the entire machine crashes.
In addition, it provides the TLS/SSL support for the redis-cluster.
Please note that this hiredis-cluster-plus-plus wrapper supercedes the older 
hiredis-cluster wrapper that we have in the DPS toolkit. If the Redis server
version is v5 and lower, one may continue to use the older hiredis-cluster in DPS.
If the Redis server version is v6 and higher, it is recommented to use the 
hiredis-cluster-plus-plus in order to work with both non-TLS and TLS.

The redis-plus-plus wrapper carries the Apache 2.0 copyright as shown in the following line.

A permissive license whose main conditions require preservation of copyright and license notices. 
Contributors provide an express grant of patent rights. Licensed works, modifications, and larger 
works may be distributed under different terms and without source code.
=====================================================================
*/
#include "DBLayer.h"

#include <tr1/memory>
#include <set>
#include <vector>
#include <sw/redis++/redis_cluster.h>

using namespace sw::redis;

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  class RedisClusterPlusPlusDBLayer;

  /// Class that implements the Iterator for Redis cluster
  class RedisClusterPlusPlusDBLayerIterator : public DBLayer::Iterator
  {
  	  public:
	  	  uint64_t store;
	  	  std::string storeName;
  	  	  std::vector<std::string> dataItemKeys;
  	  	  uint32_t sizeOfDataItemKeysVector;
  	  	  uint32_t currentIndex;
  	  	  bool hasData;
  	  	  RedisClusterPlusPlusDBLayer *redisClusterPlusPlusDBLayerPtr;
 
  	  	  RedisClusterPlusPlusDBLayerIterator();
	  	  ~RedisClusterPlusPlusDBLayerIterator();
	  	  bool getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError);
  };

  /// Class that implements the DBLayer for Redis Cluster Plus Plus
  class RedisClusterPlusPlusDBLayer : public DBLayer
  {
  private:
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
    RedisCluster *redis_cluster = NULL;

    /// Constructor
    RedisClusterPlusPlusDBLayer();

    /// Destructor            
    ~RedisClusterPlusPlusDBLayer();

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

    RedisClusterPlusPlusDBLayerIterator * newIterator(uint64_t store, PersistenceError & dbError);
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

	// Lock related methods.
    uint64_t createOrGetLock(std::string const & name, PersistenceError & lkError);
    void releaseLock(uint64_t lock, PersistenceError & lkError);
    bool acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError);
    bool removeLock(uint64_t lock, PersistenceError & lkError);
    uint32_t getPidForLock(std::string const & name, PersistenceError & lkError);
    void persist(PersistenceError & dbError);

  };
} } } } }
#endif /* REDIS_CLUSTER_PLUS_PLUS_DB_LAYER_H_ */
