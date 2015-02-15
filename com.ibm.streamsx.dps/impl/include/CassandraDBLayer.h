/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef CASSANDRA_DB_LAYER_H_
#define CASSANDRA_DB_LAYER_H_
/*
=====================================================================
Here is the copyright statement for our use of the Cassandra APIs:

Copyright (c) 2014 DataStax

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is
distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions and limitations under the
License.
=====================================================================
*/
#include "DBLayer.h"

#include <libcassandra-1.0/cassandra.h>
#include <tr1/memory>
#include <set>
#include <vector>

#include <SPL/Runtime/Type/SPLType.h>
#include <SPL/Runtime/Utility/Mutex.h>
#include <SPL/Runtime/Serialization/NativeByteBuffer.h>

using namespace SPL;
using namespace std;

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  class CassandraDBLayer;

  /// Class that implements the Iterator for Cassandra
  class CassandraDBLayerIterator : public DBLayer::Iterator
  {
  	  public:
	  	  uint64_t store;
	  	  std::string storeName;
  	  	  std::vector<std::string> dataItemKeys;
  	  	  uint32_t sizeOfDataItemKeysVector;
  	  	  uint32_t currentIndex;
  	  	  bool hasData;
  	  	  CassSession *session;
  	  	  CassandraDBLayer *cassandraDBLayerPtr;

  	  	  CassandraDBLayerIterator();
	  	  ~CassandraDBLayerIterator();
	  	  bool getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError);
  };

  /// Class that implements the DBLayer for Cassandra
  class CassandraDBLayer : public DBLayer
  {
  private:
	  CassCluster *cluster;
	  CassSession *session;
	  CassError execute_query(CassSession* session, const char* query, std::string & errorMsg);
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

  public:
	string mainTableName;
	string lockRowName;
    /// Constructor
    CassandraDBLayer();

    /// Destructor            
    ~CassandraDBLayer();

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
             unsigned char const * valueData, uint32_t valueSize, uint32_t ttl, PersistenceError & dbError);
    bool get(uint64_t store, char const * keyData, uint32_t keySize,
             unsigned char * & valueData, uint32_t & valueSize,
             PersistenceError & dbError);
    bool getSafe(uint64_t store, char const * keyData, uint32_t keySize,
             unsigned char * & valueData, uint32_t & valueSize,
             PersistenceError & dbError);
    bool getTTL(char const * keyData, uint32_t keySize,
             unsigned char * & valueData, uint32_t & valueSize,
             PersistenceError & dbError);
    bool remove(uint64_t store, char const * keyData, uint32_t keySize, PersistenceError & dbError);
    bool removeTTL(char const * keyData, uint32_t keySize, PersistenceError & dbError);
    bool has(uint64_t store, char const * keyData, uint32_t keySize, PersistenceError & dbError);
    bool hasTTL(char const * keyData, uint32_t keySize, PersistenceError & dbError);
    void clear(uint64_t store, PersistenceError & dbError);
    uint64_t size(uint64_t store, PersistenceError & dbError);
    void base64_encode(std::string const & str, std::string & base64);
    void base64_decode(std::string & base64, std::string & result);
    CassandraDBLayerIterator * newIterator(uint64_t store, PersistenceError & dbError);
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

	// Lock related methods.
    uint64_t createOrGetLock(std::string const & name, PersistenceError & lkError);
    void releaseLock(uint64_t lock, PersistenceError & lkError);
    bool acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError);
    bool removeLock(uint64_t lock, PersistenceError & lkError);
    uint32_t getPidForLock(std::string const & name, PersistenceError & lkError);

  };
} } } } }
#endif /* CASSANDRA_DB_LAYER_H_ */
