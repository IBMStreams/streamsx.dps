/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2022
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef MONGO_DB_LAYER_H_
#define MONGO_DB_LAYER_H_
/*
=================================================================================
MongoDB is a NoSQL DB open source product that can provide a Key/Value store
functionality. More specifically, MongoDB is a document database. It provides
replication for high availability, sharding for good performance with an
ability to run on multiple machines. It has a wide range of client libraries
available in multiple programming languages. For our needs here, we are
using the MongoDB client library for C which is made of the following two
components.

libbson
libmongoc

We built those two shared libraries (.so files) separately outside of Streams
and then copied the two include directories (libbson and libmongoc) into this toolkit's
impl/include directory. Those two .so files we built separately were also
copied into the impl/lib directory within the OS specific sub-directory.
=================================================================================
*/
#include "DBLayer.h"

#include <json-c/json.h>
#include <libbson-1.0/bson.h>
#include <libmongoc-1.0/mongoc.h>
#include <set>
#include <vector>

using namespace std;

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  class MongoDBLayer;

  /// Class that implements the Iterator for MongoDB
  class MongoDBLayerIterator : public DBLayer::Iterator
  {
  	  public:
	  	  uint64_t store;
	  	  std::string storeName;
  	  	  std::vector<std::string> dataItemKeys;
  	  	  uint32_t sizeOfDataItemKeysVector;
  	  	  uint32_t currentIndex;
  	  	  bool hasData;
  	  	  MongoDBLayer *mongoDBLayerPtr;

  	  	  MongoDBLayerIterator();
	  	  ~MongoDBLayerIterator();
	  	  bool getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError);
  };

  /// Class that implements the DBLayer for MongoDB
  class MongoDBLayer : public DBLayer
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

  public:
	bool ttlIndexCreated;
	bool genericLockIndexCreated;
	bool storeLockIndexCreated;
	mongoc_client_t *mClient;
	string base64_chars;

    /// Constructor
    MongoDBLayer();

    /// Destructor
    ~MongoDBLayer();

	// Mongo specific functions.
    bool readMongoDocumentValue(string const & dbName, string const & collectionName,
    	string const & key, string & value, string & errorMsg);
    bool readMongoCollectionSize(string const & dbName,
         string const & collectionName, int64_t & size, string & errorMsg);
    bool getAllKeysInCollection(string const & dbName, string const & collectionName,
    	  std::vector<std::string> & dataItemKeys, string & errorMsg);
    bool runSimpleDatabaseCommand(string const & dbName, string const & command,
    	  string const & cmdParam, string & errorMsg);
    bool runSimpleCollectionCommand(string const & dbName, string const & collectionName,
    	  string const & command, string const & cmdParam, string & errorMsg);

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
    bool is_b64(unsigned char c);
    void b64_encode(unsigned char const * & buf, uint32_t const & bufLenOrg, string & ret);
    void b64_decode(std::string & encoded_string, unsigned char * & buf, uint32_t & bufLen);
    bool isConnected();
    bool reconnect(std::set<std::string> & dbServers, PersistenceError & dbError);

    MongoDBLayerIterator * newIterator(uint64_t store, PersistenceError & dbError);
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
    void getAllKeys(uint64_t store, std::vector<unsigned char *> & keysBuffer, std::vector<uint32_t> & keysSize, PersistenceError & dbError);

  };
} } } } }
#endif /* MONGO_DB_LAYER_H_ */
