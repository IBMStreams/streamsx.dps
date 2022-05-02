/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2022
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef COUCHBASE_DB_LAYER_H_
#define COUCHBASE_DB_LAYER_H_
/*
=================================================================================
Couchbase is a NoSQL DB open source product that can provide a Key/Value store
functionality. More specifically, Couchbase is a document database. It combines
the features of membase (follow-on project for memcached) and CouchDB. It brings
the state of the art in-memory store capabilities from membase and a robust
persistence, multi-machine replication, sharding of data on many servers and
fault tolerance features from CouchDB. It has a wide range of client libraries
available in multiple programming languages. For our needs here, we are
using the Couchbase C SDK client library. For store creation, store deletion,
getting store size etc., we will use the Couchbase REST APIs using cURL.

libcouchbase

We built this shared library (.so file) separately outside of Streams
and then copied the include directory (libcouchbase) into this toolkit's
impl/include directory. That .so file we built separately was also
copied into the impl/lib directory within the OS specific sub-directory.
=================================================================================
*/
#include "DBLayer.h"

#include <curl/curl.h>
#include <json-c/json.h>
#include <libcouchbase/couchbase.h>
#include <set>
#include <vector>

using namespace std;

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  class CouchbaseDBLayer;

  /// Class that implements the Iterator for CousebaseDB
  class CouchbaseDBLayerIterator : public DBLayer::Iterator
  {
  	  public:
	  	  uint64_t store;
	  	  std::string storeName;
  	  	  std::vector<std::string> dataItemKeys;
  	  	  uint32_t sizeOfDataItemKeysVector;
  	  	  uint32_t currentIndex;
  	  	  bool hasData;
  	  	  CouchbaseDBLayer *couchbaseDBLayerPtr;

  	  	  CouchbaseDBLayerIterator();
	  	  ~CouchbaseDBLayerIterator();
	  	  bool getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError);
  };

  /// Class that implements the DBLayer for CousebaseDB
  class CouchbaseDBLayer : public DBLayer
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
	string base64_chars;
	CURL *curlForCreateCouchbaseBucket;
	CURL *curlForDeleteCouchbaseBucket;
	CURL *curlForGetCouchbaseBucket;
	struct curl_slist *headersForCreateCouchbaseBucket;
	struct curl_slist *headersForCreateCouchbaseBucket2;
	struct curl_slist *headersForDeleteCouchbaseBucket;
	struct curl_slist *headersForGetCouchbaseBucket;
	bool curlGlobalCleanupNeeded;
    int32_t curlBufferOffset;
    char curlBuffer[10*1024*1024]; // 10 MB in size.
    const char *putBuffer;
    string curlBasicAuth;
    string couchbaseServerUrl;
    string couchbaseServers[50];
    int32_t couchbaseServerIdx;
    int32_t totalCouchbaseServers;
    lcb_error_t lastCouchbaseErrorCode;
    string lastCouchbaseErrorMsg;
    string lastCouchbaseOperationKey;
    string lastCouchbaseOperationValue;

    /// Constructor
    CouchbaseDBLayer();

    /// Destructor
    ~CouchbaseDBLayer();

	// Couchbase specific functions.
	// First, we will have the cURL write and read functions.
    // Since we are using C++ and the cURL library is C based, we have to set the callback as a
    // static C++ method and configure cURL to pass our custom C++ object pointer in the 4th argument of
    // the callback function. Once we get the C++ object pointer, we can access our other non-static
    // member functions and non-static member variables via that object pointer.
	static size_t writeFunction(char *data, size_t size, size_t nmemb, void *objPtr);
	size_t writeFunctionImpl(char *data, size_t size, size_t nmemb);
	// Do the same for the read function.
	static size_t readFunction(char *data, size_t size, size_t nmemb, void *objPtr);
	size_t readFunctionImpl(char *data, size_t size, size_t nmemb);
	// Couchbase callback functions follow below with their prototypes.
	static void storage_callback(lcb_t instance, const void *cookie, lcb_storage_t op,
		lcb_error_t err, const lcb_store_resp_t *resp);
	void storageImpl(lcb_t instance, lcb_error_t err, const lcb_store_resp_t *resp);
	static void get_callback(lcb_t instance, const void *cookie, lcb_error_t err, const lcb_get_resp_t *resp);
	void getImpl(lcb_t instance, lcb_error_t err, const lcb_get_resp_t *resp);
	static void remove_callback(lcb_t instance, const void *cookie, lcb_error_t err, const lcb_remove_resp_t *resp);
	void removeImpl(lcb_t instance, lcb_error_t err, const lcb_remove_resp_t *resp);
	// Couchbase operational functions are given below.
	bool createCouchbaseBucket(string const & bucketName, string & errorMsg, string const & ramBucketQuota);
	bool deleteCouchbaseBucket(string const & bucketName, string & errorMsg);
	bool getCouchbaseBucketSize(string const & bucketName, int64_t & bucketSize, string & errorMsg);
	bool getAllKeysInCouchbaseBucket(string const & bucketName,
		  std::vector<std::string> & dataItemKeys, string & errorMsg);

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

    CouchbaseDBLayerIterator * newIterator(uint64_t store, PersistenceError & dbError);
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
    void getKeys(uint64_t store, std::vector<unsigned char *> & keysBuffer, std::vector<uint32_t> & keysSize, int32_t keyStartPosition, int32_t numberOfKeysNeeded, PersistenceError & dbError);
    void getValues(uint64_t store, std::vector<char *> const & keyData, std::vector<uint32_t> const & keySize, std::vector<unsigned char *> & valueData, std::vector<uint32_t> & valueSize, PersistenceError & dbError);
    void putKVPairs(uint64_t store, std::vector<char *> const & keyData, std::vector<uint32_t> const & keySize, std::vector<unsigned char *> const & valueData, std::vector<uint32_t> const & valueSize, PersistenceError & dbError);
    void hasKeys(uint64_t store, std::vector<char *> const & keyData, std::vector<uint32_t> const & keySize, std::vector<bool> & results, PersistenceError & dbError);
    void removeKeys(uint64_t store, std::vector<char *> const & keyData, std::vector<uint32_t> const & keySize, int32_t & totalKeysRemoved, PersistenceError & dbError);

  };
} } } } }
#endif /* COUCHBASE_DB_LAYER_H_ */
