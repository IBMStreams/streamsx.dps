/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2015
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef AEROSPIKE_DB_LAYER_H_
#define AEROSPIKE_DB_LAYER_H_
/*
=================================================================================
Aerospike is a NoSQL DB open source product that provides a Key/Value store
function. It provides flexible features to store different data types in
the store.  It provides replication for high availability and it can
run on multiple machines. It has a wide range of client libraries
available in multiple programming languages. For our needs here, we are
using the Aerospike client library for C.

libaerospike.so

We manually built the shared library (.so file) separately outside of Streams
and then copied the include directory (aerospike) into this toolkit's
impl/include directory. The .so file we built separately was also
copied into the impl/lib directory within the OS specific sub-directory.
=================================================================================
*/
#include "DBLayer.h"

// Aerospike include files have pure C code. Unlike the other K/V store C libraries we are using in
// the DPS (Cassandra, Mongo, HBase etc.), Aerospike include files don't have the extern "C" {..} declaration in them.
// Because of that, C++ compiler will mangle the Aerospike C APIs with the ugly additional characters that is typical
// in the C++ world. (e-g: _Z17aerospike_key_putP11aerospike_sP10as_error_sPK17as_policy_write_sPK8as_key_sP11as_record_s)
// Due to that name mangling, we will get a runtime exception during the PE start-up as shown below:
// libDistributedProcessStoreLib.so: undefined symbol: _Z17aerospike_key_putP11aerospike_sP10as_error_sPK17as_policy_write_sPK8as_key_sP11as_record_s
//
// To avoid that run-time error, we must do the extern "C" declaration and enclose all the aerospike include files within that.
// This should have been done inside the Aerospike files. Since our friends at Aerospike didn't do it, we will do it ourselves below.
//
extern "C" {
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_index.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_bin.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_error.h>
#include <aerospike/as_config.h>
#include <aerospike/as_key.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_password.h>
#include <aerospike/as_record.h>
#include <aerospike/as_record_iterator.h>
#include <aerospike/as_status.h>
#include <aerospike/as_string.h>
#include <aerospike/as_val.h>
}

#include <set>
#include <vector>

using namespace std;

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  class AerospikeDBLayer;

  /// Class that implements the Iterator for MongoDB
  class AerospikeDBLayerIterator : public DBLayer::Iterator
  {
  	  public:
	  	  uint64_t store;
	  	  std::string storeName;
  	  	  std::vector<std::string> dataItemKeys;
  	  	  uint32_t sizeOfDataItemKeysVector;
  	  	  uint32_t currentIndex;
  	  	  bool hasData;
  	  	  AerospikeDBLayer *AerospikeDBLayerPtr;

  	  	  AerospikeDBLayerIterator();
	  	  ~AerospikeDBLayerIterator();
	  	  bool getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError);
  };

  /// Class that implements the DBLayer for MongoDB
  class AerospikeDBLayer : public DBLayer
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
	aerospike *as;
	AerospikeDBLayerIterator *asDBLayerIterator;
	int64_t storeSetSize;

    /// Constructor
    AerospikeDBLayer();

    /// Destructor
    ~AerospikeDBLayer();

	// Aerospike specific functions.
    bool createStoreIdTrackerSet(string & errorMsg);
    bool getStoreIdNotInUse(string const & storeName, uint64_t & storeId, string & errorMsg);
    bool setStoreIdToNotInUse(uint64_t const & storeId, string & errorMsg);
    bool readAerospikeBinValue(string const & setName, const char *keyName, string & valueString, string & errorMsg);
    bool readAerospikeSetSize(string const & storeSetName, int64_t & setSize, string & errorMsg);
    bool getAllKeysInSet(string const & storeSetName, string & errorMsg);
    static bool remove_store_callback(const as_val *val, void *udata);
    void removeStoreImpl(const as_val *val);
    static bool clear_store_callback(const as_val *val, void *udata);
    void clearStoreImpl(const as_val *val);
    static bool get_all_keys_callback(const as_val *val, void *udata);
    void getAllKeysImpl(const as_val *val);
    static bool obtain_set_size_callback(const as_val *val, void *udata);
    void obtainSetSizeImpl(const as_val *val);

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
    bool is_b64(unsigned char c);
    void b64_encode(unsigned char const * & buf, uint32_t const & bufLenOrg, string & ret);
    void b64_decode(std::string & encoded_string, unsigned char * & buf, uint32_t & bufLen);
    AerospikeDBLayerIterator * newIterator(uint64_t store, PersistenceError & dbError);
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
#endif /* AEROSPIKE_DB_LAYER_H_ */
