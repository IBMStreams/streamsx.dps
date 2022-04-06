/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2022
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef HBASE_DB_LAYER_H_
#define HBASE_DB_LAYER_H_
/*
=================================================================================
HBase is an Apache NoSQL DB open source product that can run on top of a Hadoop
cluster environment to provide a fault tolerant/scalable K/V store.
HBase has a full fledged set of Java APIs, but its C APIs are not adequate (as of Nov/2014).
However, HBase supports very good REST APIs and hence we are using the following
open source components required to perform REST calls.
1) cURL
2) json-c

We built those two shared libraries (.so files) separately outside of Streams
and then copied the two include directories (curl and json) into this toolkit's
impl/include directory. Those two .so files we built separately were also
copied into the impl/lib directory within the OS specific sub-directory.
=================================================================================
*/
#include "DBLayer.h"

#include <curl/curl.h>
#include <json-c/json.h>
#include <set>
#include <vector>

using namespace std;

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  class HBaseDBLayer;

  /// Class that implements the Iterator for HBase
  class HBaseDBLayerIterator : public DBLayer::Iterator
  {
  	  public:
	  	  uint64_t store;
	  	  std::string storeName;
  	  	  std::vector<std::string> dataItemKeys;
  	  	  uint32_t sizeOfDataItemKeysVector;
  	  	  uint32_t currentIndex;
  	  	  bool hasData;
  	  	  HBaseDBLayer *hBaseDBLayerPtr;

  	  	  HBaseDBLayerIterator();
	  	  ~HBaseDBLayerIterator();
	  	  bool getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError);
  };

  /// Class that implements the DBLayer for HBase
  class HBaseDBLayer : public DBLayer
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
	string mainTableName;
	string lockRowName;
	uint32_t currentTTLValue;
	CURL *curlForCreateHBaseTable;
	CURL *curlForDeleteHBaseTable;
	CURL *curlForCreateOrUpdateHBaseColumn;
	CURL *curlForReadHBaseCellValue;
	CURL *curlForDeleteHBase_Column_CF_Row;
	CURL *curlForGetNumberOfColumnsInHBaseTableRow;
	CURL *curlForGetAllColumnsInHBaseTableRow;
	CURL *curlForHBaseTableExistenceCheck;
	CURL *curlForRunDataStoreCommand;
	struct curl_slist *headersForCreateHBaseTable;
	struct curl_slist *headersForDeleteHBaseTable;
	struct curl_slist *headersForCreateOrUpdateHBaseColumn;
	struct curl_slist *headersForReadHBaseCellValue;
	struct curl_slist *headersForDeleteHBase_Column_CF_Row;
	struct curl_slist *headersForGetNumberOfColumnsInHBaseTableRow;
	struct curl_slist *headersForGetAllColumnsInHBaseTableRow;
	struct curl_slist *headersForHBaseTableExistenceCheck;
	struct curl_slist *headersForRunDataStoreCommand;

	bool curlGlobalCleanupNeeded;
	// If you change this array's size, you must make the corresponding adjustments in a few places in the .cpp file.
	string hBaseBaseUrlPool[50];
	uint32_t currentHBaseUrlIdx;
	string httpVerbUsedInPreviousRunCommand;
	string base64_chars;
    int32_t curlBufferOffset;
    char curlBuffer[10*1024*1024]; // 10 MB in size.
    const char *putBuffer;

    /// Constructor
    HBaseDBLayer();

    /// Destructor
    ~HBaseDBLayer();

	// HBase specific cURL write and read functions.
    // Since we are using C++ and the cURL library is C based, we have to set the callback as a
    // static C++ method and configure cURL to pass our custom C++ object pointer in the 4th argument of
    // the callback function. Once we get the C++ object pointer, we can access our other non-static
    // member functions and non-static member variables via that object pointer.
	static size_t writeFunction(char *data, size_t size, size_t nmemb, void *objPtr);
	size_t writeFunctionImpl(char *data, size_t size, size_t nmemb);
	// Do the same for the read function.
	static size_t readFunction(char *data, size_t size, size_t nmemb, void *objPtr);
	size_t readFunctionImpl(char *data, size_t size, size_t nmemb);
	bool createHBaseTable(string const & url, string const & schema, int32_t & curlReturnCode,
		  string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString);
	bool deleteHBaseTable(string const & url, int32_t & curlReturnCode,
		  string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString);
	bool createOrUpdateHBaseColumn(string const & url, string const & jsonDoc, int32_t & curlReturnCode,
	  	string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString);
	bool createHBaseColumn(string const & url, string const & jsonDoc, int32_t & curlReturnCode,
		string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString);
	bool deleteHBase_Column_CF_Row(string const & url, int32_t & curlReturnCode, string & curlErrorString,
		uint64_t & httpResponseCode, string & httpReasonString);
	bool readHBaseCellValue(string const & url, string & value, bool const & base64DecodeTheResult,
		int32_t & curlReturnCode, string & curlErrorString,
		uint64_t & httpResponseCode, string & httpReasonString);
	bool getTotalNumberOfColumnsInHBaseTableRow(string const & url, uint32_t & totalColumns,
		int32_t & curlReturnCode, string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString);
	bool getAllColumnKeysInHBaseTableRow(string const & url, std::vector<std::string> & dataItemKeys,
		int32_t & curlReturnCode, string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString);
	bool checkIfHBaseTableExists(string const & tableName);
	string getNextHBaseBaseUrl();

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

    HBaseDBLayerIterator * newIterator(uint64_t store, PersistenceError & dbError);
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
#endif /* HBASE_DB_LAYER_H_ */
