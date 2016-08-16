/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef DISTRIBUTED_PROCESS_STORE_H_
#define DISTRIBUTED_PROCESS_STORE_H_

#include "PersistenceError.h"
#include "DBLayer.h"

#include <SPL/Runtime/Type/SPLType.h>
#include <SPL/Runtime/Utility/Mutex.h>
#include <SPL/Runtime/Serialization/NativeByteBuffer.h>

#include <memory>

using namespace SPL;
using namespace std;

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed 
{
  class DistributedProcessStore
  {
  public:
	static std::string dpsConfigFile_;

    /// Destructor
    ///
    ~DistributedProcessStore();

    /// Get the DB layer
    /// @return the DB layer
    DBLayer & getDBLayer();

    /// Connect to the database. This function will read db name, user,
    /// and password from the data directory and connect to the
    /// database. If we are already connected, it will do nothing.  It
    /// will throw and log if we are not successfully. We will only
    /// connect when needed, so this function gets called from almost
    /// every other member function in this class. 
    void connectToDatabase();

    /// Create a process store
    /// @param name of the store
    /// @param dummy key
    /// @param dummy value
    /// @return store id, or 0 if a store with the same name exists
    /// @param err store error code
    template<class T1, class T2>
    SPL::uint64 createStore(SPL::rstring const & name, T1 const & key, T2 const & value, SPL::uint64 & err);

    /// Create a process store or get it if it already exists
    /// @param name of the store
    /// @param dummy key
    /// @param dummy value
    /// @return store id
    /// @param err store error code
    template<class T1, class T2>
    SPL::uint64 createOrGetStore(SPL::rstring const & name, T1 const & key, T2 const & value, SPL::uint64 & err);

    /// Find a process store 
    /// @param name of the store
    /// @return store id, or 0 if a store with the given name does
    /// not exist
    /// @param err store error code
    SPL::uint64 findStore(SPL::rstring const & name, SPL::uint64 & err);
        
    /// Remove a process store 
    /// @param store store id            
    /// @param err store error code
    /// @return true if the store was there, false otherwise
    SPL::boolean removeStore(SPL::uint64 store, SPL::uint64 & err);

    /// Put an item into the given store (A better performing version with no safety checks)
    /// @param store store id
    /// @param key item's key
    /// @param value item's value
    /// @return true if there was no item with the same key in the store,
    /// false otherwise
    /// @param err store error code
    template<class T1, class T2>
    SPL::boolean put(SPL::uint64 store, T1 const & key, T2 const & value, SPL::uint64 & err);

    /// Put an item into the given store (A version with safety checks that will have performance overhead)
    /// @param store store id
    /// @param key item's key
    /// @param value item's value
    /// @return true if there was no item with the same key in the store,
    /// false otherwise
    /// @param err store error code
    template<class T1, class T2>
    SPL::boolean putSafe(SPL::uint64 store, T1 const & key, T2 const & value, SPL::uint64 & err);

    /// Put an item with a TTL (Time To Live in seconds) value into the global area of the back-end data store.
    /// @param key item's key
    /// @param value item's value
    /// @param ttl data item expiry time in seconds
    /// @return true if the put operation is successful, false otherwise
    /// @param err store error code
    template<class T1, class T2>
    SPL::boolean putTTL(T1 const & key, T2 const & value, SPL::uint32 const & ttl, SPL::uint64 & err);

    /// Get an item from the given store (A better performing version with no safety checks)
    /// @param store store id
    /// @param key item's key
    /// @param value item's value
    /// @return true if there was an item with the given key and a matching
    /// type for its value, false otherwise 
    /// @param err store error code
    template<class T1, class T2>
    SPL::boolean get(SPL::uint64 store, T1 const & key, T2 & value, SPL::uint64 & err);            

    /// Get an item from the given store (A version with safety checks that will have performance overhead)
    /// @param store store id
    /// @param key item's key
    /// @param value item's value
    /// @return true if there was an item with the given key and a matching
    /// type for its value, false otherwise
    /// @param err store error code
    template<class T1, class T2>
    SPL::boolean getSafe(SPL::uint64 store, T1 const & key, T2 & value, SPL::uint64 & err);

    /// Get an item with a TTL (Time To Live in seconds) value from the global area of the back-end data store.
    /// @param key item's key
    /// @param value item's value
    /// @return true if there was a TTL based item with the given key and a matching
    /// type for its value, false otherwise
    /// @param err store error code
    template<class T1, class T2>
    SPL::boolean getTTL(T1 const & key, T2 & value, SPL::uint64 & err);

    /// Remove an item from the given store
    /// @param store store id
    /// @param key item's key
    /// @return true if there was an item with the given key, false
    /// otherwise 
    /// @param err store error code
    template<class T1>
    SPL::boolean remove(SPL::uint64 store, T1 const & key, SPL::uint64 & err);

    /// Remove an item with a TTL (Time To Live in seconds) value from the global area of the back-end data store.
    /// @param key item's key
    /// @return true if we removed a TTL based item with the given key, false otherwise
    /// @param err store error code
    template<class T1>
    SPL::boolean removeTTL(T1 const & key, SPL::uint64 & err);

    /// Check if an item is in the given store
    /// @param store store id
    /// @param key item's key
    /// @return true if there is an item with the given key, false otherwise
    /// @param err store error code
    template<class T1>
    SPL::boolean has(SPL::uint64 store, T1 const & key, SPL::uint64 & err);

    /// Check if an item with a TTL (Time To Live in seconds) value exists in the global area of the back-end data store.
    /// @param key item's key
    /// @return true if there is a TTL based item with the given key, false otherwise
    /// @param err store error code
    template<class T1>
    SPL::boolean hasTTL(T1 const & key, SPL::uint64 & err);

    /// Clear the given store 
    /// @param store store id
    /// @param err store error code
    void clear(SPL::uint64 store, SPL::uint64 & err);

    /// Get the size of the given store
    /// @param store store id
    /// @return the size of the store
    /// @param err store error code
    SPL::uint64 size(SPL::uint64 store, SPL::uint64 & err);

    /// Begin the iteration on the given store. No other operations that can
    /// modify the state can be used until after a matching endIteration()
    /// call.
    /// @param store store id
    /// @return the iterator
    /// @param err store error code
    SPL::uint64 beginIteration(SPL::uint64 store, SPL::uint64 & err);
            
    /// Get the next key and value of given types in the given store
    /// @param store store id
    /// @param iterator the iterator
    /// @param key the key of the current item
    /// @param value the value of the current item
    /// @return true if an item was found, false otherwise.
    /// @param err PersistentStore error code
    template<class T1, class T2>           
    SPL::boolean getNext(SPL::uint64 store, SPL::uint64 iterator, T1 & key, T2 & value, SPL::uint64 & err);
    
    /// End the iteration on the given store
    /// @param store store id
    /// @param iterator the iterator
    /// @param err PersistentStore error code
    void endIteration(SPL::uint64 store, SPL::uint64 iterator, SPL::uint64 & err);

    /// Serialize the items from the serialized store
    /// @param store store handle
    /// @param data blob to serialize into
    /// @param err store error code
    template<class T1, class T2>
    void serialize(SPL::uint64 store, SPL::blob & data, SPL::uint64 & err);

    /// Deserialize the items from the serialized store
    /// @param store store handle
    /// @param data blob to deserialize from
    /// @param err store error code
    template<class T1, class T2>
    void deserialize(SPL::uint64 store, SPL::blob const & data, SPL::uint64 & err);

    /// Get the last store error string
    /// @return the last store error string
    SPL::rstring getLastPersistenceErrorString() const;

    /// Get the last error string occurred during the TTL based operation
    /// @return the last error string for a TTL based operation.
    SPL::rstring getLastPersistenceErrorStringTTL() const;

    /// Get the last store error code
    /// @return the last store error code
    SPL::uint64 getLastPersistenceErrorCode() const;

    /// Get the last store error code
    /// @return the last store error code for a TTL based operation.
    SPL::uint64 getLastPersistenceErrorCodeTTL() const;

    /// Create a distributed process store for Java primitive operators.
    /// @param name of the store
    /// @param key a dummy key to indicate the type of this store's key
    /// @param value a dummy value to indicate the type of this store's value
    /// @return store handle, or 0 if a store with the same name exists
    /// @param err GlobalStore error code
    SPL::uint64 createStoreForJava(SPL::rstring const & name, SPL::rstring const & key, SPL::rstring const & value, SPL::uint64 & err);

    /// Create a distributed process store for Java primitive operators or get it if it already exists
    /// @param name of the store
    /// @param key a dummy key to indicate the type of this store's key
    /// @param value a dummy value to indicate the type of this store's value
    /// @return store handle
    /// @param err GlobalStore error code
    SPL::uint64 createOrGetStoreForJava(SPL::rstring const & name, SPL::rstring const & key, SPL::rstring const & value, SPL::uint64 & err);

    /// Put an item into the given store for Java primitive operators (faster version with no safety checks).
    /// @param store store handle
    /// @param key item's key
    /// @param keySize item's key size
    /// @param value item's value
    /// @param valueSize item's value size
    /// @return true if item was stored in the store,
    /// false otherwise
    /// @param err GlobalStore error code
    SPL::boolean putForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, unsigned char const *value, SPL::uint32 valueSize, SPL::uint64 & err);

    /// Put an item into the given store for Java primitive operators (slower version with many safety checks).
    /// @param store store handle
    /// @param key item's key
    /// @param keySize item's key size
    /// @param value item's value
    /// @param valueSize item's value size
    /// @return true if item was stored in the store,
    /// false otherwise
    /// @param err GlobalStore error code
    SPL::boolean putSafeForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, unsigned char const *value, SPL::uint32 valueSize, SPL::uint64 & err);

    /// Put an item with a TTL (Time To Live in seconds) value into the global area of the back-end data store.
    /// @param key item's key
    /// @param keySize item's key size
    /// @param value item's value
    /// @param valueSize item's value size
    /// @param ttl data item's Time To Live in seconds
    /// @return true if the data item was stored successfully, false otherwise
    /// @param err GlobalStore error code
    SPL::boolean putTTLForJava(char const *key, SPL::uint32 keySize, unsigned char const *value, SPL::uint32 valueSize, SPL::uint32 const & ttl, SPL::uint64 & err);

    /// Get an item from the given store for Java primitive operators (faster version with no safety checks).
    /// @param store store handle
    /// @param key item's key
    /// @param keySize item's key size
    /// @param value item's pointer to receive the stored value
    /// @param valueSize item's value size
    /// @return true if there was an item with the given key and a matching type for its value
    /// false otherwise
    /// @param err GlobalStore error code
    SPL::boolean getForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, unsigned char * & value, SPL::uint32 & valueSize, SPL::uint64 & err);

    /// Get an item from the given store for Java primitive operators (slower version with many safety checks).
    /// @param store store handle
    /// @param key item's key
    /// @param keySize item's key size
    /// @param value item's pointer to receive the stored value
    /// @param valueSize item's value size
    /// @return true if there was an item with the given key and a matching type for its value
    /// false otherwise
    /// @param err GlobalStore error code
    SPL::boolean getSafeForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, unsigned char * & value, SPL::uint32 & valueSize, SPL::uint64 & err);

    /// Get an item with a TTL (Time To Live in seconds) value into the global area of the back-end data store.
    /// @param key item's key
    /// @param keySize item's key size
    /// @param value item's value
    /// @param valueSize item's value size
    /// @return true if there was a TTL based item with the given key and a matching
    /// type for its value, false otherwise
    /// @param err GlobalStore error code
    SPL::boolean getTTLForJava(char const *key, SPL::uint32 keySize, unsigned char * & value, SPL::uint32 & valueSize, SPL::uint64 & err);

    /// Remove an item from the given store for Java primitive operators.
    /// @param store store id
    /// @param key item's key
    /// @param keySize item's key size
    /// @return true if there was an item with the given key, false
    /// otherwise
    /// @param err store error code
    SPL::boolean removeForJava(SPL::uint64 store, char const *key,  SPL::uint32 keySize, SPL::uint64 & err);

    /// Remove an item with a TTL (Time To Live in seconds) value from the global area of the back-end data store.
    /// @param key item's key
    /// @param keySize item's key size
    /// @return true if we removed a TTL based item with the given key, false otherwise
    /// @param err GlobalStore error code
    SPL::boolean removeTTLForJava(char const *key,  SPL::uint32 keySize, SPL::uint64 & err);

    /// Check if an item is in the given store for Java primitive operators.
    /// @param store store handle
    /// @param key item's key
    /// @param keySize item's key size
    /// @return true if there is an item with the given key, false otherwise
    /// @param err GlobalStore error code
    SPL::boolean hasForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, SPL::uint64 & err);

    /// Check if an item with a TTL (Time To Live in seconds) value exists in the global area of the back-end data store.
    /// @param key item's key
    /// @param keySize item's key size
    /// @return true if there is a TTL based item with the given key, false otherwise
    /// @param err GlobalStore error code
    SPL::boolean hasTTLForJava(char const *key, SPL::uint32 keySize, SPL::uint64 & err);

    /// Get the next key and value of given types in the given store for Java primitive operators.
    /// @param store store handle
    /// @param iterator the iterator
    /// @param key item's pointer to receive the key
    /// @param keySize item's key size
    /// @param value item's pointer to receive the stored value
    /// @param valueSize item's value size
    /// @return true if an item was found, false otherwise.
    /// @param err store error code
    SPL::boolean getNextForJava(SPL::uint64 store, SPL::uint64 iterator, unsigned char * &  key, SPL::uint32 & keySize,
    	unsigned char * & value, SPL::uint32 & valueSize, SPL::uint64 & err);

    /// Create a lock or get it if it already exists
    /// @param name of the lock
    /// @return lock handle
    /// @param err PersistentStore error code
    SPL::uint64 createOrGetLock(SPL::rstring const & name, SPL::uint64 & err);

    /// Remove the lock
    /// @param lock lock handle
    /// @return true if the lock was there, false otherwise
    /// @param err PersistentStore error code
    SPL::boolean removeLock(SPL::uint64 lock, SPL::uint64 & err);

    /// Acquire the lock
    /// @return lock lock handle
    /// @param err PersistentStore error code
    void acquireLock(SPL::uint64 lock, SPL::uint64 & err);

    /// Acquire the lock
    /// @param leaseTime lease time (in seconds)
    /// @return lock lock handle
    /// @param err PersistentStore error code
    void acquireLock(SPL::uint64 lock, SPL::float64 leaseTime, SPL::float64  maxWaitTimeToAcquireLock, SPL::uint64 & err);

    /// Release the lock
    /// @return lock lock handle
    /// @param err PersistentStore error code
    void releaseLock(SPL::uint64 lock, SPL::uint64 & err);

    /// Get the process id that currently owns the lock.
    /// @param name of the lock
    /// @return process id
    /// @param err PersistentStore error code
    SPL::uint32 getPidForLock(SPL::rstring const & name, SPL::uint64 & err);

    /// Get the last lock error string
    /// @return the last lock error string
    SPL::rstring getLastLockErrorString() const;

    /// Get the last lock error code
    /// @return the last lock error code
    SPL::uint64 getLastLockErrorCode() const;

    /// Get the store name for a given store id.
    /// @param store store handle
    /// @return the store name
    SPL::rstring getStoreName(SPL::uint64 store);

    /// Get the SPL literal type name for a given key or value
    /// @return the SPL type name
    SPL::rstring getSPLTypeName(ConstValueHandle const & handle);

    /// Get the key SPL type name for a given store id.
    /// @param store store handle
    /// @return the SPL type name for the given store's key
    SPL::rstring getSplTypeNameForKey(SPL::uint64 store);

    /// Get the value SPL type name for a given store id.
    /// @param store store handle
    /// @return the SPL type name for the given store's value
    SPL::rstring getSplTypeNameForValue(SPL::uint64 store);

    /// Get the name of the NoSQL DB product being used.
    /// @return the name of the DB product being used.
    SPL::rstring getNoSqlDbProductName(void);

    /// Get the name of the machine and its CPU architecture where this operator is running.
    /// @param Machine name will be assigned to this reference.
    /// @param CPU architecture will be assigned to this reference.
    /// @return none
  	void getDetailsAboutThisMachine(SPL::rstring & machineName, SPL::rstring & osVersion, SPL::rstring & cpuArchitecture);

    /// If users want to execute simple arbitrary back-end data store (fire and forget)
    /// native commands, this API can be used. This covers any Redis or Cassandra(CQL)
    /// native commands that don't have to fetch and return K/V pairs or return size of the db etc.
    /// (Insert and Delete are the more suitable ones here. However, key and value can only have string types.)
    /// User must ensure that his/her command string is syntactically correct according to the
    /// rules of the back-end data store you configured. DPS logic will not do the syntax checking.
    /// @param cmd A command string that is supported by the chosen back-end data store.
    /// @param err error code
    SPL::boolean runDataStoreCommand(SPL::rstring const & cmd, SPL::uint64 & err);

    /// If users want to execute arbitrary back-end data store two way
    /// native commands, this API can be used. This is a variation of the previous API with
    /// overloaded function arguments. As of Nov/2014, this API is supported in the dps toolkit only
    /// when Cloudant NoSQL DB is used as a back-end data store. It covers any Cloudant HTTP/JSON based
    /// native commands that can perform both database and document related Cloudant APIs that are very
    /// well documented for reference on the web.
    /// @param cmdType 1 means DB related command, 2 means document related command
    /// @param httpVerb such as GET, PUT, POST, DELETE, COPY and HEAD
    /// @param baseUrl points to the cloudant base URL
    /// @param apiEndpoint points to the cloudant URL paths such as /db or /db/doc etc.
    /// @param queryParams is a sequence of name=value pairs as required by the cloudant commands.
    /// @param jsonRequest should be a well formatted request string.
    /// @param JSON response from the Cloudant server.
    /// @param err Return code from Curl or HTTP response.
    /// @return true if there is no error, false otherwise.
    SPL::boolean runDataStoreCommand(SPL::uint32 const & cmdType, SPL::rstring const & httpVerb,
            	SPL::rstring const & baseUrl, SPL::rstring const & apiEndpoint, SPL::rstring const & queryParams,
            	SPL::rstring const & jsonRequest, SPL::rstring & jsonResponse, SPL::uint64 & err);

    /// Base64 encode a given string. Encoded result will be returned in a
    /// user provided modifiable string passed as a second function argument.
    /// @param str should contain the string to be base64 encoded.
    /// @param encodedResultStr will be filled with the base64 encoded result.
    void base64Encode(SPL::rstring const & str, SPL::rstring & encodedResultStr);

    /// Base64 decode a given string. Decoded result will be returned in a
    /// user provided modifiable string passed as a second function argument.
    /// @param str should contain the string to be base64 decoded.
    /// @param decodedResultStr will be filled with the base64 decoded result.
    void base64Decode(SPL::rstring const & str, SPL::rstring & decodedResultStr);

    /// Get the singleton instance of the store store
    /// @return the global instance of the store store
    static DistributedProcessStore & getGlobalStore(); 

    /// Set the dpsConfigFile
    /// @param dpsConfigFile contains a relative path to the config file. The path is relative to the etc/dps-config directory.
    static void setConfigFile(std::string dpsConfigFile) {
    	DistributedProcessStore::dpsConfigFile_ = dpsConfigFile;
    }

  private:
    DistributedProcessStore(); // no one should do this other than us
    std::auto_ptr<DBLayer> db_;
    std::auto_ptr<PersistenceError> dbError_;
    std::auto_ptr<PersistenceError> lkError_;
  };


  /// Create a process store
  /// @param name of the store
  /// @param dummy key
  /// @param dummy value
  /// @return store id, or 0 if a store with the same name exists
  /// @param err store error code
  template<class T1, class T2>
  SPL::uint64 DistributedProcessStore::createStore(SPL::rstring const & name, T1 const & key, T2 const & value, SPL::uint64 & err)
  {
    dbError_->reset();
    // We are going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
    string keySplTypeName = getSPLTypeName(key);
    string valueSplTypeName = getSPLTypeName(value);
    SPL::uint64 res = db_->createStore(name, keySplTypeName, valueSplTypeName, *dbError_);
    err = dbError_->getErrorCode();
    return res;
  }

  /// Create a process store or get it if it already exists
  /// @param name of the store
  /// @param dummy key
  /// @param dummy value
  /// @return store id
  /// @param err store error code
  template<class T1, class T2>
  SPL::uint64 DistributedProcessStore::createOrGetStore(SPL::rstring const & name, T1 const & key, T2 const & value, SPL::uint64 & err)
  {
    dbError_->reset();
    // We are going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
    string keySplTypeName = getSPLTypeName(key);
    string valueSplTypeName = getSPLTypeName(value);
    SPL::uint64 res = db_->createOrGetStore(name, keySplTypeName, valueSplTypeName, *dbError_);
    err = dbError_->getErrorCode();
    return res;
  }

  // (A better performing version with no safety checks)
  template<class T1, class T2>
  SPL::boolean DistributedProcessStore::put(SPL::uint64 store, T1 const & key, T2 const & value, SPL::uint64 & err)
  {
    dbError_->reset();
    SPL::NativeByteBuffer key_nbf;
    key_nbf << key;
    char const * keyData = (char const *)key_nbf.getPtr();
    uint32_t keySize = key_nbf.getSerializedDataSize();
    SPL::NativeByteBuffer value_nbf;
    value_nbf << value;
    unsigned char * valueData = value_nbf.getPtr();
    uint32_t valueSize = value_nbf.getSerializedDataSize();
    bool res = db_->put(store, keyData, keySize, valueData, valueSize, *dbError_);
    err = dbError_->getErrorCode();
    return res;
  }

// (A version with safety checks that will have performance overhead)
  template<class T1, class T2>
  SPL::boolean DistributedProcessStore::putSafe(SPL::uint64 store, T1 const & key, T2 const & value, SPL::uint64 & err)
  {
    dbError_->reset();
    SPL::NativeByteBuffer key_nbf;
    key_nbf << key;
    char const * keyData = (char const *)key_nbf.getPtr();
    uint32_t keySize = key_nbf.getSerializedDataSize();
    SPL::NativeByteBuffer value_nbf;
    value_nbf << value;
    unsigned char * valueData = value_nbf.getPtr();
    uint32_t valueSize = value_nbf.getSerializedDataSize();
    bool res = db_->putSafe(store, keyData, keySize, valueData, valueSize, *dbError_);
    err = dbError_->getErrorCode();
    return res;
  }

  // Put data item with a TTL (Time To Live in seconds) value into the global area of the back-end data store.
  template<class T1, class T2>
  SPL::boolean DistributedProcessStore::putTTL(T1 const & key, T2 const & value, SPL::uint32 const & ttl, SPL::uint64 & err)
  {
    dbError_->resetTTL();
    SPL::NativeByteBuffer key_nbf;
    key_nbf << key;
    char const * keyData = (char const *)key_nbf.getPtr();
    uint32_t keySize = key_nbf.getSerializedDataSize();
    SPL::NativeByteBuffer value_nbf;
    value_nbf << value;
    unsigned char * valueData = value_nbf.getPtr();
    uint32_t valueSize = value_nbf.getSerializedDataSize();
    bool res = db_->putTTL(keyData, keySize, valueData, valueSize, ttl, *dbError_);
    err = dbError_->getErrorCodeTTL();
    return res;
  }

  // (A better performing version with no safety checks)
  template<class T1, class T2>
  SPL::boolean DistributedProcessStore::get(SPL::uint64 store, T1 const & key, T2 & value, SPL::uint64 & err)        
  {
    dbError_->reset();
    SPL::NativeByteBuffer key_nbf;
    key_nbf << key;
    char const * keyData = (char const *)key_nbf.getPtr();
    uint32_t keySize = key_nbf.getSerializedDataSize();
    unsigned char * valueData;
    uint32_t valueSize = 0; 
    bool res = db_->get(store, keyData, keySize, valueData, valueSize, *dbError_);
    err = dbError_->getErrorCode();
    if(err==0 && res) {
      SPL::NativeByteBuffer value_nbf(valueData, valueSize);
      value_nbf >> value;
    } 
    if(valueSize>0)
      delete [] valueData;
    return res;           
  }

  template<class T1, class T2>
  SPL::boolean DistributedProcessStore::getSafe(SPL::uint64 store, T1 const & key, T2 & value, SPL::uint64 & err)
  {
    dbError_->reset();
    SPL::NativeByteBuffer key_nbf;
    key_nbf << key;
    char const * keyData = (char const *)key_nbf.getPtr();
    uint32_t keySize = key_nbf.getSerializedDataSize();
    unsigned char * valueData;
    uint32_t valueSize = 0;
    bool res = db_->getSafe(store, keyData, keySize, valueData, valueSize, *dbError_);
    err = dbError_->getErrorCode();
    if(err==0 && res) {
      SPL::NativeByteBuffer value_nbf(valueData, valueSize);
      value_nbf >> value;
    }
    if(valueSize>0)
      delete [] valueData;
    return res;
  }

  // Get a TTL based data item that is stored in the global area of the back-end data store.
  template<class T1, class T2>
  SPL::boolean DistributedProcessStore::getTTL(T1 const & key, T2 & value, SPL::uint64 & err)
  {
    dbError_->resetTTL();
    SPL::NativeByteBuffer key_nbf;
    key_nbf << key;
    char const * keyData = (char const *)key_nbf.getPtr();
    uint32_t keySize = key_nbf.getSerializedDataSize();
    unsigned char * valueData;
    uint32_t valueSize = 0;
    bool res = db_->getTTL(keyData, keySize, valueData, valueSize, *dbError_);
    err = dbError_->getErrorCodeTTL();
    if(err==0 && res) {
      SPL::NativeByteBuffer value_nbf(valueData, valueSize);
      value_nbf >> value;
    }
    if(valueSize>0)
      delete [] valueData;
    return res;
  }

  template<class T1>
  SPL::boolean DistributedProcessStore::remove(SPL::uint64 store, T1 const & key, SPL::uint64 & err)
  {
    dbError_->reset();
    SPL::NativeByteBuffer key_nbf;
    key_nbf << key;
    char const * keyData = (char const *)key_nbf.getPtr();
    uint32_t keySize = key_nbf.getSerializedDataSize();
    bool res = db_->remove(store, keyData, keySize, *dbError_);
    err = dbError_->getErrorCode();            
    return res;
  }

  // Remove a TTL based data item that is stored in the global area of the back-end data store.
  template<class T1>
  SPL::boolean DistributedProcessStore::removeTTL(T1 const & key, SPL::uint64 & err)
  {
    dbError_->resetTTL();
    SPL::NativeByteBuffer key_nbf;
    key_nbf << key;
    char const * keyData = (char const *)key_nbf.getPtr();
    uint32_t keySize = key_nbf.getSerializedDataSize();
    bool res = db_->removeTTL(keyData, keySize, *dbError_);
    err = dbError_->getErrorCodeTTL();
    return res;
  }

  template<class T1>
  SPL::boolean DistributedProcessStore::has(SPL::uint64 store, T1 const & key, SPL::uint64 & err)
  {
    dbError_->reset();
    SPL::NativeByteBuffer key_nbf;
    key_nbf << key;
    char const * keyData = (char const *)key_nbf.getPtr();
    uint32_t keySize = key_nbf.getSerializedDataSize();
    bool res = db_->has(store, keyData, keySize, *dbError_);
    err = dbError_->getErrorCode();            
    return res;
  }

  // Check for the existence of a TTL based data item that is stored in the global area of the back-end data store.
  template<class T1>
  SPL::boolean DistributedProcessStore::hasTTL(T1 const & key, SPL::uint64 & err)
  {
    dbError_->resetTTL();
    SPL::NativeByteBuffer key_nbf;
    key_nbf << key;
    char const * keyData = (char const *)key_nbf.getPtr();
    uint32_t keySize = key_nbf.getSerializedDataSize();
    bool res = db_->hasTTL(keyData, keySize, *dbError_);
    err = dbError_->getErrorCodeTTL();
    return res;
  }

  template<class T1, class T2>           
    SPL::boolean DistributedProcessStore::getNext(SPL::uint64 store, SPL::uint64 iterator, T1 & key, T2 & value, SPL::uint64 & err)
    {
      dbError_->reset();
      DBLayer::Iterator * iter = reinterpret_cast<DBLayer::Iterator *>(iterator);
      unsigned char * keyData;
      uint32_t keySize = 0;
      unsigned char * valueData;
      uint32_t valueSize = 0;
      bool res = iter->getNext(store, keyData, keySize, valueData, valueSize, *dbError_);
      err = dbError_->getErrorCode();
      if(err==0 && res) {
    	SPL::NativeByteBuffer nbf_key(keyData, keySize);
    	nbf_key >> key;
        SPL::NativeByteBuffer nbf_value(valueData, valueSize);
        nbf_value >> value;
      }
      if(keySize>0)
        delete [] keyData;
      if(valueSize>0)
        delete [] valueData;
      return res;
    }

  template<class T1, class T2>
  void DistributedProcessStore::serialize(SPL::uint64 store, SPL::blob & data, SPL::uint64 & err)
  {
    dbError_->reset();
    SPL::uint64 oerr;

    SPL::NativeByteBuffer nbf;

    T1 key;
    T2 value;
    SPL::uint64 iter = beginIteration(store, err);
    if (err!=0)
      return;
    while (getNext(store, iter, key, value, err)) {
      if (err!=0) {
        endIteration(store, iter, oerr);
        return;
      }
      nbf << key;
      nbf << value;
    }
    endIteration(store, iter, err);
    nbf.setAutoDealloc(false);
    data.adoptData(nbf.getPtr(), nbf.getSerializedDataSize());
  }

  template<class T1, class T2>
  void DistributedProcessStore::deserialize(SPL::uint64 store, SPL::blob const & data, SPL::uint64 & err)
  {
    dbError_->reset();

    uint64_t size;
    unsigned char const * rdata = data.getData(size);
    SPL::NativeByteBuffer nbf(const_cast<unsigned char*>(rdata), size);

    T1 key;
    T2 value;
    while(nbf.getNRemainingBytes()>0) {
      nbf >> key;
      nbf >> value;
      put(store, key, value, err);
      if (err!=0)
        return;
    }
  }

} } } } }
#endif /* DISTRIBUTED_PROCESS_STORE_H_ */
