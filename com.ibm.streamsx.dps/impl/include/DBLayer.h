/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef DB_LAYER_H_
#define DB_LAYER_H_

#include <tr1/memory>
#include <tr1/functional>
#include <set>

#include "PersistenceError.h"
#include <SPL/Runtime/Common/RuntimeDebug.h>

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
  class DBLayer
  {
  public:
	  std::string nameOfThisMachine;
	  std::string osVersionOfThisMachine;
	  std::string cpuTypeOfThisMachine;

    /// Destructor
    ///
    virtual ~DBLayer() {}

    /// Connect to the database
    /// @param dbName the database name
    /// @param userName the user name
    /// @param password the user password
    /// @param dbError error from the database
    virtual void connectToDatabase(std::set<std::string> const & dbServers,
                                   PersistenceError & dbError) = 0;

    /// Create a process store
    /// @param name of the store
    /// @return store id, or 0 if a store with the same name exists 
    /// @param dbError any error from the database
    virtual uint64_t createStore(std::string const & name,
            					 std::string const & keySplTypeName,
            					 std::string const & valueSplTypeName,
                                 PersistenceError & dbError) = 0;
            
    /// Create a process store or get it if it already exists
    /// @param name of the store
    /// @return store id or 0 if a store with the same name but
    /// different cached setting or different types already exists
    /// @param dbError error from the database
    virtual uint64_t createOrGetStore(std::string const & name, 
			 	 	 	 	 	 	  std::string const & keySplTypeName,
			 	 	 	 	 	 	  std::string const & valueSplTypeName,
                                      PersistenceError & dbError) = 0;

    /// Find a process store 
    /// @param name of the store
    /// @return store id, or 0 if a store with the given name and
    /// properties does not exist
    /// @param dbError error from the database
    virtual uint64_t findStore(std::string const & name, 
                               PersistenceError & dbError) = 0;

    /// Remove a process store 
    /// @param store store id
    /// @param dbError error from the database
    virtual bool removeStore(uint64_t store, PersistenceError & dbError) = 0;
          

    /// Put an item into the given store (A better performing version with no safety checks)
    /// @param store store id
    /// @param keyData item's key data
    /// @param keySize item's key size
    /// @param valueData item's value data
    /// @param valueSize item's value size
    /// @return true if there was no item with the same key in the store,
    /// false otherwise
    virtual bool put(uint64_t store, char const * keyData, uint32_t keySize,
                     unsigned char const * valueData, uint32_t valueSize, 
                     PersistenceError & dbError) = 0;

    /// Put an item into the given store (A version with safety checks that will have performance overhead)
    /// @param store store id
    /// @param keyData item's key data
    /// @param keySize item's key size
    /// @param valueData item's value data
    /// @param valueSize item's value size
    /// @return true if there was no item with the same key in the store,
    /// false otherwise
    virtual bool putSafe(uint64_t store, char const * keyData, uint32_t keySize,
                     unsigned char const * valueData, uint32_t valueSize,
                     PersistenceError & dbError) = 0;

    /// Put an item with a TTL (Time To Live in seconds) value into the global area of the back-end data store.
    /// @param keyData item's key data
    /// @param keySize item's key size
    /// @param valueData item's value data
    /// @param valueSize item's value size
    /// @param ttl data item expiry time in seconds
    /// @return true if the put operation is successful, false otherwise
    virtual bool putTTL(char const * keyData, uint32_t keySize,
                     unsigned char const * valueData, uint32_t valueSize, uint32_t ttl,
                     PersistenceError & dbError) = 0;

    /// Get an item from the given store (A better performing version with no safety checks)
    /// @param store store id
    /// @param keyData item's key data 
    /// @param keySize item's key size
    /// @param valueData item's value data (ownership handed off to caller)
    /// @param valueSize item's value size 
    /// @return true if there was an item with the given key and a matching
    /// type for its value, false otherwise 
    /// @param dbError error from the database
    virtual bool get(uint64_t store, char const * keyData, uint32_t keySize,
                     unsigned char * & valueData, uint32_t & valueSize, 
                     PersistenceError & dbError) = 0;

    /// Get an item from the given store (A version with safety checks that will have performance overhead)
    /// @param store store id
    /// @param keyData item's key data
    /// @param keySize item's key size
    /// @param valueData item's value data (ownership handed off to caller)
    /// @param valueSize item's value size
    /// @return true if there was an item with the given key and a matching
    /// type for its value, false otherwise
    /// @param dbError error from the database
    virtual bool getSafe(uint64_t store, char const * keyData, uint32_t keySize,
                     unsigned char * & valueData, uint32_t & valueSize,
                     PersistenceError & dbError) = 0;

    /// Get an item with a TTL (Time To Live in seconds) value from the global area of the back-end data store.
    /// @param keyData item's key data
    /// @param keySize item's key size
    /// @param valueData item's value data (ownership handed off to caller)
    /// @param valueSize item's value size
    /// @return true if there was a TTL based data item with the given key and a matching
    /// type for its value, false otherwise
    /// @param dbError error from the database
    virtual bool getTTL(char const * keyData, uint32_t keySize,
                     unsigned char * & valueData, uint32_t & valueSize,
                     PersistenceError & dbError) = 0;

    /// Remove an item from the given store
    /// @param store store id
    /// @param key item's key
    /// @return true if there was an item with the given key, false
    /// otherwise 
    /// @param dbError error from the database
    virtual bool remove(uint64_t store, char const * keyData, uint32_t keySize,
                        PersistenceError & dbError) = 0;

    /// Remove an item with a TTL (Time To Live in seconds) value from the global area of the back-end data store.
    /// @param key item's key
    /// @return true if there was an item with the given key, false
    /// otherwise
    /// @param dbError error from the database
    virtual bool removeTTL(char const * keyData, uint32_t keySize,
                        PersistenceError & dbError) = 0;

    /// Check if an item is in the given store
    /// @param store store id
    /// @param key item's key
    /// @return true if there is an item with the given key, false otherwise
    /// @param dbError error from the database
    virtual bool has(uint64_t store, char const * keyData, uint32_t keySize,
                     PersistenceError & dbError) = 0;

    /// Check if an item with a TTL (Time To Live in seconds) value exists in the global area of the back-end data store.
    /// @param store store id
    /// @param key item's key
    /// @return true if there is an item with the given key, false otherwise
    /// @param dbError error from the database
    virtual bool hasTTL(char const * keyData, uint32_t keySize,
                     PersistenceError & dbError) = 0;

    /// Clear the given store 
    /// @param store store id
    /// @param dbError error from the database
    virtual void clear(uint64_t store, PersistenceError & dbError) = 0;

    /// Get the size of the given store
    /// @param store store id
    /// @return the size of the store
    /// @param dbError error from the database
    virtual uint64_t size(uint64_t store, PersistenceError & dbError) = 0;                       

    /// Get the store name for a given store id
    /// @param store store id
    /// @param dbError error from the database
	virtual std::string getStoreName(uint64_t store, PersistenceError & dbError) = 0;

    /// Get the SPL type name for a given store's key
    /// @param store store id
    /// @param dbError error from the database
	virtual std::string getSplTypeNameForKey(uint64_t store, PersistenceError & dbError) = 0;

    /// Get the SPL type name for a given store's value
    /// @param store store id
    /// @param dbError error from the database
	virtual std::string getSplTypeNameForValue(uint64_t store, PersistenceError & dbError) = 0;

    /// Get the name of the NoSQL DB product being used.
    /// @return the name of the DB product being used.
    virtual std::string getNoSqlDbProductName(void) = 0;

    /// Get the name of the machine and its CPU architecture where this operator is running.
    /// @param Machine name will be assigned to this reference.
    /// @param CPU architecture will be assigned to this reference.
    /// @return none
  	virtual void getDetailsAboutThisMachine(std::string & machineName, std::string & osVersion, std::string & cpuArchitecture) = 0;

	/// If users want to execute simple arbitrary back-end data store (fire and forget)
	/// native commands, this API can be used. This covers any Redis or Cassandra(CQL)
	/// native commands that don't have to fetch and return K/V pairs or return size of the db etc.
	/// (Insert and Delete are the more suitable ones here. However, key and value can only have string types.)
	/// User must ensure that his/her command string is syntactically correct according to the
	/// rules of the back-end data store you configured. DPS logic will not do the syntax checking.
	/// @param cmd A command string that is supported by the chosen back-end data store.
	/// @param err error code
	virtual bool runDataStoreCommand(std::string const & cmd, PersistenceError & dbError) = 0;

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
	virtual bool runDataStoreCommand(uint32_t const & cmdType, std::string const & httpVerb,
            	std::string const & baseUrl, std::string const & apiEndpoint, std::string const & queryParams,
            	std::string const & jsonRequest, std::string & jsonResponse, PersistenceError & dbError) = 0;

    /// Base64 encode a given string. Encoded result will be returned in a
    /// user provided modifiable string passed as a second function argument.
    /// @param str should contain the string to be base64 encoded.
    /// @param encodedResultStr will be filled with the base64 encoded result.
    virtual void base64_encode(std::string const & str, std::string & encodedResultStr) = 0;

    /// Base64 decode a given string. Decoded result will be returned in a
    /// user provided modifiable string passed as a second function argument.
    /// @param str should contain the string to be base64 decoded.
    /// @param decodedResultStr will be filled with the base64 decoded result.
    virtual void base64_decode(std::string & str, std::string & decodedResultStr) = 0;


    /// A store iterator
    class Iterator
    {
    public:
      /// Destructor
      virtual ~Iterator() {}
      
      /// Get the next key-value pair from a given store
      /// @param store store id
      /// @param keyData item's key data (ownership handed off to caller)
      /// @param keySize item's key size
      /// @param valueData item's value data (ownership handed off to caller)
      /// @param valueSize item's value size 
      /// @return true if there was an item, false if the iteration has ended
      /// @param dbError error from the database      
      virtual bool getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
                   unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError) = 0;
    };

    /// Create a new iterator on the store and return it
    /// @param store store id
    /// @return the iteartor
    /// @param dbError error from the database
    virtual Iterator * newIterator(uint64_t store, PersistenceError & dbError) = 0;

    /// Delete an existing iterator on the store
    /// @param store store id
    /// @param iter iterarator to be deleted
    /// @return the iteartor
    /// @param dbError error from the database
    virtual void deleteIterator(uint64_t store, Iterator * iter, PersistenceError & dbError) = 0;

    /// Create a lock or get it if it already exists
    /// @param name of the lock
    /// @return lock handle
    /// @param lkError lock error
    virtual uint64_t createOrGetLock(std::string const & name, PersistenceError & lkError) = 0;

    /// Release the lock
    /// @return lock lock handle
    /// @param lkError lock error
    virtual void releaseLock(uint64_t lock, PersistenceError & lkError) = 0;

    /// Try to acquire the lock
    /// @param lock lock handle
    /// @param leaseTime lease time (in seconds)
    /// @param maxWaitTimeToAcquireLock (Max time the caller is willing to wait to acquire a lock in seconds)
    /// @return true if the lock was acquired, false otherwise
    /// @param lkError lock error
    virtual bool acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError) = 0;

    /// Remove the lock
    /// @param lock lock handle
    /// @return true if the lock was there, false otherwise
    /// @param lkError lock error
    virtual bool removeLock(uint64_t lock, PersistenceError & lkError) = 0;

    /// Get the process id for a given distributed lock name
    /// @param name of the lock
    /// @return Process id that currently owns the lock
    /// @param lkError lock error
    virtual uint32_t getPidForLock(std::string const & name, PersistenceError & lkError) = 0;

    /// persist changes to disk, that may have resided in memory only so far
    /// @param dbError error from the database
    virtual void persist(PersistenceError & dbError)
    {
    	SPLAPPTRC(L_DEBUG, "Called persist() on DBlayer '" << getNoSqlDbProductName() << "' that does not support this function. Returning success." , "DBLayer");
    }

  };

} } } } 
#endif /* DB_LAYER_H_ */
