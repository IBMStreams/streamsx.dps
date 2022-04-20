/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2022
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef DISTRIBUTED_PROCESS_STORE_WRAPPERS_H_
#define DISTRIBUTED_PROCESS_STORE_WRAPPERS_H_

#include "DistributedProcessStore.h"

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{

  /// Set the path of the DPS configuration file
  /// @param dpsConfigFile the path string
  /// @return indication if the operation succeeded
  inline SPL::boolean dpsSetConfigFile(SPL::rstring const & dpsConfigFile)
  {
    DistributedProcessStore::setConfigFile(dpsConfigFile);
    return true;	// TODO do something more sensible here ?
  }

  /// Is the connection to the back-end data store active?
  /// @return true if connection is active or false if connection is inactive.
  inline SPL::boolean dpsIsConnected()
  {
     return DistributedProcessStore::getGlobalStore().isConnected();
  }

  /// Reestablish the connection to the back-end data store if needed.
  /// @return true if connection is active or false if connection is inactive.
  inline SPL::boolean dpsReconnect() {
     return DistributedProcessStore::getGlobalStore().reconnect();
  }

  // Allows you to initialize the connection to DPS without throwing an exception on failure.
  // The purpose of this is to allow the DPS connection to be attempted during
  // operator initialization, and not prevent startup from completing on failure.
  // @return true if we successfully established a connection or false if not.
  inline SPL::boolean initializeDpsNoException()
  {
      SPL::boolean result = false;
      try
      {
     	 result = DistributedProcessStore::getGlobalStore().isConnected();
      }
      catch (...)
      {
      	SPLAPPTRC(L_ERROR, "Failed to initialize DPS Connection.", "initializeDpsNoException");
      }

      return result;
  }

  /// Create a distributed process store
  /// @param name of the store
  /// @param key a dummy key to indicate the type of this store's key
  /// @param value a dummy value to indicate the type of this store's value
  /// @return store handle, or 0 if a store with the same name exists
  /// @param err GlobalStore error code
  template<class T1, class T2>
  SPL::uint64 dpsCreateStore(SPL::rstring const & name, T1 const & key, T2 const & value, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().createStore(name, key, value, err);
  }

  /// Create a distributed process store or get it if it already exists
  /// @param name of the store
  /// @param key a dummy key to indicate the type of this store's key
  /// @param value a dummy value to indicate the type of this store's value
  /// @return store handle
  /// @param err GlobalStore error code
  template<class T1, class T2>
  SPL::uint64 dpsCreateOrGetStore(SPL::rstring const & name, T1 const & key, T2 const & value, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().createOrGetStore(name, key, value, err);
  }
        
  /// Find a distributed process store
  /// @param name of the store
  /// @return store handle, or 0 if a store with the given name does
  /// not exist
  /// @param err GlobalStore error code
  inline SPL::uint64 dpsFindStore(SPL::rstring const & name, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().findStore(name, err);
  }            
        
  /// Remove a distributed process store
  /// @param handle store handle
  /// @param err GlobalStore error code
  inline SPL::boolean dpsRemoveStore(SPL::uint64 store, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().removeStore(store, err);
  }

  /// Put an item into the given store (A better performing version with no safety checks)
  /// @param store store handle
  /// @param key item's key
  /// @param value item's value
  /// @return true if there was no item with the same key in the store,
  /// false otherwise
  /// @param err GlobalStore error code
  template<class T1, class T2>
  SPL::boolean dpsPut(SPL::uint64 store, T1 const & key, T2 const & value, SPL::uint64 & err) 
  {
    return DistributedProcessStore::getGlobalStore().put(store, key, value, err);            
  }

  /// Put an item into the given store (A version with safety checks that will have performance overhead)
  /// @param store store handle
  /// @param key item's key
  /// @param value item's value
  /// @return true if there was no item with the same key in the store,
  /// false otherwise
  /// @param err GlobalStore error code
  template<class T1, class T2>
  SPL::boolean dpsPutSafe(SPL::uint64 store, T1 const & key, T2 const & value, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().putSafe(store, key, value, err);
  }

  /// Put an item with a TTL (Time To Live in seconds) value into the global area of the back-end data store.
  /// @param key item's key
  /// @param value item's value
  /// @param ttl data item's Time To Live in seconds
  /// @return true if the data item was stored successfully, false otherwise
  /// @param err GlobalStore error code
  /// @param encodeKey item's key should be encoded or not before storing in the back-end data store.
  /// @param encodeValue item's value is encoded or not in the back-end data store.
  template<class T1, class T2>
  SPL::boolean dpsPutTTL(T1 const & key, T2 const & value, SPL::uint32 const & ttl, SPL::uint64 & err, SPL::boolean encodeKey=true, SPL::boolean encodeValue=true)
  {
    uint32_t storedKeySize = 0;
    uint32_t storedValueSize = 0;
    return DistributedProcessStore::getGlobalStore().putTTL(key, value, ttl, err, storedKeySize, storedValueSize, encodeKey, encodeValue);
  }

  /// Put an item with a TTL (Time To Live in seconds) value into the global area of the back-end data store.
  /// @param key item's key
  /// @param value item's value
  /// @param ttl data item's Time To Live in seconds
  /// @return true if the data item was stored successfully, false otherwise
  /// @param err GlobalStore error code
  /// @param storedKeySize On a successful put. this will have the actual storage size of the key in the back-end data store.
  /// @param storedValueSize On a successful put. this will have the actual storage size of the value in the back-end data store.
  /// @param encodeKey item's key should be encoded or not before storing in the back-end data store.
  /// @param encodeValue item's value is encoded or not in the back-end data store.
  template<class T1, class T2>
  SPL::boolean dpsPutTTL(T1 const & key, T2 const & value, SPL::uint32 const & ttl, SPL::uint64 & err, SPL::uint32 & storedKeySize, SPL::uint32 & storedValueSize, SPL::boolean encodeKey=true, SPL::boolean encodeValue=true)
  {
    return DistributedProcessStore::getGlobalStore().putTTL(key, value, ttl, err, storedKeySize, storedValueSize, encodeKey, encodeValue);
  }

  /// Get an item from the given store (A better performing version with no safety checks)
  /// @param store store handle
  /// @param key item's key
  /// @param value item's value
  /// @return true if there was an item with the given key and a matching
  /// type for its value, false otherwise 
  /// @param err GlobalStore error code
  template<class T1, class T2>
  SPL::boolean dpsGet(SPL::uint64 store, T1 const & key, T2 & value, SPL::uint64 & err) 
  {
    return DistributedProcessStore::getGlobalStore().get(store, key, value, err);            
  }

  /// Get an item from the given store (A version with safety checks that will have performance overhead)
  /// @param store store handle
  /// @param key item's key
  /// @param value item's value
  /// @return true if there was an item with the given key and a matching
  /// type for its value, false otherwise
  /// @param err GlobalStore error code
  template<class T1, class T2>
  SPL::boolean dpsGetSafe(SPL::uint64 store, T1 const & key, T2 & value, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().getSafe(store, key, value, err);
  }

  /// Get an item with a TTL (Time To Live in seconds) value into the global area of the back-end data store.
  /// @param key item's key
  /// @param value item's value
  /// @return true if there was a TTL based item with the given key and a matching
  /// type for its value, false otherwise
  /// @param err GlobalStore error code
  /// @param encodeKey item's key should be encoded or not before getting it from the back-end data store.
  /// @param encodeValue item's value is encoded or not in the back-end data store.
  template<class T1, class T2>
  SPL::boolean dpsGetTTL(T1 const & key, T2 & value, SPL::uint64 & err, SPL::boolean encodeKey=true, SPL::boolean encodeValue=true)
  {
    return DistributedProcessStore::getGlobalStore().getTTL(key, value, err, encodeKey, encodeValue);
  }

  /// Remove an item from the given store
  /// @param store store handle
  /// @param key item's key
  /// @return true if there was an item with the given key, false
  /// otherwise 
  /// @param err GlobalStore error code
  template<class T1>
  SPL::boolean dpsRemove(SPL::uint64 store, T1 const & key, SPL::uint64 & err) 
  {
    return DistributedProcessStore::getGlobalStore().remove(store, key, err);    
  }

  /// Remove an item with a TTL (Time To Live in seconds) value from the global area of the back-end data store.
  /// @param key item's key
  /// @return true if we removed a TTL based item with the given key, false otherwise
  /// @param err GlobalStore error code
  /// @param encodeKey item's key should be encoded or not before removing from the back-end data store.
  template<class T1>
  SPL::boolean dpsRemoveTTL(T1 const & key, SPL::uint64 & err, SPL::boolean encodeKey=true)
  {
    return DistributedProcessStore::getGlobalStore().removeTTL(key, err, encodeKey);
  }

  /// Check if an item is in the given store
  /// @param store store handle
  /// @param key item's key
  /// @return true if there is an item with the given key, false otherwise
  /// @param err GlobalStore error code
  template<class T1>
  SPL::boolean dpsHas(SPL::uint64 store, T1 const & key, SPL::uint64 & err) 
  {
    return DistributedProcessStore::getGlobalStore().has(store, key, err);    
  }

  /// Check if an item with a TTL (Time To Live in seconds) value exists in the global area of the back-end data store.
  /// @param key item's key
  /// @return true if there is a TTL based item with the given key, false otherwise
  /// @param err GlobalStore error code
  /// @param encodeKey item's key should be encoded or not before checking for existence in the back-end data store.
  template<class T1>
  SPL::boolean dpsHasTTL(T1 const & key, SPL::uint64 & err, SPL::boolean encodeKey=true)
  {
    return DistributedProcessStore::getGlobalStore().hasTTL(key, err, encodeKey);
  }

  /// Clear the given store 
  /// @param store store handle
  /// @param err GlobalStore error code
  inline void dpsClear(SPL::uint64 store, SPL::uint64 & err) 
  {
    return DistributedProcessStore::getGlobalStore().clear(store, err);    
  }

  /// Get the size of the given store
  /// @param store store handle
  /// @return the size of the store
  /// @param err GlobalStore error code
  inline SPL::uint64 dpsSize(SPL::uint64 store, SPL::uint64 & err) 
  {
    return DistributedProcessStore::getGlobalStore().size(store, err);    
  }

  /// flush unpersisted data in the back end database to disk
  /// @return true on success, false on failure
  /// @param err GlobalStore error code
  inline void  dpsPersist(SPL::uint64 & err)
  {
   DistributedProcessStore::getGlobalStore().persist(err);
  }
  /// Begin the iteration on the given store. No other operations that can
  /// modify the state can be used until after a matching endIteration()
  /// call.                                                             
  /// @param store store handle 
  /// @return the iterator 
  /// @param err store error code 
  inline SPL::uint64 dpsBeginIteration(SPL::uint64 store, SPL::uint64 & err)
  {   
    return DistributedProcessStore::getGlobalStore().beginIteration(store, err); 
  }                                                  
  
  /// Get the next key and value of given types in the given store 
  /// @param store store handle
  /// @param iterator the iterator 
  /// @param key the key of the current item  
  /// @param value the value of the current item 
  /// @return true if an item was found, false otherwise. 
  /// @param err store error code
  template<class T1, class T2>                 
  SPL::boolean dpsGetNext(SPL::uint64 store, SPL::uint64 iterator, T1 & key, T2 & value, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().getNext(store, iterator, key, value, err);
  }                                                                  
  
  /// End the iteration on the given store
  /// @param store store handle
  /// @param iterator the iterator  
  /// @param err store error code  
  inline void dpsEndIteration(SPL::uint64 store, SPL::uint64 iterator, SPL::uint64 & err) 
  {       
    return DistributedProcessStore::getGlobalStore().endIteration(store, iterator, err);  
  } 

  /// Get multiple keys present in a given store.
  /// @param store The handle of the store.
  /// @param keys User provided mutable list variable. This list must be suitable for storing multiple keys found in a given store and it must be made of a given store's key data type.
  /// @param keyStartPosition User can indicate a start position from where keys should be fetched and returned. It must be greater than or equal to zero. If not, this API will return back with an empty list of keys.
  /// @param numberOfKeysNeeded User can indicate the total number of keys to be returned as available from the given key start position. It must be greater than or equal to 0 and less than or equal to 50000. If it is set to 0, then all the available keys upto a maximum of 50000 keys from the given key start position will be returned.
  /// @param keyExpression User can provide an expression made of the attributes from the key's data type. This expression will be evaluated in determining which matching keys to be returned. [This feature is not implemented at this time.]
  /// @param valueExpression User can provide an expression made of the attributes from the value's data type. This expression will be evaluated in determining which matching keys to be returned. [This feature is not implemented at this time.]
  /// @param err Contains the error code. Will be '0' if no error occurs, and a non-zero value otherwise. 
  ///
  template<class T1>
  void dpsGetKeys(SPL::uint64 store, SPL::list<T1> & keys, SPL::int32 const & keyStartPosition, SPL::int32 const & numberOfKeysNeeded, SPL::rstring const & keyExpression, SPL::rstring const & valueExpression, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().getKeys(store, keys, keyStartPosition, numberOfKeysNeeded, keyExpression, valueExpression, err);
  }           

  /// This function can be called to get values for a given list of multiple keys present in a given store.
  /// @param store The handle of the store.
  /// @param keys User provided list variable that contains keys for which values need to be fetched.
  /// @param values User provided mutabe list variable in which the fetched values will be returned. This list must be suitable for storing multiple values obtained from a given store and it must be made of a given store's value data type. Fetched values will be available in this list at the same index where the key appeared in the user provided keys list. Before using the individual values from this list, it is recommended to make sure that a given value fetch worked with no errors.
  /// @param errors User provided mutable list variable in which the individual success or failure value fetch result codes will be returned. This list must be of type uint64. Each list element will be 0 if no error occurs and a non-zero error code otherwise. Such value fetch result codes will be available in this list at the same index where the key appears in the user provided keys list. If a given result code doesn't indicate a successful value fetch, it is better to skip the corresponding element at the same index in the mutable values list.
  /// 
  template<class T1, class T2>
  bool dpsGetValues(SPL::uint64 store, SPL::list<T1> const & keys, SPL::list<T2> & values, SPL::list<SPL::uint64> & errors)
  {
    return DistributedProcessStore::getGlobalStore().getValues(store, keys, values, errors);
  }           

  /// This function can be called to get multiple Key/Value (KV) pairs present in a given store.
  /// @param store The handle of the store.
  /// @param keys User provided mutable list variable in which the keys found in the store will be returned. This list must be suitable for storing multiple keys found in a given store and it must be made of a given store's key data type.
  /// @param values User provided mutabe list variable in which the fetched values will be returned. This list must be suitable for storing multiple values obtained from a given store and it must be made of a given store's value data type. Fetched values will be available in this list at the same index where the corresponding key appears in the keys list. Before using the individual values from this list, it is recommended to make sure that a given value fetch worked with no errors.
  /// @param keyStartPosition User can indicate a start position from where the K/V pairs should be fetched and returned. It must be greater than or equal to zero. If not, this API will return back with an empty list of keys.
  /// @param numberOfPairsNeeded User can indicate the total number of K/V pairs to be returned as available from the given key start position. It must be greater than or equal to 0 and less than or equal to 50000. If it is set to 0, then all the available K/V pairs upto a maximum of 50000 pairs from the given key start position will be returned.
  /// @param errors User provided mutable list variable in which the individual success or failure value fetch result codes will be returned. This list must be of type uint64. Each list element will be 0 if no error occurs and a non-zero error code otherwise. Such value fetch result codes will be available in this list at the same index where the key appears in the keys list. If a given result code doesn't indicate a successful value fetch, it is better to skip the corresponding element at the same index in the mutable values list.
  /// @return It returns true if value fetch worked for all the keys with no errors. Else, it returns false to indicate that value fetch encountered error for one or more keys.
  ///
  template<class T1, class T2>
  bool dpsGetKVPairs(SPL::uint64 store, SPL::list<T1> & keys, SPL::list<T2> & values, SPL::int32 const & keyStartPosition, SPL::int32 const & numberOfPairsNeeded, SPL::list<SPL::uint64> & errors)
  {
    return DistributedProcessStore::getGlobalStore().getKVPairs(store, keys, values, keyStartPosition, numberOfPairsNeeded, errors);
  }           

  /// Serialize the items from the serialized store
  /// @param store store handle
  /// @param data blob to serialize into
  /// @param err store error code
  template<class T1, class T2>
  void dpsSerialize(SPL::uint64 store, SPL::blob & data, T1 const & dummyKey, T2 const & dummyValue, SPL::uint64 & err)
  {
    DistributedProcessStore::getGlobalStore().serialize<T1, T2>(store, data, err);
  }

  /// Deserialize the items from the serialized store
  /// @param store store handle
  /// @param data blob to deserialize from
  /// @param err store error code
  template<class T1, class T2>
  void dpsDeserialize(SPL::uint64 store, SPL::blob const & data, T1 const & dummyKey, T2 const & dummyValue, SPL::uint64 & err)
  {
    DistributedProcessStore::getGlobalStore().deserialize<T1, T2>(store, data, err);
  }

  /// Get the last store error string
  /// @return the last store error string
  inline SPL::rstring dpsGetLastStoreErrorString()
  {
    return DistributedProcessStore::getGlobalStore().getLastPersistenceErrorString();
  }

  /// Get the last error string occurred during the TTL based operation
  /// @return the last error string for a TTL based operation.
  inline SPL::rstring dpsGetLastErrorStringTTL()
  {
    return DistributedProcessStore::getGlobalStore().getLastPersistenceErrorStringTTL();
  }

  /// Get the last store error code
  /// @return the last store error code
  inline SPL::uint64 dpsGetLastStoreErrorCode()
  {
    return DistributedProcessStore::getGlobalStore().getLastPersistenceErrorCode();
  }

  /// Get the last error code occurred during the TTL based operation
  /// @return the last error code for a TTL based operation.
  inline SPL::uint64 dpsGetLastErrorCodeTTL()
  {
    return DistributedProcessStore::getGlobalStore().getLastPersistenceErrorCodeTTL();
  }

  /// Get the store name for a given store id.
  /// @param store store handle
  /// @return the store name
  inline SPL::rstring dpsGetStoreName(SPL::uint64 store)
  {
    return DistributedProcessStore::getGlobalStore().getStoreName(store);
  }

  /// Get the key SPL type name for a given store id.
  /// @param store store handle
  /// @return the SPL type name for the given store's key
  inline SPL::rstring dpsGetSplTypeNameForKey(SPL::uint64 store)
  {
    return DistributedProcessStore::getGlobalStore().getSplTypeNameForKey(store);
  }

  /// Get the value SPL type name for a given store id.
  /// @param store store handle
  /// @return the SPL type name for the given store's value
  inline SPL::rstring dpsGetSplTypeNameForValue(SPL::uint64 store)
  {
    return DistributedProcessStore::getGlobalStore().getSplTypeNameForValue(store);
  }

  /// Get the name of the NoSQL DB product being used.
  /// @return the name of the DB product being used.
  inline SPL::rstring dpsGetNoSqlDbProductName(void)
  {
    return DistributedProcessStore::getGlobalStore().getNoSqlDbProductName();
  }

  /// Get the name of the machine and its CPU architecture where this operator is running.
  /// @param Machine name will be assigned to this reference.
  /// @param CPU architecture will be assigned to this reference.
  /// @return none
  inline void dpsGetDetailsAboutThisMachine(SPL::rstring & machineName, SPL::rstring & osVersion, SPL::rstring & cpuArchitecture) {
	  return DistributedProcessStore::getGlobalStore().getDetailsAboutThisMachine(machineName, osVersion, cpuArchitecture);
  }

  /// If users want to execute simple arbitrary back-end data store (fire and forget)
  /// native commands, this API can be used. This covers any Redis or Cassandra(CQL)
  /// native commands that don't have to fetch and return K/V pairs or return size of the db etc.
  /// (Insert and Delete are the more suitable ones here. However, key and value can only have string types.)
  /// User must ensure that his/her command string is syntactically correct according to the
  /// rules of the back-end data store you configured. DPS logic will not do the syntax checking.
  /// @param cmd A command string that is supported by the chosen back-end data store.
  /// @param err error code
  inline SPL::boolean dpsRunDataStoreCommand(SPL::rstring const & cmd, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().runDataStoreCommand(cmd, err);
  }

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
  /// @param jsonRequest should be a well formatted reques
  /// @param JSON response from the Cloudant server.
  /// @param err Return code from Curl or HTTP response.
  /// @return true if there is no error, false otherwise.
  inline SPL::boolean dpsRunDataStoreCommand(SPL::uint32 const & cmdType, SPL::rstring const & httpVerb,
		  SPL::rstring const & baseUrl, SPL::rstring const & apiEndpoint, SPL::rstring const & queryParams,
		  SPL::rstring const & jsonRequest, SPL::rstring & jsonResponse, SPL::uint64 & err)
  {
	  return DistributedProcessStore::getGlobalStore().runDataStoreCommand(cmdType, httpVerb, baseUrl,
	     apiEndpoint, queryParams, jsonRequest, jsonResponse, err);
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
  inline SPL::boolean dpsRunDataStoreCommand(SPL::list<SPL::rstring> const & cmdList, SPL::rstring & resultValue, SPL::uint64 & err)
  {
     return DistributedProcessStore::getGlobalStore().runDataStoreCommand(cmdList, resultValue, err);
  }

  /// Base64 encode a given string. Encoded result will be returned in a
  /// user provided modifiable string passed as a second function argument.
  /// @param str should contain the string to be base64 encoded.
  /// @param encodedResultStr will be filled with the base64 encoded result.
  inline void dpsBase64Encode(SPL::rstring const & str, SPL::rstring & encodedResultStr)
  {
	  DistributedProcessStore::getGlobalStore().base64Encode(str, encodedResultStr);
  }

  /// Base64 decode a given string. Decoded result will be returned in a
  /// user provided modifiable string passed as a second function argument.
  /// @param str should contain the string to be base64 decoded.
  /// @param decodedResultStr will be filled with the base64 decoded result.
  inline void dpsBase64Decode(SPL::rstring const & str, SPL::rstring & decodedResultStr)
  {
	  DistributedProcessStore::getGlobalStore().base64Decode(str, decodedResultStr);
  }

  /// Create a distributed process store for Java primitive operators.
  /// @param name of the store
  /// @param key a dummy key to indicate the type of this store's key
  /// @param value a dummy value to indicate the type of this store's value
  /// @return store handle, or 0 if a store with the same name exists
  /// @param err GlobalStore error code
  inline SPL::uint64 dpsCreateStoreForJava(SPL::rstring const & name, SPL::rstring const & key, SPL::rstring const & value, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().createStoreForJava(name, key, value, err);
  }

  /// Create a distributed process store for Java primitive operators or get it if it already exists
  /// @param name of the store
  /// @param key a dummy key to indicate the type of this store's key
  /// @param value a dummy value to indicate the type of this store's value
  /// @return store handle
  /// @param err GlobalStore error code
  inline SPL::uint64 dpsCreateOrGetStoreForJava(SPL::rstring const & name, SPL::rstring const & key, SPL::rstring const & value, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().createOrGetStoreForJava(name, key, value, err);
  }

  /// Put an item into the given store for Java primitive operators (faster version with no safety checks).
  /// @param store store handle
  /// @param key item's key
  /// @param keySize item's key size
  /// @param value item's value
  /// @param valueSize item's value size
  /// @return true if item was stored in the store,
  /// false otherwise
  /// @param err GlobalStore error code
  inline SPL::boolean dpsPutForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, unsigned char const *value, SPL::uint32 valueSize, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().putForJava(store, key, keySize, value, valueSize, err);
  }

  /// Put an item into the given store for Java primitive operators (slower version with many safety checks).
  /// @param store store handle
  /// @param key item's key
  /// @param keySize item's key size
  /// @param value item's value
  /// @param valueSize item's value size
  /// @return true if item was stored in the store,
  /// false otherwise
  /// @param err GlobalStore error code
  inline SPL::boolean dpsPutSafeForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, unsigned char const *value, SPL::uint32 valueSize, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().putSafeForJava(store, key, keySize, value, valueSize, err);
  }

  /// Put an item with a TTL (Time To Live in seconds) value into the global area of the back-end data store.
  /// @param key item's key
  /// @param keySize item's key size
  /// @param value item's value
  /// @param valueSize item's value size
  /// @param ttl data item's Time To Live in seconds
  /// @return true if the data item was stored successfully, false otherwise
  /// @param err GlobalStore error code
  /// @param encodeKey item's key should be encoded or not before storing in the back-end data store.
  /// @param encodeValue item's value is encoded or not in the back-end data store.
  inline SPL::boolean dpsPutTTLForJava(char const *key, SPL::uint32 keySize, unsigned char const *value, SPL::uint32 valueSize, SPL::uint32 const & ttl, SPL::uint64 & err, SPL::boolean encodeKey=true, SPL::boolean encodeValue=true)
  {
    return DistributedProcessStore::getGlobalStore().putTTLForJava(key, keySize, value, valueSize, ttl, err, encodeKey, encodeValue);
  }

  /// Get an item from the given store for Java primitive operators (faster version with no safety checks).
  /// @param store store handle
  /// @param key item's key
  /// @param keySize item's key size
  /// @param value item's pointer to receive the stored value
  /// @param valueSize item's value size
  /// @return true if there was an item with the given key and a matching type for its value
  /// false otherwise
  /// @param err GlobalStore error code
  inline SPL::boolean dpsGetForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, unsigned char * & value, SPL::uint32 & valueSize, SPL::uint64 & err)
  {
     return DistributedProcessStore::getGlobalStore().getForJava(store, key, keySize, value, valueSize, err);
  }

  /// Get an item from the given store for Java primitive operators (slower version with many safety checks).
  /// @param store store handle
  /// @param key item's key
  /// @param keySize item's key size
  /// @param value item's pointer to receive the stored value
  /// @param valueSize item's value size
  /// @return true if there was an item with the given key and a matching type for its value
  /// false otherwise
  /// @param err GlobalStore error code
  inline SPL::boolean dpsGetSafeForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, unsigned char * & value, SPL::uint32 & valueSize, SPL::uint64 & err)
  {
     return DistributedProcessStore::getGlobalStore().getSafeForJava(store, key, keySize, value, valueSize, err);
  }

  /// Get an item with a TTL (Time To Live in seconds) value into the global area of the back-end data store.
  /// @param key item's key
  /// @param keySize item's key size
  /// @param value item's value
  /// @param valueSize item's value size
  /// @return true if there was a TTL based item with the given key and a matching
  /// type for its value, false otherwise
  /// @param err GlobalStore error code
  /// @param encodeKey item's key should be encoded or note before getting it from the back-end data store.
  /// @param encodeValue item's value is encoded or not in the back-end data store.
  inline SPL::boolean dpsGetTTLForJava(char const *key, SPL::uint32 keySize, unsigned char * & value, SPL::uint32 & valueSize, SPL::uint64 & err, SPL::boolean encodeKey=true, SPL::boolean encodeValue=true)
  {
     return DistributedProcessStore::getGlobalStore().getTTLForJava(key, keySize, value, valueSize, err, encodeKey, encodeValue);
  }

  /// Remove an item from the given store for Java primitive operators.
  /// @param store store handle
  /// @param key item's key
  /// @param keySize item's key size
  /// @return true if there was an item with the given key, false
  /// otherwise
  /// @param err GlobalStore error code
  inline SPL::boolean dpsRemoveForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, SPL::uint64 & err)
  {
	 return DistributedProcessStore::getGlobalStore().removeForJava(store, key, keySize, err);
  }

  /// Remove an item with a TTL (Time To Live in seconds) value from the global area of the back-end data store.
  /// @param key item's key
  /// @param keySize item's key size
  /// @return true if we removed a TTL based item with the given key, false otherwise
  /// @param err GlobalStore error code
  /// @param encodeKey item's key should be encoded or not before removing from the back-end data store.
  inline SPL::boolean dpsRemoveTTLForJava(char const *key, SPL::uint32 keySize, SPL::uint64 & err, SPL::boolean encodeKey=true)
  {
	 return DistributedProcessStore::getGlobalStore().removeTTLForJava(key, keySize, err, encodeKey);
  }

  /// Check if an item is in the given store for Java primitive operators.
  /// @param store store handle
  /// @param key item's key
  /// @param keySize item's key size
  /// @return true if there is an item with the given key, false otherwise
  /// @param err GlobalStore error code
  inline SPL::boolean dpsHasForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().hasForJava(store, key, keySize, err);
  }

  /// Check if an item with a TTL (Time To Live in seconds) value exists in the global area of the back-end data store.
  /// @param key item's key
  /// @param keySize item's key size
  /// @return true if there is a TTL based item with the given key, false otherwise
  /// @param err GlobalStore error code
  /// @param encodeKey item's key should be encoded or not before checking for existence in the back-end data store.
  inline SPL::boolean dpsHasTTLForJava(char const *key, SPL::uint32 keySize, SPL::uint64 & err, SPL::boolean encodeKey=true)
  {
    return DistributedProcessStore::getGlobalStore().hasTTLForJava(key, keySize, err, encodeKey);
  }

  /// Get the next key and value of given types in the given store for Java primitive operators.
  /// @param store store handle
  /// @param iterator the iterator
  /// @param key item's pointer to receive the key
  /// @param keySize item's key size
  /// @param value item's pointer to receive the stored value
  /// @param valueSize item's value size
  /// @return true if an item was found, false otherwise.
  /// @param err store error code
  inline SPL::boolean dpsGetNextForJava(SPL::uint64 store, SPL::uint64 iterator, unsigned char * &  key, SPL::uint32 & keySize,
	unsigned char * & value, SPL::uint32 & valueSize, SPL::uint64 & err)
  {
    return DistributedProcessStore::getGlobalStore().getNextForJava(store, iterator, key, keySize, value, valueSize, err);
  }

  // Run the Redis command as given by the Java primitive operators.
  // @param serializedListOfRString list<rstring> cmdList in serialized form
  // @param resultString Any result value obtained from Redis after executing the given command.
  // @param Error code if any while executing the given Redis command.
  // @return true if the Redis command was executed successfully. Otherwise, it returns false.
  inline SPL::boolean dpsRunDataStoreCommandForJava(unsigned char *serializedListOfRString, SPL::uint32 cmdListSize, SPL::rstring & resultString, SPL::uint64 & err) {
    return DistributedProcessStore::getGlobalStore().runDataStoreCommandForJava(serializedListOfRString, cmdListSize, resultString, err);
  } 

} } } } }

#endif /* DISTRIBUTED_PROCESS_STORE_WRAPPERS_H_ */
