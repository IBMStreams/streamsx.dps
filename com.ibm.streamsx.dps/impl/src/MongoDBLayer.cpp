/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2022
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
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

/*
==================================================================================================================
This CPP file contains the source code for the distributed process store (dps) back-end activities such as
insert, update, read, remove, etc. This dps implementation runs on top of the MongoDB NoSQL DB.

At this time (Dec/2014), MongoDB is a very popular NoSQL document data store. It ranks among the top
for use cases with schema-less documents to be stored in a NoSQL fashion.
As far as the client APIs, MongoDB has support various popular programming languages C, C++, Java, Python,
JavaScript etc. In the DPS toolkit, we will mostly use the C APIs available for working with MongoDB.

Any dpsXXXXX native function call made from a SPL composite will go through a serialization layer and then hit
this CPP file. Here, the purpose of such SPL native function calls will be fulfilled using Mongo APIs.
After that, the results will be sent to a deserialization layer. From there, results will be transformed using the
correct SPL types and delivered back to the SPL composite. In general, our distributed process store provides
a distributed no-sql cache for different processes (multiple PEs from one or more Streams applications).
We provide a set of free for all native function APIs to create/read/update/delete data items on one or more stores.
In the worst case, there could be multiple writers and multiple readers for the same store.
It is important to note that a Streams application designer/developer should carefully address how different parts
of his/her application will access the store simultaneously i.e. who puts what, who gets what and at
what frequency from where etc.

This C++ project has a companion SPL project (058_distributed_in_memory_store_for_non_fused_operators).
Please refer to the README.txt file in that SPL project directory for learning about the procedure to do an
end-to-end test run involving the SPL code, serialization/deserialization code,
MongoDB interface code (this file), and your MongoDB infrastructure.

As a first step, you should run the ./mk script from the C++ project directory (DistributedProcessStoreLib).
That will take care of building the .so file for the dps and copying it to the SPL project's impl/lib directory.
==================================================================================================================
*/

#include "MongoDBLayer.h"
#include "DpsConstants.h"

#include <SPL/Runtime/Common/RuntimeDebug.h>
#include <SPL/Runtime/Type/SPLType.h>
#include <SPL/Runtime/Function/TimeFunctions.h>
#include <SPL/Runtime/Function/UtilFunctions.h>
#include <SPL/Runtime/Function/MathFunctions.h>

#include <iostream>
#include <unistd.h>
#include <sys/utsname.h>
#include <sstream>
#include <cassert>
#include <stdio.h>
#include <time.h>
#include <string>
#include <vector>
#include <streams_boost/lexical_cast.hpp>
#include <streams_boost/algorithm/string/replace.hpp>
#include <streams_boost/algorithm/string/erase.hpp>
#include <streams_boost/algorithm/string/split.hpp>
#include <streams_boost/algorithm/string/classification.hpp>
#include <streams_boost/archive/iterators/base64_from_binary.hpp>
#include <streams_boost/archive/iterators/binary_from_base64.hpp>
#include <streams_boost/archive/iterators/transform_width.hpp>
#include <streams_boost/archive/iterators/insert_linebreaks.hpp>
#include <streams_boost/archive/iterators/remove_whitespace.hpp>

using namespace std;
using namespace SPL;
using namespace streams_boost::archive::iterators;

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  MongoDBLayer::MongoDBLayer()
  {
	  ttlIndexCreated = false;
	  genericLockIndexCreated = false;
	  storeLockIndexCreated = false;
	  mClient = NULL;
	  base64_chars = string("ABCDEFGHIJKLMNOPQRSTUVWXYZ") +
			  	  	 string("abcdefghijklmnopqrstuvwxyz") +
			  	  	 string("0123456789+/");
  }

  MongoDBLayer::~MongoDBLayer()
  {
	  // Application is ending now.
	  // Clean up the MongoDB client handle.
	  if (mClient != NULL) {
		  mongoc_client_destroy (mClient);
		  mongoc_cleanup ();
	  }
  }
        
  void MongoDBLayer::connectToDatabase(std::set<std::string> const & dbServers, PersistenceError & dbError)
  {
	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase", "MongoDBLayer");

	  // Get the name, OS version and CPU type of this machine.
	  struct utsname machineDetails;

	  if(uname(&machineDetails) < 0) {
		  dbError.set("Unable to get the machine/os/cpu details.", DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to get the machine/os/cpu details. " << DPS_INITIALIZE_ERROR, "MongoDBLayer");
		  return;
	  } else {
		  nameOfThisMachine = string(machineDetails.nodename);
		  osVersionOfThisMachine = string(machineDetails.sysname) + string(" ") + string(machineDetails.release);
		  cpuTypeOfThisMachine = string(machineDetails.machine);
	  }

	  string mongoConnectionErrorMsg = "";
	  // Let us connect to the configured MongoDB servers.
	  // MongoDB supports a server cluster. Hence, user may provide one or more server names.
	  // Form a string with all the servers listed in a CSV format: server1,server2,server3
	  std::string serverNames = "";

	  for (std::set<std::string>::iterator it=dbServers.begin(); it!=dbServers.end(); ++it) {
		 if (serverNames != "") {
			 // If there are multiple servers, add a comma next to the most recent server name we added.
			 serverNames += ",";
		 } else {
			 //Very first server name should be preceded by the following string.
			 serverNames = "mongodb://";
		 }

		 serverNames += *it;
	  }

	  // Do the mongoc init.
	  mongoc_init ();
	  // Create a client connection handle needed for all our Mongo activities.
	  mClient = mongoc_client_new (serverNames.c_str());

	  if (mClient == NULL) {
		  mongoConnectionErrorMsg = "Unable to connect to the configured MongoDB server(s).";
		  dbError.set(mongoConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error. Msg=" <<
				  mongoConnectionErrorMsg << ". " << DPS_INITIALIZE_ERROR, "MongoDBLayer");
		  return;
	  }

	  // We have now initialized the MongoDB client and made contacts with the configured MongoDB server(s).
	  // In Mongo, databases and collections are created upon the insertion of the first document.
	  // Hence, we need not create anything right now. We can exit from here now.
	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase done", "MongoDBLayer");
  }

  uint64_t MongoDBLayer::createStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside createStore for store " << name, "MongoDBLayer");

	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);

 	// Get a general purpose lock so that only one thread can
	// enter inside of this method at any given time with the same store name.
 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
 		// Unable to acquire the general purpose lock.
 		dbError.set("Unable to get a generic lock for creating a store with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for an yet to be created store with its name as " <<
 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "MongoDBLayer");
 		// User has to retry again to create this store.
 		return (0);
 	}

    // Let us first see if a store with the given name already exists in our Mongo database.
 	uint64_t storeId = findStore(name, dbError);

 	if (storeId > 0) {
		// This store already exists in our cache.
		// We can't create another one with the same name now.
 		std::ostringstream storeIdString;
 		storeIdString << storeId;
 		// The following error message carries the existing store's id at the very end of the message.
		dbError.set("A store named " + name + " already exists with a store id " + storeIdString.str(), DPS_STORE_EXISTS);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed while trying to create a duplicate store " << name << ". " << DPS_STORE_EXISTS, "MongoDBLayer");
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
 	} else {
 		// If the findStore method had any problems in accessing the GUID meta data, let us return that error now.
 		if ((dbError.hasError() == true) && (dbError.getErrorCode() == DPS_STORE_EXISTENCE_CHECK_ERROR)) {
 			// There was an error in finding the store.
 			releaseGeneralPurposeLock(base64_encoded_name);
 			return(0);
 		}

 		// Otherwise, this store doesn't exist.
 		// Reset the db error that may have been set by the findStore method we called above.
 		dbError.reset();
 	}

 	// This is a new store.
 	// Let us create a new store. We have to create a new store id.
 	// We will take the hashCode of the encoded store name and use that as the store id.
 	storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);

	/*
	We secured a guid. We can now create this store. Layout for a distributed process store (dps) looks like this in Mongo.
	In the Mongo database, DPS store/lock, and the individual store details will follow these data formats.
	That will allow us to be as close to the other DPS implementations using memcached, Redis, Cassandra, Cloudant and HBase.
	****************************************************************************************************************************************************************
	* 1) Create a root entry called "Store Name":  '0' + 'store name' => 'store id'                                                                                *
	* 2) Create "Store Contents Hash": '1' + 'store id' => 'Redis Hash'                                                                                            *
	*       This hash will always have the following three metadata entries:                                                                                       *
	*       dps_name_of_this_store ==> 'store name'                                                                                                     		   *
	*       dps_spl_type_name_of_key ==> 'spl type name for this store's key'                                                                                      *
	*       dps_spl_type_name_of_value ==> 'spl type name for this store's value'                                                                                  *
	* 3) In addition, we will also create and delete custom locks for modifying store contents in  (2) above: '4' + 'store id' + 'dps_lock' => 1                   *
	*                                                                                                                                                              *
	*                                                                                                                                                              *
	* 4) Create a root entry called "Lock Name":  '5' + 'lock name' ==> 'lock id'    [This lock is used for performing store commands in a transaction block.]     *
	* 5) Create "Lock Info":  '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'lock name' *
	* 6) In addition, we will also create and delete user-defined locks: '7' + 'lock id' + 'dl_lock' => 1                                                          *
	*                                                                                                                                                              *
	* 7) We will also allow general purpose locks to be created by any entity for sundry use:  '501' + 'entity name' + 'generic_lock' => 1                         *
	****************************************************************************************************************************************************************
	*/

 	// We will do the first two activities from the list above for our Mongo implementation.
	// Let us now do the step 1 described in the commentary above.
	// 1) Create the Store Name entry in the dps_dl_meta_data collection.
	//    '0' + 'store name' => 'store id'
	std::ostringstream storeIdString;
	storeIdString << storeId;
	string errorMsg = "";
	// MongoDB data model:  Database-->Collection-->Documents
	// Let us create or get a mongo collection structure.
	// It will create the database and the collection only if they don't already exist.
	mongoc_collection_t *mColl1 = NULL;
	mColl1 = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), string(DPS_DL_META_DATA_DB).c_str());

	if (mColl1 == NULL) {
		// Error in getting a collection structure within our DPS database in Mongo.
		errorMsg = "Unable to get a Mongo collection for the dps_dl_meta_data.";
		dbError.set("Unable to create 'StoreName-->GUID' in Mongo for the store named " + name + ". Mongo GetCollection error=" + errorMsg, DPS_STORE_NAME_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'StoreName-->GUID' in Mongo for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_NAME_CREATION_ERROR, "MongoDBLayer");
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

	// Create a bson_t structure on the heap.
	bson_t *bDoc;
	bson_error_t bErr;

	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
	string tmpStoreIdString = storeIdString.str();
	bDoc = BCON_NEW("_id", BCON_UTF8(dpsAndDlGuidKey.c_str()), "v", BCON_UTF8(tmpStoreIdString.c_str()));

	if (mongoc_collection_insert (mColl1, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == false) {
		// There was an error in creating the store name entry in the meta data table.
		bson_destroy(bDoc);
		errorMsg = string(bErr.message);
		dbError.set("Unable to create 'StoreName-->GUID' in Mongo for the store named " + name + ". Mongo insert error=" + errorMsg, DPS_STORE_NAME_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'StoreName-->GUID' in Mongo for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_NAME_CREATION_ERROR, "MongoDBLayer");
		mongoc_collection_destroy (mColl1);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

	// 2) Create the Store Contents collection now.
 	// This is the Mongo collection where we will keep storing all the key value pairs belonging to this store.
	// Collection name: 'dps_' + '1_' + 'store id'
	bson_destroy(bDoc);
 	string storeCollectionName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString.str();
	// Every store contents collection will always have these three metadata entries:
	// dps_name_of_this_store ==> 'store name'
	// dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	// dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	//
	// Every store contents collection will have at least three entries in it carrying the actual store name, key spl type name and value spl type name.
	// In addition, inside this store contents collection, we will house the
	// actual key:value data items that the user wants to keep in this store.
	// Such a store contents collection is very useful for data item read, write, deletion, enumeration etc.
 	//
 	mongoc_collection_t *mColl2 = NULL;
	mColl2 = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), storeCollectionName.c_str());

	if (mColl2 == NULL) {
		// Error in getting a collection structure within our DPS database in Mongo.
		errorMsg = "Unable to get a Mongo collection for the store contents.";
		dbError.set("Error in creating a store named " + name + ". Error=" + errorMsg, DPS_STORE_NAME_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create a store named " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_NAME_CREATION_ERROR, "MongoDBLayer");

		// We can delete the store name entry we created above.
		bDoc = BCON_NEW("_id", BCON_UTF8(dpsAndDlGuidKey.c_str()));
		mongoc_collection_remove (mColl1, MONGOC_REMOVE_NONE, bDoc, NULL, &bErr);
		bson_destroy(bDoc);
		// Free the first collection we allocated above.
		mongoc_collection_destroy (mColl1);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

	// Let us populate the new store contents collection with a mandatory element that will carry the name of this store.
	// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
	bDoc = BCON_NEW("_id", BCON_UTF8(string(MONGO_STORE_ID_TO_STORE_NAME_KEY).c_str()),
		"v", BCON_UTF8(base64_encoded_name.c_str()));

	if (mongoc_collection_insert (mColl2, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == false) {
		// There was an error in creating Meta Data 1.
		bson_destroy(bDoc);
		errorMsg = string(bErr.message);
		dbError.set("Unable to create 'Meta Data1' in Mongo for the store named " + name + ". " + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'Meta Data1' in Mongo for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "MongoDBLayer");
		// Since there is an error here, let us delete the store name entry we created above.
		bDoc = BCON_NEW("_id", BCON_UTF8(dpsAndDlGuidKey.c_str()));
		mongoc_collection_remove (mColl1, MONGOC_REMOVE_NONE, bDoc, NULL, &bErr);
		bson_destroy(bDoc);
		// We can delete entire the store collection.
		mongoc_collection_drop(mColl2, &bErr);
		mongoc_collection_destroy (mColl1);
		mongoc_collection_destroy (mColl2);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

    // We are now going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
	// Add the key spl type name metadata.
	bson_destroy(bDoc);
 	string base64_encoded_keySplTypeName;
 	base64_encode(keySplTypeName, base64_encoded_keySplTypeName);
	bDoc = BCON_NEW("_id", BCON_UTF8(string(MONGO_SPL_TYPE_NAME_OF_KEY).c_str()),
			"v", BCON_UTF8(base64_encoded_keySplTypeName.c_str()));

	if (mongoc_collection_insert (mColl2, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == false) {
		// There was an error in creating Meta Data 2.
		bson_destroy(bDoc);
		errorMsg = string(bErr.message);
		dbError.set("Unable to create 'Meta Data2' in Mongo for the store named " + name + ". " + errorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'Meta Data2' in Mongo for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "MongoDBLayer");
		// Since there is an error here, let us delete the store name entry we created above.
		bDoc = BCON_NEW("_id", BCON_UTF8(dpsAndDlGuidKey.c_str()));
		mongoc_collection_remove (mColl1, MONGOC_REMOVE_NONE, bDoc, NULL, &bErr);
		bson_destroy(bDoc);
		// We can delete entire the store collection.
		mongoc_collection_drop(mColl2, &bErr);
		mongoc_collection_destroy (mColl1);
		mongoc_collection_destroy (mColl2);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

	// Add the value spl type name metadata.
	bson_destroy(bDoc);
	string base64_encoded_valueSplTypeName;
	base64_encode(valueSplTypeName, base64_encoded_valueSplTypeName);
	bDoc = BCON_NEW("_id", BCON_UTF8(string(MONGO_SPL_TYPE_NAME_OF_VALUE).c_str()),
			"v", BCON_UTF8(base64_encoded_valueSplTypeName.c_str()));

	if (mongoc_collection_insert (mColl2, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == false) {
		// There was an error in creating Meta Data 2.
		bson_destroy(bDoc);
		errorMsg = string(bErr.message);
		dbError.set("Unable to create 'Meta Data3' in Mongo for the store named " + name + ". " + errorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'Meta Data3' in Mongo for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "MongoDBLayer");
		// Since there is an error here, let us delete the store name entry we created above.
		bDoc = BCON_NEW("_id", BCON_UTF8(dpsAndDlGuidKey.c_str()));
		mongoc_collection_remove (mColl1, MONGOC_REMOVE_NONE, bDoc, NULL, &bErr);
		bson_destroy(bDoc);
		// We can delete entire the store collection.
		mongoc_collection_drop(mColl2, &bErr);
		mongoc_collection_destroy (mColl1);
		mongoc_collection_destroy (mColl2);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	} else {
		// We created a new store as requested by the caller. Return the store id.
		bson_destroy(bDoc);
		mongoc_collection_destroy (mColl1);
		mongoc_collection_destroy (mColl2);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(storeId);
	}
  }

  uint64_t MongoDBLayer::createOrGetStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	// We will rely on a method above this and another method below this to accomplish what is needed here.
	SPLAPPTRC(L_DEBUG, "Inside createOrGetStore for store " << name, "MongoDBLayer");
	uint64_t storeId = createStore(name, keySplTypeName, valueSplTypeName, dbError);

	if (storeId > 0) {
		// It must be a new store that just got created in the method call we made above.
		return(storeId);
	}

	// Check if any error code is set from the create store method call we made above..
	if ((dbError.hasError() == true) && (dbError.getErrorCode() != DPS_STORE_EXISTS)) {
		// There was an error in creating the store.
		return(0);
	}

	// In all other cases, we are dealing with an existing store in our cache.
	// We can compute the storeId here and return the result to the caller.
	// (This idea of computing our own storeId via hashCode function is only done in the
	//  Cassandra, Cloudant, HBase, and Mongo DPS implementations. For memcached and Redis, we do it differently.)
	dbError.reset();
	// We will take the hashCode of the encoded store name and use that as the store id.
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);
	storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);
	return(storeId);
  }
                
  uint64_t MongoDBLayer::findStore(std::string const & name, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside findStore for store " << name, "MongoDBLayer");

	// We can search in the dps_dl_meta_data collection to see if a store exists for the given store name.
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);
	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
	string errorMsg = "";
	string value = "";

	bool result = readMongoDocumentValue(string(MONGO_DPS_DB_NAME), string(DPS_DL_META_DATA_DB),
		dpsAndDlGuidKey, value, errorMsg);

    if (result == true) {
		// This store already exists in our cache.
 	 	// We will take the hashCode of the encoded store name and use that as the store id.
 	 	uint64_t storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);
 	 	return(storeId);
    } else {
		// Requested data item is not there in the cache.
		dbError.set("The requested store " + name + " can't be found. Error=" + errorMsg, DPS_DATA_ITEM_READ_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed to locate the store " <<
			name << ". Error=" << errorMsg << ". " << DPS_DATA_ITEM_READ_ERROR, "MongoDBLayer");
		return(0);
    }
  }
        
  bool MongoDBLayer::removeStore(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside removeStore for store id " << store, "MongoDBLayer");

	ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MongoDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "MongoDBLayer");
		// User has to retry again to remove the store.
		return(false);
	}

	// Get rid of these two entries that are holding the store contents and the store name root entry.
	// 1) Store Contents collection
	// 2) Store Name root entry
	//
	uint32_t dataItemCnt = 0;
	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		releaseStoreLock(storeIdString);
		// This is alarming. This will put this store in a bad state. Poor user has to deal with it.
		return(false);
	}

	// Let us delete the Store Contents collection that contains all the active data items in this store.
	// Collection name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
 	string storeCollectionName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	mongoc_collection_t *mColl = NULL;
	mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), storeCollectionName.c_str());

	if (mColl == NULL) {
		// Error in getting a collection structure within our DPS database in Mongo.
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". Unable to get store collection.", "MongoDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	bson_error_t bErr;
	// Delete the store collection completely here.
	bool result = mongoc_collection_drop(mColl, &bErr);
	mongoc_collection_destroy (mColl);
	mColl = NULL;

	if (result == false) {
		// Error in deleting the store collection.
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString <<
			". (" << string(bErr.message) << ") Unable to delete the store collection.", "MongoDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), string(DPS_DL_META_DATA_DB).c_str());

	if (mColl == NULL) {
		// Error in getting a collection structure within our DPS database in Mongo.
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString <<
			". Unable to get meta data collection.", "MongoDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// Create a bson_t structure on the heap.
	bson_t *bDoc;
	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + storeName;
	bDoc = BCON_NEW("_id", BCON_UTF8(dpsAndDlGuidKey.c_str()));
	// Delete the store name entry in the meta data collection.
	result = mongoc_collection_remove (mColl, MONGOC_REMOVE_NONE, bDoc, NULL, &bErr);
	bson_destroy(bDoc);
	mongoc_collection_destroy (mColl);

	if (result == false) {
		// Error in removing the store name entry in the meta data collection.
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString <<
			". (" << string(bErr.message) << ") << Unable to remove the store name entry.", "MongoDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// Life of this store ended completely with no trace left behind.
	releaseStoreLock(storeIdString);
    return(true);
  }

  // This is a lean and mean put operation into a store.
  // It doesn't do any safety checks before putting a data item into a store.
  // If you want to go through that rigor, please use the putSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool MongoDBLayer::put(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside put for store id " << store, "MongoDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In our Mongo dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	// Let us convert the binary formatted valueData in our K/V pair to a base64 encoded string.
	string base64_encoded_data_item_value;
	b64_encode(valueData, valueSize, base64_encoded_data_item_value);

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Contents collection that takes the following name.
	// Collection name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeCollectionName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	mongoc_collection_t *mColl = NULL;
	mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), storeCollectionName.c_str());

	if (mColl == NULL) {
		// Error in getting a collection structure within our DPS database in Mongo.
		dbError.set("Unable to store a data item in the store id " + storeIdString +
			". Failed while getting the Mongo store collection.", DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed to store a data item in the store id " << storeIdString << ". "
			<< ". Failed while getting the Mongo store collection. " << DPS_DATA_ITEM_WRITE_ERROR, "MongoDBLayer");
		return(false);
	}

	// Try to insert the K/V pair in the store collection.
	bson_error_t bErr;
	bson_t *bDoc = BCON_NEW("_id", BCON_UTF8(base64_encoded_data_item_key.c_str()),
		"v", BCON_UTF8(base64_encoded_data_item_value.c_str()));

	if (mongoc_collection_insert (mColl, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == true) {
		// We stored/inserted the data item in the K/V store.
		bson_destroy(bDoc);
		mongoc_collection_destroy (mColl);
		return(true);
	}

	bson_destroy(bDoc);

	if (bErr.code != MONGO_DUPLICATE_KEY_FOUND) {
		// Error (other than a duplicate insertion) while inserting the K/V pair in our store collection.
		dbError.set("Unable to store a data item in the store id " + storeIdString +
			". Error=" + string(bErr.message), DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed to store a data item in the store id " << storeIdString << ". "
			<< ". Error=" << string(bErr.message) << ". " << DPS_DATA_ITEM_WRITE_ERROR, "MongoDBLayer");
		mongoc_collection_destroy (mColl);
		return(false);
	}

	// In Mongo, insert will fail if the given K/V pair already exists in the store. In that case, we will do an update.
	bDoc = BCON_NEW("_id", BCON_UTF8(base64_encoded_data_item_key.c_str()));
	bson_t *ubDoc = BCON_NEW("$set", "{", "v", BCON_UTF8(base64_encoded_data_item_value.c_str()), "}");

	if (mongoc_collection_update (mColl, MONGOC_UPDATE_NONE, bDoc, ubDoc, NULL, &bErr) == true) {
		// We stored/updated the data item in the K/V store.
		bson_destroy(bDoc);
		bson_destroy(ubDoc);
		mongoc_collection_destroy (mColl);
		return(true);
	} else {
		// Error while updating the K/V pair in our store collection.
		dbError.set("Unable to store/update a data item in the store id " + storeIdString +
			". Error=" + string(bErr.message), DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed to store/update a data item in the store id " << storeIdString << ". "
			<< ". Error=" << string(bErr.message) << ". " << DPS_DATA_ITEM_WRITE_ERROR, "MongoDBLayer");
		bson_destroy(bDoc);
		bson_destroy(ubDoc);
		mongoc_collection_destroy(mColl);
		return(false);
	}
  }

  // This is a special bullet proof version that does several safety checks before putting a data item into a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular put method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool MongoDBLayer::putSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside putSafe for store id " << store, "MongoDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to find a store with a store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MongoDBLayer");
		}

		return(false);
	}

	// In our Mongo dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	// Let us convert the binary formatted valueData in our K/V pair to a base64 encoded string.
	string base64_encoded_data_item_value;
	b64_encode(valueData, valueSize, base64_encoded_data_item_value);

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "MongoDBLayer");
		// User has to retry again to put the data item in the store.
		return(false);
	}

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Contents collection that takes the following name.
	// Collection name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeCollectionName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	mongoc_collection_t *mColl = NULL;
	mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), storeCollectionName.c_str());

	if (mColl == NULL) {
		// Error in getting a collection structure within our DPS database in Mongo.
		dbError.set("Unable to store a data item in the store id " + storeIdString +
			". Failed while getting the Mongo store collection.", DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to store a data item in the store id " << storeIdString << ". "
			<< ". Failed while getting the Mongo store collection. " << DPS_DATA_ITEM_WRITE_ERROR, "MongoDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// Try to insert the K/V pair in the store collection.
	bson_error_t bErr;
	bson_t *bDoc = BCON_NEW("_id", BCON_UTF8(base64_encoded_data_item_key.c_str()),
		"v", BCON_UTF8(base64_encoded_data_item_value.c_str()));

	if (mongoc_collection_insert (mColl, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == true) {
		// We stored/inserted the data item in the K/V store.
		bson_destroy(bDoc);
		mongoc_collection_destroy (mColl);
		releaseStoreLock(storeIdString);
		return(true);
	}

	bson_destroy(bDoc);

	if (bErr.code != MONGO_DUPLICATE_KEY_FOUND) {
		// Error (other than a duplicate insertion) while inserting the K/V pair in our store collection.
		dbError.set("Unable to store a data item in the store id " + storeIdString +
			". Error=" + string(bErr.message), DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to store a data item in the store id " << storeIdString << ". "
			<< ". Error=" << string(bErr.message) << ". " << DPS_DATA_ITEM_WRITE_ERROR, "MongoDBLayer");
		mongoc_collection_destroy (mColl);
		releaseStoreLock(storeIdString);
		return(false);
	}

	// In Mongo, insert will fail if the given K/V pair already exists in the store. In that case, we will do an update.
	bDoc = BCON_NEW("_id", BCON_UTF8(base64_encoded_data_item_key.c_str()));
	bson_t *ubDoc = BCON_NEW("$set", "{", "v", BCON_UTF8(base64_encoded_data_item_value.c_str()), "}");

	if (mongoc_collection_update (mColl, MONGOC_UPDATE_NONE, bDoc, ubDoc, NULL, &bErr) == true) {
		// We stored/updated the data item in the K/V store.
		bson_destroy(bDoc);
		bson_destroy(ubDoc);
		mongoc_collection_destroy (mColl);
		releaseStoreLock(storeIdString);
		return(true);
	} else {
		// Error while updating the K/V pair in our store collection.
		dbError.set("Unable to store/update a data item in the store id " + storeIdString +
			". Error=" + string(bErr.message), DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to store/update a data item in the store id " << storeIdString << ". "
			<< ". Error=" << string(bErr.message) << ". " << DPS_DATA_ITEM_WRITE_ERROR, "MongoDBLayer");
		bson_destroy(bDoc);
		bson_destroy(ubDoc);
		mongoc_collection_destroy(mColl);
		releaseStoreLock(storeIdString);
		return(false);
	}
  }

  // Put a data item with a TTL (Time To Live in seconds) value into the global area of the Mongo K/V store.
  bool MongoDBLayer::putTTL(char const * keyData, uint32_t keySize,
		  	  	  	  	    unsigned char const * valueData, uint32_t valueSize,
							uint32_t ttl, PersistenceError & dbError, bool encodeKey, bool encodeValue)
  {
	  SPLAPPTRC(L_DEBUG, "Inside putTTL.", "MongoDBLayer");

	  mongoc_collection_t *mColl = NULL;
	  bson_error_t bErr;
	  // Create a bson_t structure on the heap.
	  bson_t *bDoc;
	  mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), string(DPS_TTL_STORE_TOKEN).c_str());

	  if (mColl == NULL) {
		  // Error in getting a collection structure within our DPS database in Mongo.
		  dbError.setTTL("Unable to store a data item with TTL. Error in getting the TTL store collection.", DPS_DATA_ITEM_WRITE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside putTTL, it failed to store a data item with TTL. "
				  << "Error in getting the TTL store collection. " << DPS_DATA_ITEM_WRITE_ERROR, "MongoDBLayer");
		  return(false);
	  }

	  // Let us create the TTL expiration index ONLY ONCE here.
	  if (ttlIndexCreated == false) {
		  mongoc_index_opt_t miOpt;
		  // Initialize to default values.
		  mongoc_index_opt_init (&miOpt);
		  // Following setting of unique to false will not give an error while inserting a document with the
		  // same key of this index. We need this setting (false) to allow different K/V pairs to be inserted into this
		  // collection as well as the same K/V pair to be updated when needed.
		  miOpt.unique = false;
		  miOpt.name = string(DPS_TTL_STORE_TOKEN).c_str();
		  // Mongo DB supports document level expiration feature. Let us set the expiration time for this lock to 0.
		  // When we set this to 0, then every document inserted into this collection must set their own
		  // expiration value via a document field named "expireAt".
		  miOpt.expire_after_seconds = 0;

		  // Add a document field name that will be used as a collection index. Value of 1 means ascending index.
		  bDoc = BCON_NEW("expireAt", BCON_INT32(1));
		  // Create the index for the Mongo collection that will hold all the TTL based K/V pairs.
		  mongoc_collection_create_index (mColl, bDoc, &miOpt, &bErr);
		  bson_destroy(bDoc);
		  ttlIndexCreated = true;
	  }

	  // In our Mongo dps implementation, data item keys can have space characters.
	  string base64_encoded_data_item_key;

          if (encodeKey == true) {
	     base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
          } else {
            // Since the key data sent here will always be in the network byte buffer format (NBF), 
            // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
            // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
            // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
            if ((uint8_t)keyData[0] < 0x80) {
               // Skip the first length byte. 
               base64_encoded_data_item_key = string(&keyData[1], keySize-1);  
            } else {
               // Skip the five bytes at the beginning that represent the length of the key data.
               base64_encoded_data_item_key = string(&keyData[5], keySize-5);
            }
          }

	  // Let us convert the binary formatted valueData in our K/V pair to a base64 encoded string.
	  string base64_encoded_data_item_value;
	  b64_encode(valueData, valueSize, base64_encoded_data_item_value);

	  // In Mongo, background task that does the TTL expiration runs for every 60 seconds.
	  // Hence, setting a TTL value less than 60 seconds will take effect only in the subsequent 60 second boundary.
	  uint64_t _ttl = ttl;
	  if (_ttl == 0) {
		  // User wants this TTL to be forever.
		  // In the case of the DPS Mongo layer, we will use a TTL value of 25 years represented in seconds.
		  // If our caller opts for this, then this long-term TTL based K/V pair can be removed using
		  // the dpsRemoveTTL API anytime a premature removal of that K/V pair is needed.
		  _ttl = MONGO_MAX_TTL_VALUE;
	  }

	  // Calculate the time in the future when this K/V pair should be removed.
	  int64_t expiryTimeInMilliSeconds = (std::time(0) * 1000) + (_ttl * 1000);
	  // MongoDB will evict this K/V pair if it continues to exist without being released beyond the configured timeInMilliSeconds.
	  bDoc = BCON_NEW("expireAt", BCON_DATE_TIME(expiryTimeInMilliSeconds),
		  "_id", BCON_UTF8(base64_encoded_data_item_key.c_str()),
		  "v", BCON_UTF8(base64_encoded_data_item_value.c_str()));

	  // We are ready to either store a new data item or update an existing data item.
	  // Try to insert the K/V pair in the store collection.
	  if (mongoc_collection_insert (mColl, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == true) {
		  // We stored/inserted the data item in the K/V store.
		  bson_destroy(bDoc);
		  mongoc_collection_destroy (mColl);
		  return(true);
	  }

	  bson_destroy(bDoc);

	  if (bErr.code != MONGO_DUPLICATE_KEY_FOUND) {
		  // Error (other than a duplicate insertion) while inserting the K/V pair.
		  dbError.setTTL("Unable to store a TTL based data item. Error=" +
			  string(bErr.message), DPS_DATA_ITEM_WRITE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside putTTL, it failed to store a TTL based data item. Error=" <<
			  string(bErr.message) << ". " << DPS_DATA_ITEM_WRITE_ERROR, "MongoDBLayer");
		  mongoc_collection_destroy (mColl);
		  return(false);
	  }

	  // In Mongo, insert will fail if the given K/V pair already exists in the TTL store. In that case, we will do an update.
	  bDoc = BCON_NEW("_id", BCON_UTF8(base64_encoded_data_item_key.c_str()));
	  bson_t *ubDoc = BCON_NEW("$set", "{",
		  "expireAt", BCON_DATE_TIME(expiryTimeInMilliSeconds),
		  "v", BCON_UTF8(base64_encoded_data_item_value.c_str()), "}");

	  if (mongoc_collection_update (mColl, MONGOC_UPDATE_NONE, bDoc, ubDoc, NULL, &bErr) == true) {
		  // We stored/updated the data item in the K/V store.
		  bson_destroy(bDoc);
		  bson_destroy(ubDoc);
		  mongoc_collection_destroy (mColl);
		  return(true);
	  } else {
		  // Error while updating the K/V pair in our TTL store collection.
		  dbError.setTTL("Unable to store/update a TTL based data item. Error=" +
			  string(bErr.message), DPS_DATA_ITEM_WRITE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside putTTL, it failed to store/update a TTL based data item. Error=" <<
			  string(bErr.message) << ". " << DPS_DATA_ITEM_WRITE_ERROR, "MongoDBLayer");
		  bson_destroy(bDoc);
		  bson_destroy(ubDoc);
		  mongoc_collection_destroy(mColl);
		  return(false);
	  }
  }

  // This is a lean and mean get operation from a store.
  // It doesn't do any safety checks before getting a data item from a store.
  // If you want to go through that rigor, please use the getSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool MongoDBLayer::get(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside get for store id " << store, "MongoDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
	// Every data item's key must be prefixed with its store id before being used for CRUD operation in the Mongo store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In our Mongo dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);

	bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		false, true, valueData, valueSize, dbError);

	if ((result == false) || (dbError.hasError() == true)) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside get, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
	}

    return(result);
  }

  // This is a special bullet proof version that does several safety checks before getting a data item from a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular get method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool MongoDBLayer::getSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside getSafe for store id " << store, "MongoDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
	// Every data item's key must be prefixed with its store id before being used for CRUD operation in the Mongo store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MongoDBLayer");
		}

		return(false);
	}

	// In our Mongo dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);

	bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		false, false, valueData, valueSize, dbError);

	if ((result == false) || (dbError.hasError() == true)) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
	}

    return(result);
  }

  // Get a TTL based data item that is stored in the global area of the Mongo K/V store.
   bool MongoDBLayer::getTTL(char const * keyData, uint32_t keySize,
                              unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError, bool encodeKey)
   {
		SPLAPPTRC(L_DEBUG, "Inside getTTL.", "MongoDBLayer");

		// Since this is a data item with TTL, it is stored in the global area of Mongo and not inside a user created store.
		string data_item_key = string(keyData, keySize);
		string base64_encoded_data_item_key;

                if (encodeKey == true) {
 	           base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
                } else {
                   // Since the key data sent here will always be in the network byte buffer format (NBF), 
                   // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
                   // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
                   // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
                   if ((uint8_t)keyData[0] < 0x80) {
                      // Skip the first length byte. 
                      base64_encoded_data_item_key = string(&keyData[1], keySize-1);  
                   } else {
                      // Skip the five bytes at the beginning that represent the length of the key data.
                      base64_encoded_data_item_key = string(&keyData[5], keySize-5);
                   }
                }

		// Get the data from the TTL table (not from the user created tables).
		string errorMsg = "";
		string value = "";
		bool result = readMongoDocumentValue(string(MONGO_DPS_DB_NAME), string(DPS_TTL_STORE_TOKEN),
			base64_encoded_data_item_key, value, errorMsg);

		if (errorMsg != "") {
			// Error message means something went wrong in the call we made above.
			// Unable to get the requested data item from the cache.
			dbError.setTTL("Unable to access the TTL based K/V pair in Mongo. " + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			return(false);
		}

		// If the data item is not there, we can't do much at this point.
		if (result == false) {
		  // This data item doesn't exist. Let us raise an error.
		  // Requested data item is not there in the cache.
		  dbError.setTTL("The requested TTL based data item doesn't exist.", DPS_DATA_ITEM_READ_ERROR);
		   return(false);
		} else {
			// In Mongo, when we put the TTL based K/V pair, we b64_encoded our binary value buffer and
			// stored it as a string. We must now b64_decode it back to binary value buffer.
			b64_decode(value, valueData, valueSize);

			if (valueData == NULL) {
				// We encountered malloc error inside the b64_decode method we called above.
				// Only then, we will have a null value data.
				// Unable to allocate memory to transfer the data item value.
				dbError.setTTL("Unable to allocate memory to copy the TTL based data item value.", DPS_GET_DATA_ITEM_MALLOC_ERROR);
				return(false);
			} else {
				// In the b64_decode method, we allocated a memory buffer and directly assigned it to
				// the caller provided pointer (valueData). It is the caller's responsibility to
				// free that buffer when it is no longer needed.
				return(true);
			}
		}
   }

  bool MongoDBLayer::remove(uint64_t store, char const * keyData, uint32_t keySize,
                                PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside remove for store id " << store, "MongoDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MongoDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "MongoDBLayer");
		// User has to retry again to remove the data item from the store.
		return(false);
	}

	// In our Mongo dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	// We are ready to remove a data item from the Mongo collection.
	// This action is performed on the Store Contents collection that takes the following name.
	// Collection name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeCollectionName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	mongoc_collection_t *mColl = NULL;
	bson_error_t bErr;
	// Create a bson_t structure on the heap.
	bson_t *bDoc;
	mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), string(storeCollectionName).c_str());

	if (mColl == NULL) {
		// Error in getting a collection structure within our DPS database in Mongo.
		dbError.setTTL("Unable to remove a data item. Error in getting the store collection.", DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed to remove a data item. "
			<< "Error in getting the store collection. " << DPS_DATA_ITEM_DELETE_ERROR, "MongoDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	bDoc = BCON_NEW("_id", BCON_UTF8(base64_encoded_data_item_key.c_str()));
	// Delete the K/V entry in the store.
	bool result = mongoc_collection_remove (mColl, MONGOC_REMOVE_NONE, bDoc, NULL, &bErr);
	bson_destroy(bDoc);
	mongoc_collection_destroy (mColl);

	if (result == false) {
		dbError.set("Mongo error while removing the requested data item from the store id " + storeIdString +
			". " + string(bErr.message), DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed to remove a data item from store id " << storeIdString <<
			". " << string(bErr.message) << ". " << DPS_DATA_ITEM_DELETE_ERROR, "MongoDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// All done. An existing data item in the given store has been removed.
	releaseStoreLock(storeIdString);
    return(true);
  }

  // Remove a TTL based data item that is stored in the global area of the Mongo K/V store.
  bool MongoDBLayer::removeTTL(char const * keyData, uint32_t keySize,
                                PersistenceError & dbError, bool encodeKey)
  {
		SPLAPPTRC(L_DEBUG, "Inside removeTTL.", "MongoDBLayer");

		// In our Mongo dps implementation, data item keys can have space characters.
		string base64_encoded_data_item_key;

                if (encodeKey == true) {
 	           base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
                } else {
                   // Since the key data sent here will always be in the network byte buffer format (NBF), 
                   // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
                   // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
                   // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
                   if ((uint8_t)keyData[0] < 0x80) {
                      // Skip the first length byte. 
                      base64_encoded_data_item_key = string(&keyData[1], keySize-1);  
                   } else {
                      // Skip the five bytes at the beginning that represent the length of the key data.
                      base64_encoded_data_item_key = string(&keyData[5], keySize-5);
                   }
                }

		mongoc_collection_t *mColl = NULL;
		bson_error_t bErr;
		// Create a bson_t structure on the heap.
		bson_t *bDoc;
		mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), string(DPS_TTL_STORE_TOKEN).c_str());

		if (mColl == NULL) {
			// Error in getting a collection structure within our DPS database in Mongo.
			dbError.setTTL("Unable to remove a TTL based data item. Error in getting the TTL store collection.", DPS_DATA_ITEM_DELETE_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to remove a TTL based data item. "
				<< "Error in getting the TTL store collection. " << DPS_DATA_ITEM_DELETE_ERROR, "MongoDBLayer");
			return(false);
		}

		bDoc = BCON_NEW("_id", BCON_UTF8(base64_encoded_data_item_key.c_str()));
		// Delete the K/V entry in the TTL collection.
		bool result = mongoc_collection_remove (mColl, MONGOC_REMOVE_NONE, bDoc, NULL, &bErr);
		bson_destroy(bDoc);
		mongoc_collection_destroy (mColl);

		if (result == false) {
			dbError.setTTL("Mongo error while removing the requested TTL based data item from the TTL store. " +
				string(bErr.message), DPS_DATA_ITEM_DELETE_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to remove a TTL based data item from the TTL store. " <<
				string(bErr.message) << ". " << DPS_DATA_ITEM_DELETE_ERROR, "MongoDBLayer");
			return(false);
		}

		// All done. An existing data item in the TTL store has been removed.
		return(true);
  }

  bool MongoDBLayer::has(uint64_t store, char const * keyData, uint32_t keySize,
                             PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside has for store id " << store, "MongoDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MongoDBLayer");
		}

		return(false);
	}

	// In our Mongo dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);
	unsigned char *dummyValueData;
	uint32_t dummyValueSize;

	// Let us see if we already have this data item in our cache.
	// Check only for the data item existence and don't fetch the data item value.
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);
	bool dataItemAlreadyInCache = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		true, false, dummyValueData, dummyValueSize, dbError);

	if (dbError.getErrorCode() != 0) {
		SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
	}

	return(dataItemAlreadyInCache);
  }

  // Check for the existence of a TTL based data item that is stored in the global area of the Mongo K/V store.
  bool MongoDBLayer::hasTTL(char const * keyData, uint32_t keySize,
                             PersistenceError & dbError, bool encodeKey)
  {
		SPLAPPTRC(L_DEBUG, "Inside hasTTL.", "MongoDBLayer");

		// Since this is a data item with TTL, it is stored in the global area of Mongo and not inside a user created store.
		string data_item_key = string(keyData, keySize);
		string base64_encoded_data_item_key;

                if (encodeKey == true) {
 	           base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
                } else {
                   // Since the key data sent here will always be in the network byte buffer format (NBF), 
                   // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
                   // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
                   // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
                   if ((uint8_t)keyData[0] < 0x80) {
                      // Skip the first length byte. 
                      base64_encoded_data_item_key = string(&keyData[1], keySize-1);  
                   } else {
                      // Skip the five bytes at the beginning that represent the length of the key data.
                      base64_encoded_data_item_key = string(&keyData[5], keySize-5);
                   }
                }

		// Check for the data item existence in the TTL table (not in the user created tables).
		string errorMsg = "";
		string value = "";
		bool result = readMongoDocumentValue(string(MONGO_DPS_DB_NAME), string(DPS_TTL_STORE_TOKEN),
			base64_encoded_data_item_key, value, errorMsg);

		if (errorMsg != "") {
			// Error message means something went wrong in the call we made above.
			// Unable to get the requested data item from the cache.
			dbError.setTTL("Unable to check for the existence of a TTL based K/V pair in Mongo. " + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			return(false);
		}

		return(result);
  }

  void MongoDBLayer::clear(uint64_t store, PersistenceError & dbError) {
     SPLAPPTRC(L_DEBUG, "Inside clear for store id " << store, "MongoDBLayer");

 	ostringstream storeId;
 	storeId << store;
 	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MongoDBLayer");
		}

		return;
	}

 	// Lock the store first.
 	if (acquireStoreLock(storeIdString) == false) {
 		// Unable to acquire the store lock.
 		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside clear, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "MongoDBLayer");
 		// User has to retry again to remove the store.
 		return;
 	}

 	// Get the store name.
 	uint32_t dataItemCnt = 0;
 	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		releaseStoreLock(storeIdString);
		// This is alarming. This will put this store in a bad state. Poor user has to deal with it.
		return;
	}

 	// A very fast and quick thing to do is to simply delete the Store Contents collection and
 	// recreate the meta data entries rather than removing one element at a time.
	// This action is performed on the Store Contents collection that takes the following name.
	// Collection name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
 	string storeCollectionName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
 	// Let us delete the entire collection for this store.
 	mongoc_collection_t *mColl = NULL;
 	mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), storeCollectionName.c_str());

 	if (mColl == NULL) {
 		// Error in getting a collection structure within our DPS database in Mongo.
		dbError.set("Unable to get the store contents collection for store id " + storeIdString + ".", DPS_STORE_CLEARING_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed to get the store contents collection for store id " <<
			storeIdString << ". " << DPS_STORE_CLEARING_ERROR, "MongoDBLayer");
		releaseStoreLock(storeIdString);
 		return;
 	}

 	bson_error_t bErr;
	bool result = mongoc_collection_drop(mColl, &bErr);

	if (result == false) {
		// Unable to drop the store collection.
		dbError.set("Unable to delete the store contents collection for store id " + storeIdString +
			". Error=" + string(bErr.message), DPS_STORE_CLEARING_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed to delete the store contents collection for store id " <<
			storeIdString << ". Error=" << string(bErr.message) << ". " << DPS_STORE_CLEARING_ERROR, "MongoDBLayer");
		mongoc_collection_destroy (mColl);
		releaseStoreLock(storeIdString);
 		return;
	}

 	// Create a new store contents collection for this store.
	// Collection name: 'dps_' + '1_' + 'store id'
	// Every store contents collection will always have these three metadata entries:
	// dps_name_of_this_store ==> 'store name'
	// dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	// dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	//
	// Every store contents collection will have at least three entries in it carrying the actual store name, key spl type name and value spl type name.
	// In addition, inside this store contents collection, we will house the
	// actual key:value data items that the user wants to keep in this store.
	// Such a store contents collection is very useful for data item read, write, deletion, enumeration etc.
 	//

	// Let us populate the new store contents collection with a mandatory element that will carry the name of this store.
	// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
	string errorMsg = "";
	bson_t *bDoc;
	bDoc = BCON_NEW("_id", BCON_UTF8(string(MONGO_STORE_ID_TO_STORE_NAME_KEY).c_str()),
		"v", BCON_UTF8(storeName.c_str()));

	if (mongoc_collection_insert (mColl, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == false) {
		// There was an error in creating Meta Data 1.
		// This is not good at all. This will leave this store in a zombie state in Mongo.
		bson_destroy(bDoc);
		mongoc_collection_destroy (mColl);
		errorMsg = string(bErr.message);
		dbError.set("Critical error: Unable to create 'Meta Data1' in Mongo for the store id " + storeIdString + ". " + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside createStore, it failed to create 'Meta Data1' in Mongo for the store id " <<
			storeIdString << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "MongoDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

	bson_destroy(bDoc);

    // We are now going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
	// Add the key spl type name metadata.
	bDoc = BCON_NEW("_id", BCON_UTF8(string(MONGO_SPL_TYPE_NAME_OF_KEY).c_str()),
			"v", BCON_UTF8(keySplTypeName.c_str()));

	if (mongoc_collection_insert (mColl, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == false) {
		// There was an error in creating Meta Data 2.
		// This is not good at all. This will leave this store in a zombie state in Mongo.
		bson_destroy(bDoc);
		mongoc_collection_destroy (mColl);
		errorMsg = string(bErr.message);
		dbError.set("Critical error: Unable to create 'Meta Data2' in Mongo for the store id " + storeIdString + ". " + errorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside createStore, it failed to create 'Meta Data2' in Mongo for the store id " <<
			storeIdString << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "MongoDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

	bson_destroy(bDoc);
	bDoc = BCON_NEW("_id", BCON_UTF8(string(MONGO_SPL_TYPE_NAME_OF_VALUE).c_str()),
			"v", BCON_UTF8(valueSplTypeName.c_str()));

	if (mongoc_collection_insert (mColl, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == false) {
		// There was an error in creating Meta Data 2.
		bson_destroy(bDoc);
		mongoc_collection_destroy (mColl);
		errorMsg = string(bErr.message);
		dbError.set("Critical error: Unable to create 'Meta Data3' in Mongo for the store id " + storeIdString + ". " + errorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside createStore, it failed to create 'Meta Data3' in Mongo for the store id " <<
			storeIdString << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "MongoDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

 	// If there was an error in the store contents hash recreation, then this store is going to be in a weird state.
	// It is a fatal error. If that happened, something terribly had gone wrong in clearing the contents.
	// User should look at the dbError code and decide about a corrective action.
	bson_destroy(bDoc);
	mongoc_collection_destroy (mColl);
 	releaseStoreLock(storeIdString);
  }
        
  uint64_t MongoDBLayer::size(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside size for store id " << store, "MongoDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	string storeCollectionName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string errorMsg = "";
	int64_t collectionSize = 0;
	bool result = readMongoCollectionSize(string(MONGO_DPS_DB_NAME), storeCollectionName, collectionSize, errorMsg);

	if (result == false || collectionSize <= 0) {
		// This is not correct. We must have a minimum hash size of 3 because of the reserved elements.
		dbError.set("Wrong value (zero) observed as the store size for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_STORE_SIZE_ERROR);
		return (0);
	}

	// Our Store Contents collection for every store will have a mandatory reserved internal elements (store name, key spl type name, and value spl type name).
	// Let us not count those three elements in the actual store contents size that the caller wants now.
	return((uint64_t)(collectionSize - 3));
  }

  // We allow space characters in the data item keys.
  // Hence, it is required to based64 encode them before storing the
  // key:value data item in Mongo.
  // (Use boost functions to do this.)
  //
  void MongoDBLayer::base64_encode(std::string const & str, std::string & base64) {
	  // Insert line breaks for every 64KB characters.
	  typedef insert_linebreaks<base64_from_binary<transform_width<string::const_iterator,6,8> >, 64*1024 > it_base64_t;

	  unsigned int writePaddChars = (3-str.length()%3)%3;
	  base64 = string(it_base64_t(str.begin()),it_base64_t(str.end()));
	  base64.append(writePaddChars,'=');
  }

  // As explained above, we based64 encoded the data item keys before adding them to the store.
  // If we need to get back the original key name, this function will help us in
  // decoding the base64 encoded key.
  // (Use boost functions to do this.)
  //
  void MongoDBLayer::base64_decode(std::string & base64, std::string & result) {
	  // IMPORTANT:
	  // For performance reasons, we are not passing a const string to this method.
	  // Instead, we are passing a directly modifiable reference. Caller should be aware that
	  // the string they passed to this method gets altered during the base64 decoding logic below.
	  // After this method returns back to the caller, it is not advisable to use that modified string.
	  typedef transform_width< binary_from_base64<remove_whitespace<string::const_iterator> >, 8, 6 > it_binary_t;

	  unsigned int paddChars = count(base64.begin(), base64.end(), '=');
	  std::replace(base64.begin(),base64.end(),'=','A'); // replace '=' by base64 encoding of '\0'
	  result = string(it_binary_t(base64.begin()), it_binary_t(base64.end())); // decode
	  result.erase(result.end()-paddChars,result.end());  // erase padding '\0' characters
  }

  // This method will check if a store exists for a given store id.
  bool MongoDBLayer::storeIdExistsOrNot(string storeIdString, PersistenceError & dbError) {
	  string storeCollectionName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	  string key = string(MONGO_STORE_ID_TO_STORE_NAME_KEY);
	  string errorMsg = "";
	  mongoc_collection_t *mColl = NULL;
	  mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), storeCollectionName.c_str());

	  if (mColl == NULL) {
		  // Error in getting a collection structure within our DPS database in Mongo.
		  errorMsg = "Unable to get a Mongo collection for the given store.";
		  dbError.set("StoreIdExistsOrNot: " + storeIdString + ". Error=" + errorMsg, DPS_STORE_EXISTENCE_CHECK_ERROR);
		  return (false);
	  }

	  // Create a bson_t structure on the heap.
	  bson_t *bDoc;
	  const bson_t *bDocResult;
	  bDoc = BCON_NEW("_id", BCON_UTF8(key.c_str()));
	  mongoc_cursor_t *mCursor;
	  string valueStr = "";
	  bool dataItemFound = false;
	  // Find it inside the mongo collection.
	  mCursor = mongoc_collection_find (mColl, MONGOC_QUERY_NONE, 0, 0, 0, bDoc, NULL, NULL);

	  // Iterate through the retrieved cursor and get the result as JSON.
	  while (mongoc_cursor_next (mCursor, &bDocResult)) {
		  char *str = bson_as_json (bDocResult, NULL);

		  if (str != NULL) {
			  valueStr = string(str);
			  bson_free(str);
			  dataItemFound = true;
		  }

		  // We want to read only one entry from this collection. We can break now.
		  break;
	  }

	  bson_destroy(bDoc);
	  mongoc_cursor_destroy (mCursor);
	  mongoc_collection_destroy (mColl);

	  if (dataItemFound == true) {
		  // This store already exists in our cache.
		  return(true);
	  } else {
		  // Unable to access a K/V entry in the store contents collection for the given store id. This is not a correct behavior.
		  dbError.set("StoreIdExistsOrNot: Unable to get StoreContents meta data1 for the StoreId " + storeIdString +
		     ".", DPS_GET_STORE_CONTENTS_HASH_ERROR);
		  return(false);
	  }
  }

  // This method will acquire a lock for a given store.
  bool MongoDBLayer::acquireStoreLock(string const & storeIdString) {
	  int32_t retryCnt = 0;

	  // '4' + 'store id' + 'dps_lock' => 1
	  std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;
	  uint64_t lockId = SPL::Functions::Utility::hashCode(storeLockKey);
	  ostringstream lockIdStr;
	  lockIdStr << lockId;
	  string tmpLockIdStr = lockIdStr.str();

	  // MongoDB data model:  Database-->Collection-->Documents
	  // Let us create or get a mongo collection structure for this store specific lock.
	  // It will create the database and the collection only if they don't already exist.
	  // Create a bson_t structure on the heap.
	  bson_t *bDoc;
	  bson_error_t bErr;
	  mongoc_collection_t *mColl = NULL;
	  mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(),
		  (string(DPS_DL_META_DATA_DB) + string(DPS_STORE_LOCK_TYPE)).c_str());

	  if (mColl == NULL) {
		  // Error in getting a collection structure within our DPS database in Mongo.
		  return(false);
	  }

	  // Create it only once at the very first time we come here.
	  if (storeLockIndexCreated == false) {
		  // Define a variable for the collection index structure.
		  mongoc_index_opt_t miOpt;
		  // Initialize this structure to default values.
		  mongoc_index_opt_init (&miOpt);
		  // Following setting of unique to false will not give an error while inserting a document with the
		  // same key of this index. We need this setting (false) to allow different K/V pairs to be inserted into this
		  // collection as well as the same K/V pair to be updated when needed.
		  miOpt.unique = false;
		  miOpt.name = (string(DPS_DL_META_DATA_DB) + string(DPS_STORE_LOCK_TYPE)).c_str();
		  // Mongo DB supports document level expiration feature. Let us set the expiration time for this lock.
		  miOpt.expire_after_seconds = (int32_t)DPS_AND_DL_GET_LOCK_TTL;

		  // Add a document field name that will be used as a collection index. Value of 1 means ascending index.
		  bDoc = BCON_NEW("createdAt", BCON_INT32(1));
		  // Create the index for the Mongo collection that will hold this generic lock information.
		  mongoc_collection_create_index (mColl, bDoc, &miOpt, &bErr);
		  bson_destroy(bDoc);
		  storeLockIndexCreated = true;
	  }

	  // Try to get a lock for this generic entity.
	  while (1) {
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or the MongoDB removes it after the TTL value expires.
		// Make an attempt to create a document for this lock now.
		int64_t timeInMilliSeconds = (std::time(0) * 1000);
		// MongoDB will evict this lock if it continues to exist without being released beyond (currentTime + mOpt.expire_after_seconds).
		bDoc = BCON_NEW("createdAt", BCON_DATE_TIME(timeInMilliSeconds),
			"_id", BCON_UTF8(storeLockKey.c_str()), "v", BCON_UTF8(tmpLockIdStr.c_str()));

		if (mongoc_collection_insert (mColl, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == true) {
			bson_destroy(bDoc);
			mongoc_collection_destroy (mColl);
			return(true);
		}

		// bErr.message  will have the error string if needed to be displayed here.
		bson_destroy(bDoc);
		SPLAPPTRC(L_DEBUG, "Store specific lock acquisition error=" << string(bErr.message), "MongoDBLayer");
		// Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
			mongoc_collection_destroy (mColl);
			return(false);
		}

		/*
		// Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
		usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
			  (retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
		*/
		// Do a random wait before trying to create it.
		// This will give random value between 0 and 1.
		float64 rand = SPL::Functions::Math::random();
		// Let us wait for that random duration which is a partial second i.e. less than a second.
		SPL::Functions::Utility::block(rand);
	  }

	  mongoc_collection_destroy (mColl);
	  return(false);
  }

  void MongoDBLayer::releaseStoreLock(string const & storeIdString) {
	  // '4' + 'store id' + 'dps_lock' => 1
	  std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;
	  mongoc_collection_t *mColl = NULL;
	  mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(),
		  (string(DPS_DL_META_DATA_DB) + string(DPS_STORE_LOCK_TYPE)).c_str());

	  if (mColl == NULL) {
		  // Error in getting a collection structure within our DPS database in Mongo.
		  return;
	  }

	  bson_error_t bErr;
	  // Create a bson_t structure on the heap.
	  bson_t *bDoc;
	  bDoc = BCON_NEW("_id", BCON_UTF8(storeLockKey.c_str()));
	  // Delete the store specific lock entry in the meta data collection.
	  mongoc_collection_remove (mColl, MONGOC_REMOVE_NONE, bDoc, NULL, &bErr);
	  bson_destroy(bDoc);
	  mongoc_collection_destroy (mColl);
  }

  bool MongoDBLayer::readStoreInformation(std::string const & storeIdString, PersistenceError & dbError,
		  uint32_t & dataItemCnt, std::string & storeName, std::string & keySplTypeName, std::string & valueSplTypeName) {
	  // Read the store name, this store's key and value SPL type names,  and get the store size.
	  storeName = "";
	  keySplTypeName = "";
	  valueSplTypeName = "";
	  dataItemCnt = 0;
	  string storeCollectionName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;

	  // This action is performed on the Store Contents collection that takes the following name.
	  // 'dps_' + '1_' + 'store id'
	  // It will always have the following three metadata entries:
	  // dps_name_of_this_store ==> 'store name'
	  // dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	  // dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	  // 1) Get the store name.
	  string key = string(MONGO_STORE_ID_TO_STORE_NAME_KEY);
	  string errorMsg = "";
	  string value = "";

	  bool result = readMongoDocumentValue(string(MONGO_DPS_DB_NAME), storeCollectionName, key, value, errorMsg);

	  if (result == true) {
		  storeName = value;
	  } else {
		  // Unable to get the store name for this store's key.
		  dbError.set("Unable to get the store name for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_STORE_NAME_ERROR);
		  return (false);
	  }

	  // 2) Let us get the spl type name for this store's key.
	  key = string(MONGO_SPL_TYPE_NAME_OF_KEY);
	  errorMsg = "";
	  value = "";

	  result = readMongoDocumentValue(string(MONGO_DPS_DB_NAME), storeCollectionName, key, value, errorMsg);

	  if (result == true) {
		  keySplTypeName = value;
	  } else {
		  // Unable to get the spl type name for this store's key.
		  dbError.set("Unable to get the key spl type name for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_KEY_SPL_TYPE_NAME_ERROR);
		  return (false);
	  }

	  // 3) Let us get the spl type name for this store's value.
	  key = string(MONGO_SPL_TYPE_NAME_OF_VALUE);
	  errorMsg = "";
	  value = "";

	  result = readMongoDocumentValue(string(MONGO_DPS_DB_NAME), storeCollectionName, key, value, errorMsg);

	  if (result == true) {
		  valueSplTypeName = value;
	  } else {
		  // Unable to get the spl type name for this store's value.
		  dbError.set("Unable to get the value spl type name for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_VALUE_SPL_TYPE_NAME_ERROR);
		  return (false);
	  }

	  // 4) Let us get the size of the store contents collection now.
	  errorMsg = "";
	  int64_t collectionSize = 0;
	  result = readMongoCollectionSize(string(MONGO_DPS_DB_NAME), storeCollectionName, collectionSize, errorMsg);

	  if (result == false || collectionSize <= 0) {
		  // This is not correct. We must have a minimum hash size of 3 because of the reserved elements.
		  dbError.set("Wrong value (zero) observed as the store size for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_STORE_SIZE_ERROR);
		  return (false);
	  }

	  // Our Store Contents collection for every store will have a mandatory reserved internal elements (store name, key spl type name, and value spl type name).
	  // Let us not count those three elements in the actual store contents size that the caller wants now.
	  dataItemCnt = (uint32_t)collectionSize;
	  dataItemCnt -= 3;
	  return(true);
  }

  string MongoDBLayer::getStoreName(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MongoDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		  return("");
	  }

	  string base64_decoded_storeName;
	  base64_decode(storeName, base64_decoded_storeName);
	  return(base64_decoded_storeName);
  }

  string MongoDBLayer::getSplTypeNameForKey(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MongoDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		  return("");
	  }

	  string base64_decoded_keySplTypeName;
	  base64_decode(keySplTypeName, base64_decoded_keySplTypeName);
	  return(base64_decoded_keySplTypeName);
  }

  string MongoDBLayer::getSplTypeNameForValue(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MongoDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		  return("");
	  }

	  string base64_decoded_valueSplTypeName;
	  base64_decode(valueSplTypeName, base64_decoded_valueSplTypeName);
	  return(base64_decoded_valueSplTypeName);
  }

  std::string MongoDBLayer::getNoSqlDbProductName(void) {
	  return(string(MONGO_NO_SQL_DB_NAME));
  }

  void MongoDBLayer::getDetailsAboutThisMachine(std::string & machineName, std::string & osVersion, std::string & cpuArchitecture) {
	  machineName = nameOfThisMachine;
	  osVersion = osVersionOfThisMachine;
	  cpuArchitecture = cpuTypeOfThisMachine;
  }

  bool MongoDBLayer::runDataStoreCommand(std::string const & cmd, PersistenceError & dbError) {
		// This API can only be supported in NoSQL data stores such as Redis, Cassandra etc.
		// Mongo doesn't have a way to do this.
		dbError.set("From Mongo data store: This API to run native data store commands is not supported in Mongo.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Mongo data store: This API to run native data store commands is not supported in Mongo. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "MongoDBLayer");
		return(false);
  }

  bool MongoDBLayer::runDataStoreCommand(uint32_t const & cmdType, std::string const & httpVerb,
		std::string const & baseUrl, std::string const & apiEndpoint, std::string const & queryParams,
		std::string const & jsonRequest, std::string & jsonResponse, PersistenceError & dbError) {
		// This API can only be supported in NoSQL data stores such as Cloudant, HBase etc.
		// Mongo doesn't have a way to do this.
		dbError.set("From Mongo data store: This API to run native data store commands is not supported in Mongo.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Mongo data store: This API to run native data store commands is not supported in Mongo. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "MongoDBLayer");
		return(false);
  }

  bool MongoDBLayer::runDataStoreCommand(std::vector<std::string> const & cmdList, std::string & resultValue, PersistenceError & dbError) {
		// This API can only be supported in Redis.
		// Mongo doesn't have a way to do this.
		dbError.set("From Mongo data store: This API to run native data store commands is not supported in Mongo.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Mongo data store: This API to run native data store commands is not supported in Mongo. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "MongoDBLayer");
		return(false);
  }

  // This method will get the data item from the store for a given key.
  // Caller of this method can also ask us just to find if a data item
  // exists in the store without the extra work of fetching and returning the data item value.
  bool MongoDBLayer::getDataItemFromStore(std::string const & storeIdString,
		  std::string const & keyDataString, bool const & checkOnlyForDataItemExistence,
		  bool const & skipDataItemExistenceCheck, unsigned char * & valueData,
		  uint32_t & valueSize, PersistenceError & dbError) {
		// Let us get this data item from the cache as it is.
		// Since there could be multiple data writers, we are going to get whatever is there now.
		// It is always possible that the value for the requested item can change right after
		// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
		// This action is performed on the Store Contents collection that takes the following name.
		// Collection name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
		string storeCollectionName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
		string errorMsg = "";
		string value = "";
		bool result = readMongoDocumentValue(string(MONGO_DPS_DB_NAME), storeCollectionName, keyDataString, value, errorMsg);

		if (errorMsg != "") {
			// Error message means something went wrong in the call we made above.
			// Unable to get the requested data item from the cache.
			dbError.set("Unable to access the K/V pair in Mongo with the StoreId " + storeIdString +
				". " + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			return(false);
		}

		// If the caller only wanted us to check for the data item existence, we can exit now.
		if (checkOnlyForDataItemExistence == true) {
			return(result);
		}

		// Caller wants us to fetch and return the data item value.
		// If the data item is not there, we can't do much at this point.
		if (result == false) {
		  // This data item doesn't exist. Let us raise an error.
		  // Requested data item is not there in the cache.
		  dbError.set("The requested data item doesn't exist in the StoreId " + storeIdString +
			 ".", DPS_DATA_ITEM_READ_ERROR);
		   return(false);
		} else {
			// In Mongo, when we put the K/V pair, we b64_encoded our binary value buffer and
			// stored it as a string. We must now b64_decode it back to binary value buffer.
			b64_decode(value, valueData, valueSize);

			if (valueData == NULL) {
				// We encountered malloc error inside the b64_decode method we called above.
				// Only then, we will have a null value data.
				// Unable to allocate memory to transfer the data item value.
				dbError.set("Unable to allocate memory to copy the data item value for the StoreId " +
					storeIdString + ".", DPS_GET_DATA_ITEM_MALLOC_ERROR);
				return(false);
			} else {
				// In the b64_decode method, we allocated a memory buffer and directly assigned it to
				// the caller provided pointer (valueData). It is the caller's responsibility to
				// free that buffer when it is no longer needed.
				return(true);
			}
		}
  }

  MongoDBLayerIterator * MongoDBLayer::newIterator(uint64_t store, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside newIterator for store id " << store, "MongoDBLayer");

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  // Ensure that a store exists for the given store id.
	  if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MongoDBLayer");
		  }

		  return(NULL);
	  }

	  // Get the general information about this store.
	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayer");
		  return(NULL);
	  }

	  // It is a valid store. Create a new iterator and return it to the caller.
	  MongoDBLayerIterator *iter = new MongoDBLayerIterator();
	  iter->store = store;
	  base64_decode(storeName, iter->storeName);
	  iter->hasData = true;
	  // Give this iterator access to our MongoDBLayer object.
	  iter->mongoDBLayerPtr = this;
	  iter->sizeOfDataItemKeysVector = 0;
	  iter->currentIndex = 0;
	  return(iter);
  }

  void MongoDBLayer::deleteIterator(uint64_t store, Iterator * iter, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside deleteIterator for store id " << store, "MongoDBLayer");

	  if (iter == NULL) {
		  return;
	  }

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  MongoDBLayerIterator *myIter = static_cast<MongoDBLayerIterator *>(iter);

	  // Let us ensure that the user wants to delete an iterator that really belongs to the store passed to us.
	  // This will handle user's coding errors where a wrong combination of store id and iterator is passed to us for deletion.
	  if (myIter->store != store) {
		  // User sent us a wrong combination of a store and an iterator.
		  dbError.set("A wrong iterator has been sent for deletion. This iterator doesn't belong to the StoreId " +
		  				storeIdString + ".", DPS_STORE_ITERATION_DELETION_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside deleteIterator, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_DELETION_ERROR, "MongoDBLayer");
		  return;
	  } else {
		  delete iter;
	  }
  }

  // This method will acquire a lock for any given generic/arbitrary identifier passed as a string..
  // This is typically used inside the createStore, createOrGetStore, createOrGetLock methods to
  // provide thread safety. There are other lock acquisition/release methods once someone has a valid store id or lock id.
  bool MongoDBLayer::acquireGeneralPurposeLock(string const & entityName) {
	  int32_t retryCnt = 0;

	  // '501' + 'entity name' + 'generic_lock' => 1
	  std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
	  uint64_t lockId = SPL::Functions::Utility::hashCode(genericLockKey);
	  ostringstream lockIdStr;
	  lockIdStr << lockId;
	  string tmpLockIdStr = lockIdStr.str();

	  // MongoDB data model:  Database-->Collection-->Documents
	  // Let us create or get a mongo collection structure for this general purpose lock.
	  // It will create the database and the collection only if they don't already exist.
	  // Create a bson_t structure on the heap.
	  bson_t *bDoc;
	  bson_error_t bErr;
	  mongoc_collection_t *mColl = NULL;
	  mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(),
		  (string(DPS_DL_META_DATA_DB) + string(GENERAL_PURPOSE_LOCK_TYPE)).c_str());

	  if (mColl == NULL) {
		  // Error in getting a collection structure within our DPS database in Mongo.
		  return(false);
	  }

	  // Create it only once at the very first time we come here.
	  if (genericLockIndexCreated == false) {
		  // Define a variable for the collection index structure.
		  mongoc_index_opt_t miOpt;
		  // Initialize this structure to default values.
		  mongoc_index_opt_init (&miOpt);
		  // Following setting of unique to false will not give an error while inserting a document with the
		  // same key of this index. We need this setting (false) to allow different K/V pairs to be inserted into this
		  // collection as well as the same K/V pair to be updated when needed.
		  miOpt.unique = false;
		  miOpt.name = (string(DPS_DL_META_DATA_DB) + string(GENERAL_PURPOSE_LOCK_TYPE)).c_str();
		  // Mongo DB supports document level expiration feature. Let us set the expiration time for this lock.
		  miOpt.expire_after_seconds = (int32_t)DPS_AND_DL_GET_LOCK_TTL;

		  // Add a document field name that will be used as a collection index. Value of 1 means ascending index.
		  bDoc = BCON_NEW("createdAt", BCON_INT32(1));
		  // Create the index for the Mongo collection that will hold this generic lock information.
		  mongoc_collection_create_index (mColl, bDoc, &miOpt, &bErr);
		  bson_destroy(bDoc);
		  genericLockIndexCreated = true;
	  }

	  // Try to get a lock for this generic entity.
	  while (1) {
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or the MongoDB removes it after the TTL value expires.
		// Make an attempt to create a document for this lock now.
		int64_t timeInMilliSeconds = (std::time(0) * 1000);
		// MongoDB will evict this lock if it continues to exist without being released beyond (currentTime + mOpt.expire_after_seconds).
		bDoc = BCON_NEW("createdAt", BCON_DATE_TIME(timeInMilliSeconds),
			"_id", BCON_UTF8(genericLockKey.c_str()), "v", BCON_UTF8(tmpLockIdStr.c_str()));

		if (mongoc_collection_insert (mColl, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == true) {
			bson_destroy(bDoc);
			mongoc_collection_destroy (mColl);
			return(true);
		}

		// bErr.message  will have the error string if needed to be displayed here.
		bson_destroy(bDoc);
		SPLAPPTRC(L_DEBUG, "General purpose lock acquisition error=" << string(bErr.message), "MongoDBLayer");
		// Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
			mongoc_collection_destroy (mColl);
			return(false);
		}

		/*
		// Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
		usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
			  (retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
		*/
		// Do a random wait before trying to create it.
		// This will give random value between 0 and 1.
		float64 rand = SPL::Functions::Math::random();
		// Let us wait for that random duration which is a partial second i.e. less than a second.
		SPL::Functions::Utility::block(rand);
	  }

	  mongoc_collection_destroy (mColl);
	  return(false);
  }

  void MongoDBLayer::releaseGeneralPurposeLock(string const & entityName) {
	  // '501' + 'entity name' + 'generic_lock' => 1
	  std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
	  mongoc_collection_t *mColl = NULL;
	  mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(),
		  (string(DPS_DL_META_DATA_DB) + string(GENERAL_PURPOSE_LOCK_TYPE)).c_str());

	  if (mColl == NULL) {
		  // Error in getting a collection structure within our DPS database in Mongo.
		  return;
	  }

	  bson_error_t bErr;
	  // Create a bson_t structure on the heap.
	  bson_t *bDoc;
	  bDoc = BCON_NEW("_id", BCON_UTF8(genericLockKey.c_str()));
	  // Delete the general purpose lock entry in the meta data collection.
	  mongoc_collection_remove (mColl, MONGOC_REMOVE_NONE, bDoc, NULL, &bErr);
	  bson_destroy(bDoc);
	  mongoc_collection_destroy (mColl);
  }

    // Senthil added this on Apr/06/2022.
    // This method will get multiple keys from the given store and
    // populate them in the caller provided list (vector).
    // Be aware of the time it can take to fetch multiple keys in a store
    // that has several tens of thousands of keys. In such cases, the caller
    // has to maintain calm until we return back from here.
   void MongoDBLayer::getKeys(uint64_t store, std::vector<unsigned char *> & keysBuffer, std::vector<uint32_t> & keysSize, int32_t keyStartPosition, int32_t numberOfKeysNeeded, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside getKeys for store id " << store, "MongoDBLayer");

       // Not implemented at this time. Simply return.
       return;
    } // End of getKeys method.


  MongoDBLayerIterator::MongoDBLayerIterator() {

  }

  MongoDBLayerIterator::~MongoDBLayerIterator() {

  }

  bool MongoDBLayerIterator::getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
  	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside getNext for store id " << store, "MongoDBLayerIterator");

	  // If the iteration already ended, do a quick return back to the caller.
	  // Another possibility we want to detect is whether the caller really passed the
	  // correct store id that belongs to this iterator object. If either of them
	  // is not in our favor, bail out right away.
	  if ((this->hasData == false) || (store != this->store)) {
		  return(false);
	  }

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();
	  string data_item_key = "";

	  // Ensure that a store exists for the given store id.
	  if (this->mongoDBLayerPtr->storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayerIterator");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MongoDBLayerIterator");
		  }

		  return(false);
	  }

	  // Ensure that this store is not empty at this time. (Size check on every iteration is a performance nightmare. Optimize it later.)
	  if (this->mongoDBLayerPtr->size(store, dbError) <= 0) {
		  dbError.set("Store is empty for the StoreId " + storeIdString + ".", DPS_STORE_EMPTY_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed with an empty store whose id is " << storeIdString << ". " << DPS_STORE_EMPTY_ERROR, "MongoDBLayerIterator");
		  return(false);
	  }

	  if (this->sizeOfDataItemKeysVector <= 0) {
		  // This is the first time we are coming inside getNext for store iteration.
		  // Let us get the available data item keys from this store.
		  this->dataItemKeys.clear();
		  string errorMsg = "";
		  string storeCollectionName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
		  bool mongoResult = this->mongoDBLayerPtr->getAllKeysInCollection(string(MONGO_DPS_DB_NAME),
			  storeCollectionName, this->dataItemKeys, errorMsg);

		  if (mongoResult == false) {
			  // Unable to get data item keys from the store.
			  dbError.set("Unable to get data item keys for the StoreId " + storeIdString +
					  ". " + errorMsg, DPS_GET_STORE_DATA_ITEM_KEYS_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to get data item keys for store id " << storeIdString <<
                 ". " << errorMsg << ". " << DPS_GET_STORE_DATA_ITEM_KEYS_ERROR, "MongoDBLayerIterator");
			  this->hasData = false;
			  return(false);
		  }

		  // We can see if we have any data in the store.
		  this->sizeOfDataItemKeysVector = this->dataItemKeys.size();
		  this->currentIndex = 0;

		  if (this->sizeOfDataItemKeysVector == 0) {
			  // This is an empty store at this time.
			  // Let us exit now.
			  this->hasData = false;
			  return(false);
		  }
	  }

	  // We have data item keys.
	  // Let us get the next available data.
	  data_item_key = this->dataItemKeys.at(this->currentIndex);
	  // Advance the data item key vector index by 1 for it to be ready for the next iteration.
	  this->currentIndex += 1;

	  if (this->currentIndex >= this->sizeOfDataItemKeysVector) {
		  // We have served all the available data to the caller who is iterating this store.
		  // There is no more data to deliver for subsequent iteration requests from the caller.
		  this->dataItemKeys.clear();
		  this->currentIndex = 0;
		  this->sizeOfDataItemKeysVector = 0;
		  this->hasData = false;
	  }

	  // Get this data item's value data and value size.
	  // data_item_key was obtained straight from the store contents collection, where it is
	  // already in the base64 encoded format.
	  bool result = this->mongoDBLayerPtr->getDataItemFromStore(storeIdString, data_item_key,
		false, false, valueData, valueSize, dbError);

	  if (result == false) {
		  // Some error has occurred in reading the data item value.
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to get data item from store id " << storeIdString << ". " << dbError.getErrorCode(), "MongoDBLayerIterator");
		  // We will disable any future action for this store using the current iterator.
		  this->hasData = false;
		  return(false);
	  }

	  // We are almost done once we take care of arranging to return the key name and key size.
	  // In order to support spaces in data item keys, we base64 encoded them before storing it in Mongo.
	  // Let us base64 decode it now to get the original data item key.
	  string base64_decoded_data_item_key;
	  this->mongoDBLayerPtr->base64_decode(data_item_key, base64_decoded_data_item_key);
	  data_item_key = base64_decoded_data_item_key;
	  keySize = data_item_key.length();
	  // Allocate memory for this key and copy it to that buffer.
	  keyData = (unsigned char *) malloc(keySize);

	  if (keyData == NULL) {
		  // This error will occur very rarely.
		  // If it happens, we will handle it.
		  // We will not return any useful data to the caller.
		  if (valueSize > 0) {
			  delete [] valueData;
			  valueData = NULL;
		  }

		  // We will disable any future action for this store using the current iterator.
		  this->hasData = false;
		  keySize = 0;
		  dbError.set("Unable to allocate memory for the keyData while doing the next data item iteration for the StoreId " +
		  				storeIdString + ".", DPS_STORE_ITERATION_MALLOC_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to allocate memory for the keyData while doing the next data item iteration for store id " <<
		     storeIdString << ". " << DPS_STORE_ITERATION_MALLOC_ERROR, "MongoDBLayerIterator");
		  return(false);
	  }

	  // Copy the raw key data into the allocated buffer.
	  memcpy(keyData, data_item_key.data(), keySize);
	  // We are done. We expect the caller to free the keyData and valueData buffers.
	  return(true);
  }

// =======================================================================================================
// Beyond this point, we have code that deals with the distributed locks that a SPL developer can
// create, remove,acquire, and release.
// =======================================================================================================
  uint64_t MongoDBLayer::createOrGetLock(std::string const & name, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock with a name " << name, "MongoDBLayer");
		string base64_encoded_name;
		base64_encode(name, base64_encoded_name);

	 	// Get a general purpose lock so that only one thread can
		// enter inside of this method at any given time with the same lock name.
	 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
	 		// Unable to acquire the general purpose lock.
	 		lkError.set("Unable to get a generic lock for creating a lock with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
	 		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to get a generic lock while creating a store lock named " <<
	 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "MongoDBLayer");
	 		// User has to retry again to create this distributed lock.
	 		return (0);
	 	}

		// 1) In our Mongo dps implementation, data item keys can have space characters.
		// Inside Mongo, all our lock names will have a mapping type indicator of
		// "5" at the beginning followed by the actual lock name.
		// '5' + 'lock name' => lockId
		std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
		uint64_t lockId = SPL::Functions::Utility::hashCode(lockNameKey);
		ostringstream lockIdStr;
		lockIdStr << lockId;
		string tmpLockIdStr = lockIdStr.str();

		// MongoDB data model:  Database-->Collection-->Documents
		// Let us create or get a mongo collection structure for this user defined lock.
		// It will create the database and the collection only if they don't already exist.
		mongoc_collection_t *mColl = NULL;
		mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), string(DPS_DL_META_DATA_DB).c_str());

		if (mColl == NULL) {
			// Error in getting a collection structure within our DPS database in Mongo.
			lkError.set("Unable to get the lockId for the lockName " + name +
				". Error in creating a Mongo DPS meta data collection.", DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to get the lockId for the lockName " <<
				name << ". Error in creating a Mongo DPS meta data collection. " << DL_GET_LOCK_ID_ERROR, "MongoDBLayer");
			releaseGeneralPurposeLock(base64_encoded_name);
			return (0);
		}

		// Create a bson_t structure on the heap.
		bson_t *bDoc;
		bson_error_t bErr;
		bDoc = BCON_NEW("_id", BCON_UTF8(lockNameKey.c_str()), "v", BCON_UTF8(tmpLockIdStr.c_str()));

		if (mongoc_collection_insert (mColl, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == false) {
			bson_destroy(bDoc);
			mongoc_collection_destroy (mColl);

			if (bErr.code == MONGO_DUPLICATE_KEY_FOUND) {
				// This lock already exists. We can simply return the lockId now.
				releaseGeneralPurposeLock(base64_encoded_name);
				return(lockId);
			} else {
				lkError.set("Unable to get the lockId for the lockName " + name +
					". Error in creating a user defined lock entry. " + string(bErr.message), DL_GET_LOCK_ID_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to get the lockId for the lockName " <<
					name << ". " << string(bErr.message) << ". " << DL_GET_LOCK_ID_ERROR, "MongoDBLayer");
				releaseGeneralPurposeLock(base64_encoded_name);
				return (0);
			}
		}

		bson_destroy(bDoc);
		// If we are here that means the Mongo document entry was created by the previous call.
		// We can go ahead and create the lock info entry now.
		// 2) Create the Lock Info
		//    '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdStr.str();  // LockId becomes the new key now.
		string lockInfoValue = string("0_0_0_") + base64_encoded_name;

		bDoc = BCON_NEW("_id", BCON_UTF8(lockInfoKey.c_str()), "v", BCON_UTF8(lockInfoValue.c_str()));

		if (mongoc_collection_insert (mColl, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == false) {
			bson_destroy(bDoc);
			// There was an error in creating the user defined lock.
			// Unable to create lockinfo details..
			// Problem in creating the "LockId:LockInfo" entry in the cache.
			lkError.set("Unable to create 'LockId:LockInfo' in the cache for a lock named " + name +
				". " + string(bErr.message), DL_LOCK_INFO_CREATION_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to create 'LockId:LockInfo' for a lock named " <<
				name << ". " << string(bErr.message) << ". " << DL_LOCK_INFO_CREATION_ERROR, "MongoDBLayer");
			// Delete the previous root entry we inserted.
			bDoc = BCON_NEW("_id", BCON_UTF8(lockNameKey.c_str()));
			mongoc_collection_remove (mColl, MONGOC_REMOVE_NONE, bDoc, NULL, &bErr);
			bson_destroy(bDoc);
			mongoc_collection_destroy (mColl);
			releaseGeneralPurposeLock(base64_encoded_name);
			return(0);
		} else {
			bson_destroy(bDoc);
			mongoc_collection_destroy (mColl);
			releaseGeneralPurposeLock(base64_encoded_name);
			return(lockId);
		}
  }

  bool MongoDBLayer::removeLock(uint64_t lock, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside removeLock for lock id " << lock, "MongoDBLayer");

		ostringstream lockId;
		lockId << lock;
		string lockIdString = lockId.str();

		// If the lock doesn't exist, there is nothing to remove. Don't allow this caller inside this method.
		if(lockIdExistsOrNot(lockIdString, lkError) == false) {
			if (lkError.hasError() == true) {
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "MongoDBLayer");
			} else {
				lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to find the lock with an id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "MongoDBLayer");
			}

			return(false);
		}

		// Before removing the lock entirely, ensure that the lock is not currently being used by anyone else.
		if (acquireLock(lock, 25, 40, lkError) == false) {
			// Unable to acquire the distributed lock.
			lkError.set("Unable to get a distributed lock for the LockId " + lockIdString + ".", DL_GET_DISTRIBUTED_LOCK_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to get a distributed lock for the lock id " << lockIdString << ". " << DL_GET_DISTRIBUTED_LOCK_ERROR, "MongoDBLayer");
			// User has to retry again to remove the lock.
			return(false);
		}

		// We ensured that this lock is not being used by anyone at this time.
		// We are safe to remove this distributed lock entirely.
		// Let us first get the lock name for this lock id.
		uint32_t lockUsageCnt = 0;
		int32_t lockExpirationTime = 0;
		std::string lockName = "";
		pid_t lockOwningPid = 0;

		if (readLockInformation(lockIdString, lkError, lockUsageCnt, lockExpirationTime, lockOwningPid, lockName) == false) {
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "MongoDBLayer");
			releaseLock(lock, lkError);
			// This is alarming. This will put this lock in a bad state. Poor user has to deal with it.
			return(false);
		}

		// Since we got back the lock name for the given lock id, let us remove the lock entirely.
		// '5' + 'lock name' => lockId
		std::string lockNameKey = DL_LOCK_NAME_TYPE + lockName;
		// MongoDB data model:  Database-->Collection-->Documents
		// Let us create or get a mongo collection structure for this user defined lock.
		// It will create the database and the collection only if they don't already exist.
		mongoc_collection_t *mColl = NULL;
		mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), string(DPS_DL_META_DATA_DB).c_str());

		if (mColl == NULL) {
			// Error in getting a collection structure within our DPS database in Mongo.
			lkError.set("Unable to get the lockId for the lockId " + lockIdString +
				". Error in creating a Mongo DPS meta data collection.", DL_GET_DISTRIBUTED_LOCK_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to get the lockId for the lockId " <<
				lockIdString << ". Error in creating a Mongo DPS meta data collection. " << DL_GET_DISTRIBUTED_LOCK_ERROR, "MongoDBLayer");
			releaseLock(lock, lkError);
			return (false);
		}

		// Create a bson_t structure on the heap.
		bson_t *bDoc;
		bson_error_t bErr;
		bDoc = BCON_NEW("_id", BCON_UTF8(lockNameKey.c_str()));
		bool result = mongoc_collection_remove (mColl, MONGOC_REMOVE_NONE, bDoc, NULL, &bErr);
		bson_destroy(bDoc);

		if (result == false) {
			// There was an error in deleting the user defined lock.
			lkError.set("Unable to remove the lock named " + lockIdString + ".", DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to remove the lock with an id " << lockIdString << ". Error=" <<
				string(bErr.message) << ". " << DL_LOCK_REMOVAL_ERROR, "MongoDBLayer");
			mongoc_collection_destroy (mColl);
			releaseLock(lock, lkError);
			return(false);
		}

		// Now remove the lockInfo entry.
		// '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;  // LockId becomes the new key now.
		bDoc = BCON_NEW("_id", BCON_UTF8(lockInfoKey.c_str()));
		result = mongoc_collection_remove (mColl, MONGOC_REMOVE_NONE, bDoc, NULL, &bErr);
		bson_destroy(bDoc);
		mongoc_collection_destroy (mColl);

		if (result == false) {
			// There was an error in deleting the user defined lock.
			lkError.set("Unable to remove the lock info for a lock named " + lockIdString + ".", DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to remove the lock info with an id " << lockIdString << ". Error=" <<
				string(bErr.message) << ". " << DL_LOCK_REMOVAL_ERROR, "MongoDBLayer");
			releaseLock(lock, lkError);
			return(false);
		}

		// We successfully removed the lock for a given lock id.
		// We acquired this lock at the top of this method and we have to release it.
		// We can delete the lock item itself now.
		releaseLock(lock, lkError);
		// Inside the release lock function we called in the previous statement, it makes a
		// call to update the lock info. That will obviously fail since we removed here everything about
		// this lock. Hence, let us not propagate that error and cause the user to panic.
		// Reset any error that may have happened in the previous operation of releasing the lock.
		lkError.reset();
		// Life of this lock ended completely with no trace left behind.
	    return(true);
  }

  bool MongoDBLayer::acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside acquireLock for lock id " << lock, "MongoDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();
	  int32_t retryCnt = 0;

	  // If the lock doesn't exist, there is nothing to acquire. Don't allow this caller inside this method.
	  if(lockIdExistsOrNot(lockIdString, lkError) == false) {
		  if (lkError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "MongoDBLayer");
		  } else {
			  lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to find a lock with an id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "MongoDBLayer");
		  }

		  return(false);
	  }

	  // We will first check if we can get this lock.
	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  time_t startTime, timeNow;
	  // Get the start time for our lock acquisition attempts.
	  time(&startTime);

	  // MongoDB data model:  Database-->Collection-->Documents
	  // Let us create or get a mongo collection structure for this lock acquisition entry.
	  // It will create the database and the collection only if they don't already exist.
	  mongoc_collection_t *mColl = NULL;
	  mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(),
		  (string(DPS_DL_META_DATA_DB) + string(DL_LOCK_TYPE) + lockIdString).c_str());

	  if (mColl == NULL) {
		  // Error in getting a collection structure within our DPS database in Mongo.
		  return(false);
	  }

	  // Define a variable for the collection index structure.
	  mongoc_index_opt_t miOpt;
	  // Initialize this structure to default values.
	  mongoc_index_opt_init (&miOpt);
	  // Following setting of unique to false will not give an error while inserting a document with the
	  // same key of this index. We need this setting (false) to allow different K/V pairs to be inserted into this
	  // collection as well as the same K/V pair to be updated when needed.
	  miOpt.unique = false;
	  miOpt.name = (string(DPS_DL_META_DATA_DB) + string(DL_LOCK_TYPE) + lockIdString).c_str();
	  // Mongo DB supports document level expiration feature. Let us set the expiration time for this lock to 0.
	  // When we set this to 0, then every document inserted into this collection must set their own
	  // expiration value via a document field named "expireAt".
	  miOpt.expire_after_seconds = 0;
	  // Create a bson_t structure on the heap.
	  bson_t *bDoc;
	  // Add a document field name that will be used as a collection index. Value of 1 means ascending index.
	  bDoc = BCON_NEW("expireAt", BCON_INT32(1));

	  bson_error_t bErr;
	  // Create the index for the Mongo collection that will hold this generic lock information.
	  mongoc_collection_create_index (mColl, bDoc, &miOpt, &bErr);
	  bson_destroy(bDoc);

	  // Try to get a lock for this lock acquisition entity.
	  while (1) {
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or the MongoDB removes it after the TTL value expires.
		// Make an attempt to create a document for this lock now.
		time_t new_lock_expiry_time = time(0) + (time_t)leaseTime;
		int64_t expiryTimeInMilliSeconds = (std::time(0) * 1000) + (leaseTime * 1000);
		// MongoDB will evict this lock if it continues to exist without being released beyond expiryTimeInMilliseconds.
		bDoc = BCON_NEW("expireAt", BCON_DATE_TIME(expiryTimeInMilliSeconds),
			"_id", BCON_UTF8(distributedLockKey.c_str()), "v", BCON_INT32(1));

		if (mongoc_collection_insert (mColl, MONGOC_INSERT_NONE, bDoc, NULL, &bErr) == true) {
			// We got the lock. We can return now.
			// Let us update the lock information now.
			if(updateLockInformation(lockIdString, lkError, 1, new_lock_expiry_time, getpid()) == true) {
				bson_destroy(bDoc);
				mongoc_collection_destroy (mColl);
				return(true);
			} else {
				// Some error occurred while updating the lock information.
				// It will be in an inconsistent state. Let us release the lock.
				releaseLock(lock, lkError);
			}
		}

		// bErr.message  will have the error string if needed to be displayed here.
		bson_destroy(bDoc);
		SPLAPPTRC(L_DEBUG, "User defined lock acquisition error=" << string(bErr.message), "MongoDBLayer");
		// Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
			  lkError.set("Unable to acquire the lock named " + lockIdString + " after maximum retries.", DL_GET_LOCK_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire a lock named " << lockIdString << " after maximum retries. " <<
			     DL_GET_LOCK_ERROR, "MongoDBLayer");
			  mongoc_collection_destroy (mColl);
			  // Our caller can check the error code and try to acquire the lock again.
			  return(false);
		  }

		  // Check if we have gone past the maximum wait time the caller was willing to wait in order to acquire this lock.
		  time(&timeNow);
		  if (difftime(startTime, timeNow) > maxWaitTimeToAcquireLock) {
			  lkError.set("Unable to acquire the lock named " + lockIdString + " within the caller specified wait time.", DL_GET_LOCK_TIMEOUT_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire the lock named " << lockIdString <<
			     " within the caller specified wait time." << DL_GET_LOCK_TIMEOUT_ERROR, "MongoDBLayer");
			  mongoc_collection_destroy (mColl);
			  // Our caller can check the error code and try to acquire the lock again.
			  return(false);
		  }

		/*
		// Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
		usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
			  (retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
		*/
		// Do a random wait before trying to create it.
		// This will give random value between 0 and 1.
		float64 rand = SPL::Functions::Math::random();
		// Let us wait for that random duration which is a partial second i.e. less than a second.
		SPL::Functions::Utility::block(rand);
	  } // End of the while loop

	  mongoc_collection_destroy (mColl);
	  return(false);
  }

  void MongoDBLayer::releaseLock(uint64_t lock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside releaseLock for lock id " << lock, "MongoDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();

	  // MongoDB data model:  Database-->Collection-->Documents
	  // Let us create or get a mongo collection structure for this user defined lock.
	  // It will create the database and the collection only if they don't already exist.
	  mongoc_collection_t *mColl = NULL;
	  mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(),
		  (string(DPS_DL_META_DATA_DB) + string(DL_LOCK_TYPE) + lockIdString).c_str());

	  if (mColl == NULL) {
		  // Error in getting a collection structure within our DPS database in Mongo.
		  lkError.set("Unable to release the lockId " + lockIdString +
			  ". Error in getting a Mongo DL collection.", DL_LOCK_RELEASE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside releaseLock, it failed to release the lockId " <<
			  lockIdString << ". Error in getting a Mongo DPS meta data collection. " << DL_LOCK_RELEASE_ERROR, "MongoDBLayer");
		  return;
	  }

	  // We can delete entire the distributed lock collection.
	  bson_error_t bErr;
	  bool result = mongoc_collection_drop(mColl, &bErr);

	  if (result == false) {
		  // There was an error in dropping the mongo collection holding the user defined lock.
		  lkError.set("Unable to release the lockId " + lockIdString +
			  ". Error in dropping the Mongo DL collection. " + string(bErr.message), DL_LOCK_RELEASE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside releaseLock, it failed to release the lockId " << lockIdString
			  << ". Error in dropping the Mongo DL collection. " <<
			  string(bErr.message) << ". " << DL_LOCK_RELEASE_ERROR, "MongoDBLayer");
		  mongoc_collection_destroy (mColl);
		  return;
	  }

	  mongoc_collection_destroy (mColl);
	  updateLockInformation(lockIdString, lkError, 0, 0, 0);
  }

  bool MongoDBLayer::updateLockInformation(std::string const & lockIdString,
	PersistenceError & lkError, uint32_t const & lockUsageCnt, int32_t const & lockExpirationTime, pid_t const & lockOwningPid) {
	  // Get the lock name for this lock.
	  uint32_t _lockUsageCnt = 0;
	  int32_t _lockExpirationTime = 0;
	  std::string _lockName = "";
	  pid_t _lockOwningPid = 0;

	  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "MongoDBLayer");
		  return(false);
	  }

	  // Let us update the lock information.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  ostringstream lockInfoValue;
	  lockInfoValue << lockUsageCnt << "_" << lockExpirationTime << "_" << lockOwningPid << "_" << _lockName;

	  // MongoDB data model:  Database-->Collection-->Documents
	  // Let us create or get a mongo collection structure for this user defined lock.
	  // It will create the database and the collection only if they don't already exist.
	  mongoc_collection_t *mColl = NULL;
	  mColl = mongoc_client_get_collection (mClient, string(MONGO_DPS_DB_NAME).c_str(), string(DPS_DL_META_DATA_DB).c_str());

	  if (mColl == NULL) {
		  // Error in getting a collection structure within our DPS database in Mongo.
		  lkError.set("Unable to update the lock info for a lock named " + _lockName +
			  ". Error in creating a Mongo DPS meta data collection.", DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed to update the lock info for a lock named " <<
				  _lockName << ". Error in creating a Mongo DPS meta data collection. " << DL_LOCK_INFO_UPDATE_ERROR, "MongoDBLayer");
		  return(false);
	  }

	  bson_t *bDoc = BCON_NEW("_id", BCON_UTF8(lockInfoKey.c_str()));
	  string tmpLockInfoValue = lockInfoValue.str();
	  bson_t *ubDoc = BCON_NEW("$set", "{", "v", BCON_UTF8(tmpLockInfoValue.c_str()), "}");
	  bson_error_t bErr;

	  if (mongoc_collection_update (mColl, MONGOC_UPDATE_NONE, bDoc, ubDoc, NULL, &bErr) == true) {
		  // We stored/updated the lock information.
		  bson_destroy(bDoc);
		  bson_destroy(ubDoc);
		  mongoc_collection_destroy (mColl);
		  return(true);
	  } else {
		  // There was an error in updating the lock information.
		  bson_destroy(bDoc);
		  bson_destroy(ubDoc);
		  mongoc_collection_destroy (mColl);
		  lkError.set("Critical Error1: Unable to update 'LockId:LockInfo' in the cache for a lock named " +
			  _lockName + ". " + string(bErr.message), DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Critical Error1: Inside updateLockInformation, it failed for a lock named " <<
			  _lockName << ". " << string(bErr.message) << ". " << DL_LOCK_INFO_UPDATE_ERROR, "MongoDBLayer");
		  return(false);
	  }
  }

  bool MongoDBLayer::readLockInformation(std::string const & lockIdString, PersistenceError & lkError, uint32_t & lockUsageCnt,
		  int32_t & lockExpirationTime, pid_t & lockOwningPid, std::string & lockName) {
	  // Read the contents of the lock information.
	  lockName = "";
	  std::string lockInfo = "";

	  // Lock Info contains meta data information about a given lock.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;

	  string errorMsg = "";
	  string value = "";

	  bool result = readMongoDocumentValue(string(MONGO_DPS_DB_NAME), string(DPS_DL_META_DATA_DB),
		  lockInfoKey, value, errorMsg);

	  if (result == true) {
		  // We got the value from the Mongo K/V store.
		  lockInfo = value;
	  } else {
		  // Unable to get the LockInfo from our cache.
		  lkError.set("Unable to get LockInfo using the LockId " + lockIdString +
			". " + string(errorMsg), DL_GET_LOCK_INFO_ERROR);
		  return(false);
	  }

	  // As shown in the comment line above, lock information is a string that has multiple pieces of
	  // information each separated by an underscore character. We are interested in all the three tokens (lock usage count, lock expiration time, lock name).
	  // Let us parse it now.
	  std::vector<std::string> words;
	  streams_boost::split(words, lockInfo, streams_boost::is_any_of("_"), streams_boost::token_compress_on);
	  int32_t tokenCnt = 0;
	  lockUsageCnt = 0;

	  for (std::vector<std::string>::iterator it = words.begin(); it != words.end(); ++it) {
		  string tmpString = *it;

		  switch(++tokenCnt) {
		  	  case 1:
				  if (tmpString.empty() == false) {
					  lockUsageCnt = streams_boost::lexical_cast<uint32_t>(tmpString.c_str());
				  }

		  		  break;

		  	  case 2:
				  if (tmpString.empty() == false) {
					  lockExpirationTime = streams_boost::lexical_cast<int32_t>(tmpString.c_str());
				  }

		  		  break;

		  	  case 3:
				  if (tmpString.empty() == false) {
					  lockOwningPid = streams_boost::lexical_cast<int32_t>(tmpString.c_str());
				  }

		  		  break;

		  	  case 4:
		  		  lockName = *it;
		  		  break;

		  	  default:
		  		  // If we keep getting more than 3 tokens, then it means that the lock name has
		  		  // underscore character(s) in it. e-g: Super_Duper_Lock.
		  		  lockName += "_" + *it;
		  }
	  }

	  if (lockName == "") {
		  // Unable to get the name of this lock.
		  lkError.set("Unable to get the lock name for lockId " + lockIdString + ".", DL_GET_LOCK_NAME_ERROR);
		  return(false);
	  }

	  return(true);
  }

  // This method will check if a lock exists for a given lock id.
  bool MongoDBLayer::lockIdExistsOrNot(string lockIdString, PersistenceError & lkError) {
	  string keyString = string(DL_LOCK_INFO_TYPE) + lockIdString;
	  string errorMsg = "";
	  string value = "";

	  bool result = readMongoDocumentValue(string(MONGO_DPS_DB_NAME), string(DPS_DL_META_DATA_DB),
		  keyString, value, errorMsg);

	  if (result == true) {
		  // LockId exists.
		  return(true);
	  } else if (result == false && errorMsg != "") {
		  // Unable to get the lock info for the given lock id.
		  lkError.set("LockIdExistsOrNot: Unable to get LockInfo for the lockId " + lockIdString +
			 ". " + errorMsg, DL_GET_LOCK_INFO_ERROR);
		  return(false);
	  } else {
		  return(false);
	  }
  }

  // This method will return the process id that currently owns the given lock.
  uint32_t MongoDBLayer::getPidForLock(string const & name, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside getPidForLock with a name " << name, "MongoDBLayer");

	  string base64_encoded_name;
	  base64_encode(name, base64_encoded_name);
	  std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
	  uint64_t lock = SPL::Functions::Utility::hashCode(lockNameKey);

	  // Read the lock information.
	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();

	  uint32_t _lockUsageCnt = 0;
	  int32_t _lockExpirationTime = 0;
	  std::string _lockName = "";
	  pid_t _lockOwningPid = 0;

	  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "MongoDBLayer");
		  return(0);
	  } else {
		  return(_lockOwningPid);
	  }
  }

  /*
  Following methods (is_b64, b64_encode and b64_decode) are modified versions of
  the code originally written by Rene Nyffenegger whose copyright statement is included below.

  Author's original source is available here: http://www.adp-gmbh.ch/cpp/common/base64.html
  Additional link where this code is discussed: http://stackoverflow.com/questions/180947/base64-decode-snippet-in-c

  Copyright (C) 2004-2008 René Nyffenegger

     This source code is provided 'as-is', without any express or implied
     warranty. In no event will the author be held liable for any damages
     arising from the use of this software.

     Permission is granted to anyone to use this software for any purpose,
     including commercial applications, and to alter it and redistribute it
     freely, subject to the following restrictions:

     1. The origin of this source code must not be mis-represented; you must not
        claim that you wrote the original source code. If you use this source code
        in a product, an acknowledgment in the product documentation would be
        appreciated but is not required.

     2. Altered source versions must be plainly marked as such, and must not be
        mis-represented as being the original source code.

     3. This notice may not be removed or altered from any source distribution.

     René Nyffenegger rene.nyffenegger@adp-gmbh.ch
  */
  // Is the given character one of the base64 characters?
  // Logic here is same as in the original one except for a minor change in the method name.
  inline bool MongoDBLayer::is_b64(unsigned char c) {
    return (isalnum(c) || (c == '+') || (c == '/'));
  }

  // Base64 encode the binary buffer contents passed by the caller and return a string representation.
  // There is no change to the original code in this method other than a slight change in the method name,
  // returning back right away when encountering an empty buffer and an array initialization for
  // char_array_4 to avoid a compiler warning.
  //
  void MongoDBLayer::b64_encode(unsigned char const * & buf, uint32_t const & bufLenOrg, string & ret) {
	uint32_t bufLen = bufLenOrg;
	// If the user wants to base64 encode an empty buffer, then we can return right here.
	if (bufLen == 0) {
		ret = "";
		return;
	}

    int i = 0;
    int j = 0;
    unsigned char char_array_3[3];
    unsigned char char_array_4[4] = {0x0};

    while (bufLen--) {
      char_array_3[i++] = *(buf++);
      if (i == 3) {
        char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
        char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
        char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
        char_array_4[3] = char_array_3[2] & 0x3f;

        for(i = 0; (i <4) ; i++)
          ret += base64_chars[char_array_4[i]];
        i = 0;
      }
    }

    if (i)
    {
      for(j = i; j < 3; j++)
        char_array_3[j] = '\0';

      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for (j = 0; (j < i + 1); j++)
        ret += base64_chars[char_array_4[j]];

      while((i++ < 3))
        ret += '=';
    }

    return;
  }

  // Base64 decode the string passed by the caller.
  // Then, allocate a memory buffer and transfer the decoded bytes there and assign it to the pointer passed by the caller.
  // In addition, assign the buffer length to the variable passed by the caller.
  // Modified the input argument types and the return value types.
  // Majority of the original logic is kept intact. Method name is slightly changed,
  // and I added more code at the end of the method to return an allocated memory buffer instead of an std::vector.
  //
  void MongoDBLayer::b64_decode(std::string & encoded_string, unsigned char * & buf, uint32_t & bufLen) {
	// IMPORTANT:
	// For performance reasons, we are not passing a const string to this method.
	// Instead, we are passing a directly modifiable reference. Caller should be aware that
	// the string they passed to this method gets altered during the b64 decoding logic below.
	// After this method returns back to the caller, it is not advisable to use that modified string.
    int in_len = encoded_string.size();

    if (in_len == 0) {
    	// User stored empty data item value in the cache.
		buf = (unsigned char *)"";
		bufLen = 0;
    	return;
    }

    int i = 0;
    int j = 0;
    int in_ = 0;
    unsigned char char_array_4[4], char_array_3[3];
    std::vector<unsigned char> ret;

	while (in_len-- && ( encoded_string[in_] != '=') && is_b64(encoded_string[in_])) {
		char_array_4[i++] = encoded_string[in_]; in_++;
		if (i ==4) {
			for (i = 0; i <4; i++)
				char_array_4[i] = base64_chars.find(char_array_4[i]);

			char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
			char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
			char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

			for (i = 0; (i < 3); i++)
				ret.push_back(char_array_3[i]);
			i = 0;
		}
	}


    if (i) {
      for (j = i; j <4; j++)
        char_array_4[j] = 0;

      for (j = 0; j <4; j++)
        char_array_4[j] = base64_chars.find(char_array_4[j]);

      char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
      char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
      char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

      for (j = 0; (j < i - 1); j++) ret.push_back(char_array_3[j]);
    }

    // Get the vector size.
    bufLen = ret.size();

    // Allocate a buffer, assign the allocated buffer to the user provided pointer and copy the bytes from the std::vector into that buffer.
    // Caller is responsible to free this buffer.
	buf = (unsigned char *) malloc(bufLen);

	if (buf == NULL) {
		// A very rare error in memory allocation.
		bufLen = 0;
		return;
	}

	int32_t idx = 0;
	// We expect the caller of this method to free the allocated buffer.
	// Copy the contents of the vector into the allocated buffer.
	for (std::vector<unsigned char>::iterator it = ret.begin(); it != ret.end(); ++it) {
		buf[idx++] = *it;
	}

    return;
  }

  // This method will read the value of a given key from a given mongo collection inside a given mongo database.
  bool MongoDBLayer::readMongoDocumentValue(string const & dbName, string const & collectionName,
	string const & key, string & value, string & errorMsg) {
	    errorMsg = "";
		mongoc_collection_t *mColl = NULL;
		mColl = mongoc_client_get_collection (mClient, dbName.c_str(), collectionName.c_str());

		if (mColl == NULL) {
			// Error in getting a collection structure within our DPS database in Mongo.
			errorMsg = "Unable to get a Mongo collection for " + collectionName;
			return (false);
		}

		// Create a bson_t structure on the heap.
		bson_t *bDoc;
		const bson_t *bDocResult;
		bDoc = BCON_NEW("_id", BCON_UTF8(key.c_str()));
		mongoc_cursor_t *mCursor;
		string mongo_value = "";
		bool dataItemFound = false;
		// Find it inside the mongo collection.
		mCursor = mongoc_collection_find (mColl, MONGOC_QUERY_NONE, 0, 0, 0, bDoc, NULL, NULL);

		// Iterate through the retrieved cursor and get the result as JSON.
	    while (mongoc_cursor_next (mCursor, &bDocResult)) {
	    	char *str = bson_as_json (bDocResult, NULL);

	    	if (str != NULL) {
	    		mongo_value = string(str);
	    		bson_free(str);
	    		dataItemFound = true;
	    	}

	    	// We want to read only one entry from this collection. We can break now.
	    	break;
	    }

	    bson_destroy(bDoc);
	    mongoc_cursor_destroy (mCursor);
	    mongoc_collection_destroy (mColl);

	    if (dataItemFound == false) {
	    	return(false);
	    }

	    // At this time, our value string should have JSON data in the format shown below:
	    // { "_id" : "Lock1501generic_lock", "createdAt" : { "$date" : 1418651391000 }, "v" : "3456789" }
	    // Our goal is to read the value part held by the key "v".
		char keyBuf[256];
		strcpy(keyBuf, "v");
		json_object *jo = json_tokener_parse(mongo_value.c_str());
		json_object *joForValue = NULL;
		const char *docValue = NULL;
		// Get the value object.
		json_bool exists = json_object_object_get_ex(jo, keyBuf, &joForValue);

		if (exists) {
			docValue = json_object_get_string(joForValue);
			value = string(docValue);
		}

		// Release it now.
		json_object_put(jo);

		if (docValue != NULL) {
			return(true);
		} else {
			return(false);
		}
  }

  // This method will get the size of a given mongo collection inside a given mongo database.
  bool MongoDBLayer::readMongoCollectionSize(string const & dbName,
     string const & collectionName, int64_t & size, string & errorMsg) {
	    errorMsg = "";
	    bson_error_t bErr;
		// Create a bson_t structure on the heap.
	    const bson_t *bDoc;
		mongoc_collection_t *mColl = NULL;
		mColl = mongoc_client_get_collection (mClient, dbName.c_str(), collectionName.c_str());

		if (mColl == NULL) {
			// Error in getting a collection structure within our DPS database in Mongo.
			errorMsg = "Unable to get a Mongo collection for " + collectionName;
			return (false);
		}

		// Set the bDoc to NULL to get the size of the entire collection and not just a particular bDoc inside that collection.
		bDoc = NULL;
		size = mongoc_collection_count (mColl, MONGOC_QUERY_NONE, bDoc, 0, 0, NULL, &bErr);
		mongoc_collection_destroy (mColl);

		if (size < 0) {
			errorMsg = string(bErr.message);
			return(false);
		} else {
			return(true);
		}
  }

  // This method returns all the document keys present in a given Mongo collection.
  bool MongoDBLayer::getAllKeysInCollection(string const & dbName, string const & collectionName,
	  std::vector<std::string> & dataItemKeys, string & errorMsg) {
	  errorMsg = "";
	  mongoc_collection_t *mColl = NULL;
	  mColl = mongoc_client_get_collection (mClient, dbName.c_str(), collectionName.c_str());

	  if (mColl == NULL) {
		  // Error in getting a collection structure within our DPS database in Mongo.
		  errorMsg = "Unable to get a Mongo collection for " + collectionName;
		  return (false);
	  }

	  // Create a bson_t structure on the heap.
	  bson_t *bDoc;
	  const bson_t *bDocResult;
	  mongoc_cursor_t *mCursor;
	  string tmpStr = "";
	  // Find it inside the mongo collection.
	  // Set the bDoc with empty JSON initialization to get all the documents in a collection.
	  bDoc = bson_new();
	  mCursor = mongoc_collection_find (mColl, MONGOC_QUERY_NONE, 0, 0, 0, bDoc, NULL, NULL);
	  bson_destroy(bDoc);

	  // Iterate through the retrieved cursor and get the result as JSON.
	  while (mongoc_cursor_next (mCursor, &bDocResult)) {
		  char *str = bson_as_json (bDocResult, NULL);

		  if (str == NULL) {
			  // There is a problem in BSON-->JSON.
			  // Something wrong. Let us abort from here.
			  mongoc_cursor_destroy (mCursor);
			  mongoc_collection_destroy (mColl);
			  errorMsg = "Unable to convert a K/V pair from BSON to JSON in the mongo collection " + collectionName;
			  return (false);
		  }

		  tmpStr = string(str);
		  bson_free(str);
		  // At this time, our value string should have JSON data in the format shown below:
		  // { "_id" : "Lock1501generic_lock", "createdAt" : { "$date" : 1418651391000 }, "v" : "3456789" }
		  // Our goal is to read the value part held by the key "v".
		  char keyBuf[256];
		  strcpy(keyBuf, "_id");
		  json_object *jo = json_tokener_parse(tmpStr.c_str());
		  json_object *joForValue = NULL;
		  const char *docValue = NULL;
		  // Get the value object.
		  json_bool exists = json_object_object_get_ex(jo, keyBuf, &joForValue);

		  if (exists) {
			  docValue = json_object_get_string(joForValue);
			  tmpStr = string(docValue);
		  }

		  // Release it now.
		  json_object_put(jo);

		  if (docValue != NULL) {
			  // Every dps store will have three mandatory reserved data item keys for internal use.
			  // Let us not add them to the list of docs (i.e. store keys).
			  if (tmpStr.compare(MONGO_STORE_ID_TO_STORE_NAME_KEY) == 0) {
				  continue; // Skip this one.
			  } else if (tmpStr.compare(MONGO_SPL_TYPE_NAME_OF_KEY) == 0) {
				  continue; // Skip this one.
			  } else if (tmpStr.compare(MONGO_SPL_TYPE_NAME_OF_VALUE) == 0) {
				  continue; // Skip this one.
			  }

			  dataItemKeys.push_back(tmpStr);
		  } else {
			  // Error in obtaining the key name.
			  mongoc_cursor_destroy (mCursor);
			  mongoc_collection_destroy (mColl);
			  errorMsg = "Unable to get a valid string object for a key name in the mongo collection " + collectionName;
			  return (false);
		  }
	  } // End of the while loop.

	  mongoc_cursor_destroy (mCursor);
	  mongoc_collection_destroy (mColl);
	  return(true);
  }

  // This method runs a simple database command.
  // As of Dec/2014, this method never got used anywhere. Hence, not enough testing was done to exercise this method.
  bool MongoDBLayer::runSimpleDatabaseCommand(string const & dbName, string const & command,
	  string const & cmdParam, string & errorMsg) {
	  bson_error_t bErr;
	  bson_t *bCmd;
	  bson_t *bDoc = NULL;
	  mongoc_database_t *mDatabase = NULL;
	  errorMsg = "";

	  mDatabase = mongoc_client_get_database (mClient, dbName.c_str());

	  if (mDatabase == NULL) {
		  errorMsg = "Unable to get a Mongo database object for " + dbName;
		  return(false);
	  }

	  bCmd = BCON_NEW(command.c_str(), BCON_UTF8(cmdParam.c_str()));
	  bool result = mongoc_database_command_simple(mDatabase, bCmd, NULL, bDoc, &bErr);

	  if (result == false) {
		  errorMsg = string(bErr.message);
	  }

	  bson_destroy(bCmd);
	  bson_destroy(bDoc);
	  mongoc_database_destroy(mDatabase);
	  return(result);
  }

  // This method runs a simple collection command.
  // As of Dec/2014, this method never got used anywhere. Hence, not enough testing was done to exercise this method.
  bool MongoDBLayer::runSimpleCollectionCommand(string const & dbName, string const & collectionName,
	  string const & command, string const & cmdParam, string & errorMsg) {
	  bson_error_t bErr;
	  bson_t *bCmd;
	  bson_t *bDoc = NULL;
	  mongoc_collection_t *mColl = NULL;
	  errorMsg = "";

	  mColl = mongoc_client_get_collection (mClient, dbName.c_str(), collectionName.c_str());

	  if (mColl == NULL) {
		  // Error in getting a collection structure within our DPS database in Mongo.
		  errorMsg = "Unable to get a Mongo collection object for " + collectionName;
		  return (false);
	  }


	  bCmd = BCON_NEW(command.c_str(), BCON_UTF8(cmdParam.c_str()));
	  bool result = mongoc_collection_command_simple(mColl, bCmd, NULL, bDoc, &bErr);

	  if (result == false) {
		  errorMsg = string(bErr.message);
	  }

	  bson_destroy(bCmd);
	  bson_destroy(bDoc);
	  mongoc_collection_destroy(mColl);
	  return(result);
  }

  // This method will return the status of the connection to the back-end data store.
  bool MongoDBLayer::isConnected() {
          // Not implemented at this time.
          return(true);
  }

  bool MongoDBLayer::reconnect(std::set<std::string> & dbServers, PersistenceError & dbError) {
          // Not implemented at this time.
          return(true);
  }

} } } } }

using namespace com::ibm::streamsx::store;
using namespace com::ibm::streamsx::store::distributed;
extern "C" {
	DBLayer * create(){
	   return new MongoDBLayer();
	}
}
