/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
/*
=====================================================================
Here is the copyright statement for our use of the libmemcached APIs:

libMemcached is shipped under the Berekely Software Distribution License (BSD).
The BSD license is a popular open source license which is permissive in nature.
You can include libMemcached with your product with no obligation to license the
technology or publish your own software under an open source license.

Contributions to libMemcached are accepted under the terms of the BSD license.
Companies such as Sun Microsystems, Twitter, Yahoo, GeoSolutions, and many others
have at one point or another contributed source code.

Copyright (c) 2006.., Brian Aker

All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.

    * Neither the name of the Brian Aker nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
=====================================================================
*/
/*
==================================================================================================================
This CPP file contains the source code for the distributed process store (dps) back-end activities such as
insert, update, read, remove, etc. Our dps implementation runs on top of the popular memcached in-memory store.
memcached is a simple, but a great open source effort that carries a BSD license.
Thanks to Brad Fitzpatrick, who created memcached, when he was 23 years old in 2003. What an amazing raw talent!!!
In a (2004) Linux Journal article, he nicely describes about the reason to start that marvelous project.

http://www.linuxjournal.com/node/7451/print

In our memcached store implementation for Streams, we are using APIs from the popular libmemcached C library.

Any dpsXXXXX native function call made from a SPL composite will go through a serialization layer and then hit
this CPP file. Here, the purpose of such SPL native function calls will be fulfilled using the memcached APIs.
After that, the results will be sent to a deserialization layer. From there, results will be transformed using the
correct SPL types and delivered back to the SPL composite. In general, our distributed process store provides
a "global + distributed" in-memory cache for different processes (multiple PEs from one or more Streams applications).
We provide a set of free for all native function APIs to create/read/update/delete data items on one or more stores.
In the worst case, there could be multiple writers and multiple readers for the same store.
It is important to note that a Streams application designer/developer should carefully address how different parts
of his/her application will access the store simultaneously i.e. who puts what, who gets what and at
what frequency from where etc.

This C++ project has a companion SPL project (058_data_sharing_between_non_fused_spl_custom_and_cpp_primitive_operators).
Please refer to the commentary in that SPL project file for learning about the procedure to do an
end-to-end test run involving the SPL code, serialization/deserialization code,
memcached interface code (this file), and your memcached infrastructure.

As a first step, you should run the ./mk script from the C++ project directory (DistributedProcessStoreLib).
That will take care of building the .so file for the dps and copy it to the SPL project's impl/lib directory.
==================================================================================================================
*/

#include "MemcachedDBLayer.h"
#include "DpsConstants.h"

#include <SPL/Runtime/Common/RuntimeDebug.h>

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
#include <streams_boost/algorithm/string/erase.hpp>
#include <streams_boost/algorithm/string/split.hpp>
#include <streams_boost/algorithm/string/classification.hpp>
#include <streams_boost/archive/iterators/base64_from_binary.hpp>
#include <streams_boost/archive/iterators/binary_from_base64.hpp>
#include <streams_boost/archive/iterators/transform_width.hpp>
#include <streams_boost/archive/iterators/insert_linebreaks.hpp>
#include <streams_boost/archive/iterators/remove_whitespace.hpp>

using namespace std;
using namespace streams_boost::archive::iterators;

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  MemcachedDBLayer::MemcachedDBLayer() 
  {

  }

  MemcachedDBLayer::~MemcachedDBLayer() 
  {
    // Clear the memcached connection.
	if (memc != NULL) {
		memcached_free(memc);
		memc = NULL;
	}
  }
        
  void MemcachedDBLayer::connectToDatabase(std::set<std::string> const & dbServers, PersistenceError & dbError)
  {
	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase", "MemcachedDBLayer");

	  // Get the name, OS version and CPU type of this machine.
	  struct utsname machineDetails;

	  if(uname(&machineDetails) < 0) {
		  dbError.set("Unable to get the machine/os/cpu details.", DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to get the machine/os/cpu details. " << DPS_INITIALIZE_ERROR, "MemcachedDBLayer");
		  return;
	  } else {
		  nameOfThisMachine = string(machineDetails.nodename);
		  osVersionOfThisMachine = string(machineDetails.sysname) + string(" ") + string(machineDetails.release);
		  cpuTypeOfThisMachine = string(machineDetails.machine);
	  }

	  // Create an empty memcached interface
	  memc = memcached_create(NULL);

	  if (memc == NULL) {
		  dbError.set("Unable to initialize the memcached structure.", DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error " << DPS_INITIALIZE_ERROR, "MemcachedDBLayer");
		  return;
	  }

	  // Let us turn off TCP Nagle algorithm for this connection.
	  memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_TCP_NODELAY, 1);
	  memcached_server_st *servers = NULL;
	  memcached_return_t rc;

	  // Let us add the memcached server names that are configured by the user.
	  for (std::set<std::string>::iterator it=dbServers.begin(); it!=dbServers.end(); ++it) {
		  // Append a server to the list
		  // If you specify the port number as 0, then it will use the default memcached port of 11211.
		  std::string serverName = *it;
		  servers = memcached_server_list_append(servers, serverName.c_str(), 0, &rc);
	  }

	  // Update the memcached structure with the updated server list
	  rc = memcached_server_push(memc, servers);

	  // We have now configured our memcached structure with one or more memcached servers.
	  // Let us check if the global storeId key:value pair is already there in the cache.
	  uint32_t flags = 0;
	  std::string key = DPS_AND_DL_GUID_KEY;

	  rc = memcached_exist(memc, key.c_str(), key.length());

	  // How can we check if there is a memcached server communication error?
	  // It could be that the user has specified a wrong machine name or that the
	  // memcached server daemon is not running on any of the specified machines.
	  // Under those conditions, it would have failed to do the key existence check above and
	  // returned an error. We can check for that failure below.
	  if ((rc != MEMCACHED_SUCCESS) && (rc != MEMCACHED_NOTFOUND)) {
		// This is how we can detect that a wrong memcached server name is configured by the user or
		// not even a single memcached server daemon being up and running.
		dbError.set("Unable to connect to the memcached server(s). " + std::string(memcached_strerror(memc, rc)), DPS_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error " << DPS_CONNECTION_ERROR, "MemcachedDBLayer");
		return;
	  }

	  if (rc == MEMCACHED_NOTFOUND) {
		  // It could be that our global store id is not there now.
		  // Let us create one with an initial value of 0.
		  // memcached_add is an atomic operation. It will succeed only for the very first operator that
		  // attempts to do this setting after a memcached server is started fresh. If some other operator
		  // already raced us ahead and created this guid_key, then our attempt below will be safely rejected.
		  std::string value_string = "0";
		  rc = memcached_add(memc, key.c_str(), key.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);
	  }

	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase done", "MemcachedDBLayer");
  }

  uint64_t MemcachedDBLayer::createStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside createStore for store " << name, "MemcachedDBLayer");
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);

 	// Get a general purpose lock so that only one thread can
	// enter inside of this method at any given time with the same store name.
 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
 		// Unable to acquire the general purpose lock.
 		dbError.set("Unable to get a generic lock for creating a store with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for an yet to be created store with its name as " <<
 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "MemcachedDBLayer");
 		// User has to retry again to create this store.
 		return 0;
 	}

    // Let us first see if a store with the given name already exists.
	// IMPORTANT: We don't support store names containing space characters. memcached will reject such keys.
	uint32_t flags = 0;
	// Inside memcached, all our store names will have a mapping type indicator of
	// "0" at the beginning followed by the actual store name.  "0" + 'store name'
	// (See the store layout description documented in the next page.)
	// Additionally, in memcached, we support store names to have space characters in them.
	std::string storeNameKey = DPS_STORE_NAME_TYPE + base64_encoded_name;
	memcached_return_t rc;

	rc = memcached_exist(memc, storeNameKey.c_str(), storeNameKey.length());

	if ((rc != MEMCACHED_SUCCESS) && (rc != MEMCACHED_NOTFOUND)) {
		dbError.set("Unable to connect to the memcached server(s). " + std::string(memcached_strerror(memc, rc)), DPS_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_CONNECTION_ERROR, "MemcachedDBLayer");
		releaseGeneralPurposeLock(base64_encoded_name);
		return 0;
	}

	if (rc == MEMCACHED_SUCCESS) {
		// This store already exists in our cache.
		// We can't create another one with the same name now.
		dbError.set("A store named " + name + " already exists", DPS_STORE_EXISTS);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_STORE_EXISTS, "MemcachedDBLayer");
		releaseGeneralPurposeLock(base64_encoded_name);
		return 0;
	}

	if (rc == MEMCACHED_NOTFOUND) {
		// Create a new store.
		// At first, let us increment our global dps_guid to reserve a new store id.
		uint64_t storeId = 0;
		std::string guid_key = DPS_AND_DL_GUID_KEY;
		rc = memcached_increment(memc, guid_key.c_str(), guid_key.length(), 1, &storeId);

		if (rc != MEMCACHED_SUCCESS) {
			dbError.set("Unable to get a unique store id for a store named " + name + ". " + std::string(memcached_strerror(memc, rc)), DPS_GUID_CREATION_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_GUID_CREATION_ERROR, "MemcachedDBLayer");
			releaseGeneralPurposeLock(base64_encoded_name);
			return 0;
		}

		if (rc == MEMCACHED_SUCCESS) {
			/*
			We secured a guid. We can now create this store. Layout for a distributed process store (dps) looks like this.
			****************************************************************************************************************************************************************
			* 1) Create a root entry called "Store Name":  '0' + 'store name' => 'store id'                                                                                *
			* 2) Create "Store Info": '1' + 'store id' => 'data item count' + '_^^_' + 'catalog segment count' + '_^^_' + 'last catalog segment size' + '_^^_' +           *
			*    'store name' + '_^^_' + 'key_spl_type_name' + '_^^_' + 'value_spl_type_name'                                                                              *
			* 3) Create "Store Catalog": '2' + 'store id' + '_' + 'segment index' => 'catalog info'                                                                        *
			* 4) SPL user specified information will be stored as "Data Item(s)": '3' + 'store id' + '_' + base64('key') => 'segment index' + '_' + 'value'                *
			* 5) In addition, we will also create and delete custom locks for modifying store catalog data in (2), and (3) above: '4' + 'store id' + 'dps_lock' => 1       *
			*                                                                                                                                                              *
			*                                                                                                                                                              *
			* 6) Create a root entry called "Lock Name":  '5' + 'lock name' ==> 'lock id'                                                                                  *
			* 7) Create "Lock Info":  '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'lock name' *
			* 8) In addition, we will also create and delete user-defined locks: '7' + 'lock id' + 'dl_lock' => 1                                                          *
			*                                                                                                                                                              *
			* 9) We will also allow general purpose locks to be created by any entity for sundry use:  '501' + 'entity name' + 'generic_lock' => 1                         *
			****************************************************************************************************************************************************************
			*/
			//
			// 1) Create the Store Name
			//    '0' + 'store name' => 'store id'
			std::ostringstream value;
			value << storeId;
			std::string value_string = value.str();

			rc = memcached_set(memc, storeNameKey.c_str(), storeNameKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

			if (rc != MEMCACHED_SUCCESS) {
				// Problem in creating the "Store Name" entry in the cache.
				dbError.set("Unable to create 'StoreName:StoreId' in the cache for a store named " + name + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_NAME_CREATION_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_STORE_NAME_CREATION_ERROR, "MemcachedDBLayer");
				// We are simply leaving an incremented value for the dps_guid key in the cache that will never get used.
				// Since it is harmless, there is no need to reduce this number by 1. It is okay that this guid number will remain unassigned to any store.
				releaseGeneralPurposeLock(base64_encoded_name);
				return 0;
			}

			// 2) Create the Store Info
			// '1' + 'store id' => 'data item count' + '_^^_' + 'catalog segment count' + '_^^_' + 'last catalog segment size' +
			// '_^^_' + 'store name' + '_^^_' + 'key_spl_type_name' + '_^^_' + 'value_spl_type_name'
			std::string storeInfoKey = DPS_STORE_INFO_TYPE + value_string;  // StoreId becomes the new key now.
			std::ostringstream value_stream;
			// Store name, keySplTypeName, and valueSplTypeName may contain underscore characters in them. Hence,
			// we will use '_^^_' as the field separator inside the store info metadata string.
			string delimiter = string(MEMCACHED_STORE_INFO_DELIMITER);
			string base64_encoded_keySplTypeName;
			base64_encode(keySplTypeName, base64_encoded_keySplTypeName);
			string base64_encoded_valueSplTypeName;
			base64_encode(valueSplTypeName, base64_encoded_valueSplTypeName);
			value_stream << "0" << delimiter << "1" << delimiter << "0" << delimiter <<
				base64_encoded_name << delimiter << base64_encoded_keySplTypeName << delimiter << base64_encoded_valueSplTypeName;
			value_string = value_stream.str();
			rc = memcached_set(memc, storeInfoKey.c_str(), storeInfoKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

			if (rc != MEMCACHED_SUCCESS) {
				// Problem in creating the "StoreId:StoreInfo" entry in the cache.
				dbError.set("Unable to create 'StoreId:StoreInfo' in the cache for the store named " + name + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_INFO_CREATION_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_STORE_INFO_CREATION_ERROR, "MemcachedDBLayer");
				// Delete the previous entry we made.
				memcached_delete(memc, storeNameKey.c_str(), storeNameKey.length(), (time_t)0);
				releaseGeneralPurposeLock(base64_encoded_name);
				return 0;
			}

			// We created the store.
			// Every store will have at least one catalog. Inside this catalog, we will maintain the
			// base64 encoded key names that belong to a given store in a long string as comma separated tokens.
			// Such store catalog information is very useful for data item enumeration, addition, removal etc.
			// memcached has a user-specified maximum limit for the size of a data item (via the -I command line option).
			// Default is 1MB and maximum is 128MB. In order to adhere to that data item size limit and
			// not to suffer performance issues in parsing a long single CSV string, we are going to store
			// multiple catalog segments of such CSV strings. Maximum number of bytes a catalog segment can
			// hold is determined by the DPS_MAX_CATALOG_SEGMENT_SIZE constant defined above.
			// Let us create the very first catalog segment and keep it ready for users to add and remove future data items.
			//
			// 3) Create the Store Catalog
			//    '2' + 'store id' + '_' + 'segment index' => 'catalog info'
			std::ostringstream storeCatalog;
			storeCatalog << DPS_STORE_CATALOG_TYPE << storeId << "_1";
			std::string storeCatalogKey = storeCatalog.str();
			value_string = ""; // Initialize with an empty string
			rc = memcached_set(memc, storeCatalogKey.c_str(), storeCatalogKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

			if (rc != MEMCACHED_SUCCESS) {
				// Problem in creating a store catalog in the cache.
				dbError.set("Unable to create StoreCatalog in the cache for the store named " + name + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_CATALOG_CREATION_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_STORE_CATALOG_CREATION_ERROR, "MemcachedDBLayer");
				// Delete the previous entries we made.
				memcached_delete(memc, storeNameKey.c_str(), storeNameKey.length(), (time_t)0);
				memcached_delete(memc, storeInfoKey.c_str(), storeInfoKey.length(), (time_t)0);
				releaseGeneralPurposeLock(base64_encoded_name);
				return 0;
			}

			SPLAPPTRC(L_DEBUG, "Inside createStore done for store " << name, "MemcachedDBLayer");
			releaseGeneralPurposeLock(base64_encoded_name);
			return (storeId);
		}
	}

	releaseGeneralPurposeLock(base64_encoded_name);
    return 0;
  }

  uint64_t MemcachedDBLayer::createOrGetStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	// We will rely on a method above this and another method below this to accomplish what is needed here.
	SPLAPPTRC(L_DEBUG, "Inside createOrGetStore for store " << name, "MemcachedDBLayer");
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
	// We can get the storeId by calling the method below and return the result to the caller.
	dbError.reset();
	return(findStore(name, dbError));
  }
                
  uint64_t MemcachedDBLayer::findStore(std::string const & name, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside findStore for store " << name, "MemcachedDBLayer");
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);

	// Let us first see if this store already exists.
	uint32_t flags = 0;
	// Inside memcache, all our store names will have a mapping type indicator of
	// "0" at the beginning followed by the actual store name.  "0" + 'store name'
	std::string storeNameKey = DPS_STORE_NAME_TYPE + base64_encoded_name;
	memcached_return_t rc;

	rc = memcached_exist(memc, storeNameKey.c_str(), storeNameKey.length());

	if ((rc != MEMCACHED_SUCCESS) && (rc != MEMCACHED_NOTFOUND)) {
		dbError.set("Unable to connect to the memcached server(s) for finding a store named " + name + ". " + std::string(memcached_strerror(memc, rc)), DPS_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed for store " << name << ". " << DPS_CONNECTION_ERROR, "MemcachedDBLayer");
		return 0;
	}

	if (rc == MEMCACHED_NOTFOUND) {
		// This store is not there in our cache.
		dbError.set("Store named " + name + " not found. " + std::string(memcached_strerror(memc, rc)), DPS_STORE_DOES_NOT_EXIST);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed for store " << name << ". " << DPS_STORE_DOES_NOT_EXIST, "MemcachedDBLayer");
		return 0;
	}

	// It is an existing store.
	// We can get the storeId and return it to the caller.
	size_t return_value_length;

	char *response  = memcached_get(memc, storeNameKey.c_str(), storeNameKey.length(),
		&return_value_length,
		&flags,
		&rc);

	if (rc != MEMCACHED_SUCCESS || response == NULL) {
		// Unable to get an existing store id from the cache.
		dbError.set("Unable to get the storeId from the cache for the storeName " + name + ". " + std::string(memcached_strerror(memc, rc)), DPS_GET_STORE_ID_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed for store " << name << ". " << DPS_GET_STORE_ID_ERROR, "MemcachedDBLayer");

		if (response != NULL) {
			free(response);
		}

		return(0);
	} else {
		uint64_t storeId = streams_boost::lexical_cast<uint64_t>(response);
		free(response);
		return(storeId);
	}

    return 0;
  }
        
  bool MemcachedDBLayer::removeStore(uint64_t store, PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside removeStore for store id " << store, "MemcachedDBLayer");
	ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MemcachedDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "MemcachedDBLayer");
		// User has to retry again to remove the store.
		return(false);
	}

	// We can Call the clear method first to delete all the data items that are cached in this store.
	clearStoreThatIsAlreadyLocked(store, dbError);

	if (dbError.hasError() == true) {
		// This store is going to be in a weird state.
		// Something terribly gone wrong in clearing the contents.
		// User should look at the dbError code and decide about a corrective action since
		// there may be data items belonging to this store are still hanging around in memory.
		//
		releaseStoreLock(storeIdString);
		return(false);
	}

	// Get rid of these three entries that are holding the store meta data information.
	// 1) Store Catalog
	// 2) Store Info
	// 3) Store Name
	//
	uint32_t dataItemCnt = 0;
	uint32_t catalogSegmentCnt = 0;
	uint32_t lastCatalogSegmentSize = 0;
	std::string storeName = "";
	string keySplTypeName = "";
	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, catalogSegmentCnt,
		lastCatalogSegmentSize, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		releaseStoreLock(storeIdString);
		// This is alarming. This will put this store in a bad state. Poor user has to deal with it.
		return(false);
	}

	// Let us delete all the catalogs present inside this store.
	// '2' + 'store id' + '_' + 'segment index' => 'catalog info'
	for (uint32_t cnt = 1; cnt <= catalogSegmentCnt; cnt++) {
		ostringstream storeCatalog;
		storeCatalog << DPS_STORE_CATALOG_TYPE << storeIdString << "_" << cnt;
		string storeCatalogKey = storeCatalog.str();
		memcached_delete(memc, storeCatalogKey.c_str(), storeCatalogKey.length(), (time_t)0);
	}

	// We can delete the StoreInfo key now.
	string storeInfoKey = DPS_STORE_INFO_TYPE + storeIdString;
	memcached_delete(memc, storeInfoKey.c_str(), storeInfoKey.length(), (time_t)0);

	// Finally, delete the StoreName key now.
	string storeNameKey = DPS_STORE_NAME_TYPE + storeName;
	memcached_delete(memc, storeNameKey.c_str(), storeNameKey.length(), (time_t)0);

	// Life of this store ended completely with no trace left behind.
	releaseStoreLock(storeIdString);
    return(true);
  }

  // This is a lean and mean put operation into a store.
  // It doesn't do any safety checks before putting a data item into a store.
  // If you want to go through that rigor, please use the putSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool MemcachedDBLayer::put(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize, 
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside put for store id " << store, "MemcachedDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	uint32_t flags = 0;
	memcached_return_t rc;
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In memcached, data item keys can't have space characters.
	// Hence, we have to base64 encode them.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	uint32_t catalogSegmentIndexOfAnExistingDataItem = 0;
	unsigned char *dummyValueData;
	uint32_t dummyValueSize;

	// Let us see if we already have this data item in our cache.
	// If it is there, we will get the catalog segment index at which it is already registered.
	bool dataItemAlreadyInCache = getCatalogSegmentIndexOrDataItemOrBoth(storeIdString, base64_encoded_data_item_key,
	  	  	  true, catalogSegmentIndexOfAnExistingDataItem, false, dummyValueData, dummyValueSize, dbError);

	if ((dataItemAlreadyInCache == false) && (dbError.getErrorCode() != DPS_DATA_ITEM_READ_ERROR)) {
		// DPS_DATA_ITEM_READ_ERROR is okay since it tells us that the data item key we are dealing with now is not there in the store.
		// If we got any other error, we have to exit from this method now.
		SPLAPPTRC(L_DEBUG, "Inside put, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		return(false);
	}

	dbError.reset();

	// Before we can store the data item value in the store, we have to do the following steps.
	// 1) Get the current catalog segment index of this store that may be used to register a NEW data item.
	// 2) We have to tag the catalog segment index as part of the data item value. This tagging of the
	//    catalog segment index to the data item value is required for meta data management during data item removal.
	uint32_t dataItemCnt = 0;
	uint32_t catalogSegmentCnt = 0;
	uint32_t lastCatalogSegmentSize = 0;
	string storeName = "";
	string keySplTypeName = "";
	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, catalogSegmentCnt,
		lastCatalogSegmentSize, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside put, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		return(false);
	}

	// If it is an existing data item in the store, then it's key is already registered as part of a catalog segment.
	// If it is a new data item being added, ensure that we have room in the
	// current catalog segment to store this data item's key for future bookkeeping.
	// If there is not enough room in the current catalog segment, then we have to create a new catalog segment.
	if (dataItemAlreadyInCache == false) {
		uint32_t catalogFreeSpaceNeeded = 0;

		if (lastCatalogSegmentSize == 0) {
			// This is an empty catalog segment. We only have to store the base64 encoded key.
			catalogFreeSpaceNeeded = base64_encoded_data_item_key.length();
		} else {
			// There are already entries in this catalog segment.
			// CSV demarcation needed to distinguish it from other existing keys.
			// We have to store "," + base64 encoded key. Add one more byte for the comma character.
			catalogFreeSpaceNeeded = base64_encoded_data_item_key.length() + 1;
		}

		// Check if there is enough room in the current catalog segment.
		if ((lastCatalogSegmentSize + catalogFreeSpaceNeeded) > DPS_MAX_CATALOG_SEGMENT_SIZE) {
			// There is not enough room in the current catalog segment.
			// Let us crate a new catalog segment.
			std::ostringstream newCatalogSegmentIndex;
			newCatalogSegmentIndex << catalogSegmentCnt+1;
			string storeCatalogKey = DPS_STORE_CATALOG_TYPE + storeIdString + "_" + newCatalogSegmentIndex.str();
			string value_string = ""; // New catalog segment will start off with empty contents ready for future additions.
			rc = memcached_set(memc, storeCatalogKey.c_str(), storeCatalogKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

			if (rc == MEMCACHED_SUCCESS) {
				// Let us move the current catalog segment indicator to point at the new one with a size of 0.
				catalogSegmentCnt++;
				lastCatalogSegmentSize = 0;
			} else {
				// Problem in creating a new store catalog segment in the cache.
				// We can't do much at this point.
				dbError.set("Unable to create a new catalog segment for the store named " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_CATALOG_SEGMENT_CREATION_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside put, it failed for store " << storeIdString << ". " << DPS_STORE_CATALOG_SEGMENT_CREATION_ERROR, "MemcachedDBLayer");
				return(false);
			}
		}
	}

	// We are ready to either store a new data item or update an existing data item.
	// "Data Item(s)": '3' + 'store id' + '_' + base64('key') => 'segment index' + '_' + 'value'
	std::ostringstream catalogSegmentIndex;

	if (dataItemAlreadyInCache == false) {
		// Use the current catalog segment for this store.
		catalogSegmentIndex << catalogSegmentCnt;
	} else {
		// It is an existing data item. Use its originally assigned catalog segment.
		catalogSegmentIndex << catalogSegmentIndexOfAnExistingDataItem;
	}

	string dataItemValue = catalogSegmentIndex.str() + "_";
	// We have to prefix the value data with the catalog segment index.
	// Let us create our own buffer to include the "catalog segment index + user specified data"
	unsigned char *extendedValueData = (unsigned char *)malloc(dataItemValue.length() + valueSize);

	if (extendedValueData == NULL) {
		dbError.set("Unable to allocate memory during put data item for the store id " + storeIdString + ".", DPS_PUT_DATA_ITEM_MALLOC_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed for store id " << storeIdString << ". " << DPS_PUT_DATA_ITEM_MALLOC_ERROR, "MemcachedDBLayer");
		return(false);
	}

	// We can do memcpy of our catalog segment index into the allocated buffer.
	memcpy(extendedValueData, dataItemValue.c_str(), dataItemValue.length());
	// We can do memcpy of the user-specified data item into the extended value data buffer.
	memcpy(&extendedValueData[dataItemValue.length()], valueData, valueSize);
	size_t newValueDataSize = dataItemValue.length() + valueSize;

	// Every data item's key must be prefixed with a common store type indicator followed by
	// its store id before being used for CRUD operation in the memcached store.
	string newKeyData = DPS_STORE_DATA_ITEM_TYPE + storeIdString + "_" + base64_encoded_data_item_key;
	// Store it in the cache now.
	rc = memcached_set(memc, newKeyData.c_str(), newKeyData.length(), (const char*)extendedValueData, newValueDataSize, (time_t)0, flags);
	free(extendedValueData);
	extendedValueData = NULL;

	if (rc != MEMCACHED_SUCCESS) {
		// Problem in storing a data item in the cache.
		dbError.set("Unable to store a data item in the store id " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed for store id " << storeIdString << ". " << DPS_DATA_ITEM_WRITE_ERROR, "MemcachedDBLayer");
		return(false);
	}

	// Successfully saved the data item in the cache.
	if(dataItemAlreadyInCache == true) {
		// We are done updating the data item that was already there in the cache.
		return(true);
	}

	// If this data item is being newly added to the cache, let us update the meta data about this store (size and catalog segment index).
	// Register the newly added data item's key by adding it to our current store catalog segment. This is an absolute must for doing store maintenance, when
	// more data items get added or deleted in this particular store.
	// Store catalogs take this format: '2' + 'store id' + '_' + 'segment index' => 'catalog info'
	std::string storeCatalogKey = DPS_STORE_CATALOG_TYPE + storeIdString + "_" + catalogSegmentIndex.str();
	// We want to append the new data item key to a catalog segment. Prefix it with a comma character only if it is NOT a brand new catalog segment.
	string value_string = "";

	if (lastCatalogSegmentSize > 0) {
		value_string = "," + base64_encoded_data_item_key;
	} else {
		value_string = base64_encoded_data_item_key;
	}

	rc = memcached_append(memc, storeCatalogKey.c_str(), storeCatalogKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

	if (rc != MEMCACHED_SUCCESS) {
		// Problem in updating the store catalog value.
		dbError.set("Unable to update StoreCatalog in the cache for the store named " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_CATALOG_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed for store " << storeIdString << ". " << DPS_STORE_CATALOG_WRITE_ERROR, "MemcachedDBLayer");
		// Delete the data item we added above.
		memcached_delete(memc, newKeyData.c_str(), newKeyData.length(), (time_t)0);
		return(false);
	}

	// Update the store meta data information about the current catalog.
	dataItemCnt++;
	lastCatalogSegmentSize += value_string.length();

	// We can refresh the store info meta data now with our updated catalog segment details.
	// '1' + 'store id' => 'data item count' + '_^^_' + 'catalog segment count' + '_^^_' + 'last catalog segment size' +
	// '_^^_' + 'store name' + '_^^_' + 'key_spl_type_name' + '_^^_' + 'value_spl_type_name'
	std::string storeInfoKey = DPS_STORE_INFO_TYPE + storeIdString;
	string delimiter = string(MEMCACHED_STORE_INFO_DELIMITER);
	ostringstream storeInfoValue;
	storeInfoValue << dataItemCnt << delimiter << catalogSegmentCnt << delimiter << lastCatalogSegmentSize <<
		delimiter << storeName << delimiter << keySplTypeName << delimiter << valueSplTypeName;
	value_string = storeInfoValue.str();

	rc = memcached_set(memc, storeInfoKey.c_str(), storeInfoKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

	if (rc != MEMCACHED_SUCCESS) {
		// Problem in updating the "StoreId:StoreInfo" entry in the cache.
		dbError.set("Unable to update 'StoreId:StoreInfo' in the cache for the store named " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_INFO_UPDATE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed for store " << storeIdString << ". " << DPS_STORE_INFO_UPDATE_ERROR, "MemcachedDBLayer");
		// This is not good for this error to happen. This puts this store in a dirty state with
		// incorrect meta data information about the total number of data items, current catalog segment size etc.
		// However we added the data item in the store except this error in updating the store meta data information.
		return(false);
	}

	return(true);
  }        

  // This is a special bullet proof version that does several safety checks before putting a data item into a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular put method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool MemcachedDBLayer::putSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside putSafe for store id " << store, "MemcachedDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	uint32_t flags = 0;
	memcached_return_t rc;
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MemcachedDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "MemcachedDBLayer");
		// User has to retry again to put the data item in the store.
		return(false);
	}

	// In memcached, data item keys can't have space characters.
	// Hence, we have to base64 encode them.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	uint32_t catalogSegmentIndexOfAnExistingDataItem = 0;
	unsigned char *dummyValueData;
	uint32_t dummyValueSize;

	// Let us see if we already have this data item in our cache.
	// If it is there, we will get the catalog segment index at which it is already registered.
	bool dataItemAlreadyInCache = getCatalogSegmentIndexOrDataItemOrBoth(storeIdString, base64_encoded_data_item_key,
	  	  	  true, catalogSegmentIndexOfAnExistingDataItem, false, dummyValueData, dummyValueSize, dbError);

	if ((dataItemAlreadyInCache == false) && (dbError.getErrorCode() != DPS_DATA_ITEM_READ_ERROR)) {
		// DPS_DATA_ITEM_READ_ERROR is okay since it tells us that the data item key we are dealing with now is not there in the store.
		// If we got any other error, we have to exit from this method now.
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	dbError.reset();

	// Before we can store the data item value in the store, we have to do the following steps.
	// 1) Get the current catalog segment index of this store that may be used to register a NEW data item.
	// 2) We have to tag the catalog segment index as part of the data item value. This tagging of the
	//    catalog segment index to the data item value is required for meta data management during data item removal.
	uint32_t dataItemCnt = 0;
	uint32_t catalogSegmentCnt = 0;
	uint32_t lastCatalogSegmentSize = 0;
	string storeName = "";
	string keySplTypeName = "";
	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, catalogSegmentCnt,
		lastCatalogSegmentSize, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// If it is an existing data item in the store, then it's key is already registered as part of a catalog segment.
	// If it is a new data item being added, ensure that we have room in the
	// current catalog segment to store this data item's key for future bookkeeping.
	// If there is not enough room in the current catalog segment, then we have to create a new catalog segment.
	if (dataItemAlreadyInCache == false) {
		uint32_t catalogFreeSpaceNeeded = 0;

		if (lastCatalogSegmentSize == 0) {
			// This is an empty catalog segment. We only have to store the base64 encoded key.
			catalogFreeSpaceNeeded = base64_encoded_data_item_key.length();
		} else {
			// There are already entries in this catalog segment.
			// CSV demarcation needed to distinguish it from other existing keys.
			// We have to store "," + base64 encoded key. Add one more byte for the comma character.
			catalogFreeSpaceNeeded = base64_encoded_data_item_key.length() + 1;
		}

		// Check if there is enough room in the current catalog segment.
		if ((lastCatalogSegmentSize + catalogFreeSpaceNeeded) > DPS_MAX_CATALOG_SEGMENT_SIZE) {
			// There is not enough room in the current catalog segment.
			// Let us crate a new catalog segment.
			std::ostringstream newCatalogSegmentIndex;
			newCatalogSegmentIndex << catalogSegmentCnt+1;
			string storeCatalogKey = DPS_STORE_CATALOG_TYPE + storeIdString + "_" + newCatalogSegmentIndex.str();
			string value_string = ""; // New catalog segment will start off with empty contents ready for future additions.
			rc = memcached_set(memc, storeCatalogKey.c_str(), storeCatalogKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

			if (rc == MEMCACHED_SUCCESS) {
				// Let us move the current catalog segment indicator to point at the new one with a size of 0.
				catalogSegmentCnt++;
				lastCatalogSegmentSize = 0;
			} else {
				// Problem in creating a new store catalog segment in the cache.
				// We can't do much at this point.
				dbError.set("Unable to create a new catalog segment for the store named " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_CATALOG_SEGMENT_CREATION_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store " << storeIdString << ". " << DPS_STORE_CATALOG_SEGMENT_CREATION_ERROR, "MemcachedDBLayer");
				releaseStoreLock(storeIdString);
				return(false);
			}
		}
	}

	// We are ready to either store a new data item or update an existing data item.
	// "Data Item(s)": '3' + 'store id' + '_' + base64('key') => 'segment index' + '_' + 'value'
	std::ostringstream catalogSegmentIndex;

	if (dataItemAlreadyInCache == false) {
		// Use the current catalog segment for this store.
		catalogSegmentIndex << catalogSegmentCnt;
	} else {
		// It is an existing data item. Use its originally assigned catalog segment.
		catalogSegmentIndex << catalogSegmentIndexOfAnExistingDataItem;
	}

	string dataItemValue = catalogSegmentIndex.str() + "_";
	// We have to prefix the value data with the catalog segment index.
	// Let us create our own buffer to include the "catalog segment index + user specified data"
	unsigned char *extendedValueData = (unsigned char *)malloc(dataItemValue.length() + valueSize);

	if (extendedValueData == NULL) {
		dbError.set("Unable to allocate memory during put data item for the store id " + storeIdString + ".", DPS_PUT_DATA_ITEM_MALLOC_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << DPS_PUT_DATA_ITEM_MALLOC_ERROR, "MemcachedDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// We can do memcpy of our catalog segment index into the allocated buffer.
	memcpy(extendedValueData, dataItemValue.c_str(), dataItemValue.length());
	// We can do memcpy of the user-specified data item into the extended value data buffer.
	memcpy(&extendedValueData[dataItemValue.length()], valueData, valueSize);
	size_t newValueDataSize = dataItemValue.length() + valueSize;

	// Every data item's key must be prefixed with a common store type indicator followed by
	// its store id before being used for CRUD operation in the memcached store.
	string newKeyData = DPS_STORE_DATA_ITEM_TYPE + storeIdString + "_" + base64_encoded_data_item_key;
	// Store it in the cache now.
	rc = memcached_set(memc, newKeyData.c_str(), newKeyData.length(), (const char*)extendedValueData, newValueDataSize, (time_t)0, flags);
	free(extendedValueData);
	extendedValueData = NULL;

	if (rc != MEMCACHED_SUCCESS) {
		// Problem in storing a data item in the cache.
		dbError.set("Unable to store a data item in the store id " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << DPS_DATA_ITEM_WRITE_ERROR, "MemcachedDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// Successfully saved the data item in the cache.
	if(dataItemAlreadyInCache == true) {
		// We are done updating the data item that was already there in the cache.
		releaseStoreLock(storeIdString);
		return(true);
	}

	// If this data item is being newly added to the cache, let us update the meta data about this store (size and catalog segment index).
	// Register the newly added data item's key by adding it to our current store catalog segment. This is an absolute must for doing store maintenance, when
	// more data items get added or deleted in this particular store.
	// Store catalogs take this format: '2' + 'store id' + '_' + 'segment index' => 'catalog info'
	std::string storeCatalogKey = DPS_STORE_CATALOG_TYPE + storeIdString + "_" + catalogSegmentIndex.str();
	// We want to append the new data item key to a catalog segment. Prefix it with a comma character only if it is NOT a brand new catalog segment.
	string value_string = "";

	if (lastCatalogSegmentSize > 0) {
		value_string = "," + base64_encoded_data_item_key;
	} else {
		value_string = base64_encoded_data_item_key;
	}

	rc = memcached_append(memc, storeCatalogKey.c_str(), storeCatalogKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

	if (rc != MEMCACHED_SUCCESS) {
		// Problem in updating the store catalog value.
		dbError.set("Unable to update StoreCatalog in the cache for the store named " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_CATALOG_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store " << storeIdString << ". " << DPS_STORE_CATALOG_WRITE_ERROR, "MemcachedDBLayer");
		// Delete the data item we added above.
		memcached_delete(memc, newKeyData.c_str(), newKeyData.length(), (time_t)0);
		releaseStoreLock(storeIdString);
		return(false);
	}

	// Update the store meta data information about the current catalog.
	dataItemCnt++;
	lastCatalogSegmentSize += value_string.length();

	// We can refresh the store info meta data now with our updated catalog segment details.
	// '1' + 'store id' => 'data item count' + '_^^_' + 'catalog segment count' + '_^^_' + 'last catalog segment size' +
	// '_^^_' + 'store name' + '_^^_' + 'key_spl_type_name' + '_^^_' + 'value_spl_type_name'
	std::string storeInfoKey = DPS_STORE_INFO_TYPE + storeIdString;
	string delimiter = string(MEMCACHED_STORE_INFO_DELIMITER);
	ostringstream storeInfoValue;
	storeInfoValue << dataItemCnt << delimiter << catalogSegmentCnt << delimiter << lastCatalogSegmentSize <<
		delimiter << storeName << delimiter << keySplTypeName << delimiter << valueSplTypeName;
	value_string = storeInfoValue.str();

	rc = memcached_set(memc, storeInfoKey.c_str(), storeInfoKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

	if (rc != MEMCACHED_SUCCESS) {
		// Problem in updating the "StoreId:StoreInfo" entry in the cache.
		dbError.set("Unable to update 'StoreId:StoreInfo' in the cache for the store named " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_INFO_UPDATE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store " << storeIdString << ". " << DPS_STORE_INFO_UPDATE_ERROR, "MemcachedDBLayer");
		// This is not good for this error to happen. This puts this store in a dirty state with
		// incorrect meta data information about the total number of data items, current catalog segment size etc.
		// However we added the data item in the store except this error in updating the store meta data information.
		releaseStoreLock(storeIdString);
		return(false);
	}

	releaseStoreLock(storeIdString);
	return(true);
  }

  // Put a data item with a TTL (Time To Live in seconds) value into the global area of the memcached DB.
  bool MemcachedDBLayer::putTTL(char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             uint32_t ttl, PersistenceError & dbError, bool encodeKey, bool encodeValue)
  {
	SPLAPPTRC(L_DEBUG, "Inside putTTL.", "MemcachedDBLayer");

	uint32_t flags = 0;
	memcached_return_t rc;

	// In memcached, data item keys can't have space characters.
	// Hence, we have to base64 encode them.
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

	// Set the TTL expiration value for this data item.
	time_t ttlValue = time(NULL);

	if (ttl > 0) {
		// Add current time to the user specified TTL in seconds to calculate the actual expiration time.
		// This requires all the Streams servers and the memcached servers to be clock synchronized.
		ttlValue += (time_t)ttl;
	} else {
		// User wants to use the dpsXXXXTTL APIs instead of the other store based APIs for the sake of simplicity.
		// In that case, we will let the user store in the global area for their K/V pair to remain forever or until it is deleted by the user.
		// No TTL effect needed here.
		ttlValue = (time_t)0;
	}

	// Store it in the cache now.
	rc = memcached_set(memc, base64_encoded_data_item_key.c_str(), base64_encoded_data_item_key.length(), (const char*)valueData, valueSize, ttlValue, flags);

	if (rc != MEMCACHED_SUCCESS) {
		// Problem in storing a data item in the cache.
		dbError.setTTL("Unable to store a data item with TTL. " + std::string(memcached_strerror(memc, rc)), DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putTTL, it failed for storing a data time. Error=" << std::string(memcached_strerror(memc, rc)) << ". " << DPS_DATA_ITEM_WRITE_ERROR, "MemcachedDBLayer");
		return(false);
	}

	return(true);
  }

  // This is a lean and mean get operation from a store.
  // It doesn't do any safety checks before getting a data item from a store.
  // If you want to go through that rigor, please use the getSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool MemcachedDBLayer::get(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside get for store id " << store, "MemcachedDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed in-memory store.
	// Every data item's key must be prefixed with its store id before being used for CRUD operation in the memcached store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In memcached, data item keys can't have space characters.
	// Hence, we have to base64 encode them.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	uint32_t dummyCatalogSegmentIndex;

	bool result = getCatalogSegmentIndexOrDataItemOrBoth(storeIdString, base64_encoded_data_item_key,
		false, dummyCatalogSegmentIndex, true, valueData, valueSize, dbError);

	if (result == false) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside get, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
	}

    return(result);
  }

  // This is a special bullet proof version that does several safety checks before getting a data item from a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular get method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool MemcachedDBLayer::getSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside getSafe for store id " << store, "MemcachedDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed in-memory store.
	// Every data item's key must be prefixed with its store id before being used for CRUD operation in the memcached store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MemcachedDBLayer");
		}

		return(false);
	}

	// In memcached, data item keys can't have space characters.
	// Hence, we have to base64 encode them.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	uint32_t dummyCatalogSegmentIndex;

	bool result = getCatalogSegmentIndexOrDataItemOrBoth(storeIdString, base64_encoded_data_item_key,
		false, dummyCatalogSegmentIndex, true, valueData, valueSize, dbError);

	if (result == false) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
	}

    return(result);
  }

  // Get a TTL based data item that is stored in the global area of the memcached DB.
  bool MemcachedDBLayer::getTTL(char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError, bool encodeKey)
  {
	SPLAPPTRC(L_DEBUG, "Inside getTTL.", "MemcachedDBLayer");

	uint32_t flags = 0;
	memcached_return_t rc;
	size_t return_value_length;
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

	char *response  = memcached_get(memc,
		base64_encoded_data_item_key.c_str(),
		base64_encoded_data_item_key.length(),
		&return_value_length,
		&flags,
		&rc);

	// If SUCCESS, this result can't come as empty with a NULL response. Because, at the minimum, catalog segment index must be present in the response.
	if (rc != MEMCACHED_SUCCESS || response == NULL) {
		// Unable to get the requested data item from the cache.
		dbError.setTTL("Error in getTTL: Unable to get the requested data item with TTL value. Error=" +
			std::string(memcached_strerror(memc, rc)), DPS_DATA_ITEM_READ_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside getTTL, it failed to get the requested data item with TTL value. Error=" <<
			std::string(memcached_strerror(memc, rc)) << ". " << DPS_DATA_ITEM_READ_ERROR, "MemcachedDBLayer");

		if (response != NULL) {
			free(response);
		}

		return(false);
	}

	if (return_value_length == 0) {
		// User stored empty data item value in the cache.
		valueData = NULL;
		valueSize = 0;
	} else {
		// We can allocate memory for the exact length of the data item value.
		valueSize = return_value_length;
		valueData = (unsigned char *) malloc(valueSize);

		if (valueData == NULL) {
			// Unable to allocate memory to transfer the data item value.
			dbError.setTTL("Error in getTTL: Unable to allocate memory to copy the TTL based data item value.", DPS_GET_DATA_ITEM_MALLOC_ERROR);
			// Free the response memory pointer handed to us.
			free(response);
			valueSize = 0;
			return(false);
		}

		// We expect the caller of this method to free the valueData pointer.
		memcpy(valueData, response, valueSize);
	}

	free(response);
	return(true);
  }

  bool MemcachedDBLayer::remove(uint64_t store, char const * keyData, uint32_t keySize,
                                PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside remove for store id " << store, "MemcachedDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MemcachedDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "MemcachedDBLayer");
		// User has to retry again to remove the data item from the store.
		return(false);
	}

	// Before deleting, we have to find out the catalog segment in which we stored the meta data information for this data item key.
	// Every data item's key is prefixed with a type indicator and its store id before being used for CRUD operation in the memcached store.
	// "Data Item(s)": '3' + 'store id' + '_' + base64('key') => 'segment index' + '_' + 'value'
	memcached_return_t rc;
	uint32_t catalogSegmentForThisDataItem = 0;
	unsigned char *valueData;
	uint32_t valueSize = 0;

	// In memcached, data item keys can't have space characters.
	// Hence, we have to base64 encode them.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	// Get this data item's store catalog segment index and value size.
	bool result = getCatalogSegmentIndexOrDataItemOrBoth(storeIdString, base64_encoded_data_item_key,
		true, catalogSegmentForThisDataItem, true, valueData, valueSize, dbError);

	if (result == false) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// If the result is successful, we are required to free the valueData memory block returned to us.
	// To remove this entry, we will only need the valueSize and not the valueData.
	if(valueSize > 0) {
		delete [] valueData;
	}

	if (catalogSegmentForThisDataItem == 0) {
		// Unable to get the catalog segment index for the given data item.
		dbError.set("Unable to get catalog segment index from the cache for StoreId " + storeIdString + ".", DPS_GET_DATA_ITEM_CATALOG_INDEX_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_GET_DATA_ITEM_CATALOG_INDEX_ERROR, "MemcachedDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// Let us delete this data item from the cache.
	// Every data item's key is prefixed with type indicator and its store id before being used for CRUD operation in the memcached store.
	// '3' + 'store id' + '_' + base64('key') => 'segment index' + '_' + 'value'
	string newKeyData = DPS_STORE_DATA_ITEM_TYPE + storeIdString + "_" + base64_encoded_data_item_key;
	rc = memcached_delete(memc, newKeyData.c_str(), newKeyData.length(), (time_t)0);

	if (rc != MEMCACHED_SUCCESS) {
		// Problem in deleting the requested data item from the store.
		dbError.set("Unable to remove the requested data item from the store for the store id " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_DATA_ITEM_DELETE_ERROR, "MemcachedDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	uint32_t flags = 0;
	size_t return_value_length;
	// We have to now remove the recently deleted data item's key stored in the store catalog's meta data.
	std::ostringstream catalogSegmentIndex;
	catalogSegmentIndex << catalogSegmentForThisDataItem;
	string storeCatalogKey = DPS_STORE_CATALOG_TYPE + storeIdString + "_" + catalogSegmentIndex.str();

	char *response  = memcached_get(memc, storeCatalogKey.c_str(), storeCatalogKey.length(),
		&return_value_length,
		&flags,
		&rc);

	// This result can come as empty with a NULL response, when a given catalog segment is empty. (But, can it really happen when we deleted a data item just now?)
	if (rc != MEMCACHED_SUCCESS) {
		// Unable to get the requested data item from the cache.
		dbError.set("Unable to get the store catalog information for the StoreId " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_CATALOG_READ_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_STORE_CATALOG_READ_ERROR, "MemcachedDBLayer");
		// This is an unexpected error. It is going to cause us trouble by making this store inconsistent.
		releaseStoreLock(storeIdString);
		return(false);
	}

	bool keyRemovedWithComma = false;
	bool keyRemovedFromCatalogSegment = false;

	// Since we just now deleted a data item, response should be a non-NULL.
	// As a pointer safe practice, do this only when there is a non-null response.
	if (response != NULL) {
		string keysInThisStoreCatalogSegment = string(response, return_value_length);
		free(response);
		keyRemovedFromCatalogSegment = true;

		// Let us erase our data item key in the catalog segment.
		// There are these possibilities.
		// It is a very last entry in a list of multiple keys separated by a comma or stuck in the middle between others: x,y,z,OurKey   (OR)  x,y,OurKey,z
		// It is a very first entry followed by others:  OurKey,x,y,z
		// It is a lonely entry: OurKey
		// We will erase them in the order stated above.
		if (keysInThisStoreCatalogSegment.find("," + base64_encoded_data_item_key) != string::npos) {
			streams_boost::erase_all(keysInThisStoreCatalogSegment, "," + base64_encoded_data_item_key);
			keyRemovedWithComma = true;
		} else if (keysInThisStoreCatalogSegment.find(base64_encoded_data_item_key + ",") != string::npos) {
			streams_boost::erase_all(keysInThisStoreCatalogSegment, base64_encoded_data_item_key + ",");
			keyRemovedWithComma = true;
		} else if (keysInThisStoreCatalogSegment.find(base64_encoded_data_item_key) != string::npos) {
			streams_boost::erase_all(keysInThisStoreCatalogSegment, base64_encoded_data_item_key);
			keyRemovedWithComma = false;
		} else {
			// Our key was not found in the catalog segment. That is not very comforting.
			keyRemovedFromCatalogSegment = false;
		}

		if (keyRemovedFromCatalogSegment == true) {
			// Let us store this catalog segment back in the cache now.
			rc = memcached_set(memc, storeCatalogKey.c_str(), storeCatalogKey.length(),
				keysInThisStoreCatalogSegment.c_str(), keysInThisStoreCatalogSegment.length(), (time_t)0, flags);

			if (rc != MEMCACHED_SUCCESS) {
				// Problem in updating the store catalog segment in the cache.
				dbError.set("Unable to update the store catalog segment in the cache for the store id " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_CATALOG_WRITE_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_STORE_CATALOG_WRITE_ERROR, "MemcachedDBLayer");
				// We are leaving the size of the store and the store catalog in an inconsistent state due to this error.
				releaseStoreLock(storeIdString);
				return(false);
			}
		}
	}

	// Let us now decrement the store size by 1.
	uint32_t dataItemCnt = 0;
	uint32_t catalogSegmentCnt = 0;
	uint32_t lastCatalogSegmentSize = 0;
	std::string storeName = "";
	string keySplTypeName = "";
	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, catalogSegmentCnt,
		lastCatalogSegmentSize, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		// This is another crazy error situation. We actually removed the data item. But, it failed to fetch the store information for internal store bookkeeping.
		releaseStoreLock(storeIdString);
		return(false);
	}

	dataItemCnt--;

	if ((catalogSegmentForThisDataItem == catalogSegmentCnt) && (keyRemovedFromCatalogSegment == true)) {
		// This data item happened to be in the very last catalog segment index,
		// let us adjust the size of the very last catalog segment.
		if (keyRemovedWithComma == true) {
			// Subtract the value size + 1 byte for the comma character.
			lastCatalogSegmentSize -= (valueSize + 1);
		} else {
			// Subtract the value size.
			lastCatalogSegmentSize-= valueSize;
		}
	}

	// We can refresh the store info meta data now with our updated catalog segment details.
	// '1' + 'store id' => 'data item count' + '_^^_' + 'catalog segment count' + '_^^_' + 'last catalog segment size' +
	// '_^^_' + 'store name' + '_^^_' + 'key_spl_type_name' + '_^^_' + 'value_spl_type_name'
	std::string storeInfoKey = DPS_STORE_INFO_TYPE + storeIdString;
	string delimiter = string(MEMCACHED_STORE_INFO_DELIMITER);
	ostringstream storeInfoValue;
	storeInfoValue << dataItemCnt << delimiter << catalogSegmentCnt << delimiter << lastCatalogSegmentSize <<
		delimiter << storeName << delimiter << keySplTypeName << delimiter << valueSplTypeName;
	string value_string = storeInfoValue.str();

	rc = memcached_set(memc, storeInfoKey.c_str(), storeInfoKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

	if (rc != MEMCACHED_SUCCESS) {
		// Problem in updating the "StoreId:StoreInfo" entry in the cache.
		dbError.set("Unable to update 'StoreId:StoreInfo' in the cache for the store named " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_INFO_UPDATE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store " << storeIdString << ". " << DPS_STORE_INFO_UPDATE_ERROR, "MemcachedDBLayer");
		// This is not good for this error to happen. This puts this store in a dirty state with
		// incorrect meta data information about the total number of data items, current catalog segment size etc.
		// However we removed the data item in the store except this error in updating the store meta data information.
		releaseStoreLock(storeIdString);
		return(false);
	}

	// In case, if we failed to remove the key from the catalog segment, raise that as an error.
	if (keyRemovedFromCatalogSegment == false) {
		// This is a situation where we deleted the actual data item, but failed to unregister from its catalog segment.
		// That puts this store in an inconsistent state. Inform this condition to the caller.
		dbError.set("Unable to remove a deleted data item key from its catalog segment for the store named " + storeIdString + ".", DPS_STORE_KEY_NOT_FOUND_IN_CATALOG_AFTER_DELETION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store " << storeIdString << ". " << DPS_STORE_KEY_NOT_FOUND_IN_CATALOG_AFTER_DELETION_ERROR, "MemcachedDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	releaseStoreLock(storeIdString);
    return(true);
  }

  // Remove a TTL based data item that is stored in the global area of the memcached DB.
  bool MemcachedDBLayer::removeTTL(char const * keyData, uint32_t keySize,
                                PersistenceError & dbError, bool encodeKey)
  {
	SPLAPPTRC(L_DEBUG, "Inside removeTTL.", "MemcachedDBLayer");

	// In memcached, data item keys can't have space characters.
	// Hence, we have to base64 encode them.
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

	memcached_return_t rc;
	rc = memcached_delete(memc, base64_encoded_data_item_key.c_str(), base64_encoded_data_item_key.length(), (time_t)0);

	if (rc != MEMCACHED_SUCCESS) {
		// Problem in deleting the requested data item from the store.
		dbError.setTTL("Unable to remove the requested data item with TTL. " + std::string(memcached_strerror(memc, rc)), DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside removeTTL, it failed to remove the TTL based data item. Error=" << std::string(memcached_strerror(memc, rc)) << ". " << DPS_DATA_ITEM_DELETE_ERROR, "MemcachedDBLayer");
		return(false);
	}

	return(true);
  }

  bool MemcachedDBLayer::has(uint64_t store, char const * keyData, uint32_t keySize,
                             PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside has for store id " << store, "MemcachedDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MemcachedDBLayer");
		}

		return(false);
	}

	// Every data item's key is prefixed with a type indicator and its store id before being used for CRUD operation in the memcached store.
	// "Data Item(s)": '3' + 'store id' + '_' + base64('key') => 'segment index' + '_' + 'value'
	memcached_return_t rc;
	std::string keyDataString;
	// Let us base64 encode the data item's key.
	base64_encode(std::string(keyData, keySize), keyDataString);
	std::string newKeyData = DPS_STORE_DATA_ITEM_TYPE + storeIdString + "_" + keyDataString;

	// Let us see if we already have this data item in our cache.
	bool dataItemAlreadyInCache = true;
	rc = memcached_exist(memc, newKeyData.c_str(), newKeyData.length());

	if ((rc != MEMCACHED_SUCCESS) && (rc != MEMCACHED_NOTFOUND)) {
		dbError.set("Unable to connect to the memcached server(s). " + std::string(memcached_strerror(memc, rc)), DPS_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeId.str() << ". " << DPS_CONNECTION_ERROR, "MemcachedDBLayer");
		return(false);
	}

	if (rc == MEMCACHED_NOTFOUND) {
		dataItemAlreadyInCache = false;
	}

	return(dataItemAlreadyInCache);
  }

  // Check for the existence of a TTL based data item that is stored in the global area of the memcached DB.
  bool MemcachedDBLayer::hasTTL(char const * keyData, uint32_t keySize,
                             PersistenceError & dbError, bool encodeKey)
  {
	SPLAPPTRC(L_DEBUG, "Inside hasTTL.", "MemcachedDBLayer");

	memcached_return_t rc;
	std::string keyDataString;
	// Let us base64 encode the data item's key.

        if (encodeKey == true) {
	   base64_encode(string(keyData, keySize), keyDataString);
        } else {
            // Since the key data sent here will always be in the network byte buffer format (NBF), 
            // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
            // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
            // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
            if ((uint8_t)keyData[0] < 0x80) {
               // Skip the first length byte. 
               keyDataString = string(&keyData[1], keySize-1);  
            } else {
               // Skip the five bytes at the beginning that represent the length of the key data.
               keyDataString = string(&keyData[5], keySize-5);
            }
        }

	rc = memcached_exist(memc, keyDataString.c_str(), keyDataString.length());

	if ((rc != MEMCACHED_SUCCESS) && (rc != MEMCACHED_NOTFOUND)) {
		dbError.setTTL("Unable to connect to the memcached server(s). Error=" + std::string(memcached_strerror(memc, rc)), DPS_CONNECTION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside hasTTL, it failed while checking for the existence of a TTL based data item. Error=" <<
			std::string(memcached_strerror(memc, rc)) << ". " << DPS_CONNECTION_ERROR, "MemcachedDBLayer");
		return(false);
	}

	if (rc == MEMCACHED_NOTFOUND) {
		return(false);
	} else {
		return(true);
	}
  }

  void MemcachedDBLayer::clear(uint64_t store, PersistenceError & dbError) {
     SPLAPPTRC(L_DEBUG, "Inside clear for store id " << store, "MemcachedDBLayer");

 	ostringstream storeId;
 	storeId << store;
 	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MemcachedDBLayer");
		}

		return;
	}

 	// Lock the store first.
 	if (acquireStoreLock(storeIdString) == false) {
 		// Unable to acquire the store lock.
 		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "MemcachedDBLayer");
 		// User has to retry again to remove the store.
 		return;
 	}

 	clearStoreThatIsAlreadyLocked(store, dbError);

 	// If there was an error set inside the method we called above, then this store is going to be in a weird state.
	// If that happened, something terribly gone wrong in clearing the contents.
	// User should look at the dbError code and decide about a corrective action since
	// there may be data items belonging to this store are still hanging around in memory.
 	releaseStoreLock(storeIdString);
  }

  // This method MUST only be entered after the caller acquires a store lock.
  // Otherwise, we will run into problems with other clients trying to do write operations on the store meta data.
  // Don't call this without owning a store lock.
  void MemcachedDBLayer::clearStoreThatIsAlreadyLocked(uint64_t store, PersistenceError & dbError)
  {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// We are going to clear all the entries in the given store.
	// We are maintaining all the names of the data items cached in this store as part of the store catalog.
	// For efficiency reasons, entire store catalog is split into multiple segments.
	// We can iterate through all the names of the data items and delete them.
	uint32_t dataItemCnt = 0;
	uint32_t catalogSegmentCnt = 0;
	uint32_t lastCatalogSegmentSize = 0;
	std::string storeName = "";
	string keySplTypeName = "";
	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, catalogSegmentCnt,
		lastCatalogSegmentSize, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside clearStoreThatIsAlreadyLocked, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		return;
	}

	uint32_t flags = 0;
	memcached_return_t rc;
	size_t return_value_length;

	// Let us fetch each of the catalog segment details and delete all the keys present inside each catalog.
	// '2' + 'store id' + '_' + 'segment index' => 'catalog info'
	for (uint32_t cnt = 1; cnt <= catalogSegmentCnt; cnt++) {
		std::ostringstream catalogSegmentIndex;
		catalogSegmentIndex << cnt;
		string storeCatalogKey = DPS_STORE_CATALOG_TYPE + storeIdString + "_" + catalogSegmentIndex.str();

		char *response  = memcached_get(memc, storeCatalogKey.c_str(), storeCatalogKey.length(),
			&return_value_length,
			&flags,
			&rc);

		// This result can come as empty with a NULL response, when a given catalog segment is empty.
		if (rc != MEMCACHED_SUCCESS) {
			// Unable to get the requested data item from the cache.
			dbError.set("Unable to get the requested data item from the store with the StoreId " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_CATALOG_READ_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside clearStoreThatIsAlreadyLocked, it failed for store id " << storeIdString << ". " << DPS_STORE_CATALOG_READ_ERROR, "MemcachedDBLayer");
			return;
		}

		// For pointer safety reasons, do this only when the response is non-null.
		if (response != NULL) {
			string keysInThisStoreCatalogSegment = string(response, return_value_length);
			free(response);

			std::vector<std::string> words;
			streams_boost::split(words, keysInThisStoreCatalogSegment, streams_boost::is_any_of(","), streams_boost::token_compress_on);

			for (std::vector<std::string>::iterator it = words.begin(); it != words.end(); ++it)
			{
				// Simply delete every data item cached in this store.
				// Every data item's key is prefixed with a type indicator and its store id before being used for CRUD operation in the memcached store.
				// "Data Item(s)": '3' + 'store id' + '_' + base64('key') => 'segment index' + '_' + 'value'
				std::string keyDataString = *it;
				std::string newKeyData = DPS_STORE_DATA_ITEM_TYPE + storeIdString + "_" + keyDataString;
				// Delete it now
				memcached_delete(memc, newKeyData.c_str(), newKeyData.length(), (time_t)0);
			}
		}
	}

	// Let us now delete all the catalog segments belonging to this store
	// However, don't delete the very first catalog segment since we need to keep at least one empty catalog segment for this store.
	// '2' + 'store id' + '_' + 'segment index' => 'catalog info'
	for (uint32_t cnt = 1; cnt <= catalogSegmentCnt; cnt++) {
		if (cnt == 1) {
			continue;
		}

		ostringstream storeCatalog;
		storeCatalog << DPS_STORE_CATALOG_TYPE << storeIdString << "_" << cnt;
		string storeCatalogKey = storeCatalog.str();
		memcached_delete(memc, storeCatalogKey.c_str(), storeCatalogKey.length(), (time_t)0);
	}

	// Reset the store info and the very first catalog segment with initial values.
	// '1' + 'store id' => 'data item count' + '_^^_' + 'catalog segment count' + '_^^_' + 'last catalog segment size' +
	// '_^^_' + 'store name' + '_^^_' + 'key_spl_type_name' + '_^^_' + 'value_spl_type_name'
	std::string storeInfoKey = DPS_STORE_INFO_TYPE + storeIdString;
	string delimiter = string(MEMCACHED_STORE_INFO_DELIMITER);
	ostringstream storeInfoValue;
	storeInfoValue << "0" << delimiter << "1" << delimiter << "0" <<
		delimiter << storeName << delimiter << keySplTypeName << delimiter << valueSplTypeName;
	string value_string = storeInfoValue.str();

	rc = memcached_set(memc, storeInfoKey.c_str(), storeInfoKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

	if (rc != MEMCACHED_SUCCESS) {
		// Problem in creating the "StoreId:StoreInfo" entry in the cache.
		dbError.set("Unable to reset 'StoreId:StoreInfo' in the cache for the store named " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_INFO_RESET_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside clearStoreThatIsAlreadyLocked, it failed for store " << storeIdString << ". " << DPS_STORE_INFO_RESET_ERROR, "MemcachedDBLayer");
		return;
	}

	// Reset the very first catalog segment.
	// '2' + 'store id' + '_' + 'segment index' => 'catalog info'
	std::ostringstream storeCatalog;
	storeCatalog << DPS_STORE_CATALOG_TYPE << storeIdString << "_1";
	string storeCatalogKey = storeCatalog.str();
	value_string = ""; // Initialize with an empty string
	rc = memcached_set(memc, storeCatalogKey.c_str(), storeCatalogKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

	if (rc != MEMCACHED_SUCCESS) {
		// Problem in creating a store catalog in the cache.
		dbError.set("Unable to reset StoreCatalog in the cache for the store named " + storeIdString + ". " + std::string(memcached_strerror(memc, rc)), DPS_STORE_CATALOG_RESET_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside clearStoreThatIsAlreadyLocked, it failed for store " << storeIdString << ". " << DPS_STORE_CATALOG_RESET_ERROR, "MemcachedDBLayer");
		return;
	}

	// This particular store should be completely cleared to be empty now.
  }
        
  uint64_t MemcachedDBLayer::size(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside size for store id " << store, "MemcachedDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside size, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside size, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MemcachedDBLayer");
		}

		return(false);
	}

	// Store size information is maintained as part of the store information.
	uint32_t dataItemCnt = 0;
	uint32_t catalogSegmentCnt = 0;
	uint32_t lastCatalogSegmentSize = 0;
	std::string storeName = "";
	string keySplTypeName = "";
	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, catalogSegmentCnt,
		lastCatalogSegmentSize, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside size, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		return(0);
	}

	return((uint64_t)dataItemCnt);
  }

  // In memcached data item keys can't have spaces.
  // Hence, it is required to based64 encode them before storing the
  // key:value data item in memcached.
  // (Use boost functions to do this.)
  void MemcachedDBLayer::base64_encode(std::string const & str, std::string & base64) {
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
  void MemcachedDBLayer::base64_decode(std::string & base64, std::string & result) {
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
  bool MemcachedDBLayer::storeIdExistsOrNot(string storeIdString, PersistenceError & dbError) {
	  // Store Info format
	  // '1' + 'store id' => 'data item count' + '_^^_' + 'catalog segment count' + '_^^_' + 'last catalog segment size' +
	  // '_^^_' + 'store name' + '_^^_' + 'key_spl_type_name' + '_^^_' + 'value_spl_type_name'
	  std::string storeInfoKey = DPS_STORE_INFO_TYPE + storeIdString;
	  memcached_return_t rc;
	  bool storeIdExists = true;

	  rc = memcached_exist(memc, storeInfoKey.c_str(), storeInfoKey.length());

	  if ((rc != MEMCACHED_SUCCESS) && (rc != MEMCACHED_NOTFOUND)) {
		  dbError.set("Unable to connect to the memcached server(s). " + std::string(memcached_strerror(memc, rc)), DPS_CONNECTION_ERROR);
		  return(false);
	  }

	  if (rc == MEMCACHED_NOTFOUND) {
		  storeIdExists = false;
	  }

	  return(storeIdExists);
  }


  // This method will acquire a lock for a given store.
  bool MemcachedDBLayer::acquireStoreLock(string const & storeIdString) {
	  int32_t retryCnt = 0;

	  //Try to get a lock for this store.
	  while (1) {
		uint32_t flags = 0;
		// '4' + 'store id' + 'dps_lock' => 1
		std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;
		memcached_return_t rc;
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or
		// until the memcached back-end removes this lock entry after the DPS_AND_DL_GET_LOCK_TTL times out.
		rc = memcached_add(memc, storeLockKey.c_str(), storeLockKey.length(), "1", 1, DPS_AND_DL_GET_LOCK_TTL, flags);

		if (rc == MEMCACHED_SUCCESS) {
			return(true);
		}

		// Someone else is holding on to the lock of this store. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {

			return(false);
		}

		// Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
		usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
			  (retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
	  }

	  return(false);
  }

  void MemcachedDBLayer::releaseStoreLock(string const & storeIdString) {
	  // '4' + 'store id' + 'dps_lock' => 1
	  std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;
	  memcached_delete(memc, storeLockKey.c_str(), storeLockKey.length(), (time_t)0);
  }

  bool MemcachedDBLayer::readStoreInformation(std::string const & storeIdString, PersistenceError & dbError, uint32_t & dataItemCnt,
		  uint32_t & catalogSegmentCnt, uint32_t & lastCatalogSegmentSize, std::string & storeName,
		  std::string & keySplTypeName, std::string & valueSplTypeName) {
	  // Read the contents of the store information.
	  catalogSegmentCnt = 0;
	  storeName = "";
	  keySplTypeName = "";
	  valueSplTypeName = "";
	  uint32_t flags = 0;
	  memcached_return_t rc;
	  size_t return_value_length;

	  // Store Info contains meta data information about a given store.
	  // '1' + 'store id' => 'data item count' + '_^^_' + 'catalog segment count' + '_^^_' + 'last catalog segment size' +
	  // '_^^_' + 'store name' + '_^^_' + 'key_spl_type_name' + '_^^_' + 'value_spl_type_name'
	  string storeInfoKey = DPS_STORE_INFO_TYPE + storeIdString;

	  char *response  = memcached_get(memc, storeInfoKey.c_str(), storeInfoKey.length(),
		&return_value_length,
		&flags,
		&rc);

	  if (rc != MEMCACHED_SUCCESS || response == NULL) {
		// Unable to get the StoreInfo from our cache.
		dbError.set("Unable to get StoreInfo from the cache using the StoreId " + storeIdString +
			". " + std::string(memcached_strerror(memc, rc)), DPS_GET_STORE_INFO_ERROR);

		if (response != NULL) {
			free(response);
		}

		return(false);
	  }

	  std::string storeInfo = std::string(response, return_value_length);
	  free(response);

	  // As shown in the comment line above, store information is a string that has multiple pieces of
	  // information each separated by '_^^_'.
	  // Let us parse it now.
	  std::string delimiter = string(MEMCACHED_STORE_INFO_DELIMITER);
	  size_t pos = 0;
	  std::string token;
	  int32_t tokenCnt = 0;

	  // Stay in a loop and parse tokens using the delimiter string.
	  while ((pos = storeInfo.find(delimiter)) != std::string::npos) {
		  tokenCnt++;
		  // Carve out the current token from the storeInfo string.
		  token = storeInfo.substr(0, pos);

		  if (tokenCnt == 1) {
			  dataItemCnt = streams_boost::lexical_cast<uint32_t>(token.c_str());
		  } else if (tokenCnt == 2) {
			  catalogSegmentCnt = streams_boost::lexical_cast<uint32_t>(token.c_str());
		  } else if (tokenCnt == 3) {
			  lastCatalogSegmentSize = streams_boost::lexical_cast<uint32_t>(token.c_str());
		  } else if (tokenCnt == 4) {
			  storeName = token;
		  } else if (tokenCnt == 5) {
			  keySplTypeName = token;
		  }

		  // Since we are done with this token, remove this token from the storeInfo string.
		  storeInfo.erase(0, pos + delimiter.length());
	  } // End of while.

	  // Final leftover string will be our very last remaining token (valueSplTypeName)
	  valueSplTypeName = storeInfo;

	  if (catalogSegmentCnt == 0) {
		  // Unable to get the catalog segment count for this store.
		  dbError.set("Unable to get catalog segment count from the cache for StoreId " + storeIdString + ".", DPS_GET_STORE_CATALOG_COUNT_ERROR);
		  return(false);
	  }

	  if (storeName == "") {
		  // Unable to get the name of this store.
		  dbError.set("Unable to get the store name from the cache for StoreId " + storeIdString + ".", DPS_GET_STORE_NAME_ERROR);
		  return(false);
	  }

	  return(true);
  }

  string MemcachedDBLayer::getStoreName(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MemcachedDBLayer");
		}

		return("");
	}

	uint32_t dataItemCnt = 0;
	uint32_t catalogSegmentCnt = 0;
	uint32_t lastCatalogSegmentSize = 0;
	std::string storeName = "";
	string keySplTypeName = "";
	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, catalogSegmentCnt,
		lastCatalogSegmentSize, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		  return("");
	}


	string base64_decoded_storeName;
	base64_decode(storeName, base64_decoded_storeName);
	return(base64_decoded_storeName);
  }

  string MemcachedDBLayer::getSplTypeNameForKey(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MemcachedDBLayer");
		}

		return("");
	}

	uint32_t dataItemCnt = 0;
	uint32_t catalogSegmentCnt = 0;
	uint32_t lastCatalogSegmentSize = 0;
	std::string storeName = "";
	string keySplTypeName = "";
	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, catalogSegmentCnt,
		lastCatalogSegmentSize, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		  return("");
	}

	string base64_decoded_keySplTypeName;
	base64_decode(keySplTypeName, base64_decoded_keySplTypeName);
	return(base64_decoded_keySplTypeName);
  }

  string MemcachedDBLayer::getSplTypeNameForValue(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MemcachedDBLayer");
		}

		return("");
	}

	uint32_t dataItemCnt = 0;
	uint32_t catalogSegmentCnt = 0;
	uint32_t lastCatalogSegmentSize = 0;
	std::string storeName = "";
	string keySplTypeName = "";
	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, catalogSegmentCnt,
		lastCatalogSegmentSize, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		  return("");
	}

	string base64_decoded_valueSplTypeName;
	base64_decode(valueSplTypeName, base64_decoded_valueSplTypeName);
	return(base64_decoded_valueSplTypeName);
  }

  std::string MemcachedDBLayer::getNoSqlDbProductName(void) {
	  return(string(MEMCACHED_NO_SQL_DB_NAME));
  }

  void MemcachedDBLayer::getDetailsAboutThisMachine(std::string & machineName, std::string & osVersion, std::string & cpuArchitecture) {
	  machineName = nameOfThisMachine;
	  osVersion = osVersionOfThisMachine;
	  cpuArchitecture = cpuTypeOfThisMachine;
  }

  bool MemcachedDBLayer::runDataStoreCommand(std::string const & cmd, PersistenceError & dbError) {
	  // This API can only be supported in NoSQL data stores such as Redis, Cassandra, etc.
	  // Memcached doesn't have a way to do this.
	  dbError.set("From Memcached data store: This API to run native data store commands is not supported in memcached.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
	  SPLAPPTRC(L_DEBUG, "From Memcached data store: This API to run native data store commands is not supported in memcached. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "MemcachedDBLayer");
	  return(false);
  }

  bool MemcachedDBLayer::runDataStoreCommand(uint32_t const & cmdType, std::string const & httpVerb,
		std::string const & baseUrl, std::string const & apiEndpoint, std::string const & queryParams,
		std::string const & jsonRequest, std::string & jsonResponse, PersistenceError & dbError) {
		// This API can only be supported in NoSQL data stores such as Cloudant etc.
		// Memcached doesn't have a way to do this.
		dbError.set("From Memcached data store: This API to run native data store commands is not supported in memcached.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Memcached data store: This API to run native data store commands is not supported in memcached. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "MemcachedDBLayer");
		return(false);
  }

  bool MemcachedDBLayer::runDataStoreCommand(std::vector<std::string> const & cmdList, std::string & resultValue, PersistenceError & dbError) {
		// This API can only be supported in Redis.
		// Memcached doesn't have a way to do this.
		dbError.set("From Memcached data store: This API to run native data store commands is not supported in memcached.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Memcached data store: This API to run native data store commands is not supported in memcached. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "MemcachedDBLayer");
		return(false);
  }


  // This method will get either the catalog segment index or the data item or both.
  // Caller of this method can indicate their preference.
  bool MemcachedDBLayer::getCatalogSegmentIndexOrDataItemOrBoth(std::string const & storeIdString,
		  std::string const & keyDataString, bool const & catalogSegmentIndexNeeded,
		  uint32_t & catalogSegmentIndex, bool const & valueDataNeeded,
		  unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError) {
	  	// If the caller needs neither the catalogSegmentIndex nor the dataItem, then we will return right now.
	  	if ((catalogSegmentIndexNeeded == false) && (valueDataNeeded == false)) {
	  		// It is mischievous call.
			dbError.set("Useless call made to get data item for the StoreId " + storeIdString + "." , DPS_DATA_ITEM_FAKE_READ_ERROR);
	  		return(false);
	  	}

		// Let us get this data item from the cache as it is.
		// Since there could be multiple data writers, we are going to get whatever is there now.
		// It is always possible that the value for the requested item can change right after
		// you read it due to the data write made by some other thread. Such is life in a global distributed in-memory store.
		uint32_t flags = 0;
		memcached_return_t rc;
		size_t return_value_length;
		std::string newKeyData = keyDataString;

		// Every data item's key is prefixed with type indicator and its store id before being used for CRUD operation in the memcached store.
		// '3' + 'store id' + '_' + base64('key') => 'segment index' + '_' + 'value'
		newKeyData = DPS_STORE_DATA_ITEM_TYPE + storeIdString + "_" + newKeyData;

		char *response  = memcached_get(memc, newKeyData.c_str(), newKeyData.length(),
			&return_value_length,
			&flags,
			&rc);

		// If SUCCESS, this result can't come as empty with a NULL response. Because, at the minimum, catalog segment index must be present in the response.
		if (rc != MEMCACHED_SUCCESS || return_value_length == 0 || response == NULL) {
			// Unable to get the requested data item from the cache.
			dbError.set("Unable to get the requested data item from the store with the StoreId " + storeIdString + ". " +
				std::string(memcached_strerror(memc, rc)), DPS_DATA_ITEM_READ_ERROR);

			if (response != NULL) {
				free(response);
			}

			return(false);
		}

		// Data item value read from the store will be in this format: 'catalog segment index' + '_' + 'value'
		// We have to return either the catalog segment index or the dataItem value or both depending on the caller's preference.
		uint32_t segmentIndexLength = 0;
		bool underscoreFound = false;

		// Loop through the received response data and locate the first underscore character.
		while(segmentIndexLength < return_value_length) {
			if (response[segmentIndexLength] == (char)'_') {
				underscoreFound = true;
				break;
			}

			segmentIndexLength++;
		}

		if (underscoreFound == false) {
			// Unable to parse the catalog segment index.
			dbError.set("Unable to parse the catalog segment index from the store with the StoreId " +
				storeIdString + ".", DPS_CATALOG_SEGMENT_INDEX_PARSING_ERROR);
			free(response);
			return(false);
		}

		// Now is a good time to parse the catalog segment index.
		if (catalogSegmentIndexNeeded == true) {
			// Let us parse the catalog segment index.
			string catalogSegmentIndexString = string(response, segmentIndexLength);
			catalogSegmentIndex = streams_boost::lexical_cast<uint32_t>(catalogSegmentIndexString.c_str());
		}

		// Actual data item value starts at a location next to the underscore character.
		segmentIndexLength++;

		// Check if the caller wants us to return the dataItem value.
		if (valueDataNeeded == false) {
			free(response);
			return(true);
		}

		if (return_value_length == segmentIndexLength) {
			// User stored empty data item value in the cache.
			valueData = NULL;
			valueSize = 0;
		} else {
			// We can allocate memory for the exact length of the data item value.
			valueSize = return_value_length - segmentIndexLength;
			valueData = (unsigned char *) malloc(valueSize);

			if (valueData == NULL) {
				// Unable to allocate memory to transfer the data item value.
				dbError.set("Unable to allocate memory to copy the data item value for the StoreId " +
					storeIdString + ".", DPS_GET_DATA_ITEM_MALLOC_ERROR);
				// Free the response memory pointer handed to us.
				free(response);
				valueSize = 0;
				return(false);
			}

			// We expect the caller of this method to free the valueData pointer.
			memcpy(valueData, &response[segmentIndexLength], valueSize);
		}

		free(response);
	    return(true);
  }

  MemcachedDBLayerIterator * MemcachedDBLayer::newIterator(uint64_t store, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside newIterator for store id " << store, "MemcachedDBLayer");

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  // Ensure that a store exists for the given store id.
	  if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MemcachedDBLayer");
		  }

		  return(NULL);
	  }

	  // Get the general information about this store.
	  uint32_t dataItemCnt = 0;
	  uint32_t catalogSegmentCnt = 0;
	  uint32_t lastCatalogSegmentSize = 0;
	  std::string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, catalogSegmentCnt,
		lastCatalogSegmentSize, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayer");
		  return(NULL);
	  }

	  // It is a valid store. Create a new iterator and return it to the caller.
	  MemcachedDBLayerIterator *iter = new MemcachedDBLayerIterator();
	  // Set this up to start from catalog segment 1.
	  iter->store = store;
	  base64_decode(storeName, iter->storeName);
	  iter->currentCatalogSegment = 1;
	  iter->fetchDataItemKeys = true;
	  // Give this iterator access to our memcached connection handle.
	  iter->memc = memc;
	  iter->hasData = true;
	  // Give this iterator access to our MemcachedDBLayer object.
	  iter->memcachedDBLayerPtr = this;
	  return(iter);
  }

  void MemcachedDBLayer::deleteIterator(uint64_t store, Iterator * iter, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside deleteIterator for store id " << store, "MemcachedDBLayer");

	  if (iter == NULL) {
		  return;
	  }

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  MemcachedDBLayerIterator *myIter = static_cast<MemcachedDBLayerIterator *>(iter);

	  // Let us ensure that the user wants to delete an iterator that really belongs to the store passed to us.
	  // This will handle user's coding errors where a wrong combination of store id and iterator is passed to us for deletion.
	  if (myIter->store != store) {
		  // User sent us a wrong combination of a store and an iterator.
		  dbError.set("A wrong iterator has been sent for deletion. This iterator doesn't belong to the StoreId " +
		  				storeIdString + ".", DPS_STORE_ITERATION_DELETION_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside deleteIterator, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_DELETION_ERROR, "MemcachedDBLayer");
		  return;
	  } else {
		  delete iter;
	  }
  }

  // This method will acquire a lock for any given generic/arbitrary identifier passed as a string..
  // This is typically used inside the createStore, createOrGetStore, createOrGetLock methods to
  // provide thread safety. There are other lock acquisition/release methods when someone has a valid store id or lock id.
  bool MemcachedDBLayer::acquireGeneralPurposeLock(string const & entityName) {
	  int32_t retryCnt = 0;

	  //Try to get a lock for this generic entity.
	  while (1) {
		uint32_t flags = 0;
		// '501' + 'entity name' + 'generic_lock' => 1
		std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
		memcached_return_t rc;
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or
		// until the memcached back-end removes this lock entry after the DPS_AND_DL_GET_LOCK_TTL times out.
		rc = memcached_add(memc, genericLockKey.c_str(), genericLockKey.length(), "1", 1, DPS_AND_DL_GET_LOCK_TTL, flags);

		if (rc == MEMCACHED_SUCCESS) {
			return(true);
		}

		// Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {

			return(false);
		}

		// Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
		usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
			  (retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
	  }

	  return(false);
  }

  void MemcachedDBLayer::releaseGeneralPurposeLock(string const & entityName) {
	  // '501' + 'entity name' + 'generic_lock' => 1
	  std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
	  memcached_delete(memc, genericLockKey.c_str(), genericLockKey.length(), (time_t)0);
  }

  MemcachedDBLayerIterator::MemcachedDBLayerIterator() {

  }

  MemcachedDBLayerIterator::~MemcachedDBLayerIterator() {

  }

  bool MemcachedDBLayerIterator::getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
  	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside getNext for store id " << store, "MemcachedDBLayerIterator");

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

	  // Ensure that a store exists for the given store id.
	  if (this->memcachedDBLayerPtr->storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayerIterator");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "MemcachedDBLayerIterator");
		  }

		  return(false);
	  }

	  // Ensure that this store is not empty at this time.
	  if (this->memcachedDBLayerPtr->size(store, dbError) <= 0) {
		  dbError.set("Store is empty for the StoreId " + storeIdString + ".", DPS_STORE_EMPTY_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_STORE_EMPTY_ERROR, "MemcachedDBLayerIterator");
		  return(false);
	  }

	  if (this->fetchDataItemKeys == true) {
		  	// In the previous visit to this method by the caller, we probably had hit
		  	// the end of a catalog segment. We are being asked to move to the next
		  	// available catalog segment for this store. Or, it could be that the
		  	// caller is just beginning the iteration to start from the very
		  	// first catalog segment for this store.
			uint32_t flags = 0;
			memcached_return_t rc;
			size_t return_value_length;
			this->dataItemKeys.clear();

			// Loop until we get to any catalog segment of this store that has some
			// data item keys. If not, loop until we run out of the available catalog segments.
			while(1) {
				// '2' + 'store id' + '_' + 'segment index' => 'catalog info'
				std::ostringstream catalogSegmentIndex;
				catalogSegmentIndex << this->currentCatalogSegment;
				string storeCatalogKey = DPS_STORE_CATALOG_TYPE + storeIdString + "_" + catalogSegmentIndex.str();

				char *response  = memcached_get(this->memc, storeCatalogKey.c_str(), storeCatalogKey.length(),
				&return_value_length,
				&flags,
				&rc);

				// This result can come as empty with a NULL response, when a given catalog segment is empty.
				if (rc != MEMCACHED_SUCCESS) {
					// Unable to get the requested data item from the cache.
					// This is probably due to the reason that we reached the end of the available catalog segments for this store.
					// Due to this error, we will disable any future action for this store using the current iterator.
					this->hasData = false;
					return(false);
				}

				// For pointer safety reasons, do this only when the response is non-null.
				if (response != NULL) {
					string keysInThisStoreCatalogSegment = string(response, return_value_length);
					free(response);
					streams_boost::split(this->dataItemKeys, keysInThisStoreCatalogSegment, streams_boost::is_any_of(","), streams_boost::token_compress_on);
				}

				this->sizeOfDataItemKeysVector = this->dataItemKeys.size();

				if (this->sizeOfDataItemKeysVector > 0) {
					// We have one or more elements in the keys list.
					// We can do some iteration on them.
					this->fetchDataItemKeys = false;
					this->currentIndex = 0;
					break;
				} else {
					// Advance the catalog segment to the next one and continue looking for the data item keys.
					this->currentCatalogSegment += 1;
				}
			} // End of while(1)
	  } // End of if (this->fetchDataItemKeys == true)

	  // We have data item keys.
	  // Let us get the next available data.
	  string base64_encoded_data_item_key = this->dataItemKeys.at(this->currentIndex);
	  // Advance the data item key vector index by 1 for it to be ready for the next iteration.
	  this->currentIndex += 1;

	  if (this->currentIndex >= this->sizeOfDataItemKeysVector) {
		  // Prepare everything ready for the new data item key fetch from the next catalog segment if there will be one.
		  this->dataItemKeys.clear();
		  this->currentIndex = 0;
		  this->sizeOfDataItemKeysVector = 0;
		  this->currentCatalogSegment += 1;
		  this->fetchDataItemKeys = true;
	  }

	  uint32_t catalogSegmentForThisDataItem = 0;
	  // Get this data item's store catalog segment index and value data and value size.
	  bool result = this->memcachedDBLayerPtr->getCatalogSegmentIndexOrDataItemOrBoth(storeIdString, base64_encoded_data_item_key,
		false, catalogSegmentForThisDataItem, true, valueData, valueSize, dbError);

	  if (result == false) {
		  // Some error has occurred in reading the data item value.
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "MemcachedDBLayerIterator");
		  // We will disable any future action for this store using the current iterator.
		  this->hasData = false;
		  return(false);
	  }

	  // We are almost done once we take care of arranging to return the key name and key size.
	  string rawKey;
	  this->memcachedDBLayerPtr->base64_decode(base64_encoded_data_item_key, rawKey);
	  keySize = rawKey.length();
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
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_MALLOC_ERROR, "MemcachedDBLayerIterator");
		  return(false);
	  }

	  // Copy the raw key data into the allocated buffer.
	  memcpy(keyData, rawKey.data(), keySize);
	  // We are done. We expect the caller to free the keyData and valueData buffers.
	  return(true);
  }

// =======================================================================================================
// Beyond this point, we have code that deals with the distributed locks that a SPL developer can
// create, remove,acquire, and release.
// =======================================================================================================
  uint64_t MemcachedDBLayer::createOrGetLock(std::string const & name, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock with a name " << name, "MemcachedDBLayer");
		string base64_encoded_name;
		base64_encode(name, base64_encoded_name);

	 	// Get a general purpose lock so that only one thread can
		// enter inside of this method at any given time with the same lock name.
	 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
	 		// Unable to acquire the general purpose lock.
	 		lkError.set("Unable to get a generic lock for creating a lock with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
	 		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for an yet to be created lock with its name as " <<
	 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "MemcachedDBLayer");
	 		// User has to retry again to create this distributed lock.
	 		return 0;
	 	}

		// Let us first see if a lock with the given name already exists.
		// IMPORTANT: We don't support lock names containing space characters. memcached will reject such keys.
		uint32_t flags = 0;
		// Inside memcached, all our lock names will have a mapping type indicator of
		// "5" at the beginning followed by the actual lock name.
		// '5' + 'lock name' ==> 'lock id'
		std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
		memcached_return_t rc;

		rc = memcached_exist(memc, lockNameKey.c_str(), lockNameKey.length());

		if ((rc != MEMCACHED_SUCCESS) && (rc != MEMCACHED_NOTFOUND)) {
			lkError.set("Unable to connect to the memcached server(s). " + std::string(memcached_strerror(memc, rc)), DL_CONNECTION_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for the lock named " << name << ". " << DL_CONNECTION_ERROR, "MemcachedDBLayer");
			releaseGeneralPurposeLock(base64_encoded_name);
			return 0;
		}

		if (rc == MEMCACHED_SUCCESS) {
			// This lock already exists in our cache.
			// We can get the lockId and return it to the caller.
			size_t return_value_length;

			char *response  = memcached_get(memc, lockNameKey.c_str(), lockNameKey.length(),
				&return_value_length,
				&flags,
				&rc);

			if (rc != MEMCACHED_SUCCESS || response == NULL) {
				// Unable to get an existing lock id from the cache.
				lkError.set("Unable to get the lockId from the cache for the lockName " + name + ". " + std::string(memcached_strerror(memc, rc)), DL_GET_LOCK_ID_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for the lockName " << name << ". " << DL_GET_LOCK_ID_ERROR, "MemcachedDBLayer");
				releaseGeneralPurposeLock(base64_encoded_name);

				if (response != NULL) {
					free(response);
				}

				return(0);
			} else {
				uint64_t lockId = streams_boost::lexical_cast<uint64_t>(response);
				free(response);
				releaseGeneralPurposeLock(base64_encoded_name);
				return(lockId);
			}
		}

		if (rc == MEMCACHED_NOTFOUND) {
			// Create a new lock.
			// At first, let us increment our global dps_and_dl_guid to reserve a new lock id.
			uint64_t lockId = 0;
			std::string guid_key = DPS_AND_DL_GUID_KEY;
			rc = memcached_increment(memc, guid_key.c_str(), guid_key.length(), 1, &lockId);

			if (rc != MEMCACHED_SUCCESS) {
				lkError.set("Unable to get a unique lock id for a lock named " + name + ". " + std::string(memcached_strerror(memc, rc)), DL_GUID_CREATION_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for a lock named " << name << ". " << DL_GUID_CREATION_ERROR, "MemcachedDBLayer");
				releaseGeneralPurposeLock(base64_encoded_name);
				return 0;
			}

			if (rc == MEMCACHED_SUCCESS) {
				// We secured a guid. We can now create this lock.
				//
				// 1) Create the Lock Name
				//    '5' + 'lock name' ==> 'lock id'
				std::ostringstream value;
				value << lockId;
				std::string value_string = value.str();

				rc = memcached_set(memc, lockNameKey.c_str(), lockNameKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

				if (rc != MEMCACHED_SUCCESS) {
					// Problem in creating the "Lock Name" entry in the cache.
					lkError.set("Unable to create 'LockName:LockId' in the cache for a lock named " + name + ". " + std::string(memcached_strerror(memc, rc)), DL_LOCK_NAME_CREATION_ERROR);
					SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for a lock named " << name << ". " << DL_LOCK_NAME_CREATION_ERROR, "MemcachedDBLayer");
					// We are simply leaving an incremented value for the dps_and_dl_guid key in the cache that will never get used.
					// Since it is harmless, there is no need to reduce this number by 1. It is okay that this guid number will remain unassigned to any store or a lock.
					releaseGeneralPurposeLock(base64_encoded_name);
					return 0;
				}

				// 2) Create the Lock Info
				//    '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
				std::string lockInfoKey = DL_LOCK_INFO_TYPE + value_string;  // LockId becomes the new key now.
				value_string = string("0_0_0_") + base64_encoded_name;

				rc = memcached_set(memc, lockInfoKey.c_str(), lockInfoKey.length(), value_string.c_str(), value_string.length(), (time_t)0, flags);

				if (rc != MEMCACHED_SUCCESS) {
					// Problem in creating the "LockId:LockInfo" entry in the cache.
					lkError.set("Unable to create 'LockId:LockInfo' in the cache for a lock named " + name + ". " + std::string(memcached_strerror(memc, rc)), DL_LOCK_INFO_CREATION_ERROR);
					SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for a lock named " << name << ". " << DL_LOCK_INFO_CREATION_ERROR, "MemcachedDBLayer");
					// Delete the previous entry we made.
					memcached_delete(memc, lockNameKey.c_str(), lockNameKey.length(), (time_t)0);
					releaseGeneralPurposeLock(base64_encoded_name);
					return 0;
				}

				// We created the lock.
				SPLAPPTRC(L_DEBUG, "Inside createOrGetLock done for a lock named " << name, "MemcachedDBLayer");
				releaseGeneralPurposeLock(base64_encoded_name);
				return (lockId);
			}
		}

		releaseGeneralPurposeLock(base64_encoded_name);
		return 0;
  }

  bool MemcachedDBLayer::removeLock(uint64_t lock, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside removeLock for lock id " << lock, "MemcachedDBLayer");
		ostringstream lockId;
		lockId << lock;
		string lockIdString = lockId.str();

		// If the lock doesn't exist, there is nothing to remove. Don't allow this caller inside this method.
		if(lockIdExistsOrNot(lockIdString, lkError) == false) {
			if (lkError.hasError() == true) {
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "MemcachedDBLayer");
			} else {
				lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "MemcachedDBLayer");
			}

			return(false);
		}

		// Before removing the lock entirely, ensure that the lock is not currently being used by anyone else.
		if (acquireLock(lock, 5, 3, lkError) == false) {
			// Unable to acquire the distributed lock.
			lkError.set("Unable to get a distributed lock for the LockId " + lockIdString + ".", DL_GET_DISTRIBUTED_LOCK_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for the lock id " << lockIdString << ". " << DL_GET_DISTRIBUTED_LOCK_ERROR, "MemcachedDBLayer");
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
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "MemcachedDBLayer");
			releaseLock(lock, lkError);
			// This is alarming. This will put this lock in a bad state. Poor user has to deal with it.
			return(false);
		}

		// Let us first remove the lock info for this distributed lock.
		// '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
		memcached_delete(memc, lockInfoKey.c_str(), lockInfoKey.length(), (time_t)0);

		// We can now delete the lock name root entry.
		string lockNameKey = DL_LOCK_NAME_TYPE + lockName;
		memcached_delete(memc, lockNameKey.c_str(), lockNameKey.length(), (time_t)0);

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

  bool MemcachedDBLayer::acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside acquireLock for lock id " << lock, "MemcachedDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();
	  int32_t retryCnt = 0;

	  // If the lock doesn't exist, there is nothing to acquire. Don't allow this caller inside this method.
	  if(lockIdExistsOrNot(lockIdString, lkError) == false) {
		  if (lkError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "MemcachedDBLayer");
		  } else {
			  lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for lock id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "MemcachedDBLayer");
		  }

		  return(false);
	  }

	  // We will first check if we can get this lock.
	  uint32_t flags = 0;
	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  memcached_return_t rc;
	  time_t startTime, timeNow;
	  // Get the start time for our lock acquisition attempts.
	  time(&startTime);

	  //Try to get a distributed lock.
	  while(1) {
		  // This is an atomic activity.
		  // If multiple threads attempt to do it at the same time, only one will succeed.
		  // Winner will hold the lock until they release it voluntarily or
		  // until the memcached back-end removes this lock entry after the lease time ends.
		  // We will add the lease time to the current timestamp i.e. seconds elapsed since the epoch.
		  time_t new_lock_expiry_time = time(0) + (time_t)leaseTime;

		  rc = memcached_add(memc, distributedLockKey.c_str(), distributedLockKey.length(), "1", 1, (time_t)leaseTime, flags);

		  if (rc == MEMCACHED_SUCCESS) {
			  // We got the lock.
			  // Let us update the lock information now.
			  if(updateLockInformation(lockIdString, lkError, 1, new_lock_expiry_time, getpid()) == true) {
				  return(true);
			  } else {
				  // Some error occurred while updating the lock information.
				  // It will be in an inconsistent state. Let us release the lock.
				  releaseLock(lock, lkError);
			  }
		  } else {
			  // We didn't get the lock.
			  // Let us check if the previous owner of this lock simply forgot to release it.
			  // In that case, we will release this expired lock.
			  // Read the time at which this lock is expected to expire.
			  uint32_t _lockUsageCnt = 0;
			  int32_t _lockExpirationTime = 0;
			  std::string _lockName = "";
			  pid_t _lockOwningPid = 0;

			  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
				  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "MemcachedDBLayer");
			  } else {
				  // Is current time greater than the lock expiration time?
				  if ((_lockExpirationTime > 0) && (time(0) > (time_t)_lockExpirationTime)) {
					  // Time has passed beyond the lease of this lock.
					  // Lease expired for this lock. Original owner forgot to release the lock and simply left it hanging there without a valid lease.
					  releaseLock(lock, lkError);
				  }
			  }
		  }

		  // Someone else is holding on to this distributed lock. Wait for a while before trying again.
		  retryCnt++;

		  if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
			  lkError.set("Unable to acquire the lock named " + lockIdString + ".", DL_GET_LOCK_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for a lock named " << lockIdString << ". " << DL_GET_LOCK_ERROR, "MemcachedDBLayer");
			  // Our caller can check the error code and try to acquire the lock again.
			  return(false);
		  }

			// Check if we have gone past the maximum wait time the caller was willing to wait in order to acquire this lock.
			time(&timeNow);
			if (difftime(startTime, timeNow) > maxWaitTimeToAcquireLock) {
				lkError.set("Unable to acquire the lock named " + lockIdString + " within the caller specified wait time.", DL_GET_LOCK_TIMEOUT_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire the lock named " << lockIdString <<
					" within the caller specified wait time." << DL_GET_LOCK_TIMEOUT_ERROR, "MemcachedDBLayer");
				// Our caller can check the error code and try to acquire the lock again.
				return(false);
			}

		  // Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
		  usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
				  (retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
	  } // End of while(1)
  }

  void MemcachedDBLayer::releaseLock(uint64_t lock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside releaseLock for lock id " << lock, "MemcachedDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();
	  memcached_return_t rc;

	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  rc = memcached_delete(memc, distributedLockKey.c_str(), distributedLockKey.length(), (time_t)0);

	  if (rc != MEMCACHED_SUCCESS) {
		  lkError.set("Unable to release the distributed lock id " + lockIdString + ". " + std::string(memcached_strerror(memc, rc)), DL_LOCK_RELEASE_ERROR);
		  return;
	  }

	  updateLockInformation(lockIdString, lkError, 0, 0, 0);
  }

  bool MemcachedDBLayer::updateLockInformation(std::string const & lockIdString,
	PersistenceError & lkError, uint32_t const & lockUsageCnt, int32_t const & lockExpirationTime, pid_t const & lockOwningPid) {
	  // Get the lock name for this lock.
	  uint32_t _lockUsageCnt = 0;
	  int32_t _lockExpirationTime = 0;
	  std::string _lockName = "";
	  pid_t _lockOwningPid = 0;

	  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "MemcachedDBLayer");
		  return(false);
	  }

	  uint32_t flags = 0;
	  memcached_return_t rc;
	  // Let us update the lock information.
	  //    '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  ostringstream lockInfoValue;
	  lockInfoValue << lockUsageCnt << "_" << lockExpirationTime << "_" << lockOwningPid << "_" << _lockName;
	  string lockInfoValueString = lockInfoValue.str();

	  rc = memcached_set(memc, lockInfoKey.c_str(), lockInfoKey.length(), lockInfoValueString.c_str(), lockInfoValueString.length(), (time_t)0, flags);

	  if (rc != MEMCACHED_SUCCESS) {
		  // Problem in updating the "LockId:LockInfo" entry in the cache.
		  lkError.set("Unable to update 'LockId:LockInfo' in the cache for a lock named " + _lockName + ". " + std::string(memcached_strerror(memc, rc)), DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed for a lock named " << _lockName << ". " << DL_LOCK_INFO_UPDATE_ERROR, "MemcachedDBLayer");
		  return(false);
	  }

	  return(true);
  }

  bool MemcachedDBLayer::readLockInformation(std::string const & lockIdString, PersistenceError & lkError, uint32_t & lockUsageCnt,
		  int32_t & lockExpirationTime, pid_t & lockOwningPid, std::string & lockName) {
	  // Read the contents of the lock information.
	  lockName = "";
	  uint32_t flags = 0;
	  memcached_return_t rc;
	  size_t return_value_length;

	  // Lock Info contains meta data information about a given lock.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'lock name'
	  string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;

	  char *response  = memcached_get(memc, lockInfoKey.c_str(), lockInfoKey.length(),
		&return_value_length,
		&flags,
		&rc);

	  if (rc != MEMCACHED_SUCCESS || response == NULL) {
		// Unable to get the LockInfo from our cache.
		lkError.set("Unable to get LockInfo from the cache using the LockId " + lockIdString +
			". " + std::string(memcached_strerror(memc, rc)), DL_GET_LOCK_INFO_ERROR);

		if (response != NULL) {
			free(response);
		}

		return(false);
	  }

	  std::string lockInfo = std::string(response, return_value_length);
	  free(response);

	  // As shown in the comment line above, lock information is a string that has multiple pieces of
	  // information each separated by an underscore character. We are interested in all the three tokens (lock usage count, lock expiration time, lock name).
	  // Let us parse it now.
	  std::vector<std::string> words;
	  streams_boost::split(words, lockInfo, streams_boost::is_any_of("_"), streams_boost::token_compress_on);
	  int32_t tokenCnt = 0;

	  for (std::vector<std::string>::iterator it = words.begin(); it != words.end(); ++it) {
		  string tmpString = *it;

		  switch(++tokenCnt) {
		  	  case 1:
		  		  lockUsageCnt = streams_boost::lexical_cast<uint32_t>(tmpString.c_str());
		  		  break;

		  	  case 2:
		  		  lockExpirationTime = streams_boost::lexical_cast<int32_t>(tmpString.c_str());
		  		  break;

		  	  case 3:
		  		lockOwningPid = streams_boost::lexical_cast<int32_t>(tmpString.c_str());
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
		  lkError.set("Unable to get the lock name from the cache for lockId " + lockIdString + ".", DL_GET_LOCK_NAME_ERROR);
		  return(false);
	  }

	  return(true);
  }

  // This method will check if a lock exists for a given lock id.
  bool MemcachedDBLayer::lockIdExistsOrNot(string lockIdString, PersistenceError & lkError) {
	  // Lock Info contains meta data information about a given lock.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'lock name'
	  string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  memcached_return_t rc;
	  bool lockIdExists = true;

	  rc = memcached_exist(memc, lockInfoKey.c_str(), lockInfoKey.length());

	  if ((rc != MEMCACHED_SUCCESS) && (rc != MEMCACHED_NOTFOUND)) {
		  lkError.set("Unable to connect to the memcached server(s). " + std::string(memcached_strerror(memc, rc)), DPS_CONNECTION_ERROR);
		  return(false);
	  }

	  if (rc == MEMCACHED_NOTFOUND) {
		  lockIdExists = false;
	  }

	  return(lockIdExists);
  }

  // This method will return the process id that currently owns the given lock.
  uint32_t MemcachedDBLayer::getPidForLock(string const & name, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside getPidForLock with a name " << name, "MemcachedDBLayer");
	  string base64_encoded_name;
	  base64_encode(name, base64_encoded_name);
	  uint64_t lock = 0;

	  // Let us first see if a lock with the given name already exists.
	  // IMPORTANT: We don't support lock names containing space characters. memcached will reject such keys.
	  uint32_t flags = 0;
	  // Inside memcached, all our lock names will have a mapping type indicator of
	  // "5" at the beginning followed by the actual lock name.
	  // '5' + 'lock name' ==> 'lock id'
	  std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
	  memcached_return_t rc;

	  rc = memcached_exist(memc, lockNameKey.c_str(), lockNameKey.length());

	  if ((rc != MEMCACHED_SUCCESS) && (rc != MEMCACHED_NOTFOUND)) {
		  lkError.set("Unable to connect to the memcached server(s). " + std::string(memcached_strerror(memc, rc)), DL_CONNECTION_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for the lock named " << name << ". " << DL_CONNECTION_ERROR, "MemcachedDBLayer");
		  return 0;
	  }

	  if (rc == MEMCACHED_SUCCESS) {
		  // This lock already exists in our cache.
		  // We can get the lockId and return it to the caller.
		  size_t return_value_length;

		  char *response  = memcached_get(memc, lockNameKey.c_str(), lockNameKey.length(),
			&return_value_length,
			&flags,
			&rc);

		  if (rc != MEMCACHED_SUCCESS || response == NULL) {
			// Unable to get an existing lock id from the cache.
			lkError.set("Unable to get the lockId from the cache for the lockName " + name + ". " + std::string(memcached_strerror(memc, rc)), DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for the lockName " << name << ". " << DL_GET_LOCK_ID_ERROR, "MemcachedDBLayer");

			if (response != NULL) {
				free(response);
			}

			return(0);
		  } else {
			uint64_t lockId = streams_boost::lexical_cast<uint64_t>(response);
			free(response);
			return(lockId);
		  }
	  }

	  if (rc == MEMCACHED_NOTFOUND) {
		  // Lock with the given name doesn't exist.
		  lkError.set("Unable to find a lockName " + name + ".", DL_LOCK_NOT_FOUND_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getPidForLock, unable to find the lockName " << name << ". " << DL_LOCK_NOT_FOUND_ERROR, "MemcachedDBLayer");
		  return(0);
	  }

	  // Read the lock information.
	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();

	  uint32_t _lockUsageCnt = 0;
	  int32_t _lockExpirationTime = 0;
	  std::string _lockName = "";
	  string cmd = "";
	  pid_t _lockOwningPid = 0;

	  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "MemcachedDBLayer");
		  return(0);
	  } else {
		  return(_lockOwningPid);
	  }
  }

  // This method will return the status of the connection to the back-end data store.
  bool MemcachedDBLayer::isConnected() {
          // Not implemented at this time.
          return(true);
  }

  bool MemcachedDBLayer::reconnect(std::set<std::string> & dbServers, PersistenceError & dbError) {
          // Not implemented at this time.
          return(true);
  }

} } } } }
using namespace com::ibm::streamsx::store;
using namespace com::ibm::streamsx::store::distributed;
extern "C" {
	DBLayer * create(){
	   return new MemcachedDBLayer();
	}
}
