/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2015
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
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

/*
==================================================================================================================
This CPP file contains the source code for the distributed process store (dps) back-end activities such as
insert, update, read, remove, etc. This dps implementation runs on top of the Aerospike NoSQL DB.

At this time (Jan/2015), Aerospike is well known for its ability to effectively use the available DRAM and
SSD flash drives to store data and provide full CRUD support.
In the DPS toolkit, we will mostly use the C APIs available for working with Aerospike.

Any dpsXXXXX native function call made from a SPL composite will go through a serialization layer and then hit
this CPP file. Here, the purpose of such SPL native function calls will be fulfilled using the Aerospike C APIs.
After that, the results will be sent to a deserialization layer. From there, results will be transformed using the
correct SPL types and delivered back to the SPL composite. In general, our distributed process store provides
a distributed no-sql cache for different processes (multiple PEs from one or more Streams applications).
We provide a set of free for all native function APIs to create/read/update/delete data items on one or more stores.
In the worst case, there could be multiple writers and multiple readers for the same store.
It is important to note that a Streams application designer/developer should carefully address how different parts
of his/her application will access the store simultaneously i.e. who puts what, who gets what and at
what frequency from where etc.

This C++ project has a companion SPL project (058_data_sharing_between_non_fused_spl_custom_and_cpp_primitive_operators).
Please refer to the commentary in that SPL project file for learning about the procedure to do an
end-to-end test run involving the SPL code, serialization/deserialization code,
Aerospike interface code (this file), and your Aerospike infrastructure.

As a first step, you should run the ./mk script from the C++ project directory (DistributedProcessStoreLib).
That will take care of building the .so file for the dps and copying it to the SPL project's impl/lib directory.
==================================================================================================================
*/

#include "AerospikeDBLayer.h"
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
  AerospikeDBLayer::AerospikeDBLayer()
  {
	  base64_chars = string("ABCDEFGHIJKLMNOPQRSTUVWXYZ") +
			  	  	 string("abcdefghijklmnopqrstuvwxyz") +
			  	  	 string("0123456789+/");
	  // Initialize the Aerospike instance to NULL.
	  as = NULL;
	  asDBLayerIterator = NULL;
  }

  AerospikeDBLayer::~AerospikeDBLayer()
  {
	  // Application is ending now.
	  // Clean up the Aerospike client handle.
	  if (as != NULL) {
		  // Close the connection first.
		  as_error err;
		  aerospike_close(as, &err);
		  // Destroy the Aerospike client and release all its resources.
		  aerospike_destroy(as);
	  }
  }
        
  void AerospikeDBLayer::connectToDatabase(std::set<std::string> const & dbServers, PersistenceError & dbError)
  {
	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase", "AerospikeDBLayer");

	  // Get the name, OS version and CPU type of this machine.
	  struct utsname machineDetails;

	  if(uname(&machineDetails) < 0) {
		  dbError.set("Unable to get the machine/os/cpu details.", DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to get the machine/os/cpu details. " << DPS_INITIALIZE_ERROR, "AerospikeDBLayer");
		  return;
	  } else {
		  nameOfThisMachine = string(machineDetails.nodename);
		  osVersionOfThisMachine = string(machineDetails.sysname) + string(" ") + string(machineDetails.release);
		  cpuTypeOfThisMachine = string(machineDetails.machine);
	  }

	  // As of Feb/2015, Aerospike C client libraries are not written to work correctly in IBM Power servers due to its
	  // big endian quirkiness. Due to this, Aerospike APIs we use here will not work as expected.
	  // Hence, we will return from here now telling the user that we don't support Aerospike on IBM Power servers.
	  if (cpuTypeOfThisMachine == "ppc64") {
		  dbError.set("DPS toolkit configured with an Aerospike NoSQL server is not supported on IBM Power machines.", DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_ERROR, "Inside connectToDatabase, it failed during initialization. Reason: " <<
			  "DPS toolkit configured with an Aerospike NoSQL server is not supported on IBM Power machines. " <<
			  DPS_INITIALIZE_ERROR, "AerospikeDBLayer");
		  return;
	  }

	  string aerospikeConnectionErrorMsg = "";
	  as_config config;
	  // This buffer can hold 100 server names each with a size of 255 characters, which is
	  // more than enough for our needs.
      char globalServerNames[256*100];
      int bufferPos = 0;


	  // Initialize the Aerospike config with default values.
	  if (as_config_init(&config) == NULL) {
		  aerospikeConnectionErrorMsg = "Error while initializing the Aerospike config with default values.";
		  dbError.set(aerospikeConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed. " << aerospikeConnectionErrorMsg <<
			  DPS_INITIALIZE_ERROR, "AerospikeDBLayer");
		  return;
	  }

	  // Let us add the configured Aerospike servers.
	  // Aerospike supports a server cluster. Hence, user may provide one or more server names with port numbers.
	  for (std::set<std::string>::iterator it=dbServers.begin(); it!=dbServers.end(); ++it) {
          // Aerospoke server name can have port number specified along with it --> MyHost:2345
		  std::string serverName = *it;
	      string targetServerName = "";
	      uint16_t targetServerPort = 0;
	      char serverNameBuf[300];
          strcpy(serverNameBuf, serverName.c_str());
          char *ptr = strtok(serverNameBuf, ":");

          while(ptr != NULL) {
            if (targetServerName == "") {
              // This must be our first token.
              targetServerName = string(ptr);
              ptr = strtok(NULL, ":");
            } else {
              // This must be our second token.
              targetServerPort = atoi(ptr);
              // We are done.
              break;
            }
          }

          if (targetServerName == "") {
            // User only specified the server name and no port.
        	// (This is the case of server name followed by a : character with a missing port number)
            targetServerName = serverName;
            // In this case, use the default Aerospike server port.
            targetServerPort = AEROSPIKE_SERVER_PORT;
          }

          if (targetServerPort == 0) {
            // User didn't give a Aerospike server port.
        	// Only a server name was given not followed by a : character.
            // Use the default Aerospike server port.
            targetServerPort = AEROSPIKE_SERVER_PORT;
          }

          // Add this server to the Aerospike config.
          // Aerospike config object doesn't take a copy of the servername that we pass via the API below.
          // Instead, it holds onto the pointer we pass. Hence, we need a memory area that
          // will stay in scope until we make the Aerospike connection call below.
          // So, make a copy of this server name to a buffer that is declared outside of this for loop and
          // which will be in scope when the Aerospike connection call is made below.
          strcpy(&globalServerNames[bufferPos], targetServerName.c_str());
          as_config_add_host(&config, &globalServerNames[bufferPos], targetServerPort);
          // Change the position to beyond the server name string we just copied i.e. just after that string's null terminator.
          bufferPos += (targetServerName.length() + 1);
	  }

	  // Create a new heap allocated Aerospike instance.
	  as = aerospike_new(&config);

	  if (as == NULL) {
		  aerospikeConnectionErrorMsg = "Error while creating a new Aerospike instance.";
		  dbError.set(aerospikeConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed. " << aerospikeConnectionErrorMsg <<
			  DPS_INITIALIZE_ERROR, "AerospikeDBLayer");
		  return;
	  }

	  // Let us connect to the Aerospike cluster now.
	  as_error err;

	  if (aerospike_connect(as, &err) != AEROSPIKE_OK) {
		  char errorMsg[400];
	      sprintf(errorMsg, "Error while connecting to the Aerospike server(s). rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		  aerospikeConnectionErrorMsg = string(errorMsg);
		  dbError.set(aerospikeConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed. " << aerospikeConnectionErrorMsg <<
			  DPS_INITIALIZE_ERROR, "AerospikeDBLayer");
		  return;
	  }

	  // We have now initialized the Aerospike client and made contacts with the configured Aerospike server(s).
	  // IMPORTANT
	  // ---------
	  // In order to use the DPS successfully in an Aerospike infrastructure, a namespace called ibm_dps must have
	  // already been created manually via the aerospike.conf file and available for use by the DPS code.
	  // In the etc/dps-usage-tips.txt file, there are instructions for creating the ibm_dps namespace.
	  // Please follow those steps to create the ibm_dps namespace before executing your Streams code with the DPS APIs.
	  //
	  // In Aerospike, sets are created upon the insertion of the first record.
	  //
	  // In Aerospike, there can only be a maximum of 1023 sets inside a namespace. In addition, sets can never be
	  // deleted via APIs. It can ONLY be deleted from a command line utility. That will not help us at all here.
	  // We have to do a different approach which is customized for our needs here. Out of those 1023, we are going to
	  // allow only the first 1000 sets to be associated with the user-created stores. Remaining 23 of them will be
	  // reserved for the DPS toolkit's internal use.  As stores are created and deleted, we are going to do
	  // book-keeping of which store is available and unavailable for use. Using this information, we will
	  // dynamically assign a pre-existing set number to represent a user created store.
	  // What a pain in Aerospike?
	  //
	  // We will create a reserved Aerospike set simply to tell us whether store ids 1 through 1000 are occupied or available for use.
	  aerospikeConnectionErrorMsg = "";
	  bool result = createStoreIdTrackerSet(aerospikeConnectionErrorMsg);

	  if (result == false) {
		  dbError.set("Inside connectToDatabase, it failed to create a store id tracker set. Error=" +
			  aerospikeConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to create a store id tracker set. Error=" <<
			  aerospikeConnectionErrorMsg << DPS_INITIALIZE_ERROR, "AerospikeDBLayer");
		  return;
	  }

	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase done", "AerospikeDBLayer");
  }

  uint64_t AerospikeDBLayer::createStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside createStore for store " << name, "AerospikeDBLayer");

	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);

 	// Get a general purpose lock so that only one thread can
	// enter inside of this method at any given time with the same store name.
 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
 		// Unable to acquire the general purpose lock.
 		dbError.set("Unable to get a generic lock for creating a store with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for an yet to be created store with its name as " <<
 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "AerospikeDBLayer");
 		// User has to retry again to create this store.
 		return (0);
 	}

    // Let us first see if a store with the given name already exists in our Aerospike database.
 	uint64_t storeId = findStore(name, dbError);

 	if (storeId > 0) {
		// This store already exists in our cache.
		// We can't create another one with the same name now.
 		std::ostringstream storeIdString;
 		storeIdString << storeId;
 		// The following error message carries the existing store's id at the very end of the message.
		dbError.set("A store named " + name + " already exists with a store id " + storeIdString.str(), DPS_STORE_EXISTS);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed while trying to create a duplicate store " << name << ". " << DPS_STORE_EXISTS, "AerospikeDBLayer");
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
 	// In Aerospike, there are two severe limitations:
 	// 1) There can only be 1000 user created stores in total.
 	// 2) These stores are implemented using an Aerospike Set data structure. Once created, there is no API to delete
 	//    the Set. It will exist forever. There is a command-line way of deleting a Set which is not
 	//    going to work for our needs. Hence, in our DPS implementation we are going to recycle these
 	//    1000 Set containers for our different user created stores. If more than 1000 user created
 	//    stores are needed, then Aerospike is not a viable choice.
 	//
 	// Get a store id slot that is not currently in use.
 	string errorMsg = "";
 	bool result = getStoreIdNotInUse(name, storeId, errorMsg);

 	if (result == false) {
 		dbError.set("Failed to get a store id that is not in use for a store named " +
			name + ". " + errorMsg, AEROSPIKE_GET_STORE_ID_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to get a store id that is not in use for a store named " << name << ". " <<
			errorMsg << ". " << AEROSPIKE_GET_STORE_ID_ERROR, "AerospikeDBLayer");
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
 	}

	/*
	We secured a guid. We can now create this store. Layout for a distributed process store (dps) looks like this in Aerospike.
	In the Aerospike database, DPS store/lock, and the individual store details will follow these data formats.
	That will allow us to be as close to the other DPS implementations using memcached, Redis, Cassandra, Cloudant, HBase, Mongo and Couchbase.
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

 	// We will do the first two activities from the list above for our Aerospike implementation.
	// Let us now do the step 1 described in the commentary above.
	// 1) Create the Store Name entry in the dps_dl_meta_data Set.
	//    '0' + 'store name' => 'store id'
	std::ostringstream storeIdString;
	storeIdString << storeId;
	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;

	// Aerospike data model:  Namespace-->Set-->Records-->Bins
	// Aerospike will create the set only if it doesn't already exist.
	as_record rec1;
	// Create a record with two bins.
	as_record_inita(&rec1, 2);
	// Create the K/V bins with their string values.
	as_record_set_str(&rec1, "k", dpsAndDlGuidKey.c_str());
	string tmpStoreIdString = storeIdString.str();
	as_record_set_str(&rec1, "v", tmpStoreIdString.c_str());
	// Set the TTL value to never expire.
	rec1.ttl = -1;
	// Require that the write succeeds only if the record doesn't exist.
	as_policy_write wpol;
	as_policy_write_init(&wpol);
	wpol.exists = AS_POLICY_EXISTS_CREATE;
	// Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	as_key key1;
	as_key_init_str(&key1, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		string(DPS_DL_META_DATA_SET).c_str(), dpsAndDlGuidKey.c_str());
	as_error err;

	if (aerospike_key_put(as, &err, &wpol, &key1, &rec1) != AEROSPIKE_OK) {
		char errorString[400];
		sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		errorMsg = string(errorString);
		dbError.set("Unable to create 'StoreName-->GUID' in Aerospike for the store named " + name +
			". Aerospike insert error: " + errorMsg, DPS_STORE_NAME_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'StoreName-->GUID' in Aerospike for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_NAME_CREATION_ERROR, "AerospikeDBLayer");
		as_record_destroy(&rec1);
		// This error is a serious error since we claimed that store id slot that was available for use.
		// A write error at this stage is critical.
		// In Aerospike, there is no API to delete or drop a set. Hence, let us put back this store id slot's state as NOT IN USE.
		setStoreIdToNotInUse(storeId, errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

	// 2) Create the Store Contents set now.
 	// This is the Aerospike set where we will keep storing all the key value pairs belonging to this store.
	// Set name: 'dps_' + '1_' + 'store id'
 	string storeSetName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString.str();
	// Every store contents set will always have these three metadata entries:
	// dps_name_of_this_store ==> 'store name'
	// dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	// dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	//
	// Every store contents set will have at least three entries in it carrying the actual store name, key spl type name and value spl type name.
	// In addition, inside this store contents set, we will house the
	// actual key:value data items that the user wants to keep in this store.
	// Such a store contents set is very useful for data item read, write, deletion, enumeration etc.
 	//
	// Let us populate the new store contents set with a mandatory element that will carry the name of this store.
	// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
	as_record rec2;
	// Create a record with two bins.
	as_record_inita(&rec2, 2);
	// Create the K/V bins with their string values.
	as_record_set_str(&rec2, "k", string(AEROSPIKE_STORE_ID_TO_STORE_NAME_KEY).c_str());
	as_record_set_str(&rec2, "v", base64_encoded_name.c_str());
	// Set the TTL value to never expire.
	rec2.ttl = -1;
	// Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	as_key key2;
	as_key_init_str(&key2, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		storeSetName.c_str(), string(AEROSPIKE_STORE_ID_TO_STORE_NAME_KEY).c_str());

	if (aerospike_key_put(as, &err, &wpol, &key2, &rec2) != AEROSPIKE_OK) {
		char errorString[400];
		sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		errorMsg = string(errorString);
		dbError.set("Unable to create 'Meta Data1' in Aerospike for the store named " + name +
			". Aerospike insert error: " + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'StoreName-->GUID' in Aerospike for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "AerospikeDBLayer");
		// Delete the store name entry we created above.
		aerospike_key_remove(as, &err, NULL, &key1);
		// Free all the records we allocated in the stack above.
		as_record_destroy(&rec1);
		as_record_destroy(&rec2);
		// This error is a serious error since we claimed that store id slot that was available for use.
		// A write error at this stage is critical.
		// In Aerospike, there is no API to delete or drop a set. Hence, let us put back this store id slot's state as NOT IN USE.
		setStoreIdToNotInUse(storeId, errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

    // We are now going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
	// Add the key spl type name metadata.
 	string base64_encoded_keySplTypeName;
 	base64_encode(keySplTypeName, base64_encoded_keySplTypeName);
	as_record rec3;
	// Create a record with two bins.
	as_record_inita(&rec3, 2);
	// Create the K/V bins with their string values.
	as_record_set_str(&rec3, "k", string(AEROSPIKE_SPL_TYPE_NAME_OF_KEY).c_str());
	as_record_set_str(&rec3, "v", base64_encoded_keySplTypeName.c_str());
	// Set the TTL value to never expire.
	rec3.ttl = -1;
	// Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	as_key key3;
	as_key_init_str(&key3, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		storeSetName.c_str(), string(AEROSPIKE_SPL_TYPE_NAME_OF_KEY).c_str());

	if (aerospike_key_put(as, &err, &wpol, &key3, &rec3) != AEROSPIKE_OK) {
		char errorString[400];
		sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		errorMsg = string(errorString);
		dbError.set("Unable to create 'Meta Data2' in Aerospike for the store named " + name +
			". Aerospike insert error: " + errorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'Meta Data2' in Aerospike for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "AerospikeDBLayer");
		// Delete the store name entry we created above.
		aerospike_key_remove(as, &err, NULL, &key1);
		// Delete the meta data1 entry we created above.
		aerospike_key_remove(as, &err, NULL, &key2);
		// Free all the records we allocated in the stack above.
		as_record_destroy(&rec1);
		as_record_destroy(&rec2);
		as_record_destroy(&rec3);
		// This error is a serious error since we claimed that store id slot that was available for use.
		// A write error at this stage is critical.
		// In Aerospike, there is no API to delete or drop a set. Hence, let us put back this store id slot's state as NOT IN USE.
		setStoreIdToNotInUse(storeId, errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

	// Add the value spl type name metadata.
	string base64_encoded_valueSplTypeName;
	base64_encode(valueSplTypeName, base64_encoded_valueSplTypeName);
	as_record rec4;
	// Create a record with two bins.
	as_record_inita(&rec4, 2);
	// Create the K/V bins with their string values.
	as_record_set_str(&rec4, "k", string(AEROSPIKE_SPL_TYPE_NAME_OF_VALUE).c_str());
	as_record_set_str(&rec4, "v", base64_encoded_valueSplTypeName.c_str());
	// Set the TTL value to never expire.
	rec4.ttl = -1;
	// Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	as_key key4;
	as_key_init_str(&key4, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		storeSetName.c_str(), string(AEROSPIKE_SPL_TYPE_NAME_OF_VALUE).c_str());

	if (aerospike_key_put(as, &err, &wpol, &key4, &rec4) != AEROSPIKE_OK) {
		char errorString[400];
		sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		errorMsg = string(errorString);
		dbError.set("Unable to create 'Meta Data3' in Aerospike for the store named " + name +
			". Aerospike insert error: " + errorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'Meta Data3' in Aerospike for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "AerospikeDBLayer");
		// Delete the store name entry we created above.
		aerospike_key_remove(as, &err, NULL, &key1);
		// Delete the meta data1 entry we created above.
		aerospike_key_remove(as, &err, NULL, &key2);
		// Delete the meta data2 entry we created above.
		aerospike_key_remove(as, &err, NULL, &key3);
		// Free all the records we allocated in the stack above.
		as_record_destroy(&rec1);
		as_record_destroy(&rec2);
		as_record_destroy(&rec3);
		as_record_destroy(&rec4);
		// This error is a serious error since we claimed that store id slot that was available for use.
		// A write error at this stage is critical.
		// In Aerospike, there is no API to delete or drop a set. Hence, let us put back this store id slot's state as NOT IN USE.
		setStoreIdToNotInUse(storeId, errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	} else {
		// We created a new store as requested by the caller. Return the store id.
		// Free all the records we allocated in the stack above.
		as_record_destroy(&rec1);
		as_record_destroy(&rec2);
		as_record_destroy(&rec3);
		as_record_destroy(&rec4);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(storeId);
	}
  }

  uint64_t AerospikeDBLayer::createOrGetStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	// We will rely on a method above this and another method below this to accomplish what is needed here.
	SPLAPPTRC(L_DEBUG, "Inside createOrGetStore for store " << name, "AerospikeDBLayer");
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
	// In Aerospike, store ids get reused from an available pool of 1000 Aerospike Set containers.
	// In the DB error message we got above, it contains the existing store id as the very last token of that sentence.
	// Let us parse and return that to the caller. (Can you think of any other crazy way to do this?)
	std::vector<std::string> words;
	streams_boost::split(words, dbError.getErrorStr(), streams_boost::is_any_of(" "), streams_boost::token_compress_on);
	int32_t sizeOfWords = words.size();
	storeId = (uint64_t)atoi((words.at(sizeOfWords-1)).c_str());
	// Reset the DB error we set earlier.
	dbError.reset();
	return(storeId);
  }
                
  uint64_t AerospikeDBLayer::findStore(std::string const & name, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside findStore for store " << name, "AerospikeDBLayer");

	// We can search in the dps_dl_meta_data Set to see if a store exists for the given store name.
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);
	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
	string errorMsg = "";
	string value = "";
	bool result = readAerospikeBinValue(string(DPS_DL_META_DATA_SET), dpsAndDlGuidKey.c_str(), value, errorMsg);

    if (result == true) {
		// This store already exists in our cache.
 	 	uint64_t storeId = (uint64_t)atoi(value.c_str());
 	 	return(storeId);
    } else {
		// Requested data item is not there in the cache.
		dbError.set("The requested store " + name + " can't be found. Error=" + errorMsg, DPS_DATA_ITEM_READ_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed to locate the store " <<
			name << ". Error=" << errorMsg << ". " << DPS_DATA_ITEM_READ_ERROR, "AerospikeDBLayer");
		return(0);
    }
  }
        
  bool AerospikeDBLayer::removeStore(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside removeStore for store id " << store, "AerospikeDBLayer");

	ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "AerospikeDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "AerospikeDBLayer");
		// User has to retry again to remove the store.
		return(false);
	}

	// Get rid of these two entries that are holding the store contents and the store name root entry.
	// 1) Store Contents Set
	// 2) Store Name root entry
	//
	uint32_t dataItemCnt = 0;
	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		releaseStoreLock(storeIdString);
		// This is alarming. This will put this store in a bad state. Poor user has to deal with it.
		return(false);
	}

	// In Aerospike, there is no API to delete an entire set.
	// That has to be done by command line tools which will not help our cause here.
	// Hence let us delete all the items in the given set one at a time by using the Aerospike scan API.
	//
	// Aerospike's scan API works in a callback fashion.
	// Let us delete all the entries in this store.
	// Set name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
 	string storeSetName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
 	as_error err;
 	as_scan scan;
 	as_scan_init(&scan, string(AEROSPIKE_DPS_NAMESPACE).c_str(), storeSetName.c_str());
 	as_scan_select_inita(&scan, 1);
 	as_scan_select(&scan, "v");

 	// Callback method will be called for each scanned record.
 	// When all the records have been scanned, then the callback method will be called with a NULL value for that record.
 	// Please note below that we are passing the object pointer to this class as an argument that will be sent to our static callback method.
 	if (aerospike_scan_foreach(as, &err, NULL, &scan, remove_store_callback, this) != AEROSPIKE_OK ) {
 		char errorString[400];
 		sprintf(errorString, "Inside removeStore, it failed to delete the records in the Aerospike Set for store id %s. rc=%d, msg=%s at [%s:%d]",
			storeIdString.c_str(), err.code, err.message, err.file, err.line);
 		dbError.set(string(errorString), DPS_STORE_REMOVAL_ERROR);
		SPLAPPTRC(L_DEBUG, string(errorString) << ". " << DPS_STORE_REMOVAL_ERROR, "AerospikeDBLayer");
		as_scan_destroy(&scan);
		releaseStoreLock(storeIdString);
		return(false);
 	}

 	// This store has been removed now. As described at the top of this file,
 	// Aerospike allows only a limited number of fixed store ids (a total of 1023).
 	// Hence, we have to do some book keeping to reset the store id used for this recently
 	// deleted store.
 	// Set this store id slot to be "not in use" so that it can be reused for a new store in the future.
 	as_scan_destroy(&scan);
 	string errorMsg = "";
 	setStoreIdToNotInUse(store, errorMsg);

	// Delete the store name entry in the meta data set.
	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + storeName;
 	as_key key2;
 	as_key_init_str(&key2, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		string(DPS_DL_META_DATA_SET).c_str(), dpsAndDlGuidKey.c_str());

 	if (aerospike_key_remove(as, &err, NULL, &key2) != AEROSPIKE_OK) {
 		// Error in removing the store name entry in the meta data Set.
 		char errorString[400];
 		sprintf(errorString, "Inside removeStore, it failed to remove the GUID entry for store id %s. rc=%d, msg=%s at [%s:%d]",
			storeIdString.c_str(), err.code, err.message, err.file, err.line);
		SPLAPPTRC(L_DEBUG, string(errorString), "AerospikeDBLayer");
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
  bool AerospikeDBLayer::put(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside put for store id " << store, "AerospikeDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In our Aerospike dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	// Let us convert the binary formatted valueData in our K/V pair to a base64 encoded string.
	string base64_encoded_data_item_value;
	b64_encode(valueData, valueSize, base64_encoded_data_item_value);

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Set that takes the following name.
	// Set name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeSetName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	// Aerospike data model:  Namespace-->Set-->Records-->Bins
	// Aerospike will create the set only if it doesn't already exist.
	as_record rec;
	// Create a record with two bins.
	as_record_inita(&rec, 2);
	// Create the K/V bins with their string values.
	as_record_set_str(&rec, "k", base64_encoded_data_item_key.c_str());
	as_record_set_str(&rec, "v", base64_encoded_data_item_value.c_str());
	// Set the TTL value to be forever.
	rec.ttl = -1;

	// Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	as_key key;
	as_key_init_str(&key, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		storeSetName.c_str(), base64_encoded_data_item_key.c_str());

	// Require that the write succeeds for creating a new data item as well as updating an existing data item.
	as_policy_write wpol;
	as_policy_write_init(&wpol);
	wpol.exists = AS_POLICY_EXISTS_IGNORE;
	as_error err;

	if (aerospike_key_put(as, &err, &wpol, &key, &rec) == AEROSPIKE_OK) {
		as_record_destroy(&rec);
		return(true);
	} else {
		char errorString[400];
		sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		string errorMsg = string(errorString);
		// Error while updating the K/V pair in our store collection.
		dbError.set("Unable to store/update a data item in the store id " + storeIdString +
			". Error: " + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed to store/update a data item in the store id " << storeIdString << ". "
			<< ". Error: " << errorMsg << ". " << DPS_DATA_ITEM_WRITE_ERROR, "AerospikeDBLayer");
		as_record_destroy(&rec);
		return(false);
	}
  }

  // This is a special bullet proof version that does several safety checks before putting a data item into a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular put method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool AerospikeDBLayer::putSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside putSafe for store id " << store, "AerospikeDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to find a store with a store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "AerospikeDBLayer");
		}

		return(false);
	}

	// In our Aerospike dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	// Let us convert the binary formatted valueData in our K/V pair to a base64 encoded string.
	string base64_encoded_data_item_value;
	b64_encode(valueData, valueSize, base64_encoded_data_item_value);

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "AerospikeDBLayer");
		// User has to retry again to put the data item in the store.
		return(false);
	}

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Set that takes the following name.
	// Set name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeSetName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	// Aerospike data model:  Namespace-->Set-->Records-->Bins
	// Aerospike will create the set only if it doesn't already exist.
	as_record rec;
	// Create a record with two bins.
	as_record_inita(&rec, 2);
	// Create the K/V bins with their string values.
	as_record_set_str(&rec, "k", base64_encoded_data_item_key.c_str());
	as_record_set_str(&rec, "v", base64_encoded_data_item_value.c_str());
	// Set the TTL value to be forever.
	rec.ttl = -1;

	// Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	as_key key;
	as_key_init_str(&key, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		storeSetName.c_str(), base64_encoded_data_item_key.c_str());

	// Require that the write succeeds for creating a new data item as well as updating an existing data item.
	as_policy_write wpol;
	as_policy_write_init(&wpol);
	wpol.exists = AS_POLICY_EXISTS_IGNORE;
	as_error err;

	if (aerospike_key_put(as, &err, &wpol, &key, &rec) == AEROSPIKE_OK) {
		as_record_destroy(&rec);
		releaseStoreLock(storeIdString);
		return(true);
	} else {
		char errorString[400];
		sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		string errorMsg = string(errorString);
		// Error while updating the K/V pair in our store collection.
		dbError.set("Unable to store/update a data item in the store id " + storeIdString +
			". Error: " + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to store/update a data item in the store id " << storeIdString << ". "
			<< ". Error: " << errorMsg << ". " << DPS_DATA_ITEM_WRITE_ERROR, "AerospikeDBLayer");
		as_record_destroy(&rec);
		releaseStoreLock(storeIdString);
		return(false);
	}
  }

  // Put a data item with a TTL (Time To Live in seconds) value into the global area of the Aerospike K/V store.
  bool AerospikeDBLayer::putTTL(char const * keyData, uint32_t keySize,
		  	  	  	  	    unsigned char const * valueData, uint32_t valueSize,
							uint32_t ttl, PersistenceError & dbError)
  {
	  SPLAPPTRC(L_DEBUG, "Inside putTTL.", "AerospikeDBLayer");

	  // In our Aerospike dps implementation, data item keys can have space characters.
	  string base64_encoded_data_item_key;
	  base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	  // Let us convert the binary formatted valueData in our K/V pair to a base64 encoded string.
	  string base64_encoded_data_item_value;
	  b64_encode(valueData, valueSize, base64_encoded_data_item_value);

	  uint64_t _ttl = ttl;
	  if (_ttl == 0) {
		  // User wants this TTL to be forever.
		  // In the case of the DPS Aerospike layer, we will use a TTL value of 25 years represented in seconds.
		  // If our caller opts for this, then this long-term TTL based K/V pair can be removed using
		  // the dpsRemoveTTL API anytime a premature removal of that K/V pair is needed.
		  _ttl = AEROSPIKE_MAX_TTL_VALUE;
	  }

	  // We are ready to either store a new data item or update an existing data item.
	  // This action is performed on the Store Set that takes the following name.
	  // Set name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	  string storeSetName = string(DPS_TTL_STORE_TOKEN);
	  // Aerospike data model:  Namespace-->Set-->Records-->Bins
	  // Aerospike will create the set only if it doesn't already exist.
	  as_record rec;
	  // Create a record with two bins.
	  as_record_inita(&rec, 2);
	  // Create the K/V bins with their string values.
	  as_record_set_str(&rec, "k", base64_encoded_data_item_key.c_str());
	  as_record_set_str(&rec, "v", base64_encoded_data_item_value.c_str());
	  // Set the TTL value.
	  rec.ttl = (uint32_t)_ttl;

	  // Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	  as_key key;
	  as_key_init_str(&key, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		  storeSetName.c_str(), base64_encoded_data_item_key.c_str());

	  // Require that the write succeeds for creating a new data item as well as updating an existing data item.
	  as_policy_write wpol;
	  as_policy_write_init(&wpol);
	  wpol.exists = AS_POLICY_EXISTS_IGNORE;
	  as_error err;

	  if (aerospike_key_put(as, &err, &wpol, &key, &rec) == AEROSPIKE_OK) {
		  as_record_destroy(&rec);
		  return(true);
	  } else {
		  char errorString[400];
		  sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		  string errorMsg = string(errorString);
		  // Error while updating the K/V pair in our store collection.
		  dbError.setTTL("Unable to store a TTL based data item. Error: " + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside putTTL, it failed to store/update a TTL based data item. Error: " <<
			  errorMsg << ". " << DPS_DATA_ITEM_WRITE_ERROR, "AerospikeDBLayer");
		  as_record_destroy(&rec);
		  return(false);
	  }
  }

  // This is a lean and mean get operation from a store.
  // It doesn't do any safety checks before getting a data item from a store.
  // If you want to go through that rigor, please use the getSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool AerospikeDBLayer::get(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside get for store id " << store, "AerospikeDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In our Aerospike dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);

	bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		false, true, valueData, valueSize, dbError);

	if ((result == false) || (dbError.hasError() == true)) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside get, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
	}

    return(result);
  }

  // This is a special bullet proof version that does several safety checks before getting a data item from a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular get method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool AerospikeDBLayer::getSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside getSafe for store id " << store, "AerospikeDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "AerospikeDBLayer");
		}

		return(false);
	}

	// In our Aerospike dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);

	bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		false, false, valueData, valueSize, dbError);

	if ((result == false) || (dbError.hasError() == true)) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
	}

    return(result);
  }

  // Get a TTL based data item that is stored in the global area of the Aerospike K/V store.
   bool AerospikeDBLayer::getTTL(char const * keyData, uint32_t keySize,
                              unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
   {
		SPLAPPTRC(L_DEBUG, "Inside getTTL.", "AerospikeDBLayer");

		// Since this is a data item with TTL, it is stored in the global area of Aerospike and not inside a user created store.
		string data_item_key = string(keyData, keySize);
		string base64_encoded_data_item_key;
		base64_encode(data_item_key, base64_encoded_data_item_key);

		// Get the data from the TTL table (not from the user created tables).
		// This action is performed on the TTL store.
		string storeSetName = string(DPS_TTL_STORE_TOKEN);
		// Aerospike data model:  Namespace-->Set-->Records-->Bins
		// Aerospike will create the set only if it doesn't already exist.
		string errorMsg = "";
		string value = "";
		bool result = readAerospikeBinValue(storeSetName, base64_encoded_data_item_key.c_str(), value, errorMsg);

		if (errorMsg != "") {
			// Check if it is a record not found error.
			std::ostringstream noRecordError;
			noRecordError << "rc=" << AEROSPIKE_ERR_RECORD_NOT_FOUND;

			if (errorMsg.find(noRecordError.str()) == std::string::npos) {
				// This means some other error in reading a record from the given Aerospike Set.
				// Error message means something went wrong in the call we made above.
				// Unable to get the requested data item from the cache.
				dbError.setTTL("Unable to access the TTL based K/V pair in Aerospike. " + errorMsg, DPS_DATA_ITEM_READ_ERROR);
				return(false);
			}
		}

		// If the data item is not there, we can't do much at this point.
		if (result == false) {
		  // This data item doesn't exist. Let us raise an error.
		  // Requested data item is not there in the cache.
		  dbError.setTTL("The requested TTL based data item doesn't exist.", DPS_DATA_ITEM_READ_ERROR);
		   return(false);
		} else {
			// In Aerospike, when we put the TTL based K/V pair, we b64_encoded our binary value buffer and
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

  bool AerospikeDBLayer::remove(uint64_t store, char const * keyData, uint32_t keySize,
                                PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside remove for store id " << store, "AerospikeDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "AerospikeDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "AerospikeDBLayer");
		// User has to retry again to remove the data item from the store.
		return(false);
	}

	// In our Aerospike dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	// We are ready to remove a data item from the Aerospike collection.
	// This action is performed on the Store Set that takes the following name.
	// Set name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeSetName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;

	// Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	as_key key;
	as_key_init_str(&key, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		storeSetName.c_str(), base64_encoded_data_item_key.c_str());
	as_error err;

	if (aerospike_key_remove(as, &err, NULL, &key) != AEROSPIKE_OK) {
		char errorString[400];
		sprintf(errorString, "Error: rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		dbError.set("Aerospike error while removing the requested data item from the store id " + storeIdString +
			". " + string(errorString), DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed to remove a data item from store id " << storeIdString <<
			". " << string(errorString) << ". " << DPS_DATA_ITEM_DELETE_ERROR, "AerospikeDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// All done. An existing data item in the given store has been removed.
	releaseStoreLock(storeIdString);
    return(true);
  }

  // Remove a TTL based data item that is stored in the global area of the Aerospike K/V store.
  bool AerospikeDBLayer::removeTTL(char const * keyData, uint32_t keySize,
                                PersistenceError & dbError)
  {
		SPLAPPTRC(L_DEBUG, "Inside removeTTL.", "AerospikeDBLayer");

		// In our Aerospike dps implementation, data item keys can have space characters.
		string base64_encoded_data_item_key;
		base64_encode(string(keyData, keySize), base64_encoded_data_item_key);

		// This action is performed on the TTL store
		string storeSetName = string(DPS_TTL_STORE_TOKEN);

		// Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
		as_key key;
		as_key_init_str(&key, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
			storeSetName.c_str(), base64_encoded_data_item_key.c_str());
		as_error err;

		if (aerospike_key_remove(as, &err, NULL, &key) != AEROSPIKE_OK) {
			char errorString[400];
			sprintf(errorString, "Error: rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
			dbError.setTTL("Aerospike error while removing the requested TTL based data item from the TTL store. " +
				string(errorString), DPS_DATA_ITEM_DELETE_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to remove a TTL based data item from the TTL store. " <<
				string(errorString) << ". " << DPS_DATA_ITEM_DELETE_ERROR, "AerospikeDBLayer");
			return(false);
		}

		// All done. An existing data item in the TTL store has been removed.
		return(true);
  }

  bool AerospikeDBLayer::has(uint64_t store, char const * keyData, uint32_t keySize,
                             PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside has for store id " << store, "AerospikeDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "AerospikeDBLayer");
		}

		return(false);
	}

	// In our Aerospike dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);
	string storeSetName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string value = "";
	string errorMsg = "";
	bool result = readAerospikeBinValue(storeSetName, base64_encoded_data_item_key.c_str(), value, errorMsg);

	if (result == true) {
		return(true);
	} else {
		// Check if it is a record not found error.
		std::ostringstream noRecordError;
		noRecordError << "rc=" << AEROSPIKE_ERR_RECORD_NOT_FOUND;

		if (errorMsg.find(noRecordError.str()) != std::string::npos) {
			// This means record not found in the given Aerospike Set.
			return(false);
		} else {
			// Some other Aerospike error.
			dbError.set("Inside has, it failed for store id " + storeIdString + ". " +
				errorMsg, DPS_KEY_EXISTENCE_CHECK_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeIdString << ". " <<
				errorMsg << ". " << DPS_KEY_EXISTENCE_CHECK_ERROR, "AerospikeDBLayer");
			return(false);
		}
	}
  }

  // Check for the existence of a TTL based data item that is stored in the global area of the Aerospike K/V store.
  bool AerospikeDBLayer::hasTTL(char const * keyData, uint32_t keySize,
                             PersistenceError & dbError)
  {
		SPLAPPTRC(L_DEBUG, "Inside hasTTL.", "AerospikeDBLayer");

		// Since this is a data item with TTL, it is stored in the global area of Aerospike and not inside a user created store.
		string data_item_key = string(keyData, keySize);
		string base64_encoded_data_item_key;
		base64_encode(data_item_key, base64_encoded_data_item_key);

		// Check for the data item existence in the TTL table (not in the user created tables).
		string errorMsg = "";
		string value = "";
		string storeSetName = string(DPS_TTL_STORE_TOKEN);
		bool result = readAerospikeBinValue(storeSetName, base64_encoded_data_item_key.c_str(), value, errorMsg);

		if (result == true) {
			return(true);
		} else {
			// Check if it is a record not found error.
			std::ostringstream noRecordError;
			noRecordError << "rc=" << AEROSPIKE_ERR_RECORD_NOT_FOUND;

			if (errorMsg.find(noRecordError.str()) != std::string::npos) {
				// This means record not found in the given Aerospike Set.
				return(false);
			} else {
				// Some other Aerospike error.
				dbError.setTTL("Inside hasTTL, it failed for the TTL store. " +
					errorMsg, DPS_KEY_EXISTENCE_CHECK_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside hasTTL, it failed for the TTL store. " <<
					errorMsg << ". " << DPS_KEY_EXISTENCE_CHECK_ERROR, "AerospikeDBLayer");
				return(false);
			}
		}
  }

  void AerospikeDBLayer::clear(uint64_t store, PersistenceError & dbError) {
     SPLAPPTRC(L_DEBUG, "Inside clear for store id " << store, "AerospikeDBLayer");

 	ostringstream storeId;
 	storeId << store;
 	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "AerospikeDBLayer");
		}

		return;
	}

 	// Lock the store first.
 	if (acquireStoreLock(storeIdString) == false) {
 		// Unable to acquire the store lock.
 		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside clear, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "AerospikeDBLayer");
 		// User has to retry again to remove the store.
 		return;
 	}

	// We will delete all the entries in this store except the 3 meta data entries.
	// Aerospike's scan API works in a callback fashion.
	// Set name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
 	string storeSetName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
 	as_error err;
 	as_scan scan;
 	as_scan_init(&scan, string(AEROSPIKE_DPS_NAMESPACE).c_str(), storeSetName.c_str());
 	as_scan_select_inita(&scan, 1);
 	as_scan_select(&scan, "v");

 	// Callback method will be called for each scanned record.
 	// When all the records have been scanned, then the callback method will be called with a NULL value for that record.
 	// Please note below that we are passing the object pointer to this class as an argument that will be sent to our static callback method.
 	if (aerospike_scan_foreach(as, &err, NULL, &scan, clear_store_callback, this) != AEROSPIKE_OK ) {
 		char errorString[400];
 		sprintf(errorString, "Inside clear, it failed for store id %s. rc=%d, msg=%s at [%s:%d]",
			storeIdString.c_str(), err.code, err.message, err.file, err.line);
		dbError.set("Unable to clear the Aerospike Set for store id " + storeIdString +
			". Error:" + string(errorString), DPS_STORE_CLEARING_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed to clear the Aerospike Set for store id " <<
			storeIdString << ". Error:" << string(errorString) << ". " << DPS_STORE_CLEARING_ERROR, "AerospikeDBLayer");
 	}

 	as_scan_destroy(&scan);
 	releaseStoreLock(storeIdString);
  }
        
  uint64_t AerospikeDBLayer::size(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside size for store id " << store, "AerospikeDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	string storeSetName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string errorMsg = "";
	int64_t setSize = 0;
	bool result = readAerospikeSetSize(storeSetName, setSize, errorMsg);

	if (result == false) {
		dbError.set("Failed when trying to get the size for store id " + storeIdString + ". " + errorMsg, DPS_GET_STORE_SIZE_ERROR);
		return (false);
	} else if (setSize <= 0) {
		// This is not correct. We must have a minimum hash size of 3 because of the reserved elements.
		dbError.set("Wrong value (zero) observed as the store size for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_STORE_SIZE_ERROR);
		return (false);
	}

	// Our Store Contents Set for every store will have a mandatory reserved internal elements (store name, key spl type name, and value spl type name).
	// Let us not count those three elements in the actual store contents size that the caller wants now.
	return((uint64_t)(setSize - 3));
  }

  // We allow space characters in the data item keys.
  // Hence, it is required to based64 encode them before storing the
  // key:value data item in Aerospike.
  // (Use boost functions to do this.)
  //
  void AerospikeDBLayer::base64_encode(std::string const & str, std::string & base64) {
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
  void AerospikeDBLayer::base64_decode(std::string & base64, std::string & result) {
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
  bool AerospikeDBLayer::storeIdExistsOrNot(string storeIdString, PersistenceError & dbError) {
	  string storeSetName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	  string key = string(AEROSPIKE_STORE_ID_TO_STORE_NAME_KEY);
	  string errorMsg = "";
	  string value = "";
	  bool result = readAerospikeBinValue(storeSetName, key.c_str(), value, errorMsg);

	  if (result == true) {
		  // This store exists in our cache.
		  return(true);
	  } else {
		  // Unable to access a K/V entry in the store contents collection for the given store id.
		  dbError.set("StoreIdExistsOrNot: Unable to get StoreContents meta data1 for the StoreId " + storeIdString +
		     ". Error: " + errorMsg, DPS_GET_STORE_CONTENTS_HASH_ERROR);
		  return(false);
	  }
  }

  // This method will acquire a lock for a given store.
  bool AerospikeDBLayer::acquireStoreLock(string const & storeIdString) {
	  int32_t retryCnt = 0;

	  // '4' + 'store id' + 'dps_lock' => 1
	  std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;
	  uint64_t lockId = SPL::Functions::Utility::hashCode(storeLockKey);
	  ostringstream lockIdStr;
	  lockIdStr << lockId;
	  string tmpLockIdStr = lockIdStr.str();

	  // Aerospike data model:  Namespace-->Set-->Records-->Bins
	  // Aerospike will create the set only if it doesn't already exist.
	  as_record rec;
	  // Create a record with two bins.
	  as_record_inita(&rec, 2);
	  // Create the K/V bins with their string values.
	  as_record_set_str(&rec, "k", storeLockKey.c_str());
	  as_record_set_str(&rec, "v", tmpLockIdStr.c_str());
	  // Set the TTL value.
	  rec.ttl = (uint32_t)DPS_AND_DL_GET_LOCK_TTL;

	  // Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	  as_key key;
	  as_key_init_str(&key, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		  string(DPS_DL_META_DATA_SET).c_str(), storeLockKey.c_str());

	  // Require that the write succeeds only if the record doesn't exist.
	  as_policy_write wpol;
	  as_policy_write_init(&wpol);
	  wpol.exists = AS_POLICY_EXISTS_CREATE;

	  // Try to get a lock for this generic entity.
	  while (1) {
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or the Aerospike removes it after the TTL value expires.
		// Make an attempt to create a record for this lock now.
		as_error err;
		if (aerospike_key_put(as, &err, &wpol, &key, &rec) == AEROSPIKE_OK) {
			// We got the lock.
			as_record_destroy(&rec);
			return(true);
		}

		char errorString[400];
		sprintf(errorString, "Store specific lock acquisition error: rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		SPLAPPTRC(L_DEBUG, string(errorString), "AerospikeDBLayer");
		// Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
			as_record_destroy(&rec);
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

	  as_record_destroy(&rec);
	  return(false);
  }

  void AerospikeDBLayer::releaseStoreLock(string const & storeIdString) {
	  // '4' + 'store id' + 'dps_lock' => 1
	  std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;
	  // Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	  as_key key;
	  as_key_init_str(&key, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		  string(DPS_DL_META_DATA_SET).c_str(), storeLockKey.c_str());
	  as_error err;

	  if (aerospike_key_remove(as, &err, NULL, &key) == AEROSPIKE_OK) {
		  return;
	  } else {
		  char errorString[400];
		  sprintf(errorString, "Store specific lock release error. rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		  SPLAPPTRC(L_DEBUG, string(errorString), "AerospikeDBLayer");
		  return;
	  }
  }

  bool AerospikeDBLayer::readStoreInformation(std::string const & storeIdString, PersistenceError & dbError,
		  uint32_t & dataItemCnt, std::string & storeName, std::string & keySplTypeName, std::string & valueSplTypeName) {
	  // Read the store name, this store's key and value SPL type names,  and get the store size.
	  storeName = "";
	  keySplTypeName = "";
	  valueSplTypeName = "";
	  dataItemCnt = 0;
	  string storeSetName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;

	  // This action is performed on the Store Contents Set that takes the following name.
	  // 'dps_' + '1_' + 'store id'
	  // It will always have the following three metadata entries:
	  // dps_name_of_this_store ==> 'store name'
	  // dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	  // dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	  // 1) Get the store name.
	  string key = string(AEROSPIKE_STORE_ID_TO_STORE_NAME_KEY);
	  string errorMsg = "";
	  string value = "";

	  bool result = readAerospikeBinValue(storeSetName, key.c_str(), value, errorMsg);

	  if (result == true) {
		  storeName = value;
	  } else {
		  // Unable to get the store name for this store's key.
		  dbError.set("Unable to get the store name for StoreId " + storeIdString + ". Error: " + errorMsg, DPS_GET_STORE_NAME_ERROR);
		  return (false);
	  }

	  // 2) Let us get the spl type name for this store's key.
	  key = string(AEROSPIKE_SPL_TYPE_NAME_OF_KEY);
	  errorMsg = "";
	  value = "";

	  result = readAerospikeBinValue(storeSetName, key.c_str(), value, errorMsg);

	  if (result == true) {
		  keySplTypeName = value;
	  } else {
		  // Unable to get the spl type name for this store's key.
		  dbError.set("Unable to get the key spl type name for StoreId " + storeIdString + ". Error: " + errorMsg, DPS_GET_KEY_SPL_TYPE_NAME_ERROR);
		  return (false);
	  }

	  // 3) Let us get the spl type name for this store's value.
	  key = string(AEROSPIKE_SPL_TYPE_NAME_OF_VALUE);
	  errorMsg = "";
	  value = "";

	  result = readAerospikeBinValue(storeSetName, key.c_str(), value, errorMsg);

	  if (result == true) {
		  valueSplTypeName = value;
	  } else {
		  // Unable to get the spl type name for this store's value.
		  dbError.set("Unable to get the value spl type name for StoreId " + storeIdString + ". Error: " + errorMsg, DPS_GET_VALUE_SPL_TYPE_NAME_ERROR);
		  return (false);
	  }

	  // 4) Let us get the size of the store contents set now.
	  errorMsg = "";
	  int64_t setSize = 0;
	  result = readAerospikeSetSize(storeSetName, setSize, errorMsg);

	  if (result == false) {
		  dbError.set("Failed when trying to get the size for store id " + storeIdString + ". Error: " + errorMsg, DPS_GET_STORE_SIZE_ERROR);
		  return (false);
	  } else if (setSize <= 0) {
		  // This is not correct. We must have a minimum hash size of 3 because of the reserved elements.
		  dbError.set("Wrong value (zero) observed as the store size for StoreId " + storeIdString + ". Error: " + errorMsg, DPS_GET_STORE_SIZE_ERROR);
		  return (false);
	  }

	  // Our Store Contents collection for every store will have a mandatory reserved internal elements (store name, key spl type name, and value spl type name).
	  // Let us not count those three elements in the actual store contents size that the caller wants now.
	  dataItemCnt = (uint32_t)setSize;
	  dataItemCnt -= 3;
	  return(true);
  }

  string AerospikeDBLayer::getStoreName(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "AerospikeDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		  return("");
	  }

	  string base64_decoded_storeName;
	  base64_decode(storeName, base64_decoded_storeName);
	  return(base64_decoded_storeName);
  }

  string AerospikeDBLayer::getSplTypeNameForKey(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "AerospikeDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		  return("");
	  }

	  string base64_decoded_keySplTypeName;
	  base64_decode(keySplTypeName, base64_decoded_keySplTypeName);
	  return(base64_decoded_keySplTypeName);
  }

  string AerospikeDBLayer::getSplTypeNameForValue(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "AerospikeDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		  return("");
	  }

	  string base64_decoded_valueSplTypeName;
	  base64_decode(valueSplTypeName, base64_decoded_valueSplTypeName);
	  return(base64_decoded_valueSplTypeName);
  }

  std::string AerospikeDBLayer::getNoSqlDbProductName(void) {
	  return(string(AEROSPIKE_NO_SQL_DB_NAME));
  }

  void AerospikeDBLayer::getDetailsAboutThisMachine(std::string & machineName, std::string & osVersion, std::string & cpuArchitecture) {
	  machineName = nameOfThisMachine;
	  osVersion = osVersionOfThisMachine;
	  cpuArchitecture = cpuTypeOfThisMachine;
  }

  bool AerospikeDBLayer::runDataStoreCommand(std::string const & cmd, PersistenceError & dbError) {
		// This API can only be supported in NoSQL data stores such as Redis, Cassandra etc.
		// Aerospike doesn't have a way to do this.
		dbError.set("From Aerospike data store: This API to run native data store commands is not supported in Aerospike.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Aerospike data store: This API to run native data store commands is not supported in Aerospike. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "AerospikeDBLayer");
		return(false);
  }

  bool AerospikeDBLayer::runDataStoreCommand(uint32_t const & cmdType, std::string const & httpVerb,
		std::string const & baseUrl, std::string const & apiEndpoint, std::string const & queryParams,
		std::string const & jsonRequest, std::string & jsonResponse, PersistenceError & dbError) {
		// This API can only be supported in NoSQL data stores such as Cloudant, HBase etc.
		// Aerospike doesn't have a way to do this.
		dbError.set("From Aerospike data store: This API to run native data store commands is not supported in Aerospike.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Aerospike data store: This API to run native data store commands is not supported in Aerospike. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "AerospikeDBLayer");
		return(false);
  }

  // This method will get the data item from the store for a given key.
  // Caller of this method can also ask us just to find if a data item
  // exists in the store without the extra work of fetching and returning the data item value.
  bool AerospikeDBLayer::getDataItemFromStore(std::string const & storeIdString,
		  std::string const & keyDataString, bool const & checkOnlyForDataItemExistence,
		  bool const & skipDataItemExistenceCheck, unsigned char * & valueData,
		  uint32_t & valueSize, PersistenceError & dbError) {
		// Let us get this data item from the cache as it is.
		// Since there could be multiple data writers, we are going to get whatever is there now.
		// It is always possible that the value for the requested item can change right after
		// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
		// This action is performed on the Store Set that takes the following name.
		// Set name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
		string storeSetName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
		// Aerospike data model:  Namespace-->Set-->Records-->Bins
		// Aerospike will create the set only if it doesn't already exist.
		string errorMsg = "";
		string value = "";
		bool result = readAerospikeBinValue(storeSetName, keyDataString.c_str(), value, errorMsg);

		if (errorMsg != "") {
			// Check if it is a record not found error.
			std::ostringstream noRecordError;
			noRecordError << "rc=" << AEROSPIKE_ERR_RECORD_NOT_FOUND;

			if (errorMsg.find(noRecordError.str()) == std::string::npos) {
				// This means some other error in reading a record from the given Aerospike Set.
				// Error message means something went wrong in the call we made above.
				// Unable to get the requested data item from the cache.
				dbError.set("Unable to access the K/V pair in Aerospike with the StoreId " + storeIdString +
						". " + errorMsg, DPS_DATA_ITEM_READ_ERROR);
				return(false);
			}
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
			// In Aerospike, when we put the K/V pair, we b64_encoded our binary value buffer and
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

  AerospikeDBLayerIterator * AerospikeDBLayer::newIterator(uint64_t store, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside newIterator for store id " << store, "AerospikeDBLayer");

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  // Ensure that a store exists for the given store id.
	  if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "AerospikeDBLayer");
		  }

		  return(false);
	  }

	  // Get the general information about this store.
	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayer");
		  return(NULL);
	  }

	  // It is a valid store. Create a new iterator and return it to the caller.
	  AerospikeDBLayerIterator *iter = new AerospikeDBLayerIterator();
	  iter->store = store;
	  base64_decode(storeName, iter->storeName);
	  iter->hasData = true;
	  // Give this iterator access to our AerospikeDBLayer object.
	  iter->AerospikeDBLayerPtr = this;
	  iter->sizeOfDataItemKeysVector = 0;
	  iter->currentIndex = 0;
	  return(iter);
  }

  void AerospikeDBLayer::deleteIterator(uint64_t store, Iterator * iter, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside deleteIterator for store id " << store, "AerospikeDBLayer");

	  if (iter == NULL) {
		  return;
	  }

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  AerospikeDBLayerIterator *myIter = static_cast<AerospikeDBLayerIterator *>(iter);

	  // Let us ensure that the user wants to delete an iterator that really belongs to the store passed to us.
	  // This will handle user's coding errors where a wrong combination of store id and iterator is passed to us for deletion.
	  if (myIter->store != store) {
		  // User sent us a wrong combination of a store and an iterator.
		  dbError.set("A wrong iterator has been sent for deletion. This iterator doesn't belong to the StoreId " +
		  				storeIdString + ".", DPS_STORE_ITERATION_DELETION_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside deleteIterator, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_DELETION_ERROR, "AerospikeDBLayer");
		  return;
	  } else {
		  delete iter;
	  }
  }

  // This method will acquire a lock for any given generic/arbitrary identifier passed as a string..
  // This is typically used inside the createStore, createOrGetStore, createOrGetLock methods to
  // provide thread safety. There are other lock acquisition/release methods once someone has a valid store id or lock id.
  bool AerospikeDBLayer::acquireGeneralPurposeLock(string const & entityName) {
	  int32_t retryCnt = 0;

	  // '501' + 'entity name' + 'generic_lock' => 1
	  std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
	  uint64_t lockId = SPL::Functions::Utility::hashCode(genericLockKey);
	  ostringstream lockIdStr;
	  lockIdStr << lockId;
	  string tmpLockIdStr = lockIdStr.str();

	  // Aerospike data model:  Namespace-->Set-->Records-->Bins
	  // Aerospike will create the set only if it doesn't already exist.
	  as_record rec;
	  // Create a record with two bins.
	  as_record_inita(&rec, 2);
	  // Create the K/V bins with their string values.
	  as_record_set_str(&rec, "k", genericLockKey.c_str());
	  as_record_set_str(&rec, "v", tmpLockIdStr.c_str());
	  // Set the TTL value.
	  rec.ttl = (uint32_t)DPS_AND_DL_GET_LOCK_TTL;

	  // Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	  as_key key;
	  as_key_init_str(&key, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		  string(DPS_DL_META_DATA_SET).c_str(), genericLockKey.c_str());

	  // Require that the write succeeds only if the record doesn't exist.
	  as_policy_write wpol;
	  as_policy_write_init(&wpol);
	  wpol.exists = AS_POLICY_EXISTS_CREATE;

	  // Try to get a lock for this generic entity.
	  while (1) {
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or the Aerospike removes it after the TTL value expires.
		// Make an attempt to create a record for this lock now.
		as_error err;
		if (aerospike_key_put(as, &err, &wpol, &key, &rec) == AEROSPIKE_OK) {
			// We got the lock.
			as_record_destroy(&rec);
			return(true);
		}

		char errorString[400];
		sprintf(errorString, "General purpose lock acquisition error. rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		SPLAPPTRC(L_DEBUG, string(errorString), "AerospikeDBLayer");
		// Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
			as_record_destroy(&rec);
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

	  as_record_destroy(&rec);
	  return(false);
  }

  void AerospikeDBLayer::releaseGeneralPurposeLock(string const & entityName) {
	  // '501' + 'entity name' + 'generic_lock' => 1
	  std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
	  // Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	  as_key key;
	  as_key_init_str(&key, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		  string(DPS_DL_META_DATA_SET).c_str(), genericLockKey.c_str());
	  as_error err;

	  if (aerospike_key_remove(as, &err, NULL, &key) == AEROSPIKE_OK) {
		  return;
	  } else {
		  char errorString[400];
		  sprintf(errorString, "General purpose lock release error. rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		  SPLAPPTRC(L_DEBUG, string(errorString), "AerospikeDBLayer");
		  return;
	  }
  }

  AerospikeDBLayerIterator::AerospikeDBLayerIterator() {

  }

  AerospikeDBLayerIterator::~AerospikeDBLayerIterator() {

  }

  bool AerospikeDBLayerIterator::getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
  	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside getNext for store id " << store, "AerospikeDBLayerIterator");

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
	  if (this->AerospikeDBLayerPtr->storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayerIterator");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "AerospikeDBLayerIterator");
		  }

		  return(false);
	  }

	  // Ensure that this store is not empty at this time. (Size check on every iteration is a performance nightmare. Optimize it later.)
	  if (this->AerospikeDBLayerPtr->size(store, dbError) <= 0) {
		  dbError.set("Store is empty for the StoreId " + storeIdString + ".", DPS_STORE_EMPTY_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed with an empty store whose id is " << storeIdString << ". " << DPS_STORE_EMPTY_ERROR, "AerospikeDBLayerIterator");
		  return(false);
	  }

	  if (this->sizeOfDataItemKeysVector <= 0) {
		  // This is the first time we are coming inside getNext for store iteration.
		  // Let us get the available data item keys from this store.
		  this->dataItemKeys.clear();
		  string errorMsg = "";
		  string storeSetName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
		  // In Aerospike, to get all the keys in a set we have to go through a callback style API.
		  // Hence, it is necessary to have a pointer to the iterator object that is accessible within the
		  // callback method that is a public member of the main DBLayer class.
		  this->AerospikeDBLayerPtr->asDBLayerIterator = this;
		  bool aerospikeResult = this->AerospikeDBLayerPtr->getAllKeysInSet(storeSetName, errorMsg);
		  // Reset the pointer we assigned before making a call to the get all keys method.
		  this->AerospikeDBLayerPtr->asDBLayerIterator = NULL;

		  if (aerospikeResult == false) {
			  // Unable to get data item keys from the store.
			  dbError.set("Unable to get data item keys for the StoreId " + storeIdString +
					  ". Error: " + errorMsg, DPS_GET_STORE_DATA_ITEM_KEYS_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to get data item keys for store id " << storeIdString <<
                 ". Error: " << errorMsg << ". " << DPS_GET_STORE_DATA_ITEM_KEYS_ERROR, "AerospikeDBLayerIterator");
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
	  bool result = this->AerospikeDBLayerPtr->getDataItemFromStore(storeIdString, data_item_key,
		false, false, valueData, valueSize, dbError);

	  if (result == false) {
		  // Some error has occurred in reading the data item value.
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to get data item from store id " << storeIdString << ". " << dbError.getErrorCode(), "AerospikeDBLayerIterator");
		  // We will disable any future action for this store using the current iterator.
		  this->hasData = false;
		  return(false);
	  }

	  // We are almost done once we take care of arranging to return the key name and key size.
	  // In order to support spaces in data item keys, we base64 encoded them before storing it in Aerospike.
	  // Let us base64 decode it now to get the original data item key.
	  string base64_decoded_data_item_key;
	  this->AerospikeDBLayerPtr->base64_decode(data_item_key, base64_decoded_data_item_key);
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
		     storeIdString << ". " << DPS_STORE_ITERATION_MALLOC_ERROR, "AerospikeDBLayerIterator");
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
  uint64_t AerospikeDBLayer::createOrGetLock(std::string const & name, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock with a name " << name, "AerospikeDBLayer");
		string base64_encoded_name;
		base64_encode(name, base64_encoded_name);

	 	// Get a general purpose lock so that only one thread can
		// enter inside of this method at any given time with the same lock name.
	 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
	 		// Unable to acquire the general purpose lock.
	 		lkError.set("Unable to get a generic lock for creating a lock with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
	 		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to get a generic lock while creating a store lock named " <<
	 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "AerospikeDBLayer");
	 		// User has to retry again to create this distributed lock.
	 		return (0);
	 	}

		// 1) In our Aerospike dps implementation, data item keys can have space characters.
		// Inside Aerospike, all our lock names will have a mapping type indicator of
		// "5" at the beginning followed by the actual lock name.
		// '5' + 'lock name' => lockId
		std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
		uint64_t lockId = SPL::Functions::Utility::hashCode(lockNameKey);
		ostringstream lockIdStr;
		lockIdStr << lockId;
		string tmpLockIdStr = lockIdStr.str();

		// Aerospike data model:  Namespace-->Set-->Records-->Bins
		// Aerospike will create the set only if it doesn't already exist.
		as_record rec1;
		// Create a record with two bins.
		as_record_inita(&rec1, 2);
		// Create the K/V bins with their string values.
		as_record_set_str(&rec1, "k", lockNameKey.c_str());
		as_record_set_str(&rec1, "v", tmpLockIdStr.c_str());
		// Set the TTL value to be forever.
		rec1.ttl = -1;

		// Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
		as_key key1;
		as_key_init_str(&key1, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
			string(DPS_DL_META_DATA_SET).c_str(), lockNameKey.c_str());

		// Require that the write succeeds only if the record doesn't exist.
		as_policy_write wpol;
		as_policy_write_init(&wpol);
		wpol.exists = AS_POLICY_EXISTS_CREATE;
		// Make an attempt to create a record for this lock now.
		as_error err;
		as_status status = aerospike_key_put(as, &err, &wpol, &key1, &rec1);
		if (status == AEROSPIKE_ERR_RECORD_EXISTS) {
			// This lock already exists.
			as_record_destroy(&rec1);
			releaseGeneralPurposeLock(base64_encoded_name);
			return(lockId);
		} else if (status != AEROSPIKE_OK) {
			char errorString[400];
			sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
			lkError.set("Unable to get the lockId for the lockName " + name +
				". Error in creating a user defined lock entry. " + string(errorString), DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to get the lockId for the lockName " <<
				name << ". " << string(errorString) << ". " << DL_GET_LOCK_ID_ERROR, "AerospikeDBLayer");
			as_record_destroy(&rec1);
			releaseGeneralPurposeLock(base64_encoded_name);
			return (0);
		}

		as_record_destroy(&rec1);
		// If we are here that means Aerospike record entry was created by the previous call.
		// We can go ahead and create the lock info entry now.
		// 2) Create the Lock Info
		//    '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdStr.str();  // LockId becomes the new key now.
		string lockInfoValue = string("0_0_0_") + base64_encoded_name;

		as_record rec2;
		// Create a record with two bins.
		as_record_inita(&rec2, 2);
		// Create the K/V bins with their string values.
		as_record_set_str(&rec2, "k", lockInfoKey.c_str());
		as_record_set_str(&rec2, "v", lockInfoValue.c_str());
		// Set the TTL value to be forever.
		rec2.ttl = -1;

		// Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
		as_key key2;
		as_key_init_str(&key2, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
			string(DPS_DL_META_DATA_SET).c_str(), lockInfoKey.c_str());

		status = aerospike_key_put(as, &err, &wpol, &key2, &rec2);

		if (status == AEROSPIKE_OK) {
			as_record_destroy(&rec2);
			releaseGeneralPurposeLock(base64_encoded_name);
			return(lockId);
		} else {
			char errorString[400];
			sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
			lkError.set("Unable to create 'LockId:LockInfo' in the cache for a lock named " + name +
				". " + string(errorString), DL_LOCK_INFO_CREATION_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to create 'LockId:LockInfo' in the cache for a lock named " <<
				name << ". " << string(errorString) << ". " << DL_LOCK_INFO_CREATION_ERROR, "AerospikeDBLayer");
			as_record_destroy(&rec2);
			releaseGeneralPurposeLock(base64_encoded_name);
			return (0);
		}
  }

  bool AerospikeDBLayer::removeLock(uint64_t lock, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside removeLock for lock id " << lock, "AerospikeDBLayer");

		ostringstream lockId;
		lockId << lock;
		string lockIdString = lockId.str();

		// If the lock doesn't exist, there is nothing to remove. Don't allow this caller inside this method.
		if(lockIdExistsOrNot(lockIdString, lkError) == false) {
			if (lkError.hasError() == true) {
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "AerospikeDBLayer");
			} else {
				lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to find the lock with an id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "AerospikeDBLayer");
			}

			return(false);
		}

		// Before removing the lock entirely, ensure that the lock is not currently being used by anyone else.
		if (acquireLock(lock, 25, 40, lkError) == false) {
			// Unable to acquire the distributed lock.
			lkError.set("Unable to get a distributed lock for the LockId " + lockIdString + ".", DL_GET_DISTRIBUTED_LOCK_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to get a distributed lock for the lock id " << lockIdString << ". " << DL_GET_DISTRIBUTED_LOCK_ERROR, "AerospikeDBLayer");
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
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "AerospikeDBLayer");
			releaseLock(lock, lkError);
			// This is alarming. This will put this lock in a bad state. Poor user has to deal with it.
			return(false);
		}

		// Since we got back the lock name for the given lock id, let us remove the lock entirely.
		// '5' + 'lock name' => lockId
		std::string lockNameKey = DL_LOCK_NAME_TYPE + lockName;
		as_key key1;
		as_key_init_str(&key1, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
			  string(DPS_DL_META_DATA_SET).c_str(), lockNameKey.c_str());
		as_error err;

		if (aerospike_key_remove(as, &err, NULL, &key1) != AEROSPIKE_OK) {
			char errorString[400];
			sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
			// There was an error in deleting the user defined lock.
			lkError.set("Unable to remove the lock named " + lockIdString + ". Error: " + string(errorString), DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to remove the lock with an id " << lockIdString << ". Error:" <<
				string(errorString) << ". " << DL_LOCK_REMOVAL_ERROR, "AerospikeDBLayer");
			releaseLock(lock, lkError);
			return(false);
		}

		// Now remove the lockInfo entry.
		// '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;  // LockId becomes the new key now.
		as_key key2;
		as_key_init_str(&key2, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
			  string(DPS_DL_META_DATA_SET).c_str(), lockInfoKey.c_str());

		if (aerospike_key_remove(as, &err, NULL, &key2) != AEROSPIKE_OK) {
			char errorString[400];
			sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
			// There was an error in deleting the user defined lock.
			lkError.set("Unable to remove the lock info for a lock named " + lockIdString + ". Error: " + string(errorString), DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to remove the lock info with an id " << lockIdString << ". Error:" <<
				string(errorString) << ". " << DL_LOCK_REMOVAL_ERROR, "AerospikeDBLayer");
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

  bool AerospikeDBLayer::acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside acquireLock for lock id " << lock, "AerospikeDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();
	  int32_t retryCnt = 0;

	  // If the lock doesn't exist, there is nothing to acquire. Don't allow this caller inside this method.
	  if(lockIdExistsOrNot(lockIdString, lkError) == false) {
		  if (lkError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "AerospikeDBLayer");
		  } else {
			  lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to find a lock with an id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "AerospikeDBLayer");
		  }

		  return(false);
	  }

	  // We will first check if we can get this lock.
	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  time_t startTime, timeNow;
	  // Get the start time for our lock acquisition attempts.
	  time(&startTime);

	  // Aerospike data model:  Namespace-->Set-->Records-->Bins
	  // Aerospike will create the set only if it doesn't already exist.
	  as_record rec1;
	  // Create a record with two bins.
	  as_record_inita(&rec1, 2);
	  // Create the K/V bins with their string values.
	  as_record_set_str(&rec1, "k", distributedLockKey.c_str());
	  as_record_set_str(&rec1, "v", "1");
	  // Set the TTL value.
	  rec1.ttl = (uint32_t)leaseTime;

	  // Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	  as_key key1;
	  as_key_init_str(&key1, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		  string(DPS_DL_META_DATA_SET).c_str(), distributedLockKey.c_str());

	  // Require that the write succeeds only if the record doesn't exist.
	  as_policy_write wpol;
	  as_policy_write_init(&wpol);
	  wpol.exists = AS_POLICY_EXISTS_CREATE;

	  // Try to get a lock for this lock acquisition entity.
	  while (1) {
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or the Aerospike removes it after the TTL value expires.
		// Make an attempt to create a record for this lock now.
	    time_t new_lock_expiry_time = time(0) + (time_t)leaseTime;
		as_error err;
		as_status status = aerospike_key_put(as, &err, &wpol, &key1, &rec1);

		if (status == AEROSPIKE_OK) {
			// We got the lock. We can return now.
			// Let us update the lock information now.
			if(updateLockInformation(lockIdString, lkError, 1, new_lock_expiry_time, getpid()) == true) {
				as_record_destroy(&rec1);
				return(true);
			} else {
				// Some error occurred while updating the lock information.
				// It will be in an inconsistent state. Let us release the lock.
				releaseLock(lock, lkError);
			}
		}

		char errorString[400];
		sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		SPLAPPTRC(L_DEBUG, "User defined lock acquisition error=" << string(errorString), "AerospikeDBLayer");
		// Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
			  lkError.set("Unable to acquire the lock named " + lockIdString + " after maximum retries.", DL_GET_LOCK_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire a lock named " << lockIdString << " after maximum retries. " <<
			     DL_GET_LOCK_ERROR, "AerospikeDBLayer");
			  as_record_destroy(&rec1);
			  // Our caller can check the error code and try to acquire the lock again.
			  return(false);
		  }

		  // Check if we have gone past the maximum wait time the caller was willing to wait in order to acquire this lock.
		  time(&timeNow);
		  if (difftime(startTime, timeNow) > maxWaitTimeToAcquireLock) {
			  lkError.set("Unable to acquire the lock named " + lockIdString + " within the caller specified wait time.", DL_GET_LOCK_TIMEOUT_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire the lock named " << lockIdString <<
			     " within the caller specified wait time." << DL_GET_LOCK_TIMEOUT_ERROR, "AerospikeDBLayer");
			  as_record_destroy(&rec1);
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

	  as_record_destroy(&rec1);
	  return(false);
  }

  void AerospikeDBLayer::releaseLock(uint64_t lock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside releaseLock for lock id " << lock, "AerospikeDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;

	  as_key key1;
	  as_key_init_str(&key1, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		  string(DPS_DL_META_DATA_SET).c_str(), distributedLockKey.c_str());
	  as_error err;

	  if (aerospike_key_remove(as, &err, NULL, &key1) != AEROSPIKE_OK) {
		  char errorString[400];
		  sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		  // There was an error in deleting the user defined lock.
		  lkError.set("Unable to release the lock id " + lockIdString + ". Error: " + string(errorString), DL_LOCK_RELEASE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside releaseLock, it failed to release the lock id " << lockIdString << ". Error:" <<
			  string(errorString) << ". " << DL_LOCK_RELEASE_ERROR, "AerospikeDBLayer");
		  return;
	  }

	  updateLockInformation(lockIdString, lkError, 0, 0, 0);
  }

  bool AerospikeDBLayer::updateLockInformation(std::string const & lockIdString,
	PersistenceError & lkError, uint32_t const & lockUsageCnt, int32_t const & lockExpirationTime, pid_t const & lockOwningPid) {
	  // Get the lock name for this lock.
	  uint32_t _lockUsageCnt = 0;
	  int32_t _lockExpirationTime = 0;
	  std::string _lockName = "";
	  pid_t _lockOwningPid = 0;

	  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "AerospikeDBLayer");
		  return(false);
	  }

	  // Let us update the lock information.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  ostringstream lockInfoValue;
	  lockInfoValue << lockUsageCnt << "_" << lockExpirationTime << "_" << lockOwningPid << "_" << _lockName;

	  // Aerospike data model:  Namespace-->Set-->Records-->Bins
	  // Aerospike will create the set only if it doesn't already exist.
	  as_record rec1;
	  // Create a record with two bins.
	  as_record_inita(&rec1, 2);
	  // Create the K/V bins with their string values.
	  as_record_set_str(&rec1, "k", lockInfoKey.c_str());
	  string tmpLockInfoValue = lockInfoValue.str();
	  as_record_set_str(&rec1, "v", tmpLockInfoValue.c_str());
	  // Set the TTL value.
	  rec1.ttl = (uint32_t)DPS_AND_DL_GET_LOCK_TTL;

	  // Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	  as_key key1;
	  as_key_init_str(&key1, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		  string(DPS_DL_META_DATA_SET).c_str(), lockInfoKey.c_str());

	  // Require that the write succeeds whether the record exists or not.
	  as_policy_write wpol;
	  as_policy_write_init(&wpol);
	  wpol.exists = AS_POLICY_EXISTS_IGNORE;
	  // Make an attempt to create a record for this lock info now.
	  as_error err;
	  as_status status = aerospike_key_put(as, &err, &wpol, &key1, &rec1);

	  if (status == AEROSPIKE_OK) {
		  as_record_destroy(&rec1);
		  return(true);
	  } else {
		  char errorString[400];
		  sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		  lkError.set("Critical Error1: Unable to update 'LockId:LockInfo' in the cache for a lock named " + _lockName +
			  ". " + string(errorString), DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Critical Error1: Inside updateLockInformation, it failed for a lock named " <<
			  _lockName << ". " << string(errorString) << ". " << DL_LOCK_INFO_UPDATE_ERROR, "AerospikeDBLayer");
		  as_record_destroy(&rec1);
		  return (false);
	  }
  }

  bool AerospikeDBLayer::readLockInformation(std::string const & lockIdString, PersistenceError & lkError, uint32_t & lockUsageCnt,
		  int32_t & lockExpirationTime, pid_t & lockOwningPid, std::string & lockName) {
	  // Read the contents of the lock information.
	  lockName = "";
	  std::string lockInfo = "";

	  // Lock Info contains meta data information about a given lock.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;

	  string errorMsg = "";
	  string value = "";

	  bool result = readAerospikeBinValue(string(DPS_DL_META_DATA_SET), lockInfoKey.c_str(), value, errorMsg);

	  if (result == true) {
		  // We got the value from the Aerospike K/V store.
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
  bool AerospikeDBLayer::lockIdExistsOrNot(string lockIdString, PersistenceError & lkError) {
	  string keyString = string(DL_LOCK_INFO_TYPE) + lockIdString;
	  string errorMsg = "";
	  string value = "";

	  bool result = readAerospikeBinValue(string(DPS_DL_META_DATA_SET), keyString.c_str(), value, errorMsg);

	  if (result == true) {
		  // LockId exists.
		  return(true);
	  } else {
		  // Check if it is a record not found error.
		  std::ostringstream noRecordError;
		  noRecordError << "rc=" << AEROSPIKE_ERR_RECORD_NOT_FOUND;

		  if (errorMsg.find(noRecordError.str()) != std::string::npos) {
			  // This means record not found in the given Aerospike Set.
			  return(false);
		  } else {
			  // Some other Aerospike error.
			  // Unable to get the lock info for the given lock id.
			  lkError.set("LockIdExistsOrNot: Unable to get LockInfo for the lockId " + lockIdString +
				  ". " + errorMsg, DL_GET_LOCK_INFO_ERROR);
			  return(false);
		  }
	  }
  }

  // This method will return the process id that currently owns the given lock.
  uint32_t AerospikeDBLayer::getPidForLock(string const & name, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside getPidForLock with a name " << name, "AerospikeDBLayer");

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
		  SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "AerospikeDBLayer");
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
  inline bool AerospikeDBLayer::is_b64(unsigned char c) {
    return (isalnum(c) || (c == '+') || (c == '/'));
  }

  // Base64 encode the binary buffer contents passed by the caller and return a string representation.
  // There is no change to the original code in this method other than a slight change in the method name,
  // returning back right away when encountering an empty buffer and an array initialization for
  // char_array_4 to avoid a compiler warning.
  //
  void AerospikeDBLayer::b64_encode(unsigned char const * & buf, uint32_t const & bufLenOrg, string & ret) {
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
  void AerospikeDBLayer::b64_decode(std::string & encoded_string, unsigned char * & buf, uint32_t & bufLen) {
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

  // This method returns all the document keys present in a given Aerospike Set.
  bool AerospikeDBLayer::getAllKeysInSet(string const & storeSetName, string & errorMsg) {
	  errorMsg = "";
	  // Aerospike's scan API works in a callback fashion.
	  as_error err;
	  as_scan scan;
	  as_scan_init(&scan, string(AEROSPIKE_DPS_NAMESPACE).c_str(), storeSetName.c_str());
	  as_scan_select_inita(&scan, 1);
	  as_scan_select(&scan, "v");

	  // Callback method will be called for each scanned record.
	  // When all the records have been scanned, then the callback method will be called with a NULL value for that record.
	  // Please note below that we are passing the object pointer to this class as an argument that will be sent to our static callback method.
	  if (aerospike_scan_foreach(as, &err, NULL, &scan, get_all_keys_callback, this) != AEROSPIKE_OK ) {
		  char errorString[400];
		  sprintf(errorString, "Inside getAllKeysInSet, it failed for store id %s. rc=%d, msg=%s at [%s:%d]",
			  storeSetName.c_str(), err.code, err.message, err.file, err.line);
		  errorMsg = string(errorString);
		  as_scan_destroy(&scan);
		  return(false);
	  } else {
		  as_scan_destroy(&scan);
		  return(true);
	  }
  }

  // In Aerospike, there can only be a maximum of 1023 sets inside a namespace. In addition, sets can never be
  // deleted via APIs. It can ONLY be deleted from a command line utility. That will not help us at all here.
  // We have to do a different approach which is customized for our needs here. Out of those 1023, we are going to
  // allow only the first 1000 sets to be associated with the user-created stores. Remaining 23 of them will be
  // reserved for the DPS toolkit's internal use.  As stores are created and deleted, we are going to do
  // book-keeping of which store is available and unavailable for use. Using this information, we will
  // dynamically assign a pre-existing set number to represent a user created store.
  // What a pain in Aerospike?
  //
  // We will create a reserved Aerospike set simply to tell us whether store ids 1 through 1000 are occupied or available for use.
  bool AerospikeDBLayer::createStoreIdTrackerSet(string & errorMsg) {
	  //Create a new set and and mark the 1 to 1000 store id tracker slots as ready for use.
	  errorMsg = "";
	  // Aerospike data model:  Namespace-->Set-->Records-->Bins
	  // Aerospike will create the set only if it doesn't already exist.
	  as_record rec[1000];
	  as_key key[1000];
	  char storeIdString[130];

	  // Require that the write succeeds only if the record doesn't exist.
	  as_policy_write wpol;
	  as_policy_write_init(&wpol);
	  wpol.exists = AS_POLICY_EXISTS_CREATE;

	  for (int32_t cnt = 0; cnt < 1000; cnt++) {
		  // Create a record with two bins.
		  as_record_inita(&rec[cnt], 2);
		  // Create the K/V bins with their string values.
		  as_record_set_str(&rec[cnt], "k", storeIdString);
		  // 0 = store id slot is available i.e. empty and not being used.
		  // 1 = store id slot is not available i.e. occupied and already in use.
		  as_record_set_str(&rec[cnt], "v", "0");
		  // Set the TTL value to never expire.
		  rec[cnt].ttl = -1;
		  sprintf(storeIdString, "%d", cnt+1);
		  // Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
		  as_key_init_str(&key[cnt], string(AEROSPIKE_DPS_NAMESPACE).c_str(),
			  string(DPS_STORE_ID_TRACKER_SET).c_str(), storeIdString);

		  as_error err;
		  as_status status;
		  status = aerospike_key_put(as, &err, &wpol, &key[cnt], &rec[cnt]);

		  if ((status != AEROSPIKE_OK) && (status != AEROSPIKE_ERR_RECORD_EXISTS)) {
			  // The result we got is not one of these two: All is well or record already exists
			  // In that case, we will return now with an error.
			  char errorString[400];
			  sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
			  errorMsg = string(errorString);

			  // Release the records we allocated in the stack.
			  for (int32 idx = cnt; idx < 1000; idx++) {
				  as_record_destroy(&rec[idx]);
			  }

			  return(false);
		  }

		  // Release the resource (It is strictly not needed since we operated in our stack. However, it is okay to release it.)
		  as_record_destroy(&rec[cnt]);
	  }

	  return(true);
  }

  // Return an available store id slot for use.
  bool AerospikeDBLayer::getStoreIdNotInUse(string const & storeName, uint64_t & storeId, string & errorMsg) {
	  errorMsg = "";
	  string getStoreIdNotInUseLockName = "get_store_id_not_in_use_lock";
	  // Get a general purpose lock so that only one thread can
	  // enter inside of this method at any given time with the same store name.
	  if (acquireGeneralPurposeLock(getStoreIdNotInUseLockName) == false) {
		  // Unable to acquire the general purpose lock.
		  char errorString[400];
		  sprintf(errorString, "Unable to get a generic lock for finding a store id that is not in use for %s.", storeName.c_str());
		  errorMsg = string(errorString);
		  return (false);
	  }

	  // Aerospike data model:  Namespace-->Set-->Records-->Bins
	  // Aerospike will create the set only if it doesn't already exist.
	  as_key key[1000];
	  char storeIdString[130];
	  as_error err;
	  string valueString = "";

	  // Loop through all the store id slots 1 through 1000 and see which is available for use now.
	  for (uint64_t cnt = 0; cnt < 1000; cnt++) {
		  sprintf(storeIdString, "%ld", cnt+1);
		  valueString = "";
		  bool result = readAerospikeBinValue(string(DPS_STORE_ID_TRACKER_SET), storeIdString, valueString, errorMsg);

		  if (result != true) {
			  errorMsg = "Error while finding a store id that is not in use for " +
				  storeName + ". " + errorMsg;
			  releaseGeneralPurposeLock(getStoreIdNotInUseLockName);
			  return(false);
		  }

		  if (valueString == "1") {
			  // This store id is in use. continue searching further.
			  continue;
		  }

		  // This store id slot is available.
		  // Let us grab it now by replacing the value from 0 to 1.
		  as_record rec;
		  // Require that the write succeeds whether the record exists or not.
		  as_policy_write wpol;
		  as_policy_write_init(&wpol);
		  wpol.exists = AS_POLICY_EXISTS_IGNORE;

		  // Create a record with two bins.
		  as_record_inita(&rec, 2);
		  // Create the K/V bins with their string values.
		  as_record_set_str(&rec, "k", storeIdString);
		  // 0 = store id slot is available.
		  // 1 = store id slot is not available
		  // We want to set it to 1 to tell others this store id is in use.
		  as_record_set_str(&rec, "v", "1");
		  // Set the TTL value to never expire.
		  rec.ttl = -1;
		  // Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
		  as_key_init_str(&key[cnt], string(AEROSPIKE_DPS_NAMESPACE).c_str(),
			  string(DPS_STORE_ID_TRACKER_SET).c_str(), storeIdString);
		  as_status status;
		  status = aerospike_key_put(as, &err, &wpol, &key[cnt], &rec);
		  // Release the record we allocated in the stack.
		  as_record_destroy(&rec);

		  if (status == AEROSPIKE_OK) {
			  // We successfully grabbed this store id.
			  storeId = cnt+1;
			  releaseGeneralPurposeLock(getStoreIdNotInUseLockName);
			  return(true);
		  } else {
			  // The write result is not successful.
			  // In that case, we will return now with an error.
			  char errorString[400];
			  sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
			  errorMsg = string(errorString);
			  releaseGeneralPurposeLock(getStoreIdNotInUseLockName);
			  return(false);
		  }
	  } // End of for loop.

	  // If we reach here, that means no available store id slot is found.
	  errorMsg = "All the store ids in Aerospike are in use at this time. Unable to get a store id for " + storeName;
	  releaseGeneralPurposeLock(getStoreIdNotInUseLockName);
	  return(false);
  }

  // Set a store id slot to "not in use".
  bool AerospikeDBLayer::setStoreIdToNotInUse(uint64_t const & storeId, string & errorMsg) {
	  errorMsg = "";
	  as_record rec;
	  as_key key;
	  as_error err;
	  // Require that the write succeeds whether the record exists or not.
	  as_policy_write wpol;
	  as_policy_write_init(&wpol);
	  wpol.exists = AS_POLICY_EXISTS_IGNORE;

	  char storeIdString[130];
	  sprintf(storeIdString, "%ld", storeId);

	  // Create a record with two bins.
	  as_record_inita(&rec, 2);
	  // Create the K/V bins with their string values.
	  as_record_set_str(&rec, "k", storeIdString);
	  // 0 = store id slot is available.
	  // 1 = store id slot is not available
	  // We want to set it to 0 to tell others this store id is not in use.
	  as_record_set_str(&rec, "v", "0");
	  // Set the TTL value to never expire.
	  rec.ttl = -1;
	  // Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	  as_key_init_str(&key, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		  string(DPS_STORE_ID_TRACKER_SET).c_str(), storeIdString);
	  as_status status;
	  status = aerospike_key_put(as, &err, &wpol, &key, &rec);
	  // Release the record we allocated in the stack.
	  as_record_destroy(&rec);

	  if (status == AEROSPIKE_OK) {
		  // We successfully set this store id slot to not in use.
		  return(true);
	  } else {
		  // The write result is not successful.
		  // In that case, we will return now with an error.
		  // This failure causes a side effect where a store id slot will be hanging there with "In Use" status all the time.
		  char errorString[400];
		  sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		  errorMsg = string(errorString);
		  return(false);
	  }
  }

  // Read and return an Aerospike bin value of a given key.
  bool AerospikeDBLayer::readAerospikeBinValue(string const & setName, const char *keyName, string & valueString, string & errorMsg) {
	  errorMsg = "";
	  as_key key;
	  as_record *pRec = NULL;
	  as_error err;
	  // Initialize the key: Takes 4 args (key structure, namespace name, set name, key name)
	  as_key_init_str(&key, string(AEROSPIKE_DPS_NAMESPACE).c_str(),
		  setName.c_str(), keyName);
	  pRec = NULL;
	  // Get the K/V pair.
	  as_status result = aerospike_key_get(as, &err, NULL, &key, &pRec);

	  if (result != AEROSPIKE_OK) {
		  // Trouble in reading from the Aerospike set.
		  // Not a good sign. Let us exit quickly.
		  char errorString[400];
		  sprintf(errorString, "rc=%d, msg=%s at [%s:%d]", err.code, err.message, err.file, err.line);
		  errorMsg = string(errorString);

		  if (pRec != NULL) {
			  as_record_destroy(pRec);
		  }

		  return(false);
	  }

	  bool noValueReturned = false;
	  char *valuePtr = NULL;
	  // Get the string value stored in this K/V pair i.e. in the bin named "v".
	  as_string *value = as_record_get_string(pRec, "v");

	  if (value == NULL) {
		  noValueReturned = true;
	  } else {
		  valuePtr = as_string_get(value);

		  if (valuePtr == NULL) {
			  noValueReturned = true;
		  }
	  }

	  if (noValueReturned) {
		  char errorString[400];
		  sprintf(errorString, "Failed to read a value of a bin.");
		  errorMsg = string(errorString);

		  if (pRec != NULL) {
			  as_record_destroy(pRec);
		  }

		  if (value != NULL) {
			  // Free this one.
			  as_string_destroy(value);
		  }

		  return(false);
	  }

	  // We got the value.
	  valueString = string(valuePtr);
	  as_string_destroy(value);
	  as_record_destroy(pRec);
	  return(true);
  }

  // Return the size of a given Aerospike Set.
  bool AerospikeDBLayer::readAerospikeSetSize(string const & storeSetName, int64_t & setSize, string & errorMsg) {
	  // We will use the Aerospike background Scan API to calculate the size.
	  // Depending on the number of records in this set, this method may take a considerable amount of time to return the result.
	  errorMsg = "";
	  setSize = 0;
	  as_error err;
	  as_scan scan;
	  as_scan_init(&scan, string(AEROSPIKE_DPS_NAMESPACE).c_str(), storeSetName.c_str());
	  as_scan_select_inita(&scan, 1);
	  as_scan_select(&scan, "v");
	  // Callback method will update this variable as it iterates through the store set.
	  storeSetSize = 0;

	  // Callback method will be called for each scanned record.
	  // When all the records have been scanned, then the callback method will be called with a NULL value for that record.
	  // Please note below that we are passing the object pointer to this class as an argument that will be sent to our static callback method.
	  if (aerospike_scan_foreach(as, &err, NULL, &scan, obtain_set_size_callback, this) != AEROSPIKE_OK ) {
		  char errorString[400];
		  sprintf(errorString, "Inside readAerospikeSetSize, error in obtaining the Set size for store id %s. rc=%d, msg=%s at [%s:%d]",
			  storeSetName.c_str(), err.code, err.message, err.file, err.line);
		  errorMsg = string(errorString);
		  as_scan_destroy(&scan);
		  return(false);
	  }

	  as_scan_destroy(&scan);
	  setSize = storeSetSize;
	  return(true);
  }

  // The 2nd argument will be pointing to a object of this class. That will allow us to
  // get access to our non-static members.
  bool AerospikeDBLayer::remove_store_callback(const as_val *val, void *udata) {
	  if (val != NULL) {
		  // Delete one record at a time in the process of removing a store.
		  static_cast<AerospikeDBLayer*>(udata)->removeStoreImpl(val);
		  return(true);
	  } else {
		  // NULL value indicates that all records have been scanned.
		  // Nothing more to process. Hence, return a false to end the iteration.
		  return(false);
	  }
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  void AerospikeDBLayer::removeStoreImpl(const as_val *val) {
	  // We will get the key from the currently scanned record and use it to delete the entire record.
	  // We will eventually delete all the records from this store set.
	  as_record *pRec = as_record_fromval(val);

	  if (pRec != NULL) {
		  as_key key = pRec->key;
		  as_error err;
		  aerospike_key_remove(as, &err, NULL, &key);
	  }
  }

  // The 2nd argument will be pointing to a object of this class. That will allow us to
  // get access to our non-static members.
  bool AerospikeDBLayer::clear_store_callback(const as_val *val, void *udata) {
	  if (val != NULL) {
		  static_cast<AerospikeDBLayer*>(udata)->clearStoreImpl(val);
		  return(true);
	  } else {
		  // NULL value indicates that all records have been scanned.
		  // Nothing more to process. Hence, return a false to end the iteration.
		  return(false);
	  }
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  void AerospikeDBLayer::clearStoreImpl(const as_val *val) {
	  // We will get the key from the currently scanned record and use it to delete the entire record.
	  as_record *pRec1 = as_record_fromval(val);

	  if (pRec1 == NULL) {
		  // We can't do much now. Return back.
		  return;
	  }

	  as_key key = pRec1->key;
	  as_record *pRec2 = NULL;
	  as_string *asStringKeyBin = NULL;
	  string keyString = "";
	  as_error err;

	  // This is plain crazy that we have to do this in Aerospike.
	  // If we try to read from the "k" bin with the pRec1 we obtained above,
	  // it always returns a NULL value. So, it is necessary to use the
	  // pRec1->key and then query the record one more time using the regular key_get Aerospike API.
	  // That seems to work even though it is a roundabout way.
	  aerospike_key_get(as, &err, NULL, &key, &pRec2);

	  if (pRec2 != NULL) {
		  asStringKeyBin = as_record_get_string(pRec2, "k");

		  if (asStringKeyBin != NULL) {
			  char *strBuf = as_string_get(asStringKeyBin);

			  if (strBuf != NULL) {
				  keyString = string(strBuf);
			  }

			  as_string_destroy(asStringKeyBin);
		  }

		  as_record_destroy(pRec2);
	  }

	  if (keyString == "") {
		  // This is not good at all.
		  // In this case, we will not be able to filter the meta data keys from getting deleted.
		  // That will lead to accidental deletion of the meta data keys which will put this store in a weird state.
		  SPLAPPTRC(L_ERROR, "Critical Error: Inside clearStoreImpl, it failed to get a store key properly. " <<
			  "This will lead to the unintended clearing of the store's meta data keys which will put this store in an unusable state. " <<
			  DPS_STORE_CLEAR_NULL_KEY_ERROR, "AerospikeDBLayer");
	  } else if ((keyString == string(AEROSPIKE_STORE_ID_TO_STORE_NAME_KEY)) ||
		  (keyString == string(AEROSPIKE_SPL_TYPE_NAME_OF_KEY)) ||
		  (keyString == string(AEROSPIKE_SPL_TYPE_NAME_OF_VALUE))) {
		  // If it is one of the meta data keys, then don't delete it since we are clearing only the non-meta data keys.
		  as_record_destroy(pRec1);
		  return;
	  }

	  aerospike_key_remove(as, &err, NULL, &key);
	  as_record_destroy(pRec1);
  }

  // The 2nd argument will be pointing to a object of this class. That will allow us to
  // get access to our non-static members.
  bool AerospikeDBLayer::get_all_keys_callback(const as_val *val, void *udata) {
	  if (val != NULL) {
		  static_cast<AerospikeDBLayer*>(udata)->getAllKeysImpl(val);
		  return(true);
	  } else {
		  // NULL value indicates that all records have been scanned.
		  // Nothing more to process. Hence, return a false to end the iteration.
		  return(false);
	  }
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  void AerospikeDBLayer::getAllKeysImpl(const as_val *val) {
	  // We will get the key from the currently scanned record.
	  // When doing this, we will ignore the meta data keys as we pass by them and
	  // pay attention only to the user created keys.
	  as_record *pRec1 = as_record_fromval(val);

	  if (pRec1 == NULL) {
		  // We can't do much now. Return back.
		  return;
	  }

	  as_key key = pRec1->key;
	  as_record *pRec2 = NULL;
	  as_string *asStringKeyBin = NULL;
	  string keyString = "";
	  as_error err;

	  // This is plain crazy that we have to do this in Aerospike.
	  // If we try to read from the "k" bin with the pRec1 we obtained above,
	  // it always returns a NULL value. So, it is necessary to use the
	  // pRec1->key and then query the record one more time using the regular key_get Aerospike API.
	  // That seems to work even though it is a roundabout way.
	  aerospike_key_get(as, &err, NULL, &key, &pRec2);

	  if (pRec2 != NULL) {
		  asStringKeyBin = as_record_get_string(pRec2, "k");

		  if (asStringKeyBin != NULL) {
			  char *strBuf = as_string_get(asStringKeyBin);

			  if (strBuf != NULL) {
				  keyString = string(strBuf);
			  }

			  as_string_destroy(asStringKeyBin);
		  }

		  as_record_destroy(pRec2);
	  }

	  if (keyString == "") {
		  // This is not good at all.
		  // In this case, we will not be able to filter the meta data keys from getting deleted.
		  // That will lead to accidental deletion of the meta data keys which will put this store in a weird state.
		  SPLAPPTRC(L_DEBUG, "Minor inconvenient Error: Inside getAllKeysImpl, it failed to get a store key properly. " <<
			  "This will lead to the unintended inclusion of the store's meta data keys as part of the store iteration. " <<
			  DPS_STORE_ITERATION_NULL_KEY_ERROR, "AerospikeDBLayer");
		  as_record_destroy(pRec1);
		  return;
	  } else if ((keyString == string(AEROSPIKE_STORE_ID_TO_STORE_NAME_KEY)) ||
		  (keyString == string(AEROSPIKE_SPL_TYPE_NAME_OF_KEY)) ||
		  (keyString == string(AEROSPIKE_SPL_TYPE_NAME_OF_VALUE))) {
		  // If it is one of the meta data keys, then don't consider it since we are collecting only the non-meta data keys.
		  as_record_destroy(pRec1);
		  return;
	  }

	  asDBLayerIterator->dataItemKeys.push_back(keyString);
	  as_record_destroy(pRec1);
  }

  // The 2nd argument will be pointing to a object of this class. That will allow us to
  // get access to our non-static members.
  bool AerospikeDBLayer::obtain_set_size_callback(const as_val *val, void *udata) {
	  if (val != NULL) {
		  static_cast<AerospikeDBLayer*>(udata)->obtainSetSizeImpl(val);
		  return(true);
	  } else {
		  // NULL value indicates that all records have been scanned.
		  // Nothing more to process. Hence, return a false to end the iteration.
		  return(false);
	  }
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  void AerospikeDBLayer::obtainSetSizeImpl(const as_val *val) {
	  // We will simply count the records as we get called into this method.
	  as_record *pRec = as_record_fromval(val);

	  if (pRec != NULL) {
		  // We will increment the member variable to communicate back to
		  // the method that initiated this callback.
		  storeSetSize++;
	  }
  }


} } } } }
