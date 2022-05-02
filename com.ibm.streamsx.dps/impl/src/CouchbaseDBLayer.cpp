/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2022
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
/*
=================================================================================
Couchbase is a NoSQL DB open source product that can provide a Key/Value store
functionality. More specifically, Couchbase is a document database. It combines
the features of membase (follow-on project for memcached) and CouchDB. It brings
the state of the art in-memory store capabilities from membase and a robust
persistence, multi-machine replication, sharding of data on many servers and
fault tolerance features from CouchDB. It has a wide range of client libraries
available in multiple programming languages. For our needs here, we are
using the Couchbase C SDK client library.

libcouchbase

We built this shared library (.so file) separately outside of Streams
and then copied the include directory (libcouchbase) into this toolkit's
impl/include directory. That .so file we built separately was also
copied into the impl/lib directory within the OS specific sub-directory.
=================================================================================
*/

/*
==================================================================================================================
This CPP file contains the source code for the distributed process store (dps) back-end activities such as
insert, update, read, remove, etc. This dps implementation runs on top of the Couchbase NoSQL DB.

At this time (Jan/2015), Couchbase is one of the top document data stores available in the open source community.
It ranks among the top for use cases with schema-less documents to be stored in a NoSQL fashion.
As far as the client APIs, Couchbase has support various popular programming languages C, , Java, Python,
JavaScript etc. In the DPS toolkit, we will mostly use the C APIs available for working with Couchbase.
For store creation, store deletion, getting store size etc., we will use the Couchbase REST APIs using cURL.

Any dpsXXXXX native function call made from a SPL composite will go through a serialization layer and then hit
this CPP file. Here, the purpose of such SPL native function calls will be fulfilled using the HTTP REST APIs.
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
Couchbase interface code (this file), and your Couchbase infrastructure.

As a first step, you should run the ./mk script from the C++ project directory (DistributedProcessStoreLib).
That will take care of building the .so file for the dps and copying it to the SPL project's impl/lib directory.
==================================================================================================================
*/

/*
IMPORTANT FUTURE WORK THAT CAN BE CARRIED OUT IN THE CODE BELOW FOR COUCHBASE:

a) As of Jan/2015, we have below repeating blocks of code in every method to perform the elaborate
   connectivity steps to the Couchbase server(s). It is a code bloat and it can be written optimally in
   future revisions of this file.

b) As of Jan/2015, Couchbase allows only a maximum of 10 buckets to exist at any given time.
   What a pain in Couchbase?
   That is a severe limitation if the DPS user wants to create numerous stores. That also needs to be addressed in the future.

c) As of Jan/2015, my tests in a Couchbase cluster of 5 machines (with replica 1) was so slow and it showed
   problems in properly reading or enumerating the data we wrote earlier. Hence, I put the replica number to 0 in the
   createBucket REST method at the bottom of this file. That can be revisited and see if a non-zero replica count
   can be used when a Couchbase cluster is used.
*/

#include "CouchbaseDBLayer.h"
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
  CouchbaseDBLayer::CouchbaseDBLayer()
  {
	  base64_chars = string("ABCDEFGHIJKLMNOPQRSTUVWXYZ") +
			  	  	 string("abcdefghijklmnopqrstuvwxyz") +
			  	  	 string("0123456789+/");

	  // We must initialize all the cURL related pointers we have as member variables in our C++ class.
	  // If we don't do that, it may point to random memory locations which will give GPF during runtime.
	  curlForCreateCouchbaseBucket = NULL;
	  curlForDeleteCouchbaseBucket = NULL;
	  curlForGetCouchbaseBucket = NULL;
	  headersForCreateCouchbaseBucket = NULL;
	  headersForCreateCouchbaseBucket2 = NULL;
	  headersForDeleteCouchbaseBucket = NULL;
	  headersForGetCouchbaseBucket = NULL;

	  curlBasicAuth = "";
	  couchbaseServerUrl = "";
	  couchbaseServerIdx = 0;
	  // Initialize our server array with empty strings.
	  for(int cnt=0; cnt < 50; cnt++) {
		  couchbaseServers[cnt] = "";
	  }
  }

  CouchbaseDBLayer::~CouchbaseDBLayer()
  {
	  // Application is ending now.
	  // Release the cURL resources we allocated at the application start-up time.
	  if (curlGlobalCleanupNeeded == true) {
		  // Free the cURL headers used with every cURL handle.
		  curl_slist_free_all(headersForCreateCouchbaseBucket);
		  curl_slist_free_all(headersForCreateCouchbaseBucket2);
		  curl_slist_free_all(headersForDeleteCouchbaseBucket);
		  curl_slist_free_all(headersForGetCouchbaseBucket);
		  // Free the cURL handles.
		  curl_easy_cleanup(curlForCreateCouchbaseBucket);
		  curl_easy_cleanup(curlForDeleteCouchbaseBucket);
		  curl_easy_cleanup(curlForGetCouchbaseBucket);
		  // Do the cURL global cleanup.
		  curl_global_cleanup();
	  }
  }
        
  void CouchbaseDBLayer::connectToDatabase(std::set<std::string> const & dbServers, PersistenceError & dbError)
  {
	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase", "CouchbaseDBLayer");

	  // Get the name, OS version and CPU type of this machine.
	  struct utsname machineDetails;

	  if(uname(&machineDetails) < 0) {
		  dbError.set("Unable to get the machine/os/cpu details.", DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to get the machine/os/cpu details. " << DPS_INITIALIZE_ERROR, "CouchbaseDBLayer");
		  return;
	  } else {
		  nameOfThisMachine = string(machineDetails.nodename);
		  osVersionOfThisMachine = string(machineDetails.sysname) + string(" ") + string(machineDetails.release);
		  cpuTypeOfThisMachine = string(machineDetails.machine);
	  }

	  // As of Jan/2015, Couchbase C SDK client libraries are not written to work correctly in IBM Power servers due to its
	  // big endian quirkiness. Due to this, Couchbase APIs we use here will not work as expected.
	  // Hence, we will return from here now telling the user that we don't support Couchbase on IBM Power servers.
	  if (cpuTypeOfThisMachine == "ppc64") {
		  dbError.set("DPS toolkit configured with a Couchbase NoSQL server is not supported on IBM Power machines.", DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_ERROR, "Inside connectToDatabase, it failed during initialization. Reason: " <<
			  "DPS toolkit configured with a Couchbase NoSQL server is not supported on IBM Power machines. " <<
			  DPS_INITIALIZE_ERROR, "CouchbaseDBLayer");
		  return;
	  }

	  string couchbaseConnectionErrorMsg = "";
	  CURLcode cResult = curl_global_init(CURL_GLOBAL_ALL);

	  if (cResult != CURLE_OK) {
		  couchbaseConnectionErrorMsg = "cURL global init failed.";
		  dbError.set(couchbaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed  with a cURL error code=" << cResult <<
		     ", Error Msg='" << string(curl_easy_strerror(cResult)) << "'. " << DPS_INITIALIZE_ERROR, "CouchbaseDBLayer");
		  return;
	  }

	  // Create all the cURL handles we will be using during the lifetime of this dps instance.
	  // This way, we will create all the cURL handles only once and reuse the handles as long as we want.
	  curlForCreateCouchbaseBucket = curl_easy_init();

	  if (curlForCreateCouchbaseBucket == NULL) {
		  curl_global_cleanup();
		  couchbaseConnectionErrorMsg = "cURL easy init failed for CreateCouchbaseStore.";
		  dbError.set(couchbaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for CreateCouchbaseStore. " <<
		     DPS_INITIALIZE_ERROR, "CouchbaseDBLayer");
		  return;
	  }

	  curlForDeleteCouchbaseBucket = curl_easy_init();

	  if (curlForDeleteCouchbaseBucket == NULL) {
		  curl_easy_cleanup(curlForCreateCouchbaseBucket);
		  curl_global_cleanup();
		  couchbaseConnectionErrorMsg = "cURL easy init failed for DeleteCouchbaseStore.";
		  dbError.set(couchbaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for DeleteCouchbaseStore. " <<
		     DPS_INITIALIZE_ERROR, "CouchbaseDBLayer");
		  return;
	  }

	  curlForGetCouchbaseBucket = curl_easy_init();

	  if (curlForGetCouchbaseBucket == NULL) {
		  curl_easy_cleanup(curlForCreateCouchbaseBucket);
		  curl_easy_cleanup(curlForDeleteCouchbaseBucket);
		  curl_global_cleanup();
		  couchbaseConnectionErrorMsg = "cURL easy init failed for GetCouchbaseStore.";
		  dbError.set(couchbaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for GetCouchbaseStore. " <<
		     DPS_INITIALIZE_ERROR, "CouchbaseDBLayer");
		  return;
	  }

	  headersForCreateCouchbaseBucket = curl_slist_append(headersForCreateCouchbaseBucket, "Accept: */*");
	  headersForCreateCouchbaseBucket = curl_slist_append(headersForCreateCouchbaseBucket, "Content-Type: application/x-www-form-urlencoded");
	  // Let us have another create bucket headers with a content-type of application/json to send JSON data via HTTP PUT.
	  headersForCreateCouchbaseBucket2 = curl_slist_append(headersForCreateCouchbaseBucket2, "Accept: */*");
	  headersForCreateCouchbaseBucket2 = curl_slist_append(headersForCreateCouchbaseBucket2, "Content-Type: application/json");
	  headersForDeleteCouchbaseBucket = curl_slist_append(headersForDeleteCouchbaseBucket, "Accept: */*");
	  headersForDeleteCouchbaseBucket = curl_slist_append(headersForDeleteCouchbaseBucket, "Content-Type: text/plain");
	  headersForGetCouchbaseBucket = curl_slist_append(headersForGetCouchbaseBucket, "Accept: */*");
	  headersForGetCouchbaseBucket = curl_slist_append(headersForGetCouchbaseBucket, "Content-Type: text/plain");

	  // Set this flag so that we can release all the cURL related resources in this class' destructor method.
	  curlGlobalCleanupNeeded = true;
	  couchbaseServerIdx = 0;
	  totalCouchbaseServers = 0;
	  std::size_t tmpIdx;

	  // Let us connect to the configured Couchbase servers.
	  // Couchbase supports a server cluster. Hence, user may provide one or more server names.
	  // Form a string with all the servers listed in this format: couchbase://server1;server2;server3/
	  for (std::set<std::string>::iterator it=dbServers.begin(); it!=dbServers.end(); ++it) {
		  if (couchbaseServerUrl == "") {
			  //Before we store the very first server, let us initialize it with the couchbase protocol tag.
			  couchbaseServerUrl = "couchbase://";
		  } else {
			  // If there are multiple configured servers, add a semicolon next to the most recent server name we added.
			  couchbaseServerUrl += ";";
		  }

		  // User must specify the couchbase admin user id and password as part of any of the configured server names.
		  // This is done by tagging the username and password to any of the configured server names.
		  // In our DPS configuration file, we asked the user to tag any one of the configured server names with the
		  // admin user id and password. Let us split the user id and password now if the current server name is
		  // tagged with that information.
		  // e-g: user:password@Machine1
		  tmpIdx = (*it).find("@");

		  if (tmpIdx == std::string::npos) {
			  // There is no @ character in this server name.
			  // Since this server name is not tagged with the user id and password, the actual
			  // server name will start from index 0.
			  tmpIdx = 0;
		  } else {
			  // Parse the userid:password combination.
			  curlBasicAuth = (*it).substr(0, tmpIdx);
			  // Outside of this if block, parse the server name starting after the @ character.
			  tmpIdx++;
		  }

		  // Append it to the URL string.
		  couchbaseServerUrl += (*it).substr(tmpIdx);
		  // Add it to the list of all the available Couchbase servers.
		  // This list has a fixed number of slots as hardcoded in the .h file of this class.
		  couchbaseServers[couchbaseServerIdx++] = (*it).substr(tmpIdx);
		  totalCouchbaseServers++;
	  }

	  if (curlBasicAuth == "") {
		  // It looks like user has not tagged any of the server names with the couchbase admin user id and password.
		  couchbaseConnectionErrorMsg = "Couldn't find user:password in front of any of the configured server name(s).";
		  dbError.set(couchbaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed while parsing the admin userid:password in the configuration file. Msg=" <<
			  couchbaseConnectionErrorMsg << ". " << DPS_INITIALIZE_ERROR, "CouchbaseDBLayer");
		  return;
	  }

	  // Append a forward slash at the end of the server names.
	  couchbaseServerUrl += "/";
	  // Point to the first server in the server list.
	  couchbaseServerIdx = 0;

	  // We can now create two Couchbase stores for our use later.
	  // 1) DPS_DL meta data store
	  // 2) TTL store
	  bool result = createCouchbaseBucket(string(DPS_DL_META_DATA_DB), couchbaseConnectionErrorMsg,
		  string(COUCHBASE_META_DATA_BUCKET_QUOTA_IN_MB));

	  if (result == false) {
		  dbError.set(couchbaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error while creating the meta data store. Msg=" <<
			  couchbaseConnectionErrorMsg << ". " << DPS_INITIALIZE_ERROR, "CouchbaseDBLayer");
		  return;
	  }

	  result = createCouchbaseBucket(string(DPS_TTL_STORE_TOKEN), couchbaseConnectionErrorMsg,
		  string(COUCHBASE_TTL_BUCKET_QUOTA_IN_MB));

	  if (result == false) {
		  dbError.set(couchbaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error while creating the TTL store. Msg=" <<
			  couchbaseConnectionErrorMsg << ". " << DPS_INITIALIZE_ERROR, "CouchbaseDBLayer");
		  return;
	  }

	  // We have now initialized the Couchbase client and made contacts with the configured Couchbase server(s).
	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase done", "CouchbaseDBLayer");
  }

  uint64_t CouchbaseDBLayer::createStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside createStore for store " << name, "CouchbaseDBLayer");

	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);

 	// Get a general purpose lock so that only one thread can
	// enter inside of this method at any given time with the same store name.
 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
 		// Unable to acquire the general purpose lock.
 		dbError.set("Unable to get a generic lock for creating a store with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for an yet to be created store with its name as " <<
 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "CouchbaseDBLayer");
 		// User has to retry again to create this store.
 		return (0);
 	}

    // Let us first see if a store with the given name already exists in our Couchbase database.
 	uint64_t storeId = findStore(name, dbError);

 	if (storeId > 0) {
		// This store already exists in our cache.
		// We can't create another one with the same name now.
 		std::ostringstream storeIdString;
 		storeIdString << storeId;
 		// The following error message carries the existing store's id at the very end of the message.
		dbError.set("A store named " + name + " already exists with a store id " + storeIdString.str(), DPS_STORE_EXISTS);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed while trying to create a duplicate store " << name << ". " << DPS_STORE_EXISTS, "CouchbaseDBLayer");
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
	We secured a guid. We can now create this store. Layout for a distributed process store (dps) looks like this in Couchbase.
	In the Couchbase database, DPS store/lock, and the individual store details will follow these data formats.
	That will allow us to be as close to the other DPS implementations using memcached, Redis, Cassandra, Cloudant, HBase and Mongo.
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

 	// We will do the first two activities from the list above for our Couchbase implementation.
	// Let us now do the step 1 described in the commentary above.
	// 1) Create the Store Name entry in the dps_dl_meta_data bucket.
	//    '0' + 'store name' => 'store id'
	std::ostringstream storeIdString;
	storeIdString << storeId;
	string errorMsg = "";

	struct lcb_create_st cropts;
	memset(&cropts, 0, sizeof cropts);
	cropts.version = 3;
	cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
	lcb_error_t err;
	lcb_t instance;

	err = lcb_create(&instance, &cropts);

	if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  dbError.set("Inside createStore, it failed to create the Couchbase instance 1 for the store " +
			  name + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_NAME_CREATION_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create the Couchbase instance 1 for the store " <<
			  name << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DPS_STORE_NAME_CREATION_ERROR, "CouchbaseDBLayer");
		  releaseGeneralPurposeLock(base64_encoded_name);
		  return(0);
	}

	lcb_connect(instance);
	lcb_wait(instance);

	if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside createStore, it failed to bootstrap the connection to the Couchbase instance 1 for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_NAME_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside createStore, it failed to bootstrap the connection to the Couchbase instance 1 for the store " <<
			name << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_NAME_CREATION_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(0);
	}

	// In Couchbase, if a string contains only digits (numerals), then it stores it internally as a
	// number rather than a string. During that process, it rounds that number and loses the original
	// number intended by the string. That is bad. Hence, we will base64 encode it so that it will not be
	// made of all numerals.
	string base64_encoded_store_id;
	base64_encode(storeIdString.str(), base64_encoded_store_id);

	lcb_set_store_callback(instance, storage_callback);
	lcb_store_cmd_t scmd;
	memset(&scmd, 0, sizeof scmd);
	const lcb_store_cmd_t *scmdlist = &scmd;
	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
	scmd.v.v0.key = dpsAndDlGuidKey.c_str();
	scmd.v.v0.nkey = dpsAndDlGuidKey.length();
	scmd.v.v0.bytes = base64_encoded_store_id.c_str();
	scmd.v.v0.nbytes = base64_encoded_store_id.length();
	scmd.v.v0.operation = LCB_SET;
	lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	lastCouchbaseErrorMsg = "";
	err = lcb_store(instance, this, 1, &scmdlist);

	if (err != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside createStore, it failed to schedule a Couchbase storage operation 1 (Guid-->StoreName) for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_NAME_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside createStore, it failed to schedule a Couchbase storage operation 1 (Guid-->StoreName) for the store " <<
			name <<". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_NAME_CREATION_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(0);
	}

	//storage_callback is being invoked during this wait.
	lcb_wait(instance);

	// Check the storage operation result.
	if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		// There is some other error while storing.
		// Let us exit now.
		char errCodeString[50];
		sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		dbError.set("Inside createStore, it failed during the storage operation 1 (Guid-->StoreName) for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_STORE_NAME_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside createStore, it failed during the storage operation 1 (Guid-->StoreName) for the store " <<
			name << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			DPS_STORE_NAME_CREATION_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(0);
	}

	// 2) Create the Store Contents bucket now.
 	// This is the Couchbase bucket where we will keep storing all the key value pairs belonging to this store.
	// Bucket name: 'dps_' + '1_' + 'store id'
	string storeBucketName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString.str();
	bool result = createCouchbaseBucket(storeBucketName, errorMsg, string(COUCHBASE_RAM_BUCKET_QUOTA_IN_MB));

	if (result == false) {
		dbError.set("Inside createStore, it failed with an error while creating the Couchbase bucket for the store " +
				name + ". Msg=" + errorMsg, DPS_STORE_NAME_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed with an error while creating the Couchbase bucket for the store " +
			name + ". Msg=" << errorMsg << ". " << DPS_STORE_NAME_CREATION_ERROR, "CouchbaseDBLayer");
		// We can delete the store name entry we created above.
		lcb_set_remove_callback(instance, remove_callback);
		lcb_remove_cmd_t rcmd;
		memset(&rcmd, 0, sizeof rcmd);
		const lcb_remove_cmd_t *rcmdlist = &rcmd;
		rcmd.v.v0.key = dpsAndDlGuidKey.c_str();;
		rcmd.v.v0.nkey = dpsAndDlGuidKey.length();
		err = lcb_remove(instance, this, 1, &rcmdlist);
		lcb_wait(instance);
		lcb_destroy(instance);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(0);
	}

	// Every store contents bucket will always have these three metadata entries:
	// dps_name_of_this_store ==> 'store name'
	// dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	// dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	//
	// Every store contents bucket will have at least three entries in it carrying the actual store name, key spl type name and value spl type name.
	// In addition, inside this store contents bucket, we will house the
	// actual key:value data items that the user wants to keep in this store.
	// Such a store contents bucket is very useful for data item read, write, deletion, enumeration etc.
 	//
	struct lcb_create_st cropts2;
	memset(&cropts2, 0, sizeof cropts2);
	cropts2.version = 3;
	cropts2.v.v3.connstr = (couchbaseServerUrl + storeBucketName).c_str();
	lcb_t instance2;

	err = lcb_create(&instance2, &cropts2);

	if (err != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(NULL, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside createStore, it failed to create the Couchbase instance 2 for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create the Couchbase instance 2 for the store " <<
			name + ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_HASH_METADATA1_CREATION_ERROR, "CouchbaseDBLayer");
		// We can delete the store name entry we created above.
		lcb_set_remove_callback(instance, remove_callback);
		lcb_remove_cmd_t rcmd;
		memset(&rcmd, 0, sizeof rcmd);
		const lcb_remove_cmd_t *rcmdlist = &rcmd;
		rcmd.v.v0.key = dpsAndDlGuidKey.c_str();;
		rcmd.v.v0.nkey = dpsAndDlGuidKey.length();
		err = lcb_remove(instance, this, 1, &rcmdlist);
		lcb_wait(instance);
		lcb_destroy(instance);
		// Delete the Couchbase bucket we created for this store.
		result = deleteCouchbaseBucket(storeBucketName, errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(0);
	}

	lcb_connect(instance2);
	lcb_wait(instance2);

	if ((err = lcb_get_bootstrap_status(instance2)) != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance2, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside createStore, it failed to bootstrap the connection to the Couchbase instance 2 for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside createStore, it failed to bootstrap the connection to the Couchbase instance 2 for the store " <<
			name << " Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_HASH_METADATA1_CREATION_ERROR, "CouchbaseDBLayer");
		// We can delete the store name entry we created above.
		lcb_set_remove_callback(instance, remove_callback);
		lcb_remove_cmd_t rcmd;
		memset(&rcmd, 0, sizeof rcmd);
		const lcb_remove_cmd_t *rcmdlist = &rcmd;
		rcmd.v.v0.key = dpsAndDlGuidKey.c_str();;
		rcmd.v.v0.nkey = dpsAndDlGuidKey.length();
		err = lcb_remove(instance, this, 1, &rcmdlist);
		lcb_wait(instance);
		lcb_destroy(instance);
		lcb_destroy(instance2);
		// Delete the Couchbase bucket we created for this store.
		result = deleteCouchbaseBucket(storeBucketName, errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(0);
	}

	// Let us populate the new store contents bucket with a mandatory element that will carry the name of this store.
	// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
	lcb_set_store_callback(instance2, storage_callback);
	memset(&scmd, 0, sizeof scmd);
	scmd.v.v0.key = string(COUCHBASE_STORE_ID_TO_STORE_NAME_KEY).c_str();
	scmd.v.v0.nkey = string(COUCHBASE_STORE_ID_TO_STORE_NAME_KEY).length();
	scmd.v.v0.bytes = base64_encoded_name.c_str();
	scmd.v.v0.nbytes = base64_encoded_name.length();
	scmd.v.v0.operation = LCB_SET;
	lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	lastCouchbaseErrorMsg = "";
	err = lcb_store(instance2, this, 1, &scmdlist);

	if (err != LCB_SUCCESS) {
		// There was an error in creating Meta Data 1.
		errorMsg = string(lcb_strerror(instance2, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside createStore, it failed to schedule a Couchbase storage operation 2 (Meta Data 1) for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside createStore, it failed to schedule a Couchbase storage operation 2 (Meta Data 1) for the store " <<
			name << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_HASH_METADATA1_CREATION_ERROR, "CouchbaseDBLayer");
		// We can delete the store name entry we created above.
		lcb_set_remove_callback(instance, remove_callback);
		lcb_remove_cmd_t rcmd;
		memset(&rcmd, 0, sizeof rcmd);
		const lcb_remove_cmd_t *rcmdlist = &rcmd;
		rcmd.v.v0.key = dpsAndDlGuidKey.c_str();;
		rcmd.v.v0.nkey = dpsAndDlGuidKey.length();
		err = lcb_remove(instance, this, 1, &rcmdlist);
		lcb_wait(instance);
		lcb_destroy(instance);
		lcb_destroy(instance2);
		// Delete the Couchbase bucket we created for this store.
		result = deleteCouchbaseBucket(storeBucketName, errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(0);
	}

	//storage_callback is being invoked during this wait.
	lcb_wait(instance2);

	// Check the storage operation result.
	if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		// There was an error in creating Meta Data 1.
		// There is some other error while storing.
		// Let us exit now.
		char errCodeString[50];
		sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		dbError.set("Inside createStore, it failed during the storage operation 2 (Meta Data 1) for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside createStore, it failed during the storage operation 2 (Meta Data 1) for the store " <<
			name << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			DPS_STORE_HASH_METADATA1_CREATION_ERROR, "CouchbaseDBLayer");
		// We can delete the store name entry we created above.
		lcb_set_remove_callback(instance, remove_callback);
		lcb_remove_cmd_t rcmd;
		memset(&rcmd, 0, sizeof rcmd);
		const lcb_remove_cmd_t *rcmdlist = &rcmd;
		rcmd.v.v0.key = dpsAndDlGuidKey.c_str();;
		rcmd.v.v0.nkey = dpsAndDlGuidKey.length();
		err = lcb_remove(instance, this, 1, &rcmdlist);
		lcb_wait(instance);
		lcb_destroy(instance);
		lcb_destroy(instance2);
		// Delete the Couchbase bucket we created for this store.
		result = deleteCouchbaseBucket(storeBucketName, errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(0);
	}

    // We are now going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
	// Add the key spl type name metadata.
	string base64_encoded_keySplTypeName;
	base64_encode(keySplTypeName, base64_encoded_keySplTypeName);
	memset(&scmd, 0, sizeof scmd);
	scmd.v.v0.key = string(COUCHBASE_SPL_TYPE_NAME_OF_KEY).c_str();
	scmd.v.v0.nkey = string(COUCHBASE_SPL_TYPE_NAME_OF_KEY).length();
	scmd.v.v0.bytes = base64_encoded_keySplTypeName.c_str();
	scmd.v.v0.nbytes = base64_encoded_keySplTypeName.length();
	scmd.v.v0.operation = LCB_SET;
	lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	lastCouchbaseErrorMsg = "";
	err = lcb_store(instance2, this, 1, &scmdlist);

	if (err != LCB_SUCCESS) {
		// There was an error in creating Meta Data 2.
		errorMsg = string(lcb_strerror(instance2, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside createStore, it failed to schedule a Couchbase storage operation 3 (Meta Data 2) for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside createStore, it failed to schedule a Couchbase storage operation 3 (Meta Data 2) for the store " <<
			name << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_HASH_METADATA2_CREATION_ERROR, "CouchbaseDBLayer");
		// We can delete the store name entry we created above.
		lcb_set_remove_callback(instance, remove_callback);
		lcb_remove_cmd_t rcmd;
		memset(&rcmd, 0, sizeof rcmd);
		const lcb_remove_cmd_t *rcmdlist = &rcmd;
		rcmd.v.v0.key = dpsAndDlGuidKey.c_str();;
		rcmd.v.v0.nkey = dpsAndDlGuidKey.length();
		err = lcb_remove(instance, this, 1, &rcmdlist);
		lcb_wait(instance);
		lcb_destroy(instance);
		lcb_destroy(instance2);
		// Delete the Couchbase bucket we created for this store.
		result = deleteCouchbaseBucket(storeBucketName, errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(0);
	}

	//storage_callback is being invoked during this wait.
	lcb_wait(instance2);

	// Check the storage operation result.
	if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		// There was an error in creating Meta Data 2.
		// There is some other error while storing.
		// Let us exit now.
		char errCodeString[50];
		sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		dbError.set("Inside createStore, it failed during the storage operation 3 (Meta Data 2) for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside createStore, it failed during the storage operation 3 (Meta Data 2) for the store " <<
			name << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			DPS_STORE_HASH_METADATA2_CREATION_ERROR, "CouchbaseDBLayer");
		// We can delete the store name entry we created above.
		lcb_set_remove_callback(instance, remove_callback);
		lcb_remove_cmd_t rcmd;
		memset(&rcmd, 0, sizeof rcmd);
		const lcb_remove_cmd_t *rcmdlist = &rcmd;
		rcmd.v.v0.key = dpsAndDlGuidKey.c_str();;
		rcmd.v.v0.nkey = dpsAndDlGuidKey.length();
		err = lcb_remove(instance, this, 1, &rcmdlist);
		lcb_wait(instance);
		lcb_destroy(instance);
		lcb_destroy(instance2);
		// Delete the Couchbase bucket we created for this store.
		result = deleteCouchbaseBucket(storeBucketName, errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(0);
	}

	// Add the value spl type name metadata.
	string base64_encoded_valueSplTypeName;
	base64_encode(valueSplTypeName, base64_encoded_valueSplTypeName);
	memset(&scmd, 0, sizeof scmd);
	scmd.v.v0.key = string(COUCHBASE_SPL_TYPE_NAME_OF_VALUE).c_str();
	scmd.v.v0.nkey = string(COUCHBASE_SPL_TYPE_NAME_OF_VALUE).length();
	scmd.v.v0.bytes = base64_encoded_valueSplTypeName.c_str();
	scmd.v.v0.nbytes = base64_encoded_valueSplTypeName.length();
	scmd.v.v0.operation = LCB_SET;
	lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	lastCouchbaseErrorMsg = "";
	err = lcb_store(instance2, this, 1, &scmdlist);

	if (err != LCB_SUCCESS) {
		// There was an error in creating Meta Data 3.
		errorMsg = string(lcb_strerror(instance2, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside createStore, it failed to schedule a Couchbase storage operation 4 (Meta Data 3) for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside createStore, it failed to schedule a Couchbase storage operation 4 (Meta Data 3) for the store " <<
			name << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_HASH_METADATA3_CREATION_ERROR, "CouchbaseDBLayer");
		// We can delete the store name entry we created above.
		lcb_set_remove_callback(instance, remove_callback);
		lcb_remove_cmd_t rcmd;
		memset(&rcmd, 0, sizeof rcmd);
		const lcb_remove_cmd_t *rcmdlist = &rcmd;
		rcmd.v.v0.key = dpsAndDlGuidKey.c_str();;
		rcmd.v.v0.nkey = dpsAndDlGuidKey.length();
		err = lcb_remove(instance, this, 1, &rcmdlist);
		lcb_wait(instance);
		lcb_destroy(instance);
		lcb_destroy(instance2);
		// Delete the Couchbase bucket we created for this store.
		result = deleteCouchbaseBucket(storeBucketName, errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(0);
	}

	//storage_callback is being invoked during this wait.
	lcb_wait(instance2);

	// Check the storage operation result.
	if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		// There was an error in creating Meta Data 3.
		// There is some other error while storing.
		// Let us exit now.
		char errCodeString[50];
		sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		dbError.set("Inside createStore, it failed during the storage operation 4 (Meta Data 3) for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside createStore, it failed during the storage operation 4 (Meta Data 3) for the store " <<
			name << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			DPS_STORE_HASH_METADATA3_CREATION_ERROR, "CouchbaseDBLayer");
		// We can delete the store name entry we created above.
		lcb_set_remove_callback(instance, remove_callback);
		lcb_remove_cmd_t rcmd;
		memset(&rcmd, 0, sizeof rcmd);
		const lcb_remove_cmd_t *rcmdlist = &rcmd;
		rcmd.v.v0.key = dpsAndDlGuidKey.c_str();;
		rcmd.v.v0.nkey = dpsAndDlGuidKey.length();
		err = lcb_remove(instance, this, 1, &rcmdlist);
		lcb_wait(instance);
		lcb_destroy(instance);
		lcb_destroy(instance2);
		// Delete the Couchbase bucket we created for this store.
		result = deleteCouchbaseBucket(storeBucketName, errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(0);
	} else {
		// We created a new store as requested by the caller. Return the store id.
		lcb_destroy(instance);
		lcb_destroy(instance2);
		releaseGeneralPurposeLock(base64_encoded_name);
		return(storeId);
	}
  }

  uint64_t CouchbaseDBLayer::createOrGetStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	// We will rely on a method above this and another method below this to accomplish what is needed here.
	SPLAPPTRC(L_DEBUG, "Inside createOrGetStore for store " << name, "CouchbaseDBLayer");
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
	//  Cassandra, Cloudant, HBase, Mongo, and Couchbase DPS implementations.
	//  For memcached and Redis, we do it differently.)
	dbError.reset();
	// We will take the hashCode of the encoded store name and use that as the store id.
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);
	storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);
	return(storeId);
  }
                
  uint64_t CouchbaseDBLayer::findStore(std::string const & name, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside findStore for store " << name, "CouchbaseDBLayer");

	// We can search in the dps_dl_meta_data collection to see if a store exists for the given store name.
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);
	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
	string errorMsg = "";

	struct lcb_create_st cropts;
	memset(&cropts, 0, sizeof cropts);
	cropts.version = 3;
	cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
	lcb_error_t err;
	lcb_t instance;

	err = lcb_create(&instance, &cropts);

	if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  dbError.set("Inside findStore, it failed to create the Couchbase instance for the store " +
			  name + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_EXISTENCE_CHECK_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside findStore, it failed to create the Couchbase instance for the store " <<
			  name << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DPS_STORE_EXISTENCE_CHECK_ERROR, "CouchbaseDBLayer");
		  return(0);
	}

	lcb_connect(instance);
	lcb_wait(instance);

	if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside findStore, it failed to bootstrap the connection to the Couchbase instance for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_EXISTENCE_CHECK_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside findStore, it failed to bootstrap the connection to the Couchbase instance for the store " <<
			name << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_EXISTENCE_CHECK_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		return(0);
	}

	lcb_set_get_callback(instance, get_callback);
	lcb_get_cmd_t gcmd;
	memset(&gcmd, 0, sizeof gcmd);
	const lcb_get_cmd_t *gcmdlist = &gcmd;
	gcmd.v.v0.key = dpsAndDlGuidKey.c_str();
	gcmd.v.v0.nkey = dpsAndDlGuidKey.length();
	lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	lastCouchbaseErrorMsg = "";
	err = lcb_get(instance, this, 1, &gcmdlist);

	if (err != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside findStore, it failed to schedule a Couchbase get operation for the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_EXISTENCE_CHECK_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside findStore, it failed to schedule a Couchbase get operation for the store " <<
			name << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_EXISTENCE_CHECK_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		return(0);
	}

	// get callback is being invoked during this wait.
	lcb_wait(instance);

	if (lastCouchbaseErrorCode == LCB_SUCCESS) {
		// This store already exists in our cache.
 	 	// We will take the hashCode of the encoded store name and use that as the store id.
		lcb_destroy(instance);
 	 	uint64_t storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);
 	 	return(storeId);
	} else {
		// Requested data item is not there in Couchbase.
		char errCodeString[50];
		sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		dbError.set("Inside findStore, it couldn't find the store " +
			name + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_DATA_ITEM_READ_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside findStore, it couldn't find the store " <<
			name << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		return(0);
	}
  }
        
  bool CouchbaseDBLayer::removeStore(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside removeStore for store id " << store, "CouchbaseDBLayer");

	ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CouchbaseDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "CouchbaseDBLayer");
		// User has to retry again to remove the store.
		return(false);
	}

	// Get rid of these two entries that are holding the store contents and the store name root entry.
	// 1) Store Contents bucket
	// 2) Store Name root entry
	//
	uint32_t dataItemCnt = 0;
	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		// In Couchbase, I noticed that the readStoreInformation method returns the following error in certain cases.
		// DPS_GET_STORE_SIZE_ERROR
		// (This error can be ignored for the purposes of the removeStore operation that we have to perform right now.)
		// As long as we have the store name, we can go ahead and remove the store referred to by the caller specified store id.
		// Hence, we can bail out of this method ONLY when the error is in connection with getting the actual store name.
		if (dbError.getErrorCode() == DPS_GET_STORE_NAME_ERROR) {
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
			releaseStoreLock(storeIdString);
			// This is alarming. This will put this store in a bad state. Poor user has to deal with it.
			return(false);
		} else {
			// Reset the error that can be ignored in this particular case of removing a store.
			dbError.reset();
			// We will not return here. Instead, we will continue below with the store removal operation.
		}
	}

	// Let us delete the Store Contents bucket that contains all the active data items in this store.
	// Bucket name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
 	string storeBucketName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
 	string errorMsg = "";
 	bool result = deleteCouchbaseBucket(storeBucketName, errorMsg);

	if (result == false) {
		// Error in deleting the store bucket.
		dbError.set("Unable to delete the store bucket for the StoreId " + storeIdString +
			". Error=" + errorMsg, DPS_STORE_REMOVAL_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString <<
			". (" << errorMsg << ") Unable to delete the store bucket. " << DPS_STORE_REMOVAL_ERROR, "CouchbaseDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	struct lcb_create_st cropts;
	memset(&cropts, 0, sizeof cropts);
	cropts.version = 3;
	cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
	lcb_error_t err;
	lcb_t instance;

	err = lcb_create(&instance, &cropts);

	if (err != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(NULL, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Failed to create the Couchbase instance for the store id " + storeIdString +
			". Error=" + errorMsg, DPS_STORE_REMOVAL_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to create the Couchbase instance for the store id " <<
			storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " << DPS_STORE_REMOVAL_ERROR, "CouchbaseDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	lcb_connect(instance);
	lcb_wait(instance);

	if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Failed to bootstrap the connection to the Couchbase instance for the store id " + storeIdString +
			". Error=" + errorMsg, DPS_STORE_REMOVAL_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside removeStore, it failed to bootstrap the connection to the Couchbase instance for the store id " <<
			storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " << DPS_STORE_REMOVAL_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		releaseStoreLock(storeIdString);
		return(false);
	}

	// We can delete the store name entry we created for this store.
	lcb_set_remove_callback(instance, remove_callback);
	lcb_remove_cmd_t rcmd;
	memset(&rcmd, 0, sizeof rcmd);
	const lcb_remove_cmd_t *rcmdlist = &rcmd;
	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + storeName;
	rcmd.v.v0.key = dpsAndDlGuidKey.c_str();;
	rcmd.v.v0.nkey = dpsAndDlGuidKey.length();
	err = lcb_remove(instance, this, 1, &rcmdlist);
	lcb_wait(instance);
	lcb_destroy(instance);
	// Life of this store ended completely with no trace left behind.
	releaseStoreLock(storeIdString);
    return(true);
  }

  // This is a lean and mean put operation into a store.
  // It doesn't do any safety checks before putting a data item into a store.
  // If you want to go through that rigor, please use the putSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool CouchbaseDBLayer::put(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside put for store id " << store, "CouchbaseDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In our Couchbase dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	// Let us convert the binary formatted valueData in our K/V pair to a base64 encoded string.
	string base64_encoded_data_item_value;
	b64_encode(valueData, valueSize, base64_encoded_data_item_value);

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Contents bucket that takes the following name.
	// Bucket name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeBucketName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;

	struct lcb_create_st cropts;
	string errorMsg;
	memset(&cropts, 0, sizeof cropts);
	cropts.version = 3;
	cropts.v.v3.connstr = (couchbaseServerUrl + storeBucketName).c_str();
	lcb_error_t err;
	lcb_t instance;

	err = lcb_create(&instance, &cropts);

	if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  dbError.set("Inside put, it failed to create the Couchbase instance for the store id " +
			  storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside put, it failed to create the Couchbase instance for the store id " <<
			  storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DPS_DATA_ITEM_WRITE_ERROR, "CouchbaseDBLayer");
		  return(false);
	}

	lcb_connect(instance);
	lcb_wait(instance);

	if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside put, it failed to bootstrap the connection to the Couchbase instance for the store id " +
				storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside put, it failed to bootstrap the connection to the Couchbase instance for the store id " <<
			storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_DATA_ITEM_WRITE_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		return(false);
	}

	lcb_set_store_callback(instance, storage_callback);
	lcb_store_cmd_t scmd;
	memset(&scmd, 0, sizeof scmd);
	const lcb_store_cmd_t *scmdlist = &scmd;
	scmd.v.v0.key = base64_encoded_data_item_key.c_str();
	scmd.v.v0.nkey = base64_encoded_data_item_key.length();
	scmd.v.v0.bytes = base64_encoded_data_item_value.c_str();
	scmd.v.v0.nbytes = base64_encoded_data_item_value.length();
	// LCB_SET unconditionally stores the document to the cluster, regardless of whether it already exists or not.
	scmd.v.v0.operation = LCB_SET;
	lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	lastCouchbaseErrorMsg = "";
	err = lcb_store(instance, this, 1, &scmdlist);

	if (err != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside put, it failed to schedule a Couchbase storage operation for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside put, it failed to schedule a Couchbase storage operation for the store id " <<
			storeIdString <<". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_DATA_ITEM_WRITE_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		return(false);
	}

	//storage_callback is being invoked during this wait.
	lcb_wait(instance);

	// Check the storage operation result.
	if (lastCouchbaseErrorCode == LCB_SUCCESS) {
		lcb_destroy(instance);
		return(true);
	} else {
		// There is some other error while storing.
		// Let us exit now.
		char errCodeString[50];
		sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		dbError.set("Inside put, it failed during the storage operation for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside put, it failed during the storage operation for the store id " <<
			storeIdString << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			DPS_DATA_ITEM_WRITE_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		return(false);
	}
  }

  // This is a special bullet proof version that does several safety checks before putting a data item into a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular put method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool CouchbaseDBLayer::putSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside putSafe for store id " << store, "CouchbaseDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to find a store with a store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CouchbaseDBLayer");
		}

		return(false);
	}

	// In our Couchbase dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	// Let us convert the binary formatted valueData in our K/V pair to a base64 encoded string.
	string base64_encoded_data_item_value;
	b64_encode(valueData, valueSize, base64_encoded_data_item_value);

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "CouchbaseDBLayer");
		// User has to retry again to put the data item in the store.
		return(false);
	}

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Contents bucket that takes the following name.
	// Bucket name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeBucketName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;

	struct lcb_create_st cropts;
	string errorMsg;
	memset(&cropts, 0, sizeof cropts);
	cropts.version = 3;
	cropts.v.v3.connstr = (couchbaseServerUrl + storeBucketName).c_str();
	lcb_error_t err;
	lcb_t instance;

	err = lcb_create(&instance, &cropts);

	if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  dbError.set("Inside putSafe, it failed to create the Couchbase instance for the store id " +
			  storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to create the Couchbase instance for the store id " <<
			  storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DPS_DATA_ITEM_WRITE_ERROR, "CouchbaseDBLayer");
		  releaseStoreLock(storeIdString);
		  return(false);
	}

	lcb_connect(instance);
	lcb_wait(instance);

	if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside putSafe, it failed to bootstrap the connection to the Couchbase instance for the store id " +
				storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside putSafe, it failed to bootstrap the connection to the Couchbase instance for the store id " <<
			storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_DATA_ITEM_WRITE_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		releaseStoreLock(storeIdString);
		return(false);
	}

	lcb_set_store_callback(instance, storage_callback);
	lcb_store_cmd_t scmd;
	memset(&scmd, 0, sizeof scmd);
	const lcb_store_cmd_t *scmdlist = &scmd;
	scmd.v.v0.key = base64_encoded_data_item_key.c_str();
	scmd.v.v0.nkey = base64_encoded_data_item_key.length();
	scmd.v.v0.bytes = base64_encoded_data_item_value.c_str();
	scmd.v.v0.nbytes = base64_encoded_data_item_value.length();
	// LCB_SET unconditionally stores the document to the cluster, regardless of whether it already exists or not.
	scmd.v.v0.operation = LCB_SET;
	lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	lastCouchbaseErrorMsg = "";
	err = lcb_store(instance, this, 1, &scmdlist);

	if (err != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside putSafe, it failed to schedule a Couchbase storage operation for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside putSafe, it failed to schedule a Couchbase storage operation for the store id " <<
			storeIdString <<". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_DATA_ITEM_WRITE_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		releaseStoreLock(storeIdString);
		return(false);
	}

	//storage_callback is being invoked during this wait.
	lcb_wait(instance);

	// Check the storage operation result.
	if (lastCouchbaseErrorCode == LCB_SUCCESS) {
		lcb_destroy(instance);
		releaseStoreLock(storeIdString);
		return(true);
	} else {
		// There is some other error while storing.
		// Let us exit now.
		char errCodeString[50];
		sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		dbError.set("Inside putSafe, it failed during the storage operation for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside putSafe, it failed during the storage operation for the store id " <<
			storeIdString << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			DPS_DATA_ITEM_WRITE_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		releaseStoreLock(storeIdString);
		return(false);
	}
  }

  // Put a data item with a TTL (Time To Live in seconds) value into the global area of the Couchbase K/V store.
  bool CouchbaseDBLayer::putTTL(char const * keyData, uint32_t keySize,
		  	  	  	  	    unsigned char const * valueData, uint32_t valueSize,
							uint32_t ttl, PersistenceError & dbError, bool encodeKey, bool encodeValue)
  {
	  SPLAPPTRC(L_DEBUG, "Inside putTTL.", "CouchbaseDBLayer");

	  // In our Couchbase dps implementation, data item keys can have space characters.
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

	  // In Couchbase, background task that does the TTL expiration runs for every hour.
	  // However, any attempt to work on an already expired data item still still sitting there will automatically expire that entry.
	  uint64_t _ttl = ttl;
	  if (_ttl == 0) {
		  // User wants this TTL to be forever.
		  // In the case of the DPS Couchbase layer, we will use a TTL value of 25 years represented in seconds.
		  // If our caller opts for this, then this long-term TTL based K/V pair can be removed using
		  // the dpsRemoveTTL API anytime a premature removal of that K/V pair is needed.
		  _ttl = COUCHBASE_MAX_TTL_VALUE;
	  }

	  // Calculate the time in the future when this K/V pair should be removed.
	  int64_t expiryTimeInSeconds = std::time(0) + _ttl;
	  string errorMsg;
	  // Couchbase will evict this K/V pair if it continues to exist without being released beyond the configured timeInSeconds.
	  // We are ready to either store a new data item or update an existing data item.
	  // This action is performed on the TTL store bucket.
	  struct lcb_create_st cropts;
	  memset(&cropts, 0, sizeof cropts);
	  cropts.version = 3;
	  cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_TTL_STORE_TOKEN)).c_str();
	  lcb_error_t err;
	  lcb_t instance;

	  err = lcb_create(&instance, &cropts);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  dbError.setTTL("Inside putTTL, it failed to create the Couchbase instance. Error: rc=" +
			  string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside putTTL, it failed to create the Couchbase instance. Error: rc=" <<
			  err << ", msg=" << errorMsg << ". " <<
			  DPS_DATA_ITEM_WRITE_ERROR, "CouchbaseDBLayer");
		  return(false);
	  }

	  lcb_connect(instance);
	  lcb_wait(instance);

	  if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  dbError.setTTL("Inside putTTL, it failed to bootstrap the connection to the Couchbase instance. Error: rc=" +
			  string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		  SPLAPPTRC(L_DEBUG,
			  "Inside putTTL, it failed to bootstrap the connection to the Couchbase instance. Error: rc=" <<
			  err << ", msg=" << errorMsg << ". " <<
			  DPS_DATA_ITEM_WRITE_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  lcb_set_store_callback(instance, storage_callback);
	  lcb_store_cmd_t scmd;
	  memset(&scmd, 0, sizeof scmd);
	  const lcb_store_cmd_t *scmdlist = &scmd;
	  scmd.v.v0.key = base64_encoded_data_item_key.c_str();
	  scmd.v.v0.nkey = base64_encoded_data_item_key.length();
	  scmd.v.v0.bytes = base64_encoded_data_item_value.c_str();
	  scmd.v.v0.nbytes = base64_encoded_data_item_value.length();
	  // LCB_SET unconditionally stores the document to the cluster, regardless of whether it already exists or not.
	  scmd.v.v0.operation = LCB_SET;
	  // Upto 30 days of TTL, we can specify in seconds or in epoch time of expiry.
	  // Beyond a 30 day period, we can only specify epoch time in seconds at which it should expire.
	  // In this particular case of storing a TTL value, we will always use the epoch time in seconds at which it should expire.
	  scmd.v.v0.exptime = (uint32_t)expiryTimeInSeconds;
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";
	  err = lcb_store(instance, this, 1, &scmdlist);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  dbError.setTTL("Inside putTTL, it failed to schedule a Couchbase storage operation. Error: rc=" +
			  string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		  SPLAPPTRC(L_DEBUG,
			  "Inside putTTL, it failed to schedule a Couchbase storage operation. Error: rc=" <<
			  err << ", msg=" << errorMsg << ". " <<
			  DPS_DATA_ITEM_WRITE_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  //storage_callback is being invoked during this wait.
	  lcb_wait(instance);

	  // Check the storage operation result.
	  if (lastCouchbaseErrorCode == LCB_SUCCESS) {
		  // We stored/updated the data item in the TTL K/V store.
		  lcb_destroy(instance);
		  return(true);
	  } else {
		  // There is some other error while storing.
		  // Let us exit now.
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		  dbError.setTTL("Inside putTTL, it failed during the storage operation. Error: rc=" +
			  string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		  SPLAPPTRC(L_DEBUG,
			  "Inside putTTL, it failed during the storage operation. Error: rc=" <<
			  lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			  DPS_DATA_ITEM_WRITE_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }
  }

  // This is a lean and mean get operation from a store.
  // It doesn't do any safety checks before getting a data item from a store.
  // If you want to go through that rigor, please use the getSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool CouchbaseDBLayer::get(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside get for store id " << store, "CouchbaseDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In our Couchbase dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);

	bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		false, true, valueData, valueSize, dbError);

	if ((result == false) || (dbError.hasError() == true)) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside get, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
	}

    return(result);
  }

  // This is a special bullet proof version that does several safety checks before getting a data item from a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular get method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool CouchbaseDBLayer::getSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside getSafe for store id " << store, "CouchbaseDBLayer");

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
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CouchbaseDBLayer");
		}

		return(false);
	}

	// In our Couchbase dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);

	bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		false, false, valueData, valueSize, dbError);

	if ((result == false) || (dbError.hasError() == true)) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
	}

    return(result);
  }

  // Get a TTL based data item that is stored in the global area of the Couchbase K/V store.
   bool CouchbaseDBLayer::getTTL(char const * keyData, uint32_t keySize,
                              unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError, bool encodeKey)
   {
		SPLAPPTRC(L_DEBUG, "Inside getTTL.", "CouchbaseDBLayer");

		// Since this is a data item with TTL, it is stored in the global area of Couchbase and not inside a user created store.
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
		string errorMsg;
		struct lcb_create_st cropts;
		memset(&cropts, 0, sizeof cropts);
		cropts.version = 3;
		cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_TTL_STORE_TOKEN)).c_str();
		lcb_error_t err;
		lcb_t instance;

		err = lcb_create(&instance, &cropts);

		if (err != LCB_SUCCESS) {
			  errorMsg = string(lcb_strerror(NULL, err));
			  char errCodeString[50];
			  sprintf(errCodeString, "%d", err);
			  dbError.setTTL("Inside getTTL, it failed to create the Couchbase instance. Error: rc=" +
				  string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getTTL, it failed to create the Couchbase instance. Error: rc=" <<
				  err << ", msg=" << errorMsg << ". " <<
				  DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
			  return(false);
		}

		lcb_connect(instance);
		lcb_wait(instance);

		if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			char errCodeString[50];
			sprintf(errCodeString, "%d", err);
			dbError.setTTL("Inside getTTL, it failed to bootstrap the connection to the Couchbase instance. Error: rc=" +
				string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			SPLAPPTRC(L_DEBUG,
				"Inside getTTL, it failed to bootstrap the connection to the Couchbase instance. Error: rc=" <<
				err << ", msg=" << errorMsg << ". " <<
				DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		lcb_set_get_callback(instance, get_callback);
		lcb_get_cmd_t gcmd;
		memset(&gcmd, 0, sizeof gcmd);
		const lcb_get_cmd_t *gcmdlist = &gcmd;
		gcmd.v.v0.key = base64_encoded_data_item_key.c_str();
		gcmd.v.v0.nkey = base64_encoded_data_item_key.length();
		lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
		lastCouchbaseErrorMsg = "";
		err = lcb_get(instance, this, 1, &gcmdlist);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			char errCodeString[50];
			sprintf(errCodeString, "%d", err);
			dbError.setTTL("Inside getTTL, it failed to schedule a Couchbase get operation. Error: rc=" +
				string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			SPLAPPTRC(L_DEBUG,
				"Inside getTTL, it failed to schedule a Couchbase get operation. Error: rc=" <<
				err << ", msg=" << errorMsg << ". " <<
				DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		// get callback is being invoked during this wait.
		lcb_wait(instance);

		if (lastCouchbaseErrorCode != LCB_SUCCESS) {
			// Requested data item is not there in Couchbase.
			char errCodeString[50];
			sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
			dbError.setTTL("Inside getTTL, it couldn't get the TTL based K/V pair. Error: rc=" +
				string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_DATA_ITEM_READ_ERROR);
			SPLAPPTRC(L_DEBUG,
				"Inside getTTL, it couldn't get the TTL based K/V pair. Error: rc=" <<
				lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
				DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		lcb_destroy(instance);

		// In Couchbase, when we put the K/V pair, we b64_encoded our binary value buffer and
		// stored it as a string. We must now b64_decode it back to binary value buffer.
		b64_decode(lastCouchbaseOperationValue, valueData, valueSize);

		if (valueData == NULL) {
			// We encountered malloc error inside the b64_decode method we called above.
			// Only then, we will have a null value data.
			// Unable to allocate memory to transfer the data item value.
			dbError.setTTL("Unable to allocate memory to copy the data item value read from the TTL store.",
				DPS_GET_DATA_ITEM_MALLOC_ERROR);
			return(false);
		} else {
			// In the b64_decode method, we allocated a memory buffer and directly assigned it to
			// the caller provided pointer (valueData). It is the caller's responsibility to
			// free that buffer when it is no longer needed.
			return(true);
		}
   }

  bool CouchbaseDBLayer::remove(uint64_t store, char const * keyData, uint32_t keySize,
                                PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside remove for store id " << store, "CouchbaseDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to find a store with an id " <<
				storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CouchbaseDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed to get store lock for store id " <<
			storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "CouchbaseDBLayer");
		// User has to retry again to remove the data item from the store.
		return(false);
	}

	// In our Couchbase dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	// We are ready to remove a data item from the Couchbase bucket.
	// This action is performed on the Store Contents bucket that takes the following name.
	// Bucket name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeBucketName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string errorMsg = "";

	struct lcb_create_st cropts;
	memset(&cropts, 0, sizeof cropts);
	cropts.version = 3;
	cropts.v.v3.connstr = (couchbaseServerUrl + storeBucketName).c_str();
	lcb_error_t err;
	lcb_t instance;

	err = lcb_create(&instance, &cropts);

	if (err != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(NULL, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside remove, it failed to create the Couchbase instance for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed to create the Couchbase instance for the store id " <<
			storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_DATA_ITEM_DELETE_ERROR, "CouchbaseDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	lcb_connect(instance);
	lcb_wait(instance);

	if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside remove, it failed to bootstrap the connection to the Couchbase instance for the store id " +
				storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside remove, it failed to bootstrap the connection to the Couchbase instance for the store id " <<
			storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_DATA_ITEM_DELETE_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		releaseStoreLock(storeIdString);
		return(false);
	}

	lcb_set_remove_callback(instance, remove_callback);
	lcb_remove_cmd_t rcmd;
	memset(&rcmd, 0, sizeof rcmd);
	const lcb_remove_cmd_t *rcmdlist = &rcmd;
	rcmd.v.v0.key = base64_encoded_data_item_key.c_str();;
	rcmd.v.v0.nkey = base64_encoded_data_item_key.length();
	lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	lastCouchbaseErrorMsg = "";
	err = lcb_remove(instance, this, 1, &rcmdlist);

	if (err != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Inside remove, it failed to schedule a Couchbase storage operation for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside remove, it failed to schedule a Couchbase storage operation for the store id " <<
			storeIdString <<". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_DATA_ITEM_DELETE_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		releaseStoreLock(storeIdString);
		return(false);
	}

	// remove_callback is being invoked during this wait.
	lcb_wait(instance);

	// Check the removal operation result.
	// We can accept the error code LCB_KEY_ENOENT (i.e. key doesn't exist on the server.)
	if ((lastCouchbaseErrorCode == LCB_SUCCESS) || (lastCouchbaseErrorCode == LCB_KEY_ENOENT)) {
		// All done. An existing data item in the given store has been removed.
		lcb_destroy(instance);
		releaseStoreLock(storeIdString);
		return(true);
	} else {
		// There is some other error while removing.
		// Let us exit now.
		char errCodeString[50];
		sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		dbError.set("Inside remove, it failed during the removal operation for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Inside remove, it failed during the removal operation for the store id " <<
			storeIdString << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			DPS_DATA_ITEM_DELETE_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance);
		releaseStoreLock(storeIdString);
		return(false);
	}
  }

  // Remove a TTL based data item that is stored in the global area of the Couchbase K/V store.
  bool CouchbaseDBLayer::removeTTL(char const * keyData, uint32_t keySize,
                                PersistenceError & dbError, bool encodeKey)
  {
		SPLAPPTRC(L_DEBUG, "Inside removeTTL.", "CouchbaseDBLayer");

		// In our Couchbase dps implementation, data item keys can have space characters.
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

		string errorMsg = "";

		// We are ready to remove a data item from the Couchbase TTL store.
		struct lcb_create_st cropts;
		memset(&cropts, 0, sizeof cropts);
		cropts.version = 3;
		cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_TTL_STORE_TOKEN)).c_str();
		lcb_error_t err;
		lcb_t instance;

		err = lcb_create(&instance, &cropts);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(NULL, err));
			char errCodeString[50];
			sprintf(errCodeString, "%d", err);
			dbError.setTTL("Inside removeTTL, it failed to create the Couchbase instance. Error: rc=" +
				string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_DELETE_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeTTL, it failed to create the Couchbase instance. Error: rc=" <<
				err << ", msg=" << errorMsg << ". " <<
				DPS_DATA_ITEM_DELETE_ERROR, "CouchbaseDBLayer");
			return(false);
		}

		lcb_connect(instance);
		lcb_wait(instance);

		if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			char errCodeString[50];
			sprintf(errCodeString, "%d", err);
			dbError.setTTL("Inside removeTTL, it failed to bootstrap the connection to the Couchbase instance. Error: rc=" +
				string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_DELETE_ERROR);
			SPLAPPTRC(L_DEBUG,
				"Inside removeTTL, it failed to bootstrap the connection to the Couchbase instance. Error: rc=" <<
				err << ", msg=" << errorMsg << ". " <<
				DPS_DATA_ITEM_DELETE_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		lcb_set_remove_callback(instance, remove_callback);
		lcb_remove_cmd_t rcmd;
		memset(&rcmd, 0, sizeof rcmd);
		const lcb_remove_cmd_t *rcmdlist = &rcmd;
		rcmd.v.v0.key = base64_encoded_data_item_key.c_str();;
		rcmd.v.v0.nkey = base64_encoded_data_item_key.length();
		lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
		lastCouchbaseErrorMsg = "";
		err = lcb_remove(instance, this, 1, &rcmdlist);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			char errCodeString[50];
			sprintf(errCodeString, "%d", err);
			dbError.setTTL("Inside removeTTL, it failed to schedule a Couchbase storage operation. Error: rc=" +
				string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_DELETE_ERROR);
			SPLAPPTRC(L_DEBUG,
				"Inside removeTTL, it failed to schedule a Couchbase storage operation. Error: rc=" <<
				err << ", msg=" << errorMsg << ". " <<
				DPS_DATA_ITEM_DELETE_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		// remove_callback is being invoked during this wait.
		lcb_wait(instance);

		// Check the removal operation result.
		// We can accept the error code LCB_KEY_ENOENT (i.e. key doesn't exist on the server.)
		if ((lastCouchbaseErrorCode == LCB_SUCCESS) || (lastCouchbaseErrorCode == LCB_KEY_ENOENT)) {
			// All done. An existing data item in the TTL store has been removed.
			lcb_destroy(instance);
			return(true);
		} else {
			// There is some other error while removing.
			// Let us exit now.
			char errCodeString[50];
			sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
			dbError.setTTL("Inside removeTTL, it failed during the removal operation. Error: rc=" +
				string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_DATA_ITEM_DELETE_ERROR);
			SPLAPPTRC(L_DEBUG,
				"Inside removeTTL, it failed during the removal operation. Error: rc=" <<
				lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
				DPS_DATA_ITEM_DELETE_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}
  }

  bool CouchbaseDBLayer::has(uint64_t store, char const * keyData, uint32_t keySize,
                             PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside has for store id " << store, "CouchbaseDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CouchbaseDBLayer");
		}

		return(false);
	}

	// In our Couchbase dps implementation, data item keys can have space characters.
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
		SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
	}

	return(dataItemAlreadyInCache);
  }

  // Check for the existence of a TTL based data item that is stored in the global area of the Couchbase K/V store.
  bool CouchbaseDBLayer::hasTTL(char const * keyData, uint32_t keySize,
                             PersistenceError & dbError, bool encodeKey)
  {
		SPLAPPTRC(L_DEBUG, "Inside hasTTL.", "CouchbaseDBLayer");

		// Check for the data item existence in the TTL table (not in the user created tables).
		// Since this is a data item with TTL, it is stored in the global area of Couchbase and not inside a user created store.
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

		string errorMsg = "";

		// Get the data from the TTL table (not from the user created tables).
		struct lcb_create_st cropts;
		memset(&cropts, 0, sizeof cropts);
		cropts.version = 3;
		cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_TTL_STORE_TOKEN)).c_str();
		lcb_error_t err;
		lcb_t instance;

		err = lcb_create(&instance, &cropts);

		if (err != LCB_SUCCESS) {
			  errorMsg = string(lcb_strerror(NULL, err));
			  char errCodeString[50];
			  sprintf(errCodeString, "%d", err);
			  dbError.setTTL("Inside hasTTL, it failed to create the Couchbase instance. Error: rc=" +
				  string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside hasTTL, it failed to create the Couchbase instance. Error: rc=" <<
				  err << ", msg=" << errorMsg << ". " <<
				  DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
			  return(false);
		}

		lcb_connect(instance);
		lcb_wait(instance);

		if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			char errCodeString[50];
			sprintf(errCodeString, "%d", err);
			dbError.setTTL("Inside hasTTL, it failed to bootstrap the connection to the Couchbase instance. Error: rc=" +
				string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			SPLAPPTRC(L_DEBUG,
				"Inside hasTTL, it failed to bootstrap the connection to the Couchbase instance. Error: rc=" <<
				err << ", msg=" << errorMsg << ". " <<
				DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		lcb_set_get_callback(instance, get_callback);
		lcb_get_cmd_t gcmd;
		memset(&gcmd, 0, sizeof gcmd);
		const lcb_get_cmd_t *gcmdlist = &gcmd;
		gcmd.v.v0.key = base64_encoded_data_item_key.c_str();
		gcmd.v.v0.nkey = base64_encoded_data_item_key.length();
		lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
		lastCouchbaseErrorMsg = "";
		err = lcb_get(instance, this, 1, &gcmdlist);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			char errCodeString[50];
			sprintf(errCodeString, "%d", err);
			dbError.setTTL("Inside hasTTL, it failed to schedule a Couchbase get operation. Error: rc=" +
				string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			SPLAPPTRC(L_DEBUG,
				"Inside hasTTL, it failed to schedule a Couchbase get operation. Error: rc=" <<
				err << ", msg=" << errorMsg << ". " <<
				DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		// get callback is being invoked during this wait.
		lcb_wait(instance);

		if (lastCouchbaseErrorCode == LCB_SUCCESS) {
			// Key exists.
			lcb_destroy(instance);
			return(true);
		} else if (lastCouchbaseErrorCode == LCB_KEY_ENOENT) {
			// Key doesn't exist.
			lcb_destroy(instance);
			return(false);
		} else {
			// Requested data item is not there in Couchbase.
			char errCodeString[50];
			sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
			dbError.setTTL("Inside hasTTL, it couldn't check for the existence of a TTL based K/V pair. Error: rc=" +
				string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_DATA_ITEM_READ_ERROR);
			SPLAPPTRC(L_DEBUG,
				"Inside hasTTL, it couldn't check for the existence of a TTL based K/V pair. Error: rc=" <<
				lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
				DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}
  }

  void CouchbaseDBLayer::clear(uint64_t store, PersistenceError & dbError) {
     SPLAPPTRC(L_DEBUG, "Inside clear for store id " << store, "CouchbaseDBLayer");

 	ostringstream storeId;
 	storeId << store;
 	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CouchbaseDBLayer");
		}

		return;
	}

 	// Lock the store first.
 	if (acquireStoreLock(storeIdString) == false) {
 		// Unable to acquire the store lock.
 		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside clear, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "CouchbaseDBLayer");
 		// User has to retry again to remove the store.
 		return;
 	}

 	// Get the store name.
 	uint32_t dataItemCnt = 0;
 	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  // Continue further normally if the error is not about getting the store name.
		  if (dbError.getErrorCode() == DPS_GET_STORE_NAME_ERROR) {
			  SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
			  releaseStoreLock(storeIdString);
			  // This is alarming. This will put this store in a bad state. Poor user has to deal with it.
			  return;
		  } else {
			  // Reset the error and continue normally without returning from here.
			  dbError.reset();
		  }
	}

 	// A very fast and quick thing to do is to simply delete the Store Contents bucket and
 	// recreate the meta data entries rather than removing one element at a time.
	// This action is performed on the Store Contents bucket that takes the following name.
	// Bucket name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
 	string storeBucketName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
 	string errorMsg = "";
 	bool result = deleteCouchbaseBucket(storeBucketName, errorMsg);

	if (result == false) {
		// Error in deleting the store bucket.
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString <<
			". (" << errorMsg << ") Unable to delete the store bucket.", "CouchbaseDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

 	// Create a new store contents bucket for this store.
	// Bucket name: 'dps_' + '1_' + 'store id'
 	result = createCouchbaseBucket(storeBucketName, errorMsg, string(COUCHBASE_RAM_BUCKET_QUOTA_IN_MB));

	if (result == false) {
		// Error in create the store bucket.
		// This is bad. This store existed before. In the process of clearing it, we completely lost this store.
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString <<
			". (" << errorMsg << ") Unable to create the store bucket.", "CouchbaseDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

	// Every store contents bucket will always have these three metadata entries:
	// dps_name_of_this_store ==> 'store name'
	// dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	// dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	//
	// Every store contents bucket will have at least three entries in it carrying the actual store name, key spl type name and value spl type name.
	// In addition, inside this store contents bucket, we will house the
	// actual key:value data items that the user wants to keep in this store.
	// Such a store contents bucket is very useful for data item read, write, deletion, enumeration etc.
 	//

	// Let us populate the new store contents bucket with a mandatory element that will carry the name of this store.
	// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
	struct lcb_create_st cropts2;
	memset(&cropts2, 0, sizeof cropts2);
	cropts2.version = 3;
	cropts2.v.v3.connstr = (couchbaseServerUrl + storeBucketName).c_str();
	lcb_t instance2;
	lcb_error_t err;

	err = lcb_create(&instance2, &cropts2);

	if (err != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(NULL, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Critical Error: Inside clear, it failed to create the Couchbase instance for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical Error: Inside clear, it failed to create the Couchbase instance for the store id " <<
			storeIdString + ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_HASH_METADATA1_CREATION_ERROR, "CouchbaseDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

	lcb_connect(instance2);
	lcb_wait(instance2);

	if ((err = lcb_get_bootstrap_status(instance2)) != LCB_SUCCESS) {
		errorMsg = string(lcb_strerror(instance2, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Critical Error: Inside clear, it failed to bootstrap the connection to the Couchbase instance for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Critical Error: Inside clear, it failed to bootstrap the connection to the Couchbase instance for the store id " <<
			storeIdString << " Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_HASH_METADATA1_CREATION_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance2);
		releaseStoreLock(storeIdString);
		return;
	}

	// Let us populate the new store contents bucket with a mandatory element that will carry the name of this store.
	// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
	lcb_set_store_callback(instance2, storage_callback);
	lcb_store_cmd_t scmd;
	memset(&scmd, 0, sizeof scmd);
	const lcb_store_cmd_t *scmdlist = &scmd;
	scmd.v.v0.key = string(COUCHBASE_STORE_ID_TO_STORE_NAME_KEY).c_str();
	scmd.v.v0.nkey = string(COUCHBASE_STORE_ID_TO_STORE_NAME_KEY).length();
	scmd.v.v0.bytes = storeName.c_str();
	scmd.v.v0.nbytes = storeName.length();
	scmd.v.v0.operation = LCB_SET;
	lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	lastCouchbaseErrorMsg = "";
	err = lcb_store(instance2, this, 1, &scmdlist);

	if (err != LCB_SUCCESS) {
		// There was an error in creating Meta Data 1.
		// This is not good at all. This will leave this store in a zombie state in Couchbase.
		errorMsg = string(lcb_strerror(instance2, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Critical Error: Inside clear, it failed to schedule a Couchbase storage operation 1 (Meta Data 1) for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Critical Error: Inside clear, it failed to schedule a Couchbase storage operation 1 (Meta Data 1) for the store id " <<
			storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_HASH_METADATA1_CREATION_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance2);
		releaseStoreLock(storeIdString);
		return;
	}

	//storage_callback is being invoked during this wait.
	lcb_wait(instance2);

	// Check the storage operation result.
	if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		// There was an error in creating Meta Data 1.
		// There is some other error while storing.
		// Let us exit now.
		char errCodeString[50];
		sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		dbError.set("Critical Error: Inside clear, it failed during the storage operation 2 (Meta Data 1) for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Critical Error: Inside clear, it failed during the storage operation 2 (Meta Data 1) for the store id " <<
			storeIdString << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			DPS_STORE_HASH_METADATA1_CREATION_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance2);
		releaseStoreLock(storeIdString);
		return;
	}

    // We are now going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
	// Add the key spl type name metadata.
	memset(&scmd, 0, sizeof scmd);
	scmd.v.v0.key = string(COUCHBASE_SPL_TYPE_NAME_OF_KEY).c_str();
	scmd.v.v0.nkey = string(COUCHBASE_SPL_TYPE_NAME_OF_KEY).length();
	scmd.v.v0.bytes = keySplTypeName.c_str();
	scmd.v.v0.nbytes = keySplTypeName.length();
	scmd.v.v0.operation = LCB_SET;
	lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	lastCouchbaseErrorMsg = "";
	err = lcb_store(instance2, this, 1, &scmdlist);

	if (err != LCB_SUCCESS) {
		// There was an error in creating Meta Data 2.
		errorMsg = string(lcb_strerror(instance2, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Critical Error: Inside clear, it failed to schedule a Couchbase storage operation 3 (Meta Data 2) for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Critical Error: Inside clear, it failed to schedule a Couchbase storage operation 3 (Meta Data 2) for the store id " <<
			storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_HASH_METADATA2_CREATION_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance2);
		releaseStoreLock(storeIdString);
		return;
	}

	//storage_callback is being invoked during this wait.
	lcb_wait(instance2);

	// Check the storage operation result.
	if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		// There was an error in creating Meta Data 2.
		// There is some other error while storing.
		// Let us exit now.
		char errCodeString[50];
		sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		dbError.set("Critical Error: Inside clear, it failed during the storage operation 3 (Meta Data 2) for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Critical Error: Inside clear, it failed during the storage operation 3 (Meta Data 2) for the store id " <<
			storeIdString << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			DPS_STORE_HASH_METADATA2_CREATION_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance2);
		releaseStoreLock(storeIdString);
		return;
	}

	// Add the value spl type name metadata.
	memset(&scmd, 0, sizeof scmd);
	scmd.v.v0.key = string(COUCHBASE_SPL_TYPE_NAME_OF_VALUE).c_str();
	scmd.v.v0.nkey = string(COUCHBASE_SPL_TYPE_NAME_OF_VALUE).length();
	scmd.v.v0.bytes = valueSplTypeName.c_str();
	scmd.v.v0.nbytes = valueSplTypeName.length();
	scmd.v.v0.operation = LCB_SET;
	lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	lastCouchbaseErrorMsg = "";
	err = lcb_store(instance2, this, 1, &scmdlist);

	if (err != LCB_SUCCESS) {
		// There was an error in creating Meta Data 3.
		errorMsg = string(lcb_strerror(instance2, err));
		char errCodeString[50];
		sprintf(errCodeString, "%d", err);
		dbError.set("Critical Error: Inside clear, it failed to schedule a Couchbase storage operation 4 (Meta Data 3) for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Critical Error: Inside clear, it failed to schedule a Couchbase storage operation 4 (Meta Data 3) for the store id " <<
			storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			DPS_STORE_HASH_METADATA3_CREATION_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance2);
		releaseStoreLock(storeIdString);
		return;
	}

	//storage_callback is being invoked during this wait.
	lcb_wait(instance2);

	// Check the storage operation result.
	if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		// There was an error in creating Meta Data 3.
		// There is some other error while storing.
		// Let us exit now.
		char errCodeString[50];
		sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		dbError.set("Critical Error: Inside clear, it failed during the storage operation 4 (Meta Data 3) for the store id " +
			storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG,
			"Critical Error: Inside clear, it failed during the storage operation 4 (Meta Data 3) for the store id " <<
			storeIdString << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			DPS_STORE_HASH_METADATA3_CREATION_ERROR, "CouchbaseDBLayer");
		lcb_destroy(instance2);
		releaseStoreLock(storeIdString);
		return;
	}

 	// If there was an error in the store contents hash recreation, then this store is going to be in a weird state.
	// It is a fatal error. If that happened, something terribly had gone wrong in clearing the contents.
	// User should look at the dbError code and decide about a corrective action.
	lcb_destroy(instance2);
 	releaseStoreLock(storeIdString);
  }
        
  uint64_t CouchbaseDBLayer::size(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside size for store id " << store, "CouchbaseDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

 	uint32_t dataItemCnt = 0;
 	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside size, it failed for store id " << storeIdString <<
			". " << dbError.getErrorCode(), "CouchbaseDBLayer");
		return(0);
	}

	return(dataItemCnt);
  }

  // We allow space characters in the data item keys.
  // Hence, it is required to based64 encode them before storing the
  // key:value data item in Couchbase.
  // (Use boost functions to do this.)
  //
  void CouchbaseDBLayer::base64_encode(std::string const & str, std::string & base64) {
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
  void CouchbaseDBLayer::base64_decode(std::string & base64, std::string & result) {
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
  bool CouchbaseDBLayer::storeIdExistsOrNot(string storeIdString, PersistenceError & dbError) {
	  string storeBucketName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	  string key = string(COUCHBASE_STORE_ID_TO_STORE_NAME_KEY);
	  string errorMsg = "";
	  struct lcb_create_st cropts;
	  memset(&cropts, 0, sizeof cropts);
	  cropts.version = 3;
	  cropts.v.v3.connstr = (couchbaseServerUrl + storeBucketName).c_str();
	  lcb_error_t err;
	  lcb_t instance;

	  err = lcb_create(&instance, &cropts);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  dbError.set("Inside storeIdExistsOrNot, it failed to create the Couchbase instance for the store id " +
			  storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_EXISTENCE_CHECK_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside storeIdExistsOrNot, it failed to create the Couchbase instance for the store id " <<
			  storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DPS_STORE_EXISTENCE_CHECK_ERROR, "CouchbaseDBLayer");
		  return(false);
	  }

	  lcb_connect(instance);
	  lcb_wait(instance);

	  if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  dbError.set("Inside storeIdExistsOrNot, it failed to bootstrap the connection to the Couchbase instance for the store id " +
			  storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_EXISTENCE_CHECK_ERROR);
		  SPLAPPTRC(L_DEBUG,
			  "Inside storeIdExistsOrNot, it failed to bootstrap the connection to the Couchbase instance for the store id " <<
			  storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DPS_STORE_EXISTENCE_CHECK_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  lcb_set_get_callback(instance, get_callback);
	  lcb_get_cmd_t gcmd;
	  memset(&gcmd, 0, sizeof gcmd);
	  const lcb_get_cmd_t *gcmdlist = &gcmd;
	  gcmd.v.v0.key = key.c_str();
	  gcmd.v.v0.nkey = key.length();
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";
	  err = lcb_get(instance, this, 1, &gcmdlist);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  dbError.set("Inside storeIdExistsOrNot, it failed to schedule a Couchbase get operation for the store id " +
			  storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_STORE_EXISTENCE_CHECK_ERROR);
		  SPLAPPTRC(L_DEBUG,
			  "Inside storeIdExistsOrNot, it failed to schedule a Couchbase get operation for the store id " <<
			  storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DPS_STORE_EXISTENCE_CHECK_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  // get callback is being invoked during this wait.
	  lcb_wait(instance);

	  if (lastCouchbaseErrorCode == LCB_SUCCESS) {
		  // This store already exists in our cache.
		  // We will take the hashCode of the encoded store name and use that as the store id.
		  lcb_destroy(instance);
		  return(true);
	  } else {
		  // Requested data item is not there in Couchbase.
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		  dbError.set("Inside storeIdExistsOrNot, it couldn't find the store id " +
			  storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_DATA_ITEM_READ_ERROR);
		  SPLAPPTRC(L_DEBUG,
			  "Inside storeIdExistsOrNot, it couldn't find the store id " <<
			  storeIdString << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			  DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }
  }

  // This method will acquire a lock for a given store.
  bool CouchbaseDBLayer::acquireStoreLock(string const & storeIdString) {
	  int32_t retryCnt = 0;

	  // '4' + 'store id' + 'dps_lock' => 1
	  std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;
	  uint64_t lockId = SPL::Functions::Utility::hashCode(storeLockKey);
	  ostringstream lockIdStr;
	  lockIdStr << lockId;

	  struct lcb_create_st cropts;
	  memset(&cropts, 0, sizeof cropts);
	  cropts.version = 3;
	  cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
	  lcb_error_t err;
	  lcb_t instance;
	  string errorMsg = "";

	  err = lcb_create(&instance, &cropts);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  SPLAPPTRC(L_DEBUG, "Inside acquireStoreLock, it failed to create the Couchbase instance." <<
			  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
		  return(false);
	  }

	  lcb_connect(instance);
	  lcb_wait(instance);

	  if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  SPLAPPTRC(L_DEBUG,
			  "Inside acquireStoreLock, it failed to bootstrap the connection to the Couchbase instance." <<
			  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  // In Couchbase, if a string contains only digits (numerals), then it stores it internally as a
	  // number rather than a string. During that process, it rounds that number and loses the original
	  // number intended by the string. That is bad. Hence, we will base64 encode it so that it will not be
	  // made of all numerals.
	  string base64_encoded_lock_id;
	  base64_encode(lockIdStr.str(), base64_encoded_lock_id);

	  lcb_set_store_callback(instance, storage_callback);
	  lcb_store_cmd_t scmd;
	  memset(&scmd, 0, sizeof scmd);
	  const lcb_store_cmd_t *scmdlist = &scmd;
	  scmd.v.v0.key = storeLockKey.c_str();
	  scmd.v.v0.nkey = storeLockKey.length();
	  scmd.v.v0.bytes = base64_encoded_lock_id.c_str();
	  scmd.v.v0.nbytes = base64_encoded_lock_id.length();
	  // Upto 30 days of TTL, we can specify in seconds or in epoch time of expiry.
	  // Beyond a 30 day period, we can only specify epoch time at which it should expire.
	  scmd.v.v0.exptime = (uint32_t)DPS_AND_DL_GET_LOCK_TTL;
	  // Insert only when the key doesn't exist already.
	  // If it exists, it will return an error.
	  scmd.v.v0.operation = LCB_ADD;
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";

	  // Try to get a lock for this generic entity.
	  while (1) {
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or the Couchbase removes it after the TTL value expires.
		// Make an attempt to create a document for this lock now.
		err = lcb_store(instance, this, 1, &scmdlist);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			SPLAPPTRC(L_DEBUG,
				"Inside acquireStoreLock, it failed to schedule a Couchbase storage operation." <<
				" Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		//storage_callback is being invoked during this wait.
		lcb_wait(instance);

		// Check the storage operation result.
		if (lastCouchbaseErrorCode == LCB_SUCCESS) {
			// We got the lock.
			lcb_destroy(instance);
			return(true);
		}

		if (lastCouchbaseErrorCode != LCB_KEY_EEXISTS) {
			// There is some other error other than the key already exists error.
			// Let us exit now.
			SPLAPPTRC(L_DEBUG,
				"Inside acquireStoreLock, it failed during the storage operation." <<
				" Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		// Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
			lcb_destroy(instance);
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

	  lcb_destroy(instance);
	  return(false);
  }

  void CouchbaseDBLayer::releaseStoreLock(string const & storeIdString) {
	  // '4' + 'store id' + 'dps_lock' => 1
	  std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;
	  struct lcb_create_st cropts;
	  memset(&cropts, 0, sizeof cropts);
	  cropts.version = 3;
	  cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
	  lcb_error_t err;
	  lcb_t instance;
	  string errorMsg = "";

	  err = lcb_create(&instance, &cropts);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  SPLAPPTRC(L_DEBUG, "Inside releaseStoreLock, it failed to create the Couchbase instance." <<
			  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
		  return;
	  }

	  lcb_connect(instance);
	  lcb_wait(instance);

	  if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  SPLAPPTRC(L_DEBUG,
			  "Inside releaseStoreLock, it failed to bootstrap the connection to the Couchbase instance." <<
			  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return;
	  }

	  lcb_set_remove_callback(instance, remove_callback);
	  lcb_remove_cmd_t rcmd;
	  memset(&rcmd, 0, sizeof rcmd);
	  const lcb_remove_cmd_t *rcmdlist = &rcmd;
	  rcmd.v.v0.key = storeLockKey.c_str();;
	  rcmd.v.v0.nkey = storeLockKey.length();
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";
	  err = lcb_remove(instance, this, 1, &rcmdlist);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  SPLAPPTRC(L_DEBUG,
			  "Inside releaseStoreLock, it failed to schedule a Couchbase remove operation." <<
			  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return;
	  }

	  lcb_wait(instance);

	  // We can accept the error code LCB_KEY_ENOENT (i.e. key doesn't exist on the server.)
	  if ((lastCouchbaseErrorCode != LCB_SUCCESS) && (lastCouchbaseErrorCode != LCB_KEY_ENOENT)) {
		  SPLAPPTRC(L_DEBUG,
			  "Inside releaseStoreLock, it failed to remove the general purpose lock. Error: rc=" <<
			  lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg, "CouchbaseDBLayer");
	  }

	  lcb_destroy(instance);
	  return;
  }

  bool CouchbaseDBLayer::readStoreInformation(std::string const & storeIdString, PersistenceError & dbError,
		  uint32_t & dataItemCnt, std::string & storeName, std::string & keySplTypeName, std::string & valueSplTypeName) {
	  // Read the store name, this store's key and value SPL type names,  and get the store size.
	  storeName = "";
	  keySplTypeName = "";
	  valueSplTypeName = "";
	  dataItemCnt = 0;

	  // This action is performed on the Store contents bucket that takes the following name.
	  // 'dps_' + '1_' + 'store id'
	  // It will always have the following three metadata entries:
	  // dps_name_of_this_store ==> 'store name'
	  // dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	  // dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	  // 1) Get the store name.
	  string storeBucketName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	  string key = string(COUCHBASE_STORE_ID_TO_STORE_NAME_KEY);
	  string errorMsg = "";
	  struct lcb_create_st cropts;
	  memset(&cropts, 0, sizeof cropts);
	  cropts.version = 3;
	  cropts.v.v3.connstr = (couchbaseServerUrl + storeBucketName).c_str();
	  lcb_error_t err;
	  lcb_t instance;

	  err = lcb_create(&instance, &cropts);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  // Unable to get the store name for this store's key.
		  dbError.set("Unable to get the store name for StoreId " + storeIdString + ". rc=" +
			  string(errCodeString) + ", msg=" + errorMsg, DPS_GET_STORE_NAME_ERROR);
		  return (false);
	  }

	  lcb_connect(instance);
	  lcb_wait(instance);

	  if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  // Unable to get the store name for this store's key.
		  dbError.set("Unable to get the store name for StoreId " + storeIdString + ". rc=" +
			  string(errCodeString) + ", msg=" + errorMsg, DPS_GET_STORE_NAME_ERROR);
		  lcb_destroy(instance);
		  return(false);
	  }

	  lcb_set_get_callback(instance, get_callback);
	  lcb_get_cmd_t gcmd;
	  memset(&gcmd, 0, sizeof gcmd);
	  const lcb_get_cmd_t *gcmdlist = &gcmd;
	  gcmd.v.v0.key = key.c_str();
	  gcmd.v.v0.nkey = key.length();
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";
	  err = lcb_get(instance, this, 1, &gcmdlist);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  // Unable to get the store name for this store's key.
		  dbError.set("Unable to get the store name for StoreId " + storeIdString + ". rc=" +
			  string(errCodeString) + ", msg=" + errorMsg, DPS_GET_STORE_NAME_ERROR);
		  lcb_destroy(instance);
		  return(false);
	  }

	  // get callback is being invoked during this wait.
	  lcb_wait(instance);

	  if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		  // Requested data item is not there in Couchbase.
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		  // Unable to get the store name for this store's key.
		  dbError.set("Unable to get the store name for StoreId " + storeIdString + ". rc=" +
			  string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_GET_STORE_NAME_ERROR);
		  lcb_destroy(instance);
		  return(false);
	  }

	  storeName = lastCouchbaseOperationValue;

	  // 2) Let us get the spl type name for this store's key.
	  key = string(COUCHBASE_SPL_TYPE_NAME_OF_KEY);
	  memset(&gcmd, 0, sizeof gcmd);
	  gcmd.v.v0.key = key.c_str();
	  gcmd.v.v0.nkey = key.length();
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";
	  err = lcb_get(instance, this, 1, &gcmdlist);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  // Unable to get the store name for this store's key.
		  dbError.set("Unable to get the key spl type name for StoreId " + storeIdString + ". rc=" +
			  string(errCodeString) + ", msg=" + errorMsg, DPS_GET_KEY_SPL_TYPE_NAME_ERROR);
		  lcb_destroy(instance);
		  return(false);
	  }

	  // get callback is being invoked during this wait.
	  lcb_wait(instance);

	  if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		  // Requested data item is not there in Couchbase.
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		  // Unable to get the store name for this store's key.
		  dbError.set("Unable to get the key spl type name for StoreId " + storeIdString + ". rc=" +
			  string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_GET_KEY_SPL_TYPE_NAME_ERROR);
		  lcb_destroy(instance);
		  return(false);
	  }

	  keySplTypeName = lastCouchbaseOperationValue;

	  // 3) Let us get the spl type name for this store's value.
	  key = string(COUCHBASE_SPL_TYPE_NAME_OF_VALUE);
	  memset(&gcmd, 0, sizeof gcmd);
	  gcmd.v.v0.key = key.c_str();
	  gcmd.v.v0.nkey = key.length();
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";
	  err = lcb_get(instance, this, 1, &gcmdlist);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  // Unable to get the store name for this store's key.
		  dbError.set("Unable to get the value spl type name for StoreId " + storeIdString + ". rc=" +
			  string(errCodeString) + ", msg=" + errorMsg, DPS_GET_VALUE_SPL_TYPE_NAME_ERROR);
		  lcb_destroy(instance);
		  return(false);
	  }

	  // get callback is being invoked during this wait.
	  lcb_wait(instance);

	  if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		  // Requested data item is not there in Couchbase.
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		  // Unable to get the store name for this store's key.
		  dbError.set("Unable to get the value spl type name for StoreId " + storeIdString + ". rc=" +
			  string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_GET_VALUE_SPL_TYPE_NAME_ERROR);
		  lcb_destroy(instance);
		  return(false);
	  }

	  valueSplTypeName = lastCouchbaseOperationValue;
	  lcb_destroy(instance);

	  // 4) Let us get the size of the store contents bucket now.
	  // In Couchbase, at times querying the size via their REST API returns zero.
	  // This seems to happen when the size REST API is called immediately after the store creation.
	  // In those case, we will make six attempts with a 5 seconds delay between them.
	  // (This is plain crazy to have such a workaround for a shortcoming in the Couchbase internals.)
	  int64_t bucketSize = 0;

	  for (int32_t loopCnt=1; loopCnt <= 6; loopCnt++) {
		  bool result = getCouchbaseBucketSize(storeBucketName, bucketSize, errorMsg);

		  if (result == false || bucketSize <= 0) {
			  if (result == false) {
				  // Get size REST API failed. Return immediately.
				  dbError.set("Error in obtaining the store size for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_STORE_SIZE_ERROR);
				  return (false);
			  } else if (loopCnt >= 6) {
				  // If we are here, that means the get size REST API worked fine. But, it returned a size of 0 which is not acceptable.
				  // All 6 different attempts to get the bucket size resulted in zero size.
				  // This is not correct. We must have a minimum bucket size of 3 because of the reserved elements.
				  // We are going to give up on this operation now.
				  dbError.set("Wrong value (zero) observed as the store size for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_STORE_SIZE_ERROR);
				  return (false);
			  } else {
				  // We got a size of zero which is not acceptable. We will try to get the size again.
				  // Do this wait. It is a shame that we have to do such a workaround for Couchbase.
				  SPL::Functions::Utility::block(5.0);
				  continue;
			  }
		  } else {
			  // We got a good result for our bucket size.
			  break;
		  }
	  }

	  // Our Store Contents bucket for every store will have a mandatory reserved internal elements (store name, key spl type name, and value spl type name).
	  // Let us not count those three elements in the actual store contents size that the caller wants now.
	  dataItemCnt = (uint32_t)bucketSize;
	  dataItemCnt -= 3;
	  return(true);
  }

  string CouchbaseDBLayer::getStoreName(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CouchbaseDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  // Continue further normally if the error is not about getting the store name.
		  if (dbError.getErrorCode() == DPS_GET_STORE_NAME_ERROR) {
			  SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
			  return("");
		  } else {
			  // Reset the error and continue normally without returning from here.
			  dbError.reset();
		  }
	  }

	  string base64_decoded_storeName;
	  base64_decode(storeName, base64_decoded_storeName);
	  return(base64_decoded_storeName);
  }

  string CouchbaseDBLayer::getSplTypeNameForKey(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CouchbaseDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  // Continue further normally if the error is not about getting the SPL type name for key.
		  if (dbError.getErrorCode() == DPS_GET_KEY_SPL_TYPE_NAME_ERROR) {
			  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
			  return("");
		  } else {
			  // Reset the error and continue normally without returning from here.
			  dbError.reset();
		  }
	  }

	  string base64_decoded_keySplTypeName;
	  base64_decode(keySplTypeName, base64_decoded_keySplTypeName);
	  return(base64_decoded_keySplTypeName);
  }

  string CouchbaseDBLayer::getSplTypeNameForValue(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CouchbaseDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  // Continue further normally if the error is not about getting the SPL type name for value.
		  if (dbError.getErrorCode() == DPS_GET_VALUE_SPL_TYPE_NAME_ERROR) {
			  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
			  return("");
		  } else {
			  // Reset the error and continue normally without returning from here.
			  dbError.reset();
		  }
	  }

	  string base64_decoded_valueSplTypeName;
	  base64_decode(valueSplTypeName, base64_decoded_valueSplTypeName);
	  return(base64_decoded_valueSplTypeName);
  }

  std::string CouchbaseDBLayer::getNoSqlDbProductName(void) {
	  return(string(COUCHBASE_NO_SQL_DB_NAME));
  }

  void CouchbaseDBLayer::getDetailsAboutThisMachine(std::string & machineName, std::string & osVersion, std::string & cpuArchitecture) {
	  machineName = nameOfThisMachine;
	  osVersion = osVersionOfThisMachine;
	  cpuArchitecture = cpuTypeOfThisMachine;
  }

  bool CouchbaseDBLayer::runDataStoreCommand(std::string const & cmd, PersistenceError & dbError) {
		// This API can only be supported in NoSQL data stores such as Redis, Cassandra etc.
		// Couchbase doesn't have a way to do this.
		dbError.set("From Couchbase data store: This API to run native data store commands is not supported in Couchbase.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Couchbase data store: This API to run native data store commands is not supported in Couchbase. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CouchbaseDBLayer");
		return(false);
  }

  bool CouchbaseDBLayer::runDataStoreCommand(uint32_t const & cmdType, std::string const & httpVerb,
		std::string const & baseUrl, std::string const & apiEndpoint, std::string const & queryParams,
		std::string const & jsonRequest, std::string & jsonResponse, PersistenceError & dbError) {
		// This API can only be supported in NoSQL data stores such as Redis, Cassandra etc.
		// Couchbase doesn't have a way to do this.
		dbError.set("From Couchbase data store: This API to run native data store commands is not supported in Couchbase.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Couchbase data store: This API to run native data store commands is not supported in Couchbase. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CouchbaseDBLayer");
		return(false);
  }

  bool CouchbaseDBLayer::runDataStoreCommand(std::vector<std::string> const & cmdList, std::string & resultValue, PersistenceError & dbError) {
		// This API can only be supported in Redis.
		// Couchbase doesn't have a way to do this.
		dbError.set("From Couchbase data store: This API to run native data store commands is not supported in Couchbase.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Couchbase data store: This API to run native data store commands is not supported in Couchbase. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CouchbaseDBLayer");
		return(false);
  }

  // This method will get the data item from the store for a given key.
  // Caller of this method can also ask us just to find if a data item
  // exists in the store without the extra work of fetching and returning the data item value.
  bool CouchbaseDBLayer::getDataItemFromStore(std::string const & storeIdString,
		  std::string const & keyDataString, bool const & checkOnlyForDataItemExistence,
		  bool const & skipDataItemExistenceCheck, unsigned char * & valueData,
		  uint32_t & valueSize, PersistenceError & dbError) {
		// Let us get this data item from the cache as it is.
		// Since there could be multiple data writers, we are going to get whatever is there now.
		// It is always possible that the value for the requested item can change right after
		// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
		// This action is performed on the Store Contents bucket that takes the following name.
		// Bucket name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
		string storeBucketName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
		string errorMsg = "";

		struct lcb_create_st cropts;
		memset(&cropts, 0, sizeof cropts);
		cropts.version = 3;
		cropts.v.v3.connstr = (couchbaseServerUrl + storeBucketName).c_str();
		lcb_error_t err;
		lcb_t instance;

		err = lcb_create(&instance, &cropts);

		if (err != LCB_SUCCESS) {
			  errorMsg = string(lcb_strerror(NULL, err));
			  char errCodeString[50];
			  sprintf(errCodeString, "%d", err);
			  dbError.set("Inside getDataItemFromStore, it failed to create the Couchbase instance for the store id " +
				  storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getDataItemFromStore, it failed to create the Couchbase instance for the store id " <<
				  storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
				  DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
			  return(false);
		}

		lcb_connect(instance);
		lcb_wait(instance);

		if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			char errCodeString[50];
			sprintf(errCodeString, "%d", err);
			dbError.set("Inside getDataItemFromStore, it failed to bootstrap the connection to the Couchbase instance for the store id " +
					storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			SPLAPPTRC(L_DEBUG,
				"Inside getDataItemFromStore, it failed to bootstrap the connection to the Couchbase instance for the store id " <<
				storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
				DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		lcb_set_get_callback(instance, get_callback);
		lcb_get_cmd_t gcmd;
		memset(&gcmd, 0, sizeof gcmd);
		const lcb_get_cmd_t *gcmdlist = &gcmd;
		gcmd.v.v0.key = keyDataString.c_str();
		gcmd.v.v0.nkey = keyDataString.length();
		lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
		lastCouchbaseErrorMsg = "";
		err = lcb_get(instance, this, 1, &gcmdlist);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			char errCodeString[50];
			sprintf(errCodeString, "%d", err);
			dbError.set("Inside getDataItemFromStore, it failed to schedule a Couchbase get operation for the store id " +
				storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			SPLAPPTRC(L_DEBUG,
				"Inside getDataItemFromStore, it failed to schedule a Couchbase get operation for the store id " <<
				storeIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
				DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		// get callback is being invoked during this wait.
		lcb_wait(instance);

		if (lastCouchbaseErrorCode != LCB_SUCCESS) {
			// If the caller only wanted us to check for the data item existence, we can exit now.
			if ((checkOnlyForDataItemExistence == true) && (lastCouchbaseErrorCode == LCB_KEY_ENOENT)) {
				// Specified data item doesn't exist in Couchbase.
				lcb_destroy(instance);
				return(false);
			}

			// Requested data item is not there in Couchbase.
			char errCodeString[50];
			sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
			dbError.set("Inside getDataItemFromStore, it couldn't get the K/V pair from the store id " +
				storeIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DPS_DATA_ITEM_READ_ERROR);
			SPLAPPTRC(L_DEBUG,
				"Inside getDataItemFromStore, it couldn't get the K/V pair from the store id " <<
				storeIdString << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
				DPS_DATA_ITEM_READ_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		lcb_destroy(instance);

		// If the caller only wanted us to check for the data item existence, we can exit now.
		if (checkOnlyForDataItemExistence == true) {
			// Specified data item exists in Couchbase.
			return(true);
		} else {
			// Caller wants us to fetch and return the data item value.
			// In Couchbase, when we put the K/V pair, we b64_encoded our binary value buffer and
			// stored it as a string. We must now b64_decode it back to binary value buffer.
			b64_decode(lastCouchbaseOperationValue, valueData, valueSize);

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

  CouchbaseDBLayerIterator * CouchbaseDBLayer::newIterator(uint64_t store, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside newIterator for store id " << store, "CouchbaseDBLayer");

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  // Ensure that a store exists for the given store id.
	  if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CouchbaseDBLayer");
		  }

		  return(NULL);
	  }

	  // Get the general information about this store.
	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayer");
		  return(NULL);
	  }

	  // It is a valid store. Create a new iterator and return it to the caller.
	  CouchbaseDBLayerIterator *iter = new CouchbaseDBLayerIterator();
	  iter->store = store;
	  base64_decode(storeName, iter->storeName);
	  iter->hasData = true;
	  // Give this iterator access to our CouchbaseDBLayer object.
	  iter->couchbaseDBLayerPtr = this;
	  iter->sizeOfDataItemKeysVector = 0;
	  iter->currentIndex = 0;
	  return(iter);
  }

  void CouchbaseDBLayer::deleteIterator(uint64_t store, Iterator * iter, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside deleteIterator for store id " << store, "CouchbaseDBLayer");

	  if (iter == NULL) {
		  return;
	  }

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  CouchbaseDBLayerIterator *myIter = static_cast<CouchbaseDBLayerIterator *>(iter);

	  // Let us ensure that the user wants to delete an iterator that really belongs to the store passed to us.
	  // This will handle user's coding errors where a wrong combination of store id and iterator is passed to us for deletion.
	  if (myIter->store != store) {
		  // User sent us a wrong combination of a store and an iterator.
		  dbError.set("A wrong iterator has been sent for deletion. This iterator doesn't belong to the StoreId " +
		  				storeIdString + ".", DPS_STORE_ITERATION_DELETION_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside deleteIterator, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_DELETION_ERROR, "CouchbaseDBLayer");
		  return;
	  } else {
		  delete iter;
	  }
  }

  // This method will acquire a lock for any given generic/arbitrary identifier passed as a string..
  // This is typically used inside the createStore, createOrGetStore, createOrGetLock methods to
  // provide thread safety. There are other lock acquisition/release methods once someone has a valid store id or lock id.
  bool CouchbaseDBLayer::acquireGeneralPurposeLock(string const & entityName) {
	  int32_t retryCnt = 0;

	  // '501' + 'entity name' + 'generic_lock' => 1
	  std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
	  uint64_t lockId = SPL::Functions::Utility::hashCode(genericLockKey);
	  ostringstream lockIdStr;
	  lockIdStr << lockId;

	  struct lcb_create_st cropts;
	  memset(&cropts, 0, sizeof cropts);
	  cropts.version = 3;
	  cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
	  lcb_error_t err;
	  lcb_t instance;
	  string errorMsg = "";

	  err = lcb_create(&instance, &cropts);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  SPLAPPTRC(L_DEBUG, "Inside acquireGeneralPurposeLock, it failed to create the Couchbase instance." <<
			  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
		  return(false);
	  }

	  lcb_connect(instance);
	  lcb_wait(instance);

	  if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  SPLAPPTRC(L_DEBUG,
			  "Inside acquireGeneralPurposeLock, it failed to bootstrap the connection to the Couchbase instance." <<
			  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  // In Couchbase, if a string contains only digits (numerals), then it stores it internally as a
	  // number rather than a string. During that process, it rounds that number and loses the original
	  // number intended by the string. That is bad. Hence, we will base64 encode it so that it will not be
	  // made of all numerals.
	  string base64_encoded_lock_id;
	  base64_encode(lockIdStr.str(), base64_encoded_lock_id);

	  lcb_set_store_callback(instance, storage_callback);
	  lcb_store_cmd_t scmd;
	  memset(&scmd, 0, sizeof scmd);
	  const lcb_store_cmd_t *scmdlist = &scmd;
	  scmd.v.v0.key = genericLockKey.c_str();
	  scmd.v.v0.nkey = genericLockKey.length();
	  scmd.v.v0.bytes = base64_encoded_lock_id.c_str();
	  scmd.v.v0.nbytes = base64_encoded_lock_id.length();
	  // Upto 30 days of TTL, we can specify in seconds or in epoch time of expiry.
	  // Beyond a 30 day period, we can only specify epoch time at which it should expire.
	  scmd.v.v0.exptime = (uint32_t)DPS_AND_DL_GET_LOCK_TTL;
	  // Insert only when the key doesn't exist already.
	  // If it exists, it will return an error.
	  scmd.v.v0.operation = LCB_ADD;
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";

	  // Try to get a lock for this generic entity.
	  while (1) {
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or the Couchbase removes it after the TTL value expires.
		// Make an attempt to create a document for this lock now.
		err = lcb_store(instance, this, 1, &scmdlist);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			SPLAPPTRC(L_DEBUG,
				"Inside acquireGeneralPurposeLock, it failed to schedule a Couchbase storage operation." <<
				" Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		//storage_callback is being invoked during this wait.
		lcb_wait(instance);

		// Check the storage operation result.
		if (lastCouchbaseErrorCode == LCB_SUCCESS) {
			// We got the lock.
			lcb_destroy(instance);
			return(true);
		}

		if (lastCouchbaseErrorCode != LCB_KEY_EEXISTS) {
			// There is some other error other than the key already exists error.
			// Let us exit now.
			SPLAPPTRC(L_DEBUG,
				"Inside acquireGeneralPurposeLock, it failed during the storage operation." <<
				" Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg, "CouchbaseDBLayer");
			lcb_destroy(instance);
			return(false);
		}

		// Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
			lcb_destroy(instance);
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

	  lcb_destroy(instance);
	  return(false);
  }

  void CouchbaseDBLayer::releaseGeneralPurposeLock(string const & entityName) {
	  // '501' + 'entity name' + 'generic_lock' => 1
	  std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
	  struct lcb_create_st cropts;
	  memset(&cropts, 0, sizeof cropts);
	  cropts.version = 3;
	  cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
	  lcb_error_t err;
	  lcb_t instance;
	  string errorMsg = "";

	  err = lcb_create(&instance, &cropts);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  SPLAPPTRC(L_DEBUG, "Inside releaseGeneralPurposeLock, it failed to create the Couchbase instance." <<
			  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
		  return;
	  }

	  lcb_connect(instance);
	  lcb_wait(instance);

	  if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  SPLAPPTRC(L_DEBUG,
			  "Inside releaseGeneralPurposeLock, it failed to bootstrap the connection to the Couchbase instance." <<
			  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return;
	  }

	  lcb_set_remove_callback(instance, remove_callback);
	  lcb_remove_cmd_t rcmd;
	  memset(&rcmd, 0, sizeof rcmd);
	  const lcb_remove_cmd_t *rcmdlist = &rcmd;
	  rcmd.v.v0.key = genericLockKey.c_str();;
	  rcmd.v.v0.nkey = genericLockKey.length();
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";
	  err = lcb_remove(instance, this, 1, &rcmdlist);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  SPLAPPTRC(L_DEBUG,
			  "Inside releaseGeneralPurposeLock, it failed to schedule a Couchbase remove operation." <<
			  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return;
	  }

	  lcb_wait(instance);

	  // We can accept the error code LCB_KEY_ENOENT (i.e. key doesn't exist on the server.)
	  if ((lastCouchbaseErrorCode != LCB_SUCCESS) && (lastCouchbaseErrorCode != LCB_KEY_ENOENT)) {
		  SPLAPPTRC(L_DEBUG,
			  "Inside releaseGeneralPurposeLock, it failed to remove the general purpose lock. Error: rc=" <<
			  lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg, "CouchbaseDBLayer");
	  }

	  lcb_destroy(instance);
	  return;
  }

    // Senthil added this on Apr/06/2022.
    // This method will get multiple keys from the given store and
    // populate them in the caller provided list (vector).
    // Be aware of the time it can take to fetch multiple keys in a store
    // that has several tens of thousands of keys. In such cases, the caller
    // has to maintain calm until we return back from here.
   void CouchbaseDBLayer::getKeys(uint64_t store, std::vector<unsigned char *> & keysBuffer, std::vector<uint32_t> & keysSize, int32_t keyStartPosition, int32_t numberOfKeysNeeded, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside getKeys for store id " << store, "CouchbaseDBLayer");

       // Not implemented at this time. Simply return.
       return;
    } // End of getKeys method.

    // Senthil added this on Apr/18/2022.
    // This method will get the values for a given list of keys from the given store without
    // performing any checks for the existence of store, key etc. This is a 
    // faster version of the get method above. 
    void CouchbaseDBLayer::getValues(uint64_t store, std::vector<char *> const & keyData, std::vector<uint32_t> const & keySize, std::vector<unsigned char *> & valueData, std::vector<uint32_t> & valueSize, PersistenceError & dbError) {
       SPLAPPTRC(L_DEBUG, "Inside getValues for store id " << store, "CouchbaseDBLayer");

       // Not implemented at this time. Simply return.
       return;
    }

    // Senthil added this on Apr/21/2022.
    // This method will put the given K/V pairs in a given store. 
    void CouchbaseDBLayer::putKVPairs(uint64_t store, std::vector<char *> const & keyData, std::vector<uint32_t> const & keySize, std::vector<unsigned char *> const & valueData, std::vector<uint32_t> const & valueSize, PersistenceError & dbError) {
       SPLAPPTRC(L_DEBUG, "Inside putKVPairs for store id " << store, "CouchbaseDBLayer");

       // Not implemented at this time. Simply return.
       return;
    }

    // Senthil added this on Apr/27/2022.
    // This method checks for the existence of a given list of keys in a given store and returns the true or false results.
    void CouchbaseDBLayer::hasKeys(uint64_t store, std::vector<char *> const & keyData, std::vector<uint32_t> const & keySize, std::vector<bool> & results, PersistenceError & dbError) {
       SPLAPPTRC(L_DEBUG, "Inside hasKeys for store id " << store, "CouchbaseDBLayer");

       // Not implemented at this time. Simply return.
       return;
    }

    // Senthil added this on Apr/28/2022.
    // This method removes a given list of keys from a given store.
    void CouchbaseDBLayer::removeKeys(uint64_t store, std::vector<char *> const & keyData, std::vector<uint32_t> const & keySize, int32_t & totalKeysRemoved, PersistenceError & dbError) {
       SPLAPPTRC(L_DEBUG, "Inside removeKeys for store id " << store, "CouchbaseDBLayer");

       // Not implemented at this time. Simply return.
       return;
    }


  CouchbaseDBLayerIterator::CouchbaseDBLayerIterator() {

  }

  CouchbaseDBLayerIterator::~CouchbaseDBLayerIterator() {

  }

  bool CouchbaseDBLayerIterator::getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
  	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside getNext for store id " << store, "CouchbaseDBLayerIterator");

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
	  if (this->couchbaseDBLayerPtr->storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayerIterator");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CouchbaseDBLayerIterator");
		  }

		  return(false);
	  }

	  // In Couchbase, things are already very slow.
	  // We don't want the store size operation to slow us down more.
	  // Hence, I commented out this block of code to check for the non-zero size store.
	  /*
	  // Ensure that this store is not empty at this time. (Size check on every iteration is a performance nightmare. Optimize it later.)
	  if (this->couchbaseDBLayerPtr->size(store, dbError) <= 0) {
		  dbError.set("Store is empty for the StoreId " + storeIdString + ".", DPS_STORE_EMPTY_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed with an empty store whose id is " << storeIdString << ". " << DPS_STORE_EMPTY_ERROR, "CouchbaseDBLayerIterator");
		  return(false);
	  }
	  */

	  if (this->sizeOfDataItemKeysVector <= 0) {
		  // This is the first time we are coming inside getNext for store iteration.
		  // Let us get the available data item keys from this store.
		  this->dataItemKeys.clear();
		  string errorMsg = "";
		  string storeBucketName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
		  bool couchbaseResult = this->couchbaseDBLayerPtr->getAllKeysInCouchbaseBucket(storeBucketName, this->dataItemKeys, errorMsg);

		  if (couchbaseResult == false) {
			  // Unable to get data item keys from the store.
			  dbError.set("Unable to get data item keys for the StoreId " + storeIdString +
					  ". " + errorMsg, DPS_GET_STORE_DATA_ITEM_KEYS_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to get data item keys for store id " << storeIdString <<
                 ". " << errorMsg << ". " << DPS_GET_STORE_DATA_ITEM_KEYS_ERROR, "CouchbaseDBLayerIterator");
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
	  // data_item_key was obtained straight from the store contents bucket, where it is
	  // already in the base64 encoded format.
	  bool result = this->couchbaseDBLayerPtr->getDataItemFromStore(storeIdString, data_item_key,
		false, false, valueData, valueSize, dbError);

	  if (result == false) {
		  // Some error has occurred in reading the data item value.
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to get data item from store id " << storeIdString << ". " << dbError.getErrorCode(), "CouchbaseDBLayerIterator");
		  // We will disable any future action for this store using the current iterator.
		  this->hasData = false;
		  return(false);
	  }

	  // We are almost done once we take care of arranging to return the key name and key size.
	  // In order to support spaces in data item keys, we base64 encoded them before storing it in Couchbase.
	  // Let us base64 decode it now to get the original data item key.
	  string base64_decoded_data_item_key;
	  this->couchbaseDBLayerPtr->base64_decode(data_item_key, base64_decoded_data_item_key);
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
		     storeIdString << ". " << DPS_STORE_ITERATION_MALLOC_ERROR, "CouchbaseDBLayerIterator");
		  return(false);
	  }

	  // Copy the raw key data into the allocated buffer.
	  memcpy(keyData, data_item_key.data(), keySize);
	  // We are done. We expect the caller to free the keyData and valueData buffers.
	  return(true);
  }

// =======================================================================================================
// Beyond this point, we have code that deals with the distributed locks that a SPL developer can
// create, remove, acquire and release.
// =======================================================================================================
  uint64_t CouchbaseDBLayer::createOrGetLock(std::string const & name, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock with a name " << name, "CouchbaseDBLayer");
		string base64_encoded_name;
		base64_encode(name, base64_encoded_name);

	 	// Get a general purpose lock so that only one thread can
		// enter inside of this method at any given time with the same lock name.
	 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
	 		// Unable to acquire the general purpose lock.
	 		lkError.set("Unable to get a generic lock for creating a lock with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
	 		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to get a generic lock while creating a store lock named " <<
	 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "CouchbaseDBLayer");
	 		// User has to retry again to create this distributed lock.
	 		return (0);
	 	}

		// 1) In our Couchbase dps implementation, data item keys can have space characters.
		// Inside Couchbase, all our lock names will have a mapping type indicator of
		// "5" at the beginning followed by the actual lock name.
		// '5' + 'lock name' => lockId
		std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
		uint64_t lockId = SPL::Functions::Utility::hashCode(lockNameKey);
		ostringstream lockIdStr;
		lockIdStr << lockId;

		struct lcb_create_st cropts;
		memset(&cropts, 0, sizeof cropts);
		cropts.version = 3;
		cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
		lcb_error_t err;
		lcb_t instance;
		string errorMsg = "";

		err = lcb_create(&instance, &cropts);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(NULL, err));
			lkError.set("Unable to create or get the lockId for the lockName " + name +
				". Error in creating the Couchbase instance. " + errorMsg, DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to create or get the lockId for the lockName " <<
				name << ". Error in creating the Couchbase instance. " <<
				errorMsg << ". " << DL_GET_LOCK_ID_ERROR, "CouchbaseDBLayer");
			releaseGeneralPurposeLock(base64_encoded_name);
			return (0);
		}

		lcb_connect(instance);
		lcb_wait(instance);

		if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			lkError.set("Unable to create or get the lockId for the lockName " + name +
				". Error in bootstrapping the connection to the Couchbase instance. " + errorMsg, DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to create or get the lockId for the lockName " <<
				name << ". Error in bootstrapping the connection to the Couchbase instance. " <<
				errorMsg << ". " << DL_GET_LOCK_ID_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			releaseGeneralPurposeLock(base64_encoded_name);
			return(0);
		}

		// In Couchbase, if a string contains only digits (numerals), then it stores it internally as a
		// number rather than a string. During that process, it rounds that number and loses the original
		// number intended by the string. That is bad. Hence, we will base64 encode it so that it will not be
		// made of all numerals.
		string base64_encoded_lock_id;
		base64_encode(lockIdStr.str(), base64_encoded_lock_id);

		lcb_set_store_callback(instance, storage_callback);
		lcb_store_cmd_t scmd;
		memset(&scmd, 0, sizeof scmd);
		const lcb_store_cmd_t *scmdlist = &scmd;
		scmd.v.v0.key = lockNameKey.c_str();
		scmd.v.v0.nkey = lockNameKey.length();
		scmd.v.v0.bytes = base64_encoded_lock_id.c_str();
		scmd.v.v0.nbytes = base64_encoded_lock_id.length();
		// Insert only when the key doesn't exist already.
		// If it exists, it will return an error.
		scmd.v.v0.operation = LCB_ADD;
		lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
		lastCouchbaseErrorMsg = "";
		// Make an attempt to create a document for this lock now.
		err = lcb_store(instance, this, 1, &scmdlist);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			lkError.set("Unable to create or get the lockId for the lockName " + name +
				". Error in scheduling a Couchbase storage operation. " + errorMsg, DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to create or get the lockId for the lockName " <<
				name << ". Error in scheduling a Couchbase storage operation. " <<
				errorMsg << ". " << DL_GET_LOCK_ID_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			releaseGeneralPurposeLock(base64_encoded_name);
			return(0);
		}

		//storage_callback is being invoked during this wait.
		lcb_wait(instance);

		// Check the storage operation result.
		if (lastCouchbaseErrorCode == LCB_KEY_EEXISTS) {
			// This lock already exists. We can simply return the lockId now.
			lcb_destroy(instance);
			releaseGeneralPurposeLock(base64_encoded_name);
			return(lockId);
		} else if (lastCouchbaseErrorCode != LCB_SUCCESS)  {
			// There is some other error other than the key already exists error.
			// Let us exit now.
			lkError.set("Unable to create or get the lockId for the lockName " + name +
				". Error in creating a user defined lock entry. " + lastCouchbaseErrorMsg, DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to create or get the lockId for the lockName " <<
				name << ". Error in creating a user defined lock entry. " <<
				lastCouchbaseErrorMsg << ". " << DL_GET_LOCK_ID_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			releaseGeneralPurposeLock(base64_encoded_name);
			return (0);
		}

		// If we are here that means the Couchbase document entry was created by the previous call.
		// We can go ahead and create the lock info entry now.
		// 2) Create the Lock Info
		//    '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdStr.str();  // LockId becomes the new key now.
		// In all other NoSQL stores, we store the base64 encoded name in the lock info.
		// However, I noticed in Couchbase mixing a base64 encoded string along with a
		// plain string gets back garbage characters when we read it back.
		// Hence, I decided to store the plain lock name in the lock info string.
		string lockInfoValue = string("0_0_0_") + name;

		memset(&scmd, 0, sizeof scmd);
		scmd.v.v0.key = lockInfoKey.c_str();
		scmd.v.v0.nkey = lockInfoKey.length();
		scmd.v.v0.bytes = lockInfoValue.c_str();
		scmd.v.v0.nbytes = lockInfoValue.length();
		// Insert only when the key doesn't exist already.
		// If it exists, it will return an error.
		scmd.v.v0.operation = LCB_ADD;
		lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
		lastCouchbaseErrorMsg = "";
		// Make an attempt to create a document for this lock now.
		err = lcb_store(instance, this, 1, &scmdlist);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			lkError.set("Unable to create 'LockId:LockInfo' in the cache for a lock named " + name +
				". Error in scheduling a Couchbase storage operation. " + errorMsg, DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to create 'LockId:LockInfo' in the cache for a lock named " <<
				name << ". Error in scheduling a Couchbase storage operation. " <<
				errorMsg << ". " << DL_GET_LOCK_ID_ERROR, "CouchbaseDBLayer");
			// Delete the previous root entry we inserted.
			lcb_set_remove_callback(instance, remove_callback);
			lcb_remove_cmd_t rcmd;
			memset(&rcmd, 0, sizeof rcmd);
			const lcb_remove_cmd_t *rcmdlist = &rcmd;
			rcmd.v.v0.key = lockNameKey.c_str();;
			rcmd.v.v0.nkey = lockNameKey.length();
			err = lcb_remove(instance, this, 1, &rcmdlist);
			lcb_wait(instance);
			lcb_destroy(instance);
			releaseGeneralPurposeLock(base64_encoded_name);
			return(0);
		}

		//storage_callback is being invoked during this wait.
		lcb_wait(instance);

		// Check the storage operation result.
		if (lastCouchbaseErrorCode == LCB_KEY_EEXISTS || lastCouchbaseErrorCode == LCB_SUCCESS) {
			// This lock info entry either exists already or we created it newly.
			lcb_destroy(instance);
			releaseGeneralPurposeLock(base64_encoded_name);
			return(lockId);
		} else {
			// There is some other error other than the key already exists error.
			// Let us exit now.
			lkError.set("Unable to create 'LockId:LockInfo' in the cache for a lock named " + name +
				". Error in creating a user defined lock info entry. " + lastCouchbaseErrorMsg, DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to create the lock info for the lockName " <<
				name << ". Error in creating a user defined lock info entry. " <<
				lastCouchbaseErrorMsg << ". " << DL_GET_LOCK_ID_ERROR, "CouchbaseDBLayer");
			// Delete the previous root entry we inserted.
			lcb_set_remove_callback(instance, remove_callback);
			lcb_remove_cmd_t rcmd;
			memset(&rcmd, 0, sizeof rcmd);
			const lcb_remove_cmd_t *rcmdlist = &rcmd;
			rcmd.v.v0.key = lockNameKey.c_str();;
			rcmd.v.v0.nkey = lockNameKey.length();
			err = lcb_remove(instance, this, 1, &rcmdlist);
			lcb_wait(instance);
			lcb_destroy(instance);
			releaseGeneralPurposeLock(base64_encoded_name);
			return (0);
		}
  }

  bool CouchbaseDBLayer::removeLock(uint64_t lock, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside removeLock for lock id " << lock, "CouchbaseDBLayer");

		ostringstream lockId;
		lockId << lock;
		string lockIdString = lockId.str();

		// If the lock doesn't exist, there is nothing to remove. Don't allow this caller inside this method.
		if(lockIdExistsOrNot(lockIdString, lkError) == false) {
			if (lkError.hasError() == true) {
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "CouchbaseDBLayer");
			} else {
				lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to find the lock with an id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "CouchbaseDBLayer");
			}

			return(false);
		}

		// Before removing the lock entirely, ensure that the lock is not currently being used by anyone else.
		if (acquireLock(lock, 25, 40, lkError) == false) {
			// Unable to acquire the distributed lock.
			lkError.set("Unable to get a distributed lock for the LockId " + lockIdString + ".", DL_GET_DISTRIBUTED_LOCK_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to get a distributed lock for the lock id " << lockIdString << ". " << DL_GET_DISTRIBUTED_LOCK_ERROR, "CouchbaseDBLayer");
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
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "CouchbaseDBLayer");
			releaseLock(lock, lkError);
			// This is alarming. This will put this lock in a bad state. Poor user has to deal with it.
			return(false);
		}

		// Since we got back the lock name for the given lock id, let us remove the lock entirely.
		// '5' + 'lock name' => lockId
		// In all other No SQL stores, we store the base64 encoded name in the lock info.
		// However, I noticed in Couchbase mixing a base64 encoded string along with a
		// plain string gets back garbage characters when we read it back.
		// Hence, I stored the plain lock name in the lock info string.
		// Because of that, it is necessary to base64 encode that now to form the lockNameKey.
		string base64_encoded_name;
		base64_encode(lockName, base64_encoded_name);
		std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;

		struct lcb_create_st cropts;
		memset(&cropts, 0, sizeof cropts);
		cropts.version = 3;
		cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
		lcb_error_t err;
		lcb_t instance;
		string errorMsg = "";

		err = lcb_create(&instance, &cropts);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(NULL, err));
			lkError.set("Unable to remove the lock for the lock Id " + lockIdString +
				". Error in creating the Couchbase instance. " + errorMsg, DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to remove the lock for the lock id " <<
				lockIdString << ". Error in creating the Couchbase instance. " <<
				errorMsg << ". " << DL_LOCK_REMOVAL_ERROR, "CouchbaseDBLayer");
			releaseLock(lock, lkError);
			return (false);
		}

		lcb_connect(instance);
		lcb_wait(instance);

		if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			lkError.set("Unable to remove the lock for the lock Id " + lockIdString +
				". Error in bootstrapping the connection to the Couchbase instance. " + errorMsg, DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to remove the lock for the lock id " <<
				lockIdString << ". Error in bootstrapping the connection to the Couchbase instance. " <<
				errorMsg << ". " << DL_LOCK_REMOVAL_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			releaseLock(lock, lkError);
			return(false);
		}

		lcb_set_remove_callback(instance, remove_callback);
		lcb_remove_cmd_t rcmd;
		memset(&rcmd, 0, sizeof rcmd);
		const lcb_remove_cmd_t *rcmdlist = &rcmd;
		rcmd.v.v0.key = lockNameKey.c_str();
		rcmd.v.v0.nkey = lockNameKey.length();
		lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
		lastCouchbaseErrorMsg = "";
		// Make an attempt to delete a document for this lock now.
		err = lcb_remove(instance, this, 1, &rcmdlist);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			lkError.set("Unable to remove the lock for the lock Id " + lockIdString +
				". Error in scheduling a Couchbase storage operation. " + errorMsg, DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to remove the lock for the lock id " <<
				lockIdString << ". Error in scheduling a Couchbase storage operation. " <<
				errorMsg << ". " << DL_LOCK_REMOVAL_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			releaseLock(lock, lkError);
			return(false);
		}

		// remove_callback is being invoked during this wait.
		lcb_wait(instance);

		// We can accept the error code LCB_KEY_ENOENT (i.e. key doesn't exist on the server.)
		if ((lastCouchbaseErrorCode != LCB_SUCCESS) && (lastCouchbaseErrorCode != LCB_KEY_ENOENT)) {
			lkError.set("Unable to remove the lock for the lock Id " + lockIdString +
				". Error=" + lastCouchbaseErrorMsg, DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to remove the lock for the lock Id " <<
				lockIdString << ". Error=" << lastCouchbaseErrorMsg << ". " << DL_LOCK_REMOVAL_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			releaseLock(lock, lkError);
			return(false);
		}

		// Now remove the lockInfo entry.
		// '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;  // LockId becomes the new key now.

		memset(&rcmd, 0, sizeof rcmd);
		rcmd.v.v0.key = lockInfoKey.c_str();;
		rcmd.v.v0.nkey = lockInfoKey.length();
		lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
		lastCouchbaseErrorMsg = "";
		// Make an attempt to delete a document for this lock now.
		err = lcb_remove(instance, this, 1, &rcmdlist);

		if (err != LCB_SUCCESS) {
			errorMsg = string(lcb_strerror(instance, err));
			lkError.set("Unable to remove the lock info for the lock Id " + lockIdString +
				". Error in scheduling a Couchbase storage operation. " + errorMsg, DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to remove the lock info for the lock id " <<
				lockIdString << ". Error in scheduling a Couchbase storage operation. " <<
				errorMsg << ". " << DL_LOCK_REMOVAL_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			releaseLock(lock, lkError);
			return(false);
		}

		// remove_callback is being invoked during this wait.
		lcb_wait(instance);

		// We can accept the error code LCB_KEY_ENOENT (i.e. key doesn't exist on the server.)
		if ((lastCouchbaseErrorCode != LCB_SUCCESS) && (lastCouchbaseErrorCode != LCB_KEY_ENOENT)) {
			lkError.set("Unable to remove the lock info for the lock Id " + lockIdString +
				". Error=" + lastCouchbaseErrorMsg, DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to remove the lock info for the lock Id " <<
				lockIdString << ". Error=" << lastCouchbaseErrorMsg << ". " << DL_LOCK_REMOVAL_ERROR, "CouchbaseDBLayer");
			lcb_destroy(instance);
			releaseLock(lock, lkError);
			return(false);
		}

		// We successfully removed the lock for a given lock id.
		lcb_destroy(instance);
		releaseLock(lock, lkError);
		// Inside the release lock function we called in the previous statement, it makes a
		// call to update the lock info. That will obviously fail since we removed here everything about
		// this lock. Hence, let us not propagate that error and cause the user to panic.
		// Reset any error that may have happened in the previous operation of releasing the lock.
		lkError.reset();
		// Life of this lock ended completely with no trace left behind.
	    return(true);
  }

  bool CouchbaseDBLayer::acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside acquireLock for lock id " << lock, "CouchbaseDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();
	  int32_t retryCnt = 0;

	  // If the lock doesn't exist, there is nothing to acquire. Don't allow this caller inside this method.
	  if(lockIdExistsOrNot(lockIdString, lkError) == false) {
		  if (lkError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "CouchbaseDBLayer");
		  } else {
			  lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to find a lock with an id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "CouchbaseDBLayer");
		  }

		  return(false);
	  }

	  // We will first check if we can get this lock.
	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  time_t startTime, timeNow;
	  // Get the start time for our lock acquisition attempts.
	  time(&startTime);

	  struct lcb_create_st cropts;
	  memset(&cropts, 0, sizeof cropts);
	  cropts.version = 3;
	  cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
	  lcb_error_t err;
	  lcb_t instance;
	  string errorMsg = "";

	  err = lcb_create(&instance, &cropts);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to create the Couchbase instance." <<
			  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
		  return(false);
	  }

	  lcb_connect(instance);
	  lcb_wait(instance);

	  if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  SPLAPPTRC(L_DEBUG,
			  "Inside acquireLock, it failed to bootstrap the connection to the Couchbase instance." <<
			  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  lcb_set_store_callback(instance, storage_callback);
	  lcb_store_cmd_t scmd;
	  memset(&scmd, 0, sizeof scmd);
	  const lcb_store_cmd_t *scmdlist = &scmd;
	  scmd.v.v0.key = distributedLockKey.c_str();
	  scmd.v.v0.nkey = distributedLockKey.length();
	  scmd.v.v0.bytes = "X";
	  scmd.v.v0.nbytes = 1;
	  // Upto 30 days of TTL, we can specify in seconds or in epoch time of expiry.
	  // Beyond a 30 day period, we can only specify epoch time at which it should expire.
	  // However, in this particular case we will use the time of expiry expressed in epoch time.
	  time_t new_lock_expiry_time = time(0) + (time_t)leaseTime;
	  scmd.v.v0.exptime = (uint32_t) new_lock_expiry_time;
	  // Insert only when the key doesn't exist already.
	  // If it exists, it will return an error.
	  scmd.v.v0.operation = LCB_ADD;
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";

	  // Try to get a lock for this lock acquisition entity.
	  while (1) {
		  // This is an atomic activity.
		  // If multiple threads attempt to do it at the same time, only one will succeed.
		  // Winner will hold the lock until they release it voluntarily or the Couchbase removes it after the TTL value expires.
		  // Make an attempt to create a document for this lock now.
		  errorMsg = "";
		  err = lcb_store(instance, this, 1, &scmdlist);

		  if (err != LCB_SUCCESS) {
			  errorMsg = string(lcb_strerror(instance, err));
			  SPLAPPTRC(L_DEBUG,
				  "Inside acquireLock, it failed to schedule a Couchbase storage operation." <<
				  " Error: rc=" << err << ", msg=" << errorMsg, "CouchbaseDBLayer");
			  lcb_destroy(instance);
			  return(false);
		  }

		  //storage_callback is being invoked during this wait.
		  lcb_wait(instance);

		  // Check the storage operation result.
		  if (lastCouchbaseErrorCode == LCB_SUCCESS) {
			  // We got the lock. We can return now.
			  // Let us update the lock information now.
			  if(updateLockInformation(lockIdString, lkError, 1, new_lock_expiry_time, getpid()) == true) {
				  lcb_destroy(instance);
				  return(true);
			  } else {
				  // Some error occurred while updating the lock information.
				  // It will be in an inconsistent state. Let us release the lock.
				  releaseLock(lock, lkError);
			  }
		  }

		  if (lastCouchbaseErrorCode != LCB_KEY_EEXISTS && lastCouchbaseErrorCode != LCB_SUCCESS) {
			  // There is some other error other than the key already exists error.
			  // Let us exit now.
			  SPLAPPTRC(L_DEBUG,
				  "Inside acquireLock, it failed during the storage operation." <<
				  " Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg, "CouchbaseDBLayer");
			  lcb_destroy(instance);
			  return(false);
		  }

		  SPLAPPTRC(L_DEBUG, "User defined lock acquisition error=" << errorMsg, "CouchbaseDBLayer");
		  // Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		  retryCnt++;

		  if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
			  lkError.set("Unable to acquire the lock named " + lockIdString + " after maximum retries.", DL_GET_LOCK_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire a lock named " << lockIdString << " after maximum retries. " <<
			     DL_GET_LOCK_ERROR, "CouchbaseDBLayer");
			  lcb_destroy(instance);
			  // Our caller can check the error code and try to acquire the lock again.
			  return(false);
		  }

		  // Check if we have gone past the maximum wait time the caller was willing to wait in order to acquire this lock.
		  time(&timeNow);
		  if (difftime(startTime, timeNow) > maxWaitTimeToAcquireLock) {
			  lkError.set("Unable to acquire the lock named " + lockIdString + " within the caller specified wait time.", DL_GET_LOCK_TIMEOUT_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire the lock named " << lockIdString <<
			     " within the caller specified wait time." << DL_GET_LOCK_TIMEOUT_ERROR, "CouchbaseDBLayer");
			  lcb_destroy(instance);
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

	  lcb_destroy(instance);
	  return(false);
  }

  void CouchbaseDBLayer::releaseLock(uint64_t lock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside releaseLock for lock id " << lock, "CouchbaseDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();

	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;

	  struct lcb_create_st cropts;
	  memset(&cropts, 0, sizeof cropts);
	  cropts.version = 3;
	  cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
	  lcb_error_t err;
	  lcb_t instance;
	  string errorMsg = "";

	  err = lcb_create(&instance, &cropts);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  lkError.set("Unable to release the lock for the lock Id " + lockIdString +
			  ". Error in creating the Couchbase instance. " + errorMsg, DL_LOCK_RELEASE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside releaseLock, it failed to release the lock for the lock id " <<
			  lockIdString << ". Error in creating the Couchbase instance. " <<
			  errorMsg << ". " << DL_LOCK_RELEASE_ERROR, "CouchbaseDBLayer");
		  return;
	  }

	  lcb_connect(instance);
	  lcb_wait(instance);

	  if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  lkError.set("Unable to release the lock for the lock Id " + lockIdString +
				  ". Error in bootstrapping the connection to the Couchbase instance. " + errorMsg, DL_LOCK_RELEASE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside releaseLock, it failed to release the lock for the lock id " <<
			  lockIdString << ". Error in bootstrapping the connection to the Couchbase instance. " <<
			  errorMsg << ". " << DL_LOCK_RELEASE_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return;
	  }

	  lcb_set_remove_callback(instance, remove_callback);
	  lcb_remove_cmd_t rcmd;
	  memset(&rcmd, 0, sizeof rcmd);
	  const lcb_remove_cmd_t *rcmdlist = &rcmd;
	  rcmd.v.v0.key = distributedLockKey.c_str();;
	  rcmd.v.v0.nkey = distributedLockKey.length();
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";
	  // Make an attempt to delete a document for this lock now.
	  err = lcb_remove(instance, this, 1, &rcmdlist);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  lkError.set("Unable to release the lock for the lock Id " + lockIdString +
			  ". Error in scheduling a Couchbase storage operation. " + errorMsg, DL_LOCK_RELEASE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside releaseLock, it failed to remove the distributed lock for the lock id " <<
			  lockIdString << ". Error in scheduling a Couchbase storage operation. " <<
			  errorMsg << ". " << DL_LOCK_RELEASE_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return;
	  }

	  // remove_callback is being invoked during this wait.
	  lcb_wait(instance);

	  // Check the remove operation result.
	  // We can accept the error code LCB_KEY_ENOENT (i.e. key doesn't exist on the server.)
	  // That is because, the distributed lock we are trying to release/delete may have already been removed at the
	  // TTL expiration time registered during its creation time. In this particular case of releasing a
	  // distributed lock, we will treat that error condition as normal and continue further in the code below.
	  if ((lastCouchbaseErrorCode != LCB_SUCCESS) && (lastCouchbaseErrorCode != LCB_KEY_ENOENT)) {
		  // There is some other error other than the key already exists error.
		  // Let us exit now.
		  // There was an error in deleting the user defined lock.
		  lkError.set("Unable to release the lock for the lock id " + lockIdString +
			  ". Error=" + lastCouchbaseErrorMsg, DL_LOCK_RELEASE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside releaseLock, it failed to remove the distributed lock with an id " << lockIdString << ". Error=" <<
			  lastCouchbaseErrorMsg << ". " << DL_LOCK_RELEASE_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return;
	  }

	  // We successfully released the distributed lock entry.
	  // Update the lock information to indicate that the distributed lock is free for use by someone else.
	  lcb_destroy(instance);
	  updateLockInformation(lockIdString, lkError, 0, 0, 0);
  }

  bool CouchbaseDBLayer::updateLockInformation(std::string const & lockIdString,
	PersistenceError & lkError, uint32_t const & lockUsageCnt, int32_t const & lockExpirationTime, pid_t const & lockOwningPid) {
	  // Get the lock name for this lock.
	  uint32_t _lockUsageCnt = 0;
	  int32_t _lockExpirationTime = 0;
	  std::string _lockName = "";
	  pid_t _lockOwningPid = 0;

	  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "CouchbaseDBLayer");
		  return(false);
	  }

	  // Let us update the lock information.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  ostringstream lockInfoValue;
	  lockInfoValue << lockUsageCnt << "_" << lockExpirationTime << "_" << lockOwningPid << "_";
	  // If we append the _lockName in the previous line, we will usually get random data corruption.
	  // Hence, adding the _lockName separately to an std::string instead of an ostringstream.
	  string lockInfoValueStr = lockInfoValue.str();
	  lockInfoValueStr += _lockName;

	  struct lcb_create_st cropts;
	  memset(&cropts, 0, sizeof cropts);
	  cropts.version = 3;
	  cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
	  lcb_error_t err;
	  lcb_t instance;
	  string errorMsg = "";

	  err = lcb_create(&instance, &cropts);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  lkError.set("Unable to update the lock info for a lock named " + _lockName +
			  ". Error in creating the Couchbase instance. " + errorMsg, DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed to update the lock info for a lock named " <<
			  _lockName << ". Error in creating the Couchbase instance. " <<
			  errorMsg << ". " << DL_LOCK_INFO_UPDATE_ERROR, "CouchbaseDBLayer");
		  return (false);
	  }

	  lcb_connect(instance);
	  lcb_wait(instance);

	  if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  lkError.set("Unable to update the lock info for a lock named " + _lockName +
			  ". Error in bootstrapping the connection to the Couchbase instance. " + errorMsg, DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed to update the lock info for a lock named " <<
			  _lockName << ". Error in bootstrapping the connection to the Couchbase instance. " <<
			  errorMsg << ". " << DL_LOCK_INFO_UPDATE_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  lcb_set_store_callback(instance, storage_callback);
	  lcb_store_cmd_t scmd;
	  memset(&scmd, 0, sizeof scmd);
	  const lcb_store_cmd_t *scmdlist = &scmd;
	  scmd.v.v0.key = lockInfoKey.c_str();
	  scmd.v.v0.nkey = lockInfoKey.length();
	  scmd.v.v0.bytes = lockInfoValueStr.c_str();
	  scmd.v.v0.nbytes = lockInfoValueStr.length();
	  // Insert or update it.
	  scmd.v.v0.operation = LCB_SET;
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";
	  // Make an attempt to create a document for this lock now.
	  err = lcb_store(instance, this, 1, &scmdlist);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  lkError.set("Unable to update the lock info for a lock named " + _lockName +
			  ". Error in scheduling a Couchbase storage operation. " + errorMsg, DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed to update the lock info for a lock named " <<
			  _lockName << ". Error in scheduling a Couchbase storage operation. " <<
			  errorMsg << ". " << DL_LOCK_INFO_UPDATE_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  //storage_callback is being invoked during this wait.
	  lcb_wait(instance);

	  // Check the storage operation result.
	  if (lastCouchbaseErrorCode == LCB_SUCCESS) {
		  // We stored/updated the lock information.
		  lcb_destroy(instance);
		  return(true);
	  } else {
		  // There is some other error other than the key already exists error.
		  // Let us exit now.
		  lkError.set("Critical Error1: Unable to update 'LockId:LockInfo' in the cache for a lock named " +
			  _lockName + ". Failed while updating the lock information: " + lastCouchbaseErrorMsg, DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Critical Error1: Inside updateLockInformation, it failed for a lock named " <<
			  _lockName << ". Failed while updating the lock information: " <<
			  lastCouchbaseErrorMsg << ". " << DL_LOCK_INFO_UPDATE_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return (false);
	  }
  }

  bool CouchbaseDBLayer::readLockInformation(std::string const & lockIdString, PersistenceError & lkError, uint32_t & lockUsageCnt,
		  int32_t & lockExpirationTime, pid_t & lockOwningPid, std::string & lockName) {
	  // Read the contents of the lock information.
	  lockName = "";
	  std::string lockInfo = "";

	  // Lock Info contains meta data information about a given lock.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  string errorMsg = "";

	  struct lcb_create_st cropts;
	  memset(&cropts, 0, sizeof cropts);
	  cropts.version = 3;
	  cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
	  lcb_error_t err;
	  lcb_t instance;

	  err = lcb_create(&instance, &cropts);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  lkError.set("Inside readLockInformation, it failed to create the Couchbase instance for the lock id " +
			  lockIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DL_GET_LOCK_INFO_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside readLockInformation, it failed to create the Couchbase instance for the lock id " <<
			  lockIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DL_GET_LOCK_INFO_ERROR, "CouchbaseDBLayer");
		  return(false);
	  }

	  lcb_connect(instance);
	  lcb_wait(instance);

	  if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  lkError.set("Inside readLockInformation, it failed to bootstrap the connection to the Couchbase instance for the lock id " +
			  lockIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DL_GET_LOCK_INFO_ERROR);
		  SPLAPPTRC(L_DEBUG,
			  "Inside readLockInformation, it failed to bootstrap the connection to the Couchbase instance for the lock id " <<
			  lockIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DL_GET_LOCK_INFO_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  lcb_set_get_callback(instance, get_callback);
	  lcb_get_cmd_t gcmd;
	  memset(&gcmd, 0, sizeof gcmd);
	  const lcb_get_cmd_t *gcmdlist = &gcmd;
	  gcmd.v.v0.key = lockInfoKey.c_str();
	  gcmd.v.v0.nkey = lockInfoKey.length();
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";
	  err = lcb_get(instance, this, 1, &gcmdlist);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  lkError.set("Inside readLockInformation, it failed to schedule a Couchbase get operation for the lock id " +
			  lockIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DL_GET_LOCK_INFO_ERROR);
		  SPLAPPTRC(L_DEBUG,
			  "Inside readLockInformation, it failed to schedule a Couchbase get operation for the lock id " <<
			  lockIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DL_GET_LOCK_INFO_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  // get callback is being invoked during this wait.
	  lcb_wait(instance);

	  if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		  // Requested data item is not there in Couchbase.
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		  lkError.set("Inside readLockInformation, it couldn't get the lock info for the lock id " +
			  lockIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DL_GET_LOCK_INFO_ERROR);
		  SPLAPPTRC(L_DEBUG,
			  "Inside readLockInformation, it couldn't get the lock info for the lock id " <<
			  lockIdString << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			  DL_GET_LOCK_INFO_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  lcb_destroy(instance);
	  // We got the value from the Couchbase K/V store.
	  lockInfo = lastCouchbaseOperationValue;

	  // As shown in the comment line above, lock information is a string that has multiple pieces of
	  // information each separated by an underscore character. We are interested in all the three tokens (lock usage count, lock expiration time, lock name).
	  // Let us parse it now.
	  std::vector<std::string> words;
	  streams_boost::split(words, lockInfo, streams_boost::is_any_of("_"), streams_boost::token_compress_on);
	  int32_t tokenCnt = 0;
	  lockUsageCnt = 0;

	  // In Couchbase, I have noticed at times, it corrupts the lock information data.
	  // In that case, it returns only 3 tokens instead of 4 tokens because of a missing underscore character due to data corruption.
	  // We seldom use the first two fields (lockUsageCnt and lockExpirationTime).
	  // But, we definitely will need the last two fields (pid and the store name).
	  // Usually, this data corruption merges the first two fields with garbage characters by erasing the first underscore character.
	  // Because of this, our vector will have only 3 slots instead of 4 slots.
	  // We can compensate this data corruption by pushing a new dummy lockUsageCnt as the very first token in the vector.
	  // In some cases, I have seen a total corruption of our lockInfo variable and that will cause an unrecoverable error.
	  if (words.size() == 3) {
		  // This is a possible data corruption that ate away our first token in the vector.
		  // Let us insert a dummy lockUsageCnt value there in the first slot.
		  std::vector<std::string>::iterator it;
		  it = words.begin();
		  it = words.insert(it, string("0"));
	  }

	  for (std::vector<std::string>::iterator it = words.begin(); it != words.end(); ++it) {
		  string tmpString = *it;

		  switch(++tokenCnt) {
		  	  case 1:
				  if (tmpString.empty() == false) {
					  // In Couchbase, this field is read back in corrupted format occasionally.
					  // When that happens, the following boost call fails. Hence, I replaced it with a C function atoi.
					  // lockUsageCnt = streams_boost::lexical_cast<uint32_t>(tmpString.c_str());
					  lockUsageCnt = (uint32_t)atoi(tmpString.c_str());
				  }

		  		  break;

		  	  case 2:
				  if (tmpString.empty() == false) {
					  // In Couchbase, this field is read back in corrupted format occasionally.
					  // When that happens, the following boost call fails. Hence, I replaced it with a C function atoi.
					  // lockExpirationTime = streams_boost::lexical_cast<int32_t>(tmpString.c_str());
					  lockExpirationTime = (int32_t)atoi(tmpString.c_str());
				  }

		  		  break;

		  	  case 3:
				  if (tmpString.empty() == false) {
					  // In Couchbase, this field is read back in corrupted format occasionally.
					  // When that happens, the following boost call fails. Hence, I replaced it with a C function atoi.
					  // lockOwningPid = streams_boost::lexical_cast<int32_t>(tmpString.c_str());
					  lockOwningPid = (int32_t)atoi(tmpString.c_str());
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
  bool CouchbaseDBLayer::lockIdExistsOrNot(string lockIdString, PersistenceError & lkError) {
	  string keyString = string(DL_LOCK_INFO_TYPE) + lockIdString;
	  string errorMsg = "";
	  string value = "";

	  struct lcb_create_st cropts;
	  memset(&cropts, 0, sizeof cropts);
	  cropts.version = 3;
	  cropts.v.v3.connstr = (couchbaseServerUrl + string(DPS_DL_META_DATA_DB)).c_str();
	  lcb_error_t err;
	  lcb_t instance;

	  err = lcb_create(&instance, &cropts);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(NULL, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  lkError.set("Inside lockIdExistsOrNot, it failed to create the Couchbase instance for the lock id " +
			  lockIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DL_GET_LOCK_INFO_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside lockIdExistsOrNot, it failed to create the Couchbase instance for the lock id " <<
			  lockIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DL_GET_LOCK_INFO_ERROR, "CouchbaseDBLayer");
		  return(false);
	  }

	  lcb_connect(instance);
	  lcb_wait(instance);

	  if ((err = lcb_get_bootstrap_status(instance)) != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  lkError.set("Inside lockIdExistsOrNot, it failed to bootstrap the connection to the Couchbase instance for the lock id " +
			  lockIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DL_GET_LOCK_INFO_ERROR);
		  SPLAPPTRC(L_DEBUG,
			  "Inside lockIdExistsOrNot, it failed to bootstrap the connection to the Couchbase instance for the lock id " <<
			  lockIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DL_GET_LOCK_INFO_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  lcb_set_get_callback(instance, get_callback);
	  lcb_get_cmd_t gcmd;
	  memset(&gcmd, 0, sizeof gcmd);
	  const lcb_get_cmd_t *gcmdlist = &gcmd;
	  gcmd.v.v0.key = keyString.c_str();
	  gcmd.v.v0.nkey = keyString.length();
	  lastCouchbaseErrorCode = LCB_NOT_SUPPORTED;
	  lastCouchbaseErrorMsg = "";
	  err = lcb_get(instance, this, 1, &gcmdlist);

	  if (err != LCB_SUCCESS) {
		  errorMsg = string(lcb_strerror(instance, err));
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", err);
		  lkError.set("Inside lockIdExistsOrNot, it failed to schedule a Couchbase get operation for the lock id " +
			  lockIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + errorMsg, DL_GET_LOCK_INFO_ERROR);
		  SPLAPPTRC(L_DEBUG,
			  "Inside lockIdExistsOrNot, it failed to schedule a Couchbase get operation for the lock id " <<
			  lockIdString << ". Error: rc=" << err << ", msg=" << errorMsg << ". " <<
			  DL_GET_LOCK_INFO_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }

	  // get callback is being invoked during this wait.
	  lcb_wait(instance);

	  if (lastCouchbaseErrorCode == LCB_SUCCESS) {
		  // This lock id exists.
		  lcb_destroy(instance);
		  return(true);
	  } else {
		  // Requested data item is not there in Couchbase.
		  char errCodeString[50];
		  sprintf(errCodeString, "%d", lastCouchbaseErrorCode);
		  lkError.set("Inside lockIdExistsOrNot, it couldn't get the lock info for the lock id " +
			  lockIdString + ". Error: rc=" + string(errCodeString) + ", msg=" + lastCouchbaseErrorMsg, DL_GET_LOCK_INFO_ERROR);
		  SPLAPPTRC(L_DEBUG,
			  "Inside lockIdExistsOrNot, it couldn't get the lock info for the lock id " <<
			  lockIdString << ". Error: rc=" << lastCouchbaseErrorCode << ", msg=" << lastCouchbaseErrorMsg << ". " <<
			  DL_GET_LOCK_INFO_ERROR, "CouchbaseDBLayer");
		  lcb_destroy(instance);
		  return(false);
	  }
  }

  // This method will return the process id that currently owns the given lock.
  uint32_t CouchbaseDBLayer::getPidForLock(string const & name, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside getPidForLock with a name " << name, "CouchbaseDBLayer");

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
		  SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "CouchbaseDBLayer");
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
  inline bool CouchbaseDBLayer::is_b64(unsigned char c) {
    return (isalnum(c) || (c == '+') || (c == '/'));
  }

  // Base64 encode the binary buffer contents passed by the caller and return a string representation.
  // There is no change to the original code in this method other than a slight change in the method name,
  // returning back right away when encountering an empty buffer and an array initialization for
  // char_array_4 to avoid a compiler warning.
  //
  void CouchbaseDBLayer::b64_encode(unsigned char const * & buf, uint32_t const & bufLenOrg, string & ret) {
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
  void CouchbaseDBLayer::b64_decode(std::string & encoded_string, unsigned char * & buf, uint32_t & bufLen) {
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

  // Couchbase store, retrieve, remove callback functions.
  // The 2nd argument will be pointing to a object of this class. That will allow us to
  // get access to our non-static members.
  void CouchbaseDBLayer::storage_callback(lcb_t instance, const void *cookie, lcb_storage_t op,
	  lcb_error_t err, const lcb_store_resp_t *resp) {
	  // Get a non-constant pointer.
	  void *ptr = const_cast<void *>(cookie);
	  static_cast<CouchbaseDBLayer*>(ptr)->storageImpl(instance, err, resp);
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  void CouchbaseDBLayer::storageImpl(lcb_t instance, lcb_error_t err, const lcb_store_resp_t *resp) {
	  lastCouchbaseErrorCode = err;

	  if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		  lastCouchbaseErrorMsg = lcb_strerror(instance, err);
	  } else {
		  lastCouchbaseErrorMsg = "";
	  }
  }

  // The 2nd argument will be pointing to a object of this class. That will allow us to
  // get access to our non-static members.
  void CouchbaseDBLayer::get_callback(lcb_t instance, const void *cookie, lcb_error_t err,
	  const lcb_get_resp_t *resp) {
	  // Get a non-constant pointer.
	  void *ptr = const_cast<void *>(cookie);
	  static_cast<CouchbaseDBLayer*>(ptr)->getImpl(instance, err, resp);
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  void CouchbaseDBLayer::getImpl(lcb_t instance, lcb_error_t err, const lcb_get_resp_t *resp) {
	  lastCouchbaseErrorCode = err;

	  if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		  lastCouchbaseErrorMsg = lcb_strerror(instance, err);
	  } else {
		  lastCouchbaseErrorMsg = "";
		  lastCouchbaseOperationKey = string((const char*)resp->v.v0.key, (size_t)resp->v.v0.nkey);
		  lastCouchbaseOperationValue = string((const char*)resp->v.v0.bytes, (size_t)resp->v.v0.nbytes);
	  }
  }

  // The 2nd argument will be pointing to a object of this class. That will allow us to
  // get access to our non-static members.
  void CouchbaseDBLayer::remove_callback(lcb_t instance, const void *cookie,
	  lcb_error_t err, const lcb_remove_resp_t *resp) {
	  // Get a non-constant pointer.
	  void *ptr = const_cast<void *>(cookie);
	  static_cast<CouchbaseDBLayer*>(ptr)->removeImpl(instance, err, resp);
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  void CouchbaseDBLayer::removeImpl(lcb_t instance, lcb_error_t err, const lcb_remove_resp_t *resp) {
	  lastCouchbaseErrorCode = err;

	  if (lastCouchbaseErrorCode != LCB_SUCCESS) {
		  lastCouchbaseErrorMsg = lcb_strerror(instance, err);
	  } else {
		  lastCouchbaseErrorMsg = "";
	  }
  }

  // Couchbase specific cURL write and read functions.
  // Since we are using C++ and the cURL library is C based, we have to set the callback as a
  // static C++ method and configure cURL to pass our custom C++ object pointer in the 4th argument of
  // the callback function. Once we get the C++ object pointer, we can access our other non-static
  // member functions and non-static member variables via that object pointer.
  //
  // The 4th argument will be pointing to the object of this class.  That will allow us to
  // get access to our non-static members.
  size_t CouchbaseDBLayer::writeFunction(char *data, size_t size, size_t nmemb, void *objPtr) {
	return(static_cast<CouchbaseDBLayer*>(objPtr)->writeFunctionImpl(data, size, nmemb));
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  size_t CouchbaseDBLayer::writeFunctionImpl(char *data, size_t size, size_t nmemb) {
	  memcpy(&(curlBuffer[curlBufferOffset]), data, size * nmemb);
	  curlBufferOffset += size * nmemb;
	  return (size * nmemb);
  }

  // Do the same for the read function.
  // The 4th argument will be pointing to the object of this class. That will allow us to
  // get access to our non-static members.
  size_t CouchbaseDBLayer::readFunction(char *data, size_t size, size_t nmemb, void *objPtr) {
	  return(static_cast<CouchbaseDBLayer*>(objPtr)->readFunctionImpl(data, size, nmemb));
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  size_t CouchbaseDBLayer::readFunctionImpl(char *data, size_t size, size_t nmemb) {
	  int len = strlen(putBuffer);
	  memcpy(data, putBuffer, len);
	  return len;
  }

  bool CouchbaseDBLayer::createCouchbaseBucket(string const & bucketName, string & errorMsg,
	  string const & ramBucketQuota) {
	  errorMsg = "";
	  // We will be sending the CURL requests to a different Couchbase server every time.
	  string serverName = couchbaseServers[couchbaseServerIdx++];

	  // Validate the next available server name.
	  // Change the maximum number of servers based on how big the server list is defined in the .h of this class.
	  if (couchbaseServerIdx == 50 || couchbaseServers[couchbaseServerIdx] == "") {
		  // There is no valid server name at this slot.
		  // Set it to the very first slot in that list.
		  couchbaseServerIdx = 0;
	  }

	  // Let us get a unique web proxy port number.
	  // This must be different for every DPS store. If it is a duplicate port number, Couchbase CURL call below will
	  // give an error. We will compute a range of ports starting from 11216.
	  // This port number should be repeatable across all machines for a given store name string.
	  int32_t bucketNameLength = bucketName.length();
	  const char *bucketNameStr = bucketName.c_str();
	  int32_t bucketUniqueId = 0;

	  for (int cnt = 0; cnt < bucketNameLength; cnt++) {
		  int32_t currentChar = bucketNameStr[cnt];

		  //Our store names will have characters ranging from ASCII characters 48 to 122 (decimal).
		  // We will multiply every character by its character position in the given string starting from 1 through N.
		  // We will add the value of every digit in a multplied result ASCII character along with the character position in the string.
		  // Multiply the current character value by its character position number in the given string.
		  currentChar *= (cnt+1);
		  // Add the 100th place value.
		  bucketUniqueId += currentChar/100;

		  if (currentChar >= 100) {
			  int32_t x = currentChar/100;
			  currentChar -= (x*100);
		  }

		  // Add the 10th place value.
		  bucketUniqueId += currentChar/10;
		  // Add the last single digit place value.
		  // Take the remainder.
		  bucketUniqueId += currentChar%10;
		  // Add the current character position.
		  bucketUniqueId += cnt;
	  }

	  bucketUniqueId += COUCHBASE_BUCKET_PROXY_BASE_PORT;
	  std::ostringstream webProxyPort;
	  webProxyPort << bucketUniqueId;

	  // Please note that we have hardcoded the CURL server's port number. That is the default port number in Couchbase.
	  // It will require a code change and a recompiling of this .so library if a different port number is being used.
	  string url = "http://" + curlBasicAuth + "@" + serverName + ":" + string(COUCHBASE_WEB_ADMIN_PORT) + "/pools/default/buckets";
	  int32_t curlReturnCode = 0;
	  uint64_t httpResponseCode = 0;
	  CURLcode result;

	  // It is not the same URL we are connecting to again.
	  // We can't reuse the previously made cURL session. Reset it now.
	  curl_easy_reset(curlForCreateCouchbaseBucket);
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_URL, url.c_str());
	  // Fill the create bucket parameters as POST fields.
	  // Specify the Couchbase bucket name (i.e DPS store name).
	  string postFields = "name=" + bucketName;
	  // Specify the Couchbase bucket type.
	  postFields += string("&") + string("bucketType=") + string("couchbase");
	  // Specify the RAM Quota.
	  // The Bucket Quota is the amount of RAM allocated to an individual bucket for caching data.
	  // Bucket Quotas are configured on a per-node basis, and is allocated out of the RAM defined by the Server Quota.
	  // For example, if you create a new bucket with a Bucket Quota of 1GB, in a 10 node cluster there would be an
	  // aggregate bucket quota of 10GB across the cluster.
	  // We are going to allocate 100MB per bucket per machine. If user wants to change it, they can do so and
	  // recompile to create a new .so file.
	  postFields += string("&") + string("ramQuotaMB=") + ramBucketQuota;
	  // Specify the thread count.
	  postFields += string("&") + string("threadsNumber=") + string(COUCHBASE_THREAD_COUNT_PER_BUCKET);
	  // Specify the authentication type.
	  postFields += string("&") + string("authType=") + string("none");

	  // Specify the replication number.
	  string replicaNumber = "0";
	  // As of Jan/2015, my tests in a Couchbase cluster of 5 machines (with replica 1) was so slow and it showed
	  // problems in properly reading or enumerating the data we wrote earlier. Hence, I left the replica count to 0 here.
	  // That can be revisited in future revisions of this code to see if a non-zero replica count could be used
	  // when a Couchbase cluster is used.
	  /*
	  if (totalCouchbaseServers > 1) {
		  replicaNumber = "1";
	  }
	  */

	  postFields += string("&") + string("replicaNumber=") + replicaNumber;
	  // Specify the web proxy port. (This should be unique for every bucket.)
	  postFields += string("&") + string("proxyPort=") + webProxyPort.str();
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_POSTFIELDSIZE, postFields.length());
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_COPYPOSTFIELDS, postFields.c_str());

	  // cURL is C based and we are here in C++.
	  // Hence, we have to pass a static method pointer in the CURLOPT_WRITEFUNCTION and then pass a custom
	  // object reference (this) of our current C++ class in the CURLOPT_WRITEDATA so that cURL will pass that
	  // object pointer as the 4th argument to our static writeFunction during its callback. By passing that object
	  // pointer as an argument to a static method, we can let the static method access our
	  // non-static member functions and non-static member variables via that object pointer.
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_WRITEDATA, this);
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_WRITEFUNCTION, &CouchbaseDBLayer::writeFunction);

	  // Enable the TCP keep alive so that we don't lose the connection with the
	  // HBase server when no store functions are performed for long durations.
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_TCP_KEEPALIVE, 1);
	  // Since we are using HTTP POSTFIELDS with a queryParam string containing & characters, our header must have a
	  // content-type of application/x-www-form-urlencoded
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_HTTPHEADER, headersForCreateCouchbaseBucket);
	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForCreateCouchbaseBucket);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  errorMsg = "CURL Error: rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForCreateCouchbaseBucket, CURLINFO_RESPONSE_CODE, &httpResponseCode);

	  // Check if this bucket already exists.
	  // This error string is returned in the server's HTTP response.
	  // If and when Couchbase team changes this error message, we must make a corresponding change here.
	  if (string(curlBuffer).find("Bucket with given name already exists") != std::string::npos) {
		  // This store i.e. bucket already exists in the Couchbase infrastructure.
		  return(true);
	  }

	  // HTTP response codes: 200-->OK or 202-->Request accepted
	  if ((httpResponseCode != COUCHBASE_REST_OK) && (httpResponseCode != COUCHBASE_REST_ACCEPTED)) {
		  ostringstream hrc;
		  hrc << httpResponseCode;
		  errorMsg = "HTTP REST Error: rc=" + hrc.str() + ", msg=" + string(curlBuffer);
		  return(false);
	  }

	  // In Couchbase, I noticed that the creation of a new bucket takes an extra time to become available for real use.
	  // If we return immediately, users may get errors while inserting data before the new buckets is fully ready.
	  // Hence, we will do a wait of 10 seconds here.
	  SPL::Functions::Utility::block(10.0);

	  return(true);
  }

  // This method will delete a bucket in Couchbase.
  bool CouchbaseDBLayer::deleteCouchbaseBucket(string const & bucketName, string & errorMsg) {
	  errorMsg = "";
	  // We will be sending the CURL requests to a different Couchbase server every time.
	  string serverName = couchbaseServers[couchbaseServerIdx++];

	  // Validate the next available server name.
	  // Change the maximum number of servers based on how big the server list is defined in the .h of this class.
	  if (couchbaseServerIdx == 50 || couchbaseServers[couchbaseServerIdx] == "") {
		  // There is no valid server name at this slot.
		  // Set it to the very first slot in that list.
		  couchbaseServerIdx = 0;
	  }

	  // Please note that we have hardcoded the CURL server's port number. That is the default port number in Couchbase.
	  // It will require a code change and a recompiling of this .so library if a different port number is being used.
	  string url = "http://" + curlBasicAuth + "@" + serverName + ":" + string(COUCHBASE_WEB_ADMIN_PORT) +
		  "/pools/default/buckets/" + bucketName;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  CURLcode result;

	  // It is not the same URL we are connecting to again.
	  // We can't reuse the previously made cURL session. Reset it now.
	  curl_easy_reset(curlForDeleteCouchbaseBucket);
	  curl_easy_setopt(curlForDeleteCouchbaseBucket, CURLOPT_URL, url.c_str());
	  // cURL is C based and we are here in C++.
	  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
	  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
	  curl_easy_setopt(curlForDeleteCouchbaseBucket, CURLOPT_WRITEDATA, this);
	  curl_easy_setopt(curlForDeleteCouchbaseBucket, CURLOPT_WRITEFUNCTION, &CouchbaseDBLayer::writeFunction);
	  curl_easy_setopt(curlForDeleteCouchbaseBucket, CURLOPT_CUSTOMREQUEST, HTTP_DELETE);
	  // Enable the TCP keep alive so that we don't lose the connection with the
	  // HBase server when no store functions are performed for long durations.
	  curl_easy_setopt(curlForDeleteCouchbaseBucket, CURLOPT_TCP_KEEPALIVE, 1);
	  curl_easy_setopt(curlForDeleteCouchbaseBucket, CURLOPT_HTTPHEADER, headersForDeleteCouchbaseBucket);

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForDeleteCouchbaseBucket);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  errorMsg = "CURL Error: rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForDeleteCouchbaseBucket, CURLINFO_RESPONSE_CODE, &httpResponseCode);

	  // HTTP response codes: 200-->OK or 202-->Request accepted
	  if ((httpResponseCode != COUCHBASE_REST_OK) && (httpResponseCode != COUCHBASE_REST_ACCEPTED)) {
		  // Check if bucket deletion is not fully complete yet. In that case, we need not flag an error.
		  if (string(curlBuffer).find("Bucket deletion not yet complete") == std::string::npos) {
			  // This is a different error other than the "Bucket deletion not yet complete" error.
			  ostringstream hrc;
			  hrc << httpResponseCode;
			  errorMsg = "HTTP REST Error: rc=" + hrc.str() + ", msg=" + string(curlBuffer);
			  return(false);
		  }
	  }

	  // In Couchbase, I noticed that the deletion of a bucket takes an extra time to make that bucket go away from the disk.
	  // If we return immediately, users may get a false result if they check for the existence of that deleted bucket.
	  // Hence, we will do a wait of 10 seconds here.
	  SPL::Functions::Utility::block(10.0);

	  return(true);
  }

  bool CouchbaseDBLayer::getCouchbaseBucketSize(string const & bucketName, int64_t & bucketSize, string & errorMsg) {
	  errorMsg = "";
	  bucketSize = 0;
	  // We will be sending the CURL requests to a different Couchbase server every time.
	  string serverName = couchbaseServers[couchbaseServerIdx++];

	  // Validate the next available server name.
	  // Change the maximum number of servers based on how big the server list is defined in the .h of this class.
	  if (couchbaseServerIdx == 50 || couchbaseServers[couchbaseServerIdx] == "") {
		  // There is no valid server name at this slot.
		  // Set it to the very first slot in that list.
		  couchbaseServerIdx = 0;
	  }

	  // Please note that we have hardcoded the CURL server's port number. That is the default port number in Couchbase.
	  // It will require a code change and a recompiling of this .so library if a different port number is being used.
	  string url = "http://" + curlBasicAuth + "@" + serverName + ":" + string(COUCHBASE_WEB_ADMIN_PORT) +
		  "/pools/default/buckets/" + bucketName;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  CURLcode result;

	  // It is not the same URL we are connecting to again.
	  // We can't reuse the previously made cURL session. Reset it now.
	  curl_easy_reset(curlForGetCouchbaseBucket);
	  curl_easy_setopt(curlForGetCouchbaseBucket, CURLOPT_URL, url.c_str());
	  // cURL is C based and we are here in C++.
	  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
	  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
	  curl_easy_setopt(curlForGetCouchbaseBucket, CURLOPT_WRITEDATA, this);
	  curl_easy_setopt(curlForGetCouchbaseBucket, CURLOPT_WRITEFUNCTION, &CouchbaseDBLayer::writeFunction);
	  curl_easy_setopt(curlForGetCouchbaseBucket, CURLOPT_HTTPGET, 1);
	  // Enable the TCP keep alive so that we don't lose the connection with the
	  // HBase server when no store functions are performed for long durations.
	  curl_easy_setopt(curlForGetCouchbaseBucket, CURLOPT_TCP_KEEPALIVE, 1);
	  curl_easy_setopt(curlForGetCouchbaseBucket, CURLOPT_HTTPHEADER, headersForGetCouchbaseBucket);

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForGetCouchbaseBucket);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  errorMsg = "CURL Error: rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForGetCouchbaseBucket, CURLINFO_RESPONSE_CODE, &httpResponseCode);

	  // HTTP response codes: 200-->OK or 202-->Request accepted
	  if ((httpResponseCode != COUCHBASE_REST_OK) && (httpResponseCode != COUCHBASE_REST_ACCEPTED)) {
		  ostringstream hrc;
		  hrc << httpResponseCode;
		  errorMsg = "HTTP REST Error: rc=" + hrc.str() + ", msg=" + string(curlBuffer);
		  return(false);
	  }

	  // Let us parse the received JSON response to get the bucket size.
	  json_object *jo = json_tokener_parse(curlBuffer);
	  json_object *joForBasicStats = NULL;
	  // Let us get the total number of rows returned.
	  json_bool exists = json_object_object_get_ex(jo, "basicStats", &joForBasicStats);
	  bool bucketSizeObtained = false;

	  if (exists == false) {
		  errorMsg = "Unable to find the 'basicStats' field in the received JSON server response.";
	  } else {
		  // Let us now find a field named "itemCount" in the "basicStats" JSON object.
		  json_object *joForItemCount = NULL;
		  exists =  json_object_object_get_ex(joForBasicStats, "itemCount", &joForItemCount);

		  if (exists == true) {
			  bucketSize = json_object_get_int64(joForItemCount);
			  bucketSizeObtained = true;
		  } else {
			  errorMsg = "Unable to find the 'itemCount' field in the received JSON server response.";
		  }
	  }

	  json_object_put(jo);
	  return(bucketSizeObtained);
  }

  // This method will return all the keys stored in a given Couchbase bucket.
  bool CouchbaseDBLayer::getAllKeysInCouchbaseBucket(string const & bucketName,
	  std::vector<std::string> & dataItemKeys, string & errorMsg) {
	  errorMsg = "";
	  // To get all the keys in a Couchbase bucket, there are no C APIs available as of Jan/2015.
	  // However, there is a way to do this via REST APIs as done in this method.
	  // These REST APIs are targeted to a reserved Couchbase port 8092.
	  // We will be sending the CURL requests to a different Couchbase server every time.
	  string serverName = couchbaseServers[couchbaseServerIdx++];

	  // Validate the next available server name.
	  // Change the maximum number of servers based on how big the server list is defined in the .h of this class.
	  if (couchbaseServerIdx == 50 || couchbaseServers[couchbaseServerIdx] == "") {
		  // There is no valid server name at this slot.
		  // Set it to the very first slot in that list.
		  couchbaseServerIdx = 0;
	  }

	  // It is three different REST calls we have to make to get our final result.
	  // 1) HTTP PUT to create a design document and a view for the given store bucket.
	  // 2) HTTP GET to obtain all the keys in the given store bucket via the bucket view created in step (1).
	  // 3) HTTP DELETE to remove the bucket's design document and view created above in step (1).

	  // 1) HTTP PUT to create a design document and a view for the given store bucket.
	  // Please note that we have hardcoded the CURL server's Design Document/View port number. That is the default port number in Couchbase.
	  // It will require a code change and a recompiling of this .so library if a different port number is being used.
	  // We are going to create the design document and the view with the same name as the given store bucket name.
	  string url = "http://" + curlBasicAuth + "@" + serverName + ":" + string(COUCHBASE_DESIGN_DOC_VIEW_PORT) +
		  "/" + bucketName + "/_design/dev_" + bucketName;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  CURLcode result;
	  bool couchbaseResult = true;

	  // It is not the same URL we are connecting to again.
	  // We can't reuse the previously made cURL session. Reset it now.
	  curl_easy_reset(curlForCreateCouchbaseBucket);
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_URL, url.c_str());
	  // cURL is C based and we are here in C++.
	  // Hence, we have to pass a static method pointer in the CURLOPT_WRITEFUNCTION and then pass a custom
	  // object reference (this) of our current C++ class in the CURLOPT_WRITEDATA so that cURL will pass that
	  // object pointer as the 4th argument to our static writeFunction during its callback. By passing that object
	  // pointer as an argument to a static method, we can let the static method access our
	  // non-static member functions and non-static member variables via that object pointer.
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_WRITEDATA, this);
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_WRITEFUNCTION, &CouchbaseDBLayer::writeFunction);
	  // Do the same here for CURLOPT_READDATA and CURLOPT_READFUNCTION as we did above.
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_READDATA, this);
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_READFUNCTION, &CouchbaseDBLayer::readFunction);
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_PUT, 1);
	  // Enable the TCP keep alive so that we don't lose the connection with the
	  // Couchbase server when no store functions are performed for long durations.
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_TCP_KEEPALIVE, 1);
	  // IMPORTANT: We have to send JSON data via HTTP PUT. Hence, we will add a header list that
	  // contains a content-type of application/json
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_HTTPHEADER, headersForCreateCouchbaseBucket2);
	  // Our Couchbase view will carry the same name as the store bucket name.
	  //
	  // It should be in this format: '{"views" : {"[bucket-name]" : {"map" : "function (doc, meta) {emit(null);}"}}}'
	  //
	  string jsonDoc = "{\"views\" : {\"" + bucketName + "\" : {\"map\" : \"function (doc, meta) {emit(null);}\"}}}";
	  putBuffer = jsonDoc.c_str();
	  long putBufferLen = jsonDoc.length();
	  curl_easy_setopt(curlForCreateCouchbaseBucket, CURLOPT_INFILESIZE, putBufferLen);
	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForCreateCouchbaseBucket);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  errorMsg = "CURL Error: rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForCreateCouchbaseBucket, CURLINFO_RESPONSE_CODE, &httpResponseCode);

	  // HTTP response codes: 201-->Doc creation successful
	  if (httpResponseCode != COUCHBASE_BUCKET_DESIGNDOC_CREATED_OK) {
		 ostringstream hrc;
		 hrc << httpResponseCode;
		 errorMsg = string("HTTP REST Error: Failed to create the Couchbase design document required ") +
			 string("for getting all the keys in the store bucket '") + bucketName +
			 "'. rc=" + hrc.str() + ", msg=" + string(curlBuffer);
		 return(false);
	  }

	  // 2) HTTP GET to obtain all the keys in the given store bucket via the bucket view created in step (1).
	  // IMPORTANT: In the query string part of the URL, please note that we are limiting to retrieve a maximum of only 400 keys.
	  // If more keys are needed, this hard-coded value has to be changed below and then the DPS .so library needs to be regenerated.
	  url = "http://" + curlBasicAuth + "@" + serverName + ":" + string(COUCHBASE_DESIGN_DOC_VIEW_PORT) +
		  "/" + bucketName + "/_design/dev_" + bucketName + "/_view/" + bucketName +
		  "?stale=false&limit=400&skip=0";

	  // We are going to stay in a loop and attempt to get a list of all the store keys in a
	  // few attempts. In most cases, it will work fine in the very first attempt.
	  // In case it fails, we will try a few more times before giving up completely.
	  for (int32_t getAttemptCnt = 0; getAttemptCnt < 5; getAttemptCnt++) {
		  curl_easy_reset(curlForGetCouchbaseBucket);
		  curl_easy_setopt(curlForGetCouchbaseBucket, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
		  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
		  curl_easy_setopt(curlForGetCouchbaseBucket, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForGetCouchbaseBucket, CURLOPT_WRITEFUNCTION, &CouchbaseDBLayer::writeFunction);
		  curl_easy_setopt(curlForGetCouchbaseBucket, CURLOPT_HTTPGET, 1);
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // HBase server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForGetCouchbaseBucket, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForGetCouchbaseBucket, CURLOPT_HTTPHEADER, headersForGetCouchbaseBucket);
		  curlBufferOffset = 0;
		  result = curl_easy_perform(curlForGetCouchbaseBucket);
		  curlBuffer[curlBufferOffset] = '\0';

		  if (result != CURLE_OK) {
			  curlReturnCode = result;
			  ostringstream crc;
			  crc << curlReturnCode;
			  errorMsg = "CURL Error: rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
			  return(false);
		  }

		  curl_easy_getinfo(curlForGetCouchbaseBucket, CURLINFO_RESPONSE_CODE, &httpResponseCode);

		  // HTTP response codes: 200-->OK
		  if (httpResponseCode != COUCHBASE_REST_OK) {
			  ostringstream hrc;
			  hrc << httpResponseCode;
			  errorMsg = string("HTTP REST Error: Failed to get the Couchbase view information required ") +
				  string("for getting all the keys in the store bucket '") + bucketName +
				  "'. rc=" + hrc.str() + ", msg=" + string(curlBuffer);
			  return(false);
		  }

		  // If we reached this far, that means we have all the documents read successfully from the Couchbase server, let us parse them.
		  // Format of the received JSON document will be as shown below:
		  // (e-g:)
		  /*
	  	  {
		 	 "total_rows":7,"rows":
		 	 [
		    	{"id":"AWE=","key":null,"value":null},
		    	{"id":"AWI=","key":null,"value":null},
		    	{"id":"AWM=","key":null,"value":null},
		    	{"id":"AWQ=","key":null,"value":null},
		    	{"id":"dps_name_of_this_store","key":null,"value":null},
		    	{"id":"dps_spl_type_name_of_key","key":null,"value":null},
		    	{"id":"dps_spl_type_name_of_value","key":null,"value":null}
		 	 ]
	  	  }
		   */
		  bool emptyStoreError = false;
		  json_object *jo = json_tokener_parse(curlBuffer);
		  json_object *joForField = NULL;
		  // Let us get the total number of rows returned.
		  json_bool exists = json_object_object_get_ex(jo, "total_rows", &joForField);

		  if (exists) {
			  string value = string(json_object_get_string(joForField));
			  int32_t totalRows = atoi(value.c_str());

			  if (totalRows > 0) {
				  // We can collect all the document keys in the list passed by the caller.
				  // Get the JSON field "rows" which is an array object.
				  json_object *joRows;
				  exists = json_object_object_get_ex(jo, "rows", &joRows);

				  if (exists) {
					  int idx = 0;
					  for (idx = 0; idx < totalRows; idx++) {
						  // Get the next element in the "rows" array.
						  json_object *rowObj = json_object_array_get_idx(joRows, idx);
						  json_object *idObj;
						  // Get the "id" field in the row element JSON. This field holds our store key.
						  exists =  json_object_object_get_ex(rowObj, "id", &idObj);

						  if (exists) {
							  const char *data_item_key_ptr = json_object_get_string(idObj);
							  string data_item_key = string(data_item_key_ptr);
							  // Every dps store will have three mandatory reserved data item keys for internal use.
							  // Let us not add them to the list of docs (i.e. store keys).
							  if (data_item_key.compare(COUCHBASE_STORE_ID_TO_STORE_NAME_KEY) == 0) {
								  continue; // Skip this one.
							  } else if (data_item_key.compare(COUCHBASE_SPL_TYPE_NAME_OF_KEY) == 0) {
								  continue; // Skip this one.
							  } else if (data_item_key.compare(COUCHBASE_SPL_TYPE_NAME_OF_VALUE) == 0) {
								  continue; // Skip this one.
							  }

							  dataItemKeys.push_back(string(data_item_key));
							  // Reset the error message.
							  errorMsg = "";
						  } else {
							  httpResponseCode = COUCHBASE_DOC_FIELD_NOT_FOUND;
							  ostringstream hrc;
							  hrc << httpResponseCode;
							  errorMsg = "rc=" + hrc.str() + ", msg=Couchbase document field 'id' not found.";
							  couchbaseResult = false;
							  break;
						  }
					  } // End of the inner for loop
				  } else {
					  httpResponseCode = COUCHBASE_DOC_FIELD_NOT_FOUND;
					  ostringstream hrc;
					  hrc << httpResponseCode;
					  errorMsg = "rc=" + hrc.str() + ", msg=Couchbase document field 'rows' not found.";
					  couchbaseResult = false;
				  }
			  } else {
				  // It is an empty store? This is not correct.
				  // Because, we must at least have our 3 meta data entries.
				  httpResponseCode = COUCHBASE_DOC_FIELD_NOT_FOUND;
				  ostringstream hrc;
				  hrc << httpResponseCode;
				  errorMsg = "rc=" + hrc.str() + ", msg=Couchbase document points to an empty store. This is not correct.";
				  couchbaseResult = false;
				  emptyStoreError = true;
			  }
		  } else {
			  httpResponseCode = COUCHBASE_DOC_FIELD_NOT_FOUND;
			  ostringstream hrc;
			  hrc << httpResponseCode;
			  errorMsg = "rc=" + hrc.str() + ", msg=Couchbase document field 'total_rows' not found.";
			  couchbaseResult = false;
		  }

		  // Release it now.
		  json_object_put(jo);

		  if ((couchbaseResult == true) || (couchbaseResult == false && emptyStoreError != true)) {
			  // If we are successful in getting the keys or if we got any error other than
			  // an empty store, we can break now from the outer for loop.
			  break;
		  }
	  } // End of the outer for loop.

	  // 3) HTTP DELETE to remove the bucket's design document and view created above in step (1).
	  url = "http://" + curlBasicAuth + "@" + serverName + ":" + string(COUCHBASE_DESIGN_DOC_VIEW_PORT) +
		  "/" + bucketName + "/_design/dev_" + bucketName;
	  curl_easy_reset(curlForDeleteCouchbaseBucket);
	  curl_easy_setopt(curlForDeleteCouchbaseBucket, CURLOPT_URL, url.c_str());
	  // cURL is C based and we are here in C++.
	  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
	  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
	  curl_easy_setopt(curlForDeleteCouchbaseBucket, CURLOPT_WRITEDATA, this);
	  curl_easy_setopt(curlForDeleteCouchbaseBucket, CURLOPT_WRITEFUNCTION, &CouchbaseDBLayer::writeFunction);
	  curl_easy_setopt(curlForDeleteCouchbaseBucket, CURLOPT_CUSTOMREQUEST, HTTP_DELETE);
	  // Enable the TCP keep alive so that we don't lose the connection with the
	  // HBase server when no store functions are performed for long durations.
	  curl_easy_setopt(curlForDeleteCouchbaseBucket, CURLOPT_TCP_KEEPALIVE, 1);
	  curl_easy_setopt(curlForDeleteCouchbaseBucket, CURLOPT_HTTPHEADER, headersForDeleteCouchbaseBucket);

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForDeleteCouchbaseBucket);
	  curlBuffer[curlBufferOffset] = '\0';
	  // We obtained the store keys already.
	  // At this point, there is not much use in checking the result code for the
	  // delete operation we performed above irrespective of whether it was successful or not.
	  return(couchbaseResult);
  }

  // This method will return the status of the connection to the back-end data store.
  bool CouchbaseDBLayer::isConnected() {
          // Not implemented at this time.
          return(true);
  }

  bool CouchbaseDBLayer::reconnect(std::set<std::string> & dbServers, PersistenceError & dbError) {
          // Not implemented at this time.
          return(true);
  }

} } } } }
using namespace com::ibm::streamsx::store;
using namespace com::ibm::streamsx::store::distributed;
extern "C" {
	DBLayer * create(){
	   return new CouchbaseDBLayer();
	}
}
