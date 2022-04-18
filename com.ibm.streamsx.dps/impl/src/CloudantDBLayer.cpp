/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2022
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
/*
=================================================================================
Cloudant is an IBM NoSQL DB product that can run locally as well as on the cloud.
Since Cloudant works via HTTP and it uses JSON as the data exchange format,
we are using the following open source components.
1) cURL
2) json-c

We built those two shared libraries (.so files) separately outside of Streams
and then copied the two include directories (curl and json) into this toolkit's
impl/include directory. Those two .so files we built separately were also
copied into the impl/lib directory within the OS specific sub-directory.
=================================================================================
*/

/*
==================================================================================================================
This CPP file contains the source code for the distributed process store (dps) back-end activities such as
insert, update, read, remove, etc. This dps implementation runs on top of the Cloudant NoSQL DB.

Cloudant is a good NoSQL data store with support for any JSON formatted data. It also uses HTTP REST APIs
to perform store commands. In our Cloudant store implementation for Streams, we are using HTTP and JSON APIS from
the popular cURL and json-c libraries.

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
Cloudant interface code (this file), and your Cloudant infrastructure.

As a first step, you should run the ./mk script from the C++ project directory (DistributedProcessStoreLib).
That will take care of building the .so file for the dps and copying it to the SPL project's impl/lib directory.
==================================================================================================================
*/

#include "CloudantDBLayer.h"
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
  CloudantDBLayer::CloudantDBLayer()
  {
	  curlGlobalCleanupNeeded = false;
	  httpVerbUsedInPreviousRunCommand = "";
	  base64_chars = string("ABCDEFGHIJKLMNOPQRSTUVWXYZ") +
			  	  	 string("abcdefghijklmnopqrstuvwxyz") +
			  	  	 string("0123456789+/");

	  // We must initialize all the cURL related pointers we have as member variables in our C++ class.
	  // If we don't do that, it may point to random memory locations which will give GPF during runtime.
	  curlForCreateCloudantDatabase = NULL;
	  curlForDeleteCloudantDatabase = NULL;
	  curlForCreateOrUpdateCloudantDocument = NULL;
	  curlForReadCloudantDocumentField = NULL;
	  curlForDeleteCloudantDocument = NULL;
	  curlForGetAllDocsFromCloudantDatabase = NULL;
	  curlForRunDataStoreCommand = NULL;
	  headersForCreateCloudantDatabase = NULL;
	  headersForDeleteCloudantDatabase = NULL;
	  headersForCreateOrUpdateCloudantDocument = NULL;
	  headersForReadCloudantDocumentField = NULL;
	  headersForDeleteCloudantDocument = NULL;
	  headersForGetAllDocsFromCloudantDatabase = NULL;
	  headersForRunDataStoreCommand = NULL;
  }

  CloudantDBLayer::~CloudantDBLayer()
  {
	  // Application is ending now.
	  // Release the cURL resources we allocated at the application start-up time.
	  if (curlGlobalCleanupNeeded == true) {
		  // Free the cURL headers used with every cURL handle.
		  curl_slist_free_all(headersForCreateCloudantDatabase);
		  curl_slist_free_all(headersForDeleteCloudantDatabase);
		  curl_slist_free_all(headersForCreateOrUpdateCloudantDocument);
		  curl_slist_free_all(headersForReadCloudantDocumentField);
		  curl_slist_free_all(headersForDeleteCloudantDocument);
		  curl_slist_free_all(headersForGetAllDocsFromCloudantDatabase);
		  curl_slist_free_all(headersForRunDataStoreCommand);
		  // Free the cURL handles.
		  curl_easy_cleanup(curlForCreateCloudantDatabase);
		  curl_easy_cleanup(curlForDeleteCloudantDatabase);
		  curl_easy_cleanup(curlForCreateOrUpdateCloudantDocument);
		  curl_easy_cleanup(curlForReadCloudantDocumentField);
		  curl_easy_cleanup(curlForDeleteCloudantDocument);
		  curl_easy_cleanup(curlForGetAllDocsFromCloudantDatabase);
		  curl_easy_cleanup(curlForRunDataStoreCommand);
		  // Do the cURL global cleanup.
		  curl_global_cleanup();
	  }
  }
        
  void CloudantDBLayer::connectToDatabase(std::set<std::string> const & dbServers, PersistenceError & dbError)
  {
	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase", "CloudantDBLayer");

	  // Get the name, OS version and CPU type of this machine.
	  struct utsname machineDetails;

	  if(uname(&machineDetails) < 0) {
		  dbError.set("Unable to get the machine/os/cpu details.", DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to get the machine/os/cpu details. " << DPS_INITIALIZE_ERROR, "CloudantDBLayer");
		  return;
	  } else {
		  nameOfThisMachine = string(machineDetails.nodename);
		  osVersionOfThisMachine = string(machineDetails.sysname) + string(" ") + string(machineDetails.release);
		  cpuTypeOfThisMachine = string(machineDetails.machine);
	  }

	  string cloudantConnectionErrorMsg = "";
	  CURLcode result = curl_global_init(CURL_GLOBAL_ALL);

	  if (result != CURLE_OK) {
		  cloudantConnectionErrorMsg = "cURL global init failed.";
		  dbError.set(cloudantConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed  with a cURL error code=" << result <<
		     ", Error Msg='" << string(curl_easy_strerror(result)) << "'. " << DPS_INITIALIZE_ERROR, "CloudantDBLayer");
		  return;
	  }

	  // Create all the cURL handles we will be using during the lifetime of this dps instance.
	  // This way, we will create all the cURL handles only once and reuse the handles as long as we want.
	  curlForCreateCloudantDatabase = curl_easy_init();

	  if (curlForCreateCloudantDatabase == NULL) {
		  curl_global_cleanup();
		  cloudantConnectionErrorMsg = "cURL easy init failed for CreateCloudantDatabase.";
		  dbError.set(cloudantConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for CreateCloudantDatabase. " <<
		     DPS_INITIALIZE_ERROR, "CloudantDBLayer");
		  return;
	  }

	  curlForDeleteCloudantDatabase = curl_easy_init();

	  if (curlForDeleteCloudantDatabase == NULL) {
		  curl_easy_cleanup(curlForCreateCloudantDatabase);
		  curl_global_cleanup();
		  cloudantConnectionErrorMsg = "cURL easy init failed for DeleteCloudantDatabase.";
		  dbError.set(cloudantConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for DeleteCloudantDatabase. " <<
		     DPS_INITIALIZE_ERROR, "CloudantDBLayer");
		  return;
	  }

	  curlForCreateOrUpdateCloudantDocument = curl_easy_init();

	  if (curlForCreateOrUpdateCloudantDocument == NULL) {
		  curl_easy_cleanup(curlForCreateCloudantDatabase);
		  curl_easy_cleanup(curlForDeleteCloudantDatabase);
		  curl_global_cleanup();
		  cloudantConnectionErrorMsg = "cURL easy init failed for CreateOrUpdateCloudantDocument.";
		  dbError.set(cloudantConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for CreateOrUpdateCloudantDocument. " <<
		     DPS_INITIALIZE_ERROR, "CloudantDBLayer");
		  return;
	  }

	  curlForReadCloudantDocumentField = curl_easy_init();

	  if (curlForReadCloudantDocumentField == NULL) {
		  curl_easy_cleanup(curlForCreateCloudantDatabase);
		  curl_easy_cleanup(curlForDeleteCloudantDatabase);
		  curl_easy_cleanup(curlForCreateOrUpdateCloudantDocument);
		  curl_global_cleanup();
		  cloudantConnectionErrorMsg = "cURL easy init failed for ReadCloudantDocumentField.";
		  dbError.set(cloudantConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for ReadCloudantDocumentField. " <<
		     DPS_INITIALIZE_ERROR, "CloudantDBLayer");
		  return;
	  }

	  curlForDeleteCloudantDocument = curl_easy_init();

	  if (curlForDeleteCloudantDocument == NULL) {
		  curl_easy_cleanup(curlForCreateCloudantDatabase);
		  curl_easy_cleanup(curlForDeleteCloudantDatabase);
		  curl_easy_cleanup(curlForCreateOrUpdateCloudantDocument);
		  curl_easy_cleanup(curlForReadCloudantDocumentField);
		  curl_global_cleanup();
		  cloudantConnectionErrorMsg = "cURL easy init failed for DeleteCloudantDocument.";
		  dbError.set(cloudantConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for DeleteCloudantDocument. " <<
		     DPS_INITIALIZE_ERROR, "CloudantDBLayer");
		  return;
	  }

	  curlForGetAllDocsFromCloudantDatabase = curl_easy_init();

	  if (curlForGetAllDocsFromCloudantDatabase == NULL) {
		  curl_easy_cleanup(curlForCreateCloudantDatabase);
		  curl_easy_cleanup(curlForDeleteCloudantDatabase);
		  curl_easy_cleanup(curlForCreateOrUpdateCloudantDocument);
		  curl_easy_cleanup(curlForReadCloudantDocumentField);
		  curl_easy_cleanup(curlForDeleteCloudantDocument);
		  curl_global_cleanup();
		  cloudantConnectionErrorMsg = "cURL easy init failed for GetAllDocsFromCloudantDatabase.";
		  dbError.set(cloudantConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for GetAllDocsFromCloudantDatabase. " <<
		     DPS_INITIALIZE_ERROR, "CloudantDBLayer");
		  return;
	  }

	  curlForRunDataStoreCommand = curl_easy_init();

	  if (curlForRunDataStoreCommand == NULL) {
		  curl_easy_cleanup(curlForCreateCloudantDatabase);
		  curl_easy_cleanup(curlForDeleteCloudantDatabase);
		  curl_easy_cleanup(curlForCreateOrUpdateCloudantDocument);
		  curl_easy_cleanup(curlForReadCloudantDocumentField);
		  curl_easy_cleanup(curlForDeleteCloudantDocument);
		  curl_easy_cleanup(curlForGetAllDocsFromCloudantDatabase);
		  curl_global_cleanup();
		  cloudantConnectionErrorMsg = "cURL easy init failed for curlForRunDataStoreCommand.";
		  dbError.set(cloudantConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for curlForRunDataStoreCommand. " <<
		     DPS_INITIALIZE_ERROR, "CloudantDBLayer");
		  return;
	  }

	  headersForCreateCloudantDatabase = curl_slist_append(headersForCreateCloudantDatabase, "Accept: application/json");
	  headersForCreateCloudantDatabase = curl_slist_append(headersForCreateCloudantDatabase, "Content-Type: application/json");
	  headersForDeleteCloudantDatabase = curl_slist_append(headersForDeleteCloudantDatabase, "Accept: application/json");
	  headersForDeleteCloudantDatabase = curl_slist_append(headersForDeleteCloudantDatabase, "Content-Type: application/json");
	  headersForCreateOrUpdateCloudantDocument = curl_slist_append(headersForCreateOrUpdateCloudantDocument, "Accept: application/json");
	  headersForCreateOrUpdateCloudantDocument = curl_slist_append(headersForCreateOrUpdateCloudantDocument, "Content-Type: application/json");
	  headersForReadCloudantDocumentField = curl_slist_append(headersForReadCloudantDocumentField, "Accept: application/json");
	  headersForReadCloudantDocumentField = curl_slist_append(headersForReadCloudantDocumentField, "Content-Type: application/json");
	  headersForDeleteCloudantDocument = curl_slist_append(headersForDeleteCloudantDocument, "Accept: application/json");
	  headersForDeleteCloudantDocument = curl_slist_append(headersForDeleteCloudantDocument, "Content-Type: application/json");
	  headersForGetAllDocsFromCloudantDatabase = curl_slist_append(headersForGetAllDocsFromCloudantDatabase, "Accept: application/json");
	  headersForGetAllDocsFromCloudantDatabase = curl_slist_append(headersForGetAllDocsFromCloudantDatabase, "Content-Type: application/json");
	  headersForRunDataStoreCommand = curl_slist_append(headersForRunDataStoreCommand, "Accept: application/json");
	  headersForRunDataStoreCommand = curl_slist_append(headersForRunDataStoreCommand, "Content-Type: application/json");

	  // Set this flag so that we can release all the cURL related resources in this class' destructor method.
	  curlGlobalCleanupNeeded = true;

	  if (dbServers.size() == 0) {
		  cloudantConnectionErrorMsg = "Missing personalized Cloudant link and the required userid:password details.";
		  dbError.set(cloudantConnectionErrorMsg, DPS_MISSING_CLOUDANT_ACCESS_URL);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error '" << cloudantConnectionErrorMsg
		     << "'. " << DPS_MISSING_CLOUDANT_ACCESS_URL, "CloudantDBLayer");
		  return;
	  } else {
		  // We only need a single Cloudant URL to all our dps tasks. Let us read that here.
		  for (std::set<std::string>::iterator it=dbServers.begin(); it!=dbServers.end(); ++it) {
			  // URL format should be: http://user:password@user.cloudant.com (OR)
			  // http://user:password@XXXXX where XXXXX is a name or IP address of your
			  // on-premises Cloudant Local load balancer machine.
			  cloudantBaseUrl = *it;

			  // If the URL doesn't end with a forward slash, add one now.
			  if (cloudantBaseUrl.at(cloudantBaseUrl.length()-1) != '/') {
				  cloudantBaseUrl += string("/");
			  }

			  // A single Cloudant URL is enough.
			  break;
		  }
	  }

	  // We have now initialized the cURL layer and also obtained the user configured Cloudant URL.
	  // Let us go ahead and create a database for storing the DPS_DL_META_DATA information.
	  // This is the meta data DB where we will keep all the information about the store GUIDs,
	  // generic lock, store lock, and distributed locks.
	  // This will act as a catalog for all the active stores and locks in DPS.
	  // Form the DB URL.
	  //
	  // Two or more PEs (processes) could try to create this database exactly at the same time and
	  // get into Cloudant conflict errors. Let us avoid that by waiting for random amount of time before
	  // trying to create the database successfully in 5 consecutive attempts.
	  string dbUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB);
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  int32_t attemptCnt = 1;

	  // Seed the random number generator for every PE using its unique PE number.
	  int32_t seedValue = (int32_t)SPL::Functions::Utility::jobID() + (int32_t)SPL::Functions::Utility::PEID();
	  SPL::Functions::Math::srand(seedValue);

	  // Every PE will make 5 consecutive attempts to create the meta data DB (if it doesn't already exist) before giving up.
	  while(attemptCnt++ <= 5) {
		  // Do a random wait before trying to create it.
		  // This will give random value between 0 and 1.
		  float64 rand = SPL::Functions::Math::random();
		  // Let us wait for that random duration which is a partial second i.e. less than a second.
		  SPL::Functions::Utility::block(rand);

		  bool cloudantResult =  createCloudantDatabase(dbUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		  // We will tolerate errors within our maximum attempt counts.
		  if (attemptCnt == 5 && cloudantResult == false && curlReturnCode == -1) {
			  // cURL initialization problems.
			  dbError.set("Unable to easy initialize cURL. Error=" + curlErrorString, DPS_CONNECTION_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to easy initialize cURL. Error Msg=" <<
			     curlErrorString << ". " << DPS_CONNECTION_ERROR, "CloudantDBLayer");
			  return;
		  } else if (attemptCnt == 5 && cloudantResult == false && curlReturnCode > 0) {
			  // Other cURL errors.
			  ostringstream curlErrorCode;
			  curlErrorCode << curlReturnCode;
			  dbError.set("Unable to create a new Cloudant database named " + string(DPS_DL_META_DATA_DB) +
				  ". cURL Error code=" + curlErrorCode.str() + ", cURL Error msg=" +
				  curlErrorString + ".", DPS_CONNECTION_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to create a new Cloudant database named " <<
			     DPS_DL_META_DATA_DB << ". cURL Error code=" << curlErrorCode.str() << ", cURL Error msg="
		         << curlErrorString << ". DPS Error code=" << DPS_CONNECTION_ERROR, "CloudantDBLayer");
			  return;
		  } else if (attemptCnt == 5 && cloudantResult == false && httpResponseCode > 0) {
			  ostringstream httpErrorCode;
			  httpErrorCode << httpResponseCode;
			  dbError.set("Unable to create a new Cloudant database named " + string(DPS_DL_META_DATA_DB) +
				 ". HTTP response code=" + httpErrorCode.str() + ", HTTP Error msg=" + httpReasonString, DPS_CONNECTION_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to create a new Cloudant database named " <<
			     DPS_DL_META_DATA_DB << ". HTTP response code=" << httpErrorCode.str() <<
			     ", HTTP Error msg=" << httpReasonString << ". DPS Error code=" << DPS_CONNECTION_ERROR, "CloudantDBLayer");
			  return;
		  }

		  if (cloudantResult == true) {
			  // Database creation either succeeded or the database already exists.
			  break;
		  }
	  } // End of while loop.

	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase done", "CloudantDBLayer");
  }

  uint64_t CloudantDBLayer::createStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside createStore for store " << name, "CloudantDBLayer");

	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);

 	// Get a general purpose lock so that only one thread can
	// enter inside of this method at any given time with the same store name.
 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
 		// Unable to acquire the general purpose lock.
 		dbError.set("Unable to get a generic lock for creating a store with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for an yet to be created store with its name as " <<
 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "CloudantDBLayer");
 		// User has to retry again to create this store.
 		return (0);
 	}

    // Let us first see if a store with the given name already exists in the Cloudant DPS_DL meta data DB.
 	uint64_t storeId = findStore(name, dbError);

 	if (storeId > 0) {
		// This store already exists in our cache.
		// We can't create another one with the same name now.
 		std::ostringstream storeIdString;
 		storeIdString << storeId;
 		// The following error message carries the existing store's id at the very end of the message.
		dbError.set("A store named " + name + " already exists with a store id " + storeIdString.str(), DPS_STORE_EXISTS);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed while trying to create a duplicate store " << name << ". " << DPS_STORE_EXISTS, "CloudantDBLayer");
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
	We secured a guid. We can now create this store. Layout for a distributed process store (dps) looks like this in Cloudant.
	In the Cloudant DB, DPS store/lock, and the individual store details will follow these data formats.
	That will allow us to be as close to the other three DPS implementations using memcached, Redis and Cassandra.
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

 	// We will do the first two activities from the list above for our Cloudant implementation.
	// Let us now do the step 1 described in the commentary above.
	// 1) Create the Store Name entry in the dps_dl_meta_data DB.
	//    '0' + 'store name' => 'store id'
	std::ostringstream storeIdString;
	storeIdString << storeId;

	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
	string dpsAndDlGuidKeyUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + dpsAndDlGuidKey;
	// Form the JSON payload as a literal string.
	string jsonDoc = "{\"_id\": \"" + dpsAndDlGuidKey + "\", \"" + dpsAndDlGuidKey + "\": \"" + storeIdString.str() + "\"}";
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";

	// If "_rev" JSON field is not present, then it will create a new document. Else, it will update an existing document.
	bool cloudantResult =  createOrUpdateCloudantDocument(dpsAndDlGuidKeyUrl, jsonDoc, curlReturnCode,
			curlErrorString, httpResponseCode, httpReasonString);
	string errorMsg = "";

	if (cloudantResult == false) {
		// There was an error in creating the store name entry in the meta data table.
		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to create 'StoreName-->GUID' in Cloudant for the store named " + name + ". " + errorMsg, DPS_STORE_NAME_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'StoreName-->GUID' in Cloudant for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_NAME_CREATION_ERROR, "CloudantDBLayer");
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

	// 2) Create the Store Contents database now.
 	// This is the Cloudant DB in which we will keep storing all the key value pairs belonging to this store.
	// DB name: 'dps_' + '1_' + 'store id'
	// Before we can do anything, we need a Cloudant DB for this store. Let us create one now.
 	string storeDbName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString.str();
 	string storeDbUrl = cloudantBaseUrl + storeDbName;
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";
 	cloudantResult =  createCloudantDatabase(storeDbUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

 	if (cloudantResult == false) {
 		// Error in creating the store contents DB in Cloudant.
 		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to create a DB in Cloudant for the store named " + name + ". " + errorMsg, DPS_STORE_DB_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store DB creation [" << name << "]. Error=" <<
			errorMsg << ". " << DPS_STORE_DB_CREATION_ERROR, "CloudantDBLayer");
		// Let us remove the Store Name entry we already made in the dps_dl_meta_data DB.
		deleteCloudantDocument(dpsAndDlGuidKeyUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
 	}

	// Every store contents DB will always have these three metadata documents:
	// dps_name_of_this_store ==> 'store name'
	// dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	// dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	//
	// Every store contents DB will have at least three documents in it carrying the actual store name, key spl type name and value spl type name.
	// In addition, inside this store contents DB, we will house the
	// actual key:value data item documents that user wants to keep in this store.
	// Such a store contents DB is very useful for data item read, write, deletion, enumeration etc.
 	//

	// Let us populate the new store contents DB with a mandatory element that will carry the name of this store.
	// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
 	string metaData1Url = cloudantBaseUrl + storeDbName + "/" + string(CLOUDANT_STORE_ID_TO_STORE_NAME_KEY);
	// Form the JSON payload as a literal string.
	jsonDoc = "{\"_id\": \"" + string(CLOUDANT_STORE_ID_TO_STORE_NAME_KEY) +
		"\", \"" + string(CLOUDANT_STORE_ID_TO_STORE_NAME_KEY) + "\": \"" + base64_encoded_name + "\"}";
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";

	cloudantResult =  createOrUpdateCloudantDocument(metaData1Url, jsonDoc, curlReturnCode,
		curlErrorString, httpResponseCode, httpReasonString);

	if (cloudantResult == false) {
		// There was an error in creating Meta Data 1;
		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to create 'Meta Data1' in Cloudant for the store named " + name + ". " + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'Meta Data1' in Cloudant for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "CloudantDBLayer");
		// Since there is an error here, let us delete the store name entry we created above.
		deleteCloudantDocument(dpsAndDlGuidKeyUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
		// Let us also delete the store database we created above.
		deleteCloudantDatabase(storeDbUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

    // We are now going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
	// Add the key spl type name metadata.
 	string metaData2Url = cloudantBaseUrl + storeDbName + "/" + string(CLOUDANT_SPL_TYPE_NAME_OF_KEY);
	// Form the JSON payload as a literal string.
 	string base64_encoded_keySplTypeName;
 	base64_encode(keySplTypeName, base64_encoded_keySplTypeName);

	jsonDoc = "{\"_id\": \"" + string(CLOUDANT_SPL_TYPE_NAME_OF_KEY) +
		"\", \"" + string(CLOUDANT_SPL_TYPE_NAME_OF_KEY) + "\": \"" + base64_encoded_keySplTypeName + "\"}";
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";

	cloudantResult =  createOrUpdateCloudantDocument(metaData2Url, jsonDoc, curlReturnCode,
		curlErrorString, httpResponseCode, httpReasonString);

	if (cloudantResult == false) {
		// There was an error in creating Meta Data 2;
		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to create 'Meta Data2' in Cloudant for the store named " + name + ". " + errorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'Meta Data2' in Cloudant for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "CloudantDBLayer");
		// Since there is an error here, let us delete the store name entry we created above.
		deleteCloudantDocument(dpsAndDlGuidKeyUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
		// Let us also delete the store database we created above.
		deleteCloudantDatabase(storeDbUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

	// Add the value spl type name metadata.
 	string metaData3Url = cloudantBaseUrl + storeDbName + "/" + string(CLOUDANT_SPL_TYPE_NAME_OF_VALUE);
 	string base64_encoded_valueSplTypeName;
 	base64_encode(valueSplTypeName, base64_encoded_valueSplTypeName);
	// Form the JSON payload as a literal string.
	jsonDoc = "{\"_id\": \"" + string(CLOUDANT_SPL_TYPE_NAME_OF_VALUE) +
		"\", \"" + string(CLOUDANT_SPL_TYPE_NAME_OF_VALUE) + "\": \"" + base64_encoded_valueSplTypeName + "\"}";
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";

	cloudantResult =  createOrUpdateCloudantDocument(metaData3Url, jsonDoc, curlReturnCode,
		curlErrorString, httpResponseCode, httpReasonString);

	if (cloudantResult == false) {
		// There was an error in creating Meta Data 3;
		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to create 'Meta Data3' in Cloudant for the store named " + name + ". " + errorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'Meta Data3' in Cloudant for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "CloudantDBLayer");
		// Since there is an error here, let us delete the store name entry we created above.
		deleteCloudantDocument(dpsAndDlGuidKeyUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
		// Let us also delete the store database we created above.
		deleteCloudantDatabase(storeDbUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	} else {
		// We created a new store as requested by the caller. Return the store id.
		releaseGeneralPurposeLock(base64_encoded_name);
		return(storeId);
	}
  }

  uint64_t CloudantDBLayer::createOrGetStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	// We will rely on a method above this and another method below this to accomplish what is needed here.
	SPLAPPTRC(L_DEBUG, "Inside createOrGetStore for store " << name, "CloudantDBLayer");
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
	//  Cassandra and Cloudant DPS implementations. For memcached and Redis, we do it differently.)
	dbError.reset();
	// We will take the hashCode of the encoded store name and use that as the store id.
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);
	storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);
	return(storeId);
  }
                
  uint64_t CloudantDBLayer::findStore(std::string const & name, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside findStore for store " << name, "CloudantDBLayer");

	// We can search in the dps_dl_meta_data DB to see if a store exists for the given store name.
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);
	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
	string dpsAndDlGuidKeyUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + dpsAndDlGuidKey;
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";
	string value = "";
	string revision = "";
	bool cloudantResult = readCloudantDocumentField(dpsAndDlGuidKeyUrl, dpsAndDlGuidKey, value, revision,
		curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
		// This store already exists in our cache.
 	 	// We will take the hashCode of the encoded store name and use that as the store id.
 	 	uint64_t storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);
 	 	return(storeId);
	} else if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_NOT_FOUND) {
		// Requested data item is not there in the cache.
		dbError.set("The requested store " + name + " doesn't exist.", DPS_DATA_ITEM_READ_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed for store " << name << ". " << DPS_DATA_ITEM_READ_ERROR, "CloudantDBLayer");
		return(0);
	} else {
		// Some other HTTP error.
		// Problem in finding the existence of a store.
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to find the existence of a store named " + name + ". " + errorMsg, DPS_STORE_EXISTENCE_CHECK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed to find the existence of the store " << name << ". "
			<< errorMsg << ". " << DPS_STORE_EXISTENCE_CHECK_ERROR, "CloudantDBLayer");
		return(0);
	}
  }
        
  bool CloudantDBLayer::removeStore(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside removeStore for store id " << store, "CloudantDBLayer");

	ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CloudantDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "CloudantDBLayer");
		// User has to retry again to remove the store.
		return(false);
	}

	// Get rid of these two entries that are holding the store contents DB and the store name root entry.
	// 1) Store Contents database
	// 2) Store Name root entry
	//
	uint32_t dataItemCnt = 0;
	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		releaseStoreLock(storeIdString);
		// This is alarming. This will put this store in a bad state. Poor user has to deal with it.
		return(false);
	}

	// Let us delete the Store Contents DB that contains all the active data items in this store.
	// DB name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
 	string storeDbName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
 	string storeDbUrl = cloudantBaseUrl + storeDbName;
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";
	deleteCloudantDatabase(storeDbUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + storeName;
	string dpsAndDlGuidKeyUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + dpsAndDlGuidKey;
	// Finally, delete the StoreName key now.
	deleteCloudantDocument(dpsAndDlGuidKeyUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
	// Life of this store ended completely with no trace left behind.
	releaseStoreLock(storeIdString);
    return(true);
  }

  // This is a lean and mean put operation into a store.
  // It doesn't do any safety checks before putting a data item into a store.
  // If you want to go through that rigor, please use the putSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool CloudantDBLayer::put(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside put for store id " << store, "CloudantDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In our Cloudant dps implementation, data item keys can have space characters.
	string base64_data_item_key;
	base64_encode(string(keyData, keySize), base64_data_item_key);
	// In Cloudant, we can't store binary data since it supports data exchange only via JSON strings.
	// Hence, we must convert the valueData in our K/V pair to a string.
	// In this Cloudant CPP file, we added a new b64_encode and b64_decode methods specifically for this purpose.
	// Let us go ahead and convert the binary data buffer into a string.
	string b64_data_item_value = "";
	b64_encode(valueData, valueSize, b64_data_item_value);

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Contents DB that takes the following name.
	// DB name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeDbName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string docUrl = cloudantBaseUrl + storeDbName + "/" + base64_data_item_key;
	// Cloudant doesn't allow put operation when a data item already exists in the DB. Hence, we have to first see if a K/V pair exists in the DB.
	// If it is there, then we have to get it first from the Cloudant DB along with "_rev" JSON field and then put it back with the current revision number.
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";
	string value = "";
	string revision = "";
	string jsonDoc = "";

	// Read this K/V pair if it exists.
	bool cloudantResult = readCloudantDocumentField(docUrl, base64_data_item_key, value, revision,
		curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
		// K/V pair already exists. That means, we have to put it with the _rev field in the JSON doc.
		jsonDoc = "{\"_id\": \"" + base64_data_item_key +
			"\", \"" + base64_data_item_key + "\": \"" + b64_data_item_value + "\", \"_rev\": \"" + revision + "\"}";
	} else if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_NOT_FOUND) {
		// There is no existing K/V pair in the DB. We will add it for the first time.
		jsonDoc = "{\"_id\": \"" + base64_data_item_key +
			"\", \"" + base64_data_item_key + "\": \"" + b64_data_item_value + "\"}";
	} else {
		// Some other error.
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to check if data item exists before put. " + errorMsg, DPS_KEY_EXISTENCE_CHECK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed to check if data item exists. " << errorMsg <<
			". " << DPS_KEY_EXISTENCE_CHECK_ERROR, "CloudantDBLayer");
		return(false);
	}

	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";

	// If "_rev" JSON field is not present, then it will create a new document. Else, it will update an existing document.
	cloudantResult =  createOrUpdateCloudantDocument(docUrl, jsonDoc, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	if (cloudantResult == true && (httpResponseCode == CLOUDANT_DOC_CREATED || httpResponseCode == CLOUDANT_DOC_ACCEPTED_FOR_WRITING)) {
		// We stored the data item in the DB.
		return(true);
	} else {
		// There is some error in storing the data item.
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to store a data item in the store id " + storeIdString + ". " + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed to store a data item in the store id " << storeIdString << ". "
			<< errorMsg << ". " << DPS_DATA_ITEM_WRITE_ERROR, "CloudantDBLayer");
		return(false);
	}
  }

  // This is a special bullet proof version that does several safety checks before putting a data item into a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular put method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool CloudantDBLayer::putSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside putSafe for store id " << store, "CloudantDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to find a store with a store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CloudantDBLayer");
		}

		return(false);
	}

	// In our Cloudant dps implementation, data item keys can have space characters.
	string base64_data_item_key;
	base64_encode(string(keyData, keySize), base64_data_item_key);
	// In Cloudant, we can't store binary data since it supports data exchange only via JSON strings.
	// Hence, we must convert the valueData in our K/V pair to a string.
	// In this Cloudant CPP file, we added a new b64_encode and b64_decode methods specifically for this purpose.
	// Let us go ahead and convert the binary data buffer into a string.
	string b64_data_item_value = "";
	b64_encode(valueData, valueSize, b64_data_item_value);

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "CloudantDBLayer");
		// User has to retry again to put the data item in the store.
		return(false);
	}

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Contents DB that takes the following name.
	// DB name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeDbName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string docUrl = cloudantBaseUrl + storeDbName + "/" + base64_data_item_key;
	// Cloudant doesn't allow put operation when a data item already exists in the DB. Hence, we have to first see if a K/V pair exists in the DB.
	// If it is there, then we have to get it first from the Cloudant DB along with _rev and then put it back with the current revision number.
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";
	string value = "";
	string revision = "";
	string jsonDoc = "";

	// Read this K/V pair if it exists.
	bool cloudantResult = readCloudantDocumentField(docUrl, base64_data_item_key, value, revision,
		curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
		// K/V pair already exists. That means, we have to put it with the _rev field in the JSON doc.
		jsonDoc = "{\"_id\": \"" + base64_data_item_key +
			"\", \"" + base64_data_item_key + "\": \"" + b64_data_item_value + "\", \"_rev\": \"" + revision + "\"}";
	} else if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_NOT_FOUND) {
		// There is no existing K/V pair in the DB. We will add it for the first time.
		jsonDoc = "{\"_id\": \"" + base64_data_item_key +
			"\", \"" + base64_data_item_key + "\": \"" + b64_data_item_value + "\"}";
	} else {
		// Some other error.
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to check if data item exists before put. " + errorMsg, DPS_KEY_EXISTENCE_CHECK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to check if data item exists. " << errorMsg <<
			". " << DPS_KEY_EXISTENCE_CHECK_ERROR, "CloudantDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";

	// If "_rev" JSON field is not present, then it will create a new document. Else, it will update an existing document.
	cloudantResult =  createOrUpdateCloudantDocument(docUrl, jsonDoc, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	if (cloudantResult == true && (httpResponseCode == CLOUDANT_DOC_CREATED || httpResponseCode == CLOUDANT_DOC_ACCEPTED_FOR_WRITING)) {
		// We stored the data item in the DB.
		releaseStoreLock(storeIdString);
		return(true);
	} else {
		// There is some error in storing the data item.
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to store a data item in the store id " + storeIdString + ". " + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to store a data item in the store id " << storeIdString << ". "
			<< errorMsg << ". " << DPS_DATA_ITEM_WRITE_ERROR, "CloudantDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}
  }

  // Put a data item with a TTL (Time To Live in seconds) value into the global area of the Cloudant DB.
  bool CloudantDBLayer::putTTL(char const * keyData, uint32_t keySize,
		  	  	  	  	    unsigned char const * valueData, uint32_t valueSize,
							uint32_t ttl, PersistenceError & dbError, bool encodeKey, bool encodeValue)
  {
	  SPLAPPTRC(L_DEBUG, "Inside putTTL.", "CloudantDBLayer");
	  // This API can only be supported in NoSQL data stores such as Memcached, Redis, Cassandra, etc.
	  // Cloudant doesn't have a way to do this, because there is no TTL facility available.
	  dbError.setTTL("From Cloudant data store: This API requires TTL which is not supported in Cloudant.", DPS_TTL_NOT_SUPPORTED_ERROR);
	  SPLAPPTRC(L_DEBUG, "From Cloudant data store: This API requires TTL which is not supported in Cloudant. " << DPS_TTL_NOT_SUPPORTED_ERROR, "CloudantDBLayer");
	  return(false);
  }

  // This is a lean and mean get operation from a store.
  // It doesn't do any safety checks before getting a data item from a store.
  // If you want to go through that rigor, please use the getSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool CloudantDBLayer::get(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside get for store id " << store, "CloudantDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
	// Every data item's key must be prefixed with its store id before being used for CRUD operation in the Cloudant store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In our Cloudant dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);

	bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		false, true, valueData, valueSize, dbError);

	if ((result == false) || (dbError.hasError() == true)) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside get, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
	}

    return(result);
  }

  // This is a special bullet proof version that does several safety checks before getting a data item from a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular get method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool CloudantDBLayer::getSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside getSafe for store id " << store, "CloudantDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
	// Every data item's key must be prefixed with its store id before being used for CRUD operation in the Cloudant store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CloudantDBLayer");
		}

		return(false);
	}

	// In our Cloudant dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);

	bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		false, false, valueData, valueSize, dbError);

	if ((result == false) || (dbError.hasError() == true)) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
	}

    return(result);
  }

  // Get a TTL based data item that is stored in the global area of the Cloudant DB.
   bool CloudantDBLayer::getTTL(char const * keyData, uint32_t keySize,
                              unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError, bool encodeKey)
   {
		SPLAPPTRC(L_DEBUG, "Inside getTTL.", "CloudantDBLayer");

		// This API can only be supported in NoSQL data stores such as Memcached, Redis, Cassandra, etc.
		// Cloudant doesn't have a way to do this, because there is no TTL facility available.
		dbError.setTTL("From Cloudant data store: This API requires TTL which is not supported in Cloudant.", DPS_TTL_NOT_SUPPORTED_ERROR);
		SPLAPPTRC(L_DEBUG, "From Cloudant data store: This API requires TTL which is not supported in Cloudant. " << DPS_TTL_NOT_SUPPORTED_ERROR, "CloudantDBLayer");
		return(false);
   }

  bool CloudantDBLayer::remove(uint64_t store, char const * keyData, uint32_t keySize,
                                PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside remove for store id " << store, "CloudantDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CloudantDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "CloudantDBLayer");
		// User has to retry again to remove the data item from the store.
		return(false);
	}

	// In our Cloudant dps implementation, data item keys can have space characters.
	string base64_data_item_key;
	base64_encode(string(keyData, keySize), base64_data_item_key);
	// We are ready to remove a data item from the DB.
	// This action is performed on the Store Contents DB that takes the following name.
	// DB name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeDbName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string docUrl = cloudantBaseUrl + storeDbName + "/" + base64_data_item_key;
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";
	bool cloudantResult = deleteCloudantDocument(docUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	if ((cloudantResult == false) || ((cloudantResult == true) && (httpResponseCode == CLOUDANT_DOC_NOT_FOUND))) {
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Cloudant error while removing the requested data item from the store id " + storeIdString +
			". " + errorMsg, DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed with Cloudant reply error for store id " << storeIdString <<
			". " << errorMsg << ". " << DPS_DATA_ITEM_DELETE_ERROR, "CloudantDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// All done. An existing data item in the given store has been removed.
	releaseStoreLock(storeIdString);
    return(true);
  }

  // Remove a TTL based data item that is stored in the global area of the Cloudant DB.
  bool CloudantDBLayer::removeTTL(char const * keyData, uint32_t keySize,
                                PersistenceError & dbError, bool encodeKey)
  {
		SPLAPPTRC(L_DEBUG, "Inside removeTTL.", "CloudantDBLayer");

		// This API can only be supported in NoSQL data stores such as Memcached, Redis, Cassandra, etc.
		// Cloudant doesn't have a way to do this, because there is no TTL facility available.
		dbError.setTTL("From Cloudant data store: This API requires TTL which is not supported in Cloudant.", DPS_TTL_NOT_SUPPORTED_ERROR);
		SPLAPPTRC(L_DEBUG, "From Cloudant data store: This API requires TTL which is not supported in Cloudant. " << DPS_TTL_NOT_SUPPORTED_ERROR, "CloudantDBLayer");
		return(false);
  }

  bool CloudantDBLayer::has(uint64_t store, char const * keyData, uint32_t keySize,
                             PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside has for store id " << store, "CloudantDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CloudantDBLayer");
		}

		return(false);
	}

	// In our Cloudant dps implementation, data item keys can have space characters.
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
		SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
	}

	return(dataItemAlreadyInCache);
  }

  // Check for the existence of a TTL based data item that is stored in the global area of the Cloudant DB.
  bool CloudantDBLayer::hasTTL(char const * keyData, uint32_t keySize,
                             PersistenceError & dbError, bool encodeKey)
  {
		SPLAPPTRC(L_DEBUG, "Inside hasTTL.", "CloudantDBLayer");

		// This API can only be supported in NoSQL data stores such as Memcached, Redis, Cassandra, etc.
		// Cloudant doesn't have a way to do this, because there is no TTL facility available.
		dbError.setTTL("From Cloudant data store: This API requires TTL which is not supported in Cloudant.", DPS_TTL_NOT_SUPPORTED_ERROR);
		SPLAPPTRC(L_DEBUG, "From Cloudant data store: This API requires TTL which is not supported in Cloudant. " << DPS_TTL_NOT_SUPPORTED_ERROR, "CloudantDBLayer");
		return(false);
  }

  void CloudantDBLayer::clear(uint64_t store, PersistenceError & dbError) {
     SPLAPPTRC(L_DEBUG, "Inside clear for store id " << store, "CloudantDBLayer");

 	ostringstream storeId;
 	storeId << store;
 	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CloudantDBLayer");
		}

		return;
	}

 	// Lock the store first.
 	if (acquireStoreLock(storeIdString) == false) {
 		// Unable to acquire the store lock.
 		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside clear, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "CloudantDBLayer");
 		// User has to retry again to remove the store.
 		return;
 	}

 	// Get the store name.
 	uint32_t dataItemCnt = 0;
 	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		releaseStoreLock(storeIdString);
		// This is alarming. This will put this store in a bad state. Poor user has to deal with it.
		return;
	}

 	// A very fast and quick thing to do is to simply delete the Store Contents DB and
 	// recreate it rather than removing one element at a time.
	// This action is performed on the Store Contents DB that takes the following name.
	// DB name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
 	string storeDbName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
 	string storeDbUrl = cloudantBaseUrl + storeDbName;
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";
 	bool cloudantResult = deleteCloudantDatabase(storeDbUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

 	if (cloudantResult == false || httpResponseCode == CLOUDANT_DB_NOT_FOUND) {
		// Problem in deleting the store contents DB.
 		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to delete the store database for store id " + storeIdString + ". " + errorMsg, DPS_STORE_CLEARING_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed to delete the store database for store id " <<
			storeIdString << ". " << errorMsg << ". " << DPS_STORE_CLEARING_ERROR, "CloudantDBLayer");
		releaseStoreLock(storeIdString);
		return;
 	}

 	// Create a new database for this store.
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";
 	cloudantResult =  createCloudantDatabase(storeDbUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

 	if (cloudantResult == false) {
 		// Error in creating the store contents DB in Cloudant.
 		// This is not good at all. This will leave this store in a zombie state in Cloudant.
 		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Critical error: Unable to create a DB in Cloudant for the store id " + storeIdString + ". " + errorMsg, DPS_STORE_DB_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside clear, it failed for store DB creation [" << storeIdString << "]. Error=" <<
			errorMsg << ". " << DPS_STORE_DB_CREATION_ERROR, "CloudantDBLayer");
		releaseStoreLock(storeIdString);
		return;
 	}

	// Let us populate the new store contents DB with a mandatory element that will carry the name of this store.
	// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
 	string metaData1Url = cloudantBaseUrl + storeDbName + "/" + string(CLOUDANT_STORE_ID_TO_STORE_NAME_KEY);
	// Form the JSON payload as a literal string.
	string jsonDoc = "{\"_id\": \"" + string(CLOUDANT_STORE_ID_TO_STORE_NAME_KEY) +
		"\", \"" + string(CLOUDANT_STORE_ID_TO_STORE_NAME_KEY) + "\": \"" + storeName + "\"}";
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";
	cloudantResult =  createOrUpdateCloudantDocument(metaData1Url, jsonDoc, curlReturnCode,
		curlErrorString, httpResponseCode, httpReasonString);

	if (cloudantResult == false) {
		// There was an error in creating Meta Data 1.
		// This is not good at all. This will leave this store in a zombie state in Cloudant.
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Critical error: Unable to create 'Meta Data1' in Cloudant for the store id " + storeIdString +
			". " + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside clear, it failed to create 'Meta Data1' in Cloudant for store id " << storeIdString <<
			". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "CloudantDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

    // We are now going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
	// Add the key spl type name metadata.
 	string metaData2Url = cloudantBaseUrl + storeDbName + "/" + string(CLOUDANT_SPL_TYPE_NAME_OF_KEY);
 	string base64_encoded_keySplTypeName;
 	base64_encode(keySplTypeName, base64_encoded_keySplTypeName);
	// Form the JSON payload as a literal string.
	jsonDoc = "{\"_id\": \"" + string(CLOUDANT_SPL_TYPE_NAME_OF_KEY) +
		"\", \"" + string(CLOUDANT_SPL_TYPE_NAME_OF_KEY) + "\": \"" + base64_encoded_keySplTypeName + "\"}";
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";
	cloudantResult =  createOrUpdateCloudantDocument(metaData2Url, jsonDoc, curlReturnCode,
		curlErrorString, httpResponseCode, httpReasonString);

	if (cloudantResult == false) {
		// There was an error in creating Meta Data 2;
		// This is not good at all. This will leave this store in a zombie state in Cloudant.
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Critical error: Unable to create 'Meta Data2' in Cloudant for the store id " +
			storeIdString + ". " + errorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside clear, it failed to create 'Meta Data2' in Cloudant for store id " << storeIdString <<
			". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "CloudantDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

	// Add the value spl type name metadata.
 	string metaData3Url = cloudantBaseUrl + storeDbName + "/" + string(CLOUDANT_SPL_TYPE_NAME_OF_VALUE);
 	string base64_encoded_valueSplTypeName;
 	base64_encode(valueSplTypeName, base64_encoded_valueSplTypeName);
	// Form the JSON payload as a literal string.
	jsonDoc = "{\"_id\": \"" + string(CLOUDANT_SPL_TYPE_NAME_OF_VALUE) +
		"\", \"" + string(CLOUDANT_SPL_TYPE_NAME_OF_VALUE) + "\": \"" + base64_encoded_valueSplTypeName + "\"}";
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";
	cloudantResult =  createOrUpdateCloudantDocument(metaData3Url, jsonDoc, curlReturnCode,
		curlErrorString, httpResponseCode, httpReasonString);

	if (cloudantResult == false) {
		// There was an error in creating Meta Data 3;
		// This is not good at all. This will leave this store in a zombie state in Cloudant.
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Critical error: Unable to create 'Meta Data3' in Cloudant for the store id " +
			storeIdString + ". " + errorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside createStore, it failed to create 'Meta Data3' in Cloudant for store id " << storeIdString <<
			". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "CloudantDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

 	// If there was an error in the store contents hash recreation, then this store is going to be in a weird state.
	// It is a fatal error. If that happened, something terribly had gone wrong in clearing the contents.
	// User should look at the dbError code and decide about a corrective action.
 	releaseStoreLock(storeIdString);
  }
        
  uint64_t CloudantDBLayer::size(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside size for store id " << store, "CloudantDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside size, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside size, it failed for finding a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CloudantDBLayer");
		}

		return(false);
	}

	// Store size information is maintained as part of the store information.
	uint32_t dataItemCnt = 0;
	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside size, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		return(0);
	}

	return((uint64_t)dataItemCnt);
  }

  // We allow space characters in the data item keys.
  // Hence, it is required to based64 encode them before storing the
  // key:value data item in Cloudant.
  // (Use boost functions to do this.)
  //
  // IMPORTANT:
  // In Cloudant, every base64 encoded string may be used inside an URL or in a JSON formatted data.
  // We can't allow these two characters in the context of Cloudant because of URL format restrictions:
  // Forward slash and plus   i.e.  /  and +
  // Hence, we must replace them with ~ and - (tilde and hyphen).
  // Do the reverse of that in base64_decode.
  void CloudantDBLayer::base64_encode(std::string const & str, std::string & base64) {
	  // Insert line breaks for every 64KB characters.
	  typedef insert_linebreaks<base64_from_binary<transform_width<string::const_iterator,6,8> >, 64*1024 > it_base64_t;

	  unsigned int writePaddChars = (3-str.length()%3)%3;
	  base64 = string(it_base64_t(str.begin()),it_base64_t(str.end()));
	  base64.append(writePaddChars,'=');

	  // --- Cloudant specific change begins here ---
	  // To comply with the URL formatting rules, replace all the forward slash and plus characters if they are present.
	  if (base64.find_first_of("/") != string::npos) {
		  streams_boost::replace_all(base64, "/", "~");
	  }

	  if (base64.find_first_of("+") != string::npos) {
		  streams_boost::replace_all(base64, "+", "-");
	  }
	  // --- Cloudant specific change ends here ---
  }

  // As explained above, we based64 encoded the data item keys before adding them to the store.
  // If we need to get back the original key name, this function will help us in
  // decoding the base64 encoded key.
  // (Use boost functions to do this.)
  //
  // IMPORTANT:
  // In Cloudant, we have to comply with the URL formatting rules.
  // Hence, we did some trickery in the base64_encode method above.
  // We will do the reverse of that action here. Please read the commentary in base64_encode method.
  void CloudantDBLayer::base64_decode(std::string & base64, std::string & result) {
	  // IMPORTANT:
	  // For performance reasons, we are not passing a const string to this method.
	  // Instead, we are passing a directly modifiable reference. Caller should be aware that
	  // the string they passed to this method gets altered during the base64 decoding logic below.
	  // After this method returns back to the caller, it is not advisable to use that modified string.
	  typedef transform_width< binary_from_base64<remove_whitespace<string::const_iterator> >, 8, 6 > it_binary_t;

	  bool tildePresent = false;
	  bool hyphenPresent = false;

	  if (base64.find_first_of("~") != string::npos) {
		  tildePresent = true;
	  }

	  if (base64.find_first_of("-") != string::npos) {
		  hyphenPresent = true;
	  }

	  if (tildePresent == true || hyphenPresent == true) {
		  // --- Cloudant specific change begins here ---
		  // To comply with the URL formatting rules, we substituted / and + characters with ~ and - characters at the time of encoding.
		  // Let us do the reverse of that here if those characters are present.
		  if (tildePresent == true) {
			  streams_boost::replace_all(base64, "~", "/");
		  }

		  if (hyphenPresent == true) {
			  streams_boost::replace_all(base64, "-", "+");
		  }
		  // --- Cloudant specific change ends here ---
	  }

	  unsigned int paddChars = count(base64.begin(), base64.end(), '=');
	  std::replace(base64.begin(),base64.end(),'=','A'); // replace '=' by base64 encoding of '\0'
	  result = string(it_binary_t(base64.begin()), it_binary_t(base64.end())); // decode
	  result.erase(result.end()-paddChars,result.end());  // erase padding '\0' characters
  }

  // This method will check if a store exists for a given store id.
  bool CloudantDBLayer::storeIdExistsOrNot(string storeIdString, PersistenceError & dbError) {
	  string storeDbName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	  string key = string(CLOUDANT_STORE_ID_TO_STORE_NAME_KEY);
	  string metaData1Url = cloudantBaseUrl + storeDbName + "/" + key;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  string value = "";
	  string revision = "";
	  bool cloudantResult = readCloudantDocumentField(metaData1Url, key, value, revision,
			  curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
		  // This store already exists in our cache.
		  return(true);
	  } else {
		  // Unable to access a document in the store contents database the given store id. This is not a correct behavior.
		  dbError.set("StoreIdExistsOrNot: Unable to get StoreContents meta data1 for the StoreId " + storeIdString +
		     ".", DPS_GET_STORE_CONTENTS_HASH_ERROR);
		  return(false);
	  }
  }

  // This method will acquire a lock for a given store.
  bool CloudantDBLayer::acquireStoreLock(string const & storeIdString) {
	  int32_t retryCnt = 0;

	  // Cloudant DB doesn't have TTL (Time To Live) features like other NoSQL databases.
	  // Hence, we have to manage it ourselves to release a stale lock that the previous lock owner forgot to unlock.
	  // Cloudant also doesn't allow database names to start with a numeral digit. First letter must be an alphabet.
	  // It is going to be cumbersome. Let us deal with it.
	  //Try to get a lock for this generic entity.
	  while (1) {
		// '4' + 'store id' + 'dps_lock' => 1
		std::string storeLockKey = string(DPS_STORE_LOCK_TYPE) + storeIdString + DPS_LOCK_TOKEN;
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or we forcefully take it
		// back the forgotten stale locks.
		// Create a Cloudant DB document for this store lock in our meta data table.
		// URL format: baseURL/db-name/doc
		string docUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + storeLockKey;
		// Let us store the current time to indicate when we grabbed the lock. If we don't release the
		// lock before allowed maximum time (DPS_AND_DL_GET_LOCK_TTL), anyone else can grab it from us after that time expires.
		// (Other NoSQL DB alternatives will do this via TTL. In Cloudant, we have to do it ourselves. A weakness of Cloudant.)
		SPL::timestamp tsNow = SPL::Functions::Time::getTimestamp();
		int64_t timeInSecondsNow = SPL::Functions::Time::getSeconds(tsNow);
		std::ostringstream timeValue;
		timeValue << timeInSecondsNow;
		// Form the JSON payload as a literal string.
		string jsonDoc = "{\"_id\": \"" + storeLockKey + "\", \"" + storeLockKey + "\": \"" + timeValue.str() + "\"}";
		int32_t curlReturnCode = 0;
		string curlErrorString = "";
		uint64_t httpResponseCode = 0;
		string httpReasonString = "";

		// If "_rev" JSON field is not present, then it will create a new document. Else, it will update an existing document.
		bool cloudantResult =  createOrUpdateCloudantDocument(docUrl, jsonDoc, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		if (cloudantResult == true && (httpResponseCode == CLOUDANT_DOC_CREATED || httpResponseCode == CLOUDANT_DOC_ACCEPTED_FOR_WRITING)) {
			// We got the lock. We can return now.
			return(true);
		}

		if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_EXISTS) {
			// Someone has this lock right now. Let us check if they are well within their allowed time to own the lock.
			// If not, we can take over this lock. We can go ahead and read their lock acquisition time.
			curlReturnCode = 0;
			curlErrorString = "";
			httpResponseCode = 0;
			httpReasonString = "";
			string key = storeLockKey;
			string value = "";
			string revision = "";
			cloudantResult = readCloudantDocumentField(docUrl, key, value, revision,
				curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

			if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
				// We got the current lock owner's lock acquisition time.
				// Let us see if their time already expired for this lock.
				int64_t lockAcquiredTime = atoi(value.c_str());
				SPL::timestamp currentTimestamp = SPL::Functions::Time::getTimestamp();
				int64_t currentTime = SPL::Functions::Time::getSeconds(currentTimestamp);
				if ((currentTime - lockAcquiredTime) > DPS_AND_DL_GET_LOCK_TTL) {
					// Previous owner exceeded the time of ownership.
					// Overusing the lock or forgot to release the lock.
					// Let us take over the lock by changing the time value on this field.
					// Update the existing document's field now.
					std::ostringstream currentTimeValue;
					currentTimeValue << currentTime;
					jsonDoc = "{\"_id\": \"" + storeLockKey + "\", \"" + storeLockKey + "\": \"" + currentTimeValue.str() + "\", \"_rev\": \"" + revision + "\"}";
					curlReturnCode = 0;
					curlErrorString = "";
					httpResponseCode = 0;
					httpReasonString = "";
					cloudantResult = createOrUpdateCloudantDocument(docUrl, jsonDoc, curlReturnCode,
						curlErrorString, httpResponseCode, httpReasonString);

					if (cloudantResult == true) {
						// We got the lock.
						return(true);
					}
				}
			}
		}

		if (cloudantResult == false) {
			// Some other Cloudant HTTP API error occurred.
			return(false);
		}

		// Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {

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

	  return(false);
  }

  void CloudantDBLayer::releaseStoreLock(string const & storeIdString) {
	  // '4' + 'store id' + 'dps_lock' => 1
	  std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;
	  string docUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + storeLockKey;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  deleteCloudantDocument(docUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
  }

  bool CloudantDBLayer::readStoreInformation(std::string const & storeIdString, PersistenceError & dbError,
		  uint32_t & dataItemCnt, std::string & storeName, std::string & keySplTypeName, std::string & valueSplTypeName) {
	  // Read the store name, this store's key and value SPL type names,  and get the store size.
	  storeName = "";
	  keySplTypeName = "";
	  valueSplTypeName = "";
	  dataItemCnt = 0;
	  string storeDbName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;

	  // This action is performed on the Store Contents DB that takes the following name.
	  // 'dps_' + '1_' + 'store id'
	  // It will always have the following three metadata entries:
	  // dps_name_of_this_store ==> 'store name'
	  // dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	  // dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	  // 1) Get the store name.
	  string key = string(CLOUDANT_STORE_ID_TO_STORE_NAME_KEY);
	  string metaData1Url = cloudantBaseUrl + storeDbName + "/" + key;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  string value = "";
	  string revision = "";
	  string errorMsg = "";
	  bool cloudantResult = readCloudantDocumentField(metaData1Url, key, value, revision,
			  curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
		  storeName = value;
	  } else {
		  errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  // Unable to get the store name for this store's key.
		  dbError.set("Unable to get the store name for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_STORE_NAME_ERROR);
		  return (false);
	  }

	  // 2) Let us get the spl type name for this store's key.
	  key = string(CLOUDANT_SPL_TYPE_NAME_OF_KEY);
	  string metaData2Url = cloudantBaseUrl + storeDbName + "/" + key;
	  curlReturnCode = 0;
	  curlErrorString = "";
	  httpResponseCode = 0;
	  httpReasonString = "";
	  value = "";
	  revision = "";
	  errorMsg = "";
	  cloudantResult = readCloudantDocumentField(metaData2Url, key, value, revision,
			  curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
		  keySplTypeName = value;
	  } else {
		  errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  // Unable to get the spl type name for this store's key.
		  dbError.set("Unable to get the key spl type name for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_KEY_SPL_TYPE_NAME_ERROR);
		  return (false);
	  }

	  // 3) Let us get the spl type name for this store's value.
	  key = string(CLOUDANT_SPL_TYPE_NAME_OF_VALUE);
	  string metaData3Url = cloudantBaseUrl + storeDbName + "/" + key;
	  curlReturnCode = 0;
	  curlErrorString = "";
	  httpResponseCode = 0;
	  httpReasonString = "";
	  value = "";
	  revision = "";
	  errorMsg = "";
	  cloudantResult = readCloudantDocumentField(metaData3Url, key, value, revision,
			  curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
		  valueSplTypeName = value;
	  } else {
		  errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  // Unable to get the spl type name for this store's value.
		  dbError.set("Unable to get the value spl type name for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_VALUE_SPL_TYPE_NAME_ERROR);
		  return (false);
	  }

	  // 4) Let us get the size of the store contents db now.
	  key = "doc_count";
	  // Please note here that we are using the db URL instead of a doc URL for getting the doc_count.
	  string storeDbUrl = cloudantBaseUrl + storeDbName;
	  curlReturnCode = 0;
	  curlErrorString = "";
	  httpResponseCode = 0;
	  httpReasonString = "";
	  value = "";
	  revision = "";
	  errorMsg = "";
	  cloudantResult = readCloudantDocumentField(storeDbUrl, key, value, revision,
			  curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
		  dataItemCnt = atoi(value.c_str());
	  } else {
		  errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  // This is not correct. We must have a minimum hash size of 3 because of the reserved elements.
		  dbError.set("Wrong value (zero) observed as the store size for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_STORE_SIZE_ERROR);
		  return (false);
	  }

	  // Our Store Contents DB for every store will have a mandatory reserved internal elements (store name, key spl type name, and value spl type name).
	  // Let us not count those three elements in the actual store contents DB size that the caller wants now.
	  dataItemCnt -= 3;
	  return(true);
  }

  string CloudantDBLayer::getStoreName(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CloudantDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		  return("");
	  }

	  string base64_decoded_storeName;
	  base64_decode(storeName, base64_decoded_storeName);
	  return(base64_decoded_storeName);
  }

  string CloudantDBLayer::getSplTypeNameForKey(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CloudantDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		  return("");
	  }

	  string base64_decoded_keySplTypeName;
	  base64_decode(keySplTypeName, base64_decoded_keySplTypeName);
	  return(base64_decoded_keySplTypeName);
  }

  string CloudantDBLayer::getSplTypeNameForValue(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CloudantDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		  return("");
	  }

	  string base64_decoded_valueSplTypeName;
	  base64_decode(valueSplTypeName, base64_decoded_valueSplTypeName);
	  return(base64_decoded_valueSplTypeName);
  }

  std::string CloudantDBLayer::getNoSqlDbProductName(void) {
	  return(string(CLOUDANT_NO_SQL_DB_NAME));
  }

  void CloudantDBLayer::getDetailsAboutThisMachine(std::string & machineName, std::string & osVersion, std::string & cpuArchitecture) {
	  machineName = nameOfThisMachine;
	  osVersion = osVersionOfThisMachine;
	  cpuArchitecture = cpuTypeOfThisMachine;
  }

  bool CloudantDBLayer::runDataStoreCommand(std::string const & cmd, PersistenceError & dbError) {
		// This API can only be supported in NoSQL data stores such as Redis, Cassandra etc.
		// Cloudant doesn't have a way to do this.
		dbError.set("From Cloudant data store: This API to run native data store commands is not supported in cloudant.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Cloudant data store: This API to run native data store commands is not supported in cloudant. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CloudantDBLayer");
		return(false);
  }

  bool CloudantDBLayer::runDataStoreCommand(uint32_t const & cmdType, std::string const & httpVerb,
		std::string const & baseUrl, std::string const & apiEndpoint, std::string const & queryParams,
		std::string const & jsonRequest, std::string & jsonResponse, PersistenceError & dbError) {
	  	// If users want to execute arbitrary back-end data store two way
	  	// native commands, this API can be used. This is a variation of the previous API with
	  	// overloaded function arguments. As of Nov/2014, this API is supported in the dps toolkit only
	  	// when Cloudant NoSQL DB is used as a back-end data store. It covers any Cloudant HTTP/JSON based
	  	// native commands that can perform both database and document related Cloudant APIs that are very
	  	// well documented for reference on the web.
	    // Check the command type for its validity.
	  	if (cmdType != DPS_CLOUDANT_DB_LEVEL_COMMAND && cmdType != DPS_CLOUDANT_DOC_LEVEL_COMMAND) {
	  		std::ostringstream cmdTypeString;
	  		cmdTypeString << cmdType;
	  		// User didn't give us a proper Cloudant command type.
	  		string errorMsg = "Cloudant command type " + cmdTypeString.str() + " is not valid. " +
					"It must be either 1 (for any database level request) or 2 (for any document level request).";
			dbError.set(errorMsg, DPS_RUN_DATA_STORE_COMMAND_ERROR);
			SPLAPPTRC(L_DEBUG, errorMsg << " " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CloudantDBLayer");
			return(false);
	  	}

	  	// Validate the HTTP verb sent by the user.
	  	// We will support get, put, post, and delete (No support at this time for copy and attachments).
	  	if (httpVerb != string(HTTP_GET) && httpVerb != string(HTTP_PUT) &&
	  		httpVerb != string(HTTP_POST) && httpVerb != string(HTTP_DELETE) &&
	  		httpVerb != string(HTTP_HEAD)) {
	  		// User didn't give us a proper HTTP verb.
	  		string errorMsg = "Cloudant HTTP verb '" + httpVerb + "' is not supported.";
			dbError.set(errorMsg, DPS_RUN_DATA_STORE_COMMAND_ERROR);
			SPLAPPTRC(L_DEBUG, errorMsg << " " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CloudantDBLayer");
			return(false);
	  	}

	    string url = baseUrl;

	    // Form the full URL using the user provided command line arguments.
	  	if (url.length() <= 0) {
	  		// User gave an empty Base URL. That is okay.
	  		// In that case, we will use the URL specified in the DPS configuration file.
	  		// Exclude the trailing forward slash at the end.
	  		url = cloudantBaseUrl.substr(0, cloudantBaseUrl.length()-1);
	  	}

	    // If the base URL ends with a forward slash, raise an error now.
		if (url.at(url.length()-1) == '/') {
			// User gave a base URL that ends with a forward slash.
			dbError.set("Cloudant base URL is not valid. It ends with a forward slash.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside runDataStoreCommand: Cloudant base URL is not valid. It ends with a forward slash.. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CloudantDBLayer");
			return(false);
		}

		if (apiEndpoint.length() <= 0) {
			// User didn't give us a proper Cloudant endpoint.
			dbError.set("Cloudant API endpoint is not valid. It is empty.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside runDataStoreCommand: Cloudant API endpoint is not valid. It is empty. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CloudantDBLayer");
			return(false);
		}

		// If the API endpoint doesn't begin with a forward slash, raise an error now.
		if (apiEndpoint.at(0) != '/') {
			// User gave an API endpoint that doesn't begin with a forward slash.
			dbError.set("Cloudant API endpoint path is not valid. It doesn't begin with a forward slash.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside runDataStoreCommand: Cloudant API endpoint path is not valid. It doesn't begin with a forward slash. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CloudantDBLayer");
			return(false);
		}

		// If the API endpoint ends with a forward slash, raise an error now.
		if (apiEndpoint.at(apiEndpoint.length()-1) == '/') {
			// User gave an API endpoint that ends with a forward slash.
			dbError.set("Cloudant API endpoint path is not valid. It ends with a forward slash.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside runDataStoreCommand: Cloudant API endpoint path is not valid. It ends with a forward slash. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CloudantDBLayer");
			return(false);
		}

		// Append the Cloudant API end point.
		url += apiEndpoint;

		// Add if any query params are provided by the caller.
		if (queryParams.length() > 0) {
			// User passed query params.
			url += "?";
			url += queryParams;
		}

		int32_t curlReturnCode = 0;
		uint64_t httpResponseCode = 0;
		rstring curlErrorString = "";
		rstring httpReasonString = "";
		char *curlEffectiveUrl = NULL;
		CURLcode result;
		bool repeatingUrl = false;
		putBuffer = jsonRequest.c_str();
		long putBufferLen = jsonRequest.length();

		// Get the URL endpoint that was used when we executed this method previously.
		result = curl_easy_getinfo(curlForRunDataStoreCommand, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

		if ((result == CURLE_OK) && (httpVerbUsedInPreviousRunCommand == httpVerb) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
			// We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
			repeatingUrl = true;
		}

		if (repeatingUrl == false) {
			// It is not the same URL we are connecting to again.
			// We can't reuse the previously made cURL session. Reset it now.
			curl_easy_reset(curlForRunDataStoreCommand);
			httpVerbUsedInPreviousRunCommand = httpVerb;
			curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_URL, url.c_str());
			  // cURL is C based and we are here in C++.
			  // Hence, we have to pass a static method pointer in the CURLOPT_WRITEFUNCTION and then pass a custom
			  // object reference (this) of our current C++ class in the CURLOPT_WRITEDATA so that cURL will pass that
			  // object pointer as the 4th argument to our static writeFunction during its callback. By passing that object
			  // pointer as an argument to a static method, we can let the static method access our
			  // non-static member functions and non-static member variables via that object pointer.
			curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_WRITEDATA, this);
			curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_WRITEFUNCTION, &CloudantDBLayer::writeFunction);

			if (httpVerb == string(HTTP_GET)) {
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_HTTPGET, 1);
			}

			if (cmdType == DPS_CLOUDANT_DB_LEVEL_COMMAND && httpVerb == string(HTTP_PUT)) {
				// For a db level PUT command, using CURLOPT_PUT here hangs at times.
				// Hence, I'm using CUSTOMREQUEST PUT which seems to work.
				// In the DB level command, there is no JSON data that needs to be sent to the server.
				// Everything needed for a DB level command should be in the URL itself.
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_CUSTOMREQUEST, HTTP_PUT);
			}

			if (cmdType == DPS_CLOUDANT_DOC_LEVEL_COMMAND && httpVerb == string(HTTP_PUT)) {
				// CURLOPT_PUT works fine for document level commands. (But, not for DB level commands. See the previous if block.)
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_PUT, 1);
				// Do the same here for CURLOPT_READDATA and CURLOPT_READFUNCTION as we did above for the CURL_WRITEDATA.
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_READDATA, this);
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_READFUNCTION, &CloudantDBLayer::readFunction);
				// Put content length i.e. CURLOPT_INFILESIZE must be set outside of this repeat URL check if block.
			}

			if (httpVerb == string(HTTP_POST)) {
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_POST, 1);
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_POSTFIELDS, putBuffer);
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_POSTFIELDSIZE, putBufferLen);
			}

			if (httpVerb == string(HTTP_DELETE)) {
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_CUSTOMREQUEST, HTTP_DELETE);
			}

			if (httpVerb == string(HTTP_HEAD)) {
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_HEADER, 1);
				// We don't need the following two statements. Since we already have write function define above to
				// reroute anything that is returned by the Curl call to our callback.
				// We need the following two statement only in the absence of a write function. When we have
				// both write function and the following two callbacks, then we will double dip and display
				// every header twice. That is unnecessary.
				//
				// curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_HEADERDATA, this);
				// curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_HEADERFUNCTION, &CloudantDBLayer::writeFunction);
			}

			// Enable the TCP keep alive so that we don't lose the connection with the
			// Cloudant server when no store functions are performed for long durations.
			curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_TCP_KEEPALIVE, 1);
			curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_HTTPHEADER, headersForRunDataStoreCommand);
		}

		// This must be done every time whether it is a repeating URL or a new URL.
		// Because, JSON document content may change across different invocations of this C++ method of ours.
		if (cmdType == DPS_CLOUDANT_DOC_LEVEL_COMMAND && httpVerb == string(HTTP_PUT)) {
			curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_INFILESIZE, putBufferLen);
		}

		curlBufferOffset = 0;
		result = curl_easy_perform(curlForRunDataStoreCommand);
		curlBuffer[curlBufferOffset] = '\0';

		if (result != CURLE_OK) {
			curlReturnCode = result;
			ostringstream crc;
			crc << curlReturnCode;
			curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
			dbError.set(curlErrorString, (uint64_t)curlReturnCode);
			SPLAPPTRC(L_DEBUG, curlErrorString <<". " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CloudantDBLayer");
			return(false);
		}

		curl_easy_getinfo(curlForRunDataStoreCommand, CURLINFO_RESPONSE_CODE, &httpResponseCode);
		// We will not return a specific error message. User has to interpret the HTTP error returned by the Cloudant service.
		httpReasonString = "";
		dbError.set(httpReasonString, httpResponseCode);
		// We will simply return true from here just to indicate we sent the user's Cloudant HTTP request and
		// got a response from the Cloudant server. We are not going to pare the response from the server here.
		// That is the caller's responsibility. We will send the server response back to the caller via the
		// user provided return variable.
		jsonResponse = string(curlBuffer);
		return(true);
  }

  bool CloudantDBLayer::runDataStoreCommand(std::vector<std::string> const & cmdList, std::string & resultValue, PersistenceError & dbError) {
		// This API can only be supported in Redis.
		// Cloudant doesn't have a way to do this.
		dbError.set("From Cloudant data store: This API to run native data store commands is not supported in cloudant.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Cloudant data store: This API to run native data store commands is not supported in cloudant. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CloudantDBLayer");
		return(false);
  }

  // This method will get the data item from the store for a given key.
  // Caller of this method can also ask us just to find if a data item
  // exists in the store without the extra work of fetching and returning the data item value.
  bool CloudantDBLayer::getDataItemFromStore(std::string const & storeIdString,
		  std::string const & keyDataString, bool const & checkOnlyForDataItemExistence,
		  bool const & skipDataItemExistenceCheck, unsigned char * & valueData,
		  uint32_t & valueSize, PersistenceError & dbError) {
		// Let us get this data item from the cache as it is.
		// Since there could be multiple data writers, we are going to get whatever is there now.
		// It is always possible that the value for the requested item can change right after
		// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
		// This action is performed on the Store Contents DB that takes the following name.
		// DB name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
		string storeDbName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
		string docUrl = cloudantBaseUrl + storeDbName + "/" + keyDataString;
		int32_t curlReturnCode = 0;
		string curlErrorString = "";
		uint64_t httpResponseCode = 0;
		string httpReasonString = "";
		string value = "";
		string revision = "";
		bool dataItemExists = true;

		// Read this K/V pair if it exists.
		bool cloudantResult = readCloudantDocumentField(docUrl, keyDataString, value, revision,
			curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
			// K/V pair already exists.
			dataItemExists = true;
		} else if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_NOT_FOUND) {
			// There is no existing K/V pair in the DB.
			dataItemExists = false;
		} else {
			// Some other error.
			string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			// Unable to get the requested data item from the cache.
			dbError.set("Unable to access the K/V pair in Cloudant with the StoreId " + storeIdString +
				". " + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			return(false);
		}

		// If the caller only wanted us to check for the data item existence, we can exit now.
		if (checkOnlyForDataItemExistence == true) {
			return(dataItemExists);
		}

		// Caller wants us to fetch and return the data item value.
		// If the data item is not there, we can't do much at this point.
		if (dataItemExists == false) {
		  // This data item doesn't exist. Let us raise an error.
		  // Requested data item is not there in the cache.
		  dbError.set("The requested data item doesn't exist in the StoreId " + storeIdString +
			 ".", DPS_DATA_ITEM_READ_ERROR);
		   return(false);
		} else {
			// In Cloudant, when we put the K/V pair, there is no way to store binary values.
			// Hence, we b64_encoded our binary value buffer and stored it as a string.
			// We must now b64_decode it back to binary value buffer.
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

  CloudantDBLayerIterator * CloudantDBLayer::newIterator(uint64_t store, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside newIterator for store id " << store, "CloudantDBLayer");

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  // Ensure that a store exists for the given store id.
	  if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CloudantDBLayer");
		  }

		  return(NULL);
	  }

	  // Get the general information about this store.
	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayer");
		  return(NULL);
	  }

	  // It is a valid store. Create a new iterator and return it to the caller.
	  CloudantDBLayerIterator *iter = new CloudantDBLayerIterator();
	  iter->store = store;
	  base64_decode(storeName, iter->storeName);
	  iter->hasData = true;
	  // Give this iterator access to our CloudantDBLayer object.
	  iter->cloudantDBLayerPtr = this;
	  iter->sizeOfDataItemKeysVector = 0;
	  iter->currentIndex = 0;
	  return(iter);
  }

  void CloudantDBLayer::deleteIterator(uint64_t store, Iterator * iter, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside deleteIterator for store id " << store, "CloudantDBLayer");

	  if (iter == NULL) {
		  return;
	  }

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  CloudantDBLayerIterator *myIter = static_cast<CloudantDBLayerIterator *>(iter);

	  // Let us ensure that the user wants to delete an iterator that really belongs to the store passed to us.
	  // This will handle user's coding errors where a wrong combination of store id and iterator is passed to us for deletion.
	  if (myIter->store != store) {
		  // User sent us a wrong combination of a store and an iterator.
		  dbError.set("A wrong iterator has been sent for deletion. This iterator doesn't belong to the StoreId " +
		  				storeIdString + ".", DPS_STORE_ITERATION_DELETION_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside deleteIterator, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_DELETION_ERROR, "CloudantDBLayer");
		  return;
	  } else {
		  delete iter;
	  }
  }

  // This method will acquire a lock for any given generic/arbitrary identifier passed as a string..
  // This is typically used inside the createStore, createOrGetStore, createOrGetLock methods to
  // provide thread safety. There are other lock acquisition/release methods once someone has a valid store id or lock id.
  bool CloudantDBLayer::acquireGeneralPurposeLock(string const & entityName) {
	  int32_t retryCnt = 0;

	  // Cloudant DB doesn't have TTL (Time To Live) features like other NoSQL databases.
	  // Hence, we have to manage it ourselves to release a stale lock that the previous lock owner forgot to unlock.
	  // Cloudant also doesn't allow database names to start with a numeral digit. First letter must be an alphabet.
	  // It is going to be cumbersome. Let us deal with it.
	  //Try to get a lock for this generic entity.
	  while (1) {
		// '501' + 'entity name' + 'generic_lock' => 1
		std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or we forcefully take it
		// back the forgotten stale locks.
		// Create a Cloudant DB document for this generic lock in our meta data table.
		// URL format: baseURL/db-name/doc
		string docUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + genericLockKey;
		// Let us store the current time to indicate when we grabbed the lock. If we don't release the
		// lock before allowed maximum time (DPS_AND_DL_GET_LOCK_TTL), anyone else can grab it from us after that time expires.
		// (Other NoSQL DB alternatives will do this via TTL. In Cloudant, we have to do it ourselves. A weakness of Cloudant.)
		SPL::timestamp tsNow = SPL::Functions::Time::getTimestamp();
		int64_t timeInSecondsNow = SPL::Functions::Time::getSeconds(tsNow);
		std::ostringstream timeValue;
		timeValue << timeInSecondsNow;
		// Form the JSON payload as a literal string.
		string jsonDoc = "{\"_id\": \"" + genericLockKey + "\", \"" + genericLockKey + "\": \"" + timeValue.str() + "\"}";
		int32_t curlReturnCode = 0;
		string curlErrorString = "";
		uint64_t httpResponseCode = 0;
		string httpReasonString = "";

		// If "_rev" JSON field is not present, then it will create a new document. Else, it will update an existing document.
		bool cloudantResult =  createOrUpdateCloudantDocument(docUrl, jsonDoc, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		if (cloudantResult == true && (httpResponseCode == CLOUDANT_DOC_CREATED || httpResponseCode == CLOUDANT_DOC_ACCEPTED_FOR_WRITING)) {
			// We got the lock. We can return now.
			return(true);
		}

		if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_EXISTS) {
			// Someone has this lock right now. Let us check if they are well within their allowed time to own the lock.
			// If not, we can take over this lock. We can go ahead and read their lock acquisition time.
			curlReturnCode = 0;
			curlErrorString = "";
			httpResponseCode = 0;
			httpReasonString = "";
			string key = genericLockKey;
			string value = "";
			string revision = "";
			cloudantResult = readCloudantDocumentField(docUrl, key, value, revision,
				curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

			if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
				// We got the current lock owner's lock acquisition time.
				// Let us see if their time already expired for this lock.
				int64_t lockAcquiredTime = atoi(value.c_str());
				SPL::timestamp currentTimestamp = SPL::Functions::Time::getTimestamp();
				int64_t currentTime = SPL::Functions::Time::getSeconds(currentTimestamp);
				if ((currentTime - lockAcquiredTime) > DPS_AND_DL_GET_LOCK_TTL) {
					// Previous owner exceeded the time of ownership.
					// Overusing the lock or forgot to release the lock.
					// Let us take over the lock by changing the time value on this field.
					// Update the existing document's field now.
					std::ostringstream currentTimeValue;
					currentTimeValue << currentTime;
					jsonDoc = "{\"_id\": \"" + genericLockKey + "\", \"" + genericLockKey + "\": \"" + currentTimeValue.str() + "\", \"_rev\": \"" + revision + "\"}";
					curlReturnCode = 0;
					curlErrorString = "";
					httpResponseCode = 0;
					httpReasonString = "";
					cloudantResult = createOrUpdateCloudantDocument(docUrl, jsonDoc, curlReturnCode,
						curlErrorString, httpResponseCode, httpReasonString);

					if (cloudantResult == true) {
						// We got the lock.
						return(true);
					}
				}
			}
		}

		if (cloudantResult == false) {
			// Some other Cloudant HTTP API error occurred.
			return(false);
		}

		// Someone else is holding on to the lock of this entity. Wait for a while before trying again.
		retryCnt++;

		if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {

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

	  return(false);
  }

  void CloudantDBLayer::releaseGeneralPurposeLock(string const & entityName) {
	  // '501' + 'entity name' + 'generic_lock' => 1
	  std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
	  string docUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + genericLockKey;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  deleteCloudantDocument(docUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
  }

    // Senthil added this on Apr/06/2022.
    // This method will get multiple keys from the given store and
    // populate them in the caller provided list (vector).
    // Be aware of the time it can take to fetch multiple keys in a store
    // that has several tens of thousands of keys. In such cases, the caller
    // has to maintain calm until we return back from here.
   void CloudantDBLayer::getKeys(uint64_t store, std::vector<unsigned char *> & keysBuffer, std::vector<uint32_t> & keysSize, int32_t keyStartPosition, int32_t numberOfKeysNeeded, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside getKeys for store id " << store, "CloudantDBLayer");

       // Not implemented at this time. Simply return.
       return;
    } // End of getKeys method.


  CloudantDBLayerIterator::CloudantDBLayerIterator() {

  }

  CloudantDBLayerIterator::~CloudantDBLayerIterator() {

  }

  bool CloudantDBLayerIterator::getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
  	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside getNext for store id " << store, "CloudantDBLayerIterator");

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
	  if (this->cloudantDBLayerPtr->storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayerIterator");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CloudantDBLayerIterator");
		  }

		  return(false);
	  }

	  // Ensure that this store is not empty at this time.
	  if (this->cloudantDBLayerPtr->size(store, dbError) <= 0) {
		  dbError.set("Store is empty for the StoreId " + storeIdString + ".", DPS_STORE_EMPTY_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed with an empty store whose id is " << storeIdString << ". " << DPS_STORE_EMPTY_ERROR, "CloudantDBLayerIterator");
		  return(false);
	  }

	  if (this->sizeOfDataItemKeysVector <= 0) {
		  // This is the first time we are coming inside getNext for store iteration.
		  // Let us get the available data item keys from this store.
		  this->dataItemKeys.clear();

		  string storeDbName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
		  string docUrl = this->cloudantDBLayerPtr->cloudantBaseUrl + storeDbName + "/" + "_all_docs";
		  int32_t curlReturnCode = 0;
		  string curlErrorString = "";
		  uint64_t httpResponseCode = 0;
		  string httpReasonString = "";
		  bool cloudantResult = this->cloudantDBLayerPtr->getAllDocsFromCloudantDatabase(docUrl, this->dataItemKeys, curlReturnCode,
		     curlErrorString, httpResponseCode, httpReasonString);

		  if (cloudantResult == false) {
			  // Unable to get data item keys from the store.
			  string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			  dbError.set("Unable to get data item keys for the StoreId " + storeIdString +
					  ". " + errorMsg, DPS_GET_STORE_DATA_ITEM_KEYS_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to get data item keys for store id " << storeIdString <<
                 ". " << errorMsg << ". " << DPS_GET_STORE_DATA_ITEM_KEYS_ERROR, "CloudantDBLayerIterator");
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
	  // data_item_key was obtained straight from the store contents DB, where it is
	  // already in the base64 encoded format.
	  bool result = this->cloudantDBLayerPtr->getDataItemFromStore(storeIdString, data_item_key,
		false, false, valueData, valueSize, dbError);

	  if (result == false) {
		  // Some error has occurred in reading the data item value.
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to get data item from store id " << storeIdString << ". " << dbError.getErrorCode(), "CloudantDBLayerIterator");
		  // We will disable any future action for this store using the current iterator.
		  this->hasData = false;
		  return(false);
	  }

	  // We are almost done once we take care of arranging to return the key name and key size.
	  // In order to support spaces in data item keys, we base64 encoded them before storing it in Cloudant.
	  // Let us base64 decode it now to get the original data item key.
	  string base64_decoded_data_item_key;
	  this->cloudantDBLayerPtr->base64_decode(data_item_key, base64_decoded_data_item_key);
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
		     storeIdString << ". " << DPS_STORE_ITERATION_MALLOC_ERROR, "CloudantDBLayerIterator");
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
  uint64_t CloudantDBLayer::createOrGetLock(std::string const & name, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock with a name " << name, "CloudantDBLayer");
		string base64_encoded_name;
		base64_encode(name, base64_encoded_name);

	 	// Get a general purpose lock so that only one thread can
		// enter inside of this method at any given time with the same lock name.
	 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
	 		// Unable to acquire the general purpose lock.
	 		lkError.set("Unable to get a generic lock for creating a lock with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
	 		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to get a generic lock while creating a store lock named " <<
	 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "CloudantDBLayer");
	 		// User has to retry again to create this distributed lock.
	 		return (0);
	 	}

		// In our Cloudant dps implementation, data item keys can have space characters.
		// Inside Cloudant, all our lock names will have a mapping type indicator of
		// "5" at the beginning followed by the actual lock name.
		// '5' + 'lock name' => lockId
		std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
		uint64_t lockId = SPL::Functions::Utility::hashCode(lockNameKey);
		ostringstream lockIdStr;
		lockIdStr << lockId;

		string lockNameKeyUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + lockNameKey;
		// Form the JSON payload as a literal string.
		string jsonDoc = "{\"_id\": \"" + lockNameKey + "\", \"" + lockNameKey + "\": \"" + lockIdStr.str() + "\"}";
		int32_t curlReturnCode = 0;
		string curlErrorString = "";
		uint64_t httpResponseCode = 0;
		string httpReasonString = "";

		// If "_rev" JSON field is not present, then it will create a new document. Else, it will update an existing document.
		bool cloudantResult =  createOrUpdateCloudantDocument(lockNameKeyUrl, jsonDoc, curlReturnCode,
				curlErrorString, httpResponseCode, httpReasonString);
		string errorMsg = "";

		if (cloudantResult == false) {
			// There was an error in creating the user defined lock.
			errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			lkError.set("DB put error. Unable to put the lockId for the lockName " + name + ". " + errorMsg, DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "DB put error. Inside createOrGetLock, it failed to put the lockId for the lockName " <<
				name << ". Error = " << errorMsg << ". " << DL_GET_LOCK_ID_ERROR, "CloudantDBLayer");
			releaseGeneralPurposeLock(base64_encoded_name);
			return(0);
		}

		if (httpResponseCode == CLOUDANT_DOC_EXISTS) {
			// This lock already exists. We can simply return the lockId now.
			releaseGeneralPurposeLock(base64_encoded_name);
			return(lockId);
		}

		// If we are here that means the Cloudant doc was created by the previous call.
		// We can go ahead and create the lock info entry now.
		// 2) Create the Lock Info
		//    '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdStr.str();  // LockId becomes the new key now.
		string lockInfoKeyUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + lockInfoKey;
		string lockInfoValue = string("0_0_0_") + base64_encoded_name;
		// Form the JSON payload as a literal string.
		jsonDoc = "{\"_id\": \"" + lockInfoKey + "\", \"" + lockInfoKey + "\": \"" + lockInfoValue + "\"}";
		curlReturnCode = 0;
		curlErrorString = "";
		httpResponseCode = 0;
		httpReasonString = "";

		// If "_rev" JSON field is not present, then it will create a new document. Else, it will update an existing document.
		cloudantResult =  createOrUpdateCloudantDocument(lockInfoKeyUrl, jsonDoc, curlReturnCode,
			curlErrorString, httpResponseCode, httpReasonString);

		if (cloudantResult == false) {
			// There was an error in creating the user defined lock.
			errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			// Unable to create lockinfo details..
			// Problem in creating the "LockId:LockInfo" entry in the cache.
			lkError.set("Unable to create 'LockId:LockInfo' in the cache for a lock named " + name + ". " + errorMsg, DL_LOCK_INFO_CREATION_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to create 'LockId:LockInfo' for a lock named " <<
				name << ". Error=" << errorMsg << ". " << DL_LOCK_INFO_CREATION_ERROR, "CloudantDBLayer");
			// Delete the previous root entry we inserted.
			deleteCloudantDocument(lockNameKeyUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
			releaseGeneralPurposeLock(base64_encoded_name);
			return(0);
		} else {
			releaseGeneralPurposeLock(base64_encoded_name);
			return(lockId);
		}
  }

  bool CloudantDBLayer::removeLock(uint64_t lock, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside removeLock for lock id " << lock, "CloudantDBLayer");

		ostringstream lockId;
		lockId << lock;
		string lockIdString = lockId.str();

		// If the lock doesn't exist, there is nothing to remove. Don't allow this caller inside this method.
		if(lockIdExistsOrNot(lockIdString, lkError) == false) {
			if (lkError.hasError() == true) {
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "CloudantDBLayer");
			} else {
				lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to find the lock with an id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "CloudantDBLayer");
			}

			return(false);
		}

		// Before removing the lock entirely, ensure that the lock is not currently being used by anyone else.
		if (acquireLock(lock, 25, 40, lkError) == false) {
			// Unable to acquire the distributed lock.
			lkError.set("Unable to get a distributed lock for the LockId " + lockIdString + ".", DL_GET_DISTRIBUTED_LOCK_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to get a distributed lock for the lock id " << lockIdString << ". " << DL_GET_DISTRIBUTED_LOCK_ERROR, "CloudantDBLayer");
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
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "CloudantDBLayer");
			releaseLock(lock, lkError);
			// This is alarming. This will put this lock in a bad state. Poor user has to deal with it.
			return(false);
		}

		// Since we got back the lock name for the given lock id, let us remove the lock entirely.
		// '5' + 'lock name' => lockId
		std::string lockNameKey = DL_LOCK_NAME_TYPE + lockName;
		string lockNameKeyUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + lockNameKey;
		int32_t curlReturnCode = 0;
		string curlErrorString = "";
		uint64_t httpResponseCode = 0;
		string httpReasonString = "";

		bool cloudantResult = deleteCloudantDocument(lockNameKeyUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		if (cloudantResult == false) {
			// There was an error in deleting the user defined lock.
			string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			lkError.set("Unable to remove the lock named " + lockIdString + ".", DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to remove the lock with an id " << lockIdString << ". Error=" <<
				errorMsg << ". " << DL_LOCK_REMOVAL_ERROR, "CloudantDBLayer");
			releaseLock(lock, lkError);
			return(false);
		}

		// Now remove the lockInfo document.
		// '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;  // LockId becomes the new key now.
		string lockInfoKeyUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + lockInfoKey;
		cloudantResult = deleteCloudantDocument(lockInfoKeyUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

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

  bool CloudantDBLayer::acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside acquireLock for lock id " << lock, "CloudantDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();
	  int32_t retryCnt = 0;

	  // If the lock doesn't exist, there is nothing to acquire. Don't allow this caller inside this method.
	  if(lockIdExistsOrNot(lockIdString, lkError) == false) {
		  if (lkError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "CloudantDBLayer");
		  } else {
			  lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to find a lock with an id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "CloudantDBLayer");
		  }

		  return(false);
	  }

	  // We will first check if we can get this lock.
	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  time_t startTime, timeNow;
	  // Get the start time for our lock acquisition attempts.
	  time(&startTime);

	  //Try to get a distributed lock.
	  while(1) {
		  time_t new_lock_expiry_time = time(0) + (time_t)leaseTime;
		  // This is an atomic activity.
		  // If multiple threads attempt to do it at the same time, only one will succeed.
		  // Winner will hold the lock until they release it voluntarily or we forcefully take it
		  // back the forgotten stale locks.
		  // Create a Cloudant DB document for this user defined distributed lock in our meta data table.
		  // URL format: baseURL/db-name/doc
		  string docUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + distributedLockKey;
		  // (Other NoSQL DB alternatives will do this via TTL. In Cloudant, we have to do it ourselves. A weakness of Cloudant.)
		  // Form the JSON payload as a literal string.
		  string jsonDoc = "{\"_id\": \"" + distributedLockKey + "\", \"" + distributedLockKey + "\": \"1\"}";
		  int32_t curlReturnCode = 0;
		  string curlErrorString = "";
		  uint64_t httpResponseCode = 0;
		  string httpReasonString = "";

		  // If "_rev" JSON field is not present, then it will create a new document. Else, it will update an existing document.
		  bool cloudantResult =  createOrUpdateCloudantDocument(docUrl, jsonDoc, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		  if (cloudantResult == true && (httpResponseCode == CLOUDANT_DOC_CREATED || httpResponseCode == CLOUDANT_DOC_ACCEPTED_FOR_WRITING)) {
			  // We got the lock. We can return now.
			  // Let us update the lock information now.
			  if(updateLockInformation(lockIdString, lkError, 1, new_lock_expiry_time, getpid()) == true) {
				  return(true);
			  } else {
				  // Some error occurred while updating the lock information.
				  // It will be in an inconsistent state. Let us release the lock.
				  releaseLock(lock, lkError);
			  }
		  }

		  if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_EXISTS) {
			  // Someone has this lock right now. Let us check if they are well within their allowed time to own the lock.
			  // If not, we can take over this lock. We can go ahead and read their lock acquisition time.
			  // We didn't get the lock.
			  // Let us check if the previous owner of this lock simply forgot to release it.
			  // In that case, we will release this expired lock.
			  // Read the time at which this lock is expected to expire.
			  uint32_t _lockUsageCnt = 0;
			  int32_t _lockExpirationTime = 0;
			  std::string _lockName = "";
			  pid_t _lockOwningPid = 0;

			  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
				  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "CloudantDBLayer");
			  } else {
				  // Is current time greater than the lock expiration time?
				  if ((_lockExpirationTime > 0) && (time(0) > (time_t)_lockExpirationTime)) {
					  // Time has passed beyond the lease of this lock.
					  // Lease expired for this lock. Original owner forgot to release the lock and simply left it hanging there without a valid lease.
					  releaseLock(lock, lkError);
				  }
			  }
		  }

		  if (cloudantResult == false) {
			  // Some other Cloudant HTTP API error occurred.
			  string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			  lkError.set("Unable to acquire the lock named " + lockIdString + " due to this HTTP API error: " + errorMsg, DPS_HTTP_REST_API_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for a lock named " << lockIdString << " due to this HTTP API error: " <<
					  errorMsg << ". " << DL_GET_LOCK_ERROR, "CloudantDBLayer");
			  return(false);
		  }

		  // Someone else is holding on to this distributed lock. Wait for a while before trying again.
		  retryCnt++;

		  if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
			  lkError.set("Unable to acquire the lock named " + lockIdString + ".", DL_GET_LOCK_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire a lock named " << lockIdString << ". " << DL_GET_LOCK_ERROR, "CloudantDBLayer");
			  // Our caller can check the error code and try to acquire the lock again.
			  return(false);
		  }

		  // Check if we have gone past the maximum wait time the caller was willing to wait in order to acquire this lock.
		  time(&timeNow);
		  if (difftime(startTime, timeNow) > maxWaitTimeToAcquireLock) {
			  lkError.set("Unable to acquire the lock named " + lockIdString + " within the caller specified wait time.", DL_GET_LOCK_TIMEOUT_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire the lock named " << lockIdString <<
					  " within the caller specified wait time." << DL_GET_LOCK_TIMEOUT_ERROR, "CloudantDBLayer");
			  // Our caller can check the error code and try to acquire the lock again.
			  return(false);
		  }

		  // Yield control to other threads. Wait here with patience by doing an exponential back-off delay.
		  /*
		  usleep(DPS_AND_DL_GET_LOCK_SLEEP_TIME *
			(retryCnt%(DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT/DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR)));
		  */
		  // Do a random wait before trying to create it.
		  // This will give random value between 0 and 1.
		  float64 rand = SPL::Functions::Math::random();
		  // Let us wait for that random duration which is a partial second i.e. less than a second.
		  SPL::Functions::Utility::block(rand);
		  lkError.reset();
	  } // End of while(1)
  }

  void CloudantDBLayer::releaseLock(uint64_t lock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside releaseLock for lock id " << lock, "CloudantDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();

	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  string docUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + distributedLockKey;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  bool cloudantResult = deleteCloudantDocument(docUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (cloudantResult == false) {
		  string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  lkError.set("Unable to release the distributed lock id " + lockIdString + ". " + errorMsg, DL_LOCK_RELEASE_ERROR);
		  return;
	  }

	  updateLockInformation(lockIdString, lkError, 0, 0, 0);
  }

  bool CloudantDBLayer::updateLockInformation(std::string const & lockIdString,
	PersistenceError & lkError, uint32_t const & lockUsageCnt, int32_t const & lockExpirationTime, pid_t const & lockOwningPid) {
	  // Get the lock name for this lock.
	  uint32_t _lockUsageCnt = 0;
	  int32_t _lockExpirationTime = 0;
	  std::string _lockName = "";
	  pid_t _lockOwningPid = 0;


	  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "CloudantDBLayer");
		  return(false);
	  }

	  // Let us update the lock information.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  ostringstream lockInfoValue;
	  lockInfoValue << lockUsageCnt << "_" << lockExpirationTime << "_" << lockOwningPid << "_" << _lockName;
	  // We have to first read from the database to get the "_rev" and then we can update it.
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  string key = lockInfoKey;
	  string value = "";
	  string revision = "";
	  string jsonDoc = "";
	  string errorMsg = "";
	  string lockInfoKeyUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + lockInfoKey;
	  bool cloudantResult = readCloudantDocumentField(lockInfoKeyUrl, key, value, revision,
			  curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
		  // Let us include the "_rev" in our JSON doc for updating the lockInfoValue.
		  jsonDoc = "{\"_id\": \"" + lockInfoKey + "\", \"" + lockInfoKey + "\": \"" +
		     lockInfoValue.str() + "\", \"_rev\": \"" + revision + "\"}";
	  } else {
		  // Lock information is not there or some other HTTP related error. That is very odd.
		  // Raise an error.
		  // Problem in updating the "LockId:LockInfo" entry in the cache.
		  errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  lkError.set("Critical Error1: Unable to update 'LockId:LockInfo' in the cache for a lock named " + _lockName + ". " + errorMsg, DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Critical Error1: Inside updateLockInformation, it failed for a lock named " << _lockName << ". " <<
			errorMsg << ". " << DL_LOCK_INFO_UPDATE_ERROR, "CloudantDBLayer");
		  return(false);
	  }

	  curlReturnCode = 0;
	  curlErrorString = "";
	  httpResponseCode = 0;
	  httpReasonString = "";

	  // If "_rev" JSON field is not present, then it will create a new document. Else, it will update an existing document.
	  cloudantResult =  createOrUpdateCloudantDocument(lockInfoKeyUrl, jsonDoc, curlReturnCode,
			  curlErrorString, httpResponseCode, httpReasonString);

	  if (cloudantResult == false) {
		  errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  lkError.set("Critical Error2: Unable to update 'LockId:LockInfo' in the cache for a lock named " + _lockName + ". " + errorMsg, DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Critical Error2: Inside updateLockInformation, it failed for a lock named " << _lockName << ". " <<
			errorMsg << ". " << DL_LOCK_INFO_UPDATE_ERROR, "CloudantDBLayer");
		  return(false);
	  }

	  return(true);
  }

  bool CloudantDBLayer::readLockInformation(std::string const & lockIdString, PersistenceError & lkError, uint32_t & lockUsageCnt,
		  int32_t & lockExpirationTime, pid_t & lockOwningPid, std::string & lockName) {
	  // Read the contents of the lock information.
	  lockName = "";
	  std::string lockInfo = "";

	  // Lock Info contains meta data information about a given lock.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  string key = lockInfoKey;
	  string value = "";
	  string revision = "";
	  string lockInfoKeyUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + lockInfoKey;

	  bool cloudantResult = readCloudantDocumentField(lockInfoKeyUrl, key, value, revision,
			  curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
		  // We got the value from the Cloudant DB.
		  lockInfo = value;
	  } else {
		  // Unable to get the LockInfo from our cache.
		  string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  lkError.set("Unable to get LockInfo using the LockId " + lockIdString +
			". " + errorMsg, DL_GET_LOCK_INFO_ERROR);
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
  bool CloudantDBLayer::lockIdExistsOrNot(string lockIdString, PersistenceError & lkError) {
	  string keyString = string(DL_LOCK_INFO_TYPE) + lockIdString;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  string key = keyString;
	  string value = "";
	  string revision = "";
	  string lockInfoKeyUrl = cloudantBaseUrl + string(DPS_DL_META_DATA_DB) + "/" + keyString;

	  bool cloudantResult = readCloudantDocumentField(lockInfoKeyUrl, key, value, revision,
			  curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
		  // LockId exists.
		  return(true);
	  } else if (cloudantResult == true && httpResponseCode == CLOUDANT_DOC_NOT_FOUND) {
		  return(false);
	  } else {
		  // There is some other HTTP error.
		  // Unable to get the lock info for the given lock id.
		  string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  lkError.set("LockIdExistsOrNot: Unable to get LockInfo for the lockId " + lockIdString +
			 ". " + errorMsg, DL_GET_LOCK_INFO_ERROR);
		  return(false);
	  }
  }

  // This method will return the process id that currently owns the given lock.
  uint32_t CloudantDBLayer::getPidForLock(string const & name, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside getPidForLock with a name " << name, "CloudantDBLayer");

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
		  SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "CloudantDBLayer");
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
  inline bool CloudantDBLayer::is_b64(unsigned char c) {
    return (isalnum(c) || (c == '+') || (c == '/'));
  }

  // Base64 encode the binary buffer contents passed by the caller and return a string representation.
  // There is no change to the original code in this method other than a slight change in the method name,
  // returning back right away when encountering an empty buffer, replacing / and + characters with ~ and -
  // and an array initialization for char_array_4 to avoid a compiler warning.
  //
  // IMPORTANT:
  // In Cloudant, every b64 encoded string may be used inside an URL or in a JSON formatted data.
  // We can't allow these two characters in the context of Cloudant because of URL format restrictions:
  // Forward slash and plus   i.e.  /  and +
  // Hence, we must replace them with ~ and - (tilde and hyphen).
  // Do the reverse of that in b64_decode.
  void CloudantDBLayer::b64_encode(unsigned char const * & buf, uint32_t const & bufLenOrg, string & ret) {
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

	// --- Cloudant specific change begins here ---
	// To comply with the URL formatting rules, replace all the forward slash and plus characters if they are present.
    if (ret.find_first_of("/") != string::npos) {
    	streams_boost::replace_all(ret, "/", "~");
    }

    if (ret.find_first_of("+") != string::npos) {
    	streams_boost::replace_all(ret, "+", "-");
    }
	// --- Cloudant specific change ends here ---

    return;
  }

  // Base64 decode the string passed by the caller.
  // Then, allocate a memory buffer and transfer the decoded bytes there and assign it to the pointer passed by the caller.
  // In addition, assign the buffer length to the variable passed by the caller.
  // Modified the input argument types and the return value types.
  // Majority of the original logic is kept intact. Method name is slightly changed,
  // replaced the ~ and - characters with their original / and + characters and I added more code
  // at the end of the method to return an allocated memory buffer instead of an std::vector.
  //
  // IMPORTANT:
  // In Cloudant, we have to comply with the URL formatting rules.
  // Hence, we did some trickery in the b64_encode method above.
  // We will do the reverse of that action here. Please read the commentary in b64_encode method.
  void CloudantDBLayer::b64_decode(std::string & encoded_string, unsigned char * & buf, uint32_t & bufLen) {
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

	bool tildePresent = false;
	bool hyphenPresent = false;

	if (encoded_string.find_first_of("~") != string::npos) {
		tildePresent = true;
	}

	if (encoded_string.find_first_of("-") != string::npos) {
		hyphenPresent = true;
	}

	if (tildePresent == true || hyphenPresent == true) {
		// --- Cloudant specific change begins here ---
	    // To comply with the URL formatting rules, we substituted / and + characters with ~ and - characters at the time of encoding.
	    // Let us do the reverse of that here if those characters are present.
	    // A memory copy here so as not to modify the caller's passed data. (Can it be optimized later?)
	    if (tildePresent == true) {
	    	streams_boost::replace_all(encoded_string, "~", "/");
	    }

	    if (hyphenPresent == true) {
	    	streams_boost::replace_all(encoded_string, "-", "+");
	    }
	    // --- Cloudant specific change ends here ---
	}

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

  // Cloudant specific cURL write and read functions.
  // Since we are using C++ and the cURL library is C based, we have to set the callback as a
  // static C++ method and configure cURL to pass our custom C++ object pointer in the 4th argument of
  // the callback function. Once we get the C++ object pointer, we can access our other non-static
  // member functions and non-static member variables via that object pointer.
  //
  // The 4th argument will be pointing to a non-static method below named writeFunctionImpl.
  size_t CloudantDBLayer::writeFunction(char *data, size_t size, size_t nmemb, void *objPtr) {
	return(static_cast<CloudantDBLayer*>(objPtr)->writeFunctionImpl(data, size, nmemb));
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  size_t CloudantDBLayer::writeFunctionImpl(char *data, size_t size, size_t nmemb) {
	  memcpy(&(curlBuffer[curlBufferOffset]), data, size * nmemb);
	  curlBufferOffset += size * nmemb;
	  return (size * nmemb);
  }

  // Do the same for the read function.
  // The 4th argument will be pointing to a non-static method below named readFunctionImpl.
  size_t CloudantDBLayer::readFunction(char *data, size_t size, size_t nmemb, void *objPtr) {
	  return(static_cast<CloudantDBLayer*>(objPtr)->readFunctionImpl(data, size, nmemb));
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  size_t CloudantDBLayer::readFunctionImpl(char *data, size_t size, size_t nmemb) {
	  int len = strlen(putBuffer);
	  memcpy(data, putBuffer, len);
	  return len;
  }

  //===================================================================================
  // Following methods are specific to the Cloudant DB REST APIs that perform all
  // the CRUD operations on our stores and locks.
  //===================================================================================
  // This method will create a Cloudant database.
  // URL format should be: http://user:password@user.cloudant.com/db-name (OR)
  // http://user:password@XXXXX/db-name where XXXXX is a name or IP address of your
  // on-premises Cloudant Local load balancer machine.
  bool CloudantDBLayer::createCloudantDatabase(string const & url, int32_t & curlReturnCode,
	  string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString) {
	  curlReturnCode = 0;
	  httpResponseCode = 0;
	  curlErrorString = "";
	  httpReasonString = "";
	  char *curlEffectiveUrl = NULL;
	  CURLcode result;
	  bool repeatingUrl = false;

	  // Get the URL endpoint that was used when we executed this method previously.
	  result = curl_easy_getinfo(curlForCreateCloudantDatabase, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForCreateCloudantDatabase);
		  curl_easy_setopt(curlForCreateCloudantDatabase, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
		  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
		  curl_easy_setopt(curlForCreateCloudantDatabase, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForCreateCloudantDatabase, CURLOPT_WRITEFUNCTION, &CloudantDBLayer::writeFunction);
		  // Using CURLOPT_PUT here hangs at times. Hence, I'm using CUSTOMREQUEST PUT which seems to work.
		  // curl_easy_setopt(curlForCreateCloudantDatabase, CURLOPT_PUT, 1);
		  curl_easy_setopt(curlForCreateCloudantDatabase, CURLOPT_CUSTOMREQUEST, HTTP_PUT);
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // Cloudant server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForCreateCloudantDatabase, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForCreateCloudantDatabase, CURLOPT_HTTPHEADER, headersForCreateCloudantDatabase);
	  }

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForCreateCloudantDatabase);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForCreateCloudantDatabase, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool cloudantResult = true;

	  // HTTP response codes: 201-->DB creation successful, 412-->DB already exists, 403-->Invalid DB name.
	  if ((httpResponseCode != CLOUDANT_DB_CREATED) && (httpResponseCode != CLOUDANT_DB_EXISTS)) {
	    // Some problem in creating the database.
	    json_object *jo = json_tokener_parse(curlBuffer);
	    json_object *joForField = NULL;
	     // Third argument to this API will receive the reference to the json_object associated
	     // with the given field name. Ownership of this object is retained by the first argument
	     // i.e. the parent json_object.
	     json_bool exists = json_object_object_get_ex(jo, "reason", &joForField);
	     const char *msg;

	     if (exists) {
	       msg = json_object_get_string(joForField);
	     } else {
	       msg = "N/A";
	     }

	     // Free the json object when the reference count goes to 0.
	     curlReturnCode = 0;
	     ostringstream hrc;
	     hrc << httpResponseCode;
	     httpReasonString = "rc=" + hrc.str() + ", msg=" + string(msg);
	     json_object_put(jo);
	     cloudantResult = false;
	  }

	  return(cloudantResult);
  }

  // This method will delete a Cloudant database.
  // URL format should be: http://user:password@user.cloudant.com/db-name (OR)
  // http://user:password@XXXXX/db-name where XXXXX is a name or IP address of your
  // on-premises Cloudant Local load balancer machine.
  bool CloudantDBLayer::deleteCloudantDatabase(string const & url, int32_t & curlReturnCode,
	  string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString) {
	  curlReturnCode = 0;
	  httpResponseCode = 0;
	  curlErrorString = "";
	  httpReasonString = "";
	  char *curlEffectiveUrl = NULL;
	  CURLcode result;
	  bool repeatingUrl = false;

	  // Get the URL endpoint that was used when we executed this method previously.
	  result = curl_easy_getinfo(curlForDeleteCloudantDatabase, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForDeleteCloudantDatabase);
		  curl_easy_setopt(curlForDeleteCloudantDatabase, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
		  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
		  curl_easy_setopt(curlForDeleteCloudantDatabase, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForDeleteCloudantDatabase, CURLOPT_WRITEFUNCTION, &CloudantDBLayer::writeFunction);
		  curl_easy_setopt(curlForDeleteCloudantDatabase, CURLOPT_CUSTOMREQUEST, HTTP_DELETE);
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // Cloudant server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForDeleteCloudantDatabase, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForDeleteCloudantDatabase, CURLOPT_HTTPHEADER, headersForDeleteCloudantDatabase);
	  }

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForDeleteCloudantDatabase);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForDeleteCloudantDatabase, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool cloudantResult = true;

	  // HTTP response codes: 200-->DB deletion successful, 404-->DB not found.
	  if ((httpResponseCode != CLOUDANT_DB_DELETED) && (httpResponseCode != CLOUDANT_DB_NOT_FOUND)) {
	    // Some problem in deleting the database.
	    json_object *jo = json_tokener_parse(curlBuffer);
	    json_object *joForField = NULL;
	     // Third argument to this API will receive the reference to the json_object associated
	     // with the given field name. Ownership of this object is retained by the first argument
	     // i.e. the parent json_object.
	     json_bool exists = json_object_object_get_ex(jo, "reason", &joForField);
	     const char *msg;

	     if (exists) {
	       msg = json_object_get_string(joForField);
	     } else {
	       msg = "N/A";
	     }

	     // Free the json object when the reference count goes to 0.
	     curlReturnCode = 0;
	     ostringstream hrc;
	     hrc << httpResponseCode;
	     httpReasonString = "rc=" + hrc.str() + ", msg=" + string(msg);
	     json_object_put(jo);
	     cloudantResult = false;
	  }

	  return(cloudantResult);
  }

  // This method will create a Cloudant document or update an existing document (if "_rev" JSON field is present).
  // URL format should be: http://user:password@user.cloudant.com/db-name/doc (OR)
  // http://user:password@XXXXX/db-name/doc where XXXXX is a name or IP address of your
  // on-premises Cloudant Local load balancer machine.
  bool CloudantDBLayer::createOrUpdateCloudantDocument(string const & url, string const & jsonDoc, int32_t & curlReturnCode,
  	string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString) {
	  curlReturnCode = 0;
	  httpResponseCode = 0;
	  curlErrorString = "";
	  httpReasonString = "";
	  char *curlEffectiveUrl = NULL;
	  CURLcode result;
	  bool repeatingUrl = false;
      putBuffer = jsonDoc.c_str();
	  long putBufferLen = jsonDoc.length();

	  // Get the URL endpoint that was used when we executed this method previously.
	  result = curl_easy_getinfo(curlForCreateOrUpdateCloudantDocument, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForCreateOrUpdateCloudantDocument);
		  curl_easy_setopt(curlForCreateOrUpdateCloudantDocument, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static method pointer in the CURLOPT_WRITEFUNCTION and then pass a custom
		  // object reference (this) of our current C++ class in the CURLOPT_WRITEDATA so that cURL will pass that
		  // object pointer as the 4th argument to our static writeFunction during its callback. By passing that object
		  // pointer as an argument to a static method, we can let the static method access our
		  // non-static member functions and non-static member variables via that object pointer.
		  curl_easy_setopt(curlForCreateOrUpdateCloudantDocument, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForCreateOrUpdateCloudantDocument, CURLOPT_WRITEFUNCTION, &CloudantDBLayer::writeFunction);
		  // Do the same here for CURLOPT_READDATA and CURLOPT_READFUNCTION as we did above.
		  curl_easy_setopt(curlForCreateOrUpdateCloudantDocument, CURLOPT_READDATA, this);
		  curl_easy_setopt(curlForCreateOrUpdateCloudantDocument, CURLOPT_READFUNCTION, &CloudantDBLayer::readFunction);
		  curl_easy_setopt(curlForCreateOrUpdateCloudantDocument, CURLOPT_PUT, 1);
		  // Put content length i.e. CURLOPT_INFILESIZE must be set outside of this repeat URL check if block.
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // Cloudant server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForCreateOrUpdateCloudantDocument, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForCreateOrUpdateCloudantDocument, CURLOPT_HTTPHEADER, headersForCreateOrUpdateCloudantDocument);
	  }

	  // This must be done every time whether it is a repeating URL or a new URL.
	  // Because, JSON document content may change across different invocations of this C++ method of ours.
	  curl_easy_setopt(curlForCreateOrUpdateCloudantDocument, CURLOPT_INFILESIZE, putBufferLen);
	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForCreateOrUpdateCloudantDocument);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForCreateOrUpdateCloudantDocument, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool cloudantResult = true;

	  // HTTP response codes: 201-->Doc creation successful, 409-->Doc already exists
	  if ((httpResponseCode != CLOUDANT_DOC_CREATED) && (httpResponseCode != CLOUDANT_DOC_ACCEPTED_FOR_WRITING) && (httpResponseCode != CLOUDANT_DOC_EXISTS)) {
	    // Some problem in creating the document.
	    json_object *jo = json_tokener_parse(curlBuffer);
	    json_object *joForField = NULL;
	     // Third argument to this API will receive the reference to the json_object associated
	     // with the given field name. Ownership of this object is retained by the first argument
	     // i.e. the parent json_object.
	     json_bool exists = json_object_object_get_ex(jo, "reason", &joForField);
	     const char *msg;

	     if (exists) {
	       msg = json_object_get_string(joForField);
	     } else {
	       msg = "N/A";
	     }

	     // Free the json object when the reference count goes to 0.
	     curlReturnCode = 0;
	     ostringstream hrc;
	     hrc << httpResponseCode;
	     httpReasonString = "rc=" + hrc.str() + ", msg=" + string(msg);
	     json_object_put(jo);
	     cloudantResult = false;
	  }

	  return(cloudantResult);
  }

  // This method will read a Cloudant document field specified by the user.
  // If that document is available, it will return both the value of the specified field and
  // the revision of the document so that the caller can use it to update the document.
  // URL format should be: http://user:password@user.cloudant.com/db-name/doc (OR)
  // http://user:password@XXXXX/db-name/doc where XXXXX is a name or IP address of your
  // on-premises Cloudant Local load balancer machine.
  bool CloudantDBLayer::readCloudantDocumentField(string const & url, string const & key, string & value,
	string & revision, int32_t & curlReturnCode, string & curlErrorString,
  	uint64_t & httpResponseCode, string & httpReasonString) {
	  curlReturnCode = 0;
	  httpResponseCode = 0;
	  curlErrorString = "";
	  httpReasonString = "";
	  value = "";
	  revision = "";
	  char *curlEffectiveUrl = NULL;
	  CURLcode result;
	  bool repeatingUrl = false;

	  // Get the URL endpoint that was used when we executed this method previously.
	  result = curl_easy_getinfo(curlForReadCloudantDocumentField, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForReadCloudantDocumentField);
		  curl_easy_setopt(curlForReadCloudantDocumentField, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
		  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
		  curl_easy_setopt(curlForReadCloudantDocumentField, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForReadCloudantDocumentField, CURLOPT_WRITEFUNCTION, &CloudantDBLayer::writeFunction);
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // Cloudant server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForReadCloudantDocumentField, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForReadCloudantDocumentField, CURLOPT_HTTPHEADER, headersForReadCloudantDocumentField);
	  }

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForReadCloudantDocumentField);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForReadCloudantDocumentField, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool cloudantResult = true;

	  // HTTP response codes: 200-->Doc retrieval successful, 404-->Document can't be found
	  if ((httpResponseCode != CLOUDANT_DOC_RETRIEVED) && (httpResponseCode != CLOUDANT_DOC_NOT_FOUND)) {
		// Some problem in retrieving the document.
		json_object *jo = json_tokener_parse(curlBuffer);
		json_object *joForField = NULL;
		 // Third argument to this API will receive the reference to the json_object associated
		 // with the given field name. Ownership of this object is retained by the first argument
		 // i.e. the parent json_object.
		 json_bool exists = json_object_object_get_ex(jo, "reason", &joForField);
		 const char *msg;

		 if (exists) {
		   msg = json_object_get_string(joForField);
		 } else {
		   msg = "N/A";
		 }

		 // Free the json object when the reference count goes to 0.
		 curlReturnCode = 0;
	     ostringstream hrc;
	     hrc << httpResponseCode;
	     httpReasonString = "rc=" + hrc.str() + ", msg=" + string(msg);
		 json_object_put(jo);
		 cloudantResult = false;
	  }

	  // If we have the document read successfully from the Cloudant server, let us parse the field that the caller asked for.
	  if (httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
			json_object *jo = json_tokener_parse(curlBuffer);
			json_object *joForField = NULL;
			json_bool exists = json_object_object_get_ex(jo, key.c_str(), &joForField);

			if (exists) {
				value = string(json_object_get_string(joForField));

				// If the caller requested for "doc_count", then that is done at the database level and
				// not at the individual document level (i.e. URL will be http:XXXX/db and not http://XXXX/db/doc).
				// In that case, our caller is requesting for "db info" which will return a JSON response that will not have the "_rev" field.
				// In such cases, we can skip the following logic to fetch the revision of the document which is not applicable for the "db info" request.
				if (key != "doc_count") {
					// Let us read the revision number now.
					exists = json_object_object_get_ex(jo, "_rev", &joForField);

					if (exists) {
						revision = string(json_object_get_string(joForField));
					} else {
						httpResponseCode = CLOUDANT_REV_FIELD_NOT_FOUND;
						ostringstream hrc;
						hrc << httpResponseCode;
						httpReasonString = "rc=" + hrc.str() + ", msg=Cloudant document field '_rev' not found.";
						cloudantResult = false;
					}
				}
			} else {
				httpResponseCode = CLOUDANT_DOC_FIELD_NOT_FOUND;
				ostringstream hrc;
				hrc << httpResponseCode;
				httpReasonString = "rc=" + hrc.str() + ", msg=Cloudant document field '" + key + "' not found.";
				cloudantResult = false;
			}

			// Release it now.
			json_object_put(jo);
	  }

	  return(cloudantResult);
  }

  // This method will delete a Cloudant document.
  // URL format should be: http://user:password@user.cloudant.com/db-name/doc (OR)
  // http://user:password@XXXXX/db-name/doc where XXXXX is a name or IP address of your
  // on-premises Cloudant Local load balancer machine.
  bool CloudantDBLayer::deleteCloudantDocument(string const & url, int32_t & curlReturnCode, string & curlErrorString,
	uint64_t & httpResponseCode, string & httpReasonString) {
	  // we have to first get the current revision number of this document.
	  // Then, we can delete this document using that revision number.
	  curlReturnCode = 0;
	  httpResponseCode = 0;
	  curlErrorString = "";
	  httpReasonString = "";

	  string key = "_id";
	  string value = "";
	  string revision = "";
	  int32_t _curlReturnCode = 0;
	  string _curlErrorString = "";
	  uint64_t _httpResponseCode = 0;
	  string _httpReasonString = "";
	  bool _cloudantResult = readCloudantDocumentField(url, key, value, revision,
		 _curlReturnCode, _curlErrorString, _httpResponseCode, _httpReasonString);

	  if (_cloudantResult == true && _httpResponseCode == CLOUDANT_DOC_RETRIEVED) {
		  // We got the revision number.
		  // Using that, we can delete this document now.
		  curlReturnCode = 0;
		  httpResponseCode = 0;
		  char *curlEffectiveUrl = NULL;
		  CURLcode result;
		  bool repeatingUrl = false;

		  // Get the URL endpoint that was used when we executed this method previously.
		  result = curl_easy_getinfo(curlForDeleteCloudantDocument, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

		  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
			  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
			  repeatingUrl = true;
		  }

		  if (repeatingUrl == false) {
			  // It is not the same URL we are connecting to again.
			  // We can't reuse the previously made cURL session. Reset it now.
			  curl_easy_reset(curlForDeleteCloudantDocument);
			  // Change the URL with the _rev query string.
			  string deleteUrl = url + "?rev=" + revision;
			  curl_easy_setopt(curlForDeleteCloudantDocument, CURLOPT_URL, deleteUrl.c_str());
			  // cURL is C based and we are here in C++.
			  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
			  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
			  curl_easy_setopt(curlForDeleteCloudantDocument, CURLOPT_WRITEDATA, this);
			  curl_easy_setopt(curlForDeleteCloudantDocument, CURLOPT_WRITEFUNCTION, &CloudantDBLayer::writeFunction);
			  curl_easy_setopt(curlForDeleteCloudantDocument, CURLOPT_CUSTOMREQUEST, HTTP_DELETE);
			  // Enable the TCP keep alive so that we don't lose the connection with the
			  // Cloudant server when no store functions are performed for long durations.
			  curl_easy_setopt(curlForDeleteCloudantDocument, CURLOPT_TCP_KEEPALIVE, 1);
			  curl_easy_setopt(curlForDeleteCloudantDocument, CURLOPT_HTTPHEADER, headersForDeleteCloudantDocument);
		  }

		  curlBufferOffset = 0;
		  result = curl_easy_perform(curlForDeleteCloudantDocument);
		  curlBuffer[curlBufferOffset] = '\0';

		  if (result != CURLE_OK) {
			  curlReturnCode = result;
			  ostringstream crc;
			  crc << curlReturnCode;
			  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
			  return(false);
		  }

		  curl_easy_getinfo(curlForDeleteCloudantDocument, CURLINFO_RESPONSE_CODE, &httpResponseCode);
		  bool cloudantResult = true;
		  return(cloudantResult);
	  } else {
		  curlReturnCode = _curlReturnCode;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + _curlErrorString;
		  httpResponseCode = _httpResponseCode;
		  ostringstream hrc;
		  hrc << httpResponseCode;

		  if (httpResponseCode == CLOUDANT_DOC_NOT_FOUND) {
			  httpReasonString = "rc=" + hrc.str() + ", msg=K/V data item not found in Cloudant DB.";
		  } else {
			  httpReasonString = "rc=" + hrc.str() + ", msg=" + _httpReasonString;
		  }
		  return(_cloudantResult);
	  }
  }

  // This method will return all the keys found in the given Cloudant database.
  // URL format should be: http://user:password@user.cloudant.com/db-name/_all_docs (OR)
  // http://user:password@XXXXX/db-name/_all_docs where XXXXX is a name or IP address of your
  // on-premises Cloudant Local load balancer machine.
  bool CloudantDBLayer::getAllDocsFromCloudantDatabase(string const & url, std::vector<std::string> & dataItemKeys, int32_t & curlReturnCode,
	string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString) {
	  curlReturnCode = 0;
	  httpResponseCode = 0;
	  curlErrorString = "";
	  httpReasonString = "";
	  char *curlEffectiveUrl = NULL;
	  CURLcode result;
	  bool repeatingUrl = false;

	  // Get the URL endpoint that was used when we executed this method previously.
	  result = curl_easy_getinfo(curlForGetAllDocsFromCloudantDatabase, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForGetAllDocsFromCloudantDatabase);
		  curl_easy_setopt(curlForGetAllDocsFromCloudantDatabase, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
		  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
		  curl_easy_setopt(curlForGetAllDocsFromCloudantDatabase, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForGetAllDocsFromCloudantDatabase, CURLOPT_WRITEFUNCTION, &CloudantDBLayer::writeFunction);
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // Cloudant server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForGetAllDocsFromCloudantDatabase, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForGetAllDocsFromCloudantDatabase, CURLOPT_HTTPHEADER, headersForGetAllDocsFromCloudantDatabase);
	  }

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForGetAllDocsFromCloudantDatabase);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForGetAllDocsFromCloudantDatabase, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool cloudantResult = true;

	  // HTTP response codes: 200-->Doc retrieval successful, 404-->Document can't be found
	  if (httpResponseCode != CLOUDANT_DOC_RETRIEVED) {
		  // Some problem in retrieving the document.
		  json_object *jo = json_tokener_parse(curlBuffer);
		  json_object *joForField = NULL;
		  // Third argument to this API will receive the reference to the json_object associated
		  // with the given field name. Ownership of this object is retained by the first argument
		  // i.e. the parent json_object.
		  json_bool exists = json_object_object_get_ex(jo, "reason", &joForField);
		  const char *msg;

		  if (exists) {
			  msg = json_object_get_string(joForField);
		  } else {
			  msg = "N/A";
		  }

		  // Free the json object when the reference count goes to 0.
		  curlReturnCode = 0;
		  ostringstream hrc;
		  hrc << httpResponseCode;
		  httpReasonString = "rc=" + hrc.str() + ", msg=" + string(msg);
		  json_object_put(jo);
		  cloudantResult = false;
	  } else {
		  // If we have all the documents read successfully from the Cloudant server, let us parse them.
		  // Refer to the Cloudant documentation to learn about the format of the return JSON for the _all_docs API.
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
							json_object *keyObj;
							// Get the "key" field in the row element JSON. This field holds our store key.
							exists =  json_object_object_get_ex(rowObj, "key", &keyObj);

							if (exists) {
								const char *data_item_key_ptr = json_object_get_string(keyObj);
								string data_item_key = string(data_item_key_ptr);
								// Every dps store will have three mandatory reserved data item keys for internal use.
								// Let us not add them to the list of docs (i.e. store keys).
								if (data_item_key.compare(CLOUDANT_STORE_ID_TO_STORE_NAME_KEY) == 0) {
									continue; // Skip this one.
								} else if (data_item_key.compare(CLOUDANT_SPL_TYPE_NAME_OF_KEY) == 0) {
									continue; // Skip this one.
								} else if (data_item_key.compare(CLOUDANT_SPL_TYPE_NAME_OF_VALUE) == 0) {
									continue; // Skip this one.
								}

								dataItemKeys.push_back(string(data_item_key));
							} else {
								httpResponseCode = CLOUDANT_DOC_FIELD_NOT_FOUND;
								ostringstream hrc;
								hrc << httpResponseCode;
								httpReasonString = "rc=" + hrc.str() + ", msg=Cloudant document field 'key' not found.";
								cloudantResult = false;
								break;
							}
						} // End of for loop
					} else {
						httpResponseCode = CLOUDANT_DOC_FIELD_NOT_FOUND;
						ostringstream hrc;
						hrc << httpResponseCode;
						httpReasonString = "rc=" + hrc.str() + ", msg=Cloudant document field 'rows' not found.";
						cloudantResult = false;
					}
				} else {
					// It is an empty store? This is not correct.
					// Because, we must at least have our 3 meta data entries.
					httpResponseCode = CLOUDANT_DOC_FIELD_NOT_FOUND;
					ostringstream hrc;
					hrc << httpResponseCode;
					httpReasonString = "rc=" + hrc.str() + ", msg=Cloudant document points to an empty store. This is not correct.";
					cloudantResult = false;
				}
			} else {
				httpResponseCode = CLOUDANT_DOC_FIELD_NOT_FOUND;
				ostringstream hrc;
				hrc << httpResponseCode;
				httpReasonString = "rc=" + hrc.str() + ", msg=Cloudant document field 'total_rows' not found.";
				cloudantResult = false;
			}

			// Release it now.
			json_object_put(jo);
	  }

	  return(cloudantResult);
  }

  // This method will return the status of the connection to the back-end data store.
  bool CloudantDBLayer::isConnected() {
          // Not implemented at this time.
          return(true);
  }

  bool CloudantDBLayer::reconnect(std::set<std::string> & dbServers, PersistenceError & dbError) {
          // Not implemented at this time.
          return(true);
  }

} } } } }
using namespace com::ibm::streamsx::store;
using namespace com::ibm::streamsx::store::distributed;
extern "C" {
	DBLayer * create(){
	   return new CloudantDBLayer();
	}
}
