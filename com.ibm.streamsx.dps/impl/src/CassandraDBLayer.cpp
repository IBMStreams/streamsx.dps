/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
/*
=====================================================================
Here is the copyright statement for our use of the Cassandra APIs:

Copyright (c) 2014 DataStax

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is
distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing permissions and limitations under the
License.
=====================================================================
*/

/*
==================================================================================================================
This CPP file contains the source code for the distributed process store (dps) back-end activities such as
insert, update, read, remove, etc. This dps implementation runs on top of the popular Cassandra database.
Cassandra is a great open source effort that carries Apache license.

http://cassandra.apache.org/

Cassandra is a full fledged database with clustering and fault tolerance support. In our Cassandra store implementation for
Streams, we are using APIs from the Cassandra CPP driver.

Any dpsXXXXX native function call made from a SPL composite will go through a serialization layer and then hit
this CPP file. Here, the purpose of such SPL native function calls will be fulfilled using the Cassandra APIs.
After that, the results will be sent to a deserialization layer. From there, results will be transformed using the
correct SPL types and delivered back to the SPL composite. In general, our distributed process store provides
a "global + distributed" cache for different processes (multiple PEs from one or more Streams applications).
We provide a set of free for all native function APIs to create/read/update/delete data items on one or more stores.
In the worst case, there could be multiple writers and multiple readers for the same store.
It is important to note that a Streams application designer/developer should carefully address how different parts
of his/her application will access the store simultaneously i.e. who puts what, who gets what and at
what frequency from where etc.

This C++ project has a companion SPL project (058_data_sharing_between_non_fused_spl_custom_and_cpp_primitive_operators).
Please refer to the commentary in that SPL project file for learning about the procedure to do an
end-to-end test run involving the SPL code, serialization/deserialization code,
Cassandra interface code (this file), and your Cassandra infrastructure.

As a first step, you should run the ./mk script from the C++ project directory (DistributedProcessStoreLib).
That will take care of building the .so file for the dps and copy it to the SPL project's impl/lib directory.
==================================================================================================================
*/

#include "CassandraDBLayer.h"
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
#include <algorithm>
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
using namespace SPL;
using namespace streams_boost::archive::iterators;

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  CassandraDBLayer::CassandraDBLayer()
  {
	  mainTableName = string(CASSANDRA_DPS_KEYSPACE) + "." + string(CASSANDRA_DPS_MAIN_TABLE);
	  lockRowName = string(DPS_LOCK_TOKEN) + "_" + string(DL_LOCK_TOKEN);
	  session = NULL;
	  cluster = NULL;
  }

  CassandraDBLayer::~CassandraDBLayer()
  {
    // Clear the Cassandra connection and session.
	if (session != NULL) {
		CassFuture* close_future = NULL;
		close_future = cass_session_close(session);
		cass_future_wait(close_future);
		cass_future_free(close_future);
		cass_cluster_free(cluster);
		// Explicitly free the session object.
		cass_session_free(session);
		cluster = NULL;
		session = NULL;
	}
  }
        
  void CassandraDBLayer::connectToDatabase(std::set<std::string> const & dbServers, PersistenceError & dbError)
  {
	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase", "CassandraDBLayer");

	  // Get the name, OS version and CPU type of this machine.
	  struct utsname machineDetails;

	  if(uname(&machineDetails) < 0) {
		  dbError.set("Unable to get the machine/os/cpu details.", DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to get the machine/os/cpu details. " << DPS_INITIALIZE_ERROR, "CassandraDBLayer");
		  return;
	  } else {
		  nameOfThisMachine = string(machineDetails.nodename);
		  osVersionOfThisMachine = string(machineDetails.sysname) + string(" ") + string(machineDetails.release);
		  cpuTypeOfThisMachine = string(machineDetails.machine);
	  }

	  string cassandraConnectionErrorMsg = "Unable to initialize the Cassandra session.";
	  // Let us connect to any one of the active Cassandra servers.
	  // Cassandra supports a server cluster. Hence, user may provide one or more server names.
	  // Form a string with all the servers listed in a CSV format: server1,server2,server3
	  std::string serverNames = "";
	  for (std::set<std::string>::iterator it=dbServers.begin(); it!=dbServers.end(); ++it) {
		 if (serverNames != "") {
			 // If there are multiple servers, add a comma next to the most recent server name we added.
			 serverNames += ",";
		 }

		 serverNames += *it;
	  }

	  // Create a Cassandra cluster object and set contact points to the servers in that cluster.
	  cluster = NULL;
	  cluster = cass_cluster_new();
	  cass_cluster_set_contact_points(cluster, serverNames.c_str());

	  if (cluster == NULL) {
		  cassandraConnectionErrorMsg = "Unable to set Cassandra cluster contact points.";
		  dbError.set(cassandraConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error. Msg=" <<
			cassandraConnectionErrorMsg << ". " << DPS_INITIALIZE_ERROR, "CassandraDBLayer");
		  return;
	  }

      // Adjust the cluster configuration based on your throughput needs.
      // This number needs to scale w/ the number of request per sec
      cass_cluster_set_queue_size_io(cluster, 8192);  // Default: 4096
       // Same with this
      cass_cluster_set_pending_requests_high_water_mark(cluster, 128*5);  // Use this formula: 128 * max_conn_per_host
      // This number can vary per number of cores and benchmarking, beta5 will make this scale better
      cass_cluster_set_num_threads_io(cluster, 5);  // Default: 2
      cass_cluster_set_core_connections_per_host(cluster, 5); // Default: 2
      cass_cluster_set_max_connections_per_host(cluster, 5);  // Default: 1
      cass_cluster_set_connect_timeout(cluster, 5000);  // Default: 5000 milliseconds

	  // Create a Cassandra session for the current Streams PE process that is accessing the DPS API layer.
      session = cass_session_new();

      if (session == NULL) {
		  // Error occurred while creating a session.
		  // Close the cluster.
		  cass_cluster_free(cluster);
		  cluster = NULL;
    	  cassandraConnectionErrorMsg = "Unable to create a Cassandra session.";
		  dbError.set(cassandraConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error. Msg=" <<
			  cassandraConnectionErrorMsg << ". " << DPS_INITIALIZE_ERROR, "CassandraDBLayer");
		  return;
      }

      // Connection the session to the cluster.
      CassFuture* connect_future = cass_session_connect(session, cluster);

      if (connect_future == NULL) {
    	  // Error in getting a handle while connecting the session to the cluster.
    	  CassFuture* close_future = NULL;
    	  close_future = cass_session_close(session);
    	  cass_future_wait(close_future);
    	  cass_future_free(close_future);
    	  cass_cluster_free(cluster);
    	  // Explicitly free the session object.
    	  cass_session_free(session);
    	  cluster = NULL;
    	  session = NULL;
    	  cassandraConnectionErrorMsg = "Unable to get a handle while connecting a Cassandra session with a Cassandra cluster.";
		  dbError.set(cassandraConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error. Msg=" <<
			  cassandraConnectionErrorMsg << ". " << DPS_INITIALIZE_ERROR, "CassandraDBLayer");
		  return;
      }

      // Cassandra Session and Cassandra Cluster are connected and ready now.
      cass_future_wait(connect_future);
      CassError rc = cass_future_error_code(connect_future);

      if(rc != CASS_OK) {
        CassString message = cass_future_error_message(connect_future);
        cassandraConnectionErrorMsg = std::string(message.data, message.length);
        cass_future_free(connect_future);
        // Error while connecting the session to the cluster.
        CassFuture* close_future = NULL;
        close_future = cass_session_close(session);
        cass_future_wait(close_future);
        cass_future_free(close_future);
        cass_cluster_free(cluster);
        // Explicitly free the session object.
        cass_session_free(session);
        cluster = NULL;
        session = NULL;
        cassandraConnectionErrorMsg = "Unable to connect a Cassandra session with a Cassandra cluster.";
        	dbError.set(cassandraConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error. Msg=" <<
			cassandraConnectionErrorMsg << ". " << DPS_INITIALIZE_ERROR, "CassandraDBLayer");
		return;
      }

      cass_future_free(connect_future);

	  // Reset the error string.
	  cassandraConnectionErrorMsg = "";

	  /*
		// IMPORTANT FIRST STEP THAT USERS MUST PERFORM IN ORDER TO USE CASSANDRA AS A DATA STORE IN DPS.
		1) Pick any one of your Cassandra machines in your Cassandra environment.
		2) On that machine, change directory to <CASSANDRA_INSTALL_DIR>/bin
		3) Run this command: ./cqlsh
		4) In the CQL shell prompt, correctly type or copy/paste the following command and press enter.
		   (If you have only one Cassandra server, substitute 1 in place of XXXX below. If you have
		   more than one Cassandra server, substitute 2 in place of XXXX below. This value decides the
		   number of replicas you want to have for your data inside Cassandra.)

		   create keyspace com_ibm_streamsx_dps with replication = { 'class': 'SimpleStrategy', 'replication_factor': 'XXXX' };

		5) In that same CQL shell session, correctly type or copy/paste the following command and press enter.

		   create table com_ibm_streamsx_dps.t1(r_key text, c_key text, b_val blob, t_val text, dbsig text, info text, PRIMARY KEY (r_key, c_key));

		6) Type or copy/paste the following CQL shell commands one at a time to ensure that we got everything correctly.

		   desc keyspaces;
		   use com_ibm_streamsx_dps;
		   desc tables;
		   quit;

		// The following is the dps main table format in Cassandra.
		//
		// r_key is the partition key (a.k.a row key).
		// c_key is the column key.
		// b_val is what makes the K/V pair with c_key when we have the value as blob.
		// t_val is what makes the K/V pair with c_key when we have the value as text (Mostly used for storing meta data information).
		// dbsig is used for client thread specific unique value where needed.
		// info is used to keep any miscellaneous details (mostly used by the store locks).
		//
		// Please note that we are using a compound primary key which consists of the row_key i.e. partition key and the remaining as clustering column(s).

	  */

	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase done", "CassandraDBLayer");
  }

  CassError CassandraDBLayer::execute_query(CassSession* session, const char* query, std::string & errorMsg) {
    CassError rc = CASS_OK;
    CassFuture* future = NULL;
    CassStatement* statement = cass_statement_new(cass_string_init(query), 0);

    future = cass_session_execute(session, statement);
    cass_future_wait(future);

    rc = cass_future_error_code(future);
    if(rc != CASS_OK) {
    	CassString message = cass_future_error_message(future);
    	errorMsg = std::string(message.data, message.length);
    }

    cass_future_free(future);
    cass_statement_free(statement);
    return rc;
  }

  uint64_t CassandraDBLayer::createStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside createStore for store " << name, "CassandraDBLayer");
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);

 	// Get a general purpose lock so that only one thread can
	// enter inside of this method at any given time with the same store name.
 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
 		// Unable to acquire the general purpose lock.
 		dbError.set("Unable to get a generic lock for creating a store with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for an yet to be created store with its name as " <<
 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "CassandraDBLayer");
 		// User has to retry again to create this store.
 		return 0;
 	}

    // Let us first see if a store with the given name already exists in the Cassandra DPS GUID row.
 	uint64_t storeId = findStore(name, dbError);

 	if (storeId > 0) {
		// This store already exists in our cache.
		// We can't create another one with the same name now.
 		std::ostringstream storeIdString;
 		storeIdString << storeId;
 		// The following error message carries the existing store's id at the very end of the message.
		dbError.set("A store named " + name + " already exists with a store id " + storeIdString.str(), DPS_STORE_EXISTS);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". " << DPS_STORE_EXISTS, "CassandraDBLayer");
		releaseGeneralPurposeLock(base64_encoded_name);
		return 0;
 	} else {
 		// If the findStore method had any problems in accessing the GUID row, let us return that error now.
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
	We secured a guid. We can now create this store. Layout for a distributed process store (dps) looks like this in Cassandra.
	In the DPS GUID row, DPS store/lock row, and the individual store rows, we will follow these data formats.
	That will allow us to be as close to the other two DPS implementations using memcached and Redis.
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

 	// We will do the first two activities from the list above for our Cassandra implementation.
	// Let us now do the step 1 described in the commentary above.
	// 1) Create the Store Name entry in the dps main table.
	//    '0' + 'store name' => 'store id'
	std::ostringstream storeIdString;
	storeIdString << storeId;

	string errorMsg;
	CassError rc = CASS_OK;
	string statementString = "insert into " + mainTableName + " (r_key, c_key, t_val) values('" + string(DPS_AND_DL_GUID_KEY) +
		"', '" + string(DPS_STORE_NAME_TYPE) + base64_encoded_name + "', '" + storeIdString.str() + "');";
	rc = execute_query(session, statementString.c_str(), errorMsg);

	if (rc != CASS_OK) {
		// There was an error in creating the store name entry in the guid row.
		dbError.set("Unable to create 'StoreName-->GUID' in Cassandra for the store named " + name + ". " + errorMsg, DPS_STORE_NAME_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". Error=" << errorMsg << ". " << DPS_STORE_NAME_CREATION_ERROR, "CassandraDBLayer");
		releaseGeneralPurposeLock(base64_encoded_name);
		return 0;
	}

	// 2) Create the Store Contents wide row.
 	// This is the wide row in which we will keep storing all the key value pairs belonging to this store.
	// Row name: 'dps_' + '1_' + 'store id'
	// Every store contents row will always have these three metadata entries:
	// dps_name_of_this_store ==> 'store name'
	// dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	// dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	//
	// Every store contents row will have at least three elements in it carrying the actual store name, key spl type name and value spl type name.
	// In addition, inside this store contents row, we will house the
	// actual key:value data items that user wants to keep in this store.
	// Such a store contents row is very useful for data item read, write, deletion, enumeration etc.
 	//

	// Let us populate the new store contents row with a mandatory element that will carry the name of this store.
	// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
 	string newStoreRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString.str();
	statementString = "insert into " + mainTableName +
		" (r_key, c_key, t_val) values('" + newStoreRowName + "', '" +
		string(CASSANDRA_STORE_ID_TO_STORE_NAME_KEY) + "', '" + base64_encoded_name + "');";
	rc = execute_query(session, statementString.c_str(), errorMsg);

	if (rc != CASS_OK) {
		// There was an error in creating Meta Data 1;
		dbError.set("Unable to create 'Meta Data1' in Cassandra for the store named " + name + ". " + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "CassandraDBLayer");
		// Since there is an error here, let us delete store name entry we created above.
		statementString = "delete from " + mainTableName + " where r_key='" + string(DPS_AND_DL_GUID_KEY) +
			"' and c_key='" + string(DPS_STORE_NAME_TYPE) + base64_encoded_name + "';";
		execute_query(session, statementString.c_str(), errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return 0;
	}

    // We are now going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
	// Add the key spl type name metadata.
	string base64_encoded_keySplTypeName;
	base64_encode(keySplTypeName, base64_encoded_keySplTypeName);
	statementString = "insert into " + mainTableName +
		" (r_key, c_key, t_val) values('" + newStoreRowName + "', '" +
		string(CASSANDRA_SPL_TYPE_NAME_OF_KEY) + "', '" + base64_encoded_keySplTypeName + "');";
	rc = execute_query(session, statementString.c_str(), errorMsg);

	if (rc != CASS_OK) {
		// There was an error in creating Meta Data 2;
		dbError.set("Unable to create 'Meta Data2' in Cassandra for the store named " + name + ". " + errorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "CassandraDBLayer");
		// Since there is an error here, let us delete store name entry we created above.
		statementString = "delete from " + mainTableName + " where r_key='" + string(DPS_AND_DL_GUID_KEY) +
			"' and c_key='" + string(DPS_STORE_NAME_TYPE) + base64_encoded_name + "';";
		execute_query(session, statementString.c_str(), errorMsg);
		// We will also delete the meta data 1 we created above.
		statementString = "delete from " + mainTableName + " where r_key='" + newStoreRowName +
			"' and c_key='" + string(CASSANDRA_STORE_ID_TO_STORE_NAME_KEY) + "';";
		execute_query(session, statementString.c_str(), errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return 0;
	}

	// Add the value spl type name metadata.
	string base64_encoded_valueSplTypeName;
	base64_encode(valueSplTypeName, base64_encoded_valueSplTypeName);
	statementString = "insert into " + mainTableName +
		" (r_key, c_key, t_val) values('" + newStoreRowName + "', '" +
		string(CASSANDRA_SPL_TYPE_NAME_OF_VALUE) + "', '" + base64_encoded_valueSplTypeName + "');";
	rc = execute_query(session, statementString.c_str(), errorMsg);

	if (rc != CASS_OK) {
		// There was an error in creating Meta Data 3;
		dbError.set("Unable to create 'Meta Data3' in Cassandra for the store named " + name + ". " + errorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for store " << name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "CassandraDBLayer");
		// Since there is an error here, let us delete store name entry we created above.
		statementString = "delete from " + mainTableName + " where r_key='" + string(DPS_AND_DL_GUID_KEY) +
			"' and c_key='" + string(DPS_STORE_NAME_TYPE) + base64_encoded_name + "';";
		execute_query(session, statementString.c_str(), errorMsg);
		// We will also delete the meta data 1 we created above.
		statementString = "delete from " + mainTableName + " where r_key='" + newStoreRowName +
			"' and c_key='" + string(CASSANDRA_STORE_ID_TO_STORE_NAME_KEY) + "';";
		execute_query(session, statementString.c_str(), errorMsg);
		// We will also delete the meta data 2 we created above.
		statementString = "delete from " + mainTableName + " where r_key='" + newStoreRowName +
			"' and c_key='" + string(CASSANDRA_SPL_TYPE_NAME_OF_KEY) + "';";
		execute_query(session, statementString.c_str(), errorMsg);
		releaseGeneralPurposeLock(base64_encoded_name);
		return 0;
	} else {
		// We created a new store as requested by the caller. Return the store id.
		releaseGeneralPurposeLock(base64_encoded_name);
		return(storeId);
	}
  }

  uint64_t CassandraDBLayer::createOrGetStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	// We will rely on a method above this to accomplish what is needed here.
	SPLAPPTRC(L_DEBUG, "Inside createOrGetStore for store " << name, "CassandraDBLayer");
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
	//  Cassandra DPS implementation. For memcached and Redis, we do it differently.)
	dbError.reset();
	// We will take the hashCode of the encoded store name and use that as the store id.
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);
	storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);
	return(storeId);
  }
                
  uint64_t CassandraDBLayer::findStore(std::string const & name, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside findStore for store " << name, "CassandraDBLayer");
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);

	// I tried below three different approaches to find if a store already exists.
	// I picked one of those three and stayed with that.
	// Remaining two approaches are commented out below.
	/*
	// This is the first approach I tried.
    // Let us first see if a table exists for the given store name in the Cassandra database.
    // Another approach is here to check if a table exists in the Cassandra DB.
	// This simply checks if the select statement succeeds on a
	// table intended for the given store name. If the result is positive, then we can claim that this
	// store already exists without checking the rows any further. If there are thousands of rows in this
	// table, this may affect the time to return a result back to the caller. Hence, I didn't prefer this one.
	// This code works only when having individual stores tables. Since we moved to a single
	// Cassandra table (t1) with wide rows, this code requires change to look inside the t1 table.
	// Hence, I'm not using this approach.
	uint64 storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);
 	std::ostringstream storeTableName;
	storeTableName << string(CASSANDRA_DPS_KEYSPACE) << "." << string(DPS_TOKEN) << "_" << string(DPS_STORE_INFO_TYPE) << "_" << storeId;
 	std::string statementString = "select key from " + storeTableName.str() + ";";
 	CassError rc = CASS_OK;
 	CassStatement* statement = NULL;
 	CassFuture* future = NULL;
 	CassString query = cass_string_init(statementString.c_str());
 	statement = cass_statement_new(query, 0);
 	future = cass_session_execute(session, statement);
 	cass_future_wait(future);
 	boolean storeExists = true;

 	rc = cass_future_error_code(future);

 	if(rc != CASS_OK) {
    	CassString message = cass_future_error_message(future);
		// Unable to get an existing store id from the cache.
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed to locate the store " << name << ". Error=" <<
			std::string(message.data, message.length) << DPS_GET_STORE_ID_ERROR, "CassandraDBLayer");
		storeExists = false;
 	}

 	cass_future_free(future);
 	cass_statement_free(statement);

 	if (storeExists == true) {
 	 	// If we reached here, that means the store table is there.
 	 	// Hence the store exists.
 		return(storeId);
 	} else {
 		return(0);
 	}
	*/

	/*
 	// This is the second approach I tried.
    // Let us first see if a table exists for the given store name in the Cassandra database.
 	// We can check in the system keyspace of the Cassandra DB.
	// This will get all the existing tables available in the dps keyspace. Then, we can filter and see if a
	// store table already exists for the given store name.
	// If there are too many dps stores, the while loop below may take some time. Please be aware of it.
	// I decided to use this one among all the three approaches I tried.
	// After three weeks, I had to stop using this approach, because we moved to a
	// single Cassandra table (t1) with wide rows. Hence, this approach will not meet our needs in
	// finding a store.
	//
	uint64 storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);
 	std::ostringstream storeTableName;
	storeTableName << string(DPS_TOKEN) << "_" << string(DPS_STORE_INFO_TYPE) << "_" << storeId;
 	std::string statementString = "select columnfamily_name from system.schema_columnfamilies where keyspace_name='" +
 		string(CASSANDRA_DPS_KEYSPACE) + "';";
 	CassError rc = CASS_OK;
 	CassStatement* statement = NULL;
 	CassFuture* future = NULL;
 	CassString query = cass_string_init(statementString.c_str());
 	statement = cass_statement_new(query, 0);
 	future = cass_session_execute(session, statement);
 	cass_future_wait(future);
 	boolean storeExists = false;

 	rc = cass_future_error_code(future);

 	if(rc != CASS_OK) {
    	CassString message = cass_future_error_message(future);
		// Unable to get an existing store id from the cache.
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed to locate the store " << name << ". Error=" <<
			std::string(message.data, message.length) << DPS_GET_STORE_ID_ERROR, "CassandraDBLayer");
 	} else {
 		const CassResult* result = cass_future_get_result(future);
 		CassIterator* iterator = cass_iterator_from_result(result);
 		CassString tableName;

 	    while(cass_iterator_next(iterator)) {
			const CassRow* row = cass_iterator_get_row(iterator);
			cass_value_get_string(cass_row_get_column(row, 0), &tableName);
			SPLAPPTRC(L_DEBUG, "Inside findStore: Store=" << string(tableName.data, tableName.length), "CassandraDBLayer");

			if (string(tableName.data, tableName.length) == storeTableName.str()) {
				// We found an existing store for the user provided store name.
				storeExists = true;
				break;
			}
 	    }

 	    cass_result_free(result);
 	    cass_iterator_free(iterator);
 	}

 	cass_future_free(future);
 	cass_statement_free(statement);

 	if (storeExists == true) {
 	 	// If we reached here, that means the store table is there.
 	 	// Hence the store exists.
 		return(storeId);
 	} else {
 		return(0);
 	}
 	*/

 	// Very first approach I tried is this one. Sometimes, Cassandra returns zero rows when we
 	// try to look for a row in a table right after it was inserted. Hence, I didn't prefer this approach.
 	//
    // Let us first see if a store with the given name already exists in the Cassandra DPS GUID row.
 	// Inside Cassandra, all our store names will have a mapping type indicator of
	// "0" at the beginning followed by the actual store name.  "0" + 'store name'
 	std::string statementString = "select t_val from " + mainTableName +
 		" where r_key='" + string(DPS_AND_DL_GUID_KEY) + "' and c_key='" +
 		string(DPS_STORE_NAME_TYPE) + base64_encoded_name + "';";
 	CassError rc = CASS_OK;
 	int32_t columnCnt = 0;
 	CassStatement* statement = NULL;
 	CassFuture* future = NULL;
 	CassString query = cass_string_init(statementString.c_str());
 	statement = cass_statement_new(query, 0);
 	future = cass_session_execute(session, statement);
 	cass_future_wait(future);

 	rc = cass_future_error_code(future);

 	if(rc != CASS_OK) {
    	CassString message = cass_future_error_message(future);
		// Unable to get an existing store id from the cache.
		dbError.set("Unable to get the storeId for the storeName " + name + ". " + std::string(message.data, message.length), DPS_STORE_EXISTENCE_CHECK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed for store " << name << ". " << DPS_STORE_EXISTENCE_CHECK_ERROR, "CassandraDBLayer");
	 	cass_future_free(future);
	 	cass_statement_free(statement);
		return (0);
 	} else {
 		const CassResult* result = cass_future_get_result(future);
 		CassIterator* iterator = cass_iterator_from_result(result);

 	    if(cass_iterator_next(iterator)) {
 	    	// We at least have one resulting column.
 	    	// That means we have a column containing our store name in the GUID row.
 	    	columnCnt++;
 	    }

 	    cass_result_free(result);
 	    cass_iterator_free(iterator);
 	}

 	cass_future_free(future);
 	cass_statement_free(statement);

 	if (columnCnt > 0) {
		// This store already exists in our cache.
 	 	// We will take the hashCode of the encoded store name and use that as the store id.
 	 	uint64_t storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);
 	 	return(storeId);
 	} else {
		// Requested data item is not there in the cache.
		dbError.set("The requested store " + name + " doesn't exist.", DPS_DATA_ITEM_READ_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed for store " << name << ". " << DPS_DATA_ITEM_READ_ERROR, "CassandraDBLayer");
		return(0);
 	}
  }
        
  bool CassandraDBLayer::removeStore(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside removeStore for store id " << store, "CassandraDBLayer");
	ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CassandraDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "CassandraDBLayer");
		// User has to retry again to remove the store.
		return(false);
	}

	// Get rid of these two entries that are holding the store contents hash and the store name root entry.
	// 1) Store Contents row
	// 2) Store Name root entry
	//
	uint32_t dataItemCnt = 0;
	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		releaseStoreLock(storeIdString);
		// This is alarming. This will put this store in a bad state. Poor user has to deal with it.
		return(false);
	}

	// Let us delete the Store Contents Hash that contains all the active data items in this store.
	// Row name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string statementString = "delete from " + mainTableName + " where r_key='" + storeRowName + "';";
	string errorMsg;
	execute_query(session, statementString.c_str(), errorMsg);

	// Finally, delete the StoreName key now.
	statementString = "delete from " + mainTableName + " where r_key='" + string(DPS_AND_DL_GUID_KEY) +
		"' and c_key='" + string(DPS_STORE_NAME_TYPE) + storeName + "';";
	execute_query(session, statementString.c_str(), errorMsg);

	// Life of this store ended completely with no trace left behind.
	releaseStoreLock(storeIdString);
    return(true);
  }

  // This is a lean and mean put operation into a store.
  // It doesn't do any safety checks before putting a data item into a store.
  // If you want to go through that rigor, please use the putSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool CassandraDBLayer::put(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside put for store id " << store, "CassandraDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In our Cassandra dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Contents row that takes the following name.
	// Row name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	// To support space characters in the data item key, let us base64 encode it.
	string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string statementString = "insert into " + mainTableName + " (r_key, c_key, b_val) values(?, ?, ?);";

	CassString query = cass_string_init(statementString.c_str());
    CassError rc = CASS_OK;
    CassFuture* future = NULL;
    CassStatement* statement = cass_statement_new(query, 3);
    cass_statement_bind_string(statement, 0, cass_string_init(storeRowName.c_str()));
    cass_statement_bind_string(statement, 1, cass_string_init(base64_encoded_data_item_key.c_str()));
    cass_statement_bind_bytes(statement, 2, cass_bytes_init(valueData, valueSize));

    future = cass_session_execute(session, statement);
    cass_future_wait(future);

    rc = cass_future_error_code(future);
    string errorMsg;

    if(rc != CASS_OK) {
    	CassString message = cass_future_error_message(future);
    	errorMsg = std::string(message.data, message.length);
    }

    cass_future_free(future);
    cass_statement_free(statement);

    if (rc != CASS_OK) {
		// Problem in storing a data item in the cache.
		dbError.set("Unable to store a data item in the store id " + storeIdString + ". " + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed for store id " << storeIdString << ". Error=" << errorMsg << ". " << DPS_DATA_ITEM_WRITE_ERROR, "CassandraDBLayer");
		return(false);
    } else {
    	return(true);
    }
  }

  // This is a special bullet proof version that does several safety checks before putting a data item into a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular put method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool CassandraDBLayer::putSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside putSafe for store id " << store, "CassandraDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CassandraDBLayer");
		}

		return(false);
	}

	// In our Cassandra dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "CassandraDBLayer");
		// User has to retry again to put the data item in the store.
		return(false);
	}

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Contents row that takes the following name.
	// Row name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	// To support space characters in the data item key, let us base64 encode it.
	string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string statementString = "insert into " + mainTableName + " (r_key, c_key, b_val) values(?, ?, ?);";

	CassString query = cass_string_init(statementString.c_str());
    CassError rc = CASS_OK;
    CassFuture* future = NULL;
    CassStatement* statement = cass_statement_new(query, 3);
    cass_statement_bind_string(statement, 0, cass_string_init(storeRowName.c_str()));
    cass_statement_bind_string(statement, 1, cass_string_init(base64_encoded_data_item_key.c_str()));
    cass_statement_bind_bytes(statement, 2, cass_bytes_init(valueData, valueSize));

    future = cass_session_execute(session, statement);
    cass_future_wait(future);

    rc = cass_future_error_code(future);
    string errorMsg;

    if(rc != CASS_OK) {
    	CassString message = cass_future_error_message(future);
    	errorMsg = std::string(message.data, message.length);
    }

    cass_future_free(future);
    cass_statement_free(statement);

    if (rc != CASS_OK) {
		// Problem in storing a data item in the cache.
		dbError.set("Unable to store a data item in the store id " + storeIdString + ". " + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed for store id " << storeIdString << ". " << DPS_DATA_ITEM_WRITE_ERROR, "CassandraDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
    } else {
    	releaseStoreLock(storeIdString);
    	return(true);
    }
  }

  // Put a data item with a TTL (Time To Live in seconds) value into the global area of the Cassandra DB.
  bool CassandraDBLayer::putTTL(char const * keyData, uint32_t keySize,
		  	  	  	  	    unsigned char const * valueData, uint32_t valueSize,
							uint32_t ttl, PersistenceError & dbError, bool encodeKey, bool encodeValue)
  {
	  SPLAPPTRC(L_DEBUG, "Inside putTTL.", "CassandraDBLayer");

	  std::ostringstream ttlValue;
	  ttlValue << ttl;

	  // In our Cassandra dps implementation, data item keys can have space characters.
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

	  // We are ready to either store a new data item or update an existing data item with a TTL value specified in seconds.
	  string storeRowName = string(DPS_TTL_STORE_TOKEN);
	  string statementString = "";

	  // User is allowed to give a TTL value of 0 for which there will be no TTL expiration.
	  // User probably wants to use the dpsXXXXTTL APIs instead of the other store based APIs for the sake of simplicity.
	  // In that case, we will let the user store in the global area for their K/V pairs to remain forever or
	  // until they are deleted by the user. No TTL effect needed here.
	  statementString = "insert into " + mainTableName + " (r_key, c_key, b_val) values(?, ?, ?) using ttl " + ttlValue.str() + ";";

	  CassString query = cass_string_init(statementString.c_str());
	  CassError rc = CASS_OK;
	  CassFuture* future = NULL;
	  CassStatement* statement = cass_statement_new(query, 3);
	  cass_statement_bind_string(statement, 0, cass_string_init(storeRowName.c_str()));
	  // Let us base64 encode the key.
	  cass_statement_bind_string(statement, 1, cass_string_init(base64_encoded_data_item_key.c_str()));
	  cass_statement_bind_bytes(statement, 2, cass_bytes_init(valueData, valueSize));

	  future = cass_session_execute(session, statement);
	  cass_future_wait(future);

	  rc = cass_future_error_code(future);
	  string errorMsg;

	  if(rc != CASS_OK) {
		 CassString message = cass_future_error_message(future);
		 errorMsg = std::string(message.data, message.length);
	  }

	  cass_future_free(future);
	  cass_statement_free(statement);

	  if (rc != CASS_OK) {
		 // Problem in storing a data item in the cache.
		 dbError.setTTL("Unable to store a data item with TTL. " + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		 SPLAPPTRC(L_DEBUG, "Inside putTTL, it failed to store a data item with TTL. Error=" << errorMsg << ". " << DPS_DATA_ITEM_WRITE_ERROR, "CassandraDBLayer");
		 return(false);
	  } else {
		 return(true);
	  }
  }

  // This is a lean and mean get operation from a store.
  // It doesn't do any safety checks before getting a data item from a store.
  // If you want to go through that rigor, please use the getSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool CassandraDBLayer::get(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside get for store id " << store, "CassandraDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed in-memory store.
	// Every data item's key must be prefixed with its store id before being used for CRUD operation in the Cassandra store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In our Cassandra dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);

	bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		false, true, valueData, valueSize, dbError);

	if ((result == false) || (dbError.hasError() == true)) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside get, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
	}

    return(result);
  }

  // This is a special bullet proof version that does several safety checks before getting a data item from a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular get method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool CassandraDBLayer::getSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside getSafe for store id " << store, "CassandraDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed in-memory store.
	// Every data item's key must be prefixed with its store id before being used for CRUD operation in the Cassandra store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CassandraDBLayer");
		}

		return(false);
	}

	// In our Cassandra dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);

	bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		false, false, valueData, valueSize, dbError);

	if ((result == false) || (dbError.hasError() == true)) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
	}

    return(result);
  }

  // Get a TTL based data item that is stored in the global area of the Cassandra DB.
  bool CassandraDBLayer::getTTL(char const * keyData, uint32_t keySize,
                              unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError, bool encodeKey)
  {
	  SPLAPPTRC(L_DEBUG, "Inside getTTL.", "CassandraDBLayer");

	  // In our Cassandra dps implementation, data item keys can have space characters.
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

	  // Since this is a data item with TTL, it is stored in the global area of Cassandra and not inside a user created store.
	  string storeRowName = string(DPS_TTL_STORE_TOKEN);
	  std::string statementString = "select b_val from " + mainTableName + " where r_key='" + storeRowName +
	     "' and c_key='" + base64_encoded_data_item_key + "';";
	  CassError rc = CASS_OK;
	  int32_t columnCnt = 0;
	  CassStatement* statement = NULL;
	  CassFuture* future = NULL;
	  CassString query = cass_string_init(statementString.c_str());
	  statement = cass_statement_new(query, 0);
	  future = cass_session_execute(session, statement);
	  cass_future_wait(future);
	  CassBytes value;
	  valueSize = 0;

	  rc = cass_future_error_code(future);

	  if(rc != CASS_OK) {
		  // The store contents row probably doesn't exist.
		  CassString message = cass_future_error_message(future);
		  cass_future_free(future);
		  cass_statement_free(statement);
		  // Unable to get the requested data item from the cache.
		  dbError.setTTL("Unable to access the TTL K/V table in Cassandra. Error=" +
		     std::string(message.data, message.length), DPS_DATA_ITEM_READ_ERROR);
		  return (false);
	  } else {
		  const CassResult* result = cass_future_get_result(future);
		  CassIterator* iterator = cass_iterator_from_result(result);

		  if(cass_iterator_next(iterator)) {
			  // We at least have one resulting column.
			  // That means we have a column containing our value.
			  columnCnt++;
			  const CassRow* row = cass_iterator_get_row(iterator);
			  cass_value_get_bytes(cass_row_get_column(row, 0), &value);

			  // Data item value read from the store will be in this format: 'value'
			  if ((unsigned)value.size == 0) {
				  // User stored empty data item value in the cache.
				  valueData = (unsigned char *)"";
				  valueSize = 0;
			  } else {
				  // We can allocate memory for the exact length of the data item value.
				  valueSize = value.size;
				  valueData = (unsigned char *) malloc(valueSize);

				  if (valueData == NULL) {
					  // Unable to allocate memory to transfer the data item value.
					  dbError.setTTL("Unable to allocate memory to copy the data item value.", DPS_GET_DATA_ITEM_MALLOC_ERROR);
					  valueSize = 0;
				  } else {
					  // We expect the caller of this method to free the valueData pointer.
					  memcpy(valueData, value.data, valueSize);
				  }
			  }
		  }

		  cass_result_free(result);
		  cass_iterator_free(iterator);
	  }

	  cass_future_free(future);
	  cass_statement_free(statement);

	  if (columnCnt <= 0) {
		  // This data item doesn't exist. Let us raise an error.
		  // Requested data item is not there in the cache.
		  dbError.setTTL("The requested TTL based data item doesn't exist.", DPS_DATA_ITEM_READ_ERROR);
		  return(false);
	  } else {
		  if (valueData == NULL) {
			  // We encountered malloc error above. Only then, we will have a null value data.
			  return(false);
		  } else {
			  return(true);
		  }
	  }
  }

  bool CassandraDBLayer::remove(uint64_t store, char const * keyData, uint32_t keySize,
                                PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside remove for store id " << store, "CassandraDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CassandraDBLayer");
		}

		return(false);
	}

	// In our Cassandra dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "CassandraDBLayer");
		// User has to retry again to remove the data item from the store.
		return(false);
	}

	// This action is performed on the Store Contents row that takes the following name.
	// Row name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string errorMsg = "";
	// Delete the data item now.
	string statementString = "delete from " + mainTableName + " where r_key='" + storeRowName +
		"' and c_key='" + base64_encoded_data_item_key + "';";
	execute_query(session, statementString.c_str(), errorMsg);
	// Let us ensure that it really removed the requested data item.
	// Note: In Cassandra, a delete command always succeeds irrespective of whether a row exists in the row or not.
	// Hence, we will not be able to detect whether the remove failed for some reason. In the SPL code that calls this dpsRemove method
	// may wrongly report (or not report) if the developer uses the SPL assert API in his or her SPL code to verify their execution logic.
	if (errorMsg != "") {
		// Something is not correct here. It didn't remove the data item. Raise an error.
		dbError.set("Unable to remove the requested data item from the store id " + storeIdString + ". Error = " + errorMsg, DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed to remove the requested data item from the store id " << storeIdString <<
			". Error = " << errorMsg << ". " << DPS_DATA_ITEM_DELETE_ERROR, "CassandraDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// All done. An existing data item in the given store has been removed.
	releaseStoreLock(storeIdString);
    return(true);
  }

  // Remove a TTL based data item that is stored in the global area of the Cassandra DB.
  bool CassandraDBLayer::removeTTL(char const * keyData, uint32_t keySize,
                                PersistenceError & dbError, bool encodeKey)
  {
		SPLAPPTRC(L_DEBUG, "Inside removeTTL.", "CassandraDBLayer");

		// In our Cassandra dps implementation, data item keys can have space characters.
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

		string storeRowName = string(DPS_TTL_STORE_TOKEN);
		string errorMsg = "";
		// Delete the data item now.
		string statementString = "delete from " + mainTableName + " where r_key='" + storeRowName +
			"' and c_key='" + base64_encoded_data_item_key + "';";
		execute_query(session, statementString.c_str(), errorMsg);
		// Let us ensure that it really removed the requested data item.
		// Note: In Cassandra, a delete command always succeeds irrespective of whether a row exists in the row or not.
		// Hence, we will not be able to detect whether the remove failed for some reason. In the SPL code that calls this dpsRemove method
		// may wrongly report (or not report) if the developer uses the SPL assert API in his or her SPL code to verify their execution logic.
		if (errorMsg != "") {
			// Something is not correct here. It didn't remove the data item. Raise an error.
			dbError.setTTL("Unable to remove the requested data item with TTL. Error = " + errorMsg, DPS_DATA_ITEM_DELETE_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeTTL, it failed to remove the requested data item with TTL. Error = " <<
				errorMsg << ". " << DPS_DATA_ITEM_DELETE_ERROR, "CassandraDBLayer");
			return(false);
		} else {
			return(true);
		}
  }

  bool CassandraDBLayer::has(uint64_t store, char const * keyData, uint32_t keySize,
                             PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside has for store id " << store, "CassandraDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CassandraDBLayer");
		}

		return(false);
	}

	// In our Cassandra dps implementation, data item keys can have space characters.
	string base64_encoded_data_item_key;
	base64_encode(string(keyData, keySize), base64_encoded_data_item_key);
	unsigned char *dummyValueData;
	uint32_t dummyValueSize;

	// Let us see if we already have this data item in our cache.
	// Check only for the data item existence and don't fetch the data item value.
	bool dataItemAlreadyInCache = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		true, false, dummyValueData, dummyValueSize, dbError);

	if (dbError.getErrorCode() != 0) {
		SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
	}

	return(dataItemAlreadyInCache);
  }

  // Check for the existence of a TTL based data item that is stored in the global area of the Cassandra DB.
  bool CassandraDBLayer::hasTTL(char const * keyData, uint32_t keySize,
                             PersistenceError & dbError, bool encodeKey)
  {
		SPLAPPTRC(L_DEBUG, "Inside hasTTL.", "CassandraDBLayer");

		// In our Cassandra dps implementation, data item keys can have space characters.
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

		// Since this is a data item with TTL, it is stored in the global area of Cassandra and not inside a user created store.
		string storeRowName = string(DPS_TTL_STORE_TOKEN);
		std::string statementString = "select b_val from " + mainTableName + " where r_key='" +
			storeRowName + "' and c_key='" + base64_encoded_data_item_key + "';";
		CassError rc = CASS_OK;
		int32_t columnCnt = 0;
		CassStatement* statement = NULL;
		CassFuture* future = NULL;
		CassString query = cass_string_init(statementString.c_str());
		statement = cass_statement_new(query, 0);
		future = cass_session_execute(session, statement);
		cass_future_wait(future);

		rc = cass_future_error_code(future);

		if(rc != CASS_OK) {
			// The store contents row probably doesn't exist.
			CassString message = cass_future_error_message(future);
			cass_future_free(future);
			cass_statement_free(statement);
			// Unable to get the requested data item from the cache.
			dbError.setTTL("Unable to access the TTL K/V table in Cassandra. Error=" +
			   std::string(message.data, message.length), DPS_DATA_ITEM_READ_ERROR);
			return (false);
		} else {
			const CassResult* result = cass_future_get_result(future);
			CassIterator* iterator = cass_iterator_from_result(result);

			if(cass_iterator_next(iterator)) {
				// We at least have one resulting column.
				// That means we have a column containing our value.
				columnCnt++;
			}

			cass_result_free(result);
			cass_iterator_free(iterator);
		}

		cass_future_free(future);
		cass_statement_free(statement);

		if (columnCnt <= 0) {
			// This data item doesn't exist. Let us raise an error.
			// Requested data item is not there in the cache.
			return(false);
		} else {
			// TTL based data item exists.
			return(true);
		}
  }

  void CassandraDBLayer::clear(uint64_t store, PersistenceError & dbError) {
     SPLAPPTRC(L_DEBUG, "Inside clear for store id " << store, "CassandraDBLayer");

 	ostringstream storeId;
 	storeId << store;
 	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CassandraDBLayer");
		}

		return;
	}

 	// Lock the store first.
 	if (acquireStoreLock(storeIdString) == false) {
 		// Unable to acquire the store lock.
 		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "CassandraDBLayer");
 		// User has to retry again to remove the store.
 		return;
 	}

 	// Get the store name.
 	uint32_t dataItemCnt = 0;
 	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		releaseStoreLock(storeIdString);
		// This is alarming. This will put this store in a bad state. Poor user has to deal with it.
		return;
	}

 	// A very fast and quick thing to do is to simply delete the Store Contents row and
 	// recreate it rather than removing one element at a time.
	// This action is performed on the Store Contents row that takes the following name.
	// Row name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	// Delete the entire store contents row.
	string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string statementString = "delete from " + mainTableName + " where r_key='" + storeRowName + "';";
	string errorMsg;
	CassError rc = CASS_OK;
	execute_query(session, statementString.c_str(), errorMsg);

	if (rc != CASS_OK) {
		// There was an error in truncating a store row.
		// Problem in deleting the store contents row.
		dbError.set("Unable to truncate the Cassandra row for store id " + storeIdString + ". " + errorMsg, DPS_STORE_CLEARING_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << DPS_STORE_CLEARING_ERROR, "CassandraDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

	// Let us populate the store contents row with a mandatory element that will carry the name of this store.
	// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
	statementString = "insert into " + mainTableName +
		" (r_key, c_key, t_val) values('" + storeRowName + "', '" +
		string(CASSANDRA_STORE_ID_TO_STORE_NAME_KEY) + "', '" + storeName + "');";

    // This is a crazy situation for a Cassandra cluster. We just deleted the entire store row above.
    // Cassandra puts it in a tombstone. Below this line, we immediately ask Cassandra to create a new
    // row with the same row key and column key. It works fine in a single machine Cassandra, but not in a
    // cluster. It creates the following and then it immediately deletes it because of the in-flight delete
    // activity. That will create a store without its meta data entry 1. That is not good.
    // We are going to stay in a loop and create it for 10 times to be sure.
    for (int32_t cnt = 0; cnt < 10; cnt++) {
    	rc = execute_query(session, statementString.c_str(), errorMsg);
    }

	if (rc != CASS_OK) {
		// There was an error in creating Meta Data 1;
		// This is not good at all. This will leave this store in a zombie state in Cassandra.
		dbError.set("Critical error: Unable to create 'Meta Data1' in Cassandra for store id " + storeIdString + ". " + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside clear, it failed for store id " << storeIdString << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "CassandraDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

    // We are now going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
	// Add the key spl type name metadata.
	statementString = "insert into " + mainTableName +
		" (r_key, c_key, t_val) values('" + storeRowName + "', '" +
		string(CASSANDRA_SPL_TYPE_NAME_OF_KEY) + "', '" + keySplTypeName + "');";
	rc = execute_query(session, statementString.c_str(), errorMsg);

	if (rc != CASS_OK) {
		// There was an error in creating Meta Data 2;
		// This is not good at all. This will leave this store in a zombie state in Cassandra.
		dbError.set("Critical error: Unable to create 'Meta Data2' in Cassandra for store id " + storeIdString + ". " + errorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside clear, it failed for store " << storeIdString << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "CassandraDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

	// Add the value spl type name metadata.
	statementString = "insert into " + mainTableName +
		" (r_key, c_key, t_val) values('" + storeRowName + "', '" +
		string(CASSANDRA_SPL_TYPE_NAME_OF_VALUE) + "', '" + valueSplTypeName + "');";
	rc = execute_query(session, statementString.c_str(), errorMsg);

	if (rc != CASS_OK) {
		// There was an error in creating Meta Data 3;
		// This is not good at all. This will leave this store in a zombie state in Cassandra.
		dbError.set("Critical error: Unable to create 'Meta Data3' in Cassandra for store id " + storeIdString + ". " + errorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside clear, it failed for store " << storeIdString << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "CassandraDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

 	// If there was an error in the store contents row recreation, then this store is going to be in a weird state.
	// It is a fatal error. If that happened, something terribly had gone wrong in clearing the contents.
	// User should look at the dbError code and decide about a corrective action.
 	releaseStoreLock(storeIdString);
  }
        
  uint64_t CassandraDBLayer::size(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside size for store id " << store, "CassandraDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside size, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside size, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CassandraDBLayer");
		}

		return(false);
	}

	// Store size information is maintained as part of the store information.
	uint32_t dataItemCnt = 0;
	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside size, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		return(0);
	}

	return((uint64_t)dataItemCnt);
  }

  // We allow space characters in the data item keys.
  // Hence, it is required to based64 encode them before storing the
  // key:value data item in Cassandra.
  // (Use boost functions to do this.)
  void CassandraDBLayer::base64_encode(std::string const & str, std::string & base64) {
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
  void CassandraDBLayer::base64_decode(std::string & base64, std::string & result) {
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
  bool CassandraDBLayer::storeIdExistsOrNot(string storeIdString, PersistenceError & dbError) {
	  string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	  std::string statementString = "select c_key from " + mainTableName +
		" where r_key='" + storeRowName + "' and c_key='" + string(CASSANDRA_STORE_ID_TO_STORE_NAME_KEY) + "';";
	  CassError rc = CASS_OK;
	  int32_t columnCnt = 0;
	  CassStatement* statement = NULL;
	  CassFuture* future = NULL;
	  CassString query = cass_string_init(statementString.c_str());
	  statement = cass_statement_new(query, 0);
	  future = cass_session_execute(session, statement);
	  cass_future_wait(future);

	  rc = cass_future_error_code(future);

	  if(rc != CASS_OK) {
		  // The store contents row probably doesn't exist.
		  // That means, this store id doesn't exist in our cache.
		  cass_future_free(future);
		  cass_statement_free(statement);
		  return (false);
	  } else {
		  const CassResult* result = cass_future_get_result(future);
		  CassIterator* iterator = cass_iterator_from_result(result);

		  if(cass_iterator_next(iterator)) {
			  // We at least have one resulting column.
			  // That means we have a column containing our store name.
			  columnCnt++;
		  }

		  cass_result_free(result);
		  cass_iterator_free(iterator);
	  }

	  cass_future_free(future);
	  cass_statement_free(statement);

	  if (columnCnt > 0) {
		  // This store id exists in our cache.
		  return(true);
	  } else {
		  // We have the store contents row.
		  // Unable to access a column in the store contents row for the given store id. This is not a correct behavior.
		  dbError.set("StoreIdExistsOrNot: Unable to get StoreContentsRow from the StoreId " + storeIdString +
		     ".", DPS_GET_STORE_CONTENTS_HASH_ERROR);
		  return(false);
	  }
  }

  // This method will acquire a lock for a given store.
  bool CassandraDBLayer::acquireStoreLock(string const & storeIdString) {
	  int32_t retryCnt = 0;
	  SPL::timestamp tsNow = SPL::Functions::Time::getTimestamp();
	  uint64_t timeInNanos = ((SPL::Functions::Time::getSeconds(tsNow) * (int64_t)1000000000) + (int64_t)SPL::Functions::Time::getNanoseconds(tsNow));
	  pid_t myPid= getpid();
	  pthread_t myTid = pthread_self();
	  uint64_t dbSignature = timeInNanos + (uint64_t)myPid + (uint64_t)myTid;
	  ostringstream dbSigStr;
	  dbSigStr << dbSignature;

	  // '4' + 'store id' + 'dps_lock' => 1
	  std::string storeLockKey = string(DPS_STORE_LOCK_TYPE) + storeIdString + DPS_LOCK_TOKEN;
	  uint64_t lockId = SPL::Functions::Utility::hashCode(storeLockKey);
	  ostringstream lockIdStr;
	  lockIdStr << lockId;

	  std::ostringstream expiryTime;
	  expiryTime << DPS_AND_DL_GET_LOCK_TTL;

	  //Try to get a lock for this store.
	  while (1) {
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or
		// until the Cassandra back-end removes this lock entry after the DPS_AND_DL_GET_LOCK_TTL times out.
		std::string statementString = "insert into " + mainTableName +
			" (r_key, c_key, t_val, dbsig) values('" + lockRowName + "', '" + storeLockKey + "', '" +
			lockIdStr.str() + "', '" + dbSigStr.str() + "') if not exists using ttl " + expiryTime.str() + ";";
		CassError rc = CASS_OK;
		std::string errorMsg = "";
		rc = execute_query(session, statementString.c_str(), errorMsg);

		// As of Sep/2014, Cassandra C API returns no error if the row already exists. Hence, any error means db table not being available.
		if (rc != CASS_OK) {
			// There is some db error.
			return(false);
		}

		// We have to read the value and confirm that we really inserted the row.
		statementString = "select dbsig from " + mainTableName + " where r_key='" +
			lockRowName + "' and c_key='" + storeLockKey + "';";
		int32_t columnCnt = 0;
		CassStatement* statement = NULL;
		CassFuture* future = NULL;
		CassString query = cass_string_init(statementString.c_str());
		statement = cass_statement_new(query, 0);
		future = cass_session_execute(session, statement);
		cass_future_wait(future);
		CassString dbSigValue;
		string sigStrFromDb = "";

		rc = cass_future_error_code(future);

		if(rc != CASS_OK) {
			// The store contents row probably doesn't exist.
			cass_future_free(future);
			cass_statement_free(statement);
			// Unable to get the requested data item from the cache.
			return (false);
		} else {
			const CassResult* result = cass_future_get_result(future);
			CassIterator* iterator = cass_iterator_from_result(result);

			if(cass_iterator_next(iterator)) {
				// We at least have one resulting column.
				columnCnt++;
				const CassRow* row = cass_iterator_get_row(iterator);
				cass_value_get_string(cass_row_get_column(row, 0), &dbSigValue);
				sigStrFromDb = string(dbSigValue.data, dbSigValue.length);
			}

			cass_result_free(result);
			cass_iterator_free(iterator);
		}

		cass_future_free(future);
		cass_statement_free(statement);


		/*
		// I'm commenting out this block. In Cassandra, I have noticed that executing a select statement right after an insert statement
		// returns 0 results. We will not return if we hit that condition. Instead, we will stay in the while loop and try again.
		if (columnCnt <= 0) {
			// There is no resulting row, which is not good. Let us return now.
			return(false);
		}
		*/

		if ((columnCnt > 0) && (sigStrFromDb == dbSigStr.str())) {
			// This is the column we just inserted. That means, we got the lock.
			return(true);
		}

		// Someone else is holding on to the lock of this store. Wait for a while before trying again.
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

  void CassandraDBLayer::releaseStoreLock(string const & storeIdString) {
	  // '4' + 'store id' + 'dps_lock' => 1
	  std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;
	  std::string statementString = "delete from " + mainTableName +
	     " where r_key='" + lockRowName + "' and c_key='" + storeLockKey + "';";
	  std::string errorMsg = "";
	  execute_query(session, statementString.c_str(), errorMsg);
  }

  bool CassandraDBLayer::readStoreInformation(std::string const & storeIdString, PersistenceError & dbError,
		  uint32_t & dataItemCnt, std::string & storeName, std::string & keySplTypeName, std::string & valueSplTypeName) {
	  // Read the store name, this store's key and value SPL type names,  and get the store size.
	  storeName = "";
	  keySplTypeName = "";
	  valueSplTypeName = "";
	  dataItemCnt = 0;

	  // This action is performed on the Store Contents row that takes the following name.
	  // 'dps_' + '1_' + 'store id'
	  // It will always have the following three metadata entries:
	  // dps_name_of_this_store ==> 'store name'
	  // dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	  // dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	  // 1) Get the store name.
	  string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	  std::string statementString = "select t_val from " + mainTableName +
		" where r_key='" + storeRowName + "' and c_key='" + string(CASSANDRA_STORE_ID_TO_STORE_NAME_KEY) + "';";
	  CassError rc = CASS_OK;
	  int32_t columnCnt = 0;
	  CassStatement* statement = NULL;
	  CassFuture* future = NULL;
	  CassString query = cass_string_init(statementString.c_str());
	  statement = cass_statement_new(query, 0);
	  future = cass_session_execute(session, statement);
	  cass_future_wait(future);

	  rc = cass_future_error_code(future);

	  if(rc != CASS_OK) {
		  // The store contents row probably doesn't exist.
		  CassString message = cass_future_error_message(future);
		  cass_future_free(future);
		  cass_statement_free(statement);
		  // Unable to get the metadata1 from the store contents row for the given store id.
		  dbError.set("Unable to get StoreContentsRow metadata1 from the StoreId " + storeIdString +
		     ". " + std::string(message.data, message.length), DPS_GET_STORE_CONTENTS_HASH_ERROR);
		  return (false);
	  } else {
		  const CassResult* result = cass_future_get_result(future);
		  CassIterator* iterator = cass_iterator_from_result(result);

		  if(cass_iterator_next(iterator)) {
			  // We at least have one resulting column.
			  // That means we have a column containing our store name.
			  columnCnt++;
			  CassString data;
			  const CassRow* row = cass_iterator_get_row(iterator);
			  cass_value_get_string(cass_row_get_column(row, 0), &data);
			  storeName = string(data.data, data.length);
		  }

		  cass_result_free(result);
		  cass_iterator_free(iterator);
	  }

	  cass_future_free(future);
	  cass_statement_free(statement);

	  if (columnCnt <= 0) {
		  // We have the store contents row.
		  // Unable to access a column in the store contents row for the given store id. This is not a correct behavior.
		  // Unable to get the name of this store.
		  dbError.set("Unable to get the store name for StoreId " + storeIdString + ".", DPS_GET_STORE_NAME_ERROR);
		  return(false);
	  }

	  // 2) Let us get the spl type name for this store's key.
	  statementString = "select t_val from " + mainTableName +
		" where r_key='" + storeRowName + "' and c_key='" + string(CASSANDRA_SPL_TYPE_NAME_OF_KEY) + "';";
	  rc = CASS_OK;
	  columnCnt = 0;
	  query = cass_string_init(statementString.c_str());
	  statement = cass_statement_new(query, 0);
	  future = cass_session_execute(session, statement);
	  cass_future_wait(future);

	  rc = cass_future_error_code(future);

	  if(rc != CASS_OK) {
		  // The store contents row probably doesn't exist.
		  CassString message = cass_future_error_message(future);
		  cass_future_free(future);
		  cass_statement_free(statement);
		  // Unable to get the metadata2 from the store contents row for the given store id.
		  dbError.set("Unable to get StoreContentsRow metadata2 from the StoreId " + storeIdString +
		     ". " + std::string(message.data, message.length), DPS_GET_STORE_CONTENTS_HASH_ERROR);
		  return (false);
	  } else {
		  const CassResult* result = cass_future_get_result(future);
		  CassIterator* iterator = cass_iterator_from_result(result);

		  if(cass_iterator_next(iterator)) {
			  // We at least have one resulting column.
			  // That means we have a column containing our store name.
			  columnCnt++;
			  CassString data;
			  const CassRow* row = cass_iterator_get_row(iterator);
			  cass_value_get_string(cass_row_get_column(row, 0), &data);
			  keySplTypeName = string(data.data, data.length);
		  }

		  cass_result_free(result);
		  cass_iterator_free(iterator);
	  }

	  cass_future_free(future);
	  cass_statement_free(statement);

	  if (columnCnt <= 0) {
		  // We have the store contents row.
		  // Unable to access a column in the store contents row for the given store id. This is not a correct behavior.
		  // Unable to get the spl type name for this store's key.
		  dbError.set("Unable to get the key spl type name for StoreId " + storeIdString + ".", DPS_GET_KEY_SPL_TYPE_NAME_ERROR);
		  return(false);
	  }

	  // 3) Let us get the spl type name for this store's value.
	  statementString = "select t_val from " + mainTableName +
         " where r_key='" + storeRowName + "' and c_key='" + string(CASSANDRA_SPL_TYPE_NAME_OF_VALUE) + "';";
	  rc = CASS_OK;
	  columnCnt = 0;
	  query = cass_string_init(statementString.c_str());
	  statement = cass_statement_new(query, 0);
	  future = cass_session_execute(session, statement);
	  cass_future_wait(future);

	  rc = cass_future_error_code(future);

	  if(rc != CASS_OK) {
		  // The store contents row probably doesn't exist.
		  CassString message = cass_future_error_message(future);
		  cass_future_free(future);
		  cass_statement_free(statement);
		  // Unable to get the metadata3 from the store contents row for the given store id.
		  dbError.set("Unable to get StoreContentsRow metadata3 from the StoreId " + storeIdString +
		     ". " + std::string(message.data, message.length), DPS_GET_STORE_CONTENTS_HASH_ERROR);
		  return (false);
	  } else {
		  const CassResult* result = cass_future_get_result(future);
		  CassIterator* iterator = cass_iterator_from_result(result);

		  if(cass_iterator_next(iterator)) {
			  // We at least have one resulting column.
			  // That means we have a column containing our store name.
			  columnCnt++;
			  CassString data;
			  const CassRow* row = cass_iterator_get_row(iterator);
			  cass_value_get_string(cass_row_get_column(row, 0), &data);
			  valueSplTypeName = string(data.data, data.length);
		  }

		  cass_result_free(result);
		  cass_iterator_free(iterator);
	  }

	  cass_future_free(future);
	  cass_statement_free(statement);

	  if (columnCnt <= 0) {
		  // We have the store contents row.
		  // Unable to access a column in the store contents row for the given store id. This is not a correct behavior.
		  // Unable to get the spl type name for this store's value.
		  dbError.set("Unable to get the value spl type name for StoreId " + storeIdString + ".", DPS_GET_VALUE_SPL_TYPE_NAME_ERROR);
		  return(false);
	  }

	  // 4) Let us get the size of the store contents hash now.
	  statementString = "select count(*) from " + mainTableName + " where r_key='" + storeRowName + "';";
	  rc = CASS_OK;
	  columnCnt = 0;
	  query = cass_string_init(statementString.c_str());
	  statement = cass_statement_new(query, 0);
	  future = cass_session_execute(session, statement);
	  cass_future_wait(future);

	  rc = cass_future_error_code(future);

	  if(rc != CASS_OK) {
		  // The store contents row probably doesn't exist.
		  CassString message = cass_future_error_message(future);
		  cass_future_free(future);
		  cass_statement_free(statement);
		  dbError.set("Unable to get StoreContentsRow size from the StoreId " + storeIdString +
				  ". " + std::string(message.data, message.length), DPS_GET_STORE_SIZE_ERROR);
		  return (false);
	  } else {
		  const CassResult* result = cass_future_get_result(future);
		  CassIterator* iterator = cass_iterator_from_result(result);

		  if(cass_iterator_next(iterator)) {
			  // We at least have one resulting column.
			  // That means we have a column containing our store name.
			  columnCnt++;
			  const CassRow* row = cass_iterator_get_row(iterator);
			  cass_int64_t myDataItem = 0;
			  cass_value_get_int64(cass_row_get_column(row, 0), &myDataItem);
			  dataItemCnt = (uint32_t)myDataItem;
		  }

		  cass_result_free(result);
		  cass_iterator_free(iterator);
	  }

	  cass_future_free(future);
	  cass_statement_free(statement);

	  if (columnCnt <= 0 || dataItemCnt <= 0) {
		  // We have the store contents row.
		  // Unable to access a column in the store contents row for the given store id. This is not a correct behavior.
		  // This is not correct. We must have a minimum hash size of 3 because of the reserved elements.
		  dbError.set("Wrong value (zero) observed as the store size for StoreId " + storeIdString + ".", DPS_GET_STORE_SIZE_ERROR);
		  return(false);
	  }

	  // Our Store Contents Hash for every store will have a mandatory reserved internal elements (store name, key spl type name, and value spl type name).
	  // Let us not count those three elements in the actual store contents hash size that the caller wants now.
	  dataItemCnt -= 3;
	  return(true);
  }

  string CassandraDBLayer::getStoreName(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CassandraDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		  return("");
	  }

	  string base64_decoded_storeName;
	  base64_decode(storeName, base64_decoded_storeName);
	  return(base64_decoded_storeName);
  }

  string CassandraDBLayer::getSplTypeNameForKey(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CassandraDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		  return("");
	  }

	  string base64_decoded_keySplTypeName;
	  base64_decode(keySplTypeName, base64_decoded_keySplTypeName);
	  return(base64_decoded_keySplTypeName);
  }

  string CassandraDBLayer::getSplTypeNameForValue(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CassandraDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		  return("");
	  }

	  string base64_decoded_valueSplTypeName;
	  base64_decode(valueSplTypeName, base64_decoded_valueSplTypeName);
	  return(base64_decoded_valueSplTypeName);
  }

  std::string CassandraDBLayer::getNoSqlDbProductName(void) {
	  return(string(CASSANDRA_NO_SQL_DB_NAME));
  }

  void CassandraDBLayer::getDetailsAboutThisMachine(std::string & machineName, std::string & osVersion, std::string & cpuArchitecture) {
	  machineName = nameOfThisMachine;
	  osVersion = osVersionOfThisMachine;
	  cpuArchitecture = cpuTypeOfThisMachine;
  }

  bool CassandraDBLayer::runDataStoreCommand(std::string const & cmd, PersistenceError & dbError) {
	  // If users want to execute simple arbitrary back-end data store (fire and forget)
	  // native commands, this API can be used. This covers any Redis or Cassandra(CQL)
	  // native commands that don't have to fetch and return K/V pairs or return size of the db etc.
	  // (Insert and Delete are the more suitable ones here. However, key and value can only have string types.)
	  // User must ensure that his/her command string is syntactically correct according to the
	  // rules of the back-end data store you configured. DPS logic will not do the syntax checking.
	  //
	  // (You can't do select statements using this technique. Similarly, no complex type keys or values.
	  //  In that case, please use the regular dps APIs.)
	  //
	  // We will simply take your command string and run it. So, be sure of what
	  // command you are sending here.
	  string errorMsg = "";
	  execute_query(session, cmd.c_str(), errorMsg);

	  if (errorMsg == "") {
		  return(true);
	  } else {
		  // Problem in running an arbitrary data store command.
		  dbError.set("From Cassandra data store: Unable to run this command: '" + cmd + "'. Error=" + errorMsg, DPS_RUN_DATA_STORE_COMMAND_ERROR);
		  SPLAPPTRC(L_DEBUG, "From Cassandra data store: Inside runDataStoreCommand, it failed to run this command: '" << cmd <<
				  "'. Error=" << errorMsg << ". " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CassandraDBLayer");
		  return(false);
	  }
  }

  bool CassandraDBLayer::runDataStoreCommand(uint32_t const & cmdType, std::string const & httpVerb,
		std::string const & baseUrl, std::string const & apiEndpoint, std::string const & queryParams,
		std::string const & jsonRequest, std::string & jsonResponse, PersistenceError & dbError) {
		// This API can only be supported in NoSQL data stores such as Cloudant etc.
		// Cassandra doesn't have a way to do this.
		dbError.set("From Cassandra data store: This API to run native data store commands is not supported in Cassandra.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Cassandra data store: This API to run native data store commands is not supported in Cassandra. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CassandraDBLayer");
		return(false);
  }

  bool CassandraDBLayer::runDataStoreCommand(std::vector<std::string> const & cmdList, std::string & resultValue, PersistenceError & dbError) {
		// This API can only be supported in Redis.
		// Cassandra doesn't have a way to do this.
		dbError.set("From Cassandra data store: This API to run native data store commands is not supported in Cassandra.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From Cassandra data store: This API to run native data store commands is not supported in Cassandra. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "CassandraDBLayer");
		return(false);
  }

  // This method will get the data item from the store for a given key.
  // Caller of this method can also ask us just to find if a data item
  // exists in the store without the extra work of fetching and returning the data item value.
  bool CassandraDBLayer::getDataItemFromStore(std::string const & storeIdString,
		  std::string const & keyDataString, bool const & checkOnlyForDataItemExistence,
		  bool const & skipDataItemExistenceCheck, unsigned char * & valueData,
		  uint32_t & valueSize, PersistenceError & dbError) {
		// Let us get this data item from the cache as it is.
		// Since there could be multiple data writers, we are going to get whatever is there now.
		// It is always possible that the value for the requested item can change right after
		// you read it due to the data write made by some other thread. Such is life in a global distributed in-memory store.
	  string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	  std::string statementString = "select b_val from " + mainTableName + " where r_key='" +
	     storeRowName + "' and c_key='" + keyDataString + "';";
	  CassError rc = CASS_OK;
	  int32_t columnCnt = 0;
	  CassStatement* statement = NULL;
	  CassFuture* future = NULL;
	  CassString query = cass_string_init(statementString.c_str());
	  statement = cass_statement_new(query, 0);
	  future = cass_session_execute(session, statement);
	  cass_future_wait(future);
	  CassBytes value;
	  valueSize = 0;

	  rc = cass_future_error_code(future);

	  if(rc != CASS_OK) {
		  // The store contents row probably doesn't exist.
		  CassString message = cass_future_error_message(future);
		  cass_future_free(future);
		  cass_statement_free(statement);
		  // Unable to get the requested data item from the cache.
		  dbError.set("Unable to access the store row in Cassandra with the StoreId " + storeIdString +
		     ". " + std::string(message.data, message.length), DPS_DATA_ITEM_READ_ERROR);
		  return (false);
	  } else {
		  const CassResult* result = cass_future_get_result(future);
		  CassIterator* iterator = cass_iterator_from_result(result);

		  if(cass_iterator_next(iterator)) {
			  // We at least have one resulting column.
			  // That means we have a column containing our store name.
			  columnCnt++;
			  const CassRow* row = cass_iterator_get_row(iterator);
			  cass_value_get_bytes(cass_row_get_column(row, 0), &value);

			  // Data item value read from the store will be in this format: 'value'
			  if ((unsigned)value.size == 0) {
				  // User stored empty data item value in the cache.
				  valueData = (unsigned char *)"";
				  valueSize = 0;
			  } else {
				  // We can allocate memory for the exact length of the data item value.
				  valueSize = value.size;
				  valueData = (unsigned char *) malloc(valueSize);

				  if (valueData == NULL) {
					  // Unable to allocate memory to transfer the data item value.
					  dbError.set("Unable to allocate memory to copy the data item value for the StoreId " +
						storeIdString + ".", DPS_GET_DATA_ITEM_MALLOC_ERROR);
					  valueSize = 0;
				  } else {
					  // We expect the caller of this method to free the valueData pointer.
					  memcpy(valueData, value.data, valueSize);
				  }
			  }
		  }

		  cass_result_free(result);
		  cass_iterator_free(iterator);
	  }

	  cass_future_free(future);
	  cass_statement_free(statement);
	  bool dataItemExists = true;

	  if (columnCnt <= 0) {
		  dataItemExists = false;
	  }

	  // If the caller only wanted us to check for the data item existence, we can exit now.
	  if (checkOnlyForDataItemExistence == true) {
		  // If we had allocated memory above while processing the result from the select statement, free it now.
		  // Because, caller doesn't expect us to return the data.
		  if (valueSize > 0) {
			  delete [] valueData;
			  valueData = NULL;
			  valueSize = 0;
		  }

		  // In case we went through the code logic above that had set the DB error during malloc, we can reset the DB error.
		  dbError.reset();
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
		  if (valueData == NULL) {
			  // We encountered malloc error above. Only then, we will have a null value data.
			  return(false);
		  } else {
			  return(true);
		  }
	  }
  }

  CassandraDBLayerIterator * CassandraDBLayer::newIterator(uint64_t store, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside newIterator for store id " << store, "CassandraDBLayer");

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  // Ensure that a store exists for the given store id.
	  if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CassandraDBLayer");
		  }

		  return(NULL);
	  }

	  // Get the general information about this store.
	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayer");
		  return(NULL);
	  }

	  // It is a valid store. Create a new iterator and return it to the caller.
	  CassandraDBLayerIterator *iter = new CassandraDBLayerIterator();
	  iter->store = store;
	  base64_decode(storeName, iter->storeName);
	  // Give this iterator access to our Cassandra connection handle.
	  iter->session = session;
	  iter->hasData = true;
	  // Give this iterator access to our CassandraDBLayer object.
	  iter->cassandraDBLayerPtr = this;
	  iter->sizeOfDataItemKeysVector = 0;
	  iter->currentIndex = 0;
	  return(iter);
  }

  void CassandraDBLayer::deleteIterator(uint64_t store, Iterator * iter, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside deleteIterator for store id " << store, "CassandraDBLayer");

	  if (iter == NULL) {
		  return;
	  }

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  CassandraDBLayerIterator *myIter = static_cast<CassandraDBLayerIterator *>(iter);

	  // Let us ensure that the user wants to delete an iterator that really belongs to the store passed to us.
	  // This will handle user's coding errors where a wrong combination of store id and iterator is passed to us for deletion.
	  if (myIter->store != store) {
		  // User sent us a wrong combination of a store and an iterator.
		  dbError.set("A wrong iterator has been sent for deletion. This iterator doesn't belong to the StoreId " +
		  				storeIdString + ".", DPS_STORE_ITERATION_DELETION_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside deleteIterator, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_DELETION_ERROR, "CassandraDBLayer");
		  return;
	  } else {
		  delete iter;
	  }
  }

  // This method will acquire a lock for any given generic/arbitrary identifier passed as a string..
  // This is typically used inside the createStore, createOrGetStore, createOrGetLock methods to
  // provide thread safety. There are other lock acquisition/release methods once someone has a valid store id or lock id.
  bool CassandraDBLayer::acquireGeneralPurposeLock(string const & entityName) {
	  int32_t retryCnt = 0;
	  SPL::timestamp tsNow = SPL::Functions::Time::getTimestamp();
	  uint64_t timeInNanos = ((SPL::Functions::Time::getSeconds(tsNow) * (int64_t)1000000000) + (int64_t)SPL::Functions::Time::getNanoseconds(tsNow));
	  pid_t myPid= getpid();
	  pthread_t myTid = pthread_self();
	  uint64_t dbSignature = timeInNanos + (uint64_t)myPid + (uint64_t)myTid;
	  ostringstream dbSigStr;
	  dbSigStr << dbSignature;

	  // '501' + 'entity name' + 'generic_lock' => 1
	  std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
	  uint64_t lockId = SPL::Functions::Utility::hashCode(genericLockKey);
	  ostringstream lockIdStr;
	  lockIdStr << lockId;

	  std::ostringstream expiryTime;
	  expiryTime << DPS_AND_DL_GET_LOCK_TTL;

	  //Try to get a lock for this generic entity. We will attempt to create a row in Cassandra.
	  while (1) {
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or
		// until the Cassandra back-end removes this lock entry after the DPS_AND_DL_GET_LOCK_TTL times out.
		std::string statementString = "insert into " + mainTableName +
			" (r_key, c_key, t_val, dbsig) values('" + lockRowName + "', '" +
			genericLockKey + "', '" + lockIdStr.str() + "', '" + dbSigStr.str() + "') if not exists using ttl " + expiryTime.str() + ";";
		CassError rc = CASS_OK;
		std::string errorMsg = "";
		rc = execute_query(session, statementString.c_str(), errorMsg);

		// As of Sep/2014, Cassandra C API returns no error if the row already exists. Hence, any error means db table not being available.
		if (rc != CASS_OK) {
			// There is some db error.
			return(false);
		}

		// We have to read the value and confirm that we really inserted the row.
		statementString = "select dbsig from " + mainTableName + " where r_key='" + lockRowName +
			"' and c_key='" + genericLockKey + "';";
		int32_t columnCnt = 0;
		CassStatement* statement = NULL;
		CassFuture* future = NULL;
		CassString query = cass_string_init(statementString.c_str());
		statement = cass_statement_new(query, 0);
		future = cass_session_execute(session, statement);
		cass_future_wait(future);
		CassString dbSigValue;
		string sigStrFromDb = "";

		rc = cass_future_error_code(future);

		if(rc != CASS_OK) {
			// The store contents row probably doesn't exist.
			cass_future_free(future);
			cass_statement_free(statement);
			// Unable to get the requested data item from the cache.
			return (false);
		} else {
			const CassResult* result = cass_future_get_result(future);
			CassIterator* iterator = cass_iterator_from_result(result);

			if(cass_iterator_next(iterator)) {
				// We at least have one resulting column.
				columnCnt++;
				const CassRow* row = cass_iterator_get_row(iterator);
				cass_value_get_string(cass_row_get_column(row, 0), &dbSigValue);
				sigStrFromDb = string(dbSigValue.data, dbSigValue.length);
			}

			cass_result_free(result);
			cass_iterator_free(iterator);
		}

		cass_future_free(future);
		cass_statement_free(statement);

		/*
		// I'm commenting out this block. In Cassandra, I have noticed that executing a select statement right after an insert statement
		// returns 0 results. We will not return if we hit that condition. Instead, we will stay in the while loop and try again.
		if (columnCnt <= 0) {
			// There is no resulting column, which is not good. Let us return now.
			return(false);
		}
		*/

		if ((columnCnt > 0) && (sigStrFromDb == dbSigStr.str())) {
			// This is the column we just inserted. That means, we got the lock.
			return(true);
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

  void CassandraDBLayer::releaseGeneralPurposeLock(string const & entityName) {
	  // '501' + 'entity name' + 'generic_lock' => 1
	  std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
	  std::string statementString = "delete from " + mainTableName +
	     " where r_key='" + lockRowName + "' and c_key='" + genericLockKey + "';";
	  std::string errorMsg = "";
	  execute_query(session, statementString.c_str(), errorMsg);
  }

  CassandraDBLayerIterator::CassandraDBLayerIterator() {

  }

  CassandraDBLayerIterator::~CassandraDBLayerIterator() {

  }

  bool CassandraDBLayerIterator::getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
  	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside getNext for store id " << store, "CassandraDBLayerIterator");

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
	  if (this->cassandraDBLayerPtr->storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayerIterator");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "CassandraDBLayerIterator");
		  }

		  return(false);
	  }

	  // Ensure that this store is not empty at this time.
	  if (this->cassandraDBLayerPtr->size(store, dbError) <= 0) {
		  dbError.set("Store is empty for the StoreId " + storeIdString + ".", DPS_STORE_EMPTY_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_STORE_EMPTY_ERROR, "CassandraDBLayerIterator");
		  return(false);
	  }

	  if (this->sizeOfDataItemKeysVector <= 0) {
		  // This is the first time we are coming inside getNext for store iteration.
		  // Let us get the available data item keys from this store.
		  this->dataItemKeys.clear();
		  string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
		  std::string statementString = "select c_key from " + this->cassandraDBLayerPtr->mainTableName +
             " where r_key='" + storeRowName + "';";
		  CassError rc = CASS_OK;
		  int32_t columnCnt = 0;
		  CassStatement* statement = NULL;
		  CassFuture* future = NULL;
		  CassString query = cass_string_init(statementString.c_str());
		  statement = cass_statement_new(query, 0);
		  future = cass_session_execute(session, statement);
		  cass_future_wait(future);

		  rc = cass_future_error_code(future);

		  if(rc != CASS_OK) {
			  // The store contents row probably doesn't exist.
			  CassString message = cass_future_error_message(future);
			  cass_future_free(future);
			  cass_statement_free(statement);
			  // Unable to get data item keys from the store.
			  dbError.set("Unable to get data item keys for the StoreId " + storeIdString +
			     ". " + std::string(message.data, message.length), DPS_GET_STORE_DATA_ITEM_KEYS_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString <<
			     ". Error = " << string(message.data, message.length) << ". " << DPS_GET_STORE_DATA_ITEM_KEYS_ERROR, "CassandraDBLayerIterator");
			  this->hasData = false;
			  return(false);
		  } else {
			  const CassResult* result = cass_future_get_result(future);
			  CassIterator* iterator = cass_iterator_from_result(result);

			  // We will collect all the data item keys by staying in a loop.
			  // Let us insert them into the iterator object's member variable that will hold the data item keys for this store.
			  while(cass_iterator_next(iterator)) {
				  // We at least have one resulting row.
				  // That means we have a row containing our store name in the GUID row.
				  const CassRow* row = cass_iterator_get_row(iterator);
				  CassString key;
				  cass_value_get_string(cass_row_get_column(row, 0), &key);
				  data_item_key = string(key.data, key.length);

				  // Every dps store will have three mandatory reserved data item keys for internal use.
				  // Let us not add them to the iteration object's member variable.
				  if (data_item_key.compare(CASSANDRA_STORE_ID_TO_STORE_NAME_KEY) == 0) {
					  continue; // Skip this one.
				  } else if (data_item_key.compare(CASSANDRA_SPL_TYPE_NAME_OF_KEY) == 0) {
					  continue; // Skip this one.
				  } else if (data_item_key.compare(CASSANDRA_SPL_TYPE_NAME_OF_VALUE) == 0) {
					  continue; // Skip this one.
				  }

				  columnCnt++;
				  this->dataItemKeys.push_back(data_item_key);
			  }

			  cass_result_free(result);
			  cass_iterator_free(iterator);
		  }

		  cass_future_free(future);
		  cass_statement_free(statement);
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
	  // data_item_key was obtained straight from the store contents hash, where it is
	  // already in the base64 encoded format.
	  bool result = this->cassandraDBLayerPtr->getDataItemFromStore(storeIdString, data_item_key,
		false, false, valueData, valueSize, dbError);

	  if (result == false) {
		  // Some error has occurred in reading the data item value.
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "CassandraDBLayerIterator");
		  // We will disable any future action for this store using the current iterator.
		  this->hasData = false;
		  return(false);
	  }

	  // We are almost done once we take care of arranging to return the key name and key size.
	  // In order to support spaces in data item keys, we base64 encoded them before storing it in Cassandra.
	  // Let us base64 decode it now to get the original data item key.
	  string base64_decoded_data_item_key;
	  this->cassandraDBLayerPtr->base64_decode(data_item_key, base64_decoded_data_item_key);
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
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_MALLOC_ERROR, "CassandraDBLayerIterator");
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
//
// In the Cassandra implementation, we are using some of the high level ideas about lock creation,
// acquisition, release, and removal as we do it in the dps memcached and Redis versions.
  // However, the code implementation for locks in Cassandra is done inside a
// single table. Some of the commands we use here to achieve the same effect as in the Redis implementation are
// different. Hence, the Cassandra implementation code below will be deviating a litle bit from our the other
// dps work that already exists for memcached and redis.
// =======================================================================================================
  uint64_t CassandraDBLayer::createOrGetLock(std::string const & name, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock with a name " << name, "CassandraDBLayer");
		string base64_encoded_name;
		base64_encode(name, base64_encoded_name);

	 	// Get a general purpose lock so that only one thread can
		// enter inside of this method at any given time with the same lock name.
	 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
	 		// Unable to acquire the general purpose lock.
	 		lkError.set("Unable to get a generic lock for creating a lock with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
	 		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for an yet to be created lock with its name as " <<
	 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "CassandraDBLayer");
	 		// User has to retry again to create this distributed lock.
	 		return 0;
	 	}

		SPL::timestamp tsNow = SPL::Functions::Time::getTimestamp();
		uint64_t timeInNanos = ((SPL::Functions::Time::getSeconds(tsNow) * (int64_t)1000000000) + (int64_t)SPL::Functions::Time::getNanoseconds(tsNow));
		pid_t myPid= getpid();
		pthread_t myTid = pthread_self();
		uint64_t dbSignature = timeInNanos + (uint64_t)myPid + (uint64_t)myTid;
		ostringstream dbSigStr;
		dbSigStr << dbSignature;

		// In our Cassandra dps implementation, data item keys can have space characters.
		// Inside Cassandra, all our lock names will have a mapping type indicator of
		// "5" at the beginning followed by the actual lock name.
		// '5' + 'lock name' => lockId
		std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
		uint64_t lockId = SPL::Functions::Utility::hashCode(lockNameKey);
		ostringstream lockIdStr;
		lockIdStr << lockId;

		std::string statementString = "insert into " + mainTableName +
			" (r_key, c_key, t_val, dbsig, info) values('" + lockRowName + "', '" + lockNameKey +
			"', '" + lockIdStr.str() + "', '" + dbSigStr.str() + "', '0_0_0_ABC') if not exists;";

		CassError rc = CASS_OK;
		std::string errorMsg = "";
		rc = execute_query(session, statementString.c_str(), errorMsg);

		// As of Sep/2014, Cassandra C API returns no error if the row already exists. Hence, any error means db table not being available.
		if (rc != CASS_OK) {
			// There is some db error.
			// dps_lock_dl_lock row may not be there.
			// Unable to get an existing lock id from the cache.
			lkError.set("DB insert error. Unable to get the lockId for the lockName " + name + ". " + errorMsg, DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "DB insert error. Inside createOrGetLock, it failed for the lockName " << name << ". Error = " << errorMsg << ". " << DL_GET_LOCK_ID_ERROR, "CassandraDBLayer");
			releaseGeneralPurposeLock(base64_encoded_name);
			return(0);
		}

		// We have to read the value and confirm that we really inserted the row or a row for this lock already exists.
		statementString = "select c_key from " + mainTableName + " where r_key='" +
			lockRowName + "' and c_key='" + lockNameKey + "';";
		int32_t columnCnt = 0;
		CassStatement* statement = NULL;
		CassFuture* future = NULL;
		CassString query = cass_string_init(statementString.c_str());
		statement = cass_statement_new(query, 0);
		future = cass_session_execute(session, statement);
		cass_future_wait(future);
		CassString key;
		string lockNameFromDb = "";

		rc = cass_future_error_code(future);

		if(rc != CASS_OK) {
			// Read error.
			CassString message = cass_future_error_message(future);
			cass_future_free(future);
			// Unable to get an existing lock id from the cache.
			lkError.set("DB read error. Unable to get the lockId for the lockName " + name + ". " + std::string(message.data, message.length), DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "DB read error. Inside createOrGetLock, it failed for the lockName " << name << ". Error = " << string(message.data, message.length) << ". " << DL_GET_LOCK_ID_ERROR, "CassandraDBLayer");
			releaseGeneralPurposeLock(base64_encoded_name);
			return(0);
		} else {
			const CassResult* result = cass_future_get_result(future);
			CassIterator* iterator = cass_iterator_from_result(result);

			if(cass_iterator_next(iterator)) {
				// We at least have one resulting column.
				columnCnt++;
				const CassRow* row = cass_iterator_get_row(iterator);
				cass_value_get_string(cass_row_get_column(row, 0), &key);
				lockNameFromDb = string(key.data, key.length);
			}

			cass_result_free(result);
			cass_iterator_free(iterator);
		}

		cass_future_free(future);
		cass_statement_free(statement);

		if (columnCnt <= 0) {
			// Unable to get an existing lock id from the cache.
			lkError.set("Missing lock id row: Unable to get a unique lock id for a lock named " + name + ".", DL_GUID_CREATION_ERROR);
			SPLAPPTRC(L_DEBUG, "Missing lock id row: Inside createOrGetLock, it failed for a lock named " << name << ". " << DL_GUID_CREATION_ERROR, "CassandraDBLayer");
			releaseGeneralPurposeLock(base64_encoded_name);
			return 0;
		}

		if (lockNameFromDb == lockNameKey) {
			// This is the row we expected to have for this lock.
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock done for a lock named " << name, "CassandraDBLayer");

			// 2) Create the Lock Info
			//    '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
			std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdStr.str();  // LockId becomes the new key now.
			string lockInfoValue = "0_0_0_" + base64_encoded_name;
			statementString = "insert into " + mainTableName +
				" (r_key, c_key, info) values('" + lockRowName + "', '" + lockInfoKey + "', '" + lockInfoValue + "') if not exists;";

			rc = CASS_OK;
			rc = execute_query(session, statementString.c_str(), errorMsg);

			// Any error means db table not being available.
			if (rc != CASS_OK) {
				// There is some db error.
				// dps_lock_dl_lock row may not be there.
				// Unable to create lockinfo details..
				// Problem in creating the "LockId:LockInfo" entry in the cache.
				lkError.set("Unable to create 'LockId:LockInfo' in the cache for a lock named " + name + ". " + errorMsg, DL_LOCK_INFO_CREATION_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed for a lock named " << name << ". Error=" << errorMsg << ". " << DL_LOCK_INFO_CREATION_ERROR, "CassandraDBLayer");
				// Delete the previous root entry we inserted.
				statementString = "delete from " + mainTableName +
					" where r_key='" + lockRowName + "' and c_key='" + lockNameKey + "';";
				execute_query(session, statementString.c_str(), errorMsg);
				releaseGeneralPurposeLock(base64_encoded_name);
				return(0);
			} else {
				releaseGeneralPurposeLock(base64_encoded_name);
				return (lockId);
			}
		} else {
			releaseGeneralPurposeLock(base64_encoded_name);
			return (0);
		}
  }

  bool CassandraDBLayer::removeLock(uint64_t lock, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside removeLock for lock id " << lock, "CassandraDBLayer");
		ostringstream lockId;
		lockId << lock;
		string lockIdString = lockId.str();

		// If the lock doesn't exist, there is nothing to remove. Don't allow this caller inside this method.
		if(lockIdExistsOrNot(lockIdString, lkError) == false) {
			if (lkError.hasError() == true) {
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "CassandraDBLayer");
			} else {
				lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "CassandraDBLayer");
			}

			return(false);
		}

		// Before removing the lock entirely, ensure that the lock is not currently being used by anyone else.
		if (acquireLock(lock, 5, 3, lkError) == false) {
			// Unable to acquire the distributed lock.
			lkError.set("Unable to get a distributed lock for the LockId " + lockIdString + ".", DL_GET_DISTRIBUTED_LOCK_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for the lock id " << lockIdString << ". " << DL_GET_DISTRIBUTED_LOCK_ERROR, "CassandraDBLayer");
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
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "CassandraDBLayer");
			releaseLock(lock, lkError);
			// This is alarming. This will put this lock in a bad state. Poor user has to deal with it.
			return(false);
		}

		// Since we got back the lock name for the given lock id, let us remove the lock entirely.
		// '5' + 'lock name' => lockId
		std::string lockNameKey = DL_LOCK_NAME_TYPE + lockName;
		string statementString = "delete from " + mainTableName + " where r_key='" + lockRowName + "' and c_key='" + lockNameKey + "';";
		string errorMsg;
		CassError rc = CASS_OK;
		rc = execute_query(session, statementString.c_str(), errorMsg);

		if (rc != CASS_OK) {
			lkError.set("Unable to remove the lock named " + lockIdString + ".", DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for a lock named " << lockIdString << ". Error=" << errorMsg << ". " << DL_LOCK_REMOVAL_ERROR, "CassandraDBLayer");
			releaseLock(lock, lkError);
			return(false);
		}

		// Now remove the lockInfo row.
		// '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;  // LockId becomes the new key now.
		statementString = "delete from " +  mainTableName + " where r_key='" + lockRowName + "' and c_key='" + lockInfoKey + "';";
		execute_query(session, statementString.c_str(), errorMsg);

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

  bool CassandraDBLayer::acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside acquireLock for lock id " << lock, "CassandraDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();
	  int32_t retryCnt = 0;

	  // If the lock doesn't exist, there is nothing to acquire. Don't allow this caller inside this method.
	  if(lockIdExistsOrNot(lockIdString, lkError) == false) {
		  if (lkError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "CassandraDBLayer");
		  } else {
			  lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for lock id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "CassandraDBLayer");
		  }

		  return(false);
	  }

	  // We will first check if we can get this lock.
	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  time_t startTime, timeNow;
	  // Get the start time for our lock acquisition attempts.
	  time(&startTime);

	  ostringstream leaseTimeStream;
	  leaseTimeStream << (int64_t)leaseTime;

	  SPL::timestamp tsNow = SPL::Functions::Time::getTimestamp();
	  uint64_t timeInNanos = ((SPL::Functions::Time::getSeconds(tsNow) * (int64_t)1000000000) + (int64_t)SPL::Functions::Time::getNanoseconds(tsNow));
	  pid_t myPid= getpid();
	  pthread_t myTid = pthread_self();
	  uint64_t dbSignature = timeInNanos + (uint64_t)myPid + (uint64_t)myTid;
	  ostringstream dbSigStr;
	  dbSigStr << dbSignature;

	  //Try to get a distributed lock.
	  while(1) {
		  // This is an atomic activity.
		  // If multiple threads attempt to do it at the same time, only one will succeed.
		  // Winner will hold the lock until they release it voluntarily or
		  // until the Cassandra back-end removes this lock entry after the lease time ends.
		  std::string statementString = "insert into " + mainTableName +
			" (r_key, c_key, t_val, dbsig) values('" + lockRowName + "', '" + distributedLockKey +
			"', '1', '" + dbSigStr.str() + "') if not exists using ttl " + leaseTimeStream.str() + ";";

		  CassError rc = CASS_OK;
		  std::string errorMsg = "";
		  rc = execute_query(session, statementString.c_str(), errorMsg);

		  // As of Sep/2014, Cassandra C API returns no error if the row already exists. Hence, any error means db table not being available.
		  if (rc != CASS_OK) {
			  // There is some db error.
			  // dps_lock_dl_lock row may not be there.
			  // Unable to get an existing lock id from the cache.
			  lkError.set("DB insert error. Unable to acquire the lockName " + lockIdString + ". " + errorMsg, DL_GET_LOCK_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "DB insert error. Inside acquireLock, it failed for the lockName " << lockIdString << ". Error = " << errorMsg << ". " << DL_GET_LOCK_ID_ERROR, "CassandraDBLayer");
			  return(false);
		  }

		  // We have to read the value and confirm that we really inserted the row or a row for this lock already exists.
		  statementString = "select dbsig from " + mainTableName + " where r_key='" + lockRowName + "' and c_key='" + distributedLockKey + "';";
		  int32_t columnCnt = 0;
		  CassStatement* statement = NULL;
		  CassFuture* future = NULL;
		  CassString query = cass_string_init(statementString.c_str());
		  statement = cass_statement_new(query, 0);
		  future = cass_session_execute(session, statement);
		  cass_future_wait(future);
		  CassString currentDbSig;

		  rc = cass_future_error_code(future);
		  string sigStrFromDb = "";

		  if(rc != CASS_OK) {
			  // Read error.
			  CassString message = cass_future_error_message(future);
			  cass_future_free(future);
			  // Unable to get an existing lock id from the cache.
			  lkError.set("DB read error. Unable to get the lockId for the lockName " + lockIdString + ". " + std::string(message.data, message.length), DL_GET_LOCK_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "DB read error. Inside acquireLock, it failed for the lockName " << lockIdString << ". Error = " << string(message.data, message.length) << ". " << DL_GET_LOCK_ID_ERROR, "CassandraDBLayer");
			  return(false);
		  } else {
			  const CassResult* result = cass_future_get_result(future);
			  CassIterator* iterator = cass_iterator_from_result(result);

			  if(cass_iterator_next(iterator)) {
				  // We at least have one resulting column.
				  columnCnt++;
				  const CassRow* row = cass_iterator_get_row(iterator);
				  cass_value_get_string(cass_row_get_column(row, 0), &currentDbSig);
				  sigStrFromDb = string(currentDbSig.data, currentDbSig.length);
			  }

			  cass_result_free(result);
			  cass_iterator_free(iterator);
		  }

		  cass_future_free(future);
		  cass_statement_free(statement);

		  /*
		  // I'm commenting out this block. In Cassandra, I have noticed that executing a select statement right after an insert statement
	      // returns 0 results. We will not return if we hit that condition. Instead, we will stay in the while loop and try again.
		  if (columnCnt <= 0) {
			  // Unable to get an existing lock id from the cache.
			  lkError.set("Missing lock id row: Unable to get a unique lock id for a lock named " + lockIdString + ".", DL_GUID_CREATION_ERROR);
			  SPLAPPTRC(L_DEBUG, "Missing lock id row: Inside acquireLock, it failed for a lock named " << lockIdString << ". " << DL_GUID_CREATION_ERROR, "CassandraDBLayer");
			  return (false);
		  }
		  */

		  // If we have a column of result, let us check if this was inserted by the calling process/thread.
		  if (columnCnt > 0) {
			  if (sigStrFromDb == dbSigStr.str()) {
				  // This is the row we expected to have for this lock.
				  SPLAPPTRC(L_DEBUG, "Inside acquireLock done for a lock named " << lockIdString, "CassandraDBLayer");
				  // We got the lock.
				  // Let us update the lock information now.
				  time_t new_lock_expiry_time = time(0) + (time_t)leaseTime;
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
					  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "CassandraDBLayer");
				  } else {
					  // Is current time greater than the lock expiration time?
					  if ((_lockExpirationTime > 0) && (time(0) > (time_t)_lockExpirationTime)) {
						  // Time has passed beyond the lease of this lock.
						  // Lease expired for this lock. Original owner forgot to release the lock and simply left it hanging there without a valid lease.
						  releaseLock(lock, lkError);
					  }
				  }
			  }
		  }

		  // Someone else is holding on to this distributed lock. Wait for a while before trying again.
			retryCnt++;

			if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
				lkError.set("Max retry count reached: Unable to acquire the lock named " + lockIdString + ".", DL_GET_LOCK_ERROR);
				SPLAPPTRC(L_DEBUG, "Max retry count reached: Inside acquireLock, it failed for a lock named " << lockIdString << ". " << DL_GET_LOCK_ERROR, "CassandraDBLayer");
				// Our caller can check the error code and try to acquire the lock again.
				return(false);
			}

			// Check if we have gone past the maximum wait time the caller was willing to wait in order to acquire this lock.
			time(&timeNow);
			if (difftime(startTime, timeNow) > maxWaitTimeToAcquireLock) {
				lkError.set("Unable to acquire the lock named " + lockIdString + " within the caller specified wait time.", DL_GET_LOCK_TIMEOUT_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire the lock named " << lockIdString <<
					" within the caller specified wait time." << DL_GET_LOCK_TIMEOUT_ERROR, "CassandraDBLayer");
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
			lkError.reset();
	  } // End of while(1)
  }

  void CassandraDBLayer::releaseLock(uint64_t lock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside releaseLock for lock id " << lock, "CassandraDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();

	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  string statementString = "delete from " +  mainTableName + " where r_key='" +
         lockRowName + "' and c_key='" + distributedLockKey + "';";
	  string errorMsg;
	  CassError rc = CASS_OK;
	  rc = execute_query(session, statementString.c_str(), errorMsg);

	  if (rc != CASS_OK) {
		  lkError.set("Unable to release the lock id " + lockIdString + ". Error=" + errorMsg, DL_LOCK_RELEASE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside releaseLock, it failed for a lock named " << lockIdString << ". Error=" << errorMsg << ". " << DL_LOCK_RELEASE_ERROR, "CassandraDBLayer");
		  return;
	  }

	  updateLockInformation(lockIdString, lkError, 0, 0, 0);
  }

  bool CassandraDBLayer::updateLockInformation(std::string const & lockIdString,
	PersistenceError & lkError, uint32_t const & lockUsageCnt, int32_t const & lockExpirationTime, pid_t const & lockOwningPid) {
	  // Get the lock name for this lock.
	  uint32_t _lockUsageCnt = 0;
	  int32_t _lockExpirationTime = 0;
	  std::string _lockName = "";
	  pid_t _lockOwningPid = 0;

	  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "CassandraDBLayer");
		  return(false);
	  }

	  ostringstream lockInfoValue;
	  // 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  lockInfoValue << lockUsageCnt << "_" << lockExpirationTime << "_" << lockOwningPid << "_" << _lockName;
	  string lockInfoValueString = lockInfoValue.str();

	  std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  std::string statementString = "update " + mainTableName + " set info='" + lockInfoValueString +
	     "' where r_key='" + lockRowName + "' and c_key='" + lockInfoKey + "';";

	  CassError rc = CASS_OK;
	  std::string errorMsg = "";
	  rc = execute_query(session, statementString.c_str(), errorMsg);

	  // Any error means db table not being available.
	  if (rc != CASS_OK) {
		  // There is some db error.
		  // dps_lock_dl_lock row may not be there.
		  // Unable to get an existing lock id from the cache.
		  lkError.set("DB update error. Unable to get the lockId for the lockId " + lockIdString + ". " + errorMsg, DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "DB update error. Inside updateLockInformation, it failed for the lockId " << lockIdString << ". Error = " << errorMsg << ". " << DL_LOCK_INFO_UPDATE_ERROR, "CassandraDBLayer");
		  return(false);
	  } else {
		  return(true);
	  }
  }

  bool CassandraDBLayer::readLockInformation(std::string const & lockIdString, PersistenceError & lkError, uint32_t & lockUsageCnt,
		  int32_t & lockExpirationTime, pid_t & lockOwningPid, std::string & lockName) {
	  // Read the contents of the lock information.
	  lockName = "";

	  // Lock Info contains meta data information about a given lock.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  string statementString = "select info from " + mainTableName + " where r_key='" +
         lockRowName + "' and c_key='" + lockInfoKey + "';";
	  int32_t columnCnt = 0;
	  CassStatement* statement = NULL;
	  CassFuture* future = NULL;
	  CassString query = cass_string_init(statementString.c_str());
	  statement = cass_statement_new(query, 0);
	  future = cass_session_execute(session, statement);
	  cass_future_wait(future);
	  CassString info;
	  string lockInfo = "";
	  CassError rc = CASS_OK;

	  rc = cass_future_error_code(future);

	  if(rc != CASS_OK) {
		  // Read error.
		  CassString message = cass_future_error_message(future);
		  cass_future_free(future);
		  // Unable to get the LockInfo from our cache.
		  lkError.set("Unable to get LockInfo using the LockId " + lockIdString + ". " + std::string(message.data, message.length), DL_GET_LOCK_INFO_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside readLockInformation, it failed for the LockId " << lockIdString << ". Error = " << string(message.data, message.length) << ". " << DL_GET_LOCK_INFO_ERROR, "CassandraDBLayer");
		  return(false);
	  } else {
		  const CassResult* result = cass_future_get_result(future);
		  CassIterator* iterator = cass_iterator_from_result(result);

		  if(cass_iterator_next(iterator)) {
			  // We at least have one resulting column.
			  columnCnt++;
			  const CassRow* row = cass_iterator_get_row(iterator);
			  cass_value_get_string(cass_row_get_column(row, 0), &info);
			  lockInfo = string(info.data, info.length);
		  }

		  cass_result_free(result);
		  cass_iterator_free(iterator);
	  }

	  cass_future_free(future);
	  cass_statement_free(statement);

	  if (columnCnt <= 0) {
		  // Unable to get an existing lock id from the cache.
		  lkError.set("Missing lock info row: Unable to get LockInfo using the LockId " + lockIdString + ".", DL_GET_LOCK_INFO_ERROR);
		  SPLAPPTRC(L_DEBUG, "Missing lock info row: Inside readLockInformation, it failed for the LockId " << lockIdString << ". " << DL_GET_LOCK_INFO_ERROR, "CassandraDBLayer");
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
  bool CassandraDBLayer::lockIdExistsOrNot(string lockIdString, PersistenceError & lkError) {
	  // Get the lock name for this lock.
	  uint32_t _lockUsageCnt = 0;
	  int32_t _lockExpirationTime = 0;
	  std::string _lockName = "";
	  pid_t _lockOwningPid = 0;

	  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
		  SPLAPPTRC(L_DEBUG, "LockIdExistsOrNot: Unable to get LockInfo for the lockId " << lockIdString << ". " << lkError.getErrorCode(), "CassandraDBLayer");
		  return(false);
	  } else {
		  return(true);
	  }
  }

  // This method will return the process id that currently owns the given lock.
  uint32_t CassandraDBLayer::getPidForLock(string const & name, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside getPidForLock with a name " << name, "CassandraDBLayer");
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
		  SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "CassandraDBLayer");
		  return(0);
	  } else {
		  return(_lockOwningPid);
	  }
  }

  // This method will return the status of the connection to the back-end data store.
  bool CassandraDBLayer::isConnected() {
          // Not implemented at this time.
          return(true);
  }

  bool CassandraDBLayer::reconnect(std::set<std::string> & dbServers, PersistenceError & dbError) {
          // Not implemented at this time.
          return(true);
  }

} } } } }
using namespace com::ibm::streamsx::store;
using namespace com::ibm::streamsx::store::distributed;
extern "C" {
	DBLayer * create(){
	   return new CassandraDBLayer();
	}
}
