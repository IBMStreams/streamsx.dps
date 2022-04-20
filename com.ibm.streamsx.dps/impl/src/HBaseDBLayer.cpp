/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2022
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
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

/*
==================================================================================================================
This CPP file contains the source code for the distributed process store (dps) back-end activities such as
insert, update, read, remove, etc. This dps implementation runs on top of the HBase NoSQL DB.

At this time (Nov/2014), HBase is a good NoSQL data store that is mostly popular in the Hadoop user community.
Like Cassandra, it has its design roots borrowed from the Google BigTable paper. Both Cassandra and HBase
try to implement the design ideas published in the Google BigTable paper. Both of them are column oriented
databases. HBase can sit on top of an existing Hadoop installation and use the HDFS as its storage medium.
As far as the client APIs, HBase has very good Java support, a decent Thrift support and an okay REST support.
C APIs are not there yet at an acceptable level for HBase access. There in ongoing work right now to provide
C APIs for HBase via the open source libhbase from github sponsored by mapr. In our implementation HBase for dps
below, we will mostly use the REST APIs. (We will explore the use of the above-mentioned C APIs if time permits.)

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
HBase interface code (this file), and your HBase infrastructure.

As a first step, you should run the ./mk script from the C++ project directory (DistributedProcessStoreLib).
That will take care of building the .so file for the dps and copying it to the SPL project's impl/lib directory.
==================================================================================================================
*/

#include "HBaseDBLayer.h"
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
  HBaseDBLayer::HBaseDBLayer()
  {
	  mainTableName = string(HBASE_DPS_MAIN_TABLE);
	  lockRowName = string(DPS_LOCK_TOKEN) + "_" + string(DL_LOCK_TOKEN);
	  curlGlobalCleanupNeeded = false;
	  // Default expiration value for our TTL K/V pairs.
	  currentTTLValue = 300;
	  currentHBaseUrlIdx = 0;

	  // Initialize the REST server addresses to empty string.
	  for (int32_t cnt=0; cnt < 50; cnt++) {
		  hBaseBaseUrlPool[cnt] = "";
	  }

	  httpVerbUsedInPreviousRunCommand = "";
	  base64_chars = string("ABCDEFGHIJKLMNOPQRSTUVWXYZ") +
			  	  	 string("abcdefghijklmnopqrstuvwxyz") +
			  	  	 string("0123456789+/");

	  // We must initialize all the cURL related pointers we have as member variables in our C++ class.
	  // If we don't do that, it may point to random memory locations which will give GPF during runtime.
	  curlForCreateHBaseTable = NULL;
	  curlForDeleteHBaseTable = NULL;
	  curlForCreateOrUpdateHBaseColumn = NULL;
	  curlForReadHBaseCellValue = NULL;
	  curlForDeleteHBase_Column_CF_Row = NULL;
	  curlForGetNumberOfColumnsInHBaseTableRow = NULL;
	  curlForGetAllColumnsInHBaseTableRow = NULL;
	  curlForHBaseTableExistenceCheck = NULL;
	  curlForRunDataStoreCommand = NULL;
	  headersForCreateHBaseTable = NULL;
	  headersForDeleteHBaseTable = NULL;
	  headersForCreateOrUpdateHBaseColumn = NULL;
	  headersForReadHBaseCellValue = NULL;
	  headersForDeleteHBase_Column_CF_Row = NULL;
	  headersForGetNumberOfColumnsInHBaseTableRow = NULL;
	  headersForGetAllColumnsInHBaseTableRow = NULL;
	  headersForHBaseTableExistenceCheck = NULL;
	  headersForRunDataStoreCommand = NULL;
  }

  HBaseDBLayer::~HBaseDBLayer()
  {
	  // Application is ending now.
	  // Release the cURL resources we allocated at the application start-up time.
	  if (curlGlobalCleanupNeeded == true) {
		  // Free the cURL headers used with every cURL handle.
		  curl_slist_free_all(headersForCreateHBaseTable);
		  curl_slist_free_all(headersForDeleteHBaseTable);
		  curl_slist_free_all(headersForCreateOrUpdateHBaseColumn);
		  curl_slist_free_all(headersForReadHBaseCellValue);
		  curl_slist_free_all(headersForDeleteHBase_Column_CF_Row);
		  curl_slist_free_all(headersForGetNumberOfColumnsInHBaseTableRow);
		  curl_slist_free_all(headersForGetAllColumnsInHBaseTableRow);
		  curl_slist_free_all(headersForHBaseTableExistenceCheck);
		  curl_slist_free_all(headersForRunDataStoreCommand);
		  // Free the cURL handles.
		  curl_easy_cleanup(curlForCreateHBaseTable);
		  curl_easy_cleanup(curlForDeleteHBaseTable);
		  curl_easy_cleanup(curlForCreateOrUpdateHBaseColumn);
		  curl_easy_cleanup(curlForReadHBaseCellValue);
		  curl_easy_cleanup(curlForDeleteHBase_Column_CF_Row);
		  curl_easy_cleanup(curlForGetNumberOfColumnsInHBaseTableRow);
		  curl_easy_cleanup(curlForGetAllColumnsInHBaseTableRow);
		  curl_easy_cleanup(curlForHBaseTableExistenceCheck);
		  curl_easy_cleanup(curlForRunDataStoreCommand);
		  // Do the cURL global cleanup.
		  curl_global_cleanup();
	  }
  }
        
  void HBaseDBLayer::connectToDatabase(std::set<std::string> const & dbServers, PersistenceError & dbError)
  {
	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase", "HBaseDBLayer");

	  // Get the name, OS version and CPU type of this machine.
	  struct utsname machineDetails;

	  if(uname(&machineDetails) < 0) {
		  dbError.set("Unable to get the machine/os/cpu details.", DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to get the machine/os/cpu details. " << DPS_INITIALIZE_ERROR, "HBaseDBLayer");
		  return;
	  } else {
		  nameOfThisMachine = string(machineDetails.nodename);
		  osVersionOfThisMachine = string(machineDetails.sysname) + string(" ") + string(machineDetails.release);
		  cpuTypeOfThisMachine = string(machineDetails.machine);
	  }

	  string hBaseConnectionErrorMsg = "";
	  CURLcode result = curl_global_init(CURL_GLOBAL_ALL);

	  if (result != CURLE_OK) {
		  hBaseConnectionErrorMsg = "cURL global init failed.";
		  dbError.set(hBaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed  with a cURL error code=" << result <<
		     ", Error Msg='" << string(curl_easy_strerror(result)) << "'. " << DPS_INITIALIZE_ERROR, "HBaseDBLayer");
		  return;
	  }

	  // Create all the cURL handles we will be using during the lifetime of this dps instance.
	  // This way, we will create all the cURL handles only once and reuse the handles as long as we want.
	  curlForCreateHBaseTable = curl_easy_init();

	  if (curlForCreateHBaseTable == NULL) {
		  curl_global_cleanup();
		  hBaseConnectionErrorMsg = "cURL easy init failed for CreateHBaseTable.";
		  dbError.set(hBaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for CreateHBaseTable. " <<
		     DPS_INITIALIZE_ERROR, "HBaseDBLayer");
		  return;
	  }

	  curlForDeleteHBaseTable = curl_easy_init();

	  if (curlForDeleteHBaseTable == NULL) {
		  curl_easy_cleanup(curlForCreateHBaseTable);
		  curl_global_cleanup();
		  hBaseConnectionErrorMsg = "cURL easy init failed for DeleteHBaseTable.";
		  dbError.set(hBaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for DeleteHBaseTable. " <<
		     DPS_INITIALIZE_ERROR, "HBaseDBLayer");
		  return;
	  }

	  curlForCreateOrUpdateHBaseColumn = curl_easy_init();

	  if (curlForCreateOrUpdateHBaseColumn == NULL) {
		  curl_easy_cleanup(curlForCreateHBaseTable);
		  curl_easy_cleanup(curlForDeleteHBaseTable);
		  curl_global_cleanup();
		  hBaseConnectionErrorMsg = "cURL easy init failed for CreateOrUpdateHBaseColumn.";
		  dbError.set(hBaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for CreateOrUpdateHBaseColumn. " <<
		     DPS_INITIALIZE_ERROR, "HBaseDBLayer");
		  return;
	  }

	  curlForReadHBaseCellValue = curl_easy_init();

	  if (curlForReadHBaseCellValue == NULL) {
		  curl_easy_cleanup(curlForCreateHBaseTable);
		  curl_easy_cleanup(curlForDeleteHBaseTable);
		  curl_easy_cleanup(curlForCreateOrUpdateHBaseColumn);
		  curl_global_cleanup();
		  hBaseConnectionErrorMsg = "cURL easy init failed for ReadHBaseCellValue.";
		  dbError.set(hBaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for ReadHBaseCellValue. " <<
		     DPS_INITIALIZE_ERROR, "HBaseDBLayer");
		  return;
	  }

	  curlForDeleteHBase_Column_CF_Row = curl_easy_init();

	  if (curlForDeleteHBase_Column_CF_Row == NULL) {
		  curl_easy_cleanup(curlForCreateHBaseTable);
		  curl_easy_cleanup(curlForDeleteHBaseTable);
		  curl_easy_cleanup(curlForCreateOrUpdateHBaseColumn);
		  curl_easy_cleanup(curlForReadHBaseCellValue);
		  curl_global_cleanup();
		  hBaseConnectionErrorMsg = "cURL easy init failed for DeleteHBaseColumn.";
		  dbError.set(hBaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for DeleteHBaseColumn. " <<
		     DPS_INITIALIZE_ERROR, "HBaseDBLayer");
		  return;
	  }

	  curlForGetNumberOfColumnsInHBaseTableRow = curl_easy_init();

	  if (curlForGetNumberOfColumnsInHBaseTableRow == NULL) {
		  curl_easy_cleanup(curlForCreateHBaseTable);
		  curl_easy_cleanup(curlForDeleteHBaseTable);
		  curl_easy_cleanup(curlForCreateOrUpdateHBaseColumn);
		  curl_easy_cleanup(curlForReadHBaseCellValue);
		  curl_easy_cleanup(curlForDeleteHBase_Column_CF_Row);
		  curl_global_cleanup();
		  hBaseConnectionErrorMsg = "cURL easy init failed for GetNumberOfColumnsInHBaseTableRow .";
		  dbError.set(hBaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for GetNumberOfColumnsInHBaseTableRow. " <<
		     DPS_INITIALIZE_ERROR, "HBaseDBLayer");
		  return;
	  }

	  curlForGetAllColumnsInHBaseTableRow = curl_easy_init();

	  if (curlForGetAllColumnsInHBaseTableRow == NULL) {
		  curl_easy_cleanup(curlForCreateHBaseTable);
		  curl_easy_cleanup(curlForDeleteHBaseTable);
		  curl_easy_cleanup(curlForCreateOrUpdateHBaseColumn);
		  curl_easy_cleanup(curlForReadHBaseCellValue);
		  curl_easy_cleanup(curlForDeleteHBase_Column_CF_Row);
		  curl_easy_cleanup(curlForGetNumberOfColumnsInHBaseTableRow);
		  curl_global_cleanup();
		  hBaseConnectionErrorMsg = "cURL easy init failed for GetAllColumnsInHBaseTableRow.";
		  dbError.set(hBaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for GetAllColumnsInHBaseTableRow. " <<
		     DPS_INITIALIZE_ERROR, "HBaseDBLayer");
		  return;
	  }

	  curlForHBaseTableExistenceCheck = curl_easy_init();

	  if (curlForHBaseTableExistenceCheck == NULL) {
		  curl_easy_cleanup(curlForCreateHBaseTable);
		  curl_easy_cleanup(curlForDeleteHBaseTable);
		  curl_easy_cleanup(curlForCreateOrUpdateHBaseColumn);
		  curl_easy_cleanup(curlForReadHBaseCellValue);
		  curl_easy_cleanup(curlForDeleteHBase_Column_CF_Row);
		  curl_easy_cleanup(curlForGetNumberOfColumnsInHBaseTableRow);
		  curl_easy_cleanup(curlForGetAllColumnsInHBaseTableRow);
		  curl_global_cleanup();
		  hBaseConnectionErrorMsg = "cURL easy init failed for HBaseTableExistenceCheck.";
		  dbError.set(hBaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for HBaseTableExistenceCheck. " <<
		     DPS_INITIALIZE_ERROR, "HBaseDBLayer");
		  return;
	  }

	  curlForRunDataStoreCommand = curl_easy_init();

	  if (curlForRunDataStoreCommand == NULL) {
		  curl_easy_cleanup(curlForCreateHBaseTable);
		  curl_easy_cleanup(curlForDeleteHBaseTable);
		  curl_easy_cleanup(curlForCreateOrUpdateHBaseColumn);
		  curl_easy_cleanup(curlForReadHBaseCellValue);
		  curl_easy_cleanup(curlForDeleteHBase_Column_CF_Row);
		  curl_easy_cleanup(curlForGetNumberOfColumnsInHBaseTableRow);
		  curl_easy_cleanup(curlForGetAllColumnsInHBaseTableRow);
		  curl_easy_cleanup(curlForHBaseTableExistenceCheck);
		  curl_global_cleanup();
		  hBaseConnectionErrorMsg = "cURL easy init failed for curlForRunDataStoreCommand.";
		  dbError.set(hBaseConnectionErrorMsg, DPS_INITIALIZE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, cURL easy init failed for curlForRunDataStoreCommand. " <<
		     DPS_INITIALIZE_ERROR, "HBaseDBLayer");
		  return;
	  }

	  headersForCreateHBaseTable = curl_slist_append(headersForCreateHBaseTable, "Accept: application/json");
	  headersForCreateHBaseTable = curl_slist_append(headersForCreateHBaseTable, "Content-Type: application/json");
	  headersForDeleteHBaseTable = curl_slist_append(headersForDeleteHBaseTable, "Accept: application/json");
	  headersForDeleteHBaseTable = curl_slist_append(headersForDeleteHBaseTable, "Content-Type: application/json");
	  headersForCreateOrUpdateHBaseColumn = curl_slist_append(headersForCreateOrUpdateHBaseColumn, "Accept: application/json");
	  headersForCreateOrUpdateHBaseColumn = curl_slist_append(headersForCreateOrUpdateHBaseColumn, "Content-Type: application/json");
	  headersForReadHBaseCellValue = curl_slist_append(headersForReadHBaseCellValue, "Accept: application/json");
	  headersForReadHBaseCellValue = curl_slist_append(headersForReadHBaseCellValue, "Content-Type: application/json");
	  headersForDeleteHBase_Column_CF_Row = curl_slist_append(headersForDeleteHBase_Column_CF_Row, "Accept: application/json");
	  headersForDeleteHBase_Column_CF_Row = curl_slist_append(headersForDeleteHBase_Column_CF_Row, "Content-Type: application/json");
	  headersForGetNumberOfColumnsInHBaseTableRow = curl_slist_append(headersForGetNumberOfColumnsInHBaseTableRow, "Accept: application/json");
	  headersForGetNumberOfColumnsInHBaseTableRow = curl_slist_append(headersForGetNumberOfColumnsInHBaseTableRow, "Content-Type: application/json");
	  headersForGetAllColumnsInHBaseTableRow = curl_slist_append(headersForGetAllColumnsInHBaseTableRow, "Accept: application/json");
	  headersForGetAllColumnsInHBaseTableRow = curl_slist_append(headersForGetAllColumnsInHBaseTableRow, "Content-Type: application/json");
	  headersForHBaseTableExistenceCheck = curl_slist_append(headersForHBaseTableExistenceCheck, "Accept: application/json");
	  headersForHBaseTableExistenceCheck = curl_slist_append(headersForHBaseTableExistenceCheck, "Content-Type: application/json");
	  headersForRunDataStoreCommand = curl_slist_append(headersForRunDataStoreCommand, "Accept: application/json");
	  headersForRunDataStoreCommand = curl_slist_append(headersForRunDataStoreCommand, "Content-Type: application/json");

	  // Set this flag so that we can release all the cURL related resources in this class' destructor method.
	  curlGlobalCleanupNeeded = true;

	  if (dbServers.size() == 0) {
		  hBaseConnectionErrorMsg = "Missing HBase URL.";
		  dbError.set(hBaseConnectionErrorMsg, DPS_MISSING_HBASE_ACCESS_URL);
		  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed with an error '" << hBaseConnectionErrorMsg
		     << "'. " << DPS_MISSING_HBASE_ACCESS_URL, "HBaseDBLayer");
		  return;
	  } else {
		  int32_t urlCnt = 0;
		  string url = "";
		  // We only need a single HBase URL to all our dps tasks. Let us read that here.
		  for (std::set<std::string>::iterator it=dbServers.begin(); it!=dbServers.end(); ++it) {
			  // URL format should be: http://user:password@HBase-REST-ServerNameOrIPAddress:port
			  url = *it;

			  // If the URL doesn't end with a forward slash, add one now.
			  if (url.at(url.length()-1) != '/') {
				  url += string("/");
			  }

			  // Store it in our URL pool that will be used to spray the REST API calls to multiple REST servers.
			  hBaseBaseUrlPool[urlCnt++] = url;
			  // Exit this loop if our server address pool is full.
			  if (urlCnt >= 50) {
				  break;
			  }
		  }
	  }

	  // We have now initialized the cURL layer and also obtained the user configured HBase URL.
	  // Let us go ahead and create a main table for storing the DPS store contents + DPS store/lock meta data information.
	  // Metadata row will house all the information about the store GUIDs,
	  // generic lock, store lock, and distributed locks.
	  // This will act as a catalog for all the active stores and locks in DPS.
	  //
	  // The following is the dps main table column family format in HBase.
	  //
	  // cf1 is the column family where we will store our K/V pairs.
	  // cf2 is the column family where we will store any miscellaneous information (mostly used by the store locks).
	  // cf3 is the column family where we will store the book keeping information for the locks (lock expiration time etc.)
	  //
	  //
	  // Two or more PEs (processes) could try to create this table exactly at the same time and
	  // get into HBase conflict errors. Let us avoid that by waiting for random amount of time before
	  // trying to create the table successfully in 5 consecutive attempts.
	  string url = getNextHBaseBaseUrl() + mainTableName + "/schema";
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  int32_t attemptCnt = 1;

 	  // [I found out through trial and error that the case for the different fields in the
 	  // JSON should exactly be as shown below. Even a slight change in the lowercase or uppercase will
 	  // give errors in creating the table.]
	  string schema = "{\"name\": \"" + mainTableName + "\", \"ColumnSchema\": [" +
			  	  	  "{\"name\": \"cf1\", \"VERSIONS\": \"1\", \"IN_MEMORY\": \"TRUE\"}, " +
			  	  	  "{\"name\": \"cf2\", \"VERSIONS\": \"1\", \"IN_MEMORY\": \"TRUE\"}, " +
			  	  	  "{\"name\": \"cf3\", \"VERSIONS\": \"1\", \"IN_MEMORY\": \"TRUE\"}]}";

	  // Seed the random number generator for every PE using its unique PE number.
	  int32_t seedValue = (int32_t)SPL::Functions::Utility::jobID() + (int32_t)SPL::Functions::Utility::PEID();
	  SPL::Functions::Math::srand(seedValue);

	  // Every PE will make 5 consecutive attempts to create the main table (if it doesn't already exist) before giving up.
	  while(attemptCnt++ <= 5) {
		  // Do a random wait before trying to create it.
		  // This will give random value between 0 and 1.
		  float64 rand = SPL::Functions::Math::random();
		  // Let us wait for that random duration which is a partial second i.e. less than a second.
		  SPL::Functions::Utility::block(rand);

		  // Before attempting to create a new table, let us check if some other PE already created this table.
		  // In that case, we need not create this table in the middle of other PEs might be using this table.
		  if (checkIfHBaseTableExists(mainTableName) == true) {
			  // This table already exists. No need to create it again.
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, table '" << mainTableName <<
			     "' already exists. Skipping the initialization step to create this table.", "HBaseDBLayer");
			  break;
		  }

		  bool hBaseResult =  createHBaseTable(url, schema, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		  // We will tolerate errors within our maximum attempt counts.
		  if (attemptCnt == 5 && hBaseResult == false && curlReturnCode == -1) {
			  // cURL initialization problems.
			  dbError.set("Unable to easy initialize cURL for the main table. Error=" + curlErrorString, DPS_CONNECTION_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to easy initialize cURL for the main table. Error Msg=" <<
			     curlErrorString << ". " << DPS_CONNECTION_ERROR, "HBaseDBLayer");
			  return;
		  } else if (attemptCnt == 5 && hBaseResult == false && curlReturnCode > 0) {
			  // Other cURL errors.
			  ostringstream curlErrorCode;
			  curlErrorCode << curlReturnCode;
			  dbError.set("Unable to create a new HBase table named " + mainTableName +
				  ". cURL Error code=" + curlErrorCode.str() + ", cURL Error msg=" +
				  curlErrorString + ".", DPS_CONNECTION_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to create a new HBase table named " <<
				 mainTableName << ". cURL Error code=" << curlErrorCode.str() << ", cURL Error msg="
		         << curlErrorString << ". DPS Error code=" << DPS_CONNECTION_ERROR, "HBaseDBLayer");
			  return;
		  } else if (attemptCnt == 5 && hBaseResult == false && httpResponseCode > 0) {
			  ostringstream httpErrorCode;
			  httpErrorCode << httpResponseCode;
			  dbError.set("Unable to create a new HBase table named " + mainTableName +
				 ". HTTP response code=" + httpErrorCode.str() + ", HTTP Error msg=" + httpReasonString, DPS_CONNECTION_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to create a new HBase table named " <<
				 mainTableName << ". HTTP response code=" << httpErrorCode.str() <<
			     ", HTTP Error msg=" << httpReasonString << ". DPS Error code=" << DPS_CONNECTION_ERROR, "HBaseDBLayer");
			  return;
		  }

		  if (hBaseResult == true) {
			  // Table creation either succeeded or the table already exists.
			  break;
		  }
	  } // End of while loop.

	  // In the same way, ensure that the TTL table is also there (or create if it is not there).
	  url = getNextHBaseBaseUrl() + string(DPS_TTL_STORE_TOKEN) + "/schema";
	  curlReturnCode = 0;
	  curlErrorString = "";
	  httpResponseCode = 0;
	  httpReasonString = "";
	  attemptCnt = 1;
	  // We are going to set the default TTL value to 300 seconds for all our TTL entries.
	  // Users can change the TTL value via the dpsPutTTL API.
	  // Unlike memcached, Redis, and Cassandra, HBase doesn't allow individual TTL values for
	  // every K/V pair. Instead, HBase allows one TTL value for all the K/V Pairs in one table.
	  // This is very limiting in HBase. Users have to accept this fact and come up with one
	  // TTL value for all the K/V pairs that will be inserted via the dpsPutTTL API.
	  //
 	  // [I found out through trial and error that the case for the different fields in the
 	  // JSON should exactly be as shown below. Even a slight change in the lowercase or uppercase will
 	  // give errors in creating the table.]
	  schema = "{\"name\": \"" + string(DPS_TTL_STORE_TOKEN) + "\", \"ColumnSchema\": [" +
			   "{\"name\": \"cf1\", \"VERSIONS\": \"1\", \"IN_MEMORY\": \"TRUE\", \"TTL\": \"300\"}]}";

	  // Every PE will make 5 consecutive attempts to create the TTL table (if it doesn't already exist) before giving up.
	  while(attemptCnt++ <= 5) {
		  // Do a random wait before trying to create it.
		  // This will give random value between 0 and 1.
		  float64 rand = SPL::Functions::Math::random();
		  // Let us wait for that random duration which is a partial second i.e. less than a second.
		  SPL::Functions::Utility::block(rand);

		  // Before attempting to create a new table, let us check if some other PE already created this table.
		  // In that case, we need not create this table in the middle of other PEs might be using this table.
		  if (checkIfHBaseTableExists(string(DPS_TTL_STORE_TOKEN)) == true) {
			  // This table already exists. No need to create it again.
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, table '" << string(DPS_TTL_STORE_TOKEN) <<
			     "' already exists. Skipping the initialization step to create this table.", "HBaseDBLayer");
			  break;
		  }

		  bool hBaseResult =  createHBaseTable(url, schema, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		  // We will tolerate errors within our maximum attempt counts.
		  if (attemptCnt == 5 && hBaseResult == false && curlReturnCode == -1) {
			  // cURL initialization problems.
			  dbError.set("Unable to easy initialize cURL for the TTL table. Error=" + curlErrorString, DPS_CONNECTION_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to easy initialize cURL for the TTL table. Error Msg=" <<
			     curlErrorString << ". " << DPS_CONNECTION_ERROR, "HBaseDBLayer");
			  return;
		  } else if (attemptCnt == 5 && hBaseResult == false && curlReturnCode > 0) {
			  // Other cURL errors.
			  ostringstream curlErrorCode;
			  curlErrorCode << curlReturnCode;
			  dbError.set("Unable to create a new HBase table named " + string(DPS_TTL_STORE_TOKEN) +
				  ". cURL Error code=" + curlErrorCode.str() + ", cURL Error msg=" +
				  curlErrorString + ".", DPS_CONNECTION_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to create a new HBase table named " <<
				 DPS_TTL_STORE_TOKEN << ". cURL Error code=" << curlErrorCode.str() << ", cURL Error msg="
		         << curlErrorString << ". DPS Error code=" << DPS_CONNECTION_ERROR, "HBaseDBLayer");
			  return;
		  } else if (attemptCnt == 5 && hBaseResult == false && httpResponseCode > 0) {
			  ostringstream httpErrorCode;
			  httpErrorCode << httpResponseCode;
			  dbError.set("Unable to create a new HBase table named " + string(DPS_TTL_STORE_TOKEN) +
				 ". HTTP response code=" + httpErrorCode.str() + ", HTTP Error msg=" + httpReasonString, DPS_CONNECTION_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase, it failed to create a new HBase table named " <<
			     DPS_TTL_STORE_TOKEN << ". HTTP response code=" << httpErrorCode.str() <<
			     ", HTTP Error msg=" << httpReasonString << ". DPS Error code=" << DPS_CONNECTION_ERROR, "HBaseDBLayer");
			  return;
		  }

		  if (hBaseResult == true) {
			  // Table creation either succeeded or the table already exists.
			  break;
		  }
	  } // End of while loop.


	  SPLAPPTRC(L_DEBUG, "Inside connectToDatabase done", "HBaseDBLayer");
  }

  uint64_t HBaseDBLayer::createStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside createStore for store " << name, "HBaseDBLayer");

	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);

 	// Get a general purpose lock so that only one thread can
	// enter inside of this method at any given time with the same store name.
 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
 		// Unable to acquire the general purpose lock.
 		dbError.set("Unable to get a generic lock for creating a store with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed for an yet to be created store with its name as " <<
 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "HBaseDBLayer");
 		// User has to retry again to create this store.
 		return (0);
 	}

    // Let us first see if a store with the given name already exists in the HBase DPS GUID row.
 	uint64_t storeId = findStore(name, dbError);

 	if (storeId > 0) {
		// This store already exists in our cache.
		// We can't create another one with the same name now.
 		std::ostringstream storeIdString;
 		storeIdString << storeId;
 		// The following error message carries the existing store's id at the very end of the message.
		dbError.set("A store named " + name + " already exists with a store id " + storeIdString.str(), DPS_STORE_EXISTS);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed while trying to create a duplicate store " << name << ". " << DPS_STORE_EXISTS, "HBaseDBLayer");
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
	We secured a guid. We can now create this store. Layout for a distributed process store (dps) looks like this in HBase.
	In the HBase DB, DPS store/lock, and the individual store details will follow these data formats.
	That will allow us to be as close to the other four DPS implementations using memcached, Redis, Cassandra and Cloudant.
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

 	// We will do the first two activities from the list above for our HBase implementation.
	// Let us now do the step 1 described in the commentary above.
	// 1) Create the Store Name entry in the dps_and_dl_guid row.
	//    '0' + 'store name' => 'store id'
	std::ostringstream storeIdString;
	storeIdString << storeId;

	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
	// After the table name in the URL, you must give a row name and it can be any random string.
	// What actually matters is the row key in the JSON doc below. That will override whatever you give in the URL.
	string dpsAndDlGuidKeyUrl = getNextHBaseBaseUrl() + mainTableName + "/RowData";
	string base64_encoded_row_key, base64_encoded_column_key, base64_encoded_column_value;
	base64_encode(string(DPS_AND_DL_GUID_KEY), base64_encoded_row_key);
	base64_encode(string("cf1:") + dpsAndDlGuidKey, base64_encoded_column_key);
	base64_encode(storeIdString.str(), base64_encoded_column_value);

	// Form the JSON payload as a literal string. (Please note that in HBase, "put" command requires us to
	// base64 encode the column keys and column values. Otherwise, it will not work.)
	// HBase expects it to be in this format.
	// {"Row":[{"key":"ZHBzX2xvY2tfZGxfbG9jaw==","Cell":[{"column":"Y2YxOjUwMTQ0NjQ2NDU2NDU2NDU2NTNnZW5lcmljX2xvY2s=",
	//  "$":"NTU1MzQ1MzQ1MzQ1MzM="}]}]}
	string jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
		base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";

	// It will create a new column.
	bool hBaseResult =  createOrUpdateHBaseColumn(dpsAndDlGuidKeyUrl, jsonDoc, curlReturnCode,
			curlErrorString, httpResponseCode, httpReasonString);
	string errorMsg = "";

	if ((hBaseResult == false) || (httpResponseCode != HBASE_REST_OK)) {
		// There was an error in creating the store name entry in the meta data table.
		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to create 'StoreName-->GUID' in HBase for the store named " + name + ". " + errorMsg, DPS_STORE_NAME_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'StoreName-->GUID' in HBase for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_NAME_CREATION_ERROR, "HBaseDBLayer");
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

	// 2) Create the Store Contents row now.
 	// This is the HBase wide row in which we will keep storing all the key value pairs belonging to this store.
	// Row name: 'dps_' + '1_' + 'store id'
 	string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString.str();
	// Every store contents row will always have these three metadata entries:
	// dps_name_of_this_store ==> 'store name'
	// dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	// dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	//
	// Every store contents wide row will have at least three entries in it carrying the actual store name, key spl type name and value spl type name.
	// In addition, inside this store contents row, we will house the
	// actual key:value data items that the user wants to keep in this store.
	// Such a store contents row is very useful for data item read, write, deletion, enumeration etc.
 	//

	// Let us populate the new store contents row with a mandatory element that will carry the name of this store.
	// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
 	string metaData1Url = getNextHBaseBaseUrl() + mainTableName + "/RowData";
	base64_encode(storeRowName, base64_encoded_row_key);
	base64_encode(string("cf1:") + string(HBASE_STORE_ID_TO_STORE_NAME_KEY), base64_encoded_column_key);
	base64_encode(base64_encoded_name, base64_encoded_column_value);
	// Form the JSON payload as a literal string.
	jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
		base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";

	hBaseResult =  createOrUpdateHBaseColumn(metaData1Url, jsonDoc, curlReturnCode,
		curlErrorString, httpResponseCode, httpReasonString);

	if ((hBaseResult == false) || (httpResponseCode != HBASE_REST_OK)) {
		// There was an error in creating Meta Data 1;
		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to create 'Meta Data1' in HBase for the store named " + name + ". " + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'Meta Data1' in HBase for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "HBaseDBLayer");
		// Since there is an error here, let us delete the store name entry we created above.
		dpsAndDlGuidKeyUrl = getNextHBaseBaseUrl() + mainTableName + "/" + string(DPS_AND_DL_GUID_KEY) + "/cf1:" + dpsAndDlGuidKey;
		deleteHBase_Column_CF_Row(dpsAndDlGuidKeyUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

    // We are now going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
	// Add the key spl type name metadata.
 	string metaData2Url = getNextHBaseBaseUrl() + mainTableName + "/RowData";
	// Form the JSON payload as a literal string.
 	string base64_encoded_keySplTypeName;
 	base64_encode(keySplTypeName, base64_encoded_keySplTypeName);
	base64_encode(storeRowName, base64_encoded_row_key);
	base64_encode(string("cf1:") + string(HBASE_SPL_TYPE_NAME_OF_KEY), base64_encoded_column_key);
	base64_encode(base64_encoded_keySplTypeName, base64_encoded_column_value);
 	// Form the JSON payload as a literal string.
	jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
		base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";

	hBaseResult =  createOrUpdateHBaseColumn(metaData2Url, jsonDoc, curlReturnCode,
		curlErrorString, httpResponseCode, httpReasonString);

	if ((hBaseResult == false) || (httpResponseCode != HBASE_REST_OK)) {
		// There was an error in creating Meta Data 2;
		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to create 'Meta Data2' in HBase for the store named " + name + ". " + errorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'Meta Data2' in HBase for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "HBaseDBLayer");
		// Since there is an error here, let us delete the store name entry we created above.
		dpsAndDlGuidKeyUrl = getNextHBaseBaseUrl() + mainTableName + "/" + string(DPS_AND_DL_GUID_KEY) + "/cf1:" + dpsAndDlGuidKey;
		deleteHBase_Column_CF_Row(dpsAndDlGuidKeyUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
		// We will also delete the store contents row we created above with the meta data.
		string storeContentsRowUrl = getNextHBaseBaseUrl() + mainTableName + "/" + storeRowName;
		deleteHBase_Column_CF_Row(storeContentsRowUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	}

	// Add the value spl type name metadata.
 	string metaData3Url = getNextHBaseBaseUrl() + mainTableName + "/RowData";
 	string base64_encoded_valueSplTypeName;
 	base64_encode(valueSplTypeName, base64_encoded_valueSplTypeName);
	base64_encode(storeRowName, base64_encoded_row_key);
	base64_encode(string("cf1:") + string(HBASE_SPL_TYPE_NAME_OF_VALUE), base64_encoded_column_key);
	base64_encode(base64_encoded_valueSplTypeName, base64_encoded_column_value);
	// Form the JSON payload as a literal string.
	jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
		base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";

	hBaseResult =  createOrUpdateHBaseColumn(metaData3Url, jsonDoc, curlReturnCode,
		curlErrorString, httpResponseCode, httpReasonString);

	if ((hBaseResult == false) || (httpResponseCode != HBASE_REST_OK)) {
		// There was an error in creating Meta Data 3;
		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to create 'Meta Data3' in HBase for the store named " + name + ". " + errorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside createStore, it failed to create 'Meta Data3' in HBase for store " <<
			name << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "HBaseDBLayer");
		// Since there is an error here, let us delete the store name entry we created above.
		dpsAndDlGuidKeyUrl = getNextHBaseBaseUrl() + mainTableName + "/" + string(DPS_AND_DL_GUID_KEY) + "/cf1:" + dpsAndDlGuidKey;
		deleteHBase_Column_CF_Row(dpsAndDlGuidKeyUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
		// We will also delete the store contents row we created above with the meta data.
		string storeContentsRowUrl = getNextHBaseBaseUrl() + mainTableName + "/" + storeRowName;
		deleteHBase_Column_CF_Row(storeContentsRowUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
		releaseGeneralPurposeLock(base64_encoded_name);
		return (0);
	} else {
		// We created a new store as requested by the caller. Return the store id.
		releaseGeneralPurposeLock(base64_encoded_name);
		return(storeId);
	}
  }

  uint64_t HBaseDBLayer::createOrGetStore(std::string const & name,
	std::string const & keySplTypeName, std::string const & valueSplTypeName,
	PersistenceError & dbError)
  {
	// We will rely on a method above this and another method below this to accomplish what is needed here.
	SPLAPPTRC(L_DEBUG, "Inside createOrGetStore for store " << name, "HBaseDBLayer");
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
	//  Cassandra, Cloudant, and HBase DPS implementations. For memcached and Redis, we do it differently.)
	dbError.reset();
	// We will take the hashCode of the encoded store name and use that as the store id.
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);
	storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);
	return(storeId);
  }
                
  uint64_t HBaseDBLayer::findStore(std::string const & name, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside findStore for store " << name, "HBaseDBLayer");

	// We can search in the dps_and_dl_guid row to see if a store exists for the given store name.
	string base64_encoded_name;
	base64_encode(name, base64_encoded_name);
	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + base64_encoded_name;
	string dpsAndDlGuidKeyUrl = getNextHBaseBaseUrl() + mainTableName + "/" + string(DPS_AND_DL_GUID_KEY) + "/cf1:" + dpsAndDlGuidKey;
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";
	string value = "";
	bool hBaseResult = readHBaseCellValue(dpsAndDlGuidKeyUrl, value, true,
		curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	if (hBaseResult == true && httpResponseCode == HBASE_REST_OK) {
		// This store already exists in our cache.
 	 	// We will take the hashCode of the encoded store name and use that as the store id.
 	 	uint64_t storeId = SPL::Functions::Utility::hashCode(base64_encoded_name);
 	 	return(storeId);
	} else if (hBaseResult == true && httpResponseCode == HBASE_REST_NOT_FOUND) {
		// Requested data item is not there in the cache.
		dbError.set("The requested store " + name + " doesn't exist.", DPS_DATA_ITEM_READ_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed for store " << name << ". " << DPS_DATA_ITEM_READ_ERROR, "HBaseDBLayer");
		return(0);
	} else {
		// Some other HTTP error.
		// Problem in finding the existence of a store.
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to find the existence of a store named " + name + ". " + errorMsg, DPS_STORE_EXISTENCE_CHECK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside findStore, it failed to find the existence of the store " << name << ". "
			<< errorMsg << ". " << DPS_STORE_EXISTENCE_CHECK_ERROR, "HBaseDBLayer");
		return(0);
	}
  }
        
  bool HBaseDBLayer::removeStore(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside removeStore for store id " << store, "HBaseDBLayer");

	ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "HBaseDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "HBaseDBLayer");
		// User has to retry again to remove the store.
		return(false);
	}

	// Get rid of these two entries that are holding the store contents and the store name root entry.
	// 1) Store Contents row
	// 2) Store Name root entry
	//
	uint32_t dataItemCnt = 0;
	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside removeStore, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		releaseStoreLock(storeIdString);
		// This is alarming. This will put this store in a bad state. Poor user has to deal with it.
		return(false);
	}

	// Let us delete the Store Contents row that contains all the active data items in this store.
	// Row Key name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
 	string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
 	string storeRowUrl = getNextHBaseBaseUrl() + mainTableName + "/" + storeRowName;
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";
	deleteHBase_Column_CF_Row(storeRowUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
	string dpsAndDlGuidKey = string(DPS_STORE_NAME_TYPE) + storeName;
	string dpsAndDlGuidKeyUrl = getNextHBaseBaseUrl() + mainTableName + "/" + string(DPS_AND_DL_GUID_KEY) + "/cf1:" + dpsAndDlGuidKey;
	// Finally, delete the StoreName key now.
	deleteHBase_Column_CF_Row(dpsAndDlGuidKeyUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
	// Life of this store ended completely with no trace left behind.
	releaseStoreLock(storeIdString);
    return(true);
  }

  // This is a lean and mean put operation into a store.
  // It doesn't do any safety checks before putting a data item into a store.
  // If you want to go through that rigor, please use the putSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool HBaseDBLayer::put(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside put for store id " << store, "HBaseDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In our HBase dps implementation, data item keys can have space characters.
	string base64_data_item_key;
	base64_encode(string(keyData, keySize), base64_data_item_key);
	// Let us convert the valueData in our K/V pair to a string.
	// In this HBase CPP file, we added a new b64_encode and b64_decode methods specifically for this purpose.
	// Let us go ahead and convert the binary data buffer into a string.
	string base64_encoded_row_key, base64_encoded_column_key, base64_encoded_column_value;
	b64_encode(valueData, valueSize, base64_encoded_column_value);

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Contents row that takes the following name.
	// Row Key name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string url = getNextHBaseBaseUrl() + mainTableName + "/RowData";
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";
	base64_encode(storeRowName, base64_encoded_row_key);
	base64_encode(string("cf1:") + base64_data_item_key, base64_encoded_column_key);
	// Form the JSON payload as a literal string.
	string jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
		base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";

	// It will either create a new column or update an existing column.
	bool hBaseResult =  createOrUpdateHBaseColumn(url, jsonDoc, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	if (hBaseResult == true && httpResponseCode == HBASE_REST_OK) {
		// We stored the data item in the K/V store.
		return(true);
	} else {
		// There is some error in storing the data item.
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to store a data item in the store id " + storeIdString + ". " + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside put, it failed to store a data item in the store id " << storeIdString << ". "
			<< errorMsg << ". " << DPS_DATA_ITEM_WRITE_ERROR, "HBaseDBLayer");
		return(false);
	}
  }

  // This is a special bullet proof version that does several safety checks before putting a data item into a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular put method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool HBaseDBLayer::putSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char const * valueData, uint32_t valueSize,
                             PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside putSafe for store id " << store, "HBaseDBLayer");

	// Let us try to store this item irrespective of whether it is
	// new or it is an existing item in the cache.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to find a store with a store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "HBaseDBLayer");
		}

		return(false);
	}

	// In our HBase dps implementation, data item keys can have space characters.
	string base64_data_item_key;
	base64_encode(string(keyData, keySize), base64_data_item_key);
	// In HBase, we can't store binary data since it supports data exchange only via JSON strings.
	// Hence, we must convert the valueData in our K/V pair to a string.
	// In this HBase CPP file, we added a new b64_encode and b64_decode methods specifically for this purpose.
	// Let us go ahead and convert the binary data buffer into a string.
	string base64_encoded_row_key, base64_encoded_column_key, base64_encoded_column_value;
	b64_encode(valueData, valueSize, base64_encoded_column_value);

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "HBaseDBLayer");
		// User has to retry again to put the data item in the store.
		return(false);
	}

	// We are ready to either store a new data item or update an existing data item.
	// This action is performed on the Store Contents row that takes the following name.
	// Row Key name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string url = getNextHBaseBaseUrl() + mainTableName + "/RowData";
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";
	base64_encode(storeRowName, base64_encoded_row_key);
	base64_encode(string("cf1:") + base64_data_item_key, base64_encoded_column_key);
	// Form the JSON payload as a literal string.
	string jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
		base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";

	// It will either create a new column or update an existing column.
	bool hBaseResult =  createOrUpdateHBaseColumn(url, jsonDoc, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	if (hBaseResult == true && httpResponseCode == HBASE_REST_OK) {
		// We stored the data item in the K/V store.
		releaseStoreLock(storeIdString);
		return(true);
	} else {
		// There is some error in storing the data item.
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to store a data item in the store id " + storeIdString + ". " + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside putSafe, it failed to store a data item in the store id " << storeIdString << ". "
			<< errorMsg << ". " << DPS_DATA_ITEM_WRITE_ERROR, "HBaseDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}
  }

  // Put a data item with a TTL (Time To Live in seconds) value into the global area of the HBase K/V store.
  bool HBaseDBLayer::putTTL(char const * keyData, uint32_t keySize,
		  	  	  	  	    unsigned char const * valueData, uint32_t valueSize,
							uint32_t ttl, PersistenceError & dbError, bool encodeKey, bool encodeValue)
  {
	  SPLAPPTRC(L_DEBUG, "Inside putTTL.", "HBaseDBLayer");

	  std::ostringstream ttlValue;
	  if (ttl > 0) {
		  ttlValue << ttl;
	  } else {
		  // If the caller passes a 0 TTL value, then we will set it to a 25 year TTL expiration period.
		  ttlValue << string(HBASE_MAX_TTL_VALUE);
	  }

	  bool hBaseResult = true;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  string url = getNextHBaseBaseUrl() + string(DPS_TTL_STORE_TOKEN) + "/schema";

	  // Unlike memcached, Redis, Cassandra etc., HBase doesn't allow TTL to be set for individual columns in the
	  // HBase tables. It only allows TTL to be set for the entire column family. It is a big weakness of HBase (as of Nov/2014).
	  // It will limit the DPS users to have only one TTL value for all the data items in a HBase column family.
	  // Our DPS users have to live with this limitation until HBase supports a TTL feature like in other NoSQL K/V stores.
	  // If the current TTL value that the user tries to set is different from the previous one he/she used, let us assign
	  // this new TTL to the column family.
	  // CAUTION: If the user changes the TTL value to lower than what was used previously, HBase will use the new TTL value
	  // to evict the existing columns in that column family as well. So, users should realize that there is a risk of existing
	  // data items getting evicted sooner or later than what was originally set at the time of inserting those data items.
	  if (ttl != currentTTLValue) {
		  // User is coming here with a new TTL value. This would have been just fine in other NoSQL data stores.
		  // But, in HBase this will affect the expiration policy for the existing columns. There is nothing we can do about it.
		  // Let us change the TTL value of the column family.
		  //
		  // If the caller passes a TTL value of zero, we will set the column family to evict data items after a 25 year period.
		  // In that case, data items will stay in this TTL table for a long time unless otherwise the user removes it manually using
		  // the dpsRemoveTTL API.
		  string schema = "{\"name\": \"" + string(DPS_TTL_STORE_TOKEN) + "\", \"ColumnSchema\": [" +
		     "{\"name\": \"cf1\", \"VERSIONS\": \"1\", \"IN_MEMORY\": \"TRUE\", \"TTL\": \"" + ttlValue.str() + "\"}]}";
		  // Change the table schema with the new TTL value.
		  hBaseResult =  createHBaseTable(url, schema, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		  if (hBaseResult == false) {
			  /*
			  // Error in setting the new TTL value.
			  string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			  dbError.setTTL("Unable to change the expiration value of the column family in the HBase TTL table. " + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside putTTL, it failed to change the expiration value of the column family in the HBase TTL table. "
				<< errorMsg << ". " << DPS_DATA_ITEM_WRITE_ERROR, "HBaseDBLayer");
			  return(false);
			  */
		  }

		  // If there are multiple threads (10 or more) attempting to change the TTL value at the very same time on our
		  // HBase DPS TTL store, I noticed the call above to fail with a HTTP response code of 400. However, one of those
		  // many attempts changed the TTL to the new value. That is why I commented out the error checking code above and
		  // pass through the following statement of setting the currentTTL value to the new one passed by the caller.
		  currentTTLValue = ttl;
	  }

		// In our HBase dps implementation, data item keys can have space characters.
		string base64_data_item_key;

                if (encodeKey == true) {
	           base64_encode(string(keyData, keySize), base64_data_item_key);
                } else {
                   // Since the key data sent here will always be in the network byte buffer format (NBF), 
                   // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
                   // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
                   // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
                   if ((uint8_t)keyData[0] < 0x80) {
                      // Skip the first length byte. 
                      base64_data_item_key = string(&keyData[1], keySize-1);  
                   } else {
                      // Skip the five bytes at the beginning that represent the length of the key data.
                      base64_data_item_key = string(&keyData[5], keySize-5);
                   }
                }

		// Let us convert the valueData in our K/V pair to a string.
		// In this HBase CPP file, we added a new b64_encode and b64_decode methods specifically for this purpose.
		// Let us go ahead and convert the binary data buffer into a string.
		string base64_encoded_row_key, base64_encoded_column_key, base64_encoded_column_value;
		b64_encode(valueData, valueSize, base64_encoded_column_value);

		// We are ready to either store a new data item or update an existing data item.
		// This action is performed on the Store Contents row that takes the following name.
		// Row Key name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
		string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + string(DPS_TTL_STORE_TOKEN);
		url = getNextHBaseBaseUrl() + string(DPS_TTL_STORE_TOKEN) + "/RowData";
		base64_encode(storeRowName, base64_encoded_row_key);
		base64_encode(string("cf1:") + base64_data_item_key, base64_encoded_column_key);
		// Form the JSON payload as a literal string.
		string jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
			base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";

		// It will either create a new column or update an existing column.
		hBaseResult =  createOrUpdateHBaseColumn(url, jsonDoc, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		if (hBaseResult == true && httpResponseCode == HBASE_REST_OK) {
			// We stored the data item in the K/V store.
			return(true);
		} else {
			// There is some error in storing the data item.
			string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			dbError.setTTL("Unable to store a data item with TTL. " + errorMsg, DPS_DATA_ITEM_WRITE_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside putTTL, it failed to store a data item with TTL. "
				<< errorMsg << ". " << DPS_DATA_ITEM_WRITE_ERROR, "HBaseDBLayer");
			return(false);
		}
  }

  // This is a lean and mean get operation from a store.
  // It doesn't do any safety checks before getting a data item from a store.
  // If you want to go through that rigor, please use the getSafe method below.
  // This version will perform better since no safety checks are done in this.
  bool HBaseDBLayer::get(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside get for store id " << store, "HBaseDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
	// Every data item's key must be prefixed with its store id before being used for CRUD operation in the HBase store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// In our HBase dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);

	bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		false, true, valueData, valueSize, dbError);

	if ((result == false) || (dbError.hasError() == true)) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside get, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
	}

    return(result);
  }

  // This is a special bullet proof version that does several safety checks before getting a data item from a store.
  // Because of these checks, it will be slower. If someone doesn't care about these safety checks,
  // then the regular get method can be used.
  // This version does all the safety checks and hence will have performance overhead.
  bool HBaseDBLayer::getSafe(uint64_t store, char const * keyData, uint32_t keySize,
                             unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside getSafe for store id " << store, "HBaseDBLayer");

	// Let us get this data item from the cache as it is.
	// Since there could be multiple data writers, we are going to get whatever is there now.
	// It is always possible that the value for the requested item can change right after
	// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
	// Every data item's key must be prefixed with its store id before being used for CRUD operation in the HBase store.
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "HBaseDBLayer");
		}

		return(false);
	}

	// In our HBase dps implementation, data item keys can have space characters.
	string data_item_key = string(keyData, keySize);
	string base64_encoded_data_item_key;
	base64_encode(data_item_key, base64_encoded_data_item_key);

	bool result = getDataItemFromStore(storeIdString, base64_encoded_data_item_key,
		false, false, valueData, valueSize, dbError);

	if ((result == false) || (dbError.hasError() == true)) {
		// Some error has occurred in reading the data item value.
		SPLAPPTRC(L_DEBUG, "Inside getSafe, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
	}

    return(result);
  }

  // Get a TTL based data item that is stored in the global area of the HBase K/V store.
   bool HBaseDBLayer::getTTL(char const * keyData, uint32_t keySize,
                              unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError, bool encodeKey)
   {
		SPLAPPTRC(L_DEBUG, "Inside getTTL.", "HBaseDBLayer");

		// Since this is a data item with TTL, it is stored in the global area of HBase and not inside a user created store.
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
		string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + string(DPS_TTL_STORE_TOKEN);
		string url = getNextHBaseBaseUrl() + string(DPS_TTL_STORE_TOKEN) + "/" + storeRowName + "/cf1:" + base64_encoded_data_item_key;
		int32_t curlReturnCode = 0;
		string curlErrorString = "";
		uint64_t httpResponseCode = 0;
		string httpReasonString = "";
		string value = "";

		// Read this K/V pair if it exists.
		bool hBaseResult = readHBaseCellValue(url, value, false,
			curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		if ((hBaseResult == true) && (httpResponseCode == HBASE_REST_OK)) {
			// K/V pair already exists.
			// In HBase, when we put the K/V pair, we b64_encoded our binary value buffer and
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
		} else {
			// Some other error.
			string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			// Unable to get the requested data item from the cache.
			dbError.setTTL("Unable to get the TTL based K/V pair from HBase. " + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			return(false);
		}
   }

  bool HBaseDBLayer::remove(uint64_t store, char const * keyData, uint32_t keySize,
                                PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside remove for store id " << store, "HBaseDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside remove, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "HBaseDBLayer");
		}

		return(false);
	}

	// Lock the store first.
	if (acquireStoreLock(storeIdString) == false) {
		// Unable to acquire the store lock.
		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "HBaseDBLayer");
		// User has to retry again to remove the data item from the store.
		return(false);
	}

	// In our HBase dps implementation, data item keys can have space characters.
	string base64_data_item_key;
	base64_encode(string(keyData, keySize), base64_data_item_key);
	// We are ready to remove a data item from the HBase table.
	// This action is performed on the Store Contents row that takes the following name.
	// Row key name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
	string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	string url = getNextHBaseBaseUrl() + mainTableName + "/" + storeRowName + "/cf1:" + base64_data_item_key;
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";
	bool hBaseResult = deleteHBase_Column_CF_Row(url, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	if ((hBaseResult == false) || ((hBaseResult == true) && (httpResponseCode == HBASE_REST_NOT_FOUND))) {
		string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("HBase error while removing the requested data item from the store id " + storeIdString +
			". " + errorMsg, DPS_DATA_ITEM_DELETE_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside remove, it failed with HBase reply error for store id " << storeIdString <<
			". " << errorMsg << ". " << DPS_DATA_ITEM_DELETE_ERROR, "HBaseDBLayer");
		releaseStoreLock(storeIdString);
		return(false);
	}

	// All done. An existing data item in the given store has been removed.
	releaseStoreLock(storeIdString);
    return(true);
  }

  // Remove a TTL based data item that is stored in the global area of the HBase K/V store.
  bool HBaseDBLayer::removeTTL(char const * keyData, uint32_t keySize,
                                PersistenceError & dbError, bool encodeKey)
  {
		SPLAPPTRC(L_DEBUG, "Inside removeTTL.", "HBaseDBLayer");

		// In our HBase dps implementation, data item keys can have space characters.
		string base64_data_item_key;

                if (encodeKey == true) {
	           base64_encode(string(keyData, keySize), base64_data_item_key);
                } else {
                   // Since the key data sent here will always be in the network byte buffer format (NBF), 
                   // we can't simply use it as it is even if the user wants us to use the non-base64 encoded key data.
                   // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
                   // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
                   if ((uint8_t)keyData[0] < 0x80) {
                      // Skip the first length byte. 
                      base64_data_item_key = string(&keyData[1], keySize-1);  
                   } else {
                      // Skip the five bytes at the beginning that represent the length of the key data.
                      base64_data_item_key = string(&keyData[5], keySize-5);
                   }
                }

		// We are ready to remove a data item from the HBase table.
		// This action is performed on the TTL row that takes the following name.
		string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + string(DPS_TTL_STORE_TOKEN);
		string url = getNextHBaseBaseUrl() + string(DPS_TTL_STORE_TOKEN) + "/" + storeRowName + "/cf1:" + base64_data_item_key;
		int32_t curlReturnCode = 0;
		string curlErrorString = "";
		uint64_t httpResponseCode = 0;
		string httpReasonString = "";
		bool hBaseResult = deleteHBase_Column_CF_Row(url, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		if ((hBaseResult == false) || ((hBaseResult == true) && (httpResponseCode == HBASE_REST_NOT_FOUND))) {
			string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			dbError.setTTL("HBase error while removing the requested TTL based data item. " + errorMsg, DPS_DATA_ITEM_DELETE_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeTTL, it failed with HBase reply error while removing a TTL based K/V pair. " <<
				errorMsg << ". " << DPS_DATA_ITEM_DELETE_ERROR, "HBaseDBLayer");
			return(false);
		}

		// All done. An existing data item in the given store has been removed.
	    return(true);
  }

  bool HBaseDBLayer::has(uint64_t store, char const * keyData, uint32_t keySize,
                             PersistenceError & dbError) 
  {
	SPLAPPTRC(L_DEBUG, "Inside has for store id " << store, "HBaseDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside has, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "HBaseDBLayer");
		}

		return(false);
	}

	// In our HBase dps implementation, data item keys can have space characters.
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
		SPLAPPTRC(L_DEBUG, "Inside has, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
	}

	return(dataItemAlreadyInCache);
  }

  // Check for the existence of a TTL based data item that is stored in the global area of the HBase K/V store.
  bool HBaseDBLayer::hasTTL(char const * keyData, uint32_t keySize,
                             PersistenceError & dbError, bool encodeKey)
  {
		SPLAPPTRC(L_DEBUG, "Inside hasTTL.", "HBaseDBLayer");

		// Since this is a data item with TTL, it is stored in the global area of HBase and not inside a user created store.
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
		string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + string(DPS_TTL_STORE_TOKEN);
		string url = getNextHBaseBaseUrl() + string(DPS_TTL_STORE_TOKEN) + "/" + storeRowName + "/cf1:" + base64_encoded_data_item_key;
		int32_t curlReturnCode = 0;
		string curlErrorString = "";
		uint64_t httpResponseCode = 0;
		string httpReasonString = "";
		string value = "";

		// Read this K/V pair if it exists.
		bool hBaseResult = readHBaseCellValue(url, value, false,
			curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		if ((hBaseResult == true) && (httpResponseCode == HBASE_REST_OK)) {
			return(true);
		} else if (httpResponseCode == HBASE_REST_NOT_FOUND) {
			return(false);
		} else {
			string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			dbError.setTTL("HBase error while checking for the existence of a TTL based data item. " + errorMsg, DPS_DATA_ITEM_READ_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside hasTTL, it failed while checking for the existence of a TTL based data item. " <<
				errorMsg << ". " << DPS_DATA_ITEM_READ_ERROR, "HBaseDBLayer");
			return(false);
		}
  }

  void HBaseDBLayer::clear(uint64_t store, PersistenceError & dbError) {
     SPLAPPTRC(L_DEBUG, "Inside clear for store id " << store, "HBaseDBLayer");

 	ostringstream storeId;
 	storeId << store;
 	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside clear, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "HBaseDBLayer");
		}

		return;
	}

 	// Lock the store first.
 	if (acquireStoreLock(storeIdString) == false) {
 		// Unable to acquire the store lock.
 		dbError.set("Unable to get store lock for the StoreId " + storeIdString + ".", DPS_GET_STORE_LOCK_ERROR);
 		SPLAPPTRC(L_DEBUG, "Inside clear, it failed to get store lock for store id " << storeIdString << ". " << DPS_GET_STORE_LOCK_ERROR, "HBaseDBLayer");
 		// User has to retry again to remove the store.
 		return;
 	}

 	// Get the store name.
 	uint32_t dataItemCnt = 0;
 	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		releaseStoreLock(storeIdString);
		// This is alarming. This will put this store in a bad state. Poor user has to deal with it.
		return;
	}

 	// A very fast and quick thing to do is to simply delete the Store Contents Row's column family and
 	// recreate the meta data rows rather than removing one element at a time.
	// This action is performed on the Store Contents row that takes the following name.
	// Row key name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
 	string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
 	// Let us delete the entire column family for this store.
 	string url = getNextHBaseBaseUrl() + mainTableName + "/" + storeRowName + "/cf1";
	int32_t curlReturnCode = 0;
	string curlErrorString = "";
	uint64_t httpResponseCode = 0;
	string httpReasonString = "";
	string errorMsg = "";
	string jsonDoc = "";
 	bool hBaseResult = deleteHBase_Column_CF_Row(url, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

 	if (hBaseResult == false || httpResponseCode != HBASE_REST_OK) {
		// Problem in deleting the store contents row.
 		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Unable to delete the store contents row for store id " + storeIdString + ". " + errorMsg, DPS_STORE_CLEARING_ERROR);
		SPLAPPTRC(L_DEBUG, "Inside clear, it failed to delete the store contents row for store id " <<
			storeIdString << ". " << errorMsg << ". " << DPS_STORE_CLEARING_ERROR, "HBaseDBLayer");
		releaseStoreLock(storeIdString);
		return;
 	}

 	// Create a new store contents row column family for this store.
 	// This is the HBase wide row in which we will keep storing all the key value pairs belonging to this store.
	// Row key name: 'dps_' + '1_' + 'store id'
	// Every store contents row will always have these three metadata entries:
	// dps_name_of_this_store ==> 'store name'
	// dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	// dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	//
	// Every store contents wide row will have at least three entries in it carrying the actual store name, key spl type name and value spl type name.
	// In addition, inside this store contents row, we will house the
	// actual key:value data items that the user wants to keep in this store.
	// Such a store contents row is very useful for data item read, write, deletion, enumeration etc.
 	//

	// Let us populate the new store contents row with a mandatory element that will carry the name of this store.
	// (This mandatory entry will help us to do the reverse mapping from store id to store name.)
 	string metaData1Url = getNextHBaseBaseUrl() + mainTableName + "/RowData";
 	string base64_encoded_row_key, base64_encoded_column_key, base64_encoded_column_value;
	base64_encode(storeRowName, base64_encoded_row_key);
	base64_encode(string("cf1:") + string(HBASE_STORE_ID_TO_STORE_NAME_KEY), base64_encoded_column_key);
	base64_encode(storeName, base64_encoded_column_value);
	// Form the JSON payload as a literal string.
	jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
		base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";

	hBaseResult =  createOrUpdateHBaseColumn(metaData1Url, jsonDoc, curlReturnCode,
		curlErrorString, httpResponseCode, httpReasonString);

	if ((hBaseResult == false) || (httpResponseCode != HBASE_REST_OK)) {
		// There was an error in creating Meta Data 1;
		// This is not good at all. This will leave this store in a zombie state in HBase.
		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Critical error: Unable to create 'Meta Data1' in HBase for the store id " + storeIdString + ". " + errorMsg, DPS_STORE_HASH_METADATA1_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside createStore, it failed to create 'Meta Data1' in HBase for the store id " <<
			storeIdString << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA1_CREATION_ERROR, "HBaseDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

    // We are now going to save the SPL type names of the key and value as part of this
    // store's metadata. That will help us in the Java dps API "findStore" to cache the
    // key and spl type names inside the Java StoreImpl object by querying from the store's metadata.
	// Add the key spl type name metadata.
 	string metaData2Url = getNextHBaseBaseUrl() + mainTableName + "/RowData";
	// Form the JSON payload as a literal string.
 	string base64_encoded_keySplTypeName;
 	base64_encode(keySplTypeName, base64_encoded_keySplTypeName);
	base64_encode(storeRowName, base64_encoded_row_key);
	base64_encode(string("cf1:") + string(HBASE_SPL_TYPE_NAME_OF_KEY), base64_encoded_column_key);
	base64_encode(base64_encoded_keySplTypeName, base64_encoded_column_value);
 	// Form the JSON payload as a literal string.
	jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
		base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";

	hBaseResult =  createOrUpdateHBaseColumn(metaData2Url, jsonDoc, curlReturnCode,
		curlErrorString, httpResponseCode, httpReasonString);

	if ((hBaseResult == false) || (httpResponseCode != HBASE_REST_OK)) {
		// There was an error in creating Meta Data 2;
		// This is not good at all. This will leave this store in a zombie state in HBase.
		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Critical error: Unable to create 'Meta Data2' in HBase for the store id " + storeIdString + ". " + errorMsg, DPS_STORE_HASH_METADATA2_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside createStore, it failed to create 'Meta Data2' in HBase for the store id " <<
			storeIdString << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA2_CREATION_ERROR, "HBaseDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

	// Add the value spl type name metadata.
 	string metaData3Url = getNextHBaseBaseUrl() + mainTableName + "/RowData";
 	string base64_encoded_valueSplTypeName;
 	base64_encode(valueSplTypeName, base64_encoded_valueSplTypeName);
	base64_encode(storeRowName, base64_encoded_row_key);
	base64_encode(string("cf1:") + string(HBASE_SPL_TYPE_NAME_OF_VALUE), base64_encoded_column_key);
	base64_encode(base64_encoded_valueSplTypeName, base64_encoded_column_value);
	// Form the JSON payload as a literal string.
	jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
		base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";
	curlReturnCode = 0;
	curlErrorString = "";
	httpResponseCode = 0;
	httpReasonString = "";

	hBaseResult =  createOrUpdateHBaseColumn(metaData3Url, jsonDoc, curlReturnCode,
		curlErrorString, httpResponseCode, httpReasonString);

	if ((hBaseResult == false) || (httpResponseCode != HBASE_REST_OK)) {
		// There was an error in creating Meta Data 3;
		// This is not good at all. This will leave this store in a zombie state in HBase.
		errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		dbError.set("Critical error: Unable to create 'Meta Data3' in HBase for the store id " + storeIdString + ". " + errorMsg, DPS_STORE_HASH_METADATA3_CREATION_ERROR);
		SPLAPPTRC(L_DEBUG, "Critical error: Inside createStore, it failed to create 'Meta Data3' in HBase for the store id " <<
			storeIdString << ". Error=" << errorMsg << ". " << DPS_STORE_HASH_METADATA3_CREATION_ERROR, "HBaseDBLayer");
		releaseStoreLock(storeIdString);
		return;
	}

 	// If there was an error in the store contents hash recreation, then this store is going to be in a weird state.
	// It is a fatal error. If that happened, something terribly had gone wrong in clearing the contents.
	// User should look at the dbError code and decide about a corrective action.
 	releaseStoreLock(storeIdString);
  }
        
  uint64_t HBaseDBLayer::size(uint64_t store, PersistenceError & dbError)
  {
	SPLAPPTRC(L_DEBUG, "Inside size for store id " << store, "HBaseDBLayer");

	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside size, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside size, it failed for finding a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "HBaseDBLayer");
		}

		return(false);
	}

	// Store size information is maintained as part of the store information.
	uint32_t dataItemCnt = 0;
	string storeName = "";
 	string keySplTypeName = "";
 	string valueSplTypeName = "";

	if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		SPLAPPTRC(L_DEBUG, "Inside size, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		return(0);
	}

	return((uint64_t)dataItemCnt);
  }

  // We allow space characters in the data item keys.
  // Hence, it is required to based64 encode them before storing the
  // key:value data item in HBase.
  // (Use boost functions to do this.)
  //
  // IMPORTANT:
  // In HBase, every base64 encoded string may be used inside an URL or in a JSON formatted data.
  // We can't allow these two characters in the context of HBase because of URL format restrictions:
  // Forward slash and plus   i.e.  /  and +
  // Hence, we must replace them with ~ and - (tilde and hyphen).
  // Do the reverse of that in base64_decode.
  void HBaseDBLayer::base64_encode(std::string const & str, std::string & base64) {
	  // Insert line breaks for every 64KB characters.
	  typedef insert_linebreaks<base64_from_binary<transform_width<string::const_iterator,6,8> >, 64*1024 > it_base64_t;

	  unsigned int writePaddChars = (3-str.length()%3)%3;
	  base64 = string(it_base64_t(str.begin()),it_base64_t(str.end()));
	  base64.append(writePaddChars,'=');

	  // In HBase, it turns out we can have / and + characters in the column key names and column value names.
	  // When those fields are passed as part of the URL, we will URL encode them at that time in those functions.

	  // I commented it out on Dec/03/2014.
	  /*
	  // --- HBase specific change begins here ---
	  // To comply with the URL formatting rules, replace all the forward slash and plus characters if they are present.
	  if (base64.find_first_of("/") != string::npos) {
		  streams_boost::replace_all(base64, "/", "~");
	  }

	  if (base64.find_first_of("+") != string::npos) {
		  streams_boost::replace_all(base64, "+", "-");
	  }
	  // --- HBase specific change ends here ---
	  */
  }

  // As explained above, we based64 encoded the data item keys before adding them to the store.
  // If we need to get back the original key name, this function will help us in
  // decoding the base64 encoded key.
  // (Use boost functions to do this.)
  // Hence the following character substitutions are not needed in HBase.
  // IMPORTANT:
  // In HBase, we have to comply with the URL formatting rules.
  // Hence, we did some trickery in the base64_encode method above.
  // We will do the reverse of that action here. Please read the commentary in base64_encode method.
  void HBaseDBLayer::base64_decode(std::string & base64, std::string & result) {
	  // IMPORTANT:
	  // For performance reasons, we are not passing a const string to this method.
	  // Instead, we are passing a directly modifiable reference. Caller should be aware that
	  // the string they passed to this method gets altered during the base64 decoding logic below.
	  // After this method returns back to the caller, it is not advisable to use that modified string.
	  typedef transform_width< binary_from_base64<remove_whitespace<string::const_iterator> >, 8, 6 > it_binary_t;

	  // In HBase, it turns out we can have / and + characters in the column key names and column value names.
	  // When those fields are passed as part of the URL, we will URL encode them at that time in those functions.
	  // Hence the following character substitutions are not needed in HBase.
	  // I commented it out on Dec/03/2014.
	  /*
	  bool tildePresent = false;
	  bool hyphenPresent = false;

	  if (base64.find_first_of("~") != string::npos) {
		  tildePresent = true;
	  }

	  if (base64.find_first_of("-") != string::npos) {
		  hyphenPresent = true;
	  }

	  if (tildePresent == true || hyphenPresent == true) {
		  // --- HBase specific change begins here ---
		  // To comply with the URL formatting rules, we substituted / and + characters with ~ and - characters at the time of encoding.
		  // Let us do the reverse of that here if those characters are present.
		  if (tildePresent == true) {
			  streams_boost::replace_all(base64, "~", "/");
		  }

		  if (hyphenPresent == true) {
			  streams_boost::replace_all(base64, "-", "+");
		  }
		  // --- HBase specific change ends here ---
	  }
	  */

	  unsigned int paddChars = count(base64.begin(), base64.end(), '=');
	  std::replace(base64.begin(),base64.end(),'=','A'); // replace '=' by base64 encoding of '\0'
	  result = string(it_binary_t(base64.begin()), it_binary_t(base64.end())); // decode
	  result.erase(result.end()-paddChars,result.end());  // erase padding '\0' characters
  }

  // This method will check if a store exists for a given store id.
  bool HBaseDBLayer::storeIdExistsOrNot(string storeIdString, PersistenceError & dbError) {
	  string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
	  string key = string(HBASE_STORE_ID_TO_STORE_NAME_KEY);
	  string metaData1Url = getNextHBaseBaseUrl() + mainTableName + "/" + storeRowName + "/cf1:" + key;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  string value = "";
	  bool hBaseResult = readHBaseCellValue(metaData1Url, value, true,
			  curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (hBaseResult == true && httpResponseCode == HBASE_REST_OK) {
		  // This store already exists in our cache.
		  return(true);
	  } else {
		  // Unable to access a K/V entry in the store contents row for the given store id. This is not a correct behavior.
		  dbError.set("StoreIdExistsOrNot: Unable to get StoreContents meta data1 for the StoreId " + storeIdString +
		     ".", DPS_GET_STORE_CONTENTS_HASH_ERROR);
		  return(false);
	  }
  }

  // This method will acquire a lock for a given store.
  bool HBaseDBLayer::acquireStoreLock(string const & storeIdString) {
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

	  // HBase doesn't have a TTL (Time To Live) feature at the individual K/V pair level (like the other NoSQL databases).
	  // Hence, we have to manage it ourselves to release a stale lock that the previous lock owner forgot to unlock.
	  // HBase also doesn't allow "Data insertion only if it is not present already". (Like the "if not exists" feature in Cassandra.)
	  // It is going to be cumbersome. Let us deal with it.
	  //Try to get a lock for this generic entity.
	  while (1) {
		  // In the HBase, store lock information will be kept in the following row of the main table.
		  // Row_Key: "dps_lock_dl_lock"
		  // cf1:storeLockKey->LockIdStr
		  // cf2:storeLockKey->ThreadSignature
		  // cf3:storeLockKey->LockAcquisitionTime
		  //
		  // This is an atomic activity.
		  // If multiple threads attempt to do it at the same time, only one will succeed.
		  // Winner will hold the lock until they release it voluntarily or we forcefully take it
		  // back the forgotten stale locks.
		  // Create a HBase K/V pair entry for this generic lock in our main table.
		  // Before we do that, let us see if someone else already holds this lock.
		  // We will blindly read the cf3 value. If it is not there, then no one is holding this lock now.
		  // If it is there, someone is using this lock now. We will ensure that they have not forgotten to unlock it
		  // after their usage.
		  // URL format: baseURL/table-name/lockRowName/cf3:storeLockKey
		  string url = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf3:" + storeLockKey;
		  // Read from the HBase DPS main table to see if someone already has this lock.
		  string value = "";
		  int32_t curlReturnCode = 0;
		  string curlErrorString = "";
		  uint64_t httpResponseCode = 0;
		  string httpReasonString = "";
		  string jsonDoc = "";
		  readHBaseCellValue(url, value, true, curlReturnCode,
		     curlErrorString, httpResponseCode, httpReasonString);

		  // Let us store the current time to indicate when we grabbed the lock. If we don't release the
		  // lock before the allowed maximum time (DPS_AND_DL_GET_LOCK_TTL), anyone else can grab it from us after that time expires.
		  // (Other NoSQL DB alternatives will do this via TTL. In HBase, we have to do it ourselves. A weakness of HBase.)
		  SPL::timestamp tsNow = SPL::Functions::Time::getTimestamp();
		  int64_t timeInSecondsNow = SPL::Functions::Time::getSeconds(tsNow);
		  std::ostringstream timeValue;
		  timeValue << timeInSecondsNow;
		  bool greenFlagToCreateThisLock = true;

		  // If there is no entry for this lock in HBase, then we will create this lock.
		  if (httpResponseCode == HBASE_REST_OK) {
			  // It means, there is already an entry for this lock.
			  // Let us check if the previous owner of this lock left an old and stale lock without
			  // properly releasing it.
			  // Someone has this lock right now. Let us check if they are well within their allowed time to own the lock.
			  // If not, we can take over this lock. We can go ahead and read their lock acquisition time.
			  int64_t lockAcquiredTime = atoi(value.c_str());
			  if ((timeInSecondsNow - lockAcquiredTime) > DPS_AND_DL_GET_LOCK_TTL) {
				  // Previous owner exceeded the time of ownership.
				  // Overusing the lock or forgot to release the lock.
				  // Let us take over the lock by changing the time value on this field.
				  greenFlagToCreateThisLock = true;
			  } else {
				  // Current lock owner is still within the legal lock ownership interval.
				  // We have to wait until the lock is released.
				  greenFlagToCreateThisLock = false;
			  }
		  }

		  if (greenFlagToCreateThisLock == true) {
			  // There is no entry for this lock. Let us create the lock.
			  // We can go ahead and create our three column family data and store it in HBase.
			  // Row_Key: "dps_lock_dl_lock"
			  // cf1:storeLockKey->LockIdStr
			  // cf2:storeLockKey->ThreadSignature
			  // cf3:storeLockKey->LockAcquisitionTime
			  // JSON data will be in this format.
			  // {"Row":[{"key":"ZHBzX2xvY2tfZGxfbG9jaw==","Cell":[{"column":"Y2YxOjUwMTQ0NjQ2NDU2NDU2NDU2NTNnZW5lcmljX2xvY2s=",
			  //  "$":"NTU1MzQ1MzQ1MzQ1MzM="}, {"column":"Y2YyOjUwMTQ0NjQ2NDU2NDU2NDU2NTNnZW5lcmljX2xvY2s=",
			  //  "$":"NTY0NjM1NjQ1NjQ1NjQ1NA=="}, {"column":"Y2YzOjUwMTQ0NjQ2NDU2NDU2NDU2NTNnZW5lcmljX2xvY2s=",
			  //  "$":"MTU2NTc3NzY3Njc0NDQ2NA=="}]}]}
			  string base64_encoded_row_key, cf1_base64_encoded_column_key, cf2_base64_encoded_column_key,
			  	  cf3_base64_encoded_column_key, cf1_base64_encoded_column_value,
			  	  cf2_base64_encoded_column_value, cf3_base64_encoded_column_value;
			  base64_encode(lockRowName, base64_encoded_row_key);
			  base64_encode(string("cf1:") + storeLockKey, cf1_base64_encoded_column_key);
			  base64_encode(string("cf2:") + storeLockKey, cf2_base64_encoded_column_key);
			  base64_encode(string("cf3:") + storeLockKey, cf3_base64_encoded_column_key);
			  base64_encode(lockIdStr.str(), cf1_base64_encoded_column_value);
			  base64_encode(dbSigStr.str(), cf2_base64_encoded_column_value);
			  base64_encode(timeValue.str(), cf3_base64_encoded_column_value);

			  jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
				 cf1_base64_encoded_column_key + "\", \"$\": \"" + cf1_base64_encoded_column_value + "\"}, " +
			     "{\"column\": \"" + cf2_base64_encoded_column_key + "\", \"$\": \"" + cf2_base64_encoded_column_value +
			     "\"}, {\"column\": \"" + cf3_base64_encoded_column_key + "\", \"$\": \"" + cf3_base64_encoded_column_value +
			     "\"}]}]}";
			  // Let us create an entry in HBase for us to claim ownership of this lock.
			  // After the table name in the URL, you must give a row name and it can be any random string.
			  // What actually matters is the row key in the JSON doc above. That will override whatever you give in the URL below.
			  url = getNextHBaseBaseUrl() + mainTableName + "/RowData";
			  createOrUpdateHBaseColumn(url, jsonDoc, curlReturnCode,
			     curlErrorString, httpResponseCode, httpReasonString);

			  // If more than one thread attempted to do the same thing that we did above, there is no guarantee that
			  // we got the lock. We can be sure only if our db signature is stamped in the entry we created.
			  // Let us read it and confirm if that is the case.
			  url = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf2:" + storeLockKey;
			  readHBaseCellValue(url, value, true, curlReturnCode,
			     curlErrorString, httpResponseCode, httpReasonString);

			  if ((httpResponseCode == HBASE_REST_OK) && (value == dbSigStr.str())) {
				  // We got the lock.
				  return(true);
			  }
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

  void HBaseDBLayer::releaseStoreLock(string const & storeIdString) {
	  // '4' + 'store id' + 'dps_lock' => 1
	  std::string storeLockKey = DPS_STORE_LOCK_TYPE + storeIdString + DPS_LOCK_TOKEN;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  string url = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf1:" + storeLockKey;
	  deleteHBase_Column_CF_Row(url, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
	  url = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf2:" + storeLockKey;
	  deleteHBase_Column_CF_Row(url, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
	  url = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf3:" + storeLockKey;
	  deleteHBase_Column_CF_Row(url, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
  }

  bool HBaseDBLayer::readStoreInformation(std::string const & storeIdString, PersistenceError & dbError,
		  uint32_t & dataItemCnt, std::string & storeName, std::string & keySplTypeName, std::string & valueSplTypeName) {
	  // Read the store name, this store's key and value SPL type names,  and get the store size.
	  storeName = "";
	  keySplTypeName = "";
	  valueSplTypeName = "";
	  dataItemCnt = 0;
	  string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;

	  // This action is performed on the Store Contents row that takes the following name.
	  // 'dps_' + '1_' + 'store id'
	  // It will always have the following three metadata entries:
	  // dps_name_of_this_store ==> 'store name'
	  // dps_spl_type_name_of_key ==> 'spl type name for this store's key'
	  // dps_spl_type_name_of_value ==> 'spl type name for this store's value'
	  // 1) Get the store name.
	  string key = string(HBASE_STORE_ID_TO_STORE_NAME_KEY);
	  string metaData1Url = getNextHBaseBaseUrl() + mainTableName + "/" + storeRowName + "/cf1:" + key;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  string value = "";
	  string errorMsg = "";
	  bool hBaseResult = readHBaseCellValue(metaData1Url, value, true,
	     curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (hBaseResult == true && httpResponseCode == HBASE_REST_OK) {
		  storeName = value;
	  } else {
		  errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  // Unable to get the store name for this store's key.
		  dbError.set("Unable to get the store name for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_STORE_NAME_ERROR);
		  return (false);
	  }

	  // 2) Let us get the spl type name for this store's key.
	  key = string(HBASE_SPL_TYPE_NAME_OF_KEY);
	  string metaData2Url = getNextHBaseBaseUrl() + mainTableName + "/" + storeRowName + "/cf1:" + key;
	  curlReturnCode = 0;
	  curlErrorString = "";
	  httpResponseCode = 0;
	  httpReasonString = "";
	  value = "";
	  errorMsg = "";
	  hBaseResult = readHBaseCellValue(metaData2Url, value, true,
	     curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (hBaseResult == true && httpResponseCode == HBASE_REST_OK) {
		  keySplTypeName = value;
	  } else {
		  errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  // Unable to get the spl type name for this store's key.
		  dbError.set("Unable to get the key spl type name for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_KEY_SPL_TYPE_NAME_ERROR);
		  return (false);
	  }

	  // 3) Let us get the spl type name for this store's value.
	  key = string(HBASE_SPL_TYPE_NAME_OF_VALUE);
	  string metaData3Url = getNextHBaseBaseUrl() + mainTableName + "/" + storeRowName + "/cf1:" + key;
	  curlReturnCode = 0;
	  curlErrorString = "";
	  httpResponseCode = 0;
	  httpReasonString = "";
	  value = "";
	  errorMsg = "";
	  hBaseResult = readHBaseCellValue(metaData3Url, value, true,
	     curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (hBaseResult == true && httpResponseCode == HBASE_REST_OK) {
		  valueSplTypeName = value;
	  } else {
		  errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  // Unable to get the spl type name for this store's value.
		  dbError.set("Unable to get the value spl type name for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_VALUE_SPL_TYPE_NAME_ERROR);
		  return (false);
	  }

	  // 4) Let us get the size of the store contents row now.
	  string storeRowUrl = getNextHBaseBaseUrl() + mainTableName + "/" + storeRowName;
	  curlReturnCode = 0;
	  curlErrorString = "";
	  httpResponseCode = 0;
	  httpReasonString = "";
	  value = "";
	  errorMsg = "";
	  hBaseResult = getTotalNumberOfColumnsInHBaseTableRow(storeRowUrl, dataItemCnt,
	     curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (hBaseResult == false || httpResponseCode != HBASE_REST_OK || dataItemCnt == 0) {
		  errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  // This is not correct. We must have a minimum hash size of 3 because of the reserved elements.
		  dbError.set("Wrong value (zero) observed as the store size for StoreId " + storeIdString + ". " + errorMsg, DPS_GET_STORE_SIZE_ERROR);
		  return (false);
	  }

	  // Our Store Contents row for every store will have a mandatory reserved internal elements (store name, key spl type name, and value spl type name).
	  // Let us not count those three elements in the actual store contents size that the caller wants now.
	  dataItemCnt -= 3;
	  return(true);
  }

  string HBaseDBLayer::getStoreName(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "HBaseDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getStoreName, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		  return("");
	  }

	  string base64_decoded_storeName;
	  base64_decode(storeName, base64_decoded_storeName);
	  return(base64_decoded_storeName);
  }

  string HBaseDBLayer::getSplTypeNameForKey(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "HBaseDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForKey, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		  return("");
	  }

	  string base64_decoded_keySplTypeName;
	  base64_decode(keySplTypeName, base64_decoded_keySplTypeName);
	  return(base64_decoded_keySplTypeName);
  }

  string HBaseDBLayer::getSplTypeNameForValue(uint64_t store, PersistenceError & dbError) {
	std::ostringstream storeId;
	storeId << store;
	string storeIdString = storeId.str();

	// Ensure that a store exists for the given store id.
	if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		if (dbError.hasError() == true) {
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		} else {
			dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "HBaseDBLayer");
		}

		return("");
	}

	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside getSplTypeNameForValue, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		  return("");
	  }

	  string base64_decoded_valueSplTypeName;
	  base64_decode(valueSplTypeName, base64_decoded_valueSplTypeName);
	  return(base64_decoded_valueSplTypeName);
  }

  std::string HBaseDBLayer::getNoSqlDbProductName(void) {
	  return(string(HBASE_NO_SQL_DB_NAME));
  }

  void HBaseDBLayer::getDetailsAboutThisMachine(std::string & machineName, std::string & osVersion, std::string & cpuArchitecture) {
	  machineName = nameOfThisMachine;
	  osVersion = osVersionOfThisMachine;
	  cpuArchitecture = cpuTypeOfThisMachine;
  }

  bool HBaseDBLayer::runDataStoreCommand(std::string const & cmd, PersistenceError & dbError) {
		// This API can only be supported in NoSQL data stores such as Redis, Cassandra etc.
		// HBase doesn't have a way to do this.
		dbError.set("From HBase data store: This API to run native data store commands is not supported in HBase.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From HBase data store: This API to run native data store commands is not supported in HBase. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "HBaseDBLayer");
		return(false);
  }

  bool HBaseDBLayer::runDataStoreCommand(uint32_t const & cmdType, std::string const & httpVerb,
		std::string const & baseUrl, std::string const & apiEndpoint, std::string const & queryParams,
		std::string const & jsonRequest, std::string & jsonResponse, PersistenceError & dbError) {
	  	// If users want to execute arbitrary back-end data store two way
	  	// native commands, this API can be used. This is a variation of the previous API with
	  	// overloaded function arguments. As of Nov/2014, this API is supported in the dps toolkit only
	  	// when Cloudant or HBase NoSQL DB is used as a back-end data store. It covers any Cloudant or HBase
	  	// HTTP/JSON based native commands that can perform both database and document related Cloudant APIs or
	  	// the CRUD HBase APIs that are very well documented for reference on the web.
	    //
	  	// Validate the HTTP verb sent by the user.
	  	// We will support get, put, post, and delete (No support at this time for copy and attachments).
	  	if (httpVerb != string(HTTP_GET) && httpVerb != string(HTTP_PUT) &&
	  		httpVerb != string(HTTP_POST) && httpVerb != string(HTTP_DELETE) &&
	  		httpVerb != string(HTTP_HEAD)) {
	  		// User didn't give us a proper HTTP verb.
	  		string errorMsg = "HBase HTTP verb '" + httpVerb + "' is not supported.";
			dbError.set(errorMsg, DPS_RUN_DATA_STORE_COMMAND_ERROR);
			SPLAPPTRC(L_DEBUG, errorMsg << " " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "HBaseDBLayer");
			return(false);
	  	}

	    string url = baseUrl;

	    // Form the full URL using the user provided command line arguments.
	  	if (url.length() <= 0) {
	  		// User gave an empty Base URL. That is okay.
	  		// In that case, we will use the URL specified in the DPS configuration file.
	  		// Exclude the trailing forward slash at the end.
	  		url = getNextHBaseBaseUrl();
	  		url = url.substr(0, url.length()-1);
	  	}

	    // If the base URL ends with a forward slash, raise an error now.
		if (url.at(url.length()-1) == '/') {
			// User gave a base URL that ends with a forward slash.
			dbError.set("HBase base URL is not valid. It ends with a forward slash.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside runDataStoreCommand: HBase base URL is not valid. It ends with a forward slash.. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "HBaseDBLayer");
			return(false);
		}

		if (apiEndpoint.length() <= 0) {
			// User didn't give us a proper HBase endpoint.
			dbError.set("HBase API endpoint is not valid. It is empty.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside runDataStoreCommand: HBase API endpoint is not valid. It is empty. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "HBaseDBLayer");
			return(false);
		}

		// If the API endpoint doesn't begin with a forward slash, raise an error now.
		if (apiEndpoint.at(0) != '/') {
			// User gave an API endpoint that doesn't begin with a forward slash.
			dbError.set("HBase API endpoint path is not valid. It doesn't begin with a forward slash.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside runDataStoreCommand: HBase API endpoint path is not valid. It doesn't begin with a forward slash. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "HBaseDBLayer");
			return(false);
		}

		// If the API endpoint ends with a forward slash, raise an error now.
		// Only exception is for GET / and in that case apiEndPoint can simply be a /. That will get a list of table names.
		if ((apiEndpoint.at(apiEndpoint.length()-1) == '/') && ((apiEndpoint.length() != 1)) && (httpVerb != string(HTTP_GET))) {
			// User gave an API endpoint that ends with a forward slash.
			dbError.set("HBase API endpoint path is not valid. It ends with a forward slash.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside runDataStoreCommand: HBase API endpoint path is not valid. It ends with a forward slash. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "HBaseDBLayer");
			return(false);
		}

		// Append the HBase API end point.
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
			curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_WRITEFUNCTION, &HBaseDBLayer::writeFunction);

			if (httpVerb == string(HTTP_GET)) {
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_HTTPGET, 1);
			}

			if (httpVerb == string(HTTP_PUT)) {
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_PUT, 1);
				// Do the same here for CURLOPT_READDATA and CURLOPT_READFUNCTION as we did above for the CURL_WRITEDATA.
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_READDATA, this);
				curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_READFUNCTION, &HBaseDBLayer::readFunction);
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
				// curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_HEADERFUNCTION, &HBaseDBLayer::writeFunction);
			}

			// Enable the TCP keep alive so that we don't lose the connection with the
			// HBase server when no store functions are performed for long durations.
			curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_TCP_KEEPALIVE, 1);
			curl_easy_setopt(curlForRunDataStoreCommand, CURLOPT_HTTPHEADER, headersForRunDataStoreCommand);
		}

		// This must be done every time whether it is a repeating URL or a new URL.
		// Because, JSON document content may change across different invocations of this C++ method of ours.
		if (httpVerb == string(HTTP_PUT)) {
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
			SPLAPPTRC(L_DEBUG, curlErrorString <<". " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "HBaseDBLayer");
			return(false);
		}

		curl_easy_getinfo(curlForRunDataStoreCommand, CURLINFO_RESPONSE_CODE, &httpResponseCode);
		// We will not return a specific error message. User has to interpret the HTTP error returned by the HBase service.
		httpReasonString = "";
		dbError.set(httpReasonString, httpResponseCode);
		// We will simply return true from here just to indicate we sent the user's HBase HTTP request and
		// got a response from the HBase server. We are not going to parse the response from the server here.
		// That is the caller's responsibility. We will send the server response back to the caller via the
		// user provided return variable.
		jsonResponse = string(curlBuffer);
		return(true);
  }

  bool HBaseDBLayer::runDataStoreCommand(std::vector<std::string> const & cmdList, std::string & resultValue, PersistenceError & dbError) {
		// This API can only be supported in Redis.
		// HBase doesn't have a way to do this.
		dbError.set("From HBase data store: This API to run native data store commands is not supported in HBase.", DPS_RUN_DATA_STORE_COMMAND_ERROR);
		SPLAPPTRC(L_DEBUG, "From HBase data store: This API to run native data store commands is not supported in HBase. " << DPS_RUN_DATA_STORE_COMMAND_ERROR, "HBaseDBLayer");
		return(false);
  }

  // This method will get the data item from the store for a given key.
  // Caller of this method can also ask us just to find if a data item
  // exists in the store without the extra work of fetching and returning the data item value.
  bool HBaseDBLayer::getDataItemFromStore(std::string const & storeIdString,
		  std::string const & keyDataString, bool const & checkOnlyForDataItemExistence,
		  bool const & skipDataItemExistenceCheck, unsigned char * & valueData,
		  uint32_t & valueSize, PersistenceError & dbError) {
		// Let us get this data item from the cache as it is.
		// Since there could be multiple data writers, we are going to get whatever is there now.
		// It is always possible that the value for the requested item can change right after
		// you read it due to the data write made by some other thread. Such is life in a global distributed no-sql store.
		// This action is performed on the Store Contents row that takes the following name.
		// Row key name: 'dps_' + '1_' + 'store id'  [It will always have this entry: dps_name_of_this_store ==> 'store name']
		string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
		string _keyDataString = keyDataString;
		// If the column qualifier has any / or + characters, URL encode them now.
		streams_boost::replace_all(_keyDataString, "/", "%2F");
		streams_boost::replace_all(_keyDataString, "+", "%2B");
		streams_boost::replace_all(_keyDataString, "", "%20");
		string url = getNextHBaseBaseUrl() + mainTableName + "/" + storeRowName + "/cf1:" + _keyDataString;
		int32_t curlReturnCode = 0;
		string curlErrorString = "";
		uint64_t httpResponseCode = 0;
		string httpReasonString = "";
		string value = "";
		bool dataItemExists = true;

		// Read this K/V pair if it exists.
		bool hBaseResult = readHBaseCellValue(url, value, false,
			curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

		if ((hBaseResult == true)&& (httpResponseCode == HBASE_REST_OK)) {
			// K/V pair already exists.
			dataItemExists = true;
		} else if (httpResponseCode == HBASE_REST_NOT_FOUND) {
			// There is no existing K/V pair in the data store.
			dataItemExists = false;
		} else {
			// Some other error.
			string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			// Unable to get the requested data item from the cache.
			dbError.set("Unable to access the K/V pair in HBase with the StoreId " + storeIdString +
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
			// In HBase, when we put the K/V pair, we b64_encoded our binary value buffer and
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

  HBaseDBLayerIterator * HBaseDBLayer::newIterator(uint64_t store, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside newIterator for store id " << store, "HBaseDBLayer");

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  // Ensure that a store exists for the given store id.
	  if (storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "HBaseDBLayer");
		  }

		  return(NULL);
	  }

	  // Get the general information about this store.
	  uint32_t dataItemCnt = 0;
	  string storeName = "";
	  string keySplTypeName = "";
	  string valueSplTypeName = "";

	  if (readStoreInformation(storeIdString, dbError, dataItemCnt, storeName, keySplTypeName, valueSplTypeName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside newIterator, it failed for store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayer");
		  return(NULL);
	  }

	  // It is a valid store. Create a new iterator and return it to the caller.
	  HBaseDBLayerIterator *iter = new HBaseDBLayerIterator();
	  iter->store = store;
	  base64_decode(storeName, iter->storeName);
	  iter->hasData = true;
	  // Give this iterator access to our HBaseDBLayer object.
	  iter->hBaseDBLayerPtr = this;
	  iter->sizeOfDataItemKeysVector = 0;
	  iter->currentIndex = 0;
	  return(iter);
  }

  void HBaseDBLayer::deleteIterator(uint64_t store, Iterator * iter, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside deleteIterator for store id " << store, "HBaseDBLayer");

	  if (iter == NULL) {
		  return;
	  }

	  std::ostringstream storeId;
	  storeId << store;
	  string storeIdString = storeId.str();

	  HBaseDBLayerIterator *myIter = static_cast<HBaseDBLayerIterator *>(iter);

	  // Let us ensure that the user wants to delete an iterator that really belongs to the store passed to us.
	  // This will handle user's coding errors where a wrong combination of store id and iterator is passed to us for deletion.
	  if (myIter->store != store) {
		  // User sent us a wrong combination of a store and an iterator.
		  dbError.set("A wrong iterator has been sent for deletion. This iterator doesn't belong to the StoreId " +
		  				storeIdString + ".", DPS_STORE_ITERATION_DELETION_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside deleteIterator, it failed for store id " << storeIdString << ". " << DPS_STORE_ITERATION_DELETION_ERROR, "HBaseDBLayer");
		  return;
	  } else {
		  delete iter;
	  }
  }

  // This method will acquire a lock for any given generic/arbitrary identifier passed as a string..
  // This is typically used inside the createStore, createOrGetStore, createOrGetLock methods to
  // provide thread safety. There are other lock acquisition/release methods once someone has a valid store id or lock id.
  bool HBaseDBLayer::acquireGeneralPurposeLock(string const & entityName) {
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

	  // Like the other NoSQL databases (memcached, Redis, Cassandra etc.), HBase doesn't have TTL (Time To Live) features at
	  // the level of the individual K/V pairs. In addition, HBase doesn't provide a clean way to create a column if it doesn't exist.
	  // (Like Cassandra's "if not exists" feature)
	  // Hence, we have to manage it ourselves to release a stale lock that the previous lock owner forgot to unlock.
	  // Try to get a lock for this generic entity.
	  while (1) {
		// In the HBase, general purpose lock information will be kept in the following row of the main table.
		// Row_Key: "dps_lock_dl_lock"
		// cf1:genericLockKey->LockIdStr
		// cf2:genericLockKey->ThreadSignature
		// cf3:genericLockKey->LockAcquisitionTime
		//
		// This is an atomic activity.
		// If multiple threads attempt to do it at the same time, only one will succeed.
		// Winner will hold the lock until they release it voluntarily or we forcefully take it
		// back the forgotten stale locks.
		// Create a HBase K/V pair entry for this generic lock in our main table.
		// Before we do that, let us see if someone else already holds this lock.
		// We will blindly read the cf3 value. If it is not there, then no one is holding this lock now.
		// If it is there, someone is using this lock now. We will ensure that they have not forgotten to unlock it
		// after their usage.
		// URL format: baseURL/table-name/lockRowName/cf3:genericLockKey
		string url = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf3:" + genericLockKey;
		// Read from the HBase DPS main table to see if someone already has this lock.
		string value = "";
		int32_t curlReturnCode = 0;
		string curlErrorString = "";
		uint64_t httpResponseCode = 0;
		string httpReasonString = "";
		string jsonDoc = "";
		readHBaseCellValue(url, value, true, curlReturnCode,
			curlErrorString, httpResponseCode, httpReasonString);

		// Let us store the current time to indicate when we grabbed the lock. If we don't release the
		// lock before the allowed maximum time (DPS_AND_DL_GET_LOCK_TTL), anyone else can grab it from us after that time expires.
		// (Other NoSQL DB alternatives will do this via TTL. In HBase, we have to do it ourselves. A weakness of HBase.)
		SPL::timestamp tsNow = SPL::Functions::Time::getTimestamp();
		int64_t timeInSecondsNow = SPL::Functions::Time::getSeconds(tsNow);
		std::ostringstream timeValue;
		timeValue << timeInSecondsNow;
		bool greenFlagToCreateThisLock = true;

		// If there is no entry for this lock in HBase, then we will create this lock.
		if (httpResponseCode == HBASE_REST_OK) {
			// It means, there is already an entry for this lock.
			// Let us check if the previous owner of this lock left an old and stale lock without
			// properly releasing it.
			// Someone has this lock right now. Let us check if they are well within their allowed time to own the lock.
			// If not, we can take over this lock. We can go ahead and read their lock acquisition time.
			int64_t lockAcquiredTime = atoi(value.c_str());
			if ((timeInSecondsNow - lockAcquiredTime) > DPS_AND_DL_GET_LOCK_TTL) {
				// Previous owner exceeded the time of ownership.
				// Overusing the lock or forgot to release the lock.
				// Let us take over the lock by changing the time value on this field.
				greenFlagToCreateThisLock = true;
			} else {
				// Current lock owner is still within the legal lock ownership interval.
				// We have to wait until the lock is released.
				greenFlagToCreateThisLock = false;
			}
		}

		if (greenFlagToCreateThisLock == true) {
			// There is no entry for this lock. Let us create the lock.
			// We can go ahead and create our three column family data and store it in HBase.
			// Row_Key: "dps_lock_dl_lock"
			// cf1:genericLockKey->LockIdStr
			// cf2:genericLockKey->ThreadSignature
			// cf3:genericLockKey->LockAcquisitionTime
			// JSON data will be in this format.
			// {"Row":[{"key":"ZHBzX2xvY2tfZGxfbG9jaw==","Cell":[{"column":"Y2YxOjUwMTQ0NjQ2NDU2NDU2NDU2NTNnZW5lcmljX2xvY2s=",
			//  "$":"NTU1MzQ1MzQ1MzQ1MzM="}, {"column":"Y2YyOjUwMTQ0NjQ2NDU2NDU2NDU2NTNnZW5lcmljX2xvY2s=",
			//  "$":"NTY0NjM1NjQ1NjQ1NjQ1NA=="}, {"column":"Y2YzOjUwMTQ0NjQ2NDU2NDU2NDU2NTNnZW5lcmljX2xvY2s=",
			//  "$":"MTU2NTc3NzY3Njc0NDQ2NA=="}]}]}
			  string base64_encoded_row_key, cf1_base64_encoded_column_key, cf2_base64_encoded_column_key,
			  	  cf3_base64_encoded_column_key, cf1_base64_encoded_column_value,
			  	  cf2_base64_encoded_column_value, cf3_base64_encoded_column_value;
			  base64_encode(lockRowName, base64_encoded_row_key);
			  base64_encode(string("cf1:") + genericLockKey, cf1_base64_encoded_column_key);
			  base64_encode(string("cf2:") + genericLockKey, cf2_base64_encoded_column_key);
			  base64_encode(string("cf3:") + genericLockKey, cf3_base64_encoded_column_key);
			  base64_encode(lockIdStr.str(), cf1_base64_encoded_column_value);
			  base64_encode(dbSigStr.str(), cf2_base64_encoded_column_value);
			  base64_encode(timeValue.str(), cf3_base64_encoded_column_value);

			  jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
				 cf1_base64_encoded_column_key + "\", \"$\": \"" + cf1_base64_encoded_column_value + "\"}, " +
			     "{\"column\": \"" + cf2_base64_encoded_column_key + "\", \"$\": \"" + cf2_base64_encoded_column_value +
			     "\"}, {\"column\": \"" + cf3_base64_encoded_column_key + "\", \"$\": \"" + cf3_base64_encoded_column_value +
			     "\"}]}]}";
			// Let us create an entry in HBase for us to claim ownership of this lock.
			// After the table name in the URL, you must give a row name and it can be any random string.
			// What actually matters is the row key in the JSON doc above. That will override whatever you give in the URL below.
			url = getNextHBaseBaseUrl() + mainTableName + "/RowData";
			createOrUpdateHBaseColumn(url, jsonDoc, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

			// If more than one thread attempted to do the same thing that we did above, there is no guarantee that
			// we got the lock. We can be sure only if our db signature is stamped in the entry we created.
			// Let us read it and confirm if that is the case.
			url = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf2:" + genericLockKey;
			readHBaseCellValue(url, value, true, curlReturnCode,
				curlErrorString, httpResponseCode, httpReasonString);

			if ((httpResponseCode == HBASE_REST_OK) && (value == dbSigStr.str())) {
				// We got the lock.
				return(true);
			}
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

  void HBaseDBLayer::releaseGeneralPurposeLock(string const & entityName) {
	  // '501' + 'entity name' + 'generic_lock' => 1
	  std::string genericLockKey = GENERAL_PURPOSE_LOCK_TYPE + entityName + GENERIC_LOCK_TOKEN;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  string url = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf1:" + genericLockKey;
	  deleteHBase_Column_CF_Row(url, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
	  url = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf2:" + genericLockKey;
	  deleteHBase_Column_CF_Row(url, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
	  url = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf3:" + genericLockKey;
	  deleteHBase_Column_CF_Row(url, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
  }

    // Senthil added this on Apr/06/2022.
    // This method will get multiple keys from the given store and
    // populate them in the caller provided list (vector).
    // Be aware of the time it can take to fetch multiple keys in a store
    // that has several tens of thousands of keys. In such cases, the caller
    // has to maintain calm until we return back from here.
   void HBaseDBLayer::getKeys(uint64_t store, std::vector<unsigned char *> & keysBuffer, std::vector<uint32_t> & keysSize, int32_t keyStartPosition, int32_t numberOfKeysNeeded, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside getKeys for store id " << store, "HBaseDBLayer");

       // Not implemented at this time. Simply return.
       return;
    } // End of getKeys method.

    // Senthil added this on Apr/18/2022.
    // This method will get the value for a given key from the given store without
    // performing any checks for the existence of store, key etc. This is a slightly 
    // faster version of the get method above. 
    void HBaseDBLayer::getValue(std::string const & storeIdString, char const * & key, uint32_t const & keySize, unsigned char * & value, uint32_t & valueSize, uint64_t & error) {
       SPLAPPTRC(L_DEBUG, "Inside getValue for store id " << storeIdString, "HBaseDBLayer");

       // Not implemented at this time. Simply return.
       return;
    }

  HBaseDBLayerIterator::HBaseDBLayerIterator() {

  }

  HBaseDBLayerIterator::~HBaseDBLayerIterator() {

  }

  bool HBaseDBLayerIterator::getNext(uint64_t store, unsigned char * & keyData, uint32_t & keySize,
  	  		unsigned char * & valueData, uint32_t & valueSize, PersistenceError & dbError) {
	  SPLAPPTRC(L_DEBUG, "Inside getNext for store id " << store, "HBaseDBLayerIterator");

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
	  if (this->hBaseDBLayerPtr->storeIdExistsOrNot(storeIdString, dbError) == false) {
		  if (dbError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to check for the existence of store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayerIterator");
		  } else {
			  dbError.set("No store exists for the StoreId " + storeIdString + ".", DPS_INVALID_STORE_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to find a store with an id " << storeIdString << ". " << DPS_INVALID_STORE_ID_ERROR, "HBaseDBLayerIterator");
		  }

		  return(false);
	  }

	  // Ensure that this store is not empty at this time. (Size check on every iteration is a performance nightmare. Optimize it later.)
	  if (this->hBaseDBLayerPtr->size(store, dbError) <= 0) {
		  dbError.set("Store is empty for the StoreId " + storeIdString + ".", DPS_STORE_EMPTY_ERROR);
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed with an empty store whose id is " << storeIdString << ". " << DPS_STORE_EMPTY_ERROR, "HBaseDBLayerIterator");
		  return(false);
	  }

	  if (this->sizeOfDataItemKeysVector <= 0) {
		  // This is the first time we are coming inside getNext for store iteration.
		  // Let us get the available data item keys from this store.
		  this->dataItemKeys.clear();

		  string storeRowName = string(DPS_TOKEN) + "_" + string(DPS_STORE_INFO_TYPE) + "_" + storeIdString;
		  string url = this->hBaseDBLayerPtr->getNextHBaseBaseUrl() + this->hBaseDBLayerPtr->mainTableName + "/" + storeRowName;
		  int32_t curlReturnCode = 0;
		  string curlErrorString = "";
		  uint64_t httpResponseCode = 0;
		  string httpReasonString = "";
		  bool hBaseResult = this->hBaseDBLayerPtr->getAllColumnKeysInHBaseTableRow(url, this->dataItemKeys, curlReturnCode,
		     curlErrorString, httpResponseCode, httpReasonString);

		  if (hBaseResult == false) {
			  // Unable to get data item keys from the store.
			  string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			  dbError.set("Unable to get data item keys for the StoreId " + storeIdString +
					  ". " + errorMsg, DPS_GET_STORE_DATA_ITEM_KEYS_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to get data item keys for store id " << storeIdString <<
                 ". " << errorMsg << ". " << DPS_GET_STORE_DATA_ITEM_KEYS_ERROR, "HBaseDBLayerIterator");
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
	  // data_item_key was obtained straight from the store contents row, where it is
	  // already in the base64 encoded format.
	  bool result = this->hBaseDBLayerPtr->getDataItemFromStore(storeIdString, data_item_key,
		false, false, valueData, valueSize, dbError);

	  if (result == false) {
		  // Some error has occurred in reading the data item value.
		  SPLAPPTRC(L_DEBUG, "Inside getNext, it failed to get data item from store id " << storeIdString << ". " << dbError.getErrorCode(), "HBaseDBLayerIterator");
		  // We will disable any future action for this store using the current iterator.
		  this->hasData = false;
		  return(false);
	  }

	  // We are almost done once we take care of arranging to return the key name and key size.
	  // In order to support spaces in data item keys, we base64 encoded them before storing it in HBase.
	  // Let us base64 decode it now to get the original data item key.
	  string base64_decoded_data_item_key;
	  this->hBaseDBLayerPtr->base64_decode(data_item_key, base64_decoded_data_item_key);
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
		     storeIdString << ". " << DPS_STORE_ITERATION_MALLOC_ERROR, "HBaseDBLayerIterator");
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
  uint64_t HBaseDBLayer::createOrGetLock(std::string const & name, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock with a name " << name, "HBaseDBLayer");
		string base64_encoded_name;
		base64_encode(name, base64_encoded_name);

	 	// Get a general purpose lock so that only one thread can
		// enter inside of this method at any given time with the same lock name.
	 	if (acquireGeneralPurposeLock(base64_encoded_name) == false) {
	 		// Unable to acquire the general purpose lock.
	 		lkError.set("Unable to get a generic lock for creating a lock with its name as " + name + ".", DPS_GET_GENERIC_LOCK_ERROR);
	 		SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to get a generic lock while creating a store lock named " <<
	 			name << ". " << DPS_GET_GENERIC_LOCK_ERROR, "HBaseDBLayer");
	 		// User has to retry again to create this distributed lock.
	 		return (0);
	 	}

		// 1) In our HBase dps implementation, data item keys can have space characters.
		// Inside HBase, all our lock names will have a mapping type indicator of
		// "5" at the beginning followed by the actual lock name.
		// '5' + 'lock name' => lockId
		std::string lockNameKey = DL_LOCK_NAME_TYPE + base64_encoded_name;
		uint64_t lockId = SPL::Functions::Utility::hashCode(lockNameKey);
		ostringstream lockIdStr;
		lockIdStr << lockId;

		// URL format: baseURL/table-name/lockNameKey/cf1:lockNameKey
		string lockNameKeyValueUrl = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf1:" + lockNameKey;
		int32_t curlReturnCode = 0;
		string curlErrorString = "";
		uint64_t httpResponseCode = 0;
		string httpReasonString = "";
		string value = "";
		// Check if this entry already exists in the HBase main table.
		bool hBaseResult = readHBaseCellValue(lockNameKeyValueUrl, value, true, curlReturnCode,
			curlErrorString, httpResponseCode, httpReasonString);

		if ((httpResponseCode == HBASE_REST_OK) && (value == lockIdStr.str())) {
			// This lock already exists. We can simply return the lockId now.
			releaseGeneralPurposeLock(base64_encoded_name);
			return(lockId);
		}

		// If it doesn't exist, let us create this lock.
		string lockNameUrl = getNextHBaseBaseUrl() + mainTableName + "/RowData";
		string base64_encoded_row_key, base64_encoded_column_key, base64_encoded_column_value;
		base64_encode(lockRowName, base64_encoded_row_key);
		base64_encode(string("cf1:") + lockNameKey, base64_encoded_column_key);
		base64_encode(lockIdStr.str(), base64_encoded_column_value);

		// Form the JSON payload as a literal string. (Please note that in HBase, "put" command requires us to
		// base64 encode the column keys and column values. Otherwise, it will not work.)
		// HBase expects it to be in this format.
		// {"Row":[{"key":"ZHBzX2xvY2tfZGxfbG9jaw==","Cell":[{"column":"Y2YxOjUwMTQ0NjQ2NDU2NDU2NDU2NTNnZW5lcmljX2xvY2s=",
		//  "$":"NTU1MzQ1MzQ1MzQ1MzM="}]}]}
		string jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
			base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";
		// It will create a new column.
		hBaseResult =  createOrUpdateHBaseColumn(lockNameUrl, jsonDoc, curlReturnCode,
			curlErrorString, httpResponseCode, httpReasonString);
		string errorMsg = "";

		if ((hBaseResult == false) || (httpResponseCode != HBASE_REST_OK)) {
			// There was an error in creating the user defined lock.
			errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			lkError.set("DB put error. Unable to put the lockId for the lockName " + name + ". " + errorMsg, DL_GET_LOCK_ID_ERROR);
			SPLAPPTRC(L_DEBUG, "DB put error. Inside createOrGetLock, it failed to put the lockId for the lockName " <<
				name << ". Error = " << errorMsg << ". " << DL_GET_LOCK_ID_ERROR, "HBaseDBLayer");
			releaseGeneralPurposeLock(base64_encoded_name);
			return (0);
		}

		// If we are here that means the HBase column entry was created by the previous call.
		// We can go ahead and create the lock info entry now.
		// 2) Create the Lock Info
		//    '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdStr.str();  // LockId becomes the new key now.
		string lockInfoKeyUrl = getNextHBaseBaseUrl() + mainTableName + "/RowData";
		string lockInfoValue = string("0_0_0_") + base64_encoded_name;
		base64_encode(lockRowName, base64_encoded_row_key);
		base64_encode(string("cf1:") + lockInfoKey, base64_encoded_column_key);
		base64_encode(lockInfoValue, base64_encoded_column_value);
		// Form the JSON payload as a literal string.
		jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
			base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";
		// It will create a new column.
		hBaseResult =  createOrUpdateHBaseColumn(lockInfoKeyUrl, jsonDoc, curlReturnCode,
			curlErrorString, httpResponseCode, httpReasonString);

		if (hBaseResult == false || httpResponseCode != HBASE_REST_OK) {
			// There was an error in creating the user defined lock.
			errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			// Unable to create lockinfo details..
			// Problem in creating the "LockId:LockInfo" entry in the cache.
			lkError.set("Unable to create 'LockId:LockInfo' in the cache for a lock named " + name + ". " + errorMsg, DL_LOCK_INFO_CREATION_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside createOrGetLock, it failed to create 'LockId:LockInfo' for a lock named " <<
				name << ". Error=" << errorMsg << ". " << DL_LOCK_INFO_CREATION_ERROR, "HBaseDBLayer");
			// Delete the previous root entry we inserted.
			deleteHBase_Column_CF_Row(lockNameKeyValueUrl, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);
			releaseGeneralPurposeLock(base64_encoded_name);
			return(0);
		} else {
			releaseGeneralPurposeLock(base64_encoded_name);
			return(lockId);
		}
  }

  bool HBaseDBLayer::removeLock(uint64_t lock, PersistenceError & lkError) {
		SPLAPPTRC(L_DEBUG, "Inside removeLock for lock id " << lock, "HBaseDBLayer");

		ostringstream lockId;
		lockId << lock;
		string lockIdString = lockId.str();

		// If the lock doesn't exist, there is nothing to remove. Don't allow this caller inside this method.
		if(lockIdExistsOrNot(lockIdString, lkError) == false) {
			if (lkError.hasError() == true) {
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "HBaseDBLayer");
			} else {
				lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
				SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to find the lock with an id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "HBaseDBLayer");
			}

			return(false);
		}

		// Before removing the lock entirely, ensure that the lock is not currently being used by anyone else.
		if (acquireLock(lock, 25, 40, lkError) == false) {
			// Unable to acquire the distributed lock.
			lkError.set("Unable to get a distributed lock for the LockId " + lockIdString + ".", DL_GET_DISTRIBUTED_LOCK_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to get a distributed lock for the lock id " << lockIdString << ". " << DL_GET_DISTRIBUTED_LOCK_ERROR, "HBaseDBLayer");
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
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "HBaseDBLayer");
			releaseLock(lock, lkError);
			// This is alarming. This will put this lock in a bad state. Poor user has to deal with it.
			return(false);
		}

		// Since we got back the lock name for the given lock id, let us remove the lock entirely.
		// '5' + 'lock name' => lockId
		std::string lockNameKey = DL_LOCK_NAME_TYPE + lockName;
		string lockNameKeyUrl = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf1:" + lockNameKey;
		int32_t curlReturnCode = 0;
		string curlErrorString = "";
		uint64_t httpResponseCode = 0;
		string httpReasonString = "";

		bool hBaseResult = deleteHBase_Column_CF_Row(lockNameKeyUrl, curlReturnCode,
			curlErrorString, httpResponseCode, httpReasonString);

		if (hBaseResult == false) {
			// There was an error in deleting the user defined lock.
			string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			lkError.set("Unable to remove the lock named " + lockIdString + ".", DL_LOCK_REMOVAL_ERROR);
			SPLAPPTRC(L_DEBUG, "Inside removeLock, it failed to remove the lock with an id " << lockIdString << ". Error=" <<
				errorMsg << ". " << DL_LOCK_REMOVAL_ERROR, "HBaseDBLayer");
			releaseLock(lock, lkError);
			return(false);
		}

		// Now remove the lockInfo entry.
		// '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
		std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;  // LockId becomes the new key now.
		string lockInfoKeyUrl = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf1:" + lockInfoKey;
		hBaseResult = deleteHBase_Column_CF_Row(lockInfoKeyUrl, curlReturnCode,
			curlErrorString, httpResponseCode, httpReasonString);

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

  bool HBaseDBLayer::acquireLock(uint64_t lock, double leaseTime, double maxWaitTimeToAcquireLock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside acquireLock for lock id " << lock, "HBaseDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();
	  int32_t retryCnt = 0;

	  // If the lock doesn't exist, there is nothing to acquire. Don't allow this caller inside this method.
	  if(lockIdExistsOrNot(lockIdString, lkError) == false) {
		  if (lkError.hasError() == true) {
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to check for the existence of lock id " << lockIdString << ". " << lkError.getErrorCode(), "HBaseDBLayer");
		  } else {
			  lkError.set("No lock exists for the LockId " + lockIdString + ".", DL_INVALID_LOCK_ID_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to find a lock with an id " << lockIdString << ". " << DL_INVALID_LOCK_ID_ERROR, "HBaseDBLayer");
		  }

		  return(false);
	  }

	  // We will first check if we can get this lock.
	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  time_t startTime, timeNow;
	  // Get the start time for our lock acquisition attempts.
	  time(&startTime);

	  // Like the other NoSQL databases (memcached, Redis, Cassandra etc.), HBase doesn't have TTL (Time To Live) features at
	  // the level of the individual K/V pairs. In addition, HBase doesn't provide a clean way to create a column if it doesn't exist.
	  // (Like Cassandra's "if not exists" feature)
	  // Hence, we have to manage it ourselves to release a stale lock that the previous lock owner forgot to unlock.
	  //Try to get a distributed lock.
	  while(1) {
		  time_t new_lock_expiry_time = time(0) + (time_t)leaseTime;
		  // This is an atomic activity.
		  // If multiple threads attempt to do it at the same time, only one will succeed.
		  // Winner will hold the lock until they release it voluntarily or we forcefully take it
		  // back the forgotten stale locks.
		  //
		  // At first, we will blindly read the cf1 value. If it is not there, then no one is holding this lock now.
		  // If it is there, someone is using this lock now. We will ensure that they have not forgotten to unlock it
		  // after their usage.
		  // URL format: baseURL/table-name/lockRowName/cf1:distributedLockKey
		  string url = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf1:" + distributedLockKey;
		  // Read from the HBase DPS main table to see if someone already has this lock.
		  string value = "";
		  int32_t curlReturnCode = 0;
		  string curlErrorString = "";
		  uint64_t httpResponseCode = 0;
		  string httpReasonString = "";
		  string jsonDoc = "";
		  bool hBaseResult = readHBaseCellValue(url, value, true, curlReturnCode,
		     curlErrorString, httpResponseCode, httpReasonString);

		  // If there is no entry for this lock in HBase, then we can acquire this lock.
		  if (httpResponseCode == HBASE_REST_OK){
			  // This entry exists. That means, someone is holding on to this lock now.
			  // Let us check if the previous owner of this lock left an old and stale lock without
			  // properly releasing it.
			  // Someone has this lock right now. Let us check if they are well within their allowed time to own the lock.
			  // If not, we can take over this lock.
			  // Read the time at which this lock is expected to expire.
			  uint32_t _lockUsageCnt = 0;
			  int32_t _lockExpirationTime = 0;
			  std::string _lockName = "";
			  pid_t _lockOwningPid = 0;

			  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
				  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to read the previous lock owner details for lock id  " <<
				     lockIdString << ". " << lkError.getErrorCode(), "HBaseDBLayer");
			  } else {
				  // Is current time greater than the lock expiration time?
				  if ((_lockExpirationTime > 0) && (time(0) > (time_t)_lockExpirationTime)) {
					  // Time has passed beyond the lease of this lock.
					  // Lease expired for this lock. Original owner forgot to release the
					  // lock and simply left it hanging there without a valid lease.
					  releaseLock(lock, lkError);
				  }
			  }
		  } else if (httpResponseCode == HBASE_REST_NOT_FOUND){
			  // We couldn't find a pre-existing lock entry. We can own this lock now.
			  // (Other NoSQL DB alternatives will do this via TTL. In HBase, we have to do it ourselves. A weakness of HBase.)
			  // Form the JSON payload as a literal string.
			  url = getNextHBaseBaseUrl() + mainTableName + "/RowData";
			  string base64_encoded_row_key, base64_encoded_column_key, base64_encoded_column_value;
			  base64_encode(lockRowName, base64_encoded_row_key);
			  base64_encode(string("cf1:") + distributedLockKey, base64_encoded_column_key);
			  base64_encode(string("1"), base64_encoded_column_value);

			  // Form the JSON payload as a literal string.
			  jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
			     base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";
			  // It will create a new column.
			  hBaseResult =  createOrUpdateHBaseColumn(url, jsonDoc, curlReturnCode,
			     curlErrorString, httpResponseCode, httpReasonString);

			  if ((hBaseResult == true) && (httpResponseCode == HBASE_REST_OK)) {
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
		  } else if (hBaseResult == false) {
			  // Some other HBase HTTP API error occurred.
			  string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
			  lkError.set("Unable to acquire the lock named " + lockIdString + " due to this HTTP API error: " + errorMsg, DPS_HTTP_REST_API_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed for a lock named " << lockIdString << " due to this HTTP API error: " <<
			     errorMsg << ". " << DL_GET_LOCK_ERROR, "HBaseDBLayer");
			  return(false);
		  }

		  // Someone else is holding on to this distributed lock. Wait for a while before trying again.
		  retryCnt++;

		  if (retryCnt >= DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT) {
			  lkError.set("Unable to acquire the lock named " + lockIdString + " after maximum retries.", DL_GET_LOCK_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire a lock named " << lockIdString << " after maximum retries. " <<
			     DL_GET_LOCK_ERROR, "HBaseDBLayer");
			  // Our caller can check the error code and try to acquire the lock again.
			  return(false);
		  }

		  // Check if we have gone past the maximum wait time the caller was willing to wait in order to acquire this lock.
		  time(&timeNow);
		  if (difftime(startTime, timeNow) > maxWaitTimeToAcquireLock) {
			  lkError.set("Unable to acquire the lock named " + lockIdString + " within the caller specified wait time.", DL_GET_LOCK_TIMEOUT_ERROR);
			  SPLAPPTRC(L_DEBUG, "Inside acquireLock, it failed to acquire the lock named " << lockIdString <<
			     " within the caller specified wait time." << DL_GET_LOCK_TIMEOUT_ERROR, "HBaseDBLayer");
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

  void HBaseDBLayer::releaseLock(uint64_t lock, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside releaseLock for lock id " << lock, "HBaseDBLayer");

	  ostringstream lockId;
	  lockId << lock;
	  string lockIdString = lockId.str();

	  // '7' + 'lock id' + 'dl_lock' => 1
	  std::string distributedLockKey = DL_LOCK_TYPE + lockIdString + DL_LOCK_TOKEN;
	  string url = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf1:" + distributedLockKey;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  bool hBaseResult = deleteHBase_Column_CF_Row(url, curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (hBaseResult == false) {
		  string errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  lkError.set("Unable to release the distributed lock id " + lockIdString + ". " + errorMsg, DL_LOCK_RELEASE_ERROR);
		  return;
	  }

	  updateLockInformation(lockIdString, lkError, 0, 0, 0);
  }

  bool HBaseDBLayer::updateLockInformation(std::string const & lockIdString,
	PersistenceError & lkError, uint32_t const & lockUsageCnt, int32_t const & lockExpirationTime, pid_t const & lockOwningPid) {
	  // Get the lock name for this lock.
	  uint32_t _lockUsageCnt = 0;
	  int32_t _lockExpirationTime = 0;
	  std::string _lockName = "";
	  pid_t _lockOwningPid = 0;

	  if (readLockInformation(lockIdString, lkError, _lockUsageCnt, _lockExpirationTime, _lockOwningPid, _lockName) == false) {
		  SPLAPPTRC(L_DEBUG, "Inside updateLockInformation, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "HBaseDBLayer");
		  return(false);
	  }

	  // Let us update the lock information.
	  // '6' + 'lock id' ==> 'lock use count' + '_' + 'lock expiration time expressed as elapsed seconds since the epoch' + '_' + 'pid that owns this lock' + "_" + lock name'
	  std::string lockInfoKey = DL_LOCK_INFO_TYPE + lockIdString;
	  ostringstream lockInfoValue;
	  lockInfoValue << lockUsageCnt << "_" << lockExpirationTime << "_" << lockOwningPid << "_" << _lockName;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  string value = "";
	  string jsonDoc = "";
	  string errorMsg = "";
	  string lockInfoKeyUrl = getNextHBaseBaseUrl() + mainTableName + "/RowData";

	  string base64_encoded_row_key, base64_encoded_column_key, base64_encoded_column_value;
	  base64_encode(lockRowName, base64_encoded_row_key);
	  base64_encode(string("cf1:") + lockInfoKey, base64_encoded_column_key);
	  base64_encode(lockInfoValue.str(), base64_encoded_column_value);
	  // Form the JSON payload as a literal string.
	  jsonDoc = "{\"Row\": [{\"key\": \"" + base64_encoded_row_key + "\", \"Cell\": [{\"column\": \"" +
	     base64_encoded_column_key + "\", \"$\": \"" + base64_encoded_column_value + "\"}]}]}";

	  bool hBaseResult =  createOrUpdateHBaseColumn(lockInfoKeyUrl, jsonDoc, curlReturnCode,
         curlErrorString, httpResponseCode, httpReasonString);

	  if ((hBaseResult == false) || (httpResponseCode != HBASE_REST_OK)) {
		  // There was an error in updating the lock information.
		  errorMsg = "[cURL error:" + curlErrorString + ", httpReasonString:" + httpReasonString + "]";
		  lkError.set("Critical Error1: Unable to update 'LockId:LockInfo' in the cache for a lock named " + _lockName + ". " + errorMsg, DL_LOCK_INFO_UPDATE_ERROR);
		  SPLAPPTRC(L_DEBUG, "Critical Error1: Inside updateLockInformation, it failed for a lock named " << _lockName << ". " <<
			errorMsg << ". " << DL_LOCK_INFO_UPDATE_ERROR, "HBaseDBLayer");
		  return(false);
	  }

	  return(true);
  }

  bool HBaseDBLayer::readLockInformation(std::string const & lockIdString, PersistenceError & lkError, uint32_t & lockUsageCnt,
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
	  string value = "";
	  string lockInfoKeyUrl = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf1:" + lockInfoKey;

	  bool hBaseResult = readHBaseCellValue(lockInfoKeyUrl, value, true,
			  curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (hBaseResult == true && httpResponseCode == HBASE_REST_OK) {
		  // We got the value from the HBase K/V store.
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
  bool HBaseDBLayer::lockIdExistsOrNot(string lockIdString, PersistenceError & lkError) {
	  string keyString = string(DL_LOCK_INFO_TYPE) + lockIdString;
	  int32_t curlReturnCode = 0;
	  string curlErrorString = "";
	  uint64_t httpResponseCode = 0;
	  string httpReasonString = "";
	  string value = "";
	  string lockInfoKeyUrl = getNextHBaseBaseUrl() + mainTableName + "/" + lockRowName + "/cf1:" + keyString;

	  bool hBaseResult = readHBaseCellValue(lockInfoKeyUrl, value, true,
	     curlReturnCode, curlErrorString, httpResponseCode, httpReasonString);

	  if (hBaseResult == true && httpResponseCode == HBASE_REST_OK) {
		  // LockId exists.
		  return(true);
	  } else if (hBaseResult == true && httpResponseCode == HBASE_REST_NOT_FOUND) {
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
  uint32_t HBaseDBLayer::getPidForLock(string const & name, PersistenceError & lkError) {
	  SPLAPPTRC(L_DEBUG, "Inside getPidForLock with a name " << name, "HBaseDBLayer");

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
		  SPLAPPTRC(L_DEBUG, "Inside getPidForLock, it failed for lock id " << lockIdString << ". " << lkError.getErrorCode(), "HBaseDBLayer");
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
  inline bool HBaseDBLayer::is_b64(unsigned char c) {
    return (isalnum(c) || (c == '+') || (c == '/'));
  }

  // Base64 encode the binary buffer contents passed by the caller and return a string representation.
  // There is no change to the original code in this method other than a slight change in the method name,
  // returning back right away when encountering an empty buffer, replacing / and + characters with ~ and -
  // and an array initialization for char_array_4 to avoid a compiler warning.
  //
  // IMPORTANT:
  // In HBase, every b64 encoded string may be used inside an URL or in a JSON formatted data.
  // We can't allow these two characters in the context of HBase because of URL format restrictions:
  // Forward slash and plus   i.e.  /  and +
  // Hence, we must replace them with ~ and - (tilde and hyphen).
  // Do the reverse of that in b64_decode.
  void HBaseDBLayer::b64_encode(unsigned char const * & buf, uint32_t const & bufLenOrg, string & ret) {
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

    // Since this function is used only to convert the value part of the K/V pair which
    // is in a binary buffer, we don't need the following character substitutions.
    // I commented it out on Dec/03/2014.
    /*
	// --- HBase specific change begins here ---
	// To comply with the URL formatting rules, replace all the forward slash and plus characters if they are present.
    if (ret.find_first_of("/") != string::npos) {
    	streams_boost::replace_all(ret, "/", "~");
    }

    if (ret.find_first_of("+") != string::npos) {
    	streams_boost::replace_all(ret, "+", "-");
    }

	// --- HBase specific change ends here ---
    */

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
  // In HBase, we have to comply with the URL formatting rules.
  // Hence, we did some trickery in the b64_encode method above.
  // We will do the reverse of that action here. Please read the commentary in b64_encode method.
  void HBaseDBLayer::b64_decode(std::string & encoded_string, unsigned char * & buf, uint32_t & bufLen) {
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

    // Since this function is used only to convert the value part of the K/V pair which
    // is in a binary buffer, we don't need the following character substitutions.
    // I commented it out on Dec/03/2014.
    /*
	bool tildePresent = false;
	bool hyphenPresent = false;

	if (encoded_string.find_first_of("~") != string::npos) {
		tildePresent = true;
	}

	if (encoded_string.find_first_of("-") != string::npos) {
		hyphenPresent = true;
	}

	if (tildePresent == true || hyphenPresent == true) {
		// --- HBase specific change begins here ---
	    // To comply with the URL formatting rules, we substituted / and + characters with ~ and - characters at the time of encoding.
	    // Let us do the reverse of that here if those characters are present.
	    // A memory copy here so as not to modify the caller's passed data. (Can it be optimized later?)
	    if (tildePresent == true) {
	    	streams_boost::replace_all(encoded_string, "~", "/");
	    }

	    if (hyphenPresent == true) {
	    	streams_boost::replace_all(encoded_string, "-", "+");
	    }
	    // --- HBase specific change ends here ---
	}
	*/

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

  // This method will get the next baseURL from a pool of REST server addresses.
  inline string HBaseDBLayer::getNextHBaseBaseUrl() {
	  if (currentHBaseUrlIdx >= 50 || hBaseBaseUrlPool[currentHBaseUrlIdx] == "") {
		  // We reached the last slot in our array.
		  // (OR)
		  // There is no valid REST server address at this array index.
		  // Go back to the very first server in the address pool.
		  currentHBaseUrlIdx = 0;
	  }

	  return(hBaseBaseUrlPool[currentHBaseUrlIdx++]);
  }

  // HBase specific cURL write and read functions.
  // Since we are using C++ and the cURL library is C based, we have to set the callback as a
  // static C++ method and configure cURL to pass our custom C++ object pointer in the 4th argument of
  // the callback function. Once we get the C++ object pointer, we can access our other non-static
  // member functions and non-static member variables via that object pointer.
  //
  // The 4th argument will be pointing to a non-static method below named writeFunctionImpl.
  size_t HBaseDBLayer::writeFunction(char *data, size_t size, size_t nmemb, void *objPtr) {
	return(static_cast<HBaseDBLayer*>(objPtr)->writeFunctionImpl(data, size, nmemb));
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  size_t HBaseDBLayer::writeFunctionImpl(char *data, size_t size, size_t nmemb) {
	  memcpy(&(curlBuffer[curlBufferOffset]), data, size * nmemb);
	  curlBufferOffset += size * nmemb;
	  return (size * nmemb);
  }

  // Do the same for the read function.
  // The 4th argument will be pointing to a non-static method below named readFunctionImpl.
  size_t HBaseDBLayer::readFunction(char *data, size_t size, size_t nmemb, void *objPtr) {
	  return(static_cast<HBaseDBLayer*>(objPtr)->readFunctionImpl(data, size, nmemb));
  }

  // This method will always get called within the static method defined just above via its object pointer method argument.
  size_t HBaseDBLayer::readFunctionImpl(char *data, size_t size, size_t nmemb) {
	  int len = strlen(putBuffer);
	  memcpy(data, putBuffer, len);
	  return len;
  }

  //===================================================================================
  // Following methods are specific to the HBase REST APIs that perform all
  // the CRUD operations on our stores and locks.
  //===================================================================================
  // This method will create a HBase table.
  // URL format should be: http://user:password@HBase-REST-ServerNameOrIPAddress:port/tableName/schema
  bool HBaseDBLayer::createHBaseTable(string const & url, string const & schema, int32_t & curlReturnCode,
	  string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString) {
	  curlReturnCode = 0;
	  httpResponseCode = 0;
	  curlErrorString = "";
	  httpReasonString = "";
	  char *curlEffectiveUrl = NULL;
	  CURLcode result;
	  bool repeatingUrl = false;
      putBuffer = schema.c_str();
	  long putBufferLen = schema.length();

	  // Get the URL endpoint that was used when we executed this method previously.
	  result = curl_easy_getinfo(curlForCreateHBaseTable, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForCreateHBaseTable);
		  curl_easy_setopt(curlForCreateHBaseTable, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static method pointer in the CURLOPT_WRITEFUNCTION and then pass a custom
		  // object reference (this) of our current C++ class in the CURLOPT_WRITEDATA so that cURL will pass that
		  // object pointer as the 4th argument to our static writeFunction during its callback. By passing that object
		  // pointer as an argument to a static method, we can let the static method access our
		  // non-static member functions and non-static member variables via that object pointer.
		  curl_easy_setopt(curlForCreateHBaseTable, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForCreateHBaseTable, CURLOPT_WRITEFUNCTION, &HBaseDBLayer::writeFunction);
		  // Do the same here for CURLOPT_READDATA and CURLOPT_READFUNCTION as we did above.
		  curl_easy_setopt(curlForCreateHBaseTable, CURLOPT_READDATA, this);
		  curl_easy_setopt(curlForCreateHBaseTable, CURLOPT_READFUNCTION, &HBaseDBLayer::readFunction);
		  curl_easy_setopt(curlForCreateHBaseTable, CURLOPT_PUT, 1);
		  // Put content length i.e. CURLOPT_INFILESIZE must be set outside of this repeat URL check if block.
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // HBase server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForCreateHBaseTable, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForCreateHBaseTable, CURLOPT_HTTPHEADER, headersForCreateHBaseTable);
	  }

	  // This must be done every time whether it is a repeating URL or a new URL.
	  // Because, JSON Schema may change across different invocations of this C++ method of ours.
	  curl_easy_setopt(curlForCreateHBaseTable, CURLOPT_INFILESIZE, putBufferLen);
	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForCreateHBaseTable);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForCreateHBaseTable, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool hBaseResult = true;

	  // HTTP response codes: 201-->Table creation successful
	  if ((httpResponseCode != HBASE_REST_OK) && (httpResponseCode != HBASE_TABLE_CREATION_OK)) {
		  hBaseResult = false;
		  ostringstream hrc;
		  hrc << httpResponseCode;
		  httpReasonString = "hrc-->" + hrc.str();
	  }

	  return(hBaseResult);
  }

  // This method will delete a HBase table.
  // URL format should be: http://user:password@HBase-REST-ServerNameOrIPAddress:port/tableName/schema
  bool HBaseDBLayer::deleteHBaseTable(string const & url, int32_t & curlReturnCode,
	  string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString) {
	  curlReturnCode = 0;
	  httpResponseCode = 0;
	  curlErrorString = "";
	  httpReasonString = "";
	  char *curlEffectiveUrl = NULL;
	  CURLcode result;
	  bool repeatingUrl = false;

	  // Get the URL endpoint that was used when we executed this method previously.
	  result = curl_easy_getinfo(curlForDeleteHBaseTable, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForDeleteHBaseTable);
		  curl_easy_setopt(curlForDeleteHBaseTable, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
		  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
		  curl_easy_setopt(curlForDeleteHBaseTable, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForDeleteHBaseTable, CURLOPT_WRITEFUNCTION, &HBaseDBLayer::writeFunction);
		  curl_easy_setopt(curlForDeleteHBaseTable, CURLOPT_CUSTOMREQUEST, HTTP_DELETE);
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // HBase server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForDeleteHBaseTable, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForDeleteHBaseTable, CURLOPT_HTTPHEADER, headersForDeleteHBaseTable);
	  }

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForDeleteHBaseTable);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForDeleteHBaseTable, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool hBaseResult = true;

	  // HTTP response codes: 200-->OK on success, 404--> Not found.
	  if ((httpResponseCode != HBASE_REST_OK) && (httpResponseCode != HBASE_REST_NOT_FOUND)) {
		  hBaseResult = false;
		  ostringstream hrc;
		  hrc << httpResponseCode;
		  httpReasonString = "hrc-->" + hrc.str();
	  }

	  return(hBaseResult);
  }

  // This method will create a HBase column or update an existing column.
  // URL format should be: http://user:password@HBase-REST-ServerNameOrIPAddress:port/TableName/RowData
  // After the table name in the URL above, you must give a row name and it can be any random string.
  // What actually matters is the row key in the JSON doc passed by the user. That will override whatever is specified in the URL.
  bool HBaseDBLayer::createOrUpdateHBaseColumn(string const & url, string const & jsonDoc, int32_t & curlReturnCode,
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
	  result = curl_easy_getinfo(curlForCreateOrUpdateHBaseColumn, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForCreateOrUpdateHBaseColumn);
		  curl_easy_setopt(curlForCreateOrUpdateHBaseColumn, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static method pointer in the CURLOPT_WRITEFUNCTION and then pass a custom
		  // object reference (this) of our current C++ class in the CURLOPT_WRITEDATA so that cURL will pass that
		  // object pointer as the 4th argument to our static writeFunction during its callback. By passing that object
		  // pointer as an argument to a static method, we can let the static method access our
		  // non-static member functions and non-static member variables via that object pointer.
		  curl_easy_setopt(curlForCreateOrUpdateHBaseColumn, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForCreateOrUpdateHBaseColumn, CURLOPT_WRITEFUNCTION, &HBaseDBLayer::writeFunction);
		  // Do the same here for CURLOPT_READDATA and CURLOPT_READFUNCTION as we did above.
		  curl_easy_setopt(curlForCreateOrUpdateHBaseColumn, CURLOPT_READDATA, this);
		  curl_easy_setopt(curlForCreateOrUpdateHBaseColumn, CURLOPT_READFUNCTION, &HBaseDBLayer::readFunction);
		  curl_easy_setopt(curlForCreateOrUpdateHBaseColumn, CURLOPT_PUT, 1);
		  // Put content length i.e. CURLOPT_INFILESIZE must be set outside of this repeat URL check if block.
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // HBase server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForCreateOrUpdateHBaseColumn, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForCreateOrUpdateHBaseColumn, CURLOPT_HTTPHEADER, headersForCreateOrUpdateHBaseColumn);
	  }

	  // This must be done every time whether it is a repeating URL or a new URL.
	  // Because, JSON document content may change across different invocations of this C++ method of ours.
	  curl_easy_setopt(curlForCreateOrUpdateHBaseColumn, CURLOPT_INFILESIZE, putBufferLen);
	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForCreateOrUpdateHBaseColumn);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForCreateOrUpdateHBaseColumn, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool hBaseResult = true;

	  // HTTP response codes: 200-->OK on success, 404--> Not found.
	  if ((httpResponseCode != HBASE_REST_OK) && (httpResponseCode != HBASE_REST_NOT_FOUND)) {
		  hBaseResult = false;
		  ostringstream hrc;
		  hrc << httpResponseCode;
		  httpReasonString = "hrc-->" + hrc.str();
	  }

	  return(hBaseResult);
  }

  // This method will read the value of a HBase cell specified by the user.
  // A combination of row key, column family, and column qualifier uniquely
  // identifies a cell. The data stored in a cell is referred to as that cell's value.
  // If that cell is available, this method will return the value of the specified cell.
  // In HBase, all the DPS column key and column values are base64 encoded. While returning
  // the cell value, this method will base64 decode that value ONLY if the caller passed
  // a boolean true for the third method argument.
  // URL format should be: http://user:password@HBase-REST-ServerNameOrIPAddress:port/TableName/RowKey/CF:CQ
  bool HBaseDBLayer::readHBaseCellValue(string const & url, string & value,
	bool const & base64DecodeTheResult, int32_t & curlReturnCode, string & curlErrorString,
  	uint64_t & httpResponseCode, string & httpReasonString) {
	  curlReturnCode = 0;
	  httpResponseCode = 0;
	  curlErrorString = "";
	  httpReasonString = "";
	  value = "";
	  char *curlEffectiveUrl = NULL;
	  CURLcode result;
	  bool repeatingUrl = false;

	  // Get the URL endpoint that was used when we executed this method previously.
	  result = curl_easy_getinfo(curlForReadHBaseCellValue, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForReadHBaseCellValue);
		  curl_easy_setopt(curlForReadHBaseCellValue, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
		  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
		  curl_easy_setopt(curlForReadHBaseCellValue, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForReadHBaseCellValue, CURLOPT_WRITEFUNCTION, &HBaseDBLayer::writeFunction);
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // HBase server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForReadHBaseCellValue, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForReadHBaseCellValue, CURLOPT_HTTPHEADER, headersForReadHBaseCellValue);
	  }

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForReadHBaseCellValue);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForReadHBaseCellValue, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool hBaseResult = true;

	  // HTTP response codes: 200-->Cell value retrieval successful, 404-->Cell can't be found
	  // If we have the cell value read successfully from the HBase server, let us parse the field that the caller asked for.
	  if (httpResponseCode == HBASE_REST_OK) {
		  // In all the DPS store related cell values, we will not have a timestamp or version number.
		  // Instead, we will have the default map key as "$".
		  // A successful reading of a cell will result like this.
		  // "{\"Row\":[{\"key\":\"ZHBzX2xvY2tfZGxfbG9jaw==\",\"Cell\":[{\"column\":\"Y2YzOjUwMTQ0NjQ2NDU2NDU2NDU2NTNnZW5lcmljX2xvY2s=\",
		  //   \"timestamp\":1417138352923,\"$\":\"MTU2NTc3NzY3Njc0NDQ2NA==\"}]}]}
		  // Our goal is to read the value for the "$" map key.
		  const char *cellValue = NULL;
		  char key[256];
		  strcpy(key, "Row");
		  json_object *jo = json_tokener_parse(curlBuffer);
		  json_object *joForRestResult = NULL;
		  // Get the Row object array.
		  json_bool exists = json_object_object_get_ex(jo, key, &joForRestResult);

		  if (exists) {
			  // Get the first array element.
			  json_object *joForRow = json_object_array_get_idx(joForRestResult, 0);
			  // Get the Cell object array within that row object.
			  json_object *joForCell = NULL;
			  strcpy(key, "Cell");
			  exists = json_object_object_get_ex(joForRow, key, &joForCell);

			  if (exists) {
				  // Get the first array element from the cell.
				  json_object *joForField = json_object_array_get_idx(joForCell, 0);
				  // Get the field value.
				  strcpy(key, "$");
				  json_object *joForFieldValue = NULL;
				  exists = json_object_object_get_ex(joForField, key, &joForFieldValue);

				  if (exists) {
					  cellValue = json_object_get_string(joForFieldValue);
				  }
			  }
		  }

		  if (cellValue != NULL) {
			  // Assign it to the caller passed method argument.
			  // Cell value will be in base64 encoded format.
			  // If user asked for a base64 decoded result, let us do it now.
			  if (base64DecodeTheResult == true) {
				  string tmpString(cellValue);
				  base64_decode(tmpString, value);
			  } else {
				  value = string(cellValue);
			  }
		  } else {
			  // Unable to get the cell value.
			  hBaseResult = false;
			  httpResponseCode = HBASE_CELL_VALUE_NOT_FOUND;
			  ostringstream hrc;
			  hrc << httpResponseCode;
			  httpReasonString = "rc=" + hrc.str() + ", msg=HBase cell value can't be found.";
		  }

		  // Release it now.
		  json_object_put(jo);
	  }

	  // HTTP response codes: 200-->OK on success, 404--> Not found.
	  if ((httpResponseCode != HBASE_REST_OK) && (httpResponseCode != HBASE_REST_NOT_FOUND)) {
		  hBaseResult = false;
		  ostringstream hrc;
		  hrc << httpResponseCode;
		  httpReasonString = "hrc-->" + hrc.str();
	  }

	  return(hBaseResult);
  }

  // This method will delete a HBase column or a column family or a row..
  // URL format should be:
  // http://user:password@HBase-REST-ServerNameOrIPAddress:port/TableName/RowKey/CF:CQ
  // http://user:password@HBase-REST-ServerNameOrIPAddress:port/TableName/RowKey/CF
  // http://user:password@HBase-REST-ServerNameOrIPAddress:port/TableName/RowKey
  bool HBaseDBLayer::deleteHBase_Column_CF_Row(string const & url, int32_t & curlReturnCode, string & curlErrorString,
	uint64_t & httpResponseCode, string & httpReasonString) {
	  curlReturnCode = 0;
	  httpResponseCode = 0;
	  curlErrorString = "";
	  httpReasonString = "";

	  char *curlEffectiveUrl = NULL;
	  CURLcode result;
	  bool repeatingUrl = false;

	  // Get the URL endpoint that was used when we executed this method previously.
	  result = curl_easy_getinfo(curlForDeleteHBase_Column_CF_Row, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForDeleteHBase_Column_CF_Row);
		  curl_easy_setopt(curlForDeleteHBase_Column_CF_Row, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
		  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
		  curl_easy_setopt(curlForDeleteHBase_Column_CF_Row, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForDeleteHBase_Column_CF_Row, CURLOPT_WRITEFUNCTION, &HBaseDBLayer::writeFunction);
		  curl_easy_setopt(curlForDeleteHBase_Column_CF_Row, CURLOPT_CUSTOMREQUEST, HTTP_DELETE);
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // HBase server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForDeleteHBase_Column_CF_Row, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForDeleteHBase_Column_CF_Row, CURLOPT_HTTPHEADER, headersForDeleteHBase_Column_CF_Row);
	  }

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForDeleteHBase_Column_CF_Row);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  // In HBase, deleting a column always returns "200 OK" irrespective of whether the deletion happened or not.
	  // HBase (in Nov/2014) looks somewhat weird when compared to Redis, Cassandra etc. Sorry Hadoop fans to say this.
	  curl_easy_getinfo(curlForDeleteHBase_Column_CF_Row, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool hBaseResult = true;

	  // HTTP response codes: 200-->OK on success, 404--> Not found.
	  if ((httpResponseCode != HBASE_REST_OK) && (httpResponseCode != HBASE_REST_NOT_FOUND)) {
		  hBaseResult = false;
		  ostringstream hrc;
		  hrc << httpResponseCode;
		  httpReasonString = "hrc-->" + hrc.str();
	  }

	  return(hBaseResult);
  }

  // This method will return the total number of K/V pairs (a.k.a columns) in a table row.
  // URL format should be: http://user:password@HBase-REST-ServerNameOrIPAddress:port/TableName/RowKey
  bool HBaseDBLayer::getTotalNumberOfColumnsInHBaseTableRow(string const & url, uint32_t & totalColumns, int32_t & curlReturnCode,
     string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString) {
	  totalColumns = 0;
	  curlReturnCode = 0;
	  httpResponseCode = 0;
	  curlErrorString = "";
	  httpReasonString = "";
	  char *curlEffectiveUrl = NULL;
	  CURLcode result;
	  bool repeatingUrl = false;

	  // Get the URL endpoint that was used when we executed this method previously.
	  result = curl_easy_getinfo(curlForGetNumberOfColumnsInHBaseTableRow, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForGetNumberOfColumnsInHBaseTableRow);
		  curl_easy_setopt(curlForGetNumberOfColumnsInHBaseTableRow, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
		  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
		  curl_easy_setopt(curlForGetNumberOfColumnsInHBaseTableRow, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForGetNumberOfColumnsInHBaseTableRow, CURLOPT_WRITEFUNCTION, &HBaseDBLayer::writeFunction);
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // HBase server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForGetNumberOfColumnsInHBaseTableRow, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForGetNumberOfColumnsInHBaseTableRow, CURLOPT_HTTPHEADER,
		     headersForGetNumberOfColumnsInHBaseTableRow);
	  }

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForGetNumberOfColumnsInHBaseTableRow);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForGetNumberOfColumnsInHBaseTableRow, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool hBaseResult = true;

	  // HTTP response codes: 200-->All column retrieval successful
	  if (httpResponseCode != HBASE_REST_OK) {
		  // Some problem in retrieving all the columns.
		  curlReturnCode = 0;
		  ostringstream hrc;
		  hrc << httpResponseCode;
		  httpReasonString = "rc=" + hrc.str() + ", msg=Unable to fetch all the columns from a HBase table row.";
		  hBaseResult = false;
	  } else {
		  // If we have all the columns read successfully from the HBase server, let us parse them.
		  // All columns returned from a row will look like the example data shown below:
		  // {"Row":[{"key":"cm93MA==","Cell":[
		  // {"column":"ZmFtaWx5Mzpjb2x1bW4x","timestamp":1417127009324,"$":"dGVzdA=="},
		  // {"column":"ZmFtaWx5Mzpjb2x1bW4y","timestamp":1417225876172,"$":"dGVzdA=="},
		  // {"column":"ZmFtaWx5Mzpjb2x1bW4z","timestamp":1417225881034,"$":"dGVzdA=="},
		  // {"column":"ZmFtaWx5Mzpjb2x1bW40","timestamp":1417225884371,"$":"dGVzdA=="}]}]}
		  // Our goal is to count the number of elements in the Cell array.
		  char key[256];
		  strcpy(key, "Row");
		  json_object *jo = json_tokener_parse(curlBuffer);
		  json_object *joForRestResult = NULL;
		  bool totalNumberOfColumnsObtained = false;
		  // Get the Row object array.
		  json_bool exists = json_object_object_get_ex(jo, key, &joForRestResult);

		  if (exists) {
			  // Get the first array element.
			  json_object *joForRow = json_object_array_get_idx(joForRestResult, 0);
			  // Get the Cell object array within that row object.
			  json_object *joForCell = NULL;
			  strcpy(key, "Cell");
			  exists = json_object_object_get_ex(joForRow, key, &joForCell);

			  if (exists) {
				  // Get the size of this array object.
				  totalColumns = json_object_array_length(joForCell);
				  totalNumberOfColumnsObtained = true;
			  }
		  }

		  // Release JSON object.
		  json_object_put(jo);
		  hBaseResult = totalNumberOfColumnsObtained;
	  }

	  return(hBaseResult);
  }

  // This method will return all the keys found in the given HBase table row.
  // URL format should be: http://user:password@HBase-REST-ServerNameOrIPAddress:port/TableName/RowKey
  bool HBaseDBLayer::getAllColumnKeysInHBaseTableRow(string const & url, std::vector<std::string> & dataItemKeys, int32_t & curlReturnCode,
     string & curlErrorString, uint64_t & httpResponseCode, string & httpReasonString) {
	  curlReturnCode = 0;
	  httpResponseCode = 0;
	  curlErrorString = "";
	  httpReasonString = "";
	  char *curlEffectiveUrl = NULL;
	  CURLcode result;
	  bool repeatingUrl = false;

	  // Get the URL endpoint that was used when we executed this method previously.
	  result = curl_easy_getinfo(curlForGetAllColumnsInHBaseTableRow, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForGetAllColumnsInHBaseTableRow);
		  curl_easy_setopt(curlForGetAllColumnsInHBaseTableRow, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
		  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
		  curl_easy_setopt(curlForGetAllColumnsInHBaseTableRow, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForGetAllColumnsInHBaseTableRow, CURLOPT_WRITEFUNCTION, &HBaseDBLayer::writeFunction);
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // HBase server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForGetAllColumnsInHBaseTableRow, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForGetAllColumnsInHBaseTableRow, CURLOPT_HTTPHEADER, headersForGetAllColumnsInHBaseTableRow);
	  }

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForGetAllColumnsInHBaseTableRow);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  curlReturnCode = result;
		  ostringstream crc;
		  crc << curlReturnCode;
		  curlErrorString = "rc=" + crc.str() + ", msg=" + string(curl_easy_strerror(result));
		  return(false);
	  }

	  curl_easy_getinfo(curlForGetAllColumnsInHBaseTableRow, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool hBaseResult = true;

	  // HTTP response codes: 200-->All column retrieval successful
	  if (httpResponseCode != HBASE_REST_OK) {
		  // Some problem in retrieving all the columns.
		  curlReturnCode = 0;
		  ostringstream hrc;
		  hrc << httpResponseCode;
		  httpReasonString = "rc=" + hrc.str() + ", msg=Unable to fetch all the columns from a HBase table row.";
		  hBaseResult = false;
	  } else {
		  // If we have all the columns read successfully from the HBase server, let us parse them.
		  // All columns returned from a row will look like the example data shown below:
		  // {"Row":[{"key":"cm93MA==","Cell":[
		  // {"column":"ZmFtaWx5Mzpjb2x1bW4x","timestamp":1417127009324,"$":"dGVzdA=="},
		  // {"column":"ZmFtaWx5Mzpjb2x1bW4y","timestamp":1417225876172,"$":"dGVzdA=="},
		  // {"column":"ZmFtaWx5Mzpjb2x1bW4z","timestamp":1417225881034,"$":"dGVzdA=="},
		  // {"column":"ZmFtaWx5Mzpjb2x1bW40","timestamp":1417225884371,"$":"dGVzdA=="}]}]}
		  char key[256];
		  strcpy(key, "Row");
		  json_object *jo = json_tokener_parse(curlBuffer);
		  json_object *joForRestResult = NULL;
		  // Get the Row object array.
		  json_bool exists = json_object_object_get_ex(jo, key, &joForRestResult);

		  if (exists) {
			  // Get the first array element.
			  json_object *joForRow = json_object_array_get_idx(joForRestResult, 0);
			  // Get the Cell object array within that row object.
			  json_object *joForCell = NULL;
			  strcpy(key, "Cell");
			  exists = json_object_object_get_ex(joForRow, key, &joForCell);

			  if (exists) {
				  // Get the size of this array object.
				  uint32_t totalColumns = json_object_array_length(joForCell);

				  if (totalColumns > 0) {
					  int idx = 0;
					  for (idx = 0; idx < (int)totalColumns; idx++) {
						  // Get the next element in the "Cell" array.
						  json_object *cellObj = json_object_array_get_idx(joForCell, idx);
						  json_object *keyObj;
						  // Get the "column" field in the row element JSON. This field holds our store key.
						  exists =  json_object_object_get_ex(cellObj, "column", &keyObj);

						  if (exists) {
							  const char *data_item_key_ptr = json_object_get_string(keyObj);
							  // In the HBase table, all our keys and values are base64 encoded one additional time.
							  // That was needed by HBase. Let us peel off that layer of base64 encoding.
							  string base64_encoded_data_item_key = string(data_item_key_ptr);
							  string data_item_key;
							  base64_decode(base64_encoded_data_item_key, data_item_key);

							  // What we got back from HBase will always have the format as shown below in the example column keys:
							  // cf1:A0ZEUg==
							  // cf1:B0NsaW50b24=
							  // cf1:CVJvb3NldmVsdA==
							  //
							  // Let us discard the cf1: prefix from every column key that we got back from HBase.
							  data_item_key = data_item_key.substr(4);
							  // Every dps store will have three mandatory reserved data item keys for internal use.
							  // Let us not add them to the list of docs (i.e. store keys).
							  if (data_item_key.compare(HBASE_STORE_ID_TO_STORE_NAME_KEY) == 0) {
								  continue; // Skip this one.
							  } else if (data_item_key.compare(HBASE_SPL_TYPE_NAME_OF_KEY) == 0) {
								  continue; // Skip this one.
							  } else if (data_item_key.compare(HBASE_SPL_TYPE_NAME_OF_VALUE) == 0) {
								  continue; // Skip this one.
							  }

							  dataItemKeys.push_back(data_item_key);
						  } else {
							  httpResponseCode = HBASE_COLUMN_KEY_NOT_FOUND;
							  ostringstream hrc;
							  hrc << httpResponseCode;
							  httpReasonString = "rc=" + hrc.str() + ", msg=HBase: Store data item key is not found.";
							  hBaseResult = false;
							  break;
						  }
					  } // End of for loop
				  } else {
					  httpResponseCode = HBASE_COLUMN_KEY_NOT_FOUND;
					  ostringstream hrc;
					  hrc << httpResponseCode;
					  httpReasonString = "rc=" + hrc.str() + ", msg=HBase: No store keys are found.";
					  hBaseResult = false;
				  }
			  } else {
				  httpResponseCode = HBASE_COLUMN_KEY_NOT_FOUND;
				  ostringstream hrc;
				  hrc << httpResponseCode;
				  httpReasonString = "rc=" + hrc.str() + ", msg=HBase: Store column family cf1 Cell array is not found.";
				  hBaseResult = false;
			  }
		  } else {
			  httpResponseCode = HBASE_COLUMN_KEY_NOT_FOUND;
			  ostringstream hrc;
			  hrc << httpResponseCode;
			  httpReasonString = "rc=" + hrc.str() + ", msg=HBase: Store contents row key is not found.";
			  hBaseResult = false;
		  }

		  // Release it now.
		  json_object_put(jo);
	  }

	  return(hBaseResult);
  }

  // This method will report back whether a given HBase table exists or not.
  bool HBaseDBLayer::checkIfHBaseTableExists(string const & tableName) {
	  // We can do a table meta data query to the server and parse the results to
	  // determine whether this table exists or not. We can query the regions for this table.
	  string url = getNextHBaseBaseUrl() + tableName + "/regions";
	  uint64_t httpResponseCode = 0;
	  uint32_t totalRegions = 0;
	  char *curlEffectiveUrl = NULL;
	  CURLcode result;
	  bool repeatingUrl = false;

	  // Get the URL endpoint that was used when we executed this method previously.
	  result = curl_easy_getinfo(curlForHBaseTableExistenceCheck, CURLINFO_EFFECTIVE_URL, &curlEffectiveUrl);

	  if ((result == CURLE_OK) && (curlEffectiveUrl != NULL) && (url == (string(curlEffectiveUrl)))) {
		  // We can skip setting the cURL options if we are here to process the same URL that this function served in the previous call made here.
		  repeatingUrl = true;
	  }

	  if (repeatingUrl == false) {
		  // It is not the same URL we are connecting to again.
		  // We can't reuse the previously made cURL session. Reset it now.
		  curl_easy_reset(curlForHBaseTableExistenceCheck);
		  curl_easy_setopt(curlForHBaseTableExistenceCheck, CURLOPT_URL, url.c_str());
		  // cURL is C based and we are here in C++.
		  // Hence, we have to pass a static function pointer for WRITEFUNCTION and then pass a custom
		  // object reference for our current C++ class so that cURL will pass that object pointer during its callback.
		  curl_easy_setopt(curlForHBaseTableExistenceCheck, CURLOPT_WRITEDATA, this);
		  curl_easy_setopt(curlForHBaseTableExistenceCheck, CURLOPT_WRITEFUNCTION, &HBaseDBLayer::writeFunction);
		  // Enable the TCP keep alive so that we don't lose the connection with the
		  // HBase server when no store functions are performed for long durations.
		  curl_easy_setopt(curlForHBaseTableExistenceCheck, CURLOPT_TCP_KEEPALIVE, 1);
		  curl_easy_setopt(curlForHBaseTableExistenceCheck, CURLOPT_HTTPHEADER, headersForHBaseTableExistenceCheck);
	  }

	  curlBufferOffset = 0;
	  result = curl_easy_perform(curlForHBaseTableExistenceCheck);
	  curlBuffer[curlBufferOffset] = '\0';

	  if (result != CURLE_OK) {
		  return(false);
	  }

	  curl_easy_getinfo(curlForHBaseTableExistenceCheck, CURLINFO_RESPONSE_CODE, &httpResponseCode);
	  bool hBaseResult = true;

	  // HTTP response codes: 200-->Meta data regions query successful
	  if (httpResponseCode != HBASE_REST_OK) {
		  hBaseResult = false;
	  } else {
		  //
		  // If a table exists, reply will look like this:
		  // {"name":"table2",
		  //  "Region":[{"name":"table2,,1417126931885.2410ce7c3d4b4a8488b709a6c9fd0df8.",
		  //             "id":1417126931885,"startKey":"","endKey":"",
		  //             "location":"h0201b05.pok.hpc-ng.ibm.com:60020"}]}
		  //
		  // If a table doesn't exist, reply will look like this:
		  // {"name":"table7","Region":[]}
		  //
		  // Our goal is to count the number of elements in the Region array.
		  char key[256];
		  strcpy(key, "Region");
		  json_object *jo = json_tokener_parse(curlBuffer);
		  json_object *joForRegion = NULL;
		  // Get the Region object array.
		  json_bool exists = json_object_object_get_ex(jo, key, &joForRegion);

		  if (exists) {
			  // Get the size of this array object.
			  totalRegions = json_object_array_length(joForRegion);
		  }

		  // Release JSON object.
		  json_object_put(jo);

		  if (totalRegions <= 0) {
			  // No regions found. Hence, this table doesn't exist.
			  hBaseResult = false;
		  }
	  }

	  return(hBaseResult);
  }

  // This method will return the status of the connection to the back-end data store.
  bool HBaseDBLayer::isConnected() {
          // Not implemented at this time.
          return(true);
  }

  bool HBaseDBLayer::reconnect(std::set<std::string> & dbServers, PersistenceError & dbError) {
          // Not implemented at this time.
          return(true);
  }

} } } } }
using namespace com::ibm::streamsx::store;
using namespace com::ibm::streamsx::store::distributed;
extern "C" {
	DBLayer * create(){
	   return new HBaseDBLayer();
	}
}
