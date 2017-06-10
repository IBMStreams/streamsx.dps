/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#include <dlfcn.h>
#include "DistributedProcessStore.h"
#include "DPSToolkitResource.h"

#include <SPL/Runtime/ProcessingElement/ProcessingElement.h>
#include <SPL/Runtime/ProcessingElement/PE.h> // non-kosher
#include <SPL/Runtime/Serialization/NetworkByteBuffer.h>
#include <SPL/Runtime/Utility/Mutex.h>
#include <SPL/Runtime/Function/SPLFunctions.h>

#include <iostream>
#include <fstream>
#include <string>

#include <streams_boost/filesystem/path.hpp>
#include <streams_boost/filesystem/operations.hpp>
#include <streams_boost/algorithm/string.hpp>

using namespace std;
using namespace SPL;

#include <vector>

namespace com {
namespace ibm {
namespace streamsx {
namespace store {
namespace distributed
{
  std::string DistributedProcessStore::dpsConfigFile_ = "";

  DistributedProcessStore::DistributedProcessStore()
    : dbError_(new PersistenceError()),
      lkError_(new PersistenceError())
  {
    connectToDatabase();
  }

  DistributedProcessStore::~DistributedProcessStore()
  {
  }

  DBLayer & DistributedProcessStore::getDBLayer()
  {
    return *db_;
  }


  static void fetchDBConnectionParameters(std::string & noSqlKvStoreProductName, std::set<std::string> & dbServers, std::string & dpsConfigFile)
  {
	// Senthil commented the following code block on Nov/02/2013. Because, a stand-alone dps test from
	// Java and Python will give core dump errors because of the PE-specific code below.
	/*
    /// Non-kosher code here: The new version of SPL should have a proper API for this
    char * pePtr = reinterpret_cast<char *>(& SPL::PE::instance().getImpl());
    SPL::ProcessingElement & pe = *reinterpret_cast<SPL::ProcessingElement*>(pePtr+sizeof(void *));
    /// End of non-kosher code
    std::string const& dataDir  = pe.getDataDirectory();
    streams_boost::filesystem::path conf(dataDir);
    // For any Streams application, its "data" directory is the current directory at all times.
    // Wherever one wants to navigate inside the file system, a relative path can be
    // formed with respect to the "data" directory of a given Streams application.
    // We will open the config file from the etc sub-directory of the application trying to use the dps toolkit.
    conf /= "../etc/no-sql-kv-store-servers.cfg";
    std::string confFile = conf.string();
    */
	std::string appDirectory = ProcessingElement::pe().getApplicationDirectory();
	/*
	std::string dataDirectory = ProcessingElement::pe().getDataDirectory();
	std::string outputDirectory = ProcessingElement::pe().getOutputDirectory();
	std::string toolkitDirectory = ProcessingElement::pe().getToolkitDirectory();
	cout << "App directory=" << appDirectory << endl;
	cout << "Data directory=" << dataDirectory << endl;
	cout << "Output directory=" << outputDirectory << endl;
	cout << "Toolkit directory=" << toolkitDirectory << endl;
	*/

	// Refer to the file path provided in parameter dpsConfigFile
	// if it is relative, prepend the application directory
	std::string confFile;
	streams_boost::filesystem::path configPath(dpsConfigFile);
	if(configPath.is_relative()) {
		configPath = streams_boost::filesystem::absolute(configPath, appDirectory);
	}
	confFile = configPath.string();

    // Format of this file is as shown below.
    // Several comment lines beginning with a # character.
    // Then, the very first non-comment line will tell us the
    // no-sql store product name being used (memcached, redis, cassandra, cloudant etc.).
    // After that line, there will be a list of server names one per line.
    //
    // Read all the lines in the file.
    string line;
    ifstream myfile (confFile.c_str());
    int32 serverCnt = 0;
    bool noSqlKvStoreProductNameFound = false;

    if (myfile.is_open()) {
      while (myfile.good()){
        getline (myfile, line);
        streams_boost::algorithm::trim(line);

        // If it is an empty line, skip processing it.
        if (line == "") {
        	continue;
        }

        // Check whether it is a comment line. i.e. very first character starting with #
        size_t pos = line.find_first_of("#", 0);

        if ((pos == string::npos) || (pos > 0)) {
        	if (noSqlKvStoreProductNameFound == false) {
        		// This must be the very first non-comment line containing the
        		// name of the no-sql store product being configured.
        		noSqlKvStoreProductNameFound = true;
        		noSqlKvStoreProductName = line;
        	} else {
        		dbServers.insert(line);
        		serverCnt++;
        	}
        }
      }

      myfile.close();
    }

    if (serverCnt == 0) {
       // SPLAPPLOG is causing it to get stuck in RHEL6/CentOS6 (RHEL7/CentOS7 is fine) when the @catch annotation is used in the calling SPL code.
       // SPLAPPLOG(L_ERROR, DPSMSG_CANNOT_GET_PRODUCT_NAME(confFile), "DistributedProcessStore");
       std::string error = "Cannot get NoSQL K/V store product name and/or the server names from the configuration file '"+confFile+"'";
       SPLAPPTRC(L_ERROR, error, "DistributedProcessStore");
       throw(SPL::SPLRuntimeException("fetchDBParameters", error));
    }
  }

  void * load_dependent_lib(std::string toolkitDir, std::string lib){
     std::string libToLoad = toolkitDir + "/" + lib;
     void *handle = dlopen(libToLoad.c_str(), RTLD_NOW|RTLD_GLOBAL);

     if (handle == NULL) {
        std::string err = "DpsHelper: dlopen failed for " +lib;
        err.append(dlerror());
        SPLAPPTRC(L_ERROR, err, "DistributedProcessStore");
        // SPLAPPLOG is causing it to get stuck in RHEL6/CentOS6 (RHEL7/CentOS7 is fine) when the @catch annotation is used in the calling SPL code.
        // SPLAPPLOG(L_ERROR, DPSMSG_DLOPEN_FAILED(lib,dlerror()), "DistributedProcessStore");
     }

     return(handle);
  }

  // any errors in here are thrown during static initialization, so we
  // better log, not just throw (as SPL runtime is not around to catch it)
  void DistributedProcessStore::connectToDatabase()
  {
    dbError_->reset();
    std::string noSqlKvStoreProductName = "";
    std::set<std::string> dbServers;

    // Read the no-sql store product name and the
    // no-sql store server names from the configuration file.
    std::string configFile = (DistributedProcessStore::dpsConfigFile_ == "") ? "etc/no-sql-kv-store-servers.cfg" : DistributedProcessStore::dpsConfigFile_;
    fetchDBConnectionParameters(noSqlKvStoreProductName, dbServers, configFile);
    noSqlKvStoreProductName = streams_boost::to_lower_copy(noSqlKvStoreProductName);

	// Verify if the user has configured a valid no-sql store product that we support.
    // If it is a supported product, then initialize our back-end DBLayer accordingly.
    //
	// Deallocate the current object pointed to by this auto_ptr typed db_ object and
	// assign it to a new DBLayer instance.
    void* handle = NULL;
    void* handle1 = NULL;
    void* handle2 = NULL;
    void* handle3 = NULL;
    bool libraryLoadingError = false;
    std::string  kvLibName =  "";
    std::string toolkitDir = ProcessingElement::pe().getToolkitDirectory("com.ibm.streamsx.dps")  + "/impl/ext/lib" ;
    std::string streamsLibDir = SPL::Functions::Utility::getEnvironmentVariable("STREAMS_INSTALL") + "/ext/lib" ;
	if (noSqlKvStoreProductName.compare("memcached") == 0) {
		// reset method below is part of the C++ std::auto_ptr class.
		kvLibName= "libDPSMemcached.so";
	} else if (noSqlKvStoreProductName.compare("redis") == 0) {
		handle1 = load_dependent_lib(toolkitDir, "libuv.so");
		handle2 = load_dependent_lib(toolkitDir, "libhiredis.so");
		kvLibName= "libDPSRedis.so";

		if (handle1 == NULL || handle2 == NULL) {
		   libraryLoadingError = true;
		}
	} else if (noSqlKvStoreProductName.compare("cassandra") == 0) {
		handle1 = load_dependent_lib(toolkitDir, "libjson-c.so");
		handle2 = load_dependent_lib(toolkitDir, "libuv.so");
		handle3 = load_dependent_lib(toolkitDir, "libcassandra.so");
		kvLibName= "libDPSCassandra.so";

		if (handle1 == NULL || handle2 == NULL || handle3 == NULL) {
		   libraryLoadingError = true;
		}
	} else if (noSqlKvStoreProductName.compare("cloudant") == 0) {
		handle1 = load_dependent_lib(toolkitDir, "libjson-c.so");
		handle2 = load_dependent_lib(toolkitDir, "libcurl.so");
		kvLibName= "libDPSCloudant.so";

		if (handle1 == NULL || handle2 == NULL) {
		   libraryLoadingError = true;
		}
	} else if (noSqlKvStoreProductName.compare("hbase") == 0) {
		handle1 = load_dependent_lib(toolkitDir, "libjson-c.so");
		handle2 = load_dependent_lib(toolkitDir, "libcurl.so");
		kvLibName= "libDPSHBase.so";

		if (handle1 == NULL || handle2 == NULL) {
		   libraryLoadingError = true;
		}
	} else if (noSqlKvStoreProductName.compare("mongo") == 0) {
		handle1 = load_dependent_lib(toolkitDir, "libjson-c.so");
		handle2 = load_dependent_lib(toolkitDir, "libbson.so");
		handle3 = load_dependent_lib(toolkitDir, "libmongoc.so");
		kvLibName= "libDPSMongo.so";

		if (handle1 == NULL || handle2 == NULL || handle3 == NULL) {
		   libraryLoadingError = true;
		}
 	} else if (noSqlKvStoreProductName.compare("couchbase") == 0) {
 		handle1 = load_dependent_lib(toolkitDir, "libjson-c.so");
 		handle2 = load_dependent_lib(toolkitDir,"libcurl.so");
 		handle3 = load_dependent_lib(toolkitDir, "libcouchbase.so");
 		kvLibName= "libDPSCouchbase.so";

		if (handle1 == NULL || handle2 == NULL || handle3 == NULL) {
		   libraryLoadingError = true;
		}
#if !( defined (__PPC64__) )
 	} else if (noSqlKvStoreProductName.compare("aerospike") == 0) {
 		handle1 = load_dependent_lib(toolkitDir, "libaerospike.so");
 		kvLibName= "libDPSAerospike.so";

		if (handle1 == NULL) {
		   libraryLoadingError = true;
		}
#endif
	} else if (noSqlKvStoreProductName.compare("redis-cluster") == 0) {
		handle1 = load_dependent_lib(toolkitDir, "libuv.so");
		handle2 = load_dependent_lib(toolkitDir,"libhiredis.so");
		kvLibName= "libDPSRedisCluster.so";

		if (handle1 == NULL || handle2 == NULL) {
		   libraryLoadingError = true;
		}
	} else {
		// Invalid no-sql store product name configured. Abort now.
                // SPLAPPLOG is causing it to get stuck in RHEL6/CentOS6 (RHEL7/CentOS7 is fine) when the @catch annotation is used in the calling SPL code.
		// SPLAPPLOG(L_ERROR, DPSMSG_INVALID_PRODUCT(noSqlKvStoreProductName), "DistributedProcessStore");
		std::string error = "Invalid NoSQL store product name is specified in the configuration file: " + noSqlKvStoreProductName;
		SPLAPPTRC(L_ERROR, error, "DistributedProcessStore");
		throw(SPL::SPLRuntimeException("DistributedProcessStore::connectToDatabase", error));
	}
//	cout << "Going to load " << kvLibName << endl;
	handle = dlopen(kvLibName.c_str(), RTLD_NOW|RTLD_GLOBAL);
	if (handle == NULL || libraryLoadingError == true) {
	      std::string error = "Cannot initialize libraries for chosen database " + noSqlKvStoreProductName + ", error message: ";
	      error.append(dlerror());
	      SPLAPPTRC(L_ERROR, error, "DistributedProcessStore");
              // SPLAPPLOG is causing it to get stuck in RHEL6/CentOS6 (RHEL7/CentOS7 is fine) when the @catch annotation is used in the calling SPL code.
	      // SPLAPPLOG(L_ERROR, DPSMSG_CANNOT_OPEN_LIBS(noSqlKvStoreProductName,dlerror()), "DistributedProcessStore");

	      // Unload all the dynamically loaded libraries before throwing an exception.
	      // If we don't do this, it will cause too many OS dl reference counts for this thread which is not optimal.
              if (handle1 != NULL) {
                 dlclose(handle1);
                 handle1 = NULL;
	      }

              if (handle2 != NULL) {
                 dlclose(handle2);
		 handle2 = NULL;
	      }

              if (handle3 != NULL) {
                 dlclose(handle3);
	         handle3 = NULL;
	      }

              if (handle != NULL) {
                 dlclose(handle);
	         handle = NULL;
	      }

	      throw(SPL::SPLRuntimeException("DistributedProcessStore::connectToDatabase", error));
	}

	DBLayer *(*objPtr)()= (DBLayer *(*)())dlsym(handle, "create");
	DBLayer *newDb = (*objPtr)();
	if (newDb != NULL) {
		db_.reset(newDb);
	} else {
                 // SPLAPPLOG is causing it to get stuck in RHEL6/CentOS6 (RHEL7/CentOS7 is fine) when the @catch annotation is used in the calling SPL code.
                 // SPLAPPLOG(L_ERROR, DPSMSG_CANNOT_INIT_LIBS(noSqlKvStoreProductName,kvLibName), "DistributedProcessStore");
		 std::string error = "Cannot initialize libraries for chosen database " + noSqlKvStoreProductName + ", library " + kvLibName + " missing or corrupted";
		 SPLAPPTRC(L_ERROR, error, "DistributedProcessStore");

	         // Unload all the dynamically loaded libraries before throwing an exception.
	         // If we don't do this, it will cause too many OS dl reference counts for this thread which is not optimal.
                 if (handle1 != NULL) {
                    dlclose(handle1);
                    handle1 = NULL;
	         }

                 if (handle2 != NULL) {
                    dlclose(handle2);
		    handle2 = NULL;
	         }

                 if (handle3 != NULL) {
                    dlclose(handle3);
	            handle3 = NULL;
	         }

                 if (handle != NULL) {
                    dlclose(handle);
	            handle = NULL;
	         }

		 throw(SPL::SPLRuntimeException("DistributedProcessStore::connectToDatabase", error));
	}

    db_->connectToDatabase(dbServers, *dbError_);
    if(dbError_->hasError()) {
      std::string error = "Cannot connect to database. ";
      error += "Details: '"+dbError_->getErrorStr()+"'.";
      SPLAPPTRC(L_ERROR, error, "DistributedProcessStore");
      // SPLAPPLOG is causing it to get stuck in RHEL6/CentOS6 (RHEL7/CentOS7 is fine) when the @catch annotation is used in the calling SPL code.
      // SPLAPPLOG(L_ERROR, DPSMSG_CANNOT_CONNECT(dbError_->getErrorStr()), "DistributedProcessStore");

      // Unload all the dynamically loaded libraries before throwing an exception.
      // If we don't do this, it will cause too many OS dl reference counts for this thread which is not optimal.
      if (handle1 != NULL) {
         dlclose(handle1);
         handle1 = NULL;
      }

      if (handle2 != NULL) {
         dlclose(handle2);
         handle2 = NULL;
      }

      if (handle3 != NULL) {
         dlclose(handle3);
         handle3 = NULL;
      }

      if (handle != NULL) {
         dlclose(handle);
         handle = NULL;
      }

      // Deallocate and destruct the object pointed to by our auto ptrs.
      db_.reset();
      dbError_.reset();
      lkError_.reset();

      throw(SPL::SPLRuntimeException("DistributedProcessStore::connectToDatabase", error));
    }
  }

  DistributedProcessStore & DistributedProcessStore::getGlobalStore()
  {
	// Senthil commented the following code block on Nov/02/2013. Because, a stand-alone dps test from
	// Java and Python will give core dump errors because of the TLS (thread local storage via __thread).
	/*
	// Attempt 1:
	// We will have a thread local storage (TLS) defined via __thread. This will let us have
	// an individual memory copy of the DistributedProcessStore object for every thread that will be
	// trying to use the dps. That will let us have error codes and error messages in every thread's own object.
	//
    // If you have to remove the __thread for some reason, then remove the pointer "*" and
    // the entire if block containing "new" statement in the code below.
    static __thread DistributedProcessStore * store = 0;
    if(store==0) {
      store = new DistributedProcessStore();
    }

    return *store;
    */

	/*
	// Attempt 2:
	// This code block works fine with the stand-alone Java and Python clients.
	// But, it doesn't provide TLS (thread local storage) which is important for us.
	static DistributedProcessStore store;
    return store;
    */

	/*
	// Attempt 3:
	// Since there is no locking done for the map, following approach is also not clean.
	// Because, there is no locking of the map and there is no clean up of the allocated dps object.
	// Declare a map where we can store the dps instances for individual threads that come and go.
	static std::map<pthread_t, DistributedProcessStore*> stores;
	DistributedProcessStore * s1 = 0;
	// Get the current thread id. (This should be unique at all times on any given machine.)
	pthread_t id = pthread_self();

	if (stores.find(id) == stores.end()) {
		// Add a new thread entry into our map.
		s1 = new DistributedProcessStore();
		stores[id] = s1;
	} else {
		// This is an already running thread. Let us fetch its previously allocated store.
		s1 = stores[id];
	}

	// cout << "Current pid=" << getpid() << ", Current thread id=" << id << ", stores map size=" << stores.size() << endl;
	return *s1;
	*/

	// Attempt 4:
	// Use a static unordered map with proper locking of that map. This map will keep caching
	// the dps object pointers for every unique thread that is using the dps APIs.
	// When the parent application (Streams, Java or Python) is stopped, dps objects stored in
	// this map will also be freed and destroyed. That is possible because we are using the shared_ptr as shown below.
	static tr1::unordered_map<pthread_t, tr1::shared_ptr<DistributedProcessStore> > stores;
	static pthread_mutex_t dpsMapLock = PTHREAD_MUTEX_INITIALIZER;
	DistributedProcessStore * res;
	// Get the current thread id.
	pthread_t id = pthread_self();
	// Lock the following code block.
	pthread_mutex_lock(&dpsMapLock);
	// Get a map iterator.
	tr1::unordered_map<pthread_t, tr1::shared_ptr<DistributedProcessStore> >::iterator it;
	// Look if the current thread is a repeat customer of the dps.
	// i.e. see if this thread has its own dps object pointer already created and cached.
	it = stores.find(id);
	if (it == stores.end()) {
	   // This thread is coming here for the very first time to use the dps.
	   // Allocate a dps object and cache it for future use.
           try {
	      tr1::shared_ptr<DistributedProcessStore> dps(new DistributedProcessStore());
	      stores[id] = dps;
	      // Fetch the dps object pointer we created just now for this thread.
	      res = dps.get();
           } catch(SPL::SPLRuntimeException ex) {
              // Unlock the code block that was locked above.
              pthread_mutex_unlock(&dpsMapLock);
              // Rethrow the same exception.
              throw(ex);
           }
	} else {
	   // Fetch the dps object pointer that was already created and cached for this thread.
	   // Iterator will give us a pair. In that pair, we want to get the second element.
	   res = it->second.get();
	}

	// Unlock the code block that was locked above.
	pthread_mutex_unlock(&dpsMapLock);
	// Return the dps object pointer.
	return *res;
  }

  SPL::uint64 DistributedProcessStore::findStore(SPL::rstring const & name, SPL::uint64 & err)
  {
    dbError_->reset();
    SPL::uint64 res = db_->findStore(name, *dbError_);
    err = dbError_->getErrorCode();
    return res;
  }

  SPL::boolean DistributedProcessStore::removeStore(SPL::uint64 store, SPL::uint64 & err)
  {
    dbError_->reset();
    bool result = db_->removeStore(store, *dbError_);
    err = dbError_->getErrorCode();
    return result;
  }

  void DistributedProcessStore::clear(SPL::uint64 store, SPL::uint64 & err)
  {
    dbError_->reset();
    db_->clear(store, *dbError_);
    err = dbError_->getErrorCode();
  }

  SPL::uint64 DistributedProcessStore::size(SPL::uint64 store, SPL::uint64 & err)
  {
    dbError_->reset();
    SPL::uint64 size = db_->size(store, *dbError_);
    err = dbError_->getErrorCode();
    return size;
  }

  SPL::rstring DistributedProcessStore::getLastPersistenceErrorString() const
  {
    return dbError_->getErrorStr();
  }

  SPL::rstring DistributedProcessStore::getLastPersistenceErrorStringTTL() const
  {
    return dbError_->getErrorStrTTL();
  }

  SPL::uint64 DistributedProcessStore::getLastPersistenceErrorCode() const
  {
    return dbError_->getErrorCode();
  }

  SPL::uint64 DistributedProcessStore::getLastPersistenceErrorCodeTTL() const
  {
    return dbError_->getErrorCodeTTL();
  }

  SPL::uint64 DistributedProcessStore::beginIteration(SPL::uint64 store, SPL::uint64 & err)
  {
     dbError_->reset();
     DBLayer::Iterator * iter = db_->newIterator(store, *dbError_);
     err = dbError_->getErrorCode();
     return reinterpret_cast<SPL::uint64>(iter);
  }

  void DistributedProcessStore::endIteration(SPL::uint64 store, SPL::uint64 iterator, SPL::uint64 & err)
  {
     dbError_->reset();
     DBLayer::Iterator * iter = reinterpret_cast<DBLayer::Iterator *>(iterator);
     db_->deleteIterator(store, iter, *dbError_);
     err = dbError_->getErrorCode();
  }

  SPL::uint64 DistributedProcessStore::createStoreForJava(SPL::rstring const & name, SPL::rstring const & key, SPL::rstring const & value, SPL::uint64 & err)
  {
	dbError_->reset();
	SPL::uint64 result = db_->createStore(name, key, value, *dbError_);
	err = dbError_->getErrorCode();
	return result;
  }

  SPL::uint64 DistributedProcessStore::createOrGetStoreForJava(SPL::rstring const & name, SPL::rstring const & key, SPL::rstring const & value, SPL::uint64 & err)
  {
	dbError_->reset();
	SPL::uint64 result = db_->createOrGetStore(name, key, value, *dbError_);
	err = dbError_->getErrorCode();
	return result;
  }

  SPL::boolean DistributedProcessStore::putForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, unsigned char const *value, SPL::uint32 valueSize, SPL::uint64 & err)
  {
	dbError_->reset();
	SPL::boolean result = db_->put(store, key, keySize, value, valueSize, *dbError_);
	err = dbError_->getErrorCode();
	return result;
  }

  SPL::boolean DistributedProcessStore::putSafeForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, unsigned char const *value, SPL::uint32 valueSize, SPL::uint64 & err)
  {
	dbError_->reset();
	SPL::boolean result = db_->putSafe(store, key, keySize, value, valueSize, *dbError_);
	err = dbError_->getErrorCode();
	return result;
  }

  SPL::boolean DistributedProcessStore::putTTLForJava(char const *key, SPL::uint32 keySize, unsigned char const *value, SPL::uint32 valueSize, SPL::uint32 const & ttl, SPL::uint64 & err, SPL::boolean encodeKey, SPL::boolean encodeValue)
   {
 	dbError_->resetTTL();
 	SPL::boolean result = db_->putTTL(key, keySize, value, valueSize, ttl, *dbError_, encodeKey, encodeValue);
 	err = dbError_->getErrorCodeTTL();
 	return result;
   }

  SPL::boolean DistributedProcessStore::getForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, unsigned char * & value, SPL::uint32 & valueSize, SPL::uint64 & err)
  {
	dbError_->reset();
	SPL::boolean result = db_->get(store, key, keySize, value, valueSize, *dbError_);
	err = dbError_->getErrorCode();
	return result;
  }

  SPL::boolean DistributedProcessStore::getSafeForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, unsigned char * & value, SPL::uint32 & valueSize, SPL::uint64 & err)
  {
	dbError_->reset();
	SPL::boolean result = db_->getSafe(store, key, keySize, value, valueSize, *dbError_);
	err = dbError_->getErrorCode();
	return result;
  }

  SPL::boolean DistributedProcessStore::getTTLForJava(char const *key, SPL::uint32 keySize, unsigned char * & value, SPL::uint32 & valueSize, SPL::uint64 & err, SPL::boolean encodeKey, SPL::boolean encodeValue)
  {
	dbError_->resetTTL();
	SPL::boolean result = db_->getTTL(key, keySize, value, valueSize, *dbError_, encodeKey);
	err = dbError_->getErrorCodeTTL();
	return result;
  }

  SPL::boolean DistributedProcessStore::removeForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, SPL::uint64 & err)
  {
	dbError_->reset();
	SPL::boolean result = db_->remove(store, key, keySize, *dbError_);
	err = dbError_->getErrorCode();
	return result;
  }

  SPL::boolean DistributedProcessStore::removeTTLForJava(char const *key, SPL::uint32 keySize, SPL::uint64 & err, SPL::boolean encodeKey)
  {
	dbError_->resetTTL();
	SPL::boolean result = db_->removeTTL(key, keySize, *dbError_, encodeKey);
	err = dbError_->getErrorCodeTTL();
	return result;
  }

  SPL::boolean DistributedProcessStore::hasForJava(SPL::uint64 store, char const *key, SPL::uint32 keySize, SPL::uint64 & err)
  {
	dbError_->reset();
	SPL::boolean result = db_->has(store, key, keySize, *dbError_);
	err = dbError_->getErrorCode();
	return result;
  }

  SPL::boolean DistributedProcessStore::hasTTLForJava(char const *key, SPL::uint32 keySize, SPL::uint64 & err, SPL::boolean encodeKey)
  {
	dbError_->resetTTL();
	SPL::boolean result = db_->hasTTL(key, keySize, *dbError_, encodeKey);
	err = dbError_->getErrorCodeTTL();
	return result;
  }

  SPL::boolean DistributedProcessStore::getNextForJava(SPL::uint64 store, SPL::uint64 iterator, unsigned char * &  key, SPL::uint32 & keySize,
  	unsigned char * & value, SPL::uint32 & valueSize, SPL::uint64 & err)
  {
      dbError_->reset();
      DBLayer::Iterator * iter = reinterpret_cast<DBLayer::Iterator *>(iterator);
      SPL::boolean result = iter->getNext(store, key, keySize, value, valueSize, *dbError_);
      err = dbError_->getErrorCode();
      return result;
  }

  SPL::boolean  DistributedProcessStore::runDataStoreCommandForJava(unsigned char *serializedListOfRString, SPL::uint32 cmdListSize, SPL::rstring & resultString, SPL::uint64 & err) {
     // First argument to this method is passed from the Java operator's JNI layer as an SPL::list<rstring> in serialized form.
     // We have to deserialize the serialized bytes into a proper SPL::list<rstring> before calling the overloaded runDataStore command below.
     SPL::list<rstring> cmdList;
     SPL::NativeByteBuffer cmdList_nbf(serializedListOfRString, cmdListSize);
     cmdList_nbf >> cmdList;

     // Call the overloaded method in this file below.
     return(runDataStoreCommand(cmdList, resultString, err));
  }

   SPL::uint64 DistributedProcessStore::createOrGetLock(SPL::rstring const & name, SPL::uint64 & err)
     {
       lkError_->reset();
       SPL::uint64 lock = db_->createOrGetLock(name, *lkError_);
       err = lkError_->getErrorCode();
       return lock;
     }

     SPL::boolean DistributedProcessStore::removeLock(SPL::uint64 lock, SPL::uint64 & err)
     {
       lkError_->reset();
       bool result = db_->removeLock(lock, *lkError_);
       err = lkError_->getErrorCode();
       return result;
     }

     void DistributedProcessStore::acquireLock(SPL::uint64 lock, SPL::uint64 & err)
     {
       lkError_->reset();
       // use 10 year lease time to mean infinity and 15 seconds as max wait time to acquire the lock.
       db_->acquireLock(lock, 315360000, 15, *lkError_);
       err = lkError_->getErrorCode();
     }

     void DistributedProcessStore::acquireLock(SPL::uint64 lock, SPL::float64 leaseTime, SPL::float64 maxWaitTimeToAcquireLock, SPL::uint64 & err)
     {
       lkError_->reset();
       db_->acquireLock(lock, leaseTime, maxWaitTimeToAcquireLock, *lkError_);
       err = lkError_->getErrorCode();
     }

     void DistributedProcessStore::releaseLock(SPL::uint64 lock, SPL::uint64 & err)
     {
       lkError_->reset();
       db_->releaseLock(lock, *lkError_);
       err = lkError_->getErrorCode();
     }

     SPL::uint32 DistributedProcessStore::getPidForLock(SPL::rstring const & name, SPL::uint64 & err)
     {
         lkError_->reset();
         SPL::uint32 pid = db_->getPidForLock(name, *lkError_);
         err = lkError_->getErrorCode();
         return pid;
     }

     SPL::rstring DistributedProcessStore::getLastLockErrorString() const
     {
       return lkError_->getErrorStr();
     }

     SPL::uint64 DistributedProcessStore::getLastLockErrorCode() const
     {
       return lkError_->getErrorCode();
     }

     SPL::rstring DistributedProcessStore::getStoreName(SPL::uint64 store) {
    	 dbError_->reset();
    	 return db_->getStoreName(store, *dbError_);
     }

     // This method will be used during the C++ createStore and CreateOrGetStore calls.
     SPL::rstring DistributedProcessStore::getSPLTypeName(ConstValueHandle const & handle)
     {
       SPL::Meta::Type mtype = handle.getMetaType();
       switch(mtype) {
       	   case Meta::Type::INVALID:
       		   assert(!"cannot happen");
       		   return "";
       	   case Meta::Type::BOOLEAN:
       		   return "boolean";
       	   case Meta::Type::ENUM: {
       		   Enum const & data = handle;
       		   string res = "enum<";
       		   vector<string> const & enums = data.getValidValues();
       		   for (size_t i=0, iu=enums.size(); i<iu; ++i) {
       			   if (i>0) {
       				   res += ",";
       			   }

       			   res += enums[i];
       		   }
       		   res += ">";
       		   return res;
       	   }
       	   case Meta::Type::INT8:
       		   return "int8";
       	   case Meta::Type::INT16:
       		   return "int16";
       	   case Meta::Type::INT32:
       		   return "int32";
       	   case Meta::Type::INT64:
       		   return "int64";
       	   case Meta::Type::UINT8:
       		   return "uint8";
       	   case Meta::Type::UINT16:
       		   return "uint16";
       	   case Meta::Type::UINT32:
       		   return "uint32";
       	   case Meta::Type::UINT64:
       		   return "uint64";
       	   case Meta::Type::FLOAT32:
       		   return "float32";
       	   case Meta::Type::FLOAT64:
       		   return "float64";
       	   case Meta::Type::DECIMAL32:
       		   return "decimal32";
       	   case Meta::Type::DECIMAL64:
       		   return "decimal64";
       	   case Meta::Type::DECIMAL128:
       		   return "decimal128";
       	   case Meta::Type::COMPLEX32:
       		   return "complex32";
       	   case Meta::Type::COMPLEX64:
       		   return "complex64";
       	   case Meta::Type::TIMESTAMP:
       		   return "timestamp";
       	   case Meta::Type::RSTRING:
       		   return "rstring";
       	   case Meta::Type::BSTRING: {
       		   BString const & data = handle;
       		   string res = "rstring[";
       		   ostringstream ostr;
       		   ostr << data.getBoundedSize();
       		   res += ostr.str() + "]";
       		   return res;
       	   }
       	   case Meta::Type::USTRING:
       		   return "ustring";
       	   case Meta::Type::BLOB:
       		   return "blob";
       	   case Meta::Type::LIST: {
       		   List const & data = handle;
       		   string res = "list<";
       		   ValueHandle elem = data.createElement();
       		   // Recursion
       		   res += getSPLTypeName(elem);
       		   elem.deleteValue();
       		   res += ">";
       		   return res;
    	  }
       	   case Meta::Type::BLIST: {
       		   BList const & data = handle;
       		   string res = "list<";
       		   ValueHandle elem = data.createElement();
       		   // Recursion
       		   res += getSPLTypeName(elem);
       		   elem.deleteValue();
       		   res += ">[";
       		   ostringstream ostr;
       		   ostr << data.getBoundedSize();
       		   res += ostr.str() + "]";
       		   return res;
       	   }
    	  case Meta::Type::SET: {
    		  Set const & data = handle;
    		  string res = "set<";
    		  ValueHandle elem = data.createElement();
    		  // Recursion
    		  res += getSPLTypeName(elem);
    		  elem.deleteValue();
    		  res += ">";
    		  return res;
    	  }
    	  case Meta::Type::BSET: {
    		  BSet const & data = handle;
    		  string res = "set<";
    		  ValueHandle elem = data.createElement();
    		  // Recursion
    		  res += getSPLTypeName(elem);
    		  elem.deleteValue();
    		  res += ">[";
    		  ostringstream ostr;
    		  ostr << data.getBoundedSize();
    		  res += ostr.str() + "]";
    		  return res;
    	  }
    	  case Meta::Type::MAP: {
    		  Map const & data = handle;
    		  string res = "map<";
    		  ValueHandle key = data.createKey();
    		  ValueHandle value = data.createValue();
    		  res += getSPLTypeName(key);
    		  res += ",";
    		  // Recursion
    		  res += getSPLTypeName(value);
    		  key.deleteValue();
    		  value.deleteValue();
    		  res += ">";
    		  return res;
    	  }
    	  case Meta::Type::BMAP: {
    		  BMap const & data = handle;
    		  string res = "map<";
    		  ValueHandle key = data.createKey();
    		  ValueHandle value = data.createValue();
    		  // Recursion
    		  res += getSPLTypeName(key);
    		  res += ",";
    		  // Recursion
    		  res += getSPLTypeName(value);
    		  key.deleteValue();
    		  value.deleteValue();
    		  res += ">[";
    		  ostringstream ostr;
    		  ostr << data.getBoundedSize();
    		  res += ostr.str() + "]";
    		  return res;
    	  }
    	  case Meta::Type::TUPLE: {
    		  Tuple const & data = handle;
    		  string res = "tuple<";
    		  for (size_t i=0, iu=data.getNumberOfAttributes(); i<iu; ++i) {
    			  if (i>0) {
    				  res += ",";
    			  }
    			  ConstValueHandle attrb = data.getAttributeValue(i);
    			  res += getSPLTypeName(attrb);
    			  res += " " + data.getAttributeName(i);
    		  }
    		  res += ">";
    		  return res;
    	  }
    	  case Meta::Type::XML:
    		  return "xml";
       }
   	   assert(!"cannot happen");
   	   return "";
	 }

     SPL::rstring DistributedProcessStore::getSplTypeNameForKey(SPL::uint64 store) {
    	 dbError_->reset();
    	 return db_->getSplTypeNameForKey(store, *dbError_);
     }

     SPL::rstring DistributedProcessStore::getSplTypeNameForValue(SPL::uint64 store) {
    	 dbError_->reset();
    	 return db_->getSplTypeNameForValue(store, *dbError_);
     }

     SPL::rstring DistributedProcessStore::getNoSqlDbProductName(void) {
    	 dbError_->reset();
    	 return db_->getNoSqlDbProductName();
     }

     void DistributedProcessStore::getDetailsAboutThisMachine(SPL::rstring & machineName, SPL::rstring & osVersion, SPL::rstring & cpuArchitecture) {
		 dbError_->reset();
		 // If we pass an rstring typed non constant variable reference into the downstream
		 // DB Layer code that receives it as an std::string, it fails to compile on the IBM Power machines.
		 // To avoid that compiler error, we have to do this indirection by passing an std::string variable into
		 // the DB Layer and then type cast it back to an rstring before returning from here.
		 std::string _machineName = "";
		 std::string _osVersion = "";
		 std::string _cpuArchitecture = "";
		 db_->getDetailsAboutThisMachine(_machineName, _osVersion, _cpuArchitecture);
		 machineName = static_cast<SPL::rstring> (_machineName);
		 osVersion = static_cast<SPL::rstring> (_osVersion);
		 cpuArchitecture = static_cast<SPL::rstring> (_cpuArchitecture);
		 return;
   	}

     SPL::boolean DistributedProcessStore::runDataStoreCommand(SPL::rstring const & cmd, SPL::uint64 & err) {
    	 dbError_->reset();
    	 SPL::boolean result = db_->runDataStoreCommand(cmd, *dbError_);
    	 err = dbError_->getErrorCode();
    	 return result;
     }

     SPL::boolean DistributedProcessStore::runDataStoreCommand(SPL::uint32 const & cmdType, SPL::rstring const & httpVerb,
    		 SPL::rstring const & baseUrl, SPL::rstring const & apiEndpoint, SPL::rstring const & queryParams,
    		 SPL::rstring const & jsonRequest, SPL::rstring & jsonResponse, SPL::uint64 & err) {
    	 dbError_->reset();
    	 // If we pass an rstring typed non constant variable reference into the downstream
    	 // DB Layer code that receives it as an std::string, it fails to compile on the IBM Power machines.
    	 // To avoid that compiler error, we have to do this indirection by passing an std::string variable into
    	 // the DB Layer and then type cast it back to an rstring before returning from here.
         std::string _jsonResponse = "";
    	 SPL::boolean result = db_->runDataStoreCommand(cmdType, httpVerb, baseUrl, apiEndpoint,
    			 queryParams, jsonRequest, _jsonResponse, *dbError_);
         jsonResponse = static_cast<SPL::rstring> (_jsonResponse);
    	 err = dbError_->getErrorCode();
    	 return result;
     }

     SPL::boolean DistributedProcessStore::runDataStoreCommand(SPL::list<SPL::rstring> const & cmdList, SPL::rstring & resultValue, SPL::uint64 & err) {
    	 dbError_->reset();
         // Fix this static cast and then after that remove the code below that copies from SPL::list to std::vector in a loop.
         // It looks like this static_cast will never work because we are trying to cast from 
         // SPL::rstring [derived from C++ base_string] to std::string. They are not in the same inheritance hierarchy.
         // const std::vector<std::string> & myVector = static_cast<const std::vector<std::string> &> (cmdList);
         
         std::vector<std::string> myVector;

         for (SPL::list<SPL::rstring>::const_iterator it = cmdList.begin(); it != cmdList.end(); ++it) {
            myVector.push_back(*it);
         }

    	 SPL::boolean result = db_->runDataStoreCommand(myVector, resultValue, *dbError_);
    	 err = dbError_->getErrorCode();
    	 return result;
     }

     void DistributedProcessStore::base64Encode(SPL::rstring const & str, SPL::rstring & encodedResultStr) {
    	 // If we pass an rstring typed non constant variable reference into the downstream
    	 // DB Layer code that receives it as an std::string, it fails to compile on the IBM Power machines.
    	 // To avoid that compiler error, we have to do this indirection by passing an std::string variable into
    	 // the DB Layer and then type cast it back to an rstring before returning from here.
         std::string _encodedResultStr = "";
         db_->base64_encode(str, _encodedResultStr);
         encodedResultStr = static_cast<SPL::rstring> (_encodedResultStr);
     }

     void DistributedProcessStore::base64Decode(SPL::rstring const & str, SPL::rstring & decodedResultStr) {
    	 // If we pass an rstring typed non constant variable reference into the downstream
    	 // DB Layer code that receives it as an std::string, it fails to compile on the IBM Power machines.
    	 // To avoid that compiler error, we have to do this indirection by passing an std::string variable into
    	 // the DB Layer and then type cast it back to an rstring before returning from here.
    	 //
    	 // In addition, our C++ base64_decode method expects a modifiable std::string in the first argument.
    	 // (You can read the commentary about this topic inside the actual DBLayer implementation.)
    	 // Hence, we have to do static_cast for both the input arguments.
    	 std::string _str = "", _decodedResultStr = "";
    	 _str = static_cast<std::string> (str);
         db_->base64_decode(_str, _decodedResultStr);
         decodedResultStr = static_cast<SPL::rstring> (_decodedResultStr);
     }


     void DistributedProcessStore::persist(SPL::uint64 & err){
         dbError_->reset();
         db_->persist(*dbError_);
         err = dbError_->getErrorCode();
      }

      SPL::boolean DistributedProcessStore::isConnected() {
	dbError_->reset();
	SPL::boolean result = db_->isConnected();
	return result;      
      }

      SPL::boolean DistributedProcessStore::reconnect() {
         // When there is an intermittent disconnection, users can call this
         // method to reestablish connection if needed.
         // Before we do anything let us ensure that the connection is not really
         // broken at this time. This is to avoid reconnecting if user calls
         // this method inadvertently.
	dbError_->reset();
	SPL::boolean result = db_->isConnected();

        if (result == true) {
           // Connection is already active.
           // There is no need to reconnect.
           return(result);
        }

        // Connection is broken at this time. Let us try to reconnect.
        std::string noSqlKvStoreProductName = "";
        std::set<std::string> dbServers;

        // Read the no-sql store product name and the
        // no-sql store server names from the configuration file.
        std::string configFile = (DistributedProcessStore::dpsConfigFile_ == "") ? "etc/no-sql-kv-store-servers.cfg" : DistributedProcessStore::dpsConfigFile_;
        fetchDBConnectionParameters(noSqlKvStoreProductName, dbServers, configFile);
        return(db_->reconnect(dbServers, *dbError_));
      }


} } } } }

