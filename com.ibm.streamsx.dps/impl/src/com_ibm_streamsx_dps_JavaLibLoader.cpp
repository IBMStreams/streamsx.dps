/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2015
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#include "com_ibm_streamsx_dps_impl_DpsHelper.h"
#include <string>       // std::string
#include <iostream>     // std::cout
#include <sstream>      // std::ostringstream
#include <ctype.h>
#include <dlfcn.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

using namespace std;

// ==============================================================================================
// This file has one JNI function to load all the required libraries from within a
// Java primitive operator. This JNI function will be compiled into its own
// shared library (libDpsJavaLibLoader.so) file. That .so file will be loaded inside
// the DPSHelper.java file after which this JNI function will be called from the
// constructor method of the DpsHelper Java class.
//
// [Please refer to the build_dps_helper.sh script available in the impl/java/src/com/ibm/...
//  directory. It explains the required high level JNI development steps.]
// ==============================================================================================
// Load the client libraries for memcached, redis, cassandra, and cloudant so that the Java primitive operators can access
// the C++ APIs buried in these .so libraries via the JNI functions available in our dps .so library.
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsLoadBackEndDbClientLibraries
  (JNIEnv *env, jobject obj, jstring libDir) {
	const char *str = env->GetStringUTFChars(libDir, 0);
	std::string sharedLibDir = std::string(str);
	env->ReleaseStringUTFChars(libDir, str);
	char resultString[500];
	std::string sharedLibFileName = "";
	int32_t loopCnt = 0;
	void* handle = NULL;

	// We will load the shared object (.so) client libraries required to
	// access all our supported NoSQL K/V stores.
	// As you can see, this one is strictly a single iteration loop.
	while(loopCnt++ == 0) {
		sharedLibFileName = sharedLibDir + "/libmemcached.so";
		handle = dlopen(sharedLibFileName.c_str(), RTLD_NOW|RTLD_GLOBAL);

		// On any dlopen error, we must set the result string in such a way that
		// it has the word "failed" somewhere in the sentence.
		// Because, that is what gives an indication to the caller of this method that
		// there was an error while loading one of the required .so libraries.
		if (handle == false) {
			strcpy(resultString, "DpsHelper: dlopen failed for libmemcached.so");
			break;
		}

		sharedLibFileName = sharedLibDir + "/libhiredis.so";
		handle = dlopen(sharedLibFileName.c_str(), RTLD_NOW|RTLD_GLOBAL);

		if (handle == false) {
			strcpy(resultString, "DpsHelper: dlopen failed for libhiredis.so");
			break;
		}

		sharedLibFileName = sharedLibDir + "/libuv.so";
		handle = dlopen(sharedLibFileName.c_str(), RTLD_NOW|RTLD_GLOBAL);

		if (handle == false) {
			strcpy(resultString, "DpsHelper: dlopen failed for libuv.so");
			break;
		}

		sharedLibFileName = sharedLibDir + "/libcassandra.so";
		handle = dlopen(sharedLibFileName.c_str(), RTLD_NOW|RTLD_GLOBAL);

		if (handle == false) {
			strcpy(resultString, "DpsHelper: dlopen failed for libcassandra.so");
			break;
		}

		sharedLibFileName = sharedLibDir + "/libcurl.so";
		handle = dlopen(sharedLibFileName.c_str(), RTLD_NOW|RTLD_GLOBAL);

		if (handle == false) {
			strcpy(resultString, "DpsHelper: dlopen failed for libcurl.so");
			break;
		}

		sharedLibFileName = sharedLibDir + "/libjson-c.so";
		handle = dlopen(sharedLibFileName.c_str(), RTLD_NOW|RTLD_GLOBAL);

		if (handle == false) {
			strcpy(resultString, "DpsHelper: dlopen failed for libjson-c.so");
			break;
		}

		sharedLibFileName = sharedLibDir + "/libbson.so";
		handle = dlopen(sharedLibFileName.c_str(), RTLD_NOW|RTLD_GLOBAL);

		if (handle == false) {
			strcpy(resultString, "DpsHelper: dlopen failed for libbson.so");
			break;
		}

		sharedLibFileName = sharedLibDir + "/libmongoc.so";
		handle = dlopen(sharedLibFileName.c_str(), RTLD_NOW|RTLD_GLOBAL);

		if (handle == false) {
			strcpy(resultString, "DpsHelper: dlopen failed for libmongoc.so");
			break;
		}

		sharedLibFileName = sharedLibDir + "/libcouchbase.so";
		handle = dlopen(sharedLibFileName.c_str(), RTLD_NOW|RTLD_GLOBAL);

		if (handle == false) {
			strcpy(resultString, "DpsHelper: dlopen failed for libcouchbase.so");
			break;
		}

		sharedLibFileName = sharedLibDir + "/libaerospike.so";
		handle = dlopen(sharedLibFileName.c_str(), RTLD_NOW|RTLD_GLOBAL);

		if (handle == false) {
			strcpy(resultString, "DpsHelper: dlopen failed for libaerospike.so");
			break;
		}

		string msg = string("DpsHelper: dlopen successful for libmemcached.so, libhiredis.so, ") +
			string("libuv.so, libcassandra.so, libcurl.so, libjson-c.so, ") +
			string("libbson.so, libmongoc.so, libcouchbase.so, and libaerospike.so.");
		strcpy(resultString, msg.c_str());
	} // End of the while loop.

	return (env->NewStringUTF(resultString));
}
