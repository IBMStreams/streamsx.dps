/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2015
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#include "com_ibm_streamsx_dps_impl_DpsHelper.h"
#include "DistributedProcessStoreWrappers.h"
#include "DistributedLockWrappers.h"
#include <string>       // std::string
#include <iostream>     // std::cout
#include <sstream>      // std::ostringstream
#include <ctype.h>
#include <dlfcn.h>
#include <stdio.h>

using namespace com::ibm::streamsx::store::distributed;
using namespace com::ibm::streamsx::lock::distributed;
using namespace std;
using namespace SPL;

// ==============================================================================================
// We will have separate entry point methods to provide the dps facilities for Java primitive
// operators. Because of the use of JNI, separate wrapper methods are needed with
// JNI specific method signatures.
//
// [Please refer to the build_dps_helper.sh script available in the impl/java/src/com/ibm/...
//  directory. It explains the required high level JNI development steps.]
// ==============================================================================================
JNIEXPORT jlong JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetLastStoreErrorCodeCpp
  (JNIEnv *env, jobject obj) {
	return(dpsGetLastStoreErrorCode());
}

JNIEXPORT jlong JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetLastErrorCodeTTLCpp
  (JNIEnv *env, jobject obj) {
	return(dpsGetLastErrorCodeTTL());
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetLastStoreErrorStringCpp
  (JNIEnv *env, jobject obj) {
	string errorString = dpsGetLastStoreErrorString();
	return (env->NewStringUTF(errorString.c_str()));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetLastErrorStringTTLCpp
  (JNIEnv *env, jobject obj) {
	string errorString = dpsGetLastErrorStringTTL();
	return (env->NewStringUTF(errorString.c_str()));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsCreateStoreCpp
  (JNIEnv *env, jobject obj, jstring name, jstring keySplTypeName, jstring valueSplTypeName) {
   const char *str = env->GetStringUTFChars(name, 0);
   std::string storeName = std::string(str);
   env->ReleaseStringUTFChars(name, str);

   str = env->GetStringUTFChars(keySplTypeName, 0);
   std::string splTypeNameForKey = std::string(str);
   env->ReleaseStringUTFChars(keySplTypeName, str);

   str = env->GetStringUTFChars(valueSplTypeName, 0);
   std::string splTypeNameForValue = std::string(str);
   env->ReleaseStringUTFChars(valueSplTypeName, str);

   SPL::uint64 storeId = 0;
   SPL::uint64 dpsErrorCode = 0;
   storeId = dpsCreateStoreForJava(storeName, splTypeNameForKey, splTypeNameForValue, dpsErrorCode);

   // Result string format: "storeId,errorCode"
   char resultString[260];
   sprintf(resultString, "%ld,%ld", storeId, dpsErrorCode);
   return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsCreateOrGetStoreCpp
  (JNIEnv *env, jobject obj, jstring name, jstring keySplTypeName, jstring valueSplTypeName) {
   const char *str = env->GetStringUTFChars(name, 0);
   std::string storeName = std::string(str);
   env->ReleaseStringUTFChars(name, str);

   str = env->GetStringUTFChars(keySplTypeName, 0);
   std::string splTypeNameForKey = std::string(str);
   env->ReleaseStringUTFChars(keySplTypeName, str);

   str = env->GetStringUTFChars(valueSplTypeName, 0);
   std::string splTypeNameForValue = std::string(str);
   env->ReleaseStringUTFChars(valueSplTypeName, str);

   SPL::uint64 storeId = 0;
   SPL::uint64 dpsErrorCode = 0;
   storeId = dpsCreateOrGetStoreForJava(storeName, splTypeNameForKey, splTypeNameForValue, dpsErrorCode);

   // Result string format: "storeId,errorCode"
   char resultString[260];
   sprintf(resultString, "%ld,%ld", storeId, dpsErrorCode);
   return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsFindStoreCpp
  (JNIEnv *env, jobject obj, jstring name) {
   const char *str = env->GetStringUTFChars(name, 0);
   std::string storeName = std::string(str);
   env->ReleaseStringUTFChars(name, str);

   SPL::uint64 storeId = 0;
   SPL::uint64 dpsErrorCode = 0;
   storeId = dpsFindStore(storeName, dpsErrorCode);

   // Result string format: "storeId,errorCode"
   char resultString[260];
   sprintf(resultString, "%ld,%ld", storeId, dpsErrorCode);
   return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsRemoveStoreCpp
  (JNIEnv *env, jobject obj, jlong store) {
   SPL::uint64 dpsErrorCode = 0;
   SPL::boolean result = dpsRemoveStore(store, dpsErrorCode);

   char booleanResult[40] = "false";

   if (result == true) {
	   strcpy(booleanResult, "true");
   }

   // Result string format: "booleanResult,errorCode"
   char resultString[260];
   sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
   return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsPutCpp
  (JNIEnv *env, jobject obj, jlong store, jobject keyData, jint keySize, jobject valueData, jint valueSize) {
	uint64 dpsErrorCode = 0;
	jbyte* byteBuffer1;
	jbyte* byteBuffer2;
	byteBuffer1 = (jbyte*) env->GetDirectBufferAddress(keyData);
	byteBuffer2 = (jbyte*) env->GetDirectBufferAddress(valueData);

	boolean result = dpsPutForJava(store, (char const *)byteBuffer1, keySize, (unsigned char const *)byteBuffer2, valueSize, dpsErrorCode);

	char booleanResult[40] = "false";

	if (result == true) {
	   strcpy(booleanResult, "true");
	}

	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsPutSafeCpp
  (JNIEnv *env, jobject obj, jlong store, jobject keyData, jint keySize, jobject valueData, jint valueSize) {
	uint64 dpsErrorCode = 0;
	jbyte* byteBuffer1;
	jbyte* byteBuffer2;
	byteBuffer1 = (jbyte*) env->GetDirectBufferAddress(keyData);
	byteBuffer2 = (jbyte*) env->GetDirectBufferAddress(valueData);

	boolean result = dpsPutSafeForJava(store, (char const *)byteBuffer1, keySize, (unsigned char const *)byteBuffer2, valueSize, dpsErrorCode);

	char booleanResult[40] = "false";

	if (result == true) {
	   strcpy(booleanResult, "true");
	}

	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsPutTTLCpp
  (JNIEnv *env, jobject obj, jobject keyData, jint keySize, jobject valueData, jint valueSize, jint ttl) {
	uint64 dpsErrorCode = 0;
	jbyte* byteBuffer1;
	jbyte* byteBuffer2;
	byteBuffer1 = (jbyte*) env->GetDirectBufferAddress(keyData);
	byteBuffer2 = (jbyte*) env->GetDirectBufferAddress(valueData);

	boolean result = dpsPutTTLForJava((char const *)byteBuffer1, keySize, (unsigned char const *)byteBuffer2, valueSize, ttl, dpsErrorCode);

	char booleanResult[40] = "false";

	if (result == true) {
	   strcpy(booleanResult, "true");
	}

	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jobjectArray JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetCpp
  (JNIEnv *env, jobject obj, jlong store, jobject keyData, jint keySize) {
	uint64 dpsErrorCode = 0;
	jbyte* byteBuffer1;
	byteBuffer1 = (jbyte*) env->GetDirectBufferAddress(keyData);
	unsigned char * valueDataBuffer = (unsigned char *)"";
	uint32 valueSize = 0;
	jobject dataItemValue = NULL;
	jobject resultStringObject;
	jobjectArray resultArray;

	boolean result = dpsGetForJava(store, (char const *)byteBuffer1, keySize, valueDataBuffer, valueSize, dpsErrorCode);

	char booleanResult[40] = "false";

	if (result == true) {
	   strcpy(booleanResult, "true");
	   // We have obtained a data item value.
	   // Let us make a Java ByteBuffer object own this block of memory so that we can insert that Java object in the result array that will be returned to the caller.
	   // IMPORTANT: Memory for valueDataBuffer was dynamically allocated in the dps C++ DB layer. That will be freed by
	   // the Java garbage collector at a later time when this ByteBuffer goes out of scope inside the Java caller of this JNI method.
		dataItemValue = (jobject)env->NewDirectByteBuffer(valueDataBuffer, valueSize);
	}

	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	resultStringObject = (jobject)env->NewStringUTF(resultString);
	// Java is not as convenient as C++ in the sense that we can't pass by reference in Java.
	// Hence, we can't return multiple values from a method to the caller.
	// One way to do that in Java is by stuffing the multiple return value items in an object array.
	// Insert the two result items from this JNI method into the result object array.
	jclass objectClass = env->FindClass("java/lang/Object");
	resultArray = env->NewObjectArray(2, objectClass, NULL);
	env->SetObjectArrayElement(resultArray, 0, resultStringObject);
	env->SetObjectArrayElement(resultArray, 1, dataItemValue);
	return(resultArray);
}

JNIEXPORT jobjectArray JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetSafeCpp
  (JNIEnv *env, jobject obj, jlong store, jobject keyData, jint keySize) {
	uint64 dpsErrorCode = 0;
	jbyte* byteBuffer1;
	byteBuffer1 = (jbyte*) env->GetDirectBufferAddress(keyData);
	unsigned char * valueDataBuffer = (unsigned char *)"";
	uint32 valueSize = 0;
	jobject dataItemValue = NULL;
	jobject resultStringObject;
	jobjectArray resultArray;

	boolean result = dpsGetSafeForJava(store, (char const *)byteBuffer1, keySize, valueDataBuffer, valueSize, dpsErrorCode);

	char booleanResult[40] = "false";

	if (result == true) {
	   strcpy(booleanResult, "true");
	   // We have obtained a data item value.
	   // Let us make a Java ByteBuffer object own this block of memory so that we can insert that Java object in the result array that will be returned to the caller.
	   // IMPORTANT: Memory for valueDataBuffer was dynamically allocated in the dps C++ DB layer. That will be freed by
	   // the Java garbage collector at a later time when this ByteBuffer goes out of scope inside the Java caller of this JNI method.
		dataItemValue = (jobject)env->NewDirectByteBuffer(valueDataBuffer, valueSize);
	}

	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	resultStringObject = (jobject)env->NewStringUTF(resultString);
	// Java is not as convenient as C++ in the sense that we can't pass by reference in Java.
	// Hence, we can't return multiple values from a method to the caller.
	// One way to do that in Java is by stuffing the multiple return value items in an object array.
	// Insert the two result items from this JNI method into the result object array.
	jclass objectClass = env->FindClass("java/lang/Object");
	resultArray = env->NewObjectArray(2, objectClass, NULL);
	env->SetObjectArrayElement(resultArray, 0, resultStringObject);
	env->SetObjectArrayElement(resultArray, 1, dataItemValue);
	return(resultArray);
}

JNIEXPORT jobjectArray JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetTTLCpp
  (JNIEnv *env, jobject obj, jobject keyData, jint keySize) {
	uint64 dpsErrorCode = 0;
	jbyte* byteBuffer1;
	byteBuffer1 = (jbyte*) env->GetDirectBufferAddress(keyData);
	unsigned char * valueDataBuffer = (unsigned char *)"";
	uint32 valueSize = 0;
	jobject dataItemValue = NULL;
	jobject resultStringObject;
	jobjectArray resultArray;

	boolean result = dpsGetTTLForJava((char const *)byteBuffer1, keySize, valueDataBuffer, valueSize, dpsErrorCode);

	char booleanResult[40] = "false";

	if (result == true) {
	   strcpy(booleanResult, "true");
	   // We have obtained a data item value.
	   // Let us make a Java ByteBuffer object own this block of memory so that we can insert that Java object in the result array that will be returned to the caller.
	   // IMPORTANT: Memory for valueDataBuffer was dynamically allocated in the dps C++ DB layer. That will be freed by
	   // the Java garbage collector at a later time when this ByteBuffer goes out of scope inside the Java caller of this JNI method.
		dataItemValue = (jobject)env->NewDirectByteBuffer(valueDataBuffer, valueSize);
	}

	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	resultStringObject = (jobject)env->NewStringUTF(resultString);
	// Java is not as convenient as C++ in the sense that we can't pass by reference in Java.
	// Hence, we can't return multiple values from a method to the caller.
	// One way to do that in Java is by stuffing the multiple return value items in an object array.
	// Insert the two result items from this JNI method into the result object array.
	jclass objectClass = env->FindClass("java/lang/Object");
	resultArray = env->NewObjectArray(2, objectClass, NULL);
	env->SetObjectArrayElement(resultArray, 0, resultStringObject);
	env->SetObjectArrayElement(resultArray, 1, dataItemValue);
	return(resultArray);
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsRemoveCpp
  (JNIEnv *env, jobject obj, jlong store, jobject keyData, jint keySize) {
	uint64 dpsErrorCode = 0;
	jbyte* byteBuffer1;
	byteBuffer1 = (jbyte*) env->GetDirectBufferAddress(keyData);

	boolean result = dpsRemoveForJava(store, (char const *)byteBuffer1, keySize, dpsErrorCode);

	char booleanResult[40] = "false";

	if (result == true) {
		strcpy(booleanResult, "true");
	}

	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsRemoveTTLCpp
  (JNIEnv *env, jobject obj, jobject keyData, jint keySize) {
	uint64 dpsErrorCode = 0;
	jbyte* byteBuffer1;
	byteBuffer1 = (jbyte*) env->GetDirectBufferAddress(keyData);

	boolean result = dpsRemoveTTLForJava((char const *)byteBuffer1, keySize, dpsErrorCode);

	char booleanResult[40] = "false";

	if (result == true) {
		strcpy(booleanResult, "true");
	}

	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsHasCpp
  (JNIEnv *env, jobject obj, jlong store, jobject keyData, jint keySize) {
	uint64 dpsErrorCode = 0;
	jbyte* byteBuffer1;
	byteBuffer1 = (jbyte*) env->GetDirectBufferAddress(keyData);

	boolean result = dpsHasForJava(store, (char const *)byteBuffer1, keySize, dpsErrorCode);

	char booleanResult[40] = "false";

	if (result == true) {
		strcpy(booleanResult, "true");
	}

	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsHasTTLCpp
  (JNIEnv *env, jobject obj, jobject keyData, jint keySize) {
	uint64 dpsErrorCode = 0;
	jbyte* byteBuffer1;
	byteBuffer1 = (jbyte*) env->GetDirectBufferAddress(keyData);

	boolean result = dpsHasTTLForJava((char const *)byteBuffer1, keySize, dpsErrorCode);

	char booleanResult[40] = "false";

	if (result == true) {
		strcpy(booleanResult, "true");
	}

	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsClearCpp
  (JNIEnv *env, jobject obj, jlong store) {
	uint64 dpsErrorCode = 0;
	dpsClear(store, dpsErrorCode);

	char booleanResult[40] = "true";
	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsSizeCpp
  (JNIEnv *env, jobject obj, jlong store) {
	uint64 dpsErrorCode = 0;
	uint64 storeSize = dpsSize(store, dpsErrorCode);

	// Result string format: "storeSize,errorCode"
	char resultString[260];
	sprintf(resultString, "%ld,%ld", storeSize, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsBeginIterationCpp
  (JNIEnv *env, jobject obj, jlong store) {
	uint64 dpsErrorCode = 0;
	uint64 iterationHandle = dpsBeginIteration(store, dpsErrorCode);

	// Result string format: "iterationHandle,errorCode"
	char resultString[260];
	sprintf(resultString, "%ld,%ld", iterationHandle, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jobjectArray JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetNextCpp
  (JNIEnv *env, jobject obj, jlong store, jlong iterationHandle) {
	uint64 dpsErrorCode = 0;
	unsigned char * keyDataBuffer = (unsigned char *)"";
	unsigned char * valueDataBuffer = (unsigned char *)"";
	uint32 keySize = 0;
	uint32 valueSize = 0;
	jobject dataItemKey = NULL;
	jobject dataItemValue = NULL;
	jobject resultStringObject;
	jobjectArray resultArray;

	boolean result = dpsGetNextForJava(store, iterationHandle, keyDataBuffer, keySize, valueDataBuffer, valueSize, dpsErrorCode);

	char booleanResult[40] = "false";

	if (result == true) {
	   strcpy(booleanResult, "true");
	   // We have obtained a data item key and a data item value from the store iterator.
	   // Let us make two Java ByteBuffer objects own this block of memory so that we can insert those Java objects in the result array that will be returned to the caller.
	   // IMPORTANT: Memory for keyDataBuffer and for valueDataBuffer was dynamically allocated in the dps C++ DB layer. That will be freed by
	   // the Java garbage collector at a later time when these two ByteBuffer objects go out of scope inside the Java caller of this JNI method.
	   dataItemKey = (jobject)env->NewDirectByteBuffer(keyDataBuffer, keySize);
	   dataItemValue = (jobject)env->NewDirectByteBuffer(valueDataBuffer, valueSize);
	}

	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	resultStringObject = (jobject)env->NewStringUTF(resultString);
	// Java is not as convenient as C++ in the sense that we can't pass by reference in Java.
	// Hence, we can't return multiple values from a method to the caller.
	// One way to do that in Java is by stuffing the multiple return value items in an object array.
	// Insert the three result items from this JNI method into the result object array.
	jclass objectClass = env->FindClass("java/lang/Object");
	resultArray = env->NewObjectArray(3, objectClass, NULL);
	env->SetObjectArrayElement(resultArray, 0, resultStringObject);
	env->SetObjectArrayElement(resultArray, 1, dataItemKey);
	env->SetObjectArrayElement(resultArray, 2, dataItemValue);
	return(resultArray);
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsEndIterationCpp
  (JNIEnv *env, jobject obj, jlong store, jlong iterationHandle) {
	uint64 dpsErrorCode = 0;
	dpsEndIteration(store, iterationHandle, dpsErrorCode);

	char booleanResult[40] = "true";
	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetKeySplTypeNameCpp
  (JNIEnv *env, jobject obj, jlong store) {
	string result = dpsGetSplTypeNameForKey(store);
	return (env->NewStringUTF(result.c_str()));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetValueSplTypeNameCpp
  (JNIEnv *env, jobject obj, jlong store) {
	string result = dpsGetSplTypeNameForValue(store);
	return (env->NewStringUTF(result.c_str()));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetNoSqlDbProductNameCpp
  (JNIEnv *env, jobject obj) {
	string result = dpsGetNoSqlDbProductName();
	return (env->NewStringUTF(result.c_str()));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetDetailsAboutThisMachineCpp
  (JNIEnv *env, jobject obj) {
	SPL::rstring machineName = "", osVersion = "", cpuArchitecture = "";
	dpsGetDetailsAboutThisMachine(machineName, osVersion, cpuArchitecture);
	// Result string format: "machineName,osVersion,cpuArchitecture"
	char resultString[750];
	sprintf(resultString, "%s,%s,%s", machineName.c_str(), osVersion.c_str(), cpuArchitecture.c_str());
	return(env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsRunDataStoreCommandCpp1
  (JNIEnv *env, jobject obj, jstring cmd) {
   const char *str = env->GetStringUTFChars(cmd, 0);
   std::string command = std::string(str);
   env->ReleaseStringUTFChars(cmd, str);

   SPL::uint64 dpsErrorCode = 0;
   boolean result = dpsRunDataStoreCommand(command, dpsErrorCode);

   char booleanResult[40] = "false";

   if (result == true) {
	   strcpy(booleanResult, "true");
   }

   // Result string format: "booleanResult,errorCode"
   char resultString[260];
   sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
   return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsRunDataStoreCommandCpp2
  (JNIEnv *env, jobject obj, jint cmdType, jstring httpVerb, jstring baseUrl,
		  jstring apiEndpoint, jstring queryParams, jstring jsonRequest) {
   const char *str = env->GetStringUTFChars(httpVerb, 0);
   SPL::rstring _httpVerb = std::string(str);
   env->ReleaseStringUTFChars(httpVerb, str);

   str = env->GetStringUTFChars(baseUrl, 0);
   SPL::rstring _baseUrl = std::string(str);
   env->ReleaseStringUTFChars(baseUrl, str);

   str = env->GetStringUTFChars(apiEndpoint, 0);
   SPL::rstring _apiEndpoint = std::string(str);
   env->ReleaseStringUTFChars(apiEndpoint, str);

   str = env->GetStringUTFChars(queryParams, 0);
   SPL::rstring _queryParams = std::string(str);
   env->ReleaseStringUTFChars(queryParams, str);

   str = env->GetStringUTFChars(jsonRequest, 0);
   SPL::rstring _jsonRequest = std::string(str);
   env->ReleaseStringUTFChars(jsonRequest, str);

   SPL::uint64 dpsErrorCode = 0;
   SPL::uint32 _cmdType = cmdType;
   SPL::rstring jsonResponse = "";
   boolean result = dpsRunDataStoreCommand(_cmdType, _httpVerb, _baseUrl,
      _apiEndpoint, _queryParams, _jsonRequest, jsonResponse, dpsErrorCode);

   char booleanResult[40] = "false";

   if (result == true) {
	   strcpy(booleanResult, "true");
   }

   // Result string format: "booleanResult,errorCode,jsonResponse"
   char resultString[256*1024];
   sprintf(resultString, "%s,%ld,%s", booleanResult, dpsErrorCode, jsonResponse.c_str());
   return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsBase64EncodeCpp
  (JNIEnv *env, jobject obj, jstring normalStr) {
	const char *str = env->GetStringUTFChars(normalStr, 0);
	SPL::rstring _str = std::string(str);
	env->ReleaseStringUTFChars(normalStr, str);

	SPL::rstring encodedResultStr = "";
	dpsBase64Encode(_str, encodedResultStr);

	char booleanResult[40] = "true";

	// Result string format: "booleanResult,base64EncodedString"
	char resultString[256*1024]; // 256 KB in size. (Because of the possibility of very large HBase columns.)
	sprintf(resultString, "%s,%s", booleanResult, encodedResultStr.c_str());
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsBase64DecodeCpp
  (JNIEnv *env, jobject obj, jstring base64Str) {
	const char *str = env->GetStringUTFChars(base64Str, 0);
	SPL::rstring _str = std::string(str);
	env->ReleaseStringUTFChars(base64Str, str);

	SPL::rstring decodedResultStr = "";
	dpsBase64Decode(_str, decodedResultStr);

	char booleanResult[40] = "true";

	// Result string format: "booleanResult,base64DecodedString"
	char resultString[256*1024]; // 256 KB in size. (Because of the possibility of very large HBase columns.)
	sprintf(resultString, "%s,%s", booleanResult, decodedResultStr.c_str());
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlCreateOrGetLockCpp
  (JNIEnv *env, jobject obj, jstring name) {
	const char *str = env->GetStringUTFChars(name, 0);
	std::string lockName = std::string(str);
	env->ReleaseStringUTFChars(name, str);

	SPL::uint64 lockId = 0;
	SPL::uint64 dpsErrorCode = 0;
	lockId = dlCreateOrGetLock(lockName, dpsErrorCode);

	// Result string format: "lockId,errorCode"
	char resultString[260];
	sprintf(resultString, "%ld,%ld", lockId, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlRemoveLockCpp
  (JNIEnv *env, jobject obj, jlong lock) {
	SPL::uint64 dpsErrorCode = 0;
	SPL::boolean result = dlRemoveLock(lock, dpsErrorCode);

	char booleanResult[40] = "false";

	if (result == true) {
		strcpy(booleanResult, "true");
	}

	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlAcquireLockCpp__J
  (JNIEnv *env, jobject jobj, jlong lock) {
	SPL::uint64 dpsErrorCode = 0;
	dlAcquireLock(lock, dpsErrorCode);

	char booleanResult[40] = "true";
	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlAcquireLockCpp__JDD
  (JNIEnv *env, jobject obj, jlong lock, jdouble leaseTime, jdouble maxWaitTimeToAcquireLock) {
	SPL::uint64 dpsErrorCode = 0;
	dlAcquireLock(lock, leaseTime, maxWaitTimeToAcquireLock, dpsErrorCode);

	char booleanResult[40] = "true";
	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlReleaseLockCpp
  (JNIEnv *env, jobject obj, jlong lock) {
	SPL::uint64 dpsErrorCode = 0;
	dlReleaseLock(lock, dpsErrorCode);

	char booleanResult[40] = "true";
	// Result string format: "booleanResult,errorCode"
	char resultString[260];
	sprintf(resultString, "%s,%ld", booleanResult, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlGetPidForLockCpp
  (JNIEnv *env, jobject obj, jstring name) {
	const char *str = env->GetStringUTFChars(name, 0);
	std::string lockName = std::string(str);
	env->ReleaseStringUTFChars(name, str);

	SPL::uint32 pid = 0;
	SPL::uint64 dpsErrorCode = 0;
	pid = dlGetPidForLock(lockName, dpsErrorCode);

	// Result string format: "pid,errorCode"
	char resultString[260];
	sprintf(resultString, "%d,%ld", pid, dpsErrorCode);
	return (env->NewStringUTF(resultString));
}

JNIEXPORT jlong JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlGetLastDistributedLockErrorCodeCpp
  (JNIEnv *env, jobject obj) {
	return(dlGetLastDistributedLockErrorCode());
}

JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlGetLastDistributedLockErrorStringCpp
  (JNIEnv *env, jobject obj) {
	string errorString = dlGetLastDistributedLockErrorString();
	return (env->NewStringUTF(errorString.c_str()));
}
