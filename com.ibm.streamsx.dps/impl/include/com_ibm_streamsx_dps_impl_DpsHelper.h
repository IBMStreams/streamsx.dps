/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_ibm_streamsx_dps_impl_DpsHelper */

#ifndef _Included_com_ibm_streamsx_dps_impl_DpsHelper
#define _Included_com_ibm_streamsx_dps_impl_DpsHelper
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsLoadBackEndDbClientLibraries
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsLoadBackEndDbClientLibraries
  (JNIEnv *, jobject, jstring);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsGetLastStoreErrorCodeCpp
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetLastStoreErrorCodeCpp
  (JNIEnv *, jobject);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsGetLastErrorCodeTTLCpp
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetLastErrorCodeTTLCpp
  (JNIEnv *, jobject);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsGetLastStoreErrorStringCpp
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetLastStoreErrorStringCpp
  (JNIEnv *, jobject);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsGetLastErrorStringTTLCpp
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetLastErrorStringTTLCpp
  (JNIEnv *, jobject);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsCreateStoreCpp
 * Signature: (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsCreateStoreCpp
  (JNIEnv *, jobject, jstring, jstring, jstring);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsCreateOrGetStoreCpp
 * Signature: (Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsCreateOrGetStoreCpp
  (JNIEnv *, jobject, jstring, jstring, jstring);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsFindStoreCpp
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsFindStoreCpp
  (JNIEnv *, jobject, jstring);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsRemoveStoreCpp
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsRemoveStoreCpp
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsPutCpp
 * Signature: (JLjava/nio/ByteBuffer;ILjava/nio/ByteBuffer;I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsPutCpp
  (JNIEnv *, jobject, jlong, jobject, jint, jobject, jint);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsPutSafeCpp
 * Signature: (JLjava/nio/ByteBuffer;ILjava/nio/ByteBuffer;I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsPutSafeCpp
  (JNIEnv *, jobject, jlong, jobject, jint, jobject, jint);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsPutTTLCpp
 * Signature: (Ljava/nio/ByteBuffer;ILjava/nio/ByteBuffer;II)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsPutTTLCpp
  (JNIEnv *, jobject, jobject, jint, jobject, jint, jint);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsGetCpp
 * Signature: (JLjava/nio/ByteBuffer;I)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetCpp
  (JNIEnv *, jobject, jlong, jobject, jint);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsGetSafeCpp
 * Signature: (JLjava/nio/ByteBuffer;I)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetSafeCpp
  (JNIEnv *, jobject, jlong, jobject, jint);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsGetTTLCpp
 * Signature: (Ljava/nio/ByteBuffer;I)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetTTLCpp
  (JNIEnv *, jobject, jobject, jint);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsRemoveCpp
 * Signature: (JLjava/nio/ByteBuffer;I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsRemoveCpp
  (JNIEnv *, jobject, jlong, jobject, jint);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsRemoveTTLCpp
 * Signature: (Ljava/nio/ByteBuffer;I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsRemoveTTLCpp
  (JNIEnv *, jobject, jobject, jint);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsHasCpp
 * Signature: (JLjava/nio/ByteBuffer;I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsHasCpp
  (JNIEnv *, jobject, jlong, jobject, jint);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsHasTTLCpp
 * Signature: (Ljava/nio/ByteBuffer;I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsHasTTLCpp
  (JNIEnv *, jobject, jobject, jint);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsClearCpp
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsClearCpp
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsSizeCpp
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsSizeCpp
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsBeginIterationCpp
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsBeginIterationCpp
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsGetNextCpp
 * Signature: (JJ)[Ljava/lang/Object;
 */
JNIEXPORT jobjectArray JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetNextCpp
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsEndIterationCpp
 * Signature: (JJ)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsEndIterationCpp
  (JNIEnv *, jobject, jlong, jlong);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsGetKeySplTypeNameCpp
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetKeySplTypeNameCpp
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsGetValueSplTypeNameCpp
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetValueSplTypeNameCpp
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsGetNoSqlDbProductNameCpp
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetNoSqlDbProductNameCpp
  (JNIEnv *, jobject);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsGetDetailsAboutThisMachineCpp
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsGetDetailsAboutThisMachineCpp
  (JNIEnv *, jobject);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsRunDataStoreCommandCpp1
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsRunDataStoreCommandCpp1
  (JNIEnv *, jobject, jstring);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsRunDataStoreCommandCpp2
 * Signature: (ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsRunDataStoreCommandCpp2
  (JNIEnv *, jobject, jint, jstring, jstring, jstring, jstring, jstring);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsBase64EncodeCpp
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsBase64EncodeCpp
  (JNIEnv *, jobject, jstring);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsBase64DecodeCpp
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsBase64DecodeCpp
  (JNIEnv *, jobject, jstring);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dpsSetConfigFileCpp
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dpsSetConfigFileCpp
  (JNIEnv *, jobject, jstring);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dlCreateOrGetLockCpp
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlCreateOrGetLockCpp
  (JNIEnv *, jobject, jstring);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dlRemoveLockCpp
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlRemoveLockCpp
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dlAcquireLockCpp
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlAcquireLockCpp__J
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dlAcquireLockCpp
 * Signature: (JDD)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlAcquireLockCpp__JDD
  (JNIEnv *, jobject, jlong, jdouble, jdouble);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dlReleaseLockCpp
 * Signature: (J)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlReleaseLockCpp
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dlGetPidForLockCpp
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlGetPidForLockCpp
  (JNIEnv *, jobject, jstring);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dlGetLastDistributedLockErrorCodeCpp
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlGetLastDistributedLockErrorCodeCpp
  (JNIEnv *, jobject);

/*
 * Class:     com_ibm_streamsx_dps_impl_DpsHelper
 * Method:    dlGetLastDistributedLockErrorStringCpp
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_com_ibm_streamsx_dps_impl_DpsHelper_dlGetLastDistributedLockErrorStringCpp
  (JNIEnv *, jobject);

#ifdef __cplusplus
}
#endif
#endif
