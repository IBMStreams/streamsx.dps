/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2015
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
/*
==========================================================================================
This Java class provides helper methods to access the dps (Distributed Process Store)
add-on facility to share data with other Java and non-Java operators.

Methods available in this class can be invoked from within any Java primitive operators.
Behind the scenes, all the helper methods defined below will use JNI to execute the
equivalent C++ dps APIs. 

What is a good way to use the dps functions inside a Java primitive operator?

Streams Java primitive operator developers can simply add the location of the DpsHelper
class or the jar file (com.ibm.streamsx.dps/impl/java/bin) to their Java operator model XML file as
a library dependency. After that, any dps function can be called from within the
Java primitive operator code. 

In order for this to work, you must ensure that these two Linux environment
variables are set correctly in your shell environment :  $STREAMS_INSTALL and $JAVA_HOME
==========================================================================================
*/

package com.ibm.streamsx.dps.impl;


import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.ibm.streams.operator.StreamSchema;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.Type;
import com.ibm.streams.operator.encoding.BinaryEncoding;
import com.ibm.streams.operator.meta.TupleType;
import com.ibm.streams.operator.types.*;

public class DpsHelper {	
	// Following are the declarations for the distributed process store related JNI methods.
	private native long dpsGetLastStoreErrorCodeCpp();
	private native long dpsGetLastErrorCodeTTLCpp();
	private native String dpsGetLastStoreErrorStringCpp();
	private native String dpsGetLastErrorStringTTLCpp();
	private native String dpsCreateStoreCpp(String name, String keySplTypeName, String valueSplTypeName);
	private native String dpsCreateOrGetStoreCpp(String name, String keySplTypeName, String valueSplTypeName);
	private native String dpsFindStoreCpp(String name);
	private native String dpsRemoveStoreCpp(long store);
	private native String dpsPutCpp(long store, ByteBuffer keyData, int keySize, ByteBuffer valueData, int valueSize);
	private native String dpsPutSafeCpp(long store, ByteBuffer keyData, int keySize, ByteBuffer valueData, int valueSize);
	private native String dpsPutTTLCpp(ByteBuffer keyData, int keySize, ByteBuffer valueData, int valueSize, int ttl, boolean encodeKey, boolean encodeValue);
	private native Object[] dpsGetCpp(long store, ByteBuffer keyData, int keySize);
	private native Object[] dpsGetSafeCpp(long store, ByteBuffer keyData, int keySize);
	private native Object[] dpsGetTTLCpp(ByteBuffer keyData, int keySize, boolean encodeKey, boolean encodeValue);
        private native void dpsFreeDirectBufferMemoryCpp(ByteBuffer buffer);
	private native String dpsRemoveCpp(long store, ByteBuffer keyData, int keySize);
	private native String dpsRemoveTTLCpp(ByteBuffer keyData, int keySize, boolean encodeKey);
	private native String dpsHasCpp(long store, ByteBuffer keyData, int keySize);
	private native String dpsHasTTLCpp(ByteBuffer keyData, int keySize, boolean encodeKey);
	private native String dpsClearCpp(long store);
	private native String dpsSizeCpp(long store);
	private native String dpsBeginIterationCpp(long store);
	private native Object[] dpsGetNextCpp(long store, long iterationHandle);
	private native String dpsEndIterationCpp(long store, long iterationHandle);
	private native String dpsGetKeySplTypeNameCpp(long store);
	private native String dpsGetValueSplTypeNameCpp(long store);
	private native String dpsGetNoSqlDbProductNameCpp();
	private native String dpsGetDetailsAboutThisMachineCpp();
	private native String dpsRunDataStoreCommandCpp1(String cmd);
	private native String dpsRunDataStoreCommandCpp2(int cmdType, String httpVerb,
		String baseUrl, String apiEndpoint, String queryParams, String jsonRequest);
	private native String dpsRunDataStoreCommandCpp3(ByteBuffer cmdList, int cmdListSize);
	private native String dpsBase64EncodeCpp(String str);
	private native String dpsBase64DecodeCpp(String str);
	private native String dpsSetConfigFileCpp(String dpsConfigFile);
        private native String dpsIsConnectedCpp();
        private native String dpsReconnectCpp();
	//
	// JNI methods related to the distributed locks are declared below.
	//
	private native String dlCreateOrGetLockCpp(String name);
	private native String dlRemoveLockCpp(long lock);
	private native String dlAcquireLockCpp(long lock);
	private native String dlAcquireLockCpp(long lock, double leaseTime, double maxWaitTimeToAquireLock);
	private native String dlReleaseLockCpp(long lock);
	private native String dlGetPidForLockCpp(String name);
	private native long dlGetLastDistributedLockErrorCodeCpp();
	private native String dlGetLastDistributedLockErrorStringCpp();

	
	
		
	public DpsHelper(String dpsToolkitHome) throws Exception {
		// If users get a runtime exception while using the DPS APIs from a fused set of Java operators, they must first try
		// by adding the @SharedLoader(true) annotation to their Java operator and see if that eliminates that
		// particular class loader Java runtime exception. 
		// 
		// If that doesn't solve their runtime exception, following workaround can be attempted.
		// Users can set the following boolean variable to true and then rebuild the dps-helper.jar file.
		// After that they can try it and see if the workaround shown below fixes their class loader runtime exception problem.
	
		boolean handleJavaOperatorsFusedCondition = false;		

		boolean dpsLibLoaded = false;
		String dpsLibName = dpsToolkitHome + "/impl/lib/libDistributedProcessStoreLib.so";
		
		if (handleJavaOperatorsFusedCondition == true) {
	    // If multiple Java operators use the @SharedLoader(true) annotation, then those operators will use a single class loader.
	        // In that case, all those operators will invoke this constructor method only once thereby loading the DPS .so library
	        // only once. [@SharedLoader(true) will work only among operators with exactly the same @Libraries annotation. I also
	        // noticed it to be not working correctly in certain Linux VM environments.]
	        // If those operators don't use the @SharedLoader(true) annotation and if they are fused into a single PE or compiled into a
	        // standalone application, then they each will have their own class loader loading the same DPS .so file within a
	        // single process and that will lead to a runtime exception (UnSatisfiedLink error). We must avoid this condition where
	        // multiple class loaders attempting to load the DPS .so file within a single process (fused or standalone).
	        // The problem here is the filename of that .so file once loaded already in a given process will trigger that
	        // class loader exception claiming that a library with that particular file name is already loaded.
	        // One workaround we can think of is to make a copy of that .so library in different names for each call into this
	        // Java constructor function (where were are now) and load it using a unique file name. An obvious disadvantage of 
	        // this approach is that there will be many copies of that .so library file until the application is completely stopped.
	        // In addition, this may also use up additional memory because of multiple copies of that same .so library getting loaded.
	        // If this is agreeable to you, you can set the handleJavaOperatorsFusedCondition variable to true and try this workaround.
            // Get the current time in millis.

			Path tmpLib = Files.createTempFile("DPSLib", ".so");
             tmpLib.toFile().deleteOnExit();
             String tempDpsLibName=  tmpLib.toAbsolutePath().toString();
            
            try {
                Files.copy(Paths.get(dpsLibName), Paths.get(tempDpsLibName), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            } catch (Exception ex) {
                System.out.println("DpsHelper: Error in copying the DPS .so file. Aborting now...");
                throw(ex);
            }
            
            System.load(tempDpsLibName);
            dpsLibLoaded = true;
            
            // Delete the file we created above.
		} else {
            // This is the only block of code we had before adding the if-else block here on Apr/07/2015 as a 
            // potential workaround for the class loader runtime exception when DPS APIs are called from a fused group of
            // Java operators. The following two lines are sufficient when the DPS client Java operators are not fused.
            System.load(dpsLibName);			
            dpsLibLoaded = true;
		}
		
		// Check if the dps .so library was loaded successfully in the previous code block.
		if (dpsLibLoaded == false) {
            Exception ex = new Exception("DpsHelper: Error in loading the DPS library from " + dpsLibName + ". Aborting now...");
            System.out.println("DpsHelper: Error in loading the DPS library. Aborting now...");
            throw(ex);
		}
	}
	
	
	// This private method encodes the passed argument types in NBF (Native Bytebuffer Format).
	// It will return the end result i.e. one or two or no byte buffers in an object array as 
	// two separate elements. It will also return the final encoded size of those two buffers.
	// These byte buffers will hold the serialized bits of the key and/or value.
	// As you can see, it uses Java generics for type genericity. This method accepts diverse set of data types
	// as arguments and then serializes them to NBF. (Native Bytebuffer Format and not to be confused with Network Byteorder Format).
	//
	// It returns an Object array with the following 4 fields in it.
	// objArray[0] carries a ByteBuffer object holding the NBF encoded bits of the key
	// objArray[1] carries an Integer object to indicate the actual encoded size for the key
	// objArray[2] carries a ByteBuffer object holding the NBF encoded bits of the value
	// objArray[3] carries an Integer object to indicate the actual encoded size for the value
	private <T1, T2> Object[] nbfEncodeKeyAndValue(T1 key, T2 value, String keySplTypeName, String valueSplTypeName) throws Exception {
		// Create an object array to return to the caller and initialize all its elements with null.
		Object[] byteBufferObjectArray = new Object[4];
		Arrays.fill(byteBufferObjectArray, null);
		
		// In some cases, caller may want to encode key or value or both, but never none of it.
		if ((key == null) && (value == null)) {
			return(byteBufferObjectArray);
		} 
		
		if (key != null) {
			StreamSchema ss1 = null;
			Tuple ssTuple1 = null;
			
			// If the key passed to this method is already made of a tuple type, we can directly use that tuple object here.
			// Do the following "on the fly tuple creation" only for non-tuple key types.
			if (keySplTypeName.startsWith("tuple") == true) {
				ssTuple1 = (Tuple)key;
				ss1 = ssTuple1.getStreamSchema();
			} else {		
			    // Create a new tuple on the fly using the SPL type declaration syntax for the data item key.
				TupleType tt1 = Type.Factory.getTupleType("tuple<" + keySplTypeName + " x>");
		        // Get a schema for the newly created tuple type above.
		        ss1 = tt1.getTupleSchema();
		        // Get a map to fill our new tuple type contents.
		        Map<String, Object> attrMap1 = new HashMap<String, Object>();        
		        // Populate the tuple attributes with correct values.
				attrMap1.put("x", key);
				// Using the attribute map created above, create a new concrete tuple.
				ssTuple1 = ss1.getTuple(attrMap1);
			}
			
			// System.out.println("New Key Tuple=" + ssTuple1);  //<--- Debug print
			// Convert the tuple into a blob. 
	        BinaryEncoding be1 = ss1.newNativeBinaryEncoding();
	        // Get the maximum number of bytes required to encode our tuple.
	        long requiredKeyBufferSize = be1.getEncodedSize(ssTuple1);
	        ByteBuffer byteBuffer1 = ByteBuffer.allocateDirect((int)requiredKeyBufferSize);
	        // System.out.println("Required Key buffer size=" + requiredKeyBufferSize);  //<--- Debug print
	        // System.out.println("Key buffer remaining before encoding=" + byteBuffer1.remaining());  //<--- Debug print
	        // Encode our tuple into a blob now.
	        be1.encodeTuple(ssTuple1, byteBuffer1);
	        // System.out.println("Key buffer remaining after encoding=" + byteBuffer1.remaining());  //<--- Debug print
	        int keyBufferSizeAfterEncoding = byteBuffer1.position();
	        // System.out.println("Size of key buffer after encoding=" + keyBufferSizeAfterEncoding);    //<--- Debug print
	        // Set the buffer to its beginning.
	        byteBuffer1.rewind();
	        // System.out.println("Encoded key buffer contents=" + byteBuffer1);  //<--- Debug print
	        byteBufferObjectArray[0] = byteBuffer1;
	        byteBufferObjectArray[1] = new Integer(keyBufferSizeAfterEncoding);
		}
		
		if (value != null) {
			StreamSchema ss2 = null;
			Tuple ssTuple2 = null;
			
			// If the data item value passed to this method is already made of a tuple type, we can directly use that tuple object here.
			// Do the following "on the fly tuple creation" only for non-tuple value types.
			if (valueSplTypeName.startsWith("tuple")) {
				ssTuple2 = (Tuple)value;
				ss2 = ssTuple2.getStreamSchema();		
			} else {
		        // Create a new tuple on the fly using the SPL type declaration syntax for the data item value.
		        TupleType tt2 = Type.Factory.getTupleType("tuple<" + valueSplTypeName + " x>");
		        // Get a schema for the newly created tuple type above.
		        ss2 = tt2.getTupleSchema();
		        // Get a map to fill our new tuple type contents.
		        Map<String, Object> attrMap2 = new HashMap<String, Object>();        
		        // Populate the tuple attributes with correct values.
				attrMap2.put("x", value);
				// Using the attribute map created above, create a new concrete tuple.
				ssTuple2 = ss2.getTuple(attrMap2);
			}
			
			// System.out.println("New Value Tuple=" + ssTuple2);  //<--- Debug print
			// Convert the tuple into a blob. 
	        BinaryEncoding be2 = ss2.newNativeBinaryEncoding();
	        // Get the maximum number of bytes required to encode our tuple.
	        long requiredValueBufferSize = be2.getEncodedSize(ssTuple2);
	        ByteBuffer byteBuffer2 = ByteBuffer.allocateDirect((int)requiredValueBufferSize);
	        // System.out.println("Required Value buffer size=" + requiredValueBufferSize);  //<--- Debug print
	        // System.out.println("Value buffer remaining before encoding=" + byteBuffer2.remaining());  //<--- Debug print
	        // Encode our tuple into a blob now.
	        be2.encodeTuple(ssTuple2, byteBuffer2);
	        // System.out.println("Value buffer remaining after encoding=" + byteBuffer2.remaining());  //<--- Debug print
	        int valueBufferSizeAfterEncoding = byteBuffer2.position();
	        // System.out.println("Size of value buffer after encoding=" + valueBufferSizeAfterEncoding);    //<--- Debug print
	        // Set the buffer to its beginning.
	        byteBuffer2.rewind();
	        // System.out.println("Encoded value buffer contents=" + byteBuffer2);  //<--- Debug print
	        byteBufferObjectArray[2] = byteBuffer2;
	        byteBufferObjectArray[3] = new Integer(valueBufferSizeAfterEncoding);
		}
		
		return(byteBufferObjectArray);
	}

        // Set the user specified DPS configuration file name.
        public String dpsSetConfigFile(String dpsConfigFileName) {
               String result = dpsSetConfigFileCpp(dpsConfigFileName);
               return(result);
        }
        
	
	// Get the error code for the most recently performed dps activity.
	public long dpsGetLastStoreErrorCode() {
		long error = dpsGetLastStoreErrorCodeCpp();
		return(error);
	}
	
	// Get the error code for the most recently performed TTL based dps activity.
	public long dpsGetLastErrorCodeTTL() {
		long error = dpsGetLastErrorCodeTTLCpp();
		return(error);
	}	

	// Get the error string for the most recently performed dps activity.
	public String dpsGetLastStoreErrorString() {
		String errorString = dpsGetLastStoreErrorStringCpp();
		return(errorString);
	}
	
	// Get the error string for the most recently performed TTL based dps activity.
	public String dpsGetLastErrorStringTTL() {
		String errorString = dpsGetLastErrorStringTTLCpp();
		return(errorString);
	}	
	
	// Create a new store.
	public long dpsCreateStore(String name, String keySplTypeName, String valueSplTypeName, long[] err) {
		String result = dpsCreateStoreCpp(name, keySplTypeName, valueSplTypeName);
		// Parse the result string [Format: "storeId,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		long storeId = scanner.nextLong();
		err[0] = scanner.nextLong();
		scanner.close();
		return(storeId);
	}
	
	// Create a new store or get a store if it already exists.
	public long dpsCreateOrGetStore(String name, String keySplTypeName, String valueSplTypeName, long[] err) {
		String result = dpsCreateOrGetStoreCpp(name, keySplTypeName, valueSplTypeName);
		// Parse the result string [Format: "storeId,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		long storeId = scanner.nextLong();
		err[0] = scanner.nextLong();
		scanner.close();
		return(storeId);
	}
	
	// Find a store if it exists.
	public long dpsFindStore(String name, long[] err) {
		String result = dpsFindStoreCpp(name);
		// Parse the result string [Format: "storeId,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		long storeId = scanner.nextLong();
		err[0] = scanner.nextLong();
		scanner.close();
		return(storeId);
	}
	
	// Get the spl type name for a given store's key.
	public String dpsGetKeySplTypeName(long store) {
		return(dpsGetKeySplTypeNameCpp(store));
	}

	// Get the spl type name for a given store's value.
	public String dpsGetValueSplTypeName(long store) {
		return(dpsGetValueSplTypeNameCpp(store));
	}
	
	// Get the name of the NoSQL DB product being used now.
	public String dpsGetNoSqlDbProductName() {
		return(dpsGetNoSqlDbProductNameCpp());
	}
	
	// Get the details of the machine where this operator is running.
	public String[] dpsGetDetailsAboutThisMachine() {
		String result = dpsGetDetailsAboutThisMachineCpp();
		String[] resultArray = new String[3];
		// Our result coming back from the JNI layer will be in this format:
		// "<Machine_Name>,<OS_Version>,<CPU_Architecture>"
		// Parse the result string: [Format: "machineName,osVersion,cpuArchitecture"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		// Get the machine name.
		resultArray[0] = scanner.next();
		// Get the OS version.
		resultArray[1] = scanner.next();
		// Get the CPU architecture.
		resultArray[2] = scanner.next();
		scanner.close();
		return(resultArray);
	}
	
	// If users want to execute simple arbitrary back-end data store (fire and forget one way calls)
	// native commands, this API can be used. This covers any Redis or Cassandra(CQL)
	// native commands that don't have to fetch and return K/V pairs or return size of the db etc.
	// (Insert and Delete are the more suitable ones here. However, key and value can only have string types.)
	// User must ensure that his/her command string is syntactically correct according to the
	// rules of the back-end data store you configured. DPS logic will not do the syntax checking.
	//
	// We will simply take your command string and run it. So, be sure of what
	// command you are sending here.				
	//
	public boolean dpsRunDataStoreCommand(String cmd, long[] err) {
		String result = dpsRunDataStoreCommandCpp1(cmd);
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();		
		return(booleanResult);		
	}

	/*
    If users want to execute arbitrary back-end data store two way
    native commands, this API can be used. This is a variation of the previous API with
    overloaded function arguments. As of Nov/2014, this API is supported in the dps toolkit only
    when Cloudant NoSQL DB is used as a back-end data store. It covers any Cloudant HTTP/JSON based
    native commands that can perform both database and document related Cloudant APIs that are very
    well documented for reference on the web.
    */	
	public boolean dpsRunDataStoreCommand(int cmdType, String httpVerb,
			String baseUrl, String apiEndpoint, String queryParams, String jsonRequest, String[] jsonResponse, long[] httpResponseCode) {
		String result = dpsRunDataStoreCommandCpp2(cmdType, httpVerb, baseUrl, apiEndpoint, queryParams, jsonRequest);
		// Parse the result string [Format: "booleanResult,errorCode,jsonResponse"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		httpResponseCode[0] = scanner.nextLong();
		jsonResponse[0] = "";
		int tokenCnt = 0;
		
		// We have to assemble rest of all the available tokens to form our JSON response.
		while(scanner.hasNext() == true) {
			if (tokenCnt > 0) {
				// Add the comma back into the response string if we have more than one token.
				jsonResponse[0] += ",";				
			}

			jsonResponse[0] += scanner.next();
			tokenCnt++;
		}
		
		scanner.close();		
		return(booleanResult);		
	}

        /// If users want to send any valid Redis command to the Redis server made up as individual parts,
        /// this API can be used. This will work only with Redis. Users simply have to split their
        /// valid Redis command into individual parts that appear between spaces and pass them in 
        /// exacly in that order via a list<rstring>. DPS back-end code will put them together 
        /// correctly before executing the command on a configured Redis server. This API will also
        /// return the resulting value from executing any given Redis command as a string. It is upto
        /// the caller to interpret the Redis returned value and make sense out of it.
        /// In essence, it is a two way Redis command which is very diffferent from the other plain
        /// API that is explained above. [NOTE: If you have to deal with storing or fetching 
        /// non-string complex Streams data types, you can't use this API. Instead, use the other
        /// DPS put/get/remove/has DPS APIs.]
        public boolean dpsRunDataStoreCommand(java.util.List<RString> cmdList, String[] resultString) throws Exception {
                // We have to serialize the cmdList before calling the JNI C code.
                String cmdListSplTypeName = "list<rstring>";
                String otherSplTypeName = "dummy";
		Object[] byteBufferArray = nbfEncodeKeyAndValue(cmdList, null, cmdListSplTypeName, otherSplTypeName);
		
		// We need to have the cmdList serialized properly. If not, throw an exception.
		if ((byteBufferArray[0] == null) || (byteBufferArray[1] == null)) {
			// Something went seriously wrong.
			throw new Exception("dpsRunDataStoreCommand: Unable to serialize the command list.");
		}
		
		String result = dpsRunDataStoreCommandCpp3((ByteBuffer)byteBufferArray[0], ((Integer)byteBufferArray[1]).intValue());
		// Parse the result string [Format: "booleanResult,errorCode,redisResultString"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		long errorCode = scanner.nextLong();
		int tokenCnt = 0;
                resultString[0] = "";
		
		// We have to assemble rest of all the available tokens to form our JSON response.
		while(scanner.hasNext() == true) {
			if (tokenCnt > 0) {
				// Add the comma back into the response string if we have more than one token.
				resultString[0] += ",";				
			}

			resultString[0] += scanner.next();
			tokenCnt++;
		}
		
		scanner.close();		
	        return(booleanResult);
        }
	
	// Base64 encode the given string.
	public String dpsBase64Encode(String str) {
		String result = dpsBase64EncodeCpp(str);
		// Parse the result string [Format: "booleanResult,base64EncodedString"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		// For this API, it will always return a value of true. (We can ignore it.)
		scanner.nextBoolean();
		String encodedResultString = scanner.next();
		scanner.close();		
		return(encodedResultString);				
	}

	// Base64 decode the given string.
	public String dpsBase64Decode(String str) {
		String result = dpsBase64DecodeCpp(str);
		// Parse the result string [Format: "booleanResult,base64DecodedString"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		// For this API, it will always return a value of true. (We can ignore it.)
		scanner.nextBoolean();
		String decodedResultString = scanner.next();
		scanner.close();		
		return(decodedResultString);				
	}	
	
	// Remove an existing store.
	public boolean dpsRemoveStore(long store, long[] err) {
		String result = dpsRemoveStoreCpp(store);
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		return(booleanResult);		
	}
	
	// Put data item in a store. (It is a type generic method that can take any key type and any value type.)
	// It is a special faster version of dpsPut operation that doesn't do any safety checks.
	public <T1, T2> boolean dpsPut(long store, T1 key, T2 value, String keySplTypeName, String valueSplTypeName, long[] err) throws Exception {
		Object[] byteBufferArray = nbfEncodeKeyAndValue(key, value, keySplTypeName, valueSplTypeName);
		
		// We need to have both the key and value serialized properly. If not, throw an exception.
		if ((byteBufferArray[0] == null) || (byteBufferArray[1] == null) ||
			(byteBufferArray[2] == null) || (byteBufferArray[3] == null)) {
			// Something went seriously wrong.
			throw new Exception("dpsPut: Unable to serialize the key and the value.");
		}
		
        String result = dpsPutCpp(store, (ByteBuffer)byteBufferArray[0], ((Integer)byteBufferArray[1]).intValue(),
        	(ByteBuffer)byteBufferArray[2], ((Integer)byteBufferArray[3]).intValue());
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();		
		return(booleanResult);
	}	

	// Put data item in a store. (It is a type generic method that can take any key type and any value type.)
	// It is a special (slower) version of dpsPut operation that does all kinds of safety checks.
	public <T1, T2> boolean dpsPutSafe(long store, T1 key, T2 value, String keySplTypeName, String valueSplTypeName, long[] err) throws Exception {
		Object[] byteBufferArray = nbfEncodeKeyAndValue(key, value, keySplTypeName, valueSplTypeName);
		
		// We need to have both the key and value serialized properly. If not, throw an exception.
		if ((byteBufferArray[0] == null) || (byteBufferArray[1] == null) ||
			(byteBufferArray[2] == null) || (byteBufferArray[3] == null)) {
			// Something went seriously wrong.
			throw new Exception("dpsPutSafe: Unable to serialize the key and the value.");
		}
		
        String result = dpsPutSafeCpp(store, (ByteBuffer)byteBufferArray[0], ((Integer)byteBufferArray[1]).intValue(),
        	(ByteBuffer)byteBufferArray[2], ((Integer)byteBufferArray[3]).intValue());
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();		
		return(booleanResult);
	}	

	// Put a data item with TTL (Time To Live in seconds) into the global area of the back-end data store. 
	// This function doesn't need an user created store to perform the put operation since the data item will be
	// stored in a flat memory space inside the chosen back-end store infrastructure.
	// (It is a type generic method that can take any key type and any value type.)
	public <T1, T2> boolean dpsPutTTL(T1 key, T2 value, int ttl, String keySplTypeName, String valueSplTypeName, long[] err, int[] storedKeyValueSize, boolean encodeKey, boolean encodeValue) throws Exception {
		Object[] byteBufferArray = nbfEncodeKeyAndValue(key, value, keySplTypeName, valueSplTypeName);
		
		// We need to have both the key and value serialized properly. If not, throw an exception.
		if ((byteBufferArray[0] == null) || (byteBufferArray[1] == null) ||
			(byteBufferArray[2] == null) || (byteBufferArray[3] == null)) {
			// Something went seriously wrong.
			throw new Exception("dpsPutTTL: Unable to serialize the key and the value.");
		}
		
        String result = dpsPutTTLCpp((ByteBuffer)byteBufferArray[0], ((Integer)byteBufferArray[1]).intValue(),
        	(ByteBuffer)byteBufferArray[2], ((Integer)byteBufferArray[3]).intValue(), ttl, encodeKey, encodeValue);
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();	

                // This block of code to return the stored key and value size was added on May/16/2017.
                // Ensure that the caller passed an int[] array with a size of 2.
                if (err[0] == 0 && storedKeyValueSize.length >= 2) {
                   if (encodeKey == true) {
                      storedKeyValueSize[0] = ((Integer)byteBufferArray[1]).intValue();
                   } else {
                      // Stored as a plain string.
                      // In the NBF format, very first byte indicates the length of the key data that follows (if the key data is less than 128 characters).
                      // In the NBF format, 5 bytes at the beginning indicate the length of the key data that follows (for key data >= 128 characters).
                      if (((ByteBuffer)byteBufferArray[0]).get(0) < 0x80) {
                         // Skip the first length byte.
                         storedKeyValueSize[0] = ((Integer)byteBufferArray[1]).intValue() - 1;
                      } else {
                        // Skip the five bytes at the beginning that represent the length of the key data.
                        storedKeyValueSize[0] = ((Integer)byteBufferArray[1]).intValue() - 5;                      
                      }
                   }

                   if (encodeValue == true) {
                      storedKeyValueSize[1] = ((Integer)byteBufferArray[3]).intValue();
                   } else {
                      // Stored as a plain string.              
                      if (((ByteBuffer)byteBufferArray[2]).get(0) < 0x80) {
                         // Skip the first length byte.
                         storedKeyValueSize[1] = ((Integer)byteBufferArray[3]).intValue() - 1;
                      } else {
                        // Skip the five bytes at the beginning that represent the length of the key data.
                        storedKeyValueSize[1] = ((Integer)byteBufferArray[3]).intValue() - 5;                      
                      }
                   }
                }
	
		return(booleanResult);
	}	
		
	// Get data item from a store. (It is a type generic method that can take any key type and any value type.)
	// It is a special faster version of dpsGet operation that doesn't do any safety checks.
	// Users of the Java dps APIs must pass a dummy value to the dpsGet function to tell us
	// what type of data item value is being retrieved from the data store. Without this information,
	// it is impossible to decode the stored value in serialized form. That dummy value is 
	// passed to this method as a TYPE GENERIC third argument. If the dummy value is a collection
	// type such as List<?>, then that dummy value must include one element with any random value so that
	// we will know the generic type of the element in that collection data item.
	public <T1> Object dpsGet(long store, T1 key, String keySplTypeName, String valueSplTypeName, long[] err) throws Exception {
		Object[] byteBufferArray = nbfEncodeKeyAndValue(key, null, keySplTypeName, valueSplTypeName);
		
		// We need to have the key serialized properly. If not, throw an exception.
		if ((byteBufferArray[0] == null) || (byteBufferArray[1] == null)) {
			// Something went seriously wrong.
			throw new Exception("dpsGet: Unable to serialize the key.");
		}
		
        Object[] resultArray = dpsGetCpp(store, (ByteBuffer)byteBufferArray[0], ((Integer)byteBufferArray[1]).intValue());
    	// Java is not as convenient as C++ in the sense that we can't pass by reference in Java.
    	// Hence, we can't return multiple values from a method to the caller.
    	// One way to do that in Java is by stuffing the multiple return value items in an object array.
    	// So, dpsGetCpp we called above returns multiple items in an object array.
        // Pull the results from the object array.
        // First object in the result array is a result string with this format: [Format: "booleanResult,errorCode"]
        // Second object in the result array is a ByteBuffer object representing the data item value fetched from the back-end store.
        String result = (String)resultArray[0];
        ByteBuffer byteBuffer2 = (ByteBuffer)resultArray[1];
        // System.out.println("Encoded value buffer contents=" + byteBuffer2);  //<--- Debug print 
        // Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		
		// Decode the data item value.
		if (err[0] == 0) {
			StreamSchema ss2 = null;
			
			// If the dummy data item value passed to this method is already made of a tuple type, we can directly use that dummy tuple object here.
			// Create a new tuple on the fly using the SPL type declaration syntax for the data item value.
			if (valueSplTypeName.startsWith("tuple")) {
				TupleType tt2 = Type.Factory.getTupleType(valueSplTypeName);
				ss2 = tt2.getTupleSchema();		
			} else {
				TupleType tt2 = Type.Factory.getTupleType("tuple<" + valueSplTypeName + " x>");
		        // Get a schema for the newly created tuple type above.
		        ss2 = tt2.getTupleSchema();				
			}			
			
	        // Decode the (blob) byte buffer into a tuple now.
	        BinaryEncoding be2 = ss2.newNativeBinaryEncoding();
	        Tuple tuple = be2.decodeTuple(byteBuffer2);
                // Free the DPS C++ layer allocated memory block in which we obtained the value of the K/V entry.
                dpsFreeDirectBufferMemoryCpp((ByteBuffer)resultArray[1]);
	        // System.out.println("Decoded tuple=" + tuple);
	        
	        if (valueSplTypeName.startsWith("tuple") == true) {
	        	// If the data item value we are expecting is of tuple type, there is no need for another conversion.
	        	// Return the decoded tuple directly to the caller.
	        	return((Object)tuple);
	        } else {
	        	// For non-tuple data item values, parse the decoded tuple to pull out the required data item value.
	        	return(tuple.getObject("x"));
	        }
		} else { 
			return(null);
		}
	}

	// Get data item from a store. (It is a type generic method that can take any key type and any value type.)
	// It is a special (slower) version of dpsGet operation that does all kindss of safety checks.
	// Users of the Java dps APIs must pass a dummy value to the dpsGet function to tell us
	// what type of data item value is being retrieved from the data store. Without this information,
	// it is impossible to decode the stored value in serialized form. That dummy value is 
	// passed to this method as a TYPE GENERIC third argument. If the dummy value is a collection
	// type such as List<?>, then that dummy value must include one element with any random value so that
	// we will know the generic type of the element in that collection data item.
	public <T1> Object dpsGetSafe(long store, T1 key, String keySplTypeName, String valueSplTypeName, long[] err) throws Exception {
		Object[] byteBufferArray = nbfEncodeKeyAndValue(key, null, keySplTypeName, valueSplTypeName);
		
		// We need to have the key serialized properly. If not, throw an exception.
		if ((byteBufferArray[0] == null) || (byteBufferArray[1] == null)) {
			// Something went seriously wrong.
			throw new Exception("dpsGetSafe: Unable to serialize the key.");
		}
		
        Object[] resultArray = dpsGetSafeCpp(store, (ByteBuffer)byteBufferArray[0], ((Integer)byteBufferArray[1]).intValue());
    	// Java is not as convenient as C++ in the sense that we can't pass by reference in Java.
    	// Hence, we can't return multiple values from a method to the caller.
    	// One way to do that in Java is by stuffing the multiple return value items in an object array.
    	// So, dpsGetCpp we called above returns multiple items in an object array.
        // Pull the results from the object array.
        // First object in the result array is a result string with this format: [Format: "booleanResult,errorCode"]
        // Second object in the result array is a ByteBuffer object representing the data item value fetched from the back-end store.
        String result = (String)resultArray[0];
        ByteBuffer byteBuffer2 = (ByteBuffer)resultArray[1];
        // System.out.println("Encoded value buffer contents=" + byteBuffer2);  //<--- Debug print 
        // Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		
		// Decode the data item value.
		if (err[0] == 0) {
			StreamSchema ss2 = null;
			
			// If the dummy data item value passed to this method is already made of a tuple type, we can directly use that dummy tuple object here.
			// Create a new tuple on the fly using the SPL type declaration syntax for the data item value.
			if (valueSplTypeName.startsWith("tuple")) {
				TupleType tt2 = Type.Factory.getTupleType(valueSplTypeName);
				ss2 = tt2.getTupleSchema();		
			} else {
				TupleType tt2 = Type.Factory.getTupleType("tuple<" + valueSplTypeName + " x>");
		        // Get a schema for the newly created tuple type above.
		        ss2 = tt2.getTupleSchema();				
			}			
			
	        // Decode the (blob) byte buffer into a tuple now.
	        BinaryEncoding be2 = ss2.newNativeBinaryEncoding();
	        Tuple tuple = be2.decodeTuple(byteBuffer2);
                // Free the DPS C++ layer allocated memory block in which we obtained the value of the K/V entry.
                dpsFreeDirectBufferMemoryCpp((ByteBuffer)resultArray[1]);
	        // System.out.println("Decoded tuple=" + tuple);
	        
	        if (valueSplTypeName.startsWith("tuple") == true) {
	        	// If the data item value we are expecting is of tuple type, there is no need for another conversion.
	        	// Return the decoded tuple directly to the caller.
	        	return((Object)tuple);
	        } else {
	        	// For non-tuple data item values, parse the decoded tuple to pull out the required data item value.
	        	return(tuple.getObject("x"));
	        }
		} else { 
			return(null);
		}
	}	

	// Get a TTL based K/V pair stored in the global area of the back-end data store.
	// This function doesn't need an user created store to perform the put operation since the data item will be
	// stored in a flat memory space inside the chosen back-end store infrastructure.
	// (It is a type generic method that can take any key type and any value type.)
	// Users of the Java dps APIs must pass a dummy value to the dpsGet function to tell us
	// what type of data item value is being retrieved from the data store. Without this information,
	// it is impossible to decode the stored value in serialized form. That dummy value is 
	// passed to this method as a TYPE GENERIC third argument. If the dummy value is a collection
	// type such as List<?>, then that dummy value must include one element with any random value so that
	// we will know the generic type of the element in that collection data item.
	public <T1> Object dpsGetTTL(T1 key, String keySplTypeName, String valueSplTypeName, long[] err, boolean encodeKey, boolean encodeValue) throws Exception {
		Object[] byteBufferArray = nbfEncodeKeyAndValue(key, null, keySplTypeName, valueSplTypeName);
		
		// We need to have the key serialized properly. If not, throw an exception.
		if ((byteBufferArray[0] == null) || (byteBufferArray[1] == null)) {
			// Something went seriously wrong.
			throw new Exception("dpsGetTTL: Unable to serialize the key.");
		}
		
        Object[] resultArray = dpsGetTTLCpp((ByteBuffer)byteBufferArray[0], ((Integer)byteBufferArray[1]).intValue(), encodeKey, encodeValue);
    	// Java is not as convenient as C++ in the sense that we can't pass by reference in Java.
    	// Hence, we can't return multiple values from a method to the caller.
    	// One way to do that in Java is by stuffing the multiple return value items in an object array.
    	// So, dpsGetCpp we called above returns multiple items in an object array.
        // Pull the results from the object array.
        // First object in the result array is a result string with this format: [Format: "booleanResult,errorCode"]
        // Second object in the result array is a ByteBuffer object representing the data item value fetched from the back-end store.
        String result = (String)resultArray[0];
        ByteBuffer byteBuffer2 = (ByteBuffer)resultArray[1];
        // System.out.println("Encoded value buffer contents=" + byteBuffer2);  //<--- Debug print 
        // Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		
		// Decode the data item value.
		if (err[0] == 0) {
                   // If the caller requested for not to encode value, then we just read a string value that was stored in clear.
                   // There is no need to deserialize it. We can return the plain string value stored in clear as it is.
                   if (encodeValue == false) {
                      // This is a value stored as plain string in the back-end data store.
                      RString resultValue = new RString(byteBuffer2, byteBuffer2.remaining());
                      // Release the memory allocated by the C++ DPS code.
                      byteBuffer2 = null;
                      return(resultValue);
                   }            

			StreamSchema ss2 = null;
			
			// If the dummy data item value passed to this method is already made of a tuple type, we can directly use that dummy tuple object here.
			// Create a new tuple on the fly using the SPL type declaration syntax for the data item value.
			if (valueSplTypeName.startsWith("tuple")) {
				TupleType tt2 = Type.Factory.getTupleType(valueSplTypeName);
				ss2 = tt2.getTupleSchema();		
			} else {
				TupleType tt2 = Type.Factory.getTupleType("tuple<" + valueSplTypeName + " x>");
		        // Get a schema for the newly created tuple type above.
		        ss2 = tt2.getTupleSchema();				
			}			
			
	        // Decode the (blob) byte buffer into a tuple now.
	        BinaryEncoding be2 = ss2.newNativeBinaryEncoding();
	        Tuple tuple = be2.decodeTuple(byteBuffer2);
                // Free the DPS C++ layer allocated memory block in which we obtained the value of the K/V entry.
                dpsFreeDirectBufferMemoryCpp((ByteBuffer)resultArray[1]);
	        // System.out.println("Decoded tuple=" + tuple);
	        
	        if (valueSplTypeName.startsWith("tuple") == true) {
	        	// If the data item value we are expecting is of tuple type, there is no need for another conversion.
	        	// Return the decoded tuple directly to the caller.
	        	return((Object)tuple);
	        } else {
	        	// For non-tuple data item values, parse the decoded tuple to pull out the required data item value.
	        	return(tuple.getObject("x"));
	        }
		} else { 
			return(null);
		}
	}	
		
	// Remove an existing data item from a store.
	public <T1> boolean dpsRemove(long store, T1 key, String keySplTypeName, String valueSplTypeName, long[] err) throws Exception {
		Object[] byteBufferArray = nbfEncodeKeyAndValue(key, null, keySplTypeName, valueSplTypeName);
		
		// We need to have the key erialized properly. If not, throw an exception.
		if ((byteBufferArray[0] == null) || (byteBufferArray[1] == null)) {
			// Something went seriously wrong.
			throw new Exception("dpsRemove: Unable to serialize the key.");
		}
		
        String result = dpsRemoveCpp(store, (ByteBuffer)byteBufferArray[0], ((Integer)byteBufferArray[1]).intValue());
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		return(booleanResult);		
	}	

	// Remove a TTL based K/V pair stored in the global area of the back-end data store.
	public <T1> boolean dpsRemoveTTL(T1 key, String keySplTypeName, String valueSplTypeName, long[] err, boolean encodeKey) throws Exception {
		Object[] byteBufferArray = nbfEncodeKeyAndValue(key, null, keySplTypeName, valueSplTypeName);
		
		// We need to have the key erialized properly. If not, throw an exception.
		if ((byteBufferArray[0] == null) || (byteBufferArray[1] == null)) {
			// Something went seriously wrong.
			throw new Exception("dpsRemoveTTL: Unable to serialize the key.");
		}
		
        String result = dpsRemoveTTLCpp((ByteBuffer)byteBufferArray[0], ((Integer)byteBufferArray[1]).intValue(), encodeKey);
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		return(booleanResult);		
	}	
		
	// Check for the existence of a data item key in a store.
	public <T1> boolean dpsHas(long store, T1 key, String keySplTypeName, String valueSplTypeName, long[] err) throws Exception {
		Object[] byteBufferArray = nbfEncodeKeyAndValue(key, null, keySplTypeName, valueSplTypeName);
		
		// We need to have the key erialized properly. If not, throw an exception.
		if ((byteBufferArray[0] == null) || (byteBufferArray[1] == null)) {
			// Something went seriously wrong.
			throw new Exception("dpsHas: Unable to serialize the key.");
		}
        
        String result = dpsHasCpp(store, (ByteBuffer)byteBufferArray[0], ((Integer)byteBufferArray[1]).intValue());
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		return(booleanResult);		
	}

	// Check if a TTL based K/V pair for a given key exists in the global area of the back-end data store.
	public <T1> boolean dpsHasTTL(T1 key, String keySplTypeName, String valueSplTypeName, long[] err, boolean encodeKey) throws Exception {
		Object[] byteBufferArray = nbfEncodeKeyAndValue(key, null, keySplTypeName, valueSplTypeName);
		
		// We need to have the key erialized properly. If not, throw an exception.
		if ((byteBufferArray[0] == null) || (byteBufferArray[1] == null)) {
			// Something went seriously wrong.
			throw new Exception("dpsHasTTL: Unable to serialize the key.");
		}
        
        String result = dpsHasTTLCpp((ByteBuffer)byteBufferArray[0], ((Integer)byteBufferArray[1]).intValue(), encodeKey);
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		return(booleanResult);		
	}	
	
	// Clear the store by removing all its contents. In other words, empty this store.
	public void dpsClear(long store, long[] err) {
		String result = dpsClearCpp(store);
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		// Since this dps API is a void function, ignore the boolean result and
		// skip to the errorCode field.
		err[0] = scanner.nextLong();
		scanner.close();
		return;		
	}

	// Clear the store by removing all its contents. In other words, empty this store.
	public long dpsSize(long store, long[] err) {
		String result = dpsSizeCpp(store);
		// Parse the result string [Format: "storeSize,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		long storeSize = scanner.nextLong();
		err[0] = scanner.nextLong();
		scanner.close();
		return(storeSize);		
	}

	// Begin iteration of a store.
	public long dpsBeginIteration(long store, long[] err) {
		String result = dpsBeginIterationCpp(store);
		// Parse the result string [Format: "iterationHandle,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		long iterationHandle = scanner.nextLong();
		err[0] = scanner.nextLong();
		scanner.close();
		return(iterationHandle);			
	}
	
	// Get the next data item key and value from the store iteration.
	public Object[] dpsGetNext(long store, long iterationHandle, String keySplTypeName, String valueSplTypeName, long[] err) throws Exception {
        Object[] resultArray = dpsGetNextCpp(store, iterationHandle);
    	// dpsGetNextCpp we called above returns multiple items in an object array.
        // Pull the results from the object array.
        // First object in the result array is a result string with this format: [Format: "booleanResult,errorCode"]
        // Second object in the result array is a ByteBuffer object representing the data item key fetched from the back-end store.
        // Third object in the result array is a ByteBuffer object representing the data item value fetched from the back-end store.
        String result = (String)resultArray[0];
        // Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		
		Object[] objectArray = new Object[2];
		Arrays.fill(objectArray, null);
		
		if ((err[0] == 0) && (booleanResult == false)) {
			// A result of false means iteration reached the end of the store.
			// There are no more data items to be found.
			// In that case, we will return a result object array with null values for key and value.
			// Caller can check for that null key and value to conclude that the iteration ended.
			return(objectArray);
		} else if (err[0] != 0) {
			// Some problem occurred in the store iteration.
			return(null);
		}
		
        ByteBuffer byteBuffer1 = (ByteBuffer)resultArray[1];
        // System.out.println("Encoded key buffer contents=" + byteBuffer1);  //<--- Debug print 
        ByteBuffer byteBuffer2 = (ByteBuffer)resultArray[2];
        // System.out.println("Encoded value buffer contents=" + byteBuffer2);  //<--- Debug print         
		
		// Decode the data item key and value now.
        // 
		// Get the next data item key.
		StreamSchema ss1 = null;
		
		// If the dummy data item key passed to this method is already made of a tuple type, we can directly use that dummy tuple object here.
		// Create a new tuple on the fly using the SPL type declaration syntax for the data item key.
		if (keySplTypeName.startsWith("tuple") == true) {
			TupleType tt1 = Type.Factory.getTupleType(keySplTypeName);
			ss1 = tt1.getTupleSchema();	
		} else {
			TupleType tt1 = Type.Factory.getTupleType("tuple<" + keySplTypeName + " x>");
	        // Get a schema for the newly created tuple type above.
	        ss1 = tt1.getTupleSchema();				
		}			
		
        // Decode the (blob) byte buffer into a tuple now.
        BinaryEncoding be1 = ss1.newNativeBinaryEncoding();
        Tuple tuple1 = be1.decodeTuple(byteBuffer1);
        // Free the DPS C++ layer allocated memory block in which we obtained the key of the K/V entry.
        dpsFreeDirectBufferMemoryCpp((ByteBuffer)resultArray[1]);
        // System.out.println("Decoded key tuple=" + tuple1);
        
        if (keySplTypeName.startsWith("tuple") == true) {
        	// If the data item key we are expecting is of tuple type, there is no need for another conversion.
        	// Return the decoded tuple directly to the caller.
        	objectArray[0] = tuple1;
        } else {
        	// For non-tuple data item keys, parse the decoded tuple to pull out the required data item key.
        	objectArray[0] = tuple1.getObject("x");
        }			
		
		// Get the next data item value.
		StreamSchema ss2 = null;
		
		// If the dummy data item value passed to this method is already made of a tuple type, we can directly use that dummy tuple object here.
		// Create a new tuple on the fly using the SPL type declaration syntax for the data item value.
		if (valueSplTypeName.startsWith("tuple") == true) {
			TupleType tt2 = Type.Factory.getTupleType(valueSplTypeName);
			ss2 = tt2.getTupleSchema();	
		} else {
			TupleType tt2 = Type.Factory.getTupleType("tuple<" + valueSplTypeName + " x>");
	        // Get a schema for the newly created tuple type above.
	        ss2 = tt2.getTupleSchema();				
		}			
		
        // Decode the (blob) byte buffer into a tuple now.
        BinaryEncoding be2 = ss2.newNativeBinaryEncoding();
        Tuple tuple2 = be2.decodeTuple(byteBuffer2);
        // Free the DPS C++ layer allocated memory block in which we obtained the value of the K/V entry.
        dpsFreeDirectBufferMemoryCpp((ByteBuffer)resultArray[2]);
        // System.out.println("Decoded value tuple=" + tuple2);
        
        if (valueSplTypeName.startsWith("tuple") == true) {
        	// If the data item value we are expecting is of tuple type, there is no need for another conversion.
        	// Return the decoded tuple directly to the caller.
        	objectArray[1] = tuple2;
        } else {
        	// For non-tuple data item values, parse the decoded tuple to pull out the required data item value.
        	objectArray[1] = tuple2.getObject("x");
        }
        
        return(objectArray);
	}
	
	// End iteration of store.
	public void dpsEndIteration(long store, long iterationHandle, long[] err) {
		String result = dpsEndIterationCpp(store, iterationHandle);
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		// Since this dps API is a void function, ignore the boolean result and
		// skip to the errorCode field.
		err[0] = scanner.nextLong();
		scanner.close();
		return;				
	}

	// Serialize all the store contents into a byte buffer.
	public ByteBuffer dpsSerialize(long store, String keySplTypeName, String valueSplTypeName, long[] err) throws Exception {
		// We are going to do all of it here in Java without resorting to the JNI.
		// We are going to maintain a single byte buffer where we will store the serialized key and the serialized value in this format:
		// Key1Value1Key2Value2Key3Value3....KeyNValueN
		//
		// Let us iterate through the store given to us.
		long iterationHandle = this.dpsBeginIteration(store, err);
		if (err[0] != 0) {
			return(null);
		}
		
		// Prepare a list to dynamically grow the serialized buffer content as we iterate through the store.
		List<Byte> cumulativeSerializedByteList = new ArrayList<Byte>();
		int serializedBytesCnt = 0;
		int itemsSerialized = 0;
		
		// Stay in a loop and iterate over the full contents of the given store.
		while(true) {
			Object[] iterationResult = this.dpsGetNext(store, iterationHandle, keySplTypeName, valueSplTypeName, err);
			
			if (err[0] != 0) {
				long[] tmpErr = new long[1];
				this.dpsEndIteration(store, iterationHandle, tmpErr);
				return(null);
			}
			
			if ((iterationResult[0] == null) && (iterationResult[1] == null)) {
				// If both key and value are null, that means we reached the end of the store.
				// There are no more items to be found.
				if (itemsSerialized == 0) {
					// If we hit the end of the store right out of the gate, then it is an attempt to serialize a vacant store.
					this.dpsEndIteration(store, iterationHandle, err);
					throw new Exception("dpsSerialize: Attempt to serialize an empty store, which is not allowed.");
				} else {
					// We are done iterating over the store and serializing its contents.
					break;
				}
			}
			
			// We have a key and a value now.
			// System.out.println("Dataitem " + (itemsSerialized+1) + ", Key=" + iterationResult[0] +  ", Value=" + iterationResult[1]);
			// Encode the key and value into ByteBuffer.
			Object[] byteBufferArray = nbfEncodeKeyAndValue(iterationResult[0], iterationResult[1],
				keySplTypeName, valueSplTypeName);
			// We need to have both the key and value serialized properly. If not, throw an exception.
			if ((byteBufferArray[0] == null) || (byteBufferArray[1] == null) ||
				(byteBufferArray[2] == null) || (byteBufferArray[3] == null)) {
				// Something went seriously wrong.
				this.dpsEndIteration(store, iterationHandle, err);
				byteBufferArray = null;
				throw new Exception("dpsSerialize: Error in serializing the key:value pair.");
			}
			
			// Let us get the serialized key and value into their cumulative buffers, where we will keep collecting everything.
			// System.out.println("Dataitem " + (itemsSerialized+1) + ", KeyBufferPosition=" + ((ByteBuffer)byteBufferArray[0]).position() + ", KeyBufferCapacity=" + ((ByteBuffer)byteBufferArray[0]).capacity());
			byte[] tmpKeyByteBuffer = new byte[((Integer)byteBufferArray[1]).intValue()];
			serializedBytesCnt += tmpKeyByteBuffer.length;
			((ByteBuffer)byteBufferArray[0]).get(tmpKeyByteBuffer, 0, tmpKeyByteBuffer.length);
			List<Byte> al1 = new ArrayList<Byte>(tmpKeyByteBuffer.length);
			for (byte b: tmpKeyByteBuffer) {
				al1.add(new Byte(b));
			}
			cumulativeSerializedByteList.addAll(al1);
			tmpKeyByteBuffer = null;
			
			// System.out.println("Dataitem " + (itemsSerialized+1) + ", ValueBufferPosition=" + ((ByteBuffer)byteBufferArray[2]).position() + ", ValueBufferCapacity=" + ((ByteBuffer)byteBufferArray[2]).capacity());
			byte[] tmpValueByteBuffer = new byte[((Integer)byteBufferArray[3]).intValue()];
			serializedBytesCnt += tmpValueByteBuffer.length;
			((ByteBuffer)byteBufferArray[2]).get(tmpValueByteBuffer, 0, tmpValueByteBuffer.length);
			List<Byte> al2 = new ArrayList<Byte>(tmpValueByteBuffer.length);
			for (byte b: tmpValueByteBuffer) {
				al2.add(new Byte(b));
			}
			cumulativeSerializedByteList.addAll(al2);
			tmpValueByteBuffer = null;
			itemsSerialized++;
			byteBufferArray = null;
			al1 = al2 = null;
		} // End of while(1) loop.

		this.dpsEndIteration(store, iterationHandle, err);
		// At this point, we have completed serializing all the data items in the given store.
		// Serialized keys and values are kept in a single List<Byte>
		// Get the byte[] formatted array from the cumulative serialized List<Byte>.
		byte[] cumulativeSerializedByteBuffer = new byte[cumulativeSerializedByteList.size()];
		int cnt = 0;
		for (Byte b: cumulativeSerializedByteList) {
			cumulativeSerializedByteBuffer[cnt++] = b.byteValue();
		}
		// System.out.println("ItemsSerialized=" + itemsSerialized + ", serializedBytesCnt=" + serializedBytesCnt + ", size of the CumulativeByteList=" + cumulativeSerializedByteList.size());
		cumulativeSerializedByteList = null;
		// Wrap the byte[] array into a ByteBuffer object to be returned to the caller.
		return(ByteBuffer.wrap(cumulativeSerializedByteBuffer));
	}
	
	// Deserialize a given ByteBuffer into data items and store it in a given store.
	public void dpsDeserialize(long store, ByteBuffer data, String keySplTypeName, String valueSplTypeName, long[] err) throws Exception {
		// We are going to do all of it here in Java without resorting to the JNI.
		// We are given a single byte buffer where the serialized key and the serialized value are stored in this format:
		// Key1Value1Key2Value2Key3Value3....KeyNValueN	
		if (data.capacity() <= 0) {
			throw new Exception("dpsDeserialize: Attempt to deserialize an empty data buffer, which is not allowed.");
		}
		
		int itemsDeserialized = 0;
		// Set the position of the byte buffer to its first element.
		data.rewind();
		// Stay in a loop and process all the bytes in the data buffer.
		while(data.remaining() > 0) {
			// System.out.println("itemsDeserialized=" + ++itemsDeserialized + ", beginningPosition=" + data.position());
			// Decode the data item key from the byte buffer.
			StreamSchema ss1 = null;
			
			// If the dummy data item key passed to this method is already made of a tuple type, we can directly use that dummy tuple object here.
			// Create a new tuple on the fly using the SPL type declaration syntax for the data item key.
			if (keySplTypeName.startsWith("tuple") == true) {
				TupleType tt1 = Type.Factory.getTupleType(keySplTypeName);
		        ss1 = tt1.getTupleSchema();
			} else {
				TupleType tt1 = Type.Factory.getTupleType("tuple<" + keySplTypeName + " x>");
		        // Get a schema for the newly created tuple type above.
		        ss1 = tt1.getTupleSchema();				
			}			
			
	        // Decode the (blob) byte buffer into a tuple now.
	        BinaryEncoding be1 = ss1.newNativeBinaryEncoding();
	        Tuple tuple1 = be1.decodeTuple(data);
	        // System.out.println("Decoded key tuple1=" + tuple1);
	        Object keyObject = null;

	        if (keySplTypeName.startsWith("tuple") == true) {
	        	// If the data item key we are expecting is of tuple type, there is no need for another conversion.
	        	// Use the decoded tuple directly in putting the data item to a store below.
	        	keyObject = tuple1;
	        } else {
	        	// For non-tuple data item keys, parse the decoded tuple to pull out the required data item key.
	        	keyObject = tuple1.getObject("x");
	        }		
	        // System.out.println("itemsDeserialized=" + itemsDeserialized + ", afterKeyPosition=" + data.position());
			// =========================================================================
			// Decode the data item value from the byte buffer.
			StreamSchema ss2 = null;
			
			// If the dummy data item value passed to this method is already made of a tuple type, we can directly use that dummy tuple object here.
			// Create a new tuple on the fly using the SPL type declaration syntax for the data item value.
			if (valueSplTypeName.startsWith("tuple") == true) {
				TupleType tt2 = Type.Factory.getTupleType(valueSplTypeName);
		        ss2 = tt2.getTupleSchema();	
			} else {
				TupleType tt2 = Type.Factory.getTupleType("tuple<" + valueSplTypeName + " x>");
		        // Get a schema for the newly created tuple type above.
		        ss2 = tt2.getTupleSchema();				
			}			
			
	        // Decode the (blob) byte buffer into a tuple now.
	        BinaryEncoding be2 = ss2.newNativeBinaryEncoding();
	        Tuple tuple2 = be2.decodeTuple(data);
	        // System.out.println("Decoded value tuple2=" + tuple2);
	        Object valueObject = null;
	        
	        if (valueSplTypeName.startsWith("tuple") == true) {
	        	// If the data item value we are expecting is of tuple type, there is no need for another conversion.
	        	// Return the decoded tuple directly to the caller.
	        	valueObject = tuple2;
	        } else {
	        	// For non-tuple data item values, parse the decoded tuple to pull out the required data item value.
	        	valueObject = tuple2.getObject("x");
	        }		
	        // System.out.println("itemsDeserialized=" + itemsDeserialized + ", afterValuePosition=" + data.position());
	        // =========================================================================
	        // Let us now put the decoded key and value into the given store.
	        dpsPut(store, keyObject, valueObject,  keySplTypeName, valueSplTypeName, err);
		} // End of the while loop.
		
		return;
	}

	// Check if there is an active connection to the back-end data store.
	public boolean dpsIsConnected() {
               String result = dpsIsConnectedCpp();
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		scanner.close();
		return(booleanResult);		
	}	

	// Reconnect to the back-end data store.
	public boolean dpsReconnect() {
               String result = dpsReconnectCpp();
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		scanner.close();
		return(booleanResult);		
	}	
	
	// =======================================================
	// Distributed lock related API calls are processed below.
	// =======================================================
	// Create a new lock or get a distributed lock if it already exists.
	public long dlCreateOrGetLock(String name, long[] err) {
		String result = dlCreateOrGetLockCpp(name);
		// Parse the result string [Format: "lockId,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		long lockId = scanner.nextLong();
		err[0] = scanner.nextLong();
		scanner.close();
		return(lockId);
	}

	// Remove an existing distributed lock. 
	public boolean dlRemoveLock(long lock, long[] err) {
		String result = dlRemoveLockCpp(lock);
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		return(booleanResult);
	}
	
	// Take possession of a distributed lock by acquiring it. [lock will stay on until your release it. ***Dangerous***]
	public void dlAcquireLock(long lock, long[] err) {
		String result = dlAcquireLockCpp(lock);
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		// Since it is a void method, ignore the boolean result field.
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		return;
	}
	
	// Take possession of a distributed lock by acquiring it for a lease period you set in the second argument.
	public void dlAcquireLock(long lock, double leaseTime, double maxWaitTimeToAcquireLock, long[] err) {
		String result = dlAcquireLockCpp(lock, leaseTime, maxWaitTimeToAcquireLock);
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		// Since it is a void method, ignore the boolean result field.
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		return;
	}
	
	// Release the lock that was acquired earlier.
	public void dlReleaseLock(long lock, long[] err) {
		String result = dlReleaseLockCpp(lock);
		// Parse the result string [Format: "booleanResult,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		// Since it is a void method, ignore the boolean result field.
		boolean booleanResult = scanner.nextBoolean();
		err[0] = scanner.nextLong();
		scanner.close();
		return;
	}

	// Get the Linux process id that currently owns a given a lock.
	public int dlGetPidForLock(String name, long[] err) {
		String result = dlGetPidForLockCpp(name);
		// Parse the result string [Format: "pid,errorCode"]
		Scanner scanner = new Scanner(result);
		scanner.useDelimiter(",");
		int pid = scanner.nextInt();
		err[0] = scanner.nextLong();
		scanner.close();
		return(pid);		
	}
	
	// Get the error code for the most recently performed dl activtity.
	public long dlGetLastDistributedLockErrorCode() {
		long error = dlGetLastDistributedLockErrorCodeCpp();
		return(error);
	}

	// Get the error string for the most recently performed dl activtity.
	public String dlGetLastDistributedLockErrorString() {
		String errorString = dpsGetLastStoreErrorStringCpp();
		return(errorString);
	}	
}
