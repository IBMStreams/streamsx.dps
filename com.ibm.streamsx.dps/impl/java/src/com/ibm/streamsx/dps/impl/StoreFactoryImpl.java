/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is the implementation class for the store factory.
// Through the object of this class, a Java dps user can create new stores or get an existing store, or
// find an existing store or remove an existing store.
package com.ibm.streamsx.dps.impl;

import com.ibm.streams.operator.types.*;

import com.ibm.streamsx.dps.Store;
import com.ibm.streamsx.dps.StoreFactory;
import com.ibm.streamsx.dps.StoreFactoryException;

public class StoreFactoryImpl implements StoreFactory
{
	// Create a brand new store.
	public Store createStore(String name, String keySplTypeName, String valueSplTypeName) throws StoreFactoryException {
		long result;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsCreateStore(name, keySplTypeName, valueSplTypeName, err);
		} catch(Exception e) { 
			// Either dps cannot be initialized or createStore went wrong.
			// An error code of 65535 indicates that this error occurred inside
			// the dps JNI glue layer and not inside the actual store functions.
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreFactoryException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return new StoreImpl(name, result, keySplTypeName, valueSplTypeName);
	}
	
	// Create a brand new store or get an existing store. 
	public Store createOrGetStore(String name, String keySplTypeName, String valueSplTypeName) throws StoreFactoryException {
		long result;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsCreateOrGetStore(name, keySplTypeName, valueSplTypeName, err);
		} catch(Exception e) { 
			// Either dps cannot be initialized or createOrGetStore went wrong.
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreFactoryException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return new StoreImpl(name, result, keySplTypeName, valueSplTypeName);
	}
	
	// Find an existing store.
	public Store findStore(String name) throws StoreFactoryException {
		long result;
		long[] err = new long[1];
		DpsHelper dps = null;
		String keySplTypeName = "";
		String valueSplTypeName = "";
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsFindStore(name, err);
			// If we successfully found the store, then let us get the 
			// spl type name for this store's key and value.
			keySplTypeName = dps.dpsGetKeySplTypeName(result);
			valueSplTypeName = dps.dpsGetValueSplTypeName(result);
		} catch(Exception e) { 
			// either dps cannot be initialized or findStore went wrong
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreFactoryException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}

		// Ensure that we obtained the spl type names for this store's key and value.
		if ((keySplTypeName == "") || (valueSplTypeName == "")) {
			throw new StoreFactoryException(65535, "Unable to obtain the spl type names for the key and value of a store named " + name);
		}
		
		return new StoreImpl(name, result, keySplTypeName, valueSplTypeName);
	}
	
	// Remove an existing store.
	public boolean removeStore(Store store) throws StoreFactoryException {
		boolean result;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsRemoveStore(((StoreImpl)store).getId(), err);
		} catch(Exception e) { 
			// either dps cannot be initialized or removeStore went wrong
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreFactoryException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return(result);
	}	
	
	// Following special functions were implemented on OCT/12/2014.
	// Since these functions don't need a store object to perform put, get, has, and remove of the TTL based
	// K/V pairs, we are implementing them here in the StoreFactoryImpl class instead of in the StoreImpl class.
	//

        // Overloaded method (with no encodeKey method argument)
	// Put a data item with TTL (Time To Live in seconds) into the global area of the back-end data store.
	public <T1, T2> boolean putTTL(T1 key, T2 value, int ttl, String keySplTypeName, String valueSplTypeName) throws StoreFactoryException {
           // Call the other overloaded method by passing encodeKey=true and encodeValue=true. 
           return(putTTL(key, value, ttl, keySplTypeName, valueSplTypeName, true, true));
        }

	// Put a data item with TTL (Time To Live in seconds) into the global area of the back-end data store.
	public <T1, T2> boolean putTTL(T1 key, T2 value, int ttl, String keySplTypeName, String valueSplTypeName, boolean encodeKey, boolean encodeValue) throws StoreFactoryException {
		boolean result;
		long[] err = new long[1];
		DpsHelper dps = null;

                // Validate that the caller can ask us not to encode the key only for rstring data type.
                if (encodeKey == false && keySplTypeName != "rstring") {
                   // It is non-rstring data type. We must base64 encode the data.
                   encodeKey = true;
                }

                // Validate that the caller can ask us not to encode the value only for rstring data type.
                if (encodeValue == false && valueSplTypeName != "rstring") {
                   // It is non-rstring data type. We must base64 encode the data.
                   encodeValue = true;
                }

		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsPutTTL(key, value, ttl, keySplTypeName, valueSplTypeName, err, encodeKey, encodeValue);
		} catch(Exception e) {
			// Either dps cannot be initialized or putTTL went wrong.
			// An error code of 65535 indicates that this error occurred inside
			// the dps JNI glue layer and not inside the actual store functions.
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreFactoryException(dps.dpsGetLastErrorCodeTTL(), 
				dps.dpsGetLastErrorStringTTL());
		}
		
		return(result);
	}

        // Overloaded method (with no encodeKey method argument)
	// Get a TTL based K/V pair stored in the global area of the back-end data store.
	public <T1> Object getTTL(T1 key, String keySplTypeName, String valueSplTypeName) throws StoreFactoryException {
           // Call the other overloaded method by passing encodeKey=true and encodeValue=true. 
           return(getTTL(key, keySplTypeName, valueSplTypeName, true, true));
        }
	
	// Get a TTL based K/V pair stored in the global area of the back-end data store.
	public <T1> Object getTTL(T1 key, String keySplTypeName, String valueSplTypeName, boolean encodeKey, boolean encodeValue) throws StoreFactoryException {
		Object result = null;
		long[] err = new long[1];
		DpsHelper dps = null;
		
                // Validate that the caller can ask us not to encode the key only for rstring data type.
                if (encodeKey == false && keySplTypeName != "rstring") {
                   // It is non-rstring data type. We must base64 encode the data.
                   encodeKey = true;
                }

                // Validate that the caller can ask us not to encode the value only for rstring data type.
                if (encodeValue == false && valueSplTypeName != "rstring") {
                   // It is non-rstring data type. We must encode the value data by serializing it.
                   encodeValue = true;
                }

		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsGetTTL(key, keySplTypeName, valueSplTypeName, err, encodeKey, encodeValue);
		} catch(Exception e) {
			// Either dps cannot be initialized or getTTL went wrong.
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreFactoryException(dps.dpsGetLastErrorCodeTTL(), 
				dps.dpsGetLastErrorStringTTL());
		}
		
		return(result);		
	}	

        // Overloaded method (with no encodeKey method argument)
	// Remove a TTL based K/V pair stored in the global area of the back-end data store.
	public <T1> boolean removeTTL(T1 key, String keySplTypeName) throws StoreFactoryException {
           // Call the other overloaded method by passing encodeKey=true.
           return(removeTTL(key, keySplTypeName, true));
        }
	
	// Remove a TTL based K/V pair stored in the global area of the back-end data store.
	public <T1> boolean removeTTL(T1 key, String keySplTypeName, boolean encodeKey) throws StoreFactoryException {
		boolean result;
		long[] err = new long[1];
		DpsHelper dps = null;

                // Validate that the caller can ask us not to encode the key only for rstring data type.
                if (encodeKey == false && keySplTypeName != "rstring") {
                   // It is non-rstring data type. We must base64 encode the data.
                   encodeKey = true;
                }
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsRemoveTTL(key, keySplTypeName, "", err, encodeKey);
		} catch(Exception e) {
			// Either dps cannot be initialized or removeTTL went wrong.
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreFactoryException(dps.dpsGetLastErrorCodeTTL(), 
				dps.dpsGetLastErrorStringTTL());
		}
		
		return(result);			
	}

        // Overloaded method (with no encodeKey method argument)
	// Check if a TTL based K/V pair for a given key exists in the global area of the back-end data store.
	public <T1> boolean hasTTL(T1 key, String keySplTypeName) throws StoreFactoryException {
           // Call the other overloaded method by passing encodeKey=true.
           return(hasTTL(key, keySplTypeName, true));
        }

	// Check if a TTL based K/V pair for a given key exists in the global area of the back-end data store.
	public <T1> boolean hasTTL(T1 key, String keySplTypeName, boolean encodeKey) throws StoreFactoryException {
		boolean result;
		long[] err = new long[1];
		DpsHelper dps = null;

                // Validate that the caller can ask us not to encode the key only for rstring data type.
                if (encodeKey == false && keySplTypeName != "rstring") {
                   // It is non-rstring data type. We must base64 encode the data.
                   encodeKey = true;
                }
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsHasTTL(key, keySplTypeName, "", err, encodeKey);
		} catch(Exception e) {
			// Either dps cannot be initialized or hasTTL went wrong.
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreFactoryException(dps.dpsGetLastErrorCodeTTL(), 
				dps.dpsGetLastErrorStringTTL());
		}
		
		return(result);		
	}	

	// Get the name of the NoSQL DB product being used now.
	public String getNoSqlDbProductName() throws StoreFactoryException {
		String result = "";
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsGetNoSqlDbProductName();
		} catch(Exception e) {
			// Either dps cannot be initialized or the called method went wrong.
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		return(result);				
		
	}

	// Get the details of the machine where this operator is running.
	// This method will return a String array with the following values in each of its elements:
	// Name of this machine.
	// OS version of this machine.
	// CPU architecture of this machine
	public String[] getDetailsAboutThisMachine() throws StoreFactoryException {
		String[] result;
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsGetDetailsAboutThisMachine();
		} catch(Exception e) {
			// Either dps cannot be initialized or the called method went wrong.
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		return(result);			
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
	public boolean runDataStoreCommand(String cmd) throws StoreFactoryException {
		boolean result;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsRunDataStoreCommand(cmd, err);
		} catch(Exception e) {
			// Either dps cannot be initialized or put went wrong.
			// An error code of 65535 indicates that this error occurred inside
			// the dps JNI glue layer and not inside the actual store functions.
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreFactoryException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return(result);		
	}
	
	/*
    If users want to execute arbitrary back-end data store two way
    native commands, this API can be used. This is a variation of the previous API with
    overloaded function arguments. As of Nov/2014, this API is supported in the dps toolkit only
    when Cloudant NoSQL DB is used as a back-end data store. It covers any Cloudant HTTP/JSON based
    native commands that can perform both database and document related Cloudant APIs that are very
    well documented for reference on the web.
    */
	public String runDataStoreCommand(int cmdType, String httpVerb,
			String baseUrl, String apiEndpoint, String queryParams, String jsonRequest, long[] httpResponseCode) throws StoreFactoryException {
		boolean result;
		String[] jsonResponse = new String[1];
		DpsHelper dps = null;
		jsonResponse[0] = "";
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsRunDataStoreCommand(cmdType, httpVerb, baseUrl, apiEndpoint,
				queryParams, jsonRequest, jsonResponse, httpResponseCode);
		} catch(Exception e) {
			// Either dps cannot be initialized or put went wrong.
			// An error code of 65535 indicates that this error occurred inside
			// the dps JNI glue layer and not inside the actual store functions.
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		// This method has a slightly complicated set of return values.
		// There are three things getting returned here.
		// 1) This DPS API returns false when it encounters error inside the DPS or in cURL calls before your command reaches the Cloudant server.
		//    In the case of a false return value, we will throw an exception to our Java dps user with error code and error message.
		// 2) A return value of true simply means that your HTTP REST API request was sent to the Cloudant server and a response was received from the server.
		//    In that case, you will get the HTTP response code in the method argument we passed and you should interpret the meaning of that HTTP code to 
		//    know whether your request really succeeded or not.
		// 3) If the HTTP response code indicates "all is well", then the JSON response string from the Cloudant service will be returned back to the Java dps user.
		
		if (result == false) {
			throw new StoreFactoryException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return(jsonResponse[0]);			
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
        public String runDataStoreCommand(java.util.List<RString> cmdList) throws StoreFactoryException {
		boolean result;
		String[] resultString = new String[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsRunDataStoreCommand(cmdList, resultString);
		} catch(Exception e) {
			// Either dps cannot be initialized or put went wrong.
			// An error code of 65535 indicates that this error occurred inside
			// the dps JNI glue layer and not inside the actual store functions.
			throw new StoreFactoryException(65535, e.getMessage());
		}

		if (result == false) {
			throw new StoreFactoryException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return(resultString[0]);
        }

	// Base64 encode the given string.
	public String base64Encode(String str) throws StoreFactoryException {
		DpsHelper dps = null;
		String encodedResultString;

		try {
			dps = DpsHelperHolder.getDpsHelper();
			encodedResultString = dps.dpsBase64Encode(str);
		} catch(Exception e) {
			// Either dps cannot be initialized or put went wrong.
			// An error code of 65535 indicates that this error occurred inside
			// the dps JNI glue layer and not inside the actual store functions.
			throw new StoreFactoryException(65535, e.getMessage());
		}		
		
		return(encodedResultString);
	}
	
	// Base64 decode the given string.
	public String base64Decode(String str) throws StoreFactoryException {
		DpsHelper dps = null;
		String decodedResultString;

		try {
			dps = DpsHelperHolder.getDpsHelper();
			decodedResultString = dps.dpsBase64Decode(str);
		} catch(Exception e) {
			// Either dps cannot be initialized or put went wrong.
			// An error code of 65535 indicates that this error occurred inside
			// the dps JNI glue layer and not inside the actual store functions.
			throw new StoreFactoryException(65535, e.getMessage());
		}		
		
		return(decodedResultString);		
	}

	// Check if there is an active connection to the back-end data store.
	public boolean isConnected() throws StoreFactoryException {
		boolean result;
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsIsConnected();
		} catch(Exception e) {
			// Either dps cannot be initialized or isConnected went wrong.
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		return(result);		
	}	

	// Reconnect with the back-end data store.
	public boolean reconnect() throws StoreFactoryException {
		boolean result;
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsReconnect();
		} catch(Exception e) {
			// Either dps cannot be initialized or reconnect went wrong.
			throw new StoreFactoryException(65535, e.getMessage());
		}
		
		return(result);		
	}	
	
}
