/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
 */
package com.ibm.streamsx.dps;

import com.ibm.streams.operator.types.*;

/**
 *This is an interface for the store factory with the available methods declared here.
 * A store factory implementation class will provide concrete logic for these methods.
 * It is also the main entry point for the Time To Live (TTL) store functions.
 TTL based K/V pairs can be stored and retrieved without having to create a user specified store.
 Such K/V entries will be automatically deleted when their TTL expires.
 Only SPL primitive types, such as "rstring" and composite types, such as "map" are supported.
 Most user created store and TTL store operations (get, put, createStore) require specifying the <i>name</i> of the SPL type of the key and/or value in question, for example,
 To create a store with keys of type rstring and values of type boolean, you would need to do the following: 
<pre>{@code
	try {
	   //specify the SPL types for the keys and values in the store
	   String keyType = "rstring";
	   String valueType = "boolean";
	  Store  store = myStoreFactoryInstance.createOrGetStore("Java Test Store1", keyType, valueType);
	} catch (StoreFactoryException sfe) {
		// use	sfe.getErrorCode() and  sfe.getErrorMessage()) for more info
	}
	}
</pre>
 *
 */
public interface StoreFactory {	
	/**Creates a new store with a given store name.  
	 * The type of the key and value are the string names of any valid SPL primitive or composite types, such as <pre>{@code rstring, list<int32>}</pre>.
	 * 
	 * @param name name of the new store
	 * @param keySplTypeName the name of the SPL type for the keys, e.g. <code>rstring </code>
	 * @param valueSplTypeName name of the SPL type for the values, e.g. <code> map&lt;rstring,int32&gt;</code>
	 * @return the {@link Store} that was created.
	 * @throws StoreFactoryException if the store could not be created
	 * Example:
	 * 
	  <pre>
	  <code>
StoreFactory sf = DistributedStores.getStoreFactory();
Store store = null;
	try {
   		store = sf.createStore("Java Test Store1", "boolean", "boolean");
   		System.out.println("Created the Java Test Store1 with a storeId of " + store.getId()); 
	} catch (StoreFactoryException sfe) {
		throw sfe;
	}
	 </code> </pre> 
	 */
	public Store createStore(String name, String keySplTypeName, String valueSplTypeName) throws StoreFactoryException;
	/***Create the new store if it does not already exist or return the store if it exists.
	 * The type of the key and value are the string names of any valid SPL primitive or composite types, such as <pre>{@code rstring, list<int32>}</pre>.

	 * @param name name of the new store
	 * @param keySplTypeName the name of the SPL type for the keys, e.g. <pre>rstring </pre>
	 * @param valueSplTypeName name of the SPL type for the values, e.g. <pre>{@code  map<rstring,int32>}</pre>
	 * @return the created or retrieved {@link Store}
	 * @throws StoreFactoryException if an error occurs.
	 */
	public Store createOrGetStore(String name, String keySplTypeName, String valueSplTypeName) throws StoreFactoryException;

	/***Finds a store with a given store name
	 *  
	 * @param name The name of the store to look up 
	 * @return a {@link Store} object representing the store, if it exists
	 * @throws StoreFactoryException  if an error occurred
	 * <pre>
	 * <code>
StoreFactory  sf = DistributedStores.getStoreFactory();
Store store = null;

try {
   store = sf.findStore("Java Test Store1");
} catch (StoreFactoryException sfe) {
   System.out.println("32) Unable to find the Java Test Store1. Error code = " +
      sfe.getErrorCode() + ", Error Msg = " + sfe.getErrorMessage());
}
</code></pre>
	 */
	public Store findStore(String name) throws StoreFactoryException;
	/***Remove the given {@link Store}.
	 * 
	 * @param store the {@link Store} to remove
	 * @return true if the store existed and was successfully removed
	 * @throws StoreFactoryException if an error occurs
	 */
	public boolean removeStore(Store store) throws StoreFactoryException;
	//
	// Following special functions were added on OCT/12/2014.
	// These functions are not applicable for user created stores.
	// Instead, these functions can be used to put, get, has, and remove TTL based 
	// K/V pairs in the global area of the back-end data store. Because of that,
	// they are being made available here as part of the Store Factory class.
	//


	/*** Put a TTL based K/V pair stored in the global area of the back-end data store.
	 * 
	 * Example:
	 *  <pre>
	 <code>
	 *StoreFactory  sf = DistributedStores.getStoreFactory();
RString myKey = new RString(""), myValue = new RString("");
myKey = new RString("New Jersey");
myValue = new RString("Trenton");

try {
   // Put a K/V pair with 5 seconds of TTL.  
   // We must provide the SPL type names for our key and value as literal strings as shown below.
   sf.putTTL(myKey, myValue, 5, "rstring", "rstring");
} catch (StoreFactoryException sfe) {
   System.out.println("Unexpected error in putTTL. Error code=" + 
      sfe.getErrorCode() + ", Error msg=" + sfe.getErrorMessage());
}

	 * </code>
	 * </pre> 

	 * @param <T1> the key to store
	 * @param <T2> value to associate with the given key
	 * @param ttl The amount of time, in seconds, to keep the pair in the store. It will be automatically removed after {@code ttl} seconds.
	 * A value of '0' means that this pair will be in the store indefinitely until manually removed via removeTTL. 
	 * @param keySplTypeName name of the SPL type of the key
	 * @param value the value to insert
	 * @param valueSplTypeName name of the SPL type of the value
	 * 
	 * @return true on success.
	*@throws StoreFactoryException if an error occured
	*
	*
	*
	* */
        // There are three variations of this method via overloading.
	public <T1, T2> boolean putTTL(T1 key, T2 value, int ttl, String keySplTypeName, String valueSplTypeName) throws StoreFactoryException;
	public <T1, T2> boolean putTTL(T1 key, T2 value, int ttl, String keySplTypeName, String valueSplTypeName, boolean encodeKey, boolean encodeValue) throws StoreFactoryException;
	public <T1, T2> boolean putTTL(T1 key, T2 value, int ttl, String keySplTypeName, String valueSplTypeName, int[] storedKeyValueSize, boolean encodeKey, boolean encodeValue) throws StoreFactoryException;
	/**Get a K/V pair with TTL (Time To Live in seconds) into the global area of the back-end data store.
	@param <T1> key to lookup
	@param keySplTypeName name of the SPL type of the key
	 * @param valueSplTypeName name of the SPL type of the value
	 * @return the value if it exists
	 * @throws StoreFactoryException if  an error occurred
	 * <pre> <code>
 myComplexKey = new HashMap&lt;String, Integer&gt;();     
 myComplexKey.put("Apple", (int)1);
 myComplexKey.put("Orange", (int)2);
 try {
  Tuple	myComplexValue = (Tuple)sf.getTTL(myComplexKey, "map&lt;ustring,uint32&gt;", "tuple&lt;list&lt;rstring&gt; tickers&gt;");
  System.out.println("TTL based K/V pair is read successfully from the global store. Key=" + myComplexKey + ", Value=" + myComplexValue);
 } catch (StoreFactoryException sfe) {
  	System.out.println("Unexpected error in reading the K/V pair for " + myComplexKey + ". Error code=" + 
   		sfe.getErrorCode() + ", Error msg=" + sfe.getErrorMessage());
}</code>
	 * </pre>
	 */
	public <T1> Object getTTL(T1 key, String keySplTypeName, String valueSplTypeName) throws StoreFactoryException;
	public <T1> Object getTTL(T1 key, String keySplTypeName, String valueSplTypeName, boolean encodeKey, boolean encodeValue) throws StoreFactoryException;
	/** Remove a TTL based K/V pair stored in the global area of the back-end data store.
	 * 
	 * @param <T1> the key to remove
	 * @param keySplTypeName name of the key's SPL type
	 * @return true on success
	 * @throws StoreFactoryException if an error occurred
	 */
	public <T1> boolean removeTTL(T1 key, String keySplTypeName) throws StoreFactoryException;
	public <T1> boolean removeTTL(T1 key, String keySplTypeName, boolean encodeKey) throws StoreFactoryException;
	/**Check if a TTL based K/V pair for a given key exists in the global area of the back-end data store.
	 * @param <T1> an object of the type specified when creating the store.
	 * @param key the key to look up
	 * @param keySplTypeName the name of the type of the key, e.g. "rstring"
	 * @return whether or not the key exists
	 * @throws StoreFactoryException if an error occurs.*/
	public <T1> boolean hasTTL(T1 key, String keySplTypeName) throws StoreFactoryException;
	public <T1> boolean hasTTL(T1 key, String keySplTypeName, boolean encodeKey) throws StoreFactoryException;

	/**@return the name of the NoSQL DB product being used as a back-end data store.
	 * @throws StoreFactoryException  if an error occurs
	 * */
	public String getNoSqlDbProductName() throws StoreFactoryException;
	/**Get the details of the machine where this operator is running.
	 * @return an array containing the machine name, Linux OS version, and CPU architecture of the current machine, in that order.
	 * @throws StoreFactoryException if an error occurs
	 * */
	public String[] getDetailsAboutThisMachine() throws StoreFactoryException;
	/**Run native commands on the chosen back-end data store.
	 * @param cmd the command to run, e.g. "set foo bar"
	 * @return true on success, false otherwise.
	 * @throws StoreFactoryException if running the command failed.*/
	public boolean runDataStoreCommand(String cmd) throws StoreFactoryException;
	/*If users want to execute arbitrary back-end data store two way
    native commands, this API can be used. This is a variation of the previous API with
    overloaded function arguments. As of Nov/2014, this function is supported in the dps toolkit only
    when Cloudant NoSQL DB is used as a back-end data store. It covers any Cloudant HTTP/JSON based
    native commands that can perform both database and document related Cloudant APIs that are very
    well documented for reference on the web.
	 */
	/**This is an advanced function that can be used to execute arbitrary back-end data store two way native commands for database technologies that work with cURL.  A HTTP request is sent to the server and the response is saved in the jsonResponse paramter.   This function is not supported with Redis. Therefore, this function can only be used with database technologies that are currently not officially supported by Streams, such as HBase. See the samples for this toolkit for a detailed example.
	 *     
	 * @param cmdType 1 or 2 for Cloudant database or document type API, respectively 
	 * @param httpVerb HTTP command, e.g. GET, POST, e.t.c. COPY is not supported.
	 * @param baseUrl For the public cloud based Cloudant service, it must be in this format:
					  http://user:password@user.cloudant.com
					 For the "Cloudant Local" on-premises infrastructure, it must be in this format: 
				   http://user:password@XXXXX where XXXXX is a name or IP address of your on-premises "Cloudant Local" load balancer machine.
	     NOTE: If you give an empty string for the URL, then the Cloudant server configured
		  in the SPL project directory's etc/no-sql-kv-store-servers.cfg file will be used.
		@param apiEndpoint It should be a Cloudant DB or document related portion of the URL path as documented in the Cloudant APIs.
		@param queryParams It should be in this format name1=value1&amp;name2=value2
		@param jsonRequest This is your JSON request needed by the Cloudant API you are executing. Please ensure that any special  characters such as double quotes are properly escaped using the backslash character.     
	 * @param httpResponseCode On succesful execution of the command, then this will be the HTTP response code returned by the Cloudant server.
	 You have to interpret the meaning of the returned HTTP response code and make your further logic from there. 
	 * @return the json response string.
	 * @throws StoreFactoryException if an error occurs.
	 */
	
	public String runDataStoreCommand(int cmdType, String httpVerb,
			String baseUrl, String apiEndpoint, String queryParams, String jsonRequest, long[] httpResponseCode) throws StoreFactoryException;


        /*
        If users want to send any valid Redis command to the Redis server made up as individual parts,
        this API can be used. This will work only with Redis. Users simply have to split their
        valid Redis command into individual parts that appear between spaces and pass them in 
        exacly in that order via a list<rstring>. DPS back-end code will put them together 
        correctly before executing the command on a configured Redis server. This API will also
        return the resulting value from executing any given Redis command as a string. It is upto
        the caller to interpret the Redis returned value and make sense out of it.
        In essence, it is a two way Redis command which is very diffferent from the other plain
        API that is explained above. [NOTE: If you have to deal with storing or fetching 
        non-string complex Streams data types, you can't use this API. Instead, use the other
        DPS put/get/remove/has DPS APIs.]
        @param cmdList A java.util.List of RString representing individual parts that make up the Redis command to be run.
        @return Result string returned by Redis after executing the given command.
        @throws StoreFactoryException if an error occurs while executing the Redis command.
        */
        public String runDataStoreCommand(java.util.List<RString> cmdList) throws StoreFactoryException;

	/** Base64 encode the given string.
	 * @param str string to encode
	 * @return the encoded representation of the given string
	 * @throws StoreFactoryException on error
	*/

	public String base64Encode(String str) throws StoreFactoryException;

	/**Decode the given base64 string.
	 * @param str the base64 string to decode
	 * @return decoded value
	 * @throws StoreFactoryException if an error occurs*/
	public String base64Decode(String str) throws StoreFactoryException;

        /** Check if there is an active connection to the back-end data store.
         *  @return current connection status.
         *  @throws StoreFactoryException if an error occurs */
	public boolean isConnected() throws StoreFactoryException;

        /** Reconnect with the back-end data store.
         *  @return Reconnection status.
         *  @throws StoreFactoryException if an error occurs */
	public boolean reconnect() throws StoreFactoryException;
}
