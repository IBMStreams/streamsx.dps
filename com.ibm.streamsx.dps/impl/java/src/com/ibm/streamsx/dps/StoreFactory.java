/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is an interface for the store factory with the available methods declared here.
// A store factory implementaton class will provide concrete logic for these methods.
package com.ibm.streamsx.dps;

public interface StoreFactory {	
	public Store createStore(String name, String keySplTypeName, String valueSplTypeName) throws StoreFactoryException;
	public Store createOrGetStore(String name, String keySplTypeName, String valueSplTypeName) throws StoreFactoryException;
	public Store findStore(String name) throws StoreFactoryException;
	public boolean removeStore(Store store) throws StoreFactoryException;
	//
	// Following special functions were added on OCT/12/2014.
	// These functions are not applicable for user created stores.
	// Instead, these functions can be used to put, get, has, and remove TTL based 
	// K/V pairs in the global area of the back-end data store. Because of that,
	// they are being made available here as part of the Store Factory class.
	// TTL based K/V pairs can be stored and retrieved without having to create a user specified store.
	// Such K/V entries will be automatically deleted when their TTL expires.
	//
	// Put a K/V pair with TTL (Time To Live in seconds) into the global area of the back-end data store.
	public <T1, T2> boolean putTTL(T1 key, T2 value, int ttl, String keySplTypeName, String valueSplTypeName) throws StoreFactoryException;
	// Get a TTL based K/V pair stored in the global area of the back-end data store.
	public <T1> Object getTTL(T1 key, String keySplTypeName, String valueSplTypeName) throws StoreFactoryException;
	// Remove a TTL based K/V pair stored in the global area of the back-end data store.
	public <T1> boolean removeTTL(T1 key, String keySplTypeName) throws StoreFactoryException;
	// Check if a TTL based K/V pair for a given key exists in the global area of the back-end data store.
	public <T1> boolean hasTTL(T1 key, String keySplTypeName) throws StoreFactoryException;
	
	//Get the name of the NoSQL DB product being used as a back-end data store.
	public String getNoSqlDbProductName() throws StoreFactoryException;
	// Get the details of the machine where this operator is running.
	public String[] getDetailsAboutThisMachine() throws StoreFactoryException;
	// Run native commands on the chosen back-end data store.
	public boolean runDataStoreCommand(String cmd) throws StoreFactoryException;
	/*
    If users want to execute arbitrary back-end data store two way
    native commands, this API can be used. This is a variation of the previous API with
    overloaded function arguments. As of Nov/2014, this API is supported in the dps toolkit only
    when Cloudant NoSQL DB is used as a back-end data store. It covers any Cloudant HTTP/JSON based
    native commands that can perform both database and document related Cloudant APIs that are very
    well documented for reference on the web.
    */
	public String runDataStoreCommand(int cmdType, String httpVerb,
		String baseUrl, String apiEndpoint, String queryParams, String jsonRequest, long[] httpResponseCode) throws StoreFactoryException;
	// Base64 encode the given string.
	public String base64Encode(String str) throws StoreFactoryException;
	// Base64 decode the given string.
	public String base64Decode(String str) throws StoreFactoryException;
}
