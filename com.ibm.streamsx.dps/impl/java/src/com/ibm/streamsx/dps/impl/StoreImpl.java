/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is the implementation class for the store.
// Through the object of this class, a Java dps user can execute all the available dps APIs to
// perform store operations.
package com.ibm.streamsx.dps.impl;

import com.ibm.streamsx.dps.Store;
import com.ibm.streamsx.dps.StoreException;
import com.ibm.streamsx.dps.StoreIterator;
import com.ibm.streamsx.dps.KeyValuePair;

import java.util.Iterator;
import java.nio.ByteBuffer;

// Implement the store interface for store operations. In addition, that inteface also 
// extends the java.lang.Iterable interface to provide a nice set of store iteration APIs in Java style.
public class StoreImpl implements Store
{	
	private long id;
	private String name;
	private String keySplTypeName;
	private String valueSplTypeName;
	
	// Create a store implementation.
	public StoreImpl(String name, long id, String keySplTypeName, String valueSplTypeName) {
		this.name = name;
		this.id = id;
		this.keySplTypeName = keySplTypeName;
		this.valueSplTypeName = valueSplTypeName;
	}
	
	// Get the store id.
	public long getId() {
		return(id);
	}

	// Get the name of this store.
	public String getStoreName() {
		return(name);
	}
	
	// Get the SPL type name of this store's key.
	public String getKeySplTypeName() {
		return(keySplTypeName);
	}
	
	// Get the SPL type name of this store's value.
	public String getValueSplTypeName() {
		return(valueSplTypeName);
	}
			
	// Put a data item into this store. (Faster put method with no internal safety checks.)
	public <T1, T2> boolean put(T1 key, T2 value) throws StoreException {
		boolean result;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsPut(id, key, value, keySplTypeName, valueSplTypeName, err);
		} catch(Exception e) {
			// Either dps cannot be initialized or put went wrong.
			// An error code of 65535 indicates that this error occurred inside
			// the dps JNI glue layer and not inside the actual store functions.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return(result);
	}

	// Put a data item into this store. (Slower put method due to overhead from internal safety checks.)
	public <T1, T2> boolean putSafe(T1 key, T2 value) throws StoreException {
		boolean result;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsPutSafe(id, key, value, keySplTypeName, valueSplTypeName, err);
		} catch(Exception e) {
			// Either dps cannot be initialized or put went wrong.
			// An error code of 65535 indicates that this error occurred inside
			// the dps JNI glue layer and not inside the actual store functions.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return(result);
	}	
	
	// Get a data item from this store. (Faster Get method with no internal safety checks.)
	public <T1> Object get(T1 key) throws StoreException {
		Object result = null;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsGet(id, key, keySplTypeName, valueSplTypeName, err);
		} catch(Exception e) {
			// Either dps cannot be initialized or get went wrong.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return(result);		
	}

	// Get a data item from this store. (Slower get method due to overhead from internal safety checks..)
	public <T1> Object getSafe(T1 key) throws StoreException {
		Object result = null;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsGetSafe(id, key, keySplTypeName, valueSplTypeName, err);
		} catch(Exception e) {
			// Either dps cannot be initialized or get went wrong.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return(result);		
	}	
	
	// Remove a data item from this store.
	public <T1> boolean remove(T1 key) throws StoreException {
		boolean result;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsRemove(id, key, keySplTypeName, valueSplTypeName, err);
		} catch(Exception e) {
			// Either dps cannot be initialized or remove went wrong.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return(result);			
	}

	// Check if a given data item key exists in this store.
	public <T1> boolean has(T1 key) throws StoreException {
		boolean result;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsHas(id, key, keySplTypeName, valueSplTypeName, err);
		} catch(Exception e) {
			// Either dps cannot be initialized or has went wrong.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return(result);		
	}
	
	// Clear (i.e. empty) this store by removing all its contents.
	public void clear() throws StoreException {
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			dps.dpsClear(id, err);
		} catch(Exception e) {
			// Either dps cannot be initialized or clear went wrong.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}		
	}
	
	// Get the size of this store.
	public long size() throws StoreException {
		long result;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsSize(id, err);
		} catch(Exception e) {
			// Either dps cannot be initialized or size went wrong.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}		
		
		return(result);
	}	
				
	// Serialize the contents of this store into a ByteBuffer.
	public ByteBuffer serialize() throws StoreException {
		ByteBuffer result;
		long[] err = new long[1];
		DpsHelper dps = null;
				
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dpsSerialize(id, keySplTypeName, valueSplTypeName, err);
		} catch (Exception e) {
			// Either dps cannot be initialized or serialize went wrong.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return(result);
	}
	
	// Deserialize the byte buffer contents into this store to populate data items.
	public void deserialize(ByteBuffer data) throws StoreException {
		long[] err = new long[1];
		DpsHelper dps = null;
				
		try {
			dps = DpsHelperHolder.getDpsHelper();
			dps.dpsDeserialize(id, data, keySplTypeName, valueSplTypeName, err);
		} catch (Exception e) {
			// Either dps cannot be initialized or deserialize went wrong.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return;
	}
	
	// Get an iterator for this store.
	public StoreIteratorImpl iterator() {
		StoreIteratorImpl it = null;
		
		try {
			// Instantiate a store iterator class.
			it = new StoreIteratorImpl<KeyValuePair>(id, keySplTypeName, valueSplTypeName);
		} catch (StoreException se) {
			// Since we are providing implementation for the iterator method from the java.lang.Iterable
			// interface, we can't rethrow this exception. All we can do is simply log the exception error and
			// return a null iterator.
			System.out.println("Error in getting an iterator for the store " + name + ". Error code = " + 
        		se.getErrorCode() + ", Error msg = " + se.getErrorMessage());
		}
		
		return(it);
	}
}
