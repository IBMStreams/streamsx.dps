/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is the implementation class for the store iterator.
// Iteration of a store from a Java primitive operator will be 
// handled by this class.
package com.ibm.streamsx.dps.impl;

import java.util.Iterator;

import com.ibm.streamsx.dps.StoreIterator;
import com.ibm.streamsx.dps.StoreException;
import com.ibm.streamsx.dps.KeyValuePair;
import com.ibm.streamsx.dps.impl.StoreImpl;
import com.ibm.streamsx.dps.impl.DpsHelper;
import com.ibm.streamsx.dps.impl.DpsHelperHolder;

public class StoreIteratorImpl<T1> implements StoreIterator {
	private long storeId;
	private String keySplTypeName;
	private String valueSplTypeName;
	private long iterationHandle;
	private Object[] iterationResult;
	
	// Constructor method for this class with a protected access modifier.
	// It can only be instantiated from the StoreImpl class which is in the same Java package.
	protected StoreIteratorImpl(long storeId, String keySplTypeName, String valueSplTypeName) throws StoreException {
		this.storeId = storeId;
		this.keySplTypeName = keySplTypeName;
		this.valueSplTypeName = valueSplTypeName;
		// Let us get a dps store iterator now.
		beginIteration();
	}
	
	// Finalize method for this class (Will be called when this object is garbage collected.)
	// i.e. when this object goes out of scope or explicitly set to null.
	protected void finalize() throws Throwable {
		try {
			// End the store iteration.
			endIteration();
		} finally {
			super.finalize();
		}
	 }
	
	// Check if there is a next available data item in this store.
	public boolean hasNext() {
		boolean result = false;
		
		try {
			result = getNext();
		} catch (StoreException se) {
			// Since this is an implementation for the hasNext method from the 
			// java.util.Iterator interface, we can't rethrow this exception.
			// Only thing we can do is to log the exception error and return false.
			System.out.println("Error in hasNext() of an iterator for the store id " + this.storeId + ". Error code = " + 
				se.getErrorCode() + ", Error msg = " + se.getErrorMessage());
			result = false;
		}
		
		return(result);
	}
	
	// Get the next available item from this store.
	// public KeyValuePair next() throws StoreException {
	public KeyValuePair next() throws java.util.NoSuchElementException {
		// Take care of the funny attempt to get the next data item after having already finished the store iteration.
		if (iterationHandle == 0) {
			String errorMsg = "Error in next() of an iterator for the store id " + this.storeId + 
			". Error msg = " + "Attempt to get the next data item after the store iteration has already reached the end.";
			throw new java.util.NoSuchElementException(errorMsg);
		}
		
		KeyValuePair kvPair = new KeyValuePair(iterationResult[0], iterationResult[1]); 
		return(kvPair);
	}
	
	// Remove an item if supported.
	public void remove() throws java.lang.UnsupportedOperationException {
		// We don't support this during store iteration.
		throw new java.lang.UnsupportedOperationException("Remove operation is not supported during the store iteration.");
	}
	
	// Begin the iteration of this store.
	private boolean beginIteration() throws StoreException {
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			// Store the iteration handle in this object's member variable.
			iterationHandle = dps.dpsBeginIteration(storeId, err);
		} catch (Exception e) {
			// Either dps cannot be initialized or beginIteration went wrong.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			iterationHandle = 0;
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return(true);
	}
	
	// Get the next data item during the iteration of this store.
	private boolean getNext()  throws StoreException {
		long[] err = new long[1];
		DpsHelper dps = null;
		
		if (iterationHandle == 0) {
			return(false);
		}
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			// Following dps API will return an object array with two elements in it.
			// First element will hold the key object and the other will hold the value object.
			iterationResult = dps.dpsGetNext(storeId, iterationHandle, keySplTypeName, valueSplTypeName, err);
		} catch (Exception e) {
			// Either dps cannot be initialized or getNext went wrong.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		if ((iterationResult[0] == null) && (iterationResult[1] == null)) {
			// If both key and value are null, that means we reached the end of the store.
			// There are no more items to be found.
			// Use this opportunity to end the store iteration thereby releasing the
			// iteration resources allocated in the C++ backend DBLayer code.
			endIteration();
			return(false);
		} else {
			return(true);
		}
	}
	
	// End the iteration of this store.
	private void endIteration() throws StoreException {
		long[] err = new long[1];
		DpsHelper dps = null;

		if (iterationHandle == 0) {
			// Iteration has already ended. Don't try to end it again.
			return;
		}
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			dps.dpsEndIteration(storeId, iterationHandle, err);
			// Since the iteration ended, we can clear the iteration result object array.
			iterationHandle = 0;
			iterationResult = null;
		} catch (Exception e) {
			// Either dps cannot be initialized or endIteration went wrong.
			throw new StoreException(65535, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new StoreException(dps.dpsGetLastStoreErrorCode(), 
				dps.dpsGetLastStoreErrorString());
		}
		
		return;
	}
	
}

