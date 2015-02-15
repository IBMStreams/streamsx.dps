/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is an interface file where all the available dps store methods are declared.
// A store implemntation class will provide concrete logic for these methods.
package com.ibm.streamsx.dps;

import com.ibm.streamsx.dps.KeyValuePair;
import com.ibm.streamsx.dps.impl.StoreIteratorImpl;
import java.lang.Iterable;
import java.nio.ByteBuffer;

public interface Store extends Iterable<KeyValuePair> {
	public long getId();
	// Faster Put method with no internal safety checks.	
	public <T1, T2> boolean put(T1 key, T2 value) throws StoreException;
	// Slower Put method due to overhead arising from internal safety checks.
	public <T1, T2> boolean putSafe(T1 key, T2 value) throws StoreException;
	// Faster Get method with no internal safety checks.
	public <T1> Object get(T1 key) throws StoreException;
	// Slower Get method due to overhead arising from internal safety checks.
	public <T1> Object getSafe(T1 key) throws StoreException;
	public <T1> boolean remove(T1 key) throws StoreException;
	public <T1> boolean has(T1 key) throws StoreException;
	public void clear() throws StoreException;
	public long size() throws StoreException;
	public ByteBuffer serialize() throws StoreException;
	public void deserialize(ByteBuffer data) throws StoreException;
	public String getStoreName();
	public String getKeySplTypeName();
	public String getValueSplTypeName();
	public StoreIterator iterator();
}
