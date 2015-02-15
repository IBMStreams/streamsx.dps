/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This class keeps a key and a value read from a store. It is used in the
// dps store iteration performed inside the Java primitive operators.
package com.ibm.streamsx.dps;

public class KeyValuePair {	
	private Object key;
	private Object value;
	
	// Create a key value pair object.
	public KeyValuePair(Object key, Object value) {
		this.key = key;
		this.value = value;
	}
	
	// Get the key.
	public Object getKey() {
		return(key);
	}

	// Get the value.
	public Object getValue() {
		return(value);
	}
}
